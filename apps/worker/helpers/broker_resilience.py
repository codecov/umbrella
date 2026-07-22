"""Make the Celery broker consumer resilient to undecodable messages.

kombu's Redis transport deserializes every message with JSON in the
consumer event loop (`Channel._brpop_read` -> `loads(...)`) *before* any
task-level handling runs. A single non-JSON element on a queue therefore
raises `JSONDecodeError` straight out of the consumer loop, which Celery
treats as an "Unrecoverable error" and the whole worker process exits --
a crash loop that takes down the entire pool. Sources include a stray
`redis_admin` clear tombstone popped mid-clear (or orphaned by a clear
job killed mid-run) and any otherwise-corrupted broker payload.

kombu already guards its fanout/LISTEN path this way (`_receive_one`
catches `(TypeError, ValueError)` and skips) but NOT the main BRPOP queue
path. We wrap `_brpop_read` with the same protection: an undecodable
message is logged and dropped -- it has already been removed from Redis
by `BRPOP`, and `_brpop_read`'s own `finally` resets the poll state -- so
the consumer simply skips delivery and continues with the next message.
"""

from __future__ import annotations

import json
import logging
import threading

from kombu.transport import redis as kombu_redis

log = logging.getLogger(__name__)

# Mirror kombu._receive_one's caught types (`TypeError`, `ValueError`)
# plus the byte-decode error. `json.JSONDecodeError` subclasses
# `ValueError`, so it is already covered, but we list it for clarity.
# Deliberately NARROW: connection errors (re-raised by `_brpop_read` for
# reconnect) and `queue.Empty` (normal "no message" signal) are NOT in
# this tuple and must keep propagating.
_DECODE_ERRORS = (json.JSONDecodeError, ValueError, TypeError, UnicodeDecodeError)

# An incident can produce hundreds of thousands of drops; log the first
# and then every _LOG_EVERY-th so the problem is visible without flooding
# logs / Sentry breadcrumbs.
_LOG_EVERY = 1000


class _DropStats:
    """Thread-safe counter of dropped messages (avoids a module `global`)."""

    def __init__(self) -> None:
        self.count = 0
        self.lock = threading.Lock()


_drops = _DropStats()


def _note_drop(exc: BaseException) -> None:
    with _drops.lock:
        _drops.count += 1
        count = _drops.count
    if count == 1 or count % _LOG_EVERY == 0:
        log.warning(
            "broker_resilience: dropped undecodable broker message (%s); "
            "consumer continuing. total_dropped=%d",
            type(exc).__name__,
            count,
        )


def install_broker_resilience() -> bool:
    """Idempotently wrap kombu's redis `Channel._brpop_read`.

    Returns True if the patch was applied, False if it was already in
    place.
    """

    channel_cls = kombu_redis.Channel
    if getattr(channel_cls, "_codecov_resilient_brpop", False):
        return False

    _orig_brpop_read = channel_cls._brpop_read

    def _brpop_read(self, **options):
        try:
            return _orig_brpop_read(self, **options)
        except _DECODE_ERRORS as exc:
            _note_drop(exc)
            # Message already removed from Redis by BRPOP and the poll
            # state was reset in `_brpop_read`'s finally; returning None
            # skips delivery so the hub proceeds to the next event.
            return None

    _brpop_read.__wrapped__ = _orig_brpop_read  # type: ignore[attr-defined]
    channel_cls._brpop_read = _brpop_read
    channel_cls._codecov_resilient_brpop = True
    log.info("broker_resilience: installed resilient _brpop_read on %s", channel_cls)
    return True
