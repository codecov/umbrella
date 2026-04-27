"""Registry of Redis key "families" surfaced in the admin.

A `Family` describes a Redis key pattern owned by some part of the system,
how to recognise it, and (in later milestones) how to extract repo/commit
metadata and decode individual items.

Milestone 1 only needs the bare minimum to enumerate keys and report their
type and depth in the admin changelist, so we ship two representative
families today (`uploads` and `ta_flake_key`). Subsequent milestones extend
this module rather than touching the queryset/admin layers.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Literal

from . import conn as _conn
from . import settings as redis_admin_settings

log = logging.getLogger(__name__)


RedisType = Literal["list", "set", "hash", "string"]


@dataclass(frozen=True)
class ParsedKey:
    """Structured view of a Redis key once we know which family it belongs to.

    All fields are optional because not every family encodes every dimension
    in its key. Later milestones populate `repoid`, `commitid`, etc. from the
    family-specific parser; in M1 we only need `family` so the admin can show
    the family column.
    """

    family: str
    repoid: int | None = None
    commitid: str | None = None
    report_type: str | None = None
    upload_id: str | None = None


def _parse_uploads_key(key: str) -> ParsedKey:
    # Shape: uploads/{repoid}/{commitid}[/{report_type}]
    return ParsedKey(family="uploads")


def _parse_ta_flake_key(key: str) -> ParsedKey:
    # Shape: ta_flake_key:{repoid}
    return ParsedKey(family="ta_flake_key")


@dataclass(frozen=True)
class Family:
    name: str
    scan_pattern: str
    redis_type: RedisType
    parse_key: Callable[[str], ParsedKey]
    fixed_keys: tuple[str, ...] = field(default_factory=tuple)


FAMILIES: tuple[Family, ...] = (
    Family(
        name="uploads",
        scan_pattern="uploads/*",
        redis_type="list",
        parse_key=_parse_uploads_key,
    ),
    Family(
        name="ta_flake_key",
        scan_pattern="ta_flake_key:*",
        redis_type="list",
        parse_key=_parse_ta_flake_key,
    ),
)


def _decode(value: bytes | str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="replace")
    return value


def iter_keys(redis=None) -> Iterator[tuple[str, Family]]:
    """Yield `(key, family)` pairs for every known family.

    Uses Redis SCAN with a per-call `COUNT` and a hard cap on the total number
    of keys returned across all families so a runaway keyspace cannot block
    Redis. Always returns the matching `Family` so callers do not have to
    re-classify the key.
    """

    redis = redis if redis is not None else _conn.get_connection()
    seen = 0
    cap = redis_admin_settings.MAX_SCAN_KEYS
    count = redis_admin_settings.SCAN_COUNT

    for family in FAMILIES:
        for key in family.fixed_keys:
            if redis.exists(key):
                yield key, family
                seen += 1
                if seen >= cap:
                    log.warning(
                        "redis_admin: hit MAX_SCAN_KEYS=%s while iterating fixed keys",
                        cap,
                    )
                    return

        if not family.scan_pattern:
            continue

        for raw_key in redis.scan_iter(match=family.scan_pattern, count=count):
            yield _decode(raw_key), family
            seen += 1
            if seen >= cap:
                log.warning(
                    "redis_admin: hit MAX_SCAN_KEYS=%s while scanning %s",
                    cap,
                    family.scan_pattern,
                )
                return
