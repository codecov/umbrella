"""
Dedupes the enqueue of the `save_commit_measurements` task.

The upload finisher runs once per batch of uploads for a commit and used to enqueue
a save task every time, so a commit with many uploads produced many concurrent tasks
upserting the same timeseries rows. Only the first finisher in a short window should
enqueue; the task runs `TIMESERIES_SAVE_COUNTDOWN_SECONDS` later and reads the latest
merged report.

The gate TTL must stay below the countdown: any finisher that checks the gate before
it expires has already made its merge durable, so the delayed task will see it, and a
finisher arriving after expiry enqueues a fresh task.
"""

import logging

from shared.helpers.redis import get_redis_connection

log = logging.getLogger(__name__)

TIMESERIES_SAVE_GATE_KEY_PREFIX = "timeseries_save"
TIMESERIES_SAVE_GATE_TTL_SECONDS = 45
TIMESERIES_SAVE_COUNTDOWN_SECONDS = 60


def timeseries_save_gate_key(repo_id: int, commit_sha: str) -> str:
    return f"{TIMESERIES_SAVE_GATE_KEY_PREFIX}_{repo_id}_{commit_sha}"


def try_acquire_timeseries_save_gate(repo_id: int, commit_sha: str) -> bool:
    """Returns True if the caller should enqueue the save task.

    Fails open: if Redis is unavailable we enqueue, as before this gate existed.
    """
    try:
        return bool(
            get_redis_connection().set(
                timeseries_save_gate_key(repo_id, commit_sha),
                "1",
                nx=True,
                ex=TIMESERIES_SAVE_GATE_TTL_SECONDS,
            )
        )
    except Exception:
        log.warning("Timeseries save gate check failed, enqueuing", exc_info=True)
        return True


def release_timeseries_save_gate(repo_id: int, commit_sha: str) -> None:
    """Best effort: lets the next finisher enqueue after a failed enqueue."""
    try:
        get_redis_connection().delete(timeseries_save_gate_key(repo_id, commit_sha))
    except Exception:
        log.warning("Failed to release timeseries save gate", exc_info=True)
