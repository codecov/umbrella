import logging
import random
import time
from contextlib import contextmanager
from enum import Enum

import sentry_sdk
from redis import Redis  # type: ignore
from redis.exceptions import ConnectionError as RedisConnectionError  # type: ignore
from redis.exceptions import LockError  # type: ignore
from redis.exceptions import TimeoutError as RedisTimeoutError  # type: ignore

from database.enums import ReportType
from shared.celery_config import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_SECONDS,
)
from shared.helpers.redis import get_redis_connection  # type: ignore

log = logging.getLogger(__name__)

MAX_RETRY_COUNTDOWN_SECONDS = 60 * 60 * 5
BASE_RETRY_COUNTDOWN_SECONDS = 200
RETRY_BACKOFF_MULTIPLIER = 3
RETRY_COUNTDOWN_RANGE_DIVISOR = 2
LOCK_NAME_SEPARATOR = "_lock_"
LOCK_ATTEMPTS_KEY_PREFIX = "lock_attempts:"
LOCK_ATTEMPTS_TTL_SECONDS = (
    86400  # TTL for attempt counter key so it expires after one day
)

# Exponential backoff calculation: BASE * MULTIPLIER^retry_num
# With BASE=200 and MULTIPLIER=3, this yields:
# retry_num=0: 200s (~3.3 min), retry_num=1: 600s (~10 min),
# retry_num=2: 1800s (~30 min), retry_num=3: 5400s (~90 min),
# retry_num=4: 16200s (~4.5 hours), retry_num>=4: capped at 5 hours


class LockType(Enum):
    BUNDLE_ANALYSIS_PROCESSING = "bundle_analysis_processing"
    BUNDLE_ANALYSIS_NOTIFY = "bundle_analysis_notify"
    NOTIFICATION = "notify"
    UPLOAD = "upload"
    UPLOAD_PROCESSING = "upload_processing"
    UPLOAD_FINISHER = "upload_finisher"
    PREPROCESS_UPLOAD = "preprocess_upload"
    MANUAL_TRIGGER = "manual_trigger"


class LockRetry(Exception):
    """Raised when lock acquisition fails. Check max_retries_exceeded to determine if retries should stop."""

    def __init__(
        self,
        countdown: int,
        max_retries_exceeded: bool = False,
        retry_num: int | None = None,
        max_retries: int | None = None,
        lock_name: str | None = None,
        repoid: int | None = None,
        commitid: str | None = None,
    ):
        self.countdown = countdown
        self.max_retries_exceeded = max_retries_exceeded
        self.retry_num = retry_num
        self.max_retries = max_retries
        self.lock_name = lock_name
        self.repoid = repoid
        self.commitid = commitid
        if max_retries_exceeded:
            error_msg = (
                f"Lock acquisition failed after {retry_num} retries (max: {max_retries}). "
                f"Repo: {repoid}, Commit: {commitid}"
            )
            super().__init__(error_msg)
        else:
            super().__init__(f"Lock acquisition failed, retry in {countdown} seconds")


class LockManager:
    def __init__(
        self,
        repoid: int,
        commitid: str,
        report_type=ReportType.COVERAGE,
        lock_timeout=DEFAULT_LOCK_TIMEOUT_SECONDS,
        blocking_timeout: int | None = DEFAULT_BLOCKING_TIMEOUT_SECONDS,
        redis_connection: Redis | None = None,
    ):
        self.repoid = repoid
        self.commitid = commitid
        self.report_type = report_type
        self.lock_timeout = lock_timeout
        self.blocking_timeout = blocking_timeout
        self.redis_connection = redis_connection or get_redis_connection()

    def lock_name(self, lock_type: LockType):
        if self.report_type == ReportType.COVERAGE:
            return (
                f"{lock_type.value}{LOCK_NAME_SEPARATOR}{self.repoid}_{self.commitid}"
            )
        else:
            return f"{lock_type.value}{LOCK_NAME_SEPARATOR}{self.repoid}_{self.commitid}_{self.report_type.value}"

    def _clear_lock_attempt_counter(self, attempt_key: str, lock_name: str) -> None:
        """Clear the lock attempt counter. Log and swallow Redis errors so teardown does not mask other failures."""
        try:
            self.redis_connection.delete(attempt_key)
        except (RedisConnectionError, RedisTimeoutError, OSError) as e:
            log.warning(
                "Failed to clear lock attempt counter (Redis unavailable or error)",
                extra={
                    "attempt_key": attempt_key,
                    "commitid": self.commitid,
                    "lock_name": lock_name,
                    "repoid": self.repoid,
                },
                exc_info=True,
            )

    @contextmanager
    def locked(
        self,
        lock_type: LockType,
        retry_num=0,
        max_retries: int | None = None,
    ):
        """Acquire a Redis lock with retry logic.

        Args:
            lock_type: Type of lock to acquire
            retry_num: Attempt count (should be self.attempts from BaseCodecovTask).
                Used for both exponential backoff and max retry checking.
            max_retries: Maximum total attempts (stop when attempts >= this).
        """
        lock_name = self.lock_name(lock_type)
        attempt_key = f"{LOCK_ATTEMPTS_KEY_PREFIX}{lock_name}"
        try:
            log.info(
                "Acquiring lock",
                extra={
                    "commitid": self.commitid,
                    "lock_name": lock_name,
                    "lock_type": lock_type.value,
                    "repoid": self.repoid,
                },
            )
            with self.redis_connection.lock(
                lock_name,
                timeout=self.lock_timeout,
                blocking_timeout=self.blocking_timeout,
            ):
                lock_acquired_time = time.time()
                log.info(
                    "Acquired lock",
                    extra={
                        "commitid": self.commitid,
                        "lock_name": lock_name,
                        "lock_type": lock_type.value,
                        "repoid": self.repoid,
                    },
                )
                try:
                    yield
                finally:
                    self._clear_lock_attempt_counter(attempt_key, lock_name)
                lock_duration = time.time() - lock_acquired_time
                log.info(
                    "Releasing lock",
                    extra={
                        "commitid": self.commitid,
                        "lock_duration_seconds": lock_duration,
                        "lock_name": lock_name,
                        "lock_type": lock_type.value,
                        "repoid": self.repoid,
                    },
                )
        except LockError:
            # incr/expire can raise RedisConnectionError/RedisTimeoutError when Redis
            # is unavailable; we let those propagate so the task fails once (no infinite loop).
            attempts = self.redis_connection.incr(attempt_key)
            if attempts == 1:
                self.redis_connection.expire(attempt_key, LOCK_ATTEMPTS_TTL_SECONDS)

            max_retry_unbounded = BASE_RETRY_COUNTDOWN_SECONDS * (
                RETRY_BACKOFF_MULTIPLIER**retry_num
            )
            if max_retry_unbounded >= MAX_RETRY_COUNTDOWN_SECONDS:
                countdown = MAX_RETRY_COUNTDOWN_SECONDS
            else:
                countdown_unbounded = random.randint(
                    max_retry_unbounded // RETRY_COUNTDOWN_RANGE_DIVISOR,
                    max_retry_unbounded,
                )
                countdown = countdown_unbounded

            if max_retries is not None and attempts >= max_retries:
                error = LockRetry(
                    countdown=0,
                    max_retries_exceeded=True,
                    retry_num=attempts,
                    max_retries=max_retries,
                    lock_name=lock_name,
                    repoid=self.repoid,
                    commitid=self.commitid,
                )
                log.error(
                    "Not retrying since we already had too many attempts",
                    extra={
                        "attempts": attempts,
                        "commitid": self.commitid,
                        "lock_name": lock_name,
                        "lock_type": lock_type.value,
                        "max_retries": max_retries,
                        "repoid": self.repoid,
                        "report_type": self.report_type.value,
                    },
                    exc_info=True,
                )
                sentry_sdk.capture_exception(
                    error,
                    contexts={
                        "lock_acquisition": {
                            "attempts": attempts,
                            "blocking_timeout": self.blocking_timeout,
                            "commitid": self.commitid,
                            "lock_name": lock_name,
                            "lock_timeout": self.lock_timeout,
                            "lock_type": lock_type.value,
                            "max_retries": max_retries,
                            "repoid": self.repoid,
                            "report_type": self.report_type.value,
                        }
                    },
                    tags={
                        "error_type": "lock_max_retries_exceeded",
                        "lock_name": lock_name,
                        "lock_type": lock_type.value,
                    },
                )
                raise error

            log.warning(
                "Unable to acquire lock",
                extra={
                    "commitid": self.commitid,
                    "countdown": countdown,
                    "lock_name": lock_name,
                    "lock_type": lock_type.value,
                    "max_retries": max_retries,
                    "repoid": self.repoid,
                    "retry_num": retry_num,
                },
            )
            raise LockRetry(countdown)
