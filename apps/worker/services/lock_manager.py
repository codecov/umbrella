import logging
import random
import time
from contextlib import contextmanager
from enum import Enum

import sentry_sdk
from redis import Redis  # type: ignore
from redis.exceptions import LockError  # type: ignore

from shared.django_apps.enums import ReportType
from shared.celery_config import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_SECONDS,
)
from shared.config import get_config
from shared.helpers.redis import get_redis_connection  # type: ignore

log = logging.getLogger(__name__)

MAX_RETRY_COUNTDOWN_SECONDS = 60 * 60 * 5
BASE_RETRY_COUNTDOWN_SECONDS = 200
RETRY_BACKOFF_MULTIPLIER = 3
RETRY_COUNTDOWN_RANGE_DIVISOR = 2
LOCK_NAME_SEPARATOR = "_lock_"


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
        max_attempts: int | None = None,
        lock_name: str | None = None,
        repoid: int | None = None,
        commitid: str | None = None,
    ):
        self.countdown = countdown
        self.max_retries_exceeded = max_retries_exceeded
        self.retry_num = retry_num
        self.max_attempts = max_attempts
        self.lock_name = lock_name
        self.repoid = repoid
        self.commitid = commitid
        if max_retries_exceeded:
            error_msg = (
                f"Lock acquisition failed after {retry_num} retries (max: {max_attempts}). "
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
        base_retry_countdown: int = BASE_RETRY_COUNTDOWN_SECONDS,
    ):
        self.repoid = repoid
        self.commitid = commitid
        self.report_type = report_type
        self.lock_timeout = lock_timeout
        self.blocking_timeout = blocking_timeout
        self.redis_connection = redis_connection or get_redis_connection()
        self.base_retry_countdown = base_retry_countdown

    def lock_name(self, lock_type: LockType):
        if self.report_type == ReportType.COVERAGE:
            return (
                f"{lock_type.value}{LOCK_NAME_SEPARATOR}{self.repoid}_{self.commitid}"
            )
        else:
            return f"{lock_type.value}{LOCK_NAME_SEPARATOR}{self.repoid}_{self.commitid}_{self.report_type.value}"

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
            max_retries: Maximum number of retries allowed
        """
        lock_name = self.lock_name(lock_type)
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
                yield
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
            max_retry_unbounded = self.base_retry_countdown * (
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

            if max_retries is not None and retry_num > max_retries:
                max_attempts = max_retries + 1
                error = LockRetry(
                    countdown=0,
                    max_retries_exceeded=True,
                    retry_num=retry_num,
                    max_attempts=max_attempts,
                    lock_name=lock_name,
                    repoid=self.repoid,
                    commitid=self.commitid,
                )
                log.error(
                    "Not retrying since we already had too many retries",
                    extra={
                        "commitid": self.commitid,
                        "lock_name": lock_name,
                        "lock_type": lock_type.value,
                        "max_attempts": max_attempts,
                        "max_retries": max_retries,
                        "repoid": self.repoid,
                        "report_type": self.report_type.value,
                        "retry_num": retry_num,
                    },
                    exc_info=True,
                )
                sentry_sdk.capture_exception(
                    error,
                    contexts={
                        "lock_acquisition": {
                            "blocking_timeout": self.blocking_timeout,
                            "commitid": self.commitid,
                            "lock_name": lock_name,
                            "lock_timeout": self.lock_timeout,
                            "lock_type": lock_type.value,
                            "max_attempts": max_attempts,
                            "repoid": self.repoid,
                            "report_type": self.report_type.value,
                            "retry_num": retry_num,
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


def get_bundle_analysis_lock_manager(
    repoid: int,
    commitid: str,
    redis_connection: Redis | None = None,
) -> LockManager:
    """
    Create a LockManager configured for Bundle Analysis tasks with optimized
    settings for high-concurrency scenarios.

    Returns a LockManager with:
    - blocking_timeout: 30s (default) - wait longer before giving up
    - base_retry_countdown: 10s (default) - faster retries than default 200s
    """
    return LockManager(
        repoid=repoid,
        commitid=commitid,
        report_type=ReportType.BUNDLE_ANALYSIS,
        blocking_timeout=int(
            get_config(
                "setup", "tasks", "bundle_analysis", "blocking_timeout", default=30
            )
        ),
        base_retry_countdown=int(
            get_config(
                "setup",
                "tasks",
                "bundle_analysis",
                "base_retry_countdown",
                default=10,
            )
        ),
        redis_connection=redis_connection,
    )
