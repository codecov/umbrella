import logging
import random
import time
from contextlib import contextmanager
from enum import Enum

import sentry_sdk
from redis import Redis  # type: ignore
from redis.exceptions import LockError  # type: ignore

from database.enums import ReportType
from shared.celery_config import (
    DEFAULT_BLOCKING_TIMEOUT_SECONDS,
    DEFAULT_LOCK_TIMEOUT_SECONDS,
)
from shared.helpers.redis import get_redis_connection  # type: ignore

log = logging.getLogger(__name__)


class LockType(Enum):
    BUNDLE_ANALYSIS_PROCESSING = "bundle_analysis_processing"
    BUNDLE_ANALYSIS_NOTIFY = "bundle_analysis_notify"
    NOTIFICATION = "notify"
    UPLOAD = "upload"
    UPLOAD_PROCESSING = "upload_processing"
    UPLOAD_FINISHER = "upload_finisher"
    PREPROCESS_UPLOAD = "preprocess_upload"
    MANUAL_TRIGGER = "manual_trigger"
    # NOTE: All tasks following the repoid+commitid pattern have been migrated to use LockManager.
    # The following tasks cannot be migrated because they use different lock naming patterns:
    # - sync_repos.py: owner-based locks (syncrepos_lock_{ownerid}_{using_integration})
    # - sync_pull.py: pull-based locks (pullsync_{repoid}_{pullid})
    # - crontasks.py: task-name based locks (worker.executionlock.{task_name})


class LockRetry(Exception):
    def __init__(self, countdown: int):
        self.countdown = countdown


class LockMaxRetriesExceededError(Exception):
    """Raised when lock acquisition exceeds max retries."""

    def __init__(
        self,
        retry_num: int,
        max_attempts: int,
        lock_name: str,
        repoid: int,
        commitid: str,
    ):
        self.retry_num = retry_num
        self.max_attempts = max_attempts
        self.lock_name = lock_name
        self.repoid = repoid
        self.commitid = commitid
        super().__init__("Lock acquisition failed after exceeding max retries")


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
            # for backward compat this does not include the report type
            return f"{lock_type.value}_lock_{self.repoid}_{self.commitid}"
        else:
            return f"{lock_type.value}_lock_{self.repoid}_{self.commitid}_{self.report_type.value}"

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
            max_retry_cap = 60 * 60 * 5  # 5 hours
            max_retry_unbounded = 200 * 3**retry_num
            countdown_unbounded = random.randint(
                max_retry_unbounded // 2, max_retry_unbounded
            )
            countdown = min(countdown_unbounded, max_retry_cap)

            # None means no max retries (infinite retries)
            max_attempts = max_retries + 1 if max_retries is not None else None
            if max_attempts is not None and retry_num >= max_attempts:
                error = LockMaxRetriesExceededError(
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
                            "max_retries": max_retries,
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
                # TODO: should we raise this, or would a return be ok?
                raise LockRetry(countdown)

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
