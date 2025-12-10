import logging
import random
import time
from contextlib import contextmanager
from enum import Enum

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
    def locked(self, lock_type: LockType, retry_num=0):
        """
        Context manager for acquiring a distributed lock.

        Args:
            lock_type: Type of lock to acquire
            retry_num: Current retry number (used for calculating exponential backoff countdown)

        Raises:
            LockRetry: If lock cannot be acquired, with countdown for retry scheduling.
                      The calling task should handle max_retries checking via self.retry().
        """
        lock_name = self.lock_name(lock_type)
        try:
            log.info(
                "Acquiring lock",
                extra={
                    "repoid": self.repoid,
                    "commitid": self.commitid,
                    "lock_name": lock_name,
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
                        "repoid": self.repoid,
                        "commitid": self.commitid,
                        "lock_name": lock_name,
                    },
                )
                try:
                    yield
                finally:
                    # Ensure lock release is logged even if an exception occurs (e.g., timeout)
                    # The Redis lock's __exit__ will release the lock automatically
                    lock_duration = time.time() - lock_acquired_time
                    log.info(
                        "Releasing lock",
                        extra={
                            "repoid": self.repoid,
                            "commitid": self.commitid,
                            "lock_name": lock_name,
                            "lock_duration_seconds": lock_duration,
                        },
                    )
        except LockError:
            max_retry_cap = 60 * 60 * 5  # 5 hours
            max_retry_unbounded = 200 * 3**retry_num
            countdown_unbounded = random.randint(
                max_retry_unbounded // 2, max_retry_unbounded
            )
            countdown = min(countdown_unbounded, max_retry_cap)

            log.warning(
                "Unable to acquire lock",
                extra={
                    "repoid": self.repoid,
                    "commitid": self.commitid,
                    "lock_name": lock_name,
                    "countdown": countdown,
                    "retry_num": retry_num,
                },
            )
            raise LockRetry(countdown)
