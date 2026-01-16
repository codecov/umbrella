import logging
from contextlib import contextmanager
from typing import cast

from celery.exceptions import MaxRetriesExceededError as CeleryMaxRetriesExceededError
from redis import Redis

from services.lock_manager import LockManager, LockRetry, LockType

log = logging.getLogger(__name__)

SKIP_DEBOUNCE_TOKEN = 1
"""
Token value to use in tests to skip debounce acquisition step.

When passed as fencing_token, the task will skip acquiring a new token
and proceed directly to the main notification logic. This is useful for
tests that want to test the core functionality without the debounce retry.
"""


class LockAcquisitionLimitError(Exception):
    """Raised when lock acquisition attempts exceed the maximum allowed.

    This is distinct from Celery's MaxRetriesExceededError which tracks task retries.
    This exception specifically tracks failed attempts to acquire a distributed lock.
    """

    pass


class NotificationDebouncer[T]:
    """
    Handles debouncing logic for notification tasks using fencing tokens.

    This ensures that when multiple notification tasks are queued for the same
    commit, only the latest one actually sends a notification after a debounce period.
    """

    def __init__(
        self,
        redis_key_template: str,
        debounce_period_seconds: int = 30,
        lock_type: LockType = LockType.NOTIFICATION,
        max_lock_retries: int = 5,
    ):
        """
        :param redis_key_template: Template string for Redis key, e.g. "notifier_fence:{}_{}"
        :param debounce_period_seconds: How long to wait before sending notification
        :param lock_type: Type of lock to use for notification locking
        :param max_lock_retries: Maximum number of retries for lock acquisition
        """
        self.redis_key_template = redis_key_template
        self.debounce_period_seconds = debounce_period_seconds
        self.lock_type = lock_type
        self.max_lock_retries = max_lock_retries

    def acquire_fencing_token(
        self,
        redis_client: Redis,
        repo_id: int,
        commit_sha: str,
    ) -> int:
        """
        Acquire a fencing token for deduplication.

        :returns: The acquired fencing token value
        """
        redis_key = self.redis_key_template.format(repo_id, commit_sha)
        with redis_client.pipeline() as pipeline:
            pipeline.incr(redis_key)
            pipeline.expire(redis_key, 24 * 60 * 60)
            results = pipeline.execute()
            return int(cast(str, results[0]))

    def check_fencing_token_stale(
        self,
        redis_client: Redis,
        repo_id: int,
        commit_sha: str,
        fencing_token: int,
    ) -> bool:
        """
        Check if the fencing token is stale, indicating another notification task
        is in progress and can take over.

        This method should be called within a lock context.

        :returns: True if the token is stale (task should exit early),
                  False if the token is current (task should continue).
        """
        current_token = int(
            cast(
                str,
                redis_client.get(self.redis_key_template.format(repo_id, commit_sha))
                or "0",
            )
        )

        return fencing_token < current_token

    @contextmanager
    def notification_lock(self, lock_manager: LockManager, task_attempts: int):
        """
        Context manager to handle the repeated lock acquisition pattern
        with automatic retry handling for LockRetry exceptions.
        """
        try:
            with lock_manager.locked(
                self.lock_type,
                max_retries=self.max_lock_retries,
                retry_num=task_attempts,
            ):
                yield
                return
        except LockRetry as e:
            if e.max_retries_exceeded:
                log.error(
                    "Not retrying lock acquisition - max retries exceeded",
                    extra={
                        "retry_num": e.retry_num,
                        "max_attempts": e.max_attempts,
                    },
                )
                raise LockAcquisitionLimitError(
                    f"Lock acquisition limit exceeded: {e.retry_num} >= {e.max_attempts}",
                )
            raise

    def handle_lock_retry(
        self,
        task,
        lock_retry: LockRetry,
        failure_result: T,
    ) -> T:
        """
        Handle LockRetry exception by attempting to retry the task.

        :param task: The Celery task instance
        :param lock_retry: The LockRetry exception
        :param failure_result: The result to return if max retries exceeded
        :returns: failure_result if max retries exceeded
        :raises Retry: When retry is successfully scheduled (caller should catch this)
        """
        try:
            task.retry(max_retries=task.max_retries, countdown=lock_retry.countdown)
            raise AssertionError("task.retry() should always raise Retry")
        except CeleryMaxRetriesExceededError:
            return failure_result
