import logging
import time
from contextlib import contextmanager
from datetime import datetime

import sentry_sdk
from celery._state import get_current_task
from celery.exceptions import MaxRetriesExceededError, SoftTimeLimitExceeded
from celery.worker.request import Request
from django.db import InterfaceError, close_old_connections
from sqlalchemy.exc import (
    DataError,
    IntegrityError,
    InvalidRequestError,
    SQLAlchemyError,
)

from app import celery_app
from celery_task_router import _get_ownerid_from_task, _get_user_plan_from_task
from database.engine import get_db_session
from database.enums import CommitErrorTypes
from database.models.core import (
    GITHUB_APP_INSTALLATION_DEFAULT_NAME,
    Commit,
    Repository,
)
from helpers.checkpoint_logger import from_kwargs as load_checkpoints_from_kwargs
from helpers.checkpoint_logger.flows import TestResultsFlow, UploadFlow
from helpers.clock import get_seconds_to_next_hour
from helpers.exceptions import NoConfiguredAppsAvailable, RepositoryWithoutValidBotError
from helpers.log_context import LogContext, set_log_context
from helpers.save_commit_error import save_commit_error
from services.repository import get_repo_provider_service
from shared.celery_config import (
    TASK_RETRY_BACKOFF_BASE_SECONDS,
    upload_breadcrumb_task_name,
)
from shared.celery_router import route_tasks_based_on_user_plan
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
)
from shared.metrics import Counter, Histogram
from shared.torngit.base import TorngitBaseAdapter
from shared.torngit.exceptions import TorngitClientError, TorngitRepoNotFoundError
from shared.typings.torngit import AdditionalData
from shared.utils.sentry import current_sentry_trace_id

log = logging.getLogger("worker")

REQUEST_TIMEOUT_COUNTER = Counter(
    "worker_task_counts_timeouts",
    "Number of times a task experienced any kind of timeout",
    ["task"],
)
REQUEST_HARD_TIMEOUT_COUNTER = Counter(
    "worker_task_counts_hard_timeouts",
    "Number of times a task experienced a hard timeout",
    ["task"],
)

TASK_RETRY_WITH_COUNT_COUNTER = Counter(
    "worker_task_counts_retries_by_count",
    "Number of times this task was retried, labeled by retry count",
    ["task", "retry_count"],
)
TASK_MAX_RETRIES_EXCEEDED_COUNTER = Counter(
    "worker_task_counts_max_retries_exceeded",
    "Number of times this task exceeded maximum retry limit",
    ["task"],
)


class BaseCodecovRequest(Request):
    @property
    def metrics_prefix(self):
        return f"worker.task.{self.name}"

    def on_timeout(self, soft: bool, timeout: int):
        res = super().on_timeout(soft, timeout)
        if not soft:
            REQUEST_HARD_TIMEOUT_COUNTER.labels(task=self.name).inc()
            try:
                self._capture_hard_timeout_to_sentry(timeout)
            except Exception:
                pass

        REQUEST_TIMEOUT_COUNTER.labels(task=self.name).inc()

        if UploadFlow.has_begun():
            UploadFlow.log(UploadFlow.CELERY_TIMEOUT)
        if TestResultsFlow.has_begun():
            TestResultsFlow.log(TestResultsFlow.CELERY_TIMEOUT)

        return res

    def _capture_hard_timeout_to_sentry(self, timeout: int):
        """Capture hard timeout to Sentry with immediate flush.

        Hard timeouts result in SIGKILL, so we must flush immediately
        to ensure the event is sent before the process is killed.
        """
        scope = sentry_sdk.get_current_scope()
        scope.set_tag("failure_type", "hard_timeout")
        scope.set_context(
            "timeout",
            {
                "task_name": self.name,
                "task_id": self.id,
                "timeout_seconds": timeout,
            },
        )
        sentry_sdk.capture_message(
            f"Hard timeout in task {self.name} after {timeout}s",
            level="error",
        )
        sentry_sdk.flush(timeout=2)


# Task reliability metrics
TASK_RUN_COUNTER = Counter(
    "worker_task_counts_runs", "Number of times this task was run", ["task"]
)
TASK_RETRY_COUNTER = Counter(
    "worker_task_counts_retries", "Number of times this task was retried", ["task"]
)
TASK_SUCCESS_COUNTER = Counter(
    "worker_task_counts_successes",
    "Number of times this task completed without error",
    ["task"],
)
TASK_FAILURE_COUNTER = Counter(
    "worker_task_counts_failures",
    "Number of times this task failed with an exception",
    ["task"],
)

# Task runtime metrics
TASK_FULL_RUNTIME = Histogram(
    "worker_task_timers_full_runtime_seconds",
    "Total runtime in seconds of this task including db commits and error handling",
    ["task"],
    buckets=[0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 180, 300, 600, 900],
)
TASK_CORE_RUNTIME = Histogram(
    "worker_task_timers_core_runtime_seconds",
    "Runtime in seconds of this task's main logic, not including db commits or error handling",
    ["task"],
    buckets=[0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 180, 300, 600, 900],
)
TASK_TIME_IN_QUEUE = Histogram(
    "worker_tasks_timers_time_in_queue_seconds",
    "Time in seconds spent waiting in the queue before being run",
    ["task", "queue"],
    buckets=[
        0.05,
        0.1,
        0.25,
        0.5,
        0.75,
        1,
        2,
        5,
        7,
        15,
        30,
        60,
        120,
        300,
        600,
        1200,
    ],
)


def _get_request_headers(request) -> dict:
    """Extract headers from Celery request object, returning empty dict if None."""
    if request is None:
        return {}
    return getattr(request, "headers", {}) or {}


class BaseCodecovTask(celery_app.Task):
    """Base task for Codecov workers.

    In this codebase, max_retries is the maximum total attempts (cap on runs).
    We stop when attempts >= max_retries. The name matches Celery/config.
    """

    Request = BaseCodecovRequest

    def __init_subclass__(cls, name=None):
        cls.name = name

        cls.metrics_prefix = f"worker.task.{name}"
        cls.task_run_counter = TASK_RUN_COUNTER.labels(task=name)
        cls.task_retry_counter = TASK_RETRY_COUNTER.labels(task=name)
        cls.task_success_counter = TASK_SUCCESS_COUNTER.labels(task=name)
        cls.task_failure_counter = TASK_FAILURE_COUNTER.labels(task=name)
        cls.task_full_runtime = TASK_FULL_RUNTIME.labels(task=name)
        cls.task_core_runtime = TASK_CORE_RUNTIME.labels(task=name)

    @property
    def hard_time_limit_task(self):
        if self.request.timelimit is not None and self.request.timelimit[0] is not None:
            return self.request.timelimit[0]
        if self.time_limit is not None:
            return self.time_limit
        return self.app.conf.task_time_limit or 0

    def get_lock_timeout(self, default_timeout: int) -> int:
        """
        Calculate the lock timeout based on hard_time_limit_task.

        Returns the maximum of default_timeout and hard_time_limit_task.
        In production, hard_time_limit_task always returns a numeric value.

        Args:
            default_timeout: The default lock timeout to use

        Returns:
            The calculated lock timeout
        """
        return max(default_timeout, self.hard_time_limit_task)

    @sentry_sdk.trace
    def apply_async(self, args=None, kwargs=None, **options):
        db_session = get_db_session()
        user_plan = _get_user_plan_from_task(db_session, self.name, kwargs)
        ownerid = _get_ownerid_from_task(db_session, self.name, kwargs)
        route_with_extra_config = route_tasks_based_on_user_plan(
            self.name, user_plan, ownerid
        )
        extra_config = route_with_extra_config.get("extra_config", {})
        celery_compatible_config = {
            "time_limit": extra_config.get("hard_timelimit", None),
            "soft_time_limit": extra_config.get("soft_timelimit", None),
            "user_plan": user_plan,
        }
        options = {**options, **celery_compatible_config}

        # Explicitly propagate parent_id from the current task context.
        # Celery only does this automatically for chains/chords; manual
        # apply_async calls from within a task would lose the lineage.
        if "parent_id" not in options:
            caller = get_current_task()
            if caller and caller.request:
                options["parent_id"] = getattr(caller.request, "id", None)

        opt_headers = options.pop("headers", {})
        opt_headers = opt_headers if opt_headers is not None else {}

        # Track creation time and attempts in headers for queue time metrics
        # and visibility timeout re-delivery detection
        current_time = datetime.now()
        headers = {
            **opt_headers,
            "created_timestamp": current_time.isoformat(),
            # Preserve existing attempts if present (e.g., from retry or re-delivery)
            # Only set to 1 if this is a new task creation
            "attempts": opt_headers.get("attempts", 1),
        }
        return super().apply_async(args=args, kwargs=kwargs, headers=headers, **options)

    def retry(self, max_retries=None, countdown=None, exc=None, **kwargs):
        """Override Celery's retry to always update attempts header."""
        request = getattr(self, "request", None)
        current_attempts = self.attempts
        kwargs["headers"] = {
            **_get_request_headers(request),
            **(kwargs.get("headers") or {}),
            "attempts": current_attempts + 1,
        }

        return super().retry(
            max_retries=max_retries, countdown=countdown, exc=exc, **kwargs
        )

    @property
    def attempts(self) -> int:
        """Get attempts count including re-deliveries from visibility timeout expiration.

        This is a property (not an instance variable) because:
        1. The request object is set by Celery after task instantiation (not available in __init__)
        2. We need to handle task reuse across different executions (different request IDs)
        3. We need to handle cases where request doesn't exist yet

        The value is cached in __dict__ keyed by request ID to avoid recomputation.
        """
        request = getattr(self, "request", None)
        if request is None:
            return 0

        # Cache the computed value keyed by request ID to handle task reuse across executions
        request_id = getattr(request, "id", None)
        cache_key = f"_attempts_{request_id}" if request_id else "_attempts"

        if cache_key in self.__dict__:
            return self.__dict__[cache_key]

        headers = _get_request_headers(request)
        attempts_header = headers.get("attempts")
        if attempts_header is not None:
            try:
                attempts_value = int(attempts_header)
                self.__dict__[cache_key] = attempts_value
                return attempts_value
            except (ValueError, TypeError):
                log.warning(
                    "Invalid attempts header value",
                    extra={
                        "value": attempts_header,
                        "retry_count": getattr(request, "retries", 0),
                    },
                )
        retry_count = getattr(request, "retries", 0)
        attempts_value = retry_count + 1
        self.__dict__[cache_key] = attempts_value
        return attempts_value

    def _has_exceeded_max_attempts(self, max_retries: int | None) -> bool:
        """Return True if attempts >= max_retries (max_retries is max total attempts)."""
        if max_retries is None:
            return False

        return self.attempts >= max_retries

    def safe_retry(self, countdown=None, exc=None, **kwargs):
        """Safely retry with max retry limit. Returns False if max exceeded, otherwise raises Retry."""
        if self._has_exceeded_max_attempts(self.max_retries):
            log.error(
                f"Task {self.name} exceeded max retries",
                extra={
                    "attempts": self.attempts,
                    "max_retries": self.max_retries,
                    "task_name": self.name,
                },
            )
            TASK_MAX_RETRIES_EXCEEDED_COUNTER.labels(task=self.name).inc()
            sentry_sdk.capture_exception(
                MaxRetriesExceededError(
                    f"Task {self.name} exceeded max retries: {self.attempts} >= {self.max_retries}"
                ),
                contexts={
                    "task": {
                        "attempts": self.attempts,
                        "max_retries": self.max_retries,
                        "task_name": self.name,
                    }
                },
                tags={"error_type": "max_retries_exceeded", "task": self.name},
            )
            return False

        retry_count = (
            getattr(self.request, "retries", 0) if hasattr(self, "request") else 0
        )
        if countdown is None:
            countdown = TASK_RETRY_BACKOFF_BASE_SECONDS * (2**retry_count)

        try:
            self.retry(
                max_retries=self.max_retries, countdown=countdown, exc=exc, **kwargs
            )
        except MaxRetriesExceededError:
            TASK_MAX_RETRIES_EXCEEDED_COUNTER.labels(task=self.name).inc()
            if UploadFlow.has_begun():
                UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
            if TestResultsFlow.has_begun():
                TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
            sentry_sdk.capture_exception(
                MaxRetriesExceededError(
                    f"Task {self.name} exceeded max retries during retry() call"
                ),
                contexts={
                    "task": {
                        "max_retries": self.max_retries,
                        "task_name": self.name,
                    }
                },
                tags={"error_type": "max_retries_exceeded", "task": self.name},
            )
            return False

    def _analyse_error(self, exception: SQLAlchemyError, *args, **kwargs):
        """Analyze SQLAlchemy errors and log appropriate messages for PostgreSQL-specific issues."""
        try:
            import psycopg2  # noqa: PLC0415

            if hasattr(exception, "orig") and isinstance(
                exception.orig, psycopg2.errors.DeadlockDetected
            ):
                log.exception(
                    "Deadlock while talking to database",
                    extra={"task_args": args, "task_kwargs": kwargs},
                    exc_info=True,
                )
                return
            elif hasattr(exception, "orig") and isinstance(
                exception.orig, psycopg2.OperationalError
            ):
                log.warning(
                    "Database seems to be unavailable",
                    extra={"task_args": args, "task_kwargs": kwargs},
                    exc_info=True,
                )
                return
        except ImportError:
            log.debug(
                "psycopg2 not available, skipping PostgreSQL-specific error analysis"
            )
        log.exception(
            "An error talking to the database occurred",
            extra={"task_args": args, "task_kwargs": kwargs},
            exc_info=True,
        )

    @sentry_sdk.trace
    def run(self, *args, **kwargs):
        with self.task_full_runtime.time():
            db_session = get_db_session()

            log_context = LogContext(
                repo_id=kwargs.get("repoid") or kwargs.get("repo_id"),
                owner_id=kwargs.get("ownerid"),
                commit_sha=kwargs.get("commitid") or kwargs.get("commit_id"),
            )

            task = get_current_task()
            if task and task.request:
                log_context.task_name = task.name
                task_id = getattr(task.request, "id", None)
                if task_id:
                    log_context.task_id = task_id
                log_context.parent_task_id = getattr(task.request, "parent_id", None)

            log_context.populate_from_sqlalchemy(db_session)
            set_log_context(log_context)
            load_checkpoints_from_kwargs([UploadFlow, TestResultsFlow], kwargs)

            self.task_run_counter.inc()
            if (
                hasattr(self, "request")
                and self.request is not None
                and hasattr(self.request, "get")
            ):
                created_timestamp = self.request.get("created_timestamp", None)
                if created_timestamp:
                    enqueued_time = datetime.fromisoformat(created_timestamp)
                    now = datetime.now()
                    delta = now - enqueued_time

                    delivery_info = self.request.get("delivery_info", {})
                    queue_name = (
                        delivery_info.get("routing_key", None)
                        if isinstance(delivery_info, dict)
                        else None
                    )
                    time_in_queue_timer = TASK_TIME_IN_QUEUE.labels(
                        task=self.name, queue=queue_name
                    )
                    time_in_queue_timer.observe(delta.total_seconds())

            close_old_connections()

            try:
                with self.task_core_runtime.time():
                    return self.run_impl(db_session, *args, **kwargs)
            except InterfaceError as ex:
                sentry_sdk.capture_exception(
                    ex,
                )
            except (DataError, IntegrityError):
                log.exception(
                    "Errors related to the constraints of database happened",
                    extra={"task_args": args, "task_kwargs": kwargs},
                )
                db_session.rollback()
                retry_count = getattr(self.request, "retries", 0)
                countdown = TASK_RETRY_BACKOFF_BASE_SECONDS * (2**retry_count)
                try:
                    if not self.safe_retry(countdown=countdown):
                        return None
                except MaxRetriesExceededError:
                    if UploadFlow.has_begun():
                        UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
                    if TestResultsFlow.has_begun():
                        TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
                    return None
            except SQLAlchemyError as ex:
                self._analyse_error(ex, args, kwargs)
                db_session.rollback()
                retry_count = getattr(self.request, "retries", 0)
                countdown = TASK_RETRY_BACKOFF_BASE_SECONDS * (2**retry_count)
                try:
                    if not self.safe_retry(countdown=countdown):
                        return None
                except MaxRetriesExceededError:
                    if UploadFlow.has_begun():
                        UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
                    if TestResultsFlow.has_begun():
                        TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
                    return None
            except MaxRetriesExceededError as ex:
                if UploadFlow.has_begun():
                    UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
                if TestResultsFlow.has_begun():
                    TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
            finally:
                self.wrap_up_dbsession(db_session)

    def wrap_up_dbsession(self, db_session):
        """Commit and close database session, handling timeout edge cases.

        Handles the corner case where `SoftTimeLimitExceeded` is raised during
        `db_session.commit()`, which can leave the session in an unusable state.
        Since we reuse sessions across tasks, this would break future tasks in
        the same process, so we catch both timeout and invalid state exceptions.
        """
        try:
            db_session.commit()
            db_session.close()
        except SoftTimeLimitExceeded:
            log.warning(
                "We had an issue where a timeout happened directly during the DB commit",
                exc_info=True,
            )
            try:
                db_session.commit()
                db_session.close()
            except InvalidRequestError:
                log.warning(
                    "DB session cannot be operated on any longer. Closing it and removing it",
                    exc_info=True,
                )
                get_db_session.remove()
        except InvalidRequestError:
            log.warning(
                "DB session cannot be operated on any longer. Closing it and removing it",
                exc_info=True,
            )
            get_db_session.remove()

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        res = super().on_retry(exc, task_id, args, kwargs, einfo)
        self.task_retry_counter.inc()
        retry_count = (
            getattr(self.request, "retries", 0) if hasattr(self, "request") else 0
        )
        TASK_RETRY_WITH_COUNT_COUNTER.labels(
            task=self.name, retry_count=str(retry_count)
        ).inc()
        return res

    def on_success(self, retval, task_id, args, kwargs):
        res = super().on_success(retval, task_id, args, kwargs)
        self.task_success_counter.inc()
        return res

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """Handle task failure, logging flow checkpoints and incrementing metrics."""
        res = super().on_failure(exc, task_id, args, kwargs, einfo)
        self.task_failure_counter.inc()

        if UploadFlow.has_begun():
            UploadFlow.log(UploadFlow.CELERY_FAILURE)
        if TestResultsFlow.has_begun():
            TestResultsFlow.log(TestResultsFlow.CELERY_FAILURE)

        return res

    def get_repo_provider_service(
        self,
        repository: Repository,
        installation_name_to_use: str = GITHUB_APP_INSTALLATION_DEFAULT_NAME,
        additional_data: AdditionalData | None = None,
        commit: Commit | None = None,
    ) -> TorngitBaseAdapter | None:
        try:
            return get_repo_provider_service(
                repository, installation_name_to_use, additional_data
            )
        except RepositoryWithoutValidBotError:
            if commit:
                save_commit_error(
                    commit,
                    error_code=CommitErrorTypes.REPO_BOT_INVALID.value,
                    error_params={"repoid": repository.repoid},
                )
                self._call_upload_breadcrumb_task(
                    commit_sha=commit.commitid,
                    repo_id=repository.repoid,
                    error=Errors.REPO_MISSING_VALID_BOT,
                )
            log.warning(
                "Unable to reach git provider because repo doesn't have a valid bot"
            )
        except NoConfiguredAppsAvailable as exp:
            if exp.rate_limited_count > 0:
                # At least one GitHub app is available but rate-limited. Retry after
                # waiting until the next hour (minimum 1 minute delay).
                retry_delay_seconds = max(60, get_seconds_to_next_hour())
                log.warning(
                    "Unable to get repo provider service due to rate limits. Retrying again later.",
                    extra={
                        "apps_available": exp.apps_count,
                        "apps_rate_limited": exp.rate_limited_count,
                        "apps_suspended": exp.suspended_count,
                        "countdown_seconds": retry_delay_seconds,
                    },
                )
                if commit:
                    self._call_upload_breadcrumb_task(
                        commit_sha=commit.commitid,
                        repo_id=repository.repoid,
                        error=Errors.INTERNAL_APP_RATE_LIMITED,
                    )
                    self._call_upload_breadcrumb_task(
                        commit_sha=commit.commitid,
                        repo_id=repository.repoid,
                        error=Errors.INTERNAL_RETRYING,
                    )
                self.retry(countdown=retry_delay_seconds)
            else:
                log.warning(
                    "Unable to get repo provider service. Apps appear to be suspended.",
                    extra={
                        "apps_available": exp.apps_count,
                        "apps_rate_limited": exp.rate_limited_count,
                        "apps_suspended": exp.suspended_count,
                    },
                )
        except TorngitRepoNotFoundError:
            if commit:
                self._call_upload_breadcrumb_task(
                    commit_sha=commit.commitid,
                    repo_id=repository.repoid,
                    error=Errors.REPO_NOT_FOUND,
                )
            log.warning(
                "Unable to reach git provider because this specific bot/integration can't see that repository",
                extra={"repoid": repository.repoid},
            )
        except TorngitClientError:
            if commit:
                self._call_upload_breadcrumb_task(
                    commit_sha=commit.commitid,
                    repo_id=repository.repoid,
                    error=Errors.GIT_CLIENT_ERROR,
                )
            log.warning(
                "Unable to get repo provider service",
                extra={"repoid": repository.repoid},
                exc_info=True,
            )
        except Exception as e:
            log.exception("Uncaught exception when trying to get repository service")
            if commit:
                self._call_upload_breadcrumb_task(
                    commit_sha=commit.commitid,
                    repo_id=repository.repoid,
                    error=Errors.UNKNOWN,
                    error_text=repr(e),
                )

        return None

    @contextmanager
    def with_logged_lock(self, lock, lock_name: str, **extra_log_context):
        """
        Context manager that wraps a Redis lock with standardized logging.

        This method provides consistent lock logging across all tasks:
        - Logs "Acquiring lock" before attempting to acquire
        - Logs "Acquired lock" after successful acquisition
        - Logs "Releasing lock" with duration after release

        Args:
            lock: A Redis lock object (from redis_connection.lock())
            lock_name: Name of the lock for logging purposes
            **extra_log_context: Additional context to include in all log messages

        Example:
            with self.with_logged_lock(
                redis_connection.lock(lock_name, timeout=300),
                lock_name=lock_name,
                repoid=repoid,
                commitid=commitid,
            ):
                # Your code here
                pass
        """
        log.info(
            "Acquiring lock",
            extra={
                "lock_name": lock_name,
                **extra_log_context,
            },
        )
        with lock:
            lock_acquired_time = time.time()
            log.info(
                "Acquired lock",
                extra={
                    "lock_name": lock_name,
                    **extra_log_context,
                },
            )
            try:
                yield
            finally:
                lock_duration = time.time() - lock_acquired_time
                log.info(
                    "Releasing lock",
                    extra={
                        "lock_name": lock_name,
                        "lock_duration_seconds": lock_duration,
                        **extra_log_context,
                    },
                )

    def _call_upload_breadcrumb_task(
        self,
        commit_sha: str,
        repo_id: int,
        milestone: Milestones | None = None,
        upload_ids: list[int] = [],
        error: Errors | None = None,
        error_text: str | None = None,
    ):
        """
        Queue a task to create an upload breadcrumb.
        """
        try:
            self.app.tasks[upload_breadcrumb_task_name].apply_async(
                kwargs={
                    "commit_sha": commit_sha,
                    "repo_id": repo_id,
                    "breadcrumb_data": BreadcrumbData(
                        milestone=milestone, error=error, error_text=error_text
                    ),
                    "upload_ids": upload_ids,
                    "sentry_trace_id": current_sentry_trace_id(),
                }
            )
        except Exception:
            log.exception(
                "Failed to queue upload breadcrumb task",
                extra={
                    "commit_sha": commit_sha,
                    "repo_id": repo_id,
                    "milestone": milestone,
                    "upload_ids": upload_ids,
                    "error": error,
                    "error_text": error_text,
                },
            )
