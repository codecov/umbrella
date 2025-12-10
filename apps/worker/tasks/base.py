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

# Task retry tracking with retry count
TASK_RETRY_WITH_COUNT_COUNTER = Counter(
    "worker_task_counts_retries_by_count",
    "Number of times this task was retried, labeled by retry count",
    ["task", "retry_count"],
)

# Task max retries exceeded counter
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
        REQUEST_TIMEOUT_COUNTER.labels(task=self.name).inc()

        if UploadFlow.has_begun():
            UploadFlow.log(UploadFlow.CELERY_TIMEOUT)
        if TestResultsFlow.has_begun():
            TestResultsFlow.log(TestResultsFlow.CELERY_TIMEOUT)

        return res


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
    "Time in {TODO} spent waiting in the queue before being run",
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


class BaseCodecovTask(celery_app.Task):
    Request = BaseCodecovRequest

    def __init_subclass__(cls, name=None):
        cls.name = name

        cls.metrics_prefix = f"worker.task.{name}"

        # Task reliability metrics
        cls.task_run_counter = TASK_RUN_COUNTER.labels(task=name)
        cls.task_retry_counter = TASK_RETRY_COUNTER.labels(task=name)
        cls.task_success_counter = TASK_SUCCESS_COUNTER.labels(task=name)
        cls.task_failure_counter = TASK_FAILURE_COUNTER.labels(task=name)

        # Task runtime metrics
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

        opt_headers = options.pop("headers", {})
        opt_headers = opt_headers if opt_headers is not None else {}

        # Pass current time in task headers so we can emit a metric of
        # how long the task was in the queue for
        current_time = datetime.now()
        headers = {
            **opt_headers,
            "created_timestamp": current_time.isoformat(),
        }
        return super().apply_async(args=args, kwargs=kwargs, headers=headers, **options)

    def retry(
        self, countdown=None, exc=None, kwargs=None, max_retries=None, **other_kwargs
    ):
        """
        Override Celery's retry() to automatically check max_retries and track metrics.
        If max_retries is not provided, uses self.max_retries from the task class.

        Args:
            countdown: Seconds to wait before retry (optional)
            exc: Exception to include in retry (optional)
            kwargs: Task kwargs to use when retrying (optional, Celery-specific parameter)
            max_retries: Maximum number of retries allowed (default: task's max_retries)
            **other_kwargs: Additional kwargs to pass to Celery's retry()
        """
        request = getattr(self, "request", None)
        current_retries = request.retries if request else 0
        task_max_retries = (
            max_retries if max_retries is not None else getattr(self, "max_retries", 3)
        )

        request_kwargs = {}
        if request and hasattr(request, "kwargs"):
            request_kwargs = request.kwargs if request.kwargs is not None else {}
        # Note: kwargs parameter is Celery's special parameter for task kwargs on retry
        retry_kwargs = kwargs if kwargs is not None else {}
        all_kwargs = {**request_kwargs, **retry_kwargs}

        context = {}
        if all_kwargs.get("commitid") is not None:
            context["commitid"] = all_kwargs.get("commitid")
        if all_kwargs.get("repoid") is not None:
            context["repoid"] = all_kwargs.get("repoid")
        if all_kwargs.get("report_type") is not None:
            context["report_type"] = all_kwargs.get("report_type")
        if task_max_retries is not None and current_retries >= task_max_retries:
            log.error(
                f"Task {self.name} exceeded max retries",
                extra={
                    "context": context if context else None,
                    "current_retries": current_retries,
                    "max_retries": task_max_retries,
                    "task_name": self.name,
                },
            )
            TASK_MAX_RETRIES_EXCEEDED_COUNTER.labels(task=self.name).inc()
            if UploadFlow.has_begun():
                UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
            if TestResultsFlow.has_begun():
                TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
            raise MaxRetriesExceededError(
                f"Task {self.name} exceeded max retries ({task_max_retries})"
            )

        # Default countdown: exponential backoff if not provided
        # Uses TASK_RETRY_BACKOFF_BASE_SECONDS from shared config (default: 20s)
        if countdown is None:
            countdown = TASK_RETRY_BACKOFF_BASE_SECONDS * (2**current_retries)

        # Log retry attempt as warning
        log.warning(
            f"Task {self.name} retrying",
            extra={
                "context": context if context else None,
                "countdown": countdown,
                "current_retries": current_retries,
                "exception_type": type(exc).__name__ if exc else None,
                "max_retries": task_max_retries,
                "task_id": getattr(request, "id", None) if request else None,
                "task_name": self.name,
            },
        )

        if kwargs is not None:
            return super().retry(
                countdown=countdown,
                exc=exc,
                kwargs=kwargs,
                max_retries=task_max_retries,
                **other_kwargs,
            )
        else:
            return super().retry(
                countdown=countdown,
                exc=exc,
                max_retries=task_max_retries,
                **other_kwargs,
            )

    def _analyse_error(self, exception: SQLAlchemyError, *args, **kwargs):
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
            pass
        log.exception(
            "An error talking to the database occurred",
            extra={"task_args": args, "task_kwargs": kwargs},
            exc_info=True,
        )

    def _emit_queue_metrics(self):
        created_timestamp = self.request.get("created_timestamp", None)
        if created_timestamp:
            enqueued_time = datetime.fromisoformat(created_timestamp)
            now = datetime.now()
            delta = now - enqueued_time

            queue_name = self.request.get("delivery_info", {}).get("routing_key", None)
            time_in_queue_timer = TASK_TIME_IN_QUEUE.labels(
                task=self.name, queue=queue_name
            )  # TODO is None a valid label value
            time_in_queue_timer.observe(delta.total_seconds())

    @sentry_sdk.trace
    def run(self, *args, **kwargs):
        with self.task_full_runtime.time():  # Timer isn't tested
            db_session = get_db_session()

            log_context = LogContext(
                repo_id=kwargs.get("repoid") or kwargs.get("repo_id"),
                owner_id=kwargs.get("ownerid"),
                commit_sha=kwargs.get("commitid") or kwargs.get("commit_id"),
            )

            task = get_current_task()
            if task and task.request:
                log_context.task_name = task.name
                log_context.task_id = task.request.id

            log_context.populate_from_sqlalchemy(db_session)
            set_log_context(log_context)
            load_checkpoints_from_kwargs([UploadFlow, TestResultsFlow], kwargs)

            self.task_run_counter.inc()
            self._emit_queue_metrics()

            close_old_connections()

            try:
                with self.task_core_runtime.time():  # Timer isn't tested
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
                self.retry()
            except SQLAlchemyError as ex:
                self._analyse_error(ex, args, kwargs)
                db_session.rollback()
                self.retry()
            except MaxRetriesExceededError as ex:
                if UploadFlow.has_begun():
                    UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
                if TestResultsFlow.has_begun():
                    TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
            finally:
                self.wrap_up_dbsession(db_session)

    def wrap_up_dbsession(self, db_session):
        """
        Wraps up dbsession, commita what is relevant and closes the session

        This function deals with the very corner case of when a `SoftTimeLimitExceeded`
            is raised during the execution of `db_session.commit()`. When it happens,
            the dbsession gets into a bad state, which disallows further operations in it.

        And because we reuse dbsessions, this would mean future tasks happening inside the
            same process would also lose access to db.

        So we need to do two ugly exception-catching:
            1) For if `SoftTimeLimitExceeded` was raised  while commiting
            2) For if the exception left `db_session` in an unusable state
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
        # Track retry count for better observability
        retry_count = self.request.retries if hasattr(self, "request") else 0
        TASK_RETRY_WITH_COUNT_COUNTER.labels(
            task=self.name, retry_count=str(retry_count)
        ).inc()
        return res

    def on_success(self, retval, task_id, args, kwargs):
        res = super().on_success(retval, task_id, args, kwargs)
        self.task_success_counter.inc()
        return res

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        """
        Includes SoftTimeLimitExceeded, for example
        """
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
                # There's at least 1 app that we can use to communicate with GitHub,
                # but this app happens to be rate limited now. We try again later.
                # Min wait time of 1 minute
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
