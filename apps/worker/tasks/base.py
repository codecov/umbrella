import logging
from datetime import datetime

import orjson
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
    DLQ_KEY_PREFIX,
    DLQ_TTL_SECONDS,
    TASK_RETRY_BACKOFF_BASE_SECONDS,
    upload_breadcrumb_task_name,
)
from shared.celery_router import route_tasks_based_on_user_plan
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Errors,
    Milestones,
)
from shared.helpers.redis import get_redis_connection
from shared.metrics import Counter, Histogram
from shared.torngit.base import TorngitBaseAdapter
from shared.torngit.exceptions import TorngitClientError, TorngitRepoNotFoundError
from shared.typings.torngit import AdditionalData
from shared.utils.sentry import current_sentry_trace_id
from shared.yaml import UserYaml

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

# Dead Letter Queue (DLQ) metric
TASK_DLQ_SAVED_COUNTER = Counter(
    "worker_task_counts_dlq_saved",
    "Number of tasks saved to Dead Letter Queue after exhausting retries",
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

    # Called when attempting to retry the task on db error
    def _retry(self, countdown=None):
        if not countdown:
            countdown = self.default_retry_delay

        try:
            self.retry(countdown=countdown)
        except MaxRetriesExceededError:
            if UploadFlow.has_begun():
                UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
            if TestResultsFlow.has_begun():
                TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)

    def safe_retry(self, max_retries=None, countdown=None, exc=None, **kwargs):
        """
        Safely retry with max retry limit and proper metrics tracking.

        This method provides a common retry pattern for all tasks with:
        - Configurable max retry limit
        - Automatic metric tracking
        - Consistent error handling

        Args:
            max_retries: Maximum number of retries allowed (default: task's max_retries)
            countdown: Seconds to wait before retry (default: exponential backoff)
            exc: Exception to include in retry (optional)
            **kwargs: Additional kwargs to pass to self.retry()

        Returns:
            True if retry was scheduled
            False if max retries exceeded

        Example:
            if some_condition_requires_retry:
                if not self.safe_retry(max_retries=5, countdown=60):
                    # Max retries exceeded
                    log.error("Giving up after too many retries")
                    return {"success": False, "reason": "max_retries"}
        """
        current_retries = self.request.retries if hasattr(self, "request") else 0
        task_max_retries = max_retries if max_retries is not None else self.max_retries

        if task_max_retries is not None and current_retries >= task_max_retries:
            log.error(
                f"Task {self.name} exceeded max retries",
                extra={
                    "task_name": self.name,
                    "current_retries": current_retries,
                    "max_retries": task_max_retries,
                },
            )
            TASK_MAX_RETRIES_EXCEEDED_COUNTER.labels(task=self.name).inc()
            return False

        # Default countdown: exponential backoff
        # Uses TASK_RETRY_BACKOFF_BASE_SECONDS from shared config (default: 20s)
        if countdown is None:
            countdown = TASK_RETRY_BACKOFF_BASE_SECONDS * (2**current_retries)

        try:
            self.retry(
                max_retries=task_max_retries, countdown=countdown, exc=exc, **kwargs
            )
            return True
        except MaxRetriesExceededError:
            TASK_MAX_RETRIES_EXCEEDED_COUNTER.labels(task=self.name).inc()
            if UploadFlow.has_begun():
                UploadFlow.log(UploadFlow.UNCAUGHT_RETRY_EXCEPTION)
            if TestResultsFlow.has_begun():
                TestResultsFlow.log(TestResultsFlow.UNCAUGHT_RETRY_EXCEPTION)
            return False

    def _save_to_task_dlq(
        self, task_data: dict, dlq_key_suffix: str | None = None
    ) -> str | None:
        """
        Save task data to Dead Letter Queue (DLQ) for manual inspection/recovery.

        This method is called when a task exhausts all retries to prevent data loss.
        The task data is stored in Redis with a configurable TTL (default: 7 days).

        Args:
            task_data: Dictionary containing task information (args, kwargs, etc.)
            dlq_key_suffix: Optional suffix to append to DLQ key for task-specific organization.
                           If None, uses task name and timestamp.

        Returns:
            DLQ key if successful, None if failed
        """
        try:
            redis_conn = get_redis_connection()

            # Generate DLQ key: task_dlq/{task_name}/{suffix or timestamp}
            if dlq_key_suffix:
                dlq_key = f"{DLQ_KEY_PREFIX}/{self.name}/{dlq_key_suffix}"
            else:
                timestamp = datetime.now().isoformat()
                dlq_key = f"{DLQ_KEY_PREFIX}/{self.name}/{timestamp}"

            # Serialize task data as JSON
            # Ensure all UserYaml objects are converted to dicts recursively
            def convert_user_yaml(obj):
                """Recursively convert UserYaml objects to dicts."""
                if isinstance(obj, UserYaml):
                    return obj.to_dict()
                elif isinstance(obj, dict):
                    return {k: convert_user_yaml(v) for k, v in obj.items()}
                elif isinstance(obj, list | tuple):
                    return [convert_user_yaml(item) for item in obj]
                else:
                    return obj

            # Convert any UserYaml objects in task_data before serialization
            task_data = convert_user_yaml(task_data)
            serialized_data = orjson.dumps(task_data).decode("utf-8")

            # Save to Redis list (allows multiple entries per key)
            redis_conn.rpush(dlq_key, serialized_data)
            redis_conn.expire(dlq_key, DLQ_TTL_SECONDS)

            # Track metric
            TASK_DLQ_SAVED_COUNTER.labels(task=self.name).inc()

            log.error(
                f"Task {self.name} saved to DLQ after exhausting retries",
                extra={
                    "task_name": self.name,
                    "dlq_key": dlq_key,
                    "retries": self.request.retries if hasattr(self, "request") else 0,
                },
            )

            return dlq_key
        except Exception as e:
            log.exception(
                f"Failed to save task {self.name} to DLQ",
                extra={"task_name": self.name, "error": str(e)},
            )
            return None

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
                self._retry()
            except SQLAlchemyError as ex:
                self._analyse_error(ex, args, kwargs)
                db_session.rollback()
                self._retry()
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
                self._retry(countdown=retry_delay_seconds)
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
