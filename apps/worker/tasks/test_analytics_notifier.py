import logging

from asgiref.sync import async_to_sync
from celery.exceptions import MaxRetriesExceededError as CeleryMaxRetriesExceededError
from django.conf import settings
from sqlalchemy.orm import Session

from app import celery_app
from database.enums import ReportType
from helpers.checkpoint_logger.flows import TestResultsFlow
from helpers.notifier import NotifierResult
from helpers.string import shorten_file_paths
from services.lock_manager import LockManager, LockRetry, LockType
from services.notification.debounce import (
    LockAcquisitionLimitError,
    NotificationDebouncer,
)
from services.repository import (
    fetch_pull_request_information,
    get_repo_provider_service,
)
from services.test_analytics.ta_metrics import (
    read_failures_summary,
    read_tests_totals_summary,
)
from services.test_analytics.ta_timeseries import (
    FailedTestInstance,
    get_flaky_tests_dict,
    get_pr_comment_agg,
    get_pr_comment_failures,
)
from services.test_results import (
    NotifierTaskResult,
    TACommentInDepthInfo,
    TestResultsNotificationFailure,
    TestResultsNotificationPayload,
    TestResultsNotifier,
    should_do_flaky_detection,
)
from shared.celery_config import (
    TASK_MAX_RETRIES_DEFAULT,
    test_analytics_notifier_task_name,
)
from shared.django_apps.core.models import Repository
from shared.helpers.redis import get_redis_connection
from shared.reports.types import UploadType
from shared.typings.torngit import AdditionalData
from shared.upload.types import TAUploadContext, UploadPipeline
from shared.yaml.user_yaml import UserYaml
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

TA_NOTIFIER_FENCING_TOKEN = "ta_notifier_fence:{}_{}"
DEBOUNCE_PERIOD_SECONDS = 30


class TestAnalyticsNotifierTask(
    BaseCodecovTask, name=test_analytics_notifier_task_name
):
    """
    Send test analytics notifications while ensuring compliance with Sentry's
    data retention policies.
    """

    max_retries = TASK_MAX_RETRIES_DEFAULT

    def __init__(self):
        super().__init__()
        self.debouncer = NotificationDebouncer[NotifierTaskResult](
            redis_key_template=TA_NOTIFIER_FENCING_TOKEN,
            debounce_period_seconds=DEBOUNCE_PERIOD_SECONDS,
            lock_type=LockType.NOTIFICATION,
            max_lock_retries=self.max_retries,
        )

    def run_impl(
        self,
        _db_session: Session,
        *,
        repoid: int,
        upload_context: TAUploadContext,
        fencing_token: int | None = None,
        **kwargs,
    ):
        """
        `fencing_token` is not expected to be passed in for the initial run, it
        will automatically get populated during a task re-run. See the logic
        below.
        """
        repo_id = int(repoid)

        self.extra_dict = {
            "repo_id": repo_id,
            "upload_context": upload_context,
            "fencing_token": fencing_token,
        }

        log.info("Starting test analytics notifier task", extra=self.extra_dict)

        assert upload_context.get("branch"), (
            "Branch information is required for notifier"
        )

        redis_client = get_redis_connection()

        lock_manager = LockManager(
            repoid=repo_id,
            commitid=upload_context["commit_sha"],
            report_type=ReportType.TEST_RESULTS,
            lock_timeout=max(80, self.hard_time_limit_task),
            blocking_timeout=None,
            redis_connection=redis_client,
        )

        if fencing_token is None:
            try:
                with self.debouncer.notification_lock(lock_manager, self.attempts):
                    fencing_token = self.debouncer.acquire_fencing_token(
                        redis_client, repo_id, upload_context["commit_sha"]
                    )

                self.extra_dict["fencing_token"] = fencing_token
                retry_kwargs = {
                    "repoid": repo_id,
                    "upload_context": upload_context,
                    "fencing_token": fencing_token,
                    **kwargs,
                }
                log.info(
                    "Acquired fencing token, retrying for debounce period",
                    extra=self.extra_dict,
                )
                try:
                    self.retry(countdown=DEBOUNCE_PERIOD_SECONDS, kwargs=retry_kwargs)
                except CeleryMaxRetriesExceededError:
                    log.warning(
                        "Max retries exceeded during debounce retry, proceeding immediately with notification",
                        extra=self.extra_dict,
                    )
                    # Continue execution to process notification with acquired token
            except LockAcquisitionLimitError:
                if TestResultsFlow.has_begun():
                    TestResultsFlow.log(TestResultsFlow.NOTIF_LOCK_ERROR)
                return NotifierTaskResult(
                    attempted=False,
                    succeeded=False,
                )
            except LockRetry as e:
                result = self.debouncer.handle_lock_retry(
                    self,
                    e,
                    NotifierTaskResult(attempted=False, succeeded=False),
                )
                if result is not None:
                    return result

        assert fencing_token, "Fencing token not acquired"
        try:
            with self.debouncer.notification_lock(lock_manager, self.attempts):
                if self.debouncer.check_fencing_token_stale(
                    redis_client, repo_id, upload_context["commit_sha"], fencing_token
                ):
                    log.info(
                        "Fencing token is stale, another notification task is in progress, exiting",
                        extra=self.extra_dict,
                    )
                    return NotifierTaskResult(
                        attempted=False,
                        succeeded=False,
                    )
        except LockAcquisitionLimitError:
            if TestResultsFlow.has_begun():
                TestResultsFlow.log(TestResultsFlow.NOTIF_LOCK_ERROR)
            return NotifierTaskResult(
                attempted=False,
                succeeded=False,
            )
        except LockRetry as e:
            result = self.debouncer.handle_lock_retry(
                self,
                e,
                NotifierTaskResult(attempted=False, succeeded=False),
            )
            if result is not None:
                return result

        notifier = self.notification_preparation(
            repo_id=repo_id,
            upload_context=upload_context,
            **kwargs,
        )

        if not notifier:
            return NotifierTaskResult(
                attempted=False,
                succeeded=False,
            )

        try:
            with self.debouncer.notification_lock(lock_manager, self.attempts):
                if self.debouncer.check_fencing_token_stale(
                    redis_client, repo_id, upload_context["commit_sha"], fencing_token
                ):
                    log.info(
                        "Fencing token is stale, another notification task is in progress, exiting",
                        extra=self.extra_dict,
                    )
                    return NotifierTaskResult(
                        attempted=False,
                        succeeded=False,
                    )

                notifier_result = notifier.notify()

            success = notifier_result is NotifierResult.COMMENT_POSTED
            log.info("Posted TA comment", extra={**self.extra_dict, "success": success})
            return NotifierTaskResult(
                attempted=True,
                succeeded=success,
            )
        except LockAcquisitionLimitError:
            if TestResultsFlow.has_begun():
                TestResultsFlow.log(TestResultsFlow.NOTIF_LOCK_ERROR)
            return NotifierTaskResult(
                attempted=False,
                succeeded=False,
            )
        except LockRetry as e:
            result = self.debouncer.handle_lock_retry(
                self,
                e,
                NotifierTaskResult(attempted=False, succeeded=False),
            )
            if result is not None:
                return result

    def notification_preparation(
        self,
        *,
        repo_id: int,
        upload_context: TAUploadContext,
        commit_yaml: UserYaml = UserYaml({}),
    ) -> TestResultsNotifier | None:
        """
        Prepare the test results notifier.

        :returns: The prepared test results notifier or None if either
            preparation failed or the necessary conditions were not met.
        """
        log.info("Running test analytics notifier preparation", extra=self.extra_dict)

        repo = Repository.objects.get(repoid=repo_id)
        assert repo, "Repository not found"

        if not commit_yaml.read_yaml_field("comment", _else=True):
            log.info("Comment is disabled, not posting comment", extra=self.extra_dict)
            return None

        with read_tests_totals_summary.labels(impl="new").time():
            summary = get_pr_comment_agg(repo_id, upload_context["commit_sha"])

        if summary["failed"] == 0:
            log.info(
                "No failures so not posting comment",
                extra=self.extra_dict,
            )
            return None

        additional_data: AdditionalData = {"upload_type": UploadType.TEST_RESULTS}

        match upload_context["pipeline"]:
            case UploadPipeline.CODECOV:
                repo_service = get_repo_provider_service(
                    repo, additional_data=additional_data
                )
            case UploadPipeline.SENTRY:
                repo_service = get_repo_provider_service(
                    repo,
                    installation_name_to_use=settings.GITHUB_SENTRY_APP_NAME,
                    additional_data=additional_data,
                )
        pull = async_to_sync(fetch_pull_request_information)(
            repo_service,
            repo_id,
            upload_context["commit_sha"],
            upload_context["branch"],
            upload_context["pull_id"],
        )

        if not pull:
            log.info("No pull so not posting comment", extra=self.extra_dict)
            return None

        notifier = TestResultsNotifier(
            repo,
            upload_context,
            commit_yaml,
            _pull=pull,
            _repo_service=repo_service,
        )

        with read_failures_summary.labels(impl="new").time():
            failures = get_pr_comment_failures(repo_id, upload_context["commit_sha"])

        notif_failures = transform_failures(failures)

        flaky_tests = {}

        if should_do_flaky_detection(repo, commit_yaml):
            flaky_tests = get_flaky_tests_dict(repo_id)

        payload = TestResultsNotificationPayload(
            failed=summary["failed"],
            passed=summary["passed"],
            skipped=summary["skipped"],
            info=TACommentInDepthInfo(notif_failures, flaky_tests),
        )

        notifier.payload = payload

        return notifier


def transform_failures(
    failures: list[FailedTestInstance],
) -> list[TestResultsNotificationFailure[bytes]]:
    result = []
    for failure in failures:
        failure_message = failure["failure_message"]
        if failure_message is not None:
            failure_message = shorten_file_paths(failure_message).replace("\r", "")

        result.append(
            TestResultsNotificationFailure(
                display_name=failure["computed_name"],
                failure_message=failure_message,
                test_id=failure["test_id"],
                envs=failure["flags"],
                duration_seconds=failure["duration_seconds"] or 0,
                build_url=None,
            )
        )
    return result


RegisteredTestAnalyticsNotifierTask = celery_app.register_task(
    TestAnalyticsNotifierTask()
)
test_analytics_notifier_task = celery_app.tasks[
    RegisteredTestAnalyticsNotifierTask.name
]
