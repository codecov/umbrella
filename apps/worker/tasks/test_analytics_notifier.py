import logging
from contextlib import contextmanager
from typing import cast

from asgiref.sync import async_to_sync
from celery.exceptions import MaxRetriesExceededError as CeleryMaxRetriesExceededError
from django.conf import settings
from redis import Redis
from sqlalchemy.orm import Session

from app import celery_app
from helpers.notifier import NotifierResult
from helpers.string import shorten_file_paths
from services.lock_manager import LockManager, LockRetry, LockType
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
from shared.celery_config import test_analytics_notifier_task_name
from shared.django_apps.core.models import Repository
from shared.django_apps.reports.models import ReportType
from shared.helpers.redis import get_redis_connection
from shared.reports.types import UploadType
from shared.typings.torngit import AdditionalData
from shared.upload.types import TAUploadContext, UploadPipeline
from shared.yaml.user_yaml import UserYaml
from tasks.base import BaseCodecovTask


class MaxRetriesExceededError(Exception):
    """Raised when lock acquisition exceeds max retries."""

    pass


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
                with self._notification_lock(lock_manager):
                    # Acquire a fencing token to deduplicate notifications
                    redis_key = TA_NOTIFIER_FENCING_TOKEN.format(
                        repo_id, upload_context["commit_sha"]
                    )
                    with redis_client.pipeline() as pipeline:
                        pipeline.incr(redis_key)
                        # Set expiry to prevent stale keys from accumulating
                        pipeline.expire(redis_key, 24 * 60 * 60)  # 24 hours
                        results = pipeline.execute()
                        fencing_token = int(cast(str, results[0]))

                # Add fencing token to the args and then retry for a debounce period
                self.extra_dict["fencing_token"] = fencing_token
                kwargs["fencing_token"] = fencing_token
                log.info(
                    "Acquired fencing token, retrying for debounce period",
                    extra=self.extra_dict,
                )
                self.retry(countdown=DEBOUNCE_PERIOD_SECONDS, kwargs=kwargs)
            except MaxRetriesExceededError:
                return NotifierTaskResult(
                    attempted=False,
                    succeeded=False,
                )
            except LockRetry as e:
                try:
                    self.retry(max_retries=5, countdown=e.countdown)
                except CeleryMaxRetriesExceededError:
                    return NotifierTaskResult(
                        attempted=False,
                        succeeded=False,
                    )

        # At this point we have a fencing token, but want to check if another
        # notification task has incremented it, indicating a newer notification
        # task is in progress and can take over
        assert fencing_token, "Fencing token not acquired"
        # The lock here is not required for correctness (only the one at the
        # end is), but it reduces duplicated work and database queries
        try:
            with self._notification_lock(lock_manager):
                stale_token_result = self._check_fencing_token(
                    redis_client, repo_id, upload_context, fencing_token
                )
                if stale_token_result:
                    return stale_token_result
        except MaxRetriesExceededError:
            return NotifierTaskResult(
                attempted=False,
                succeeded=False,
            )
        except LockRetry as e:
            try:
                self.retry(max_retries=5, countdown=e.countdown)
            except CeleryMaxRetriesExceededError:
                return NotifierTaskResult(
                    attempted=False,
                    succeeded=False,
                )

        # Do preparation work without a lock
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
            with self._notification_lock(lock_manager):
                # Check fencing token again before sending the notification
                stale_token_result = self._check_fencing_token(
                    redis_client, repo_id, upload_context, fencing_token
                )
                if stale_token_result:
                    return stale_token_result

                # Send notification
                notifier_result = notifier.notify()

            success = (
                True if notifier_result is NotifierResult.COMMENT_POSTED else False
            )
            log.info("Posted TA comment", extra={**self.extra_dict, "success": success})
            return NotifierTaskResult(
                attempted=True,
                succeeded=success,
            )
        except MaxRetriesExceededError:
            return NotifierTaskResult(
                attempted=False,
                succeeded=False,
            )
        except LockRetry as e:
            try:
                self.retry(max_retries=5, countdown=e.countdown)
            except CeleryMaxRetriesExceededError:
                return NotifierTaskResult(
                    attempted=False,
                    succeeded=False,
                )

    @contextmanager
    def _notification_lock(self, lock_manager: LockManager):
        """
        Context manager to handle the repeated lock acquisition pattern
        with automatic retry handling for LockRetry exceptions.
        """
        try:
            with lock_manager.locked(
                LockType.NOTIFICATION,
                max_retries=5,
                retry_num=self.attempts,
            ):
                yield
                return
        except LockRetry as e:
            # Lock acquisition failed - handle immediately without yielding
            # This ensures the with block body never executes without lock protection
            if e.max_retries_exceeded:
                log.error(
                    "Not retrying lock acquisition - max retries exceeded",
                    extra={
                        "retry_num": e.retry_num,
                        "max_attempts": e.max_attempts,
                    },
                )
                raise MaxRetriesExceededError(
                    f"Lock acquisition exceeded max retries: {e.retry_num} >= {e.max_attempts}",
                )
            # Re-raise LockRetry to be handled by the caller's retry logic
            # The caller will catch this and call self.retry()
            raise

    def _check_fencing_token(
        self,
        redis_client: Redis,
        repo_id: int,
        upload_context: TAUploadContext,
        fencing_token: int,
    ) -> NotifierTaskResult | None:
        """
        Check if the fencing token is stale, indicating another notification task
        is in progress and can take over.

        This method should be called within a lock context.

        :returns:
            NotifierTaskResult if the token is stale (task should exit early),
            None if the token is current (task should continue).
        """
        current_token = int(
            cast(
                str,
                redis_client.get(
                    TA_NOTIFIER_FENCING_TOKEN.format(
                        repo_id, upload_context["commit_sha"]
                    )
                )
                or "0",
            )
        )

        # We do a less than comparison since it guarantees safety at
        # the cost of no debouncing for that commit in the case of
        # losing the key in Redis
        if fencing_token < current_token:
            log.info(
                "Fencing token is stale, another notification task is in progress, exiting",
                extra=self.extra_dict,
            )
            return NotifierTaskResult(
                attempted=False,
                succeeded=False,
            )

        return None

    def notification_preparation(
        self,
        *,
        repo_id: int,
        upload_context: TAUploadContext,
        commit_yaml: UserYaml = UserYaml({}),  # TODO: Actual commit_yaml
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

        # TODO: Add upload errors in a compliant way

        # TODO: Remove impl label
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

        # TODO: Seat activation

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
    notif_failures = []
    for failure in failures:
        if failure["failure_message"] is not None:
            failure["failure_message"] = shorten_file_paths(
                failure["failure_message"]
            ).replace("\r", "")

        notif_failures.append(
            TestResultsNotificationFailure(
                display_name=failure["computed_name"],
                failure_message=failure["failure_message"],
                test_id=failure["test_id"],
                envs=failure["flags"],
                duration_seconds=failure["duration_seconds"] or 0,
                build_url=None,  # TODO: Figure out how we can save this in a compliant way
            )
        )
    return notif_failures


RegisteredTestAnalyticsNotifierTask = celery_app.register_task(
    TestAnalyticsNotifierTask()
)
test_analytics_notifier_task = celery_app.tasks[
    RegisteredTestAnalyticsNotifierTask.name
]
