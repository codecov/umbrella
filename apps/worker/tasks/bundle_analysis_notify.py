import logging
from typing import Any

import sentry_sdk
from celery.exceptions import MaxRetriesExceededError as CeleryMaxRetriesExceededError
from celery.exceptions import Retry

from app import celery_app
from database.enums import ReportType
from database.models import Commit
from helpers.github_installation import get_installation_name_for_owner_for_task
from services.bundle_analysis.notify import BundleAnalysisNotifyService
from services.bundle_analysis.notify.types import NotificationSuccess
from services.lock_manager import LockManager, LockRetry, LockType
from services.notification.debounce import (
    LockAcquisitionLimitError,
    NotificationDebouncer,
)
from shared.celery_config import (
    BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES,
    bundle_analysis_notify_task_name,
)
from shared.helpers.redis import get_redis_connection
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

BUNDLE_ANALYSIS_NOTIFIER_FENCING_TOKEN = "bundle_analysis_notifier_fence:{}_{}"
DEBOUNCE_PERIOD_SECONDS = 30


class BundleAnalysisNotifyTask(BaseCodecovTask, name=bundle_analysis_notify_task_name):
    max_retries = BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES

    def __init__(self):
        super().__init__()
        self.debouncer = NotificationDebouncer[dict](
            redis_key_template=BUNDLE_ANALYSIS_NOTIFIER_FENCING_TOKEN,
            debounce_period_seconds=DEBOUNCE_PERIOD_SECONDS,
            lock_type=LockType.BUNDLE_ANALYSIS_NOTIFY,
            max_lock_retries=self.max_retries,
        )

    def run_impl(
        self,
        db_session,
        # Celery `chain` injects this argument - it's the list of processing results
        # from prior processor tasks in the chain
        previous_result: list[dict[str, Any]],
        *,
        repoid: int,
        commitid: str,
        commit_yaml: dict,
        fencing_token: int | None = None,
        **kwargs,
    ):
        repoid = int(repoid)
        commit_yaml = UserYaml.from_dict(commit_yaml)

        log.info(
            "Starting bundle analysis notify",
            extra={
                "repoid": repoid,
                "commit": commitid,
                "commit_yaml": commit_yaml,
            },
        )

        redis_connection = get_redis_connection()
        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            report_type=ReportType.BUNDLE_ANALYSIS,
            redis_connection=redis_connection,
        )

        if fencing_token is None:
            try:
                with self.debouncer.notification_lock(lock_manager, self.attempts):
                    fencing_token = self.debouncer.acquire_fencing_token(
                        redis_connection, repoid, commitid
                    )

                log.info(
                    "Acquired fencing token, retrying for debounce period",
                    extra={
                        "repoid": repoid,
                        "commit": commitid,
                        "fencing_token": fencing_token,
                    },
                )
                retry_kwargs = {
                    "previous_result": previous_result,
                    "repoid": repoid,
                    "commitid": commitid,
                    "commit_yaml": commit_yaml.to_dict(),
                    "fencing_token": fencing_token,
                    **kwargs,
                }
                try:
                    self.retry(countdown=DEBOUNCE_PERIOD_SECONDS, kwargs=retry_kwargs)
                except CeleryMaxRetriesExceededError:
                    # Max retries exceeded during debounce retry, but we have a valid token.
                    # Proceed immediately with notification instead of dropping it.
                    log.warning(
                        "Max retries exceeded during debounce retry, proceeding immediately with notification",
                        extra={
                            "repoid": repoid,
                            "commit": commitid,
                            "fencing_token": fencing_token,
                        },
                    )
                    # Continue execution to process notification with acquired token
            except LockAcquisitionLimitError:
                return {
                    "notify_attempted": False,
                    "notify_succeeded": None,
                }
            except LockRetry as e:
                try:
                    result = self.debouncer.handle_lock_retry(
                        self,
                        e,
                        {
                            "notify_attempted": False,
                            "notify_succeeded": None,
                        },
                    )
                    if result is not None:
                        return result
                except Retry:
                    # Re-raise Retry exception to ensure task retry is scheduled
                    # and execution doesn't continue past this point
                    raise

        assert fencing_token, "Fencing token not acquired"
        try:
            with self.debouncer.notification_lock(lock_manager, self.attempts):
                if self.debouncer.check_fencing_token_stale(
                    redis_connection, repoid, commitid, fencing_token
                ):
                    log.info(
                        "Fencing token is stale, another notification task is in progress, exiting",
                        extra={"repoid": repoid, "commit": commitid},
                    )
                    return {
                        "notify_attempted": False,
                        "notify_succeeded": None,
                    }

                return self.process_impl_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=commit_yaml,
                    previous_result=previous_result,
                    **kwargs,
                )
        except LockAcquisitionLimitError:
            return {
                "notify_attempted": False,
                "notify_succeeded": None,
            }
        except LockRetry as retry:
            try:
                result = self.debouncer.handle_lock_retry(
                    self,
                    retry,
                    {
                        "notify_attempted": False,
                        "notify_succeeded": None,
                    },
                )
                if result is not None:
                    return result
            except Retry:
                # Re-raise Retry exception to ensure task retry is scheduled
                # and execution doesn't continue past this point
                raise

    @sentry_sdk.trace
    def process_impl_within_lock(
        self,
        *,
        db_session,
        repoid: int,
        commitid: str,
        commit_yaml: UserYaml,
        previous_result: list[dict[str, Any]],
        **kwargs,
    ):
        log.info(
            "Running bundle analysis notify",
            extra={
                "repoid": repoid,
                "commit": commitid,
                "commit_yaml": commit_yaml,
                "parent_task": self.request.parent_id,
            },
        )

        commit = (
            db_session.query(Commit).filter_by(repoid=repoid, commitid=commitid).first()
        )
        assert commit, "commit not found"

        # previous_result is the list of processing results from prior processor tasks
        # (they get accumulated as we execute each task in succession)
        processing_results = (
            previous_result if isinstance(previous_result, list) else []
        )

        if all(result["error"] is not None for result in processing_results):
            # every processor errored, nothing to notify on
            return {
                "notify_attempted": False,
                "notify_succeeded": NotificationSuccess.ALL_ERRORED,
            }

        installation_name_to_use = get_installation_name_for_owner_for_task(
            self.name, commit.repository.author
        )
        notifier = BundleAnalysisNotifyService(
            commit, commit_yaml, gh_app_installation_name=installation_name_to_use
        )
        result = notifier.notify()

        log.info(
            "Finished bundle analysis notify",
            extra={
                "repoid": repoid,
                "commit": commitid,
                "commit_yaml": commit_yaml,
                "parent_task": self.request.parent_id,
                "result": result,
            },
        )

        return {
            "notify_attempted": True,
            "notify_succeeded": result.to_NotificationSuccess(),
        }


RegisteredBundleAnalysisNotifyTask = celery_app.register_task(
    BundleAnalysisNotifyTask()
)
bundle_analysis_notify_task = celery_app.tasks[RegisteredBundleAnalysisNotifyTask.name]
