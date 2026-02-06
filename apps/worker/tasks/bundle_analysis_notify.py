import logging
from typing import Any

import sentry_sdk

from app import celery_app
from database.enums import ReportType
from database.models import Commit
from helpers.github_installation import get_installation_name_for_owner_for_task
from services.bundle_analysis.notify import BundleAnalysisNotifyService
from services.bundle_analysis.notify.types import NotificationSuccess
from services.lock_manager import LockManager, LockRetry, LockType
from shared.celery_config import (
    BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES,
    bundle_analysis_notify_task_name,
)
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class BundleAnalysisNotifyTask(BaseCodecovTask, name=bundle_analysis_notify_task_name):
    max_retries = BUNDLE_ANALYSIS_NOTIFY_MAX_RETRIES

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

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            report_type=ReportType.BUNDLE_ANALYSIS,
        )

        try:
            with lock_manager.locked(
                LockType.BUNDLE_ANALYSIS_NOTIFY,
                max_retries=self.max_retries,
                retry_num=self.attempts,
            ):
                return self.process_impl_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=commit_yaml,
                    previous_result=previous_result,
                    **kwargs,
                )
        except LockRetry as retry:
            if retry.max_retries_exceeded:
                log.error(
                    "Not retrying lock acquisition - max retries exceeded",
                    extra={
                        "commitid": commitid,
                        "repoid": repoid,
                        "retry_num": retry.retry_num,
                        "max_retries": retry.max_retries,
                    },
                )
                return {
                    "notify_attempted": False,
                    "notify_succeeded": None,
                }
            self.retry(max_retries=self.max_retries, countdown=retry.countdown)

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
