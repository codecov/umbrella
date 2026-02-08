import logging

from celery.exceptions import MaxRetriesExceededError

from app import celery_app
from database.models import Commit, Pull
from database.models.reports import CommitReport, Upload
from services.comparison import get_or_create_comparison
from services.lock_manager import LockManager, LockRetry, LockType
from shared.celery_config import (
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    TASK_MAX_RETRIES_DEFAULT,
    compute_comparison_task_name,
    manual_upload_completion_trigger_task_name,
    notify_task_name,
    pulls_task_name,
)
from shared.django_apps.reports.models import ReportType
from shared.reports.enums import UploadState
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class ManualTriggerTask(
    BaseCodecovTask, name=manual_upload_completion_trigger_task_name
):
    def run_impl(
        self,
        db_session,
        *,
        repoid: int,
        commitid: str,
        current_yaml=None,
        **kwargs,
    ):
        log.info(
            "Received manual trigger task",
            extra={"repoid": repoid, "commit": commitid},
        )
        repoid = int(repoid)
        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            lock_timeout=self.get_lock_timeout(DEFAULT_LOCK_TIMEOUT_SECONDS),
        )
        try:
            with lock_manager.locked(
                LockType.MANUAL_TRIGGER,
                max_retries=TASK_MAX_RETRIES_DEFAULT,
                retry_num=self.attempts,
            ):
                return self.process_impl_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=current_yaml,
                    **kwargs,
                )
        except LockRetry as retry:
            if self._has_exceeded_max_attempts(TASK_MAX_RETRIES_DEFAULT):
                return {
                    "notifications_called": False,
                    "message": "Unable to acquire lock",
                }
            self.retry(max_retries=TASK_MAX_RETRIES_DEFAULT, countdown=retry.countdown)

    def process_impl_within_lock(
        self,
        *,
        db_session,
        repoid,
        commitid,
        commit_yaml,
        **kwargs,
    ):
        commit = (
            db_session.query(Commit)
            .filter(
                Commit.repoid == repoid,
                Commit.commitid == commitid,
            )
            .first()
        )
        uploads = (
            db_session.query(Upload)
            .join(CommitReport)
            .filter(
                CommitReport.code == None,  # noqa: E711
                CommitReport.commit == commit,
                (CommitReport.report_type == None)  # noqa: E711
                | (CommitReport.report_type == ReportType.COVERAGE.value),
            )
        )

        still_processing = 0
        for upload in uploads:
            if (
                not upload.state
                or upload.state == "started"
                or upload.state_id == UploadState.UPLOADED.db_id
            ):
                still_processing += 1

        if still_processing == 0:
            self.trigger_notifications(repoid, commitid, commit_yaml)
            if commit.pullid:
                self.trigger_pull_sync(db_session, repoid, commit)
            return {
                "notifications_called": True,
                "message": "All uploads are processed. Triggering notifications.",
            }
        else:
            # reschedule the task
            try:
                log.info(
                    "Retrying ManualTriggerTask. Some uploads are still being processed.",
                    extra={
                        "repoid": repoid,
                        "commitid": commitid,
                        "uploads_still_processing": still_processing,
                    },
                )
                retry_in = 60 * 3**self.request.retries
                self.retry(max_retries=5, countdown=retry_in)
            except MaxRetriesExceededError:
                log.warning(
                    "Not attempting to wait for all uploads to get processed since we already retried too many times",
                    extra={
                        "commit": commit.commitid,
                        "max_retries": 5,
                        "next_countdown_would_be": retry_in,
                        "repoid": commit.repoid,
                    },
                )
                return {
                    "notifications_called": False,
                    "message": "Uploads are still in process and the task got retired so many times. Not triggering notifications.",
                }

    def trigger_notifications(self, repoid, commitid, commit_yaml):
        log.info(
            "Scheduling notify task",
            extra={
                "repoid": repoid,
                "commit": commitid,
                "commit_yaml": commit_yaml.to_dict() if commit_yaml else None,
            },
        )
        self.app.tasks[notify_task_name].apply_async(
            kwargs={
                "repoid": repoid,
                "commitid": commitid,
                "current_yaml": commit_yaml.to_dict() if commit_yaml else None,
            }
        )

    def trigger_pull_sync(self, db_session, repoid, commit):
        pull = (
            db_session.query(Pull)
            .filter_by(repoid=commit.repoid, pullid=commit.pullid)
            .first()
        )

        if pull:
            head = pull.get_head_commit()
            if head is None or head.timestamp <= commit.timestamp:
                pull.head = commit.commitid
            if pull.head == commit.commitid:
                db_session.commit()
                log.info(
                    "Scheduling pulls syc task",
                    extra={
                        "repoid": repoid,
                        "pullid": pull.pullid,
                    },
                )
                self.app.tasks[pulls_task_name].apply_async(
                    kwargs={
                        "repoid": repoid,
                        "pullid": pull.pullid,
                        "should_send_notifications": False,
                    }
                )
                compared_to = pull.get_comparedto_commit()
                if compared_to:
                    comparison = get_or_create_comparison(
                        db_session, compared_to, commit
                    )
                    db_session.commit()
                    self.app.tasks[compute_comparison_task_name].apply_async(
                        kwargs={"comparison_id": comparison.id}
                    )


RegisteredManualTriggerTask = celery_app.register_task(ManualTriggerTask())
manual_trigger_task = celery_app.tasks[RegisteredManualTriggerTask.name]
