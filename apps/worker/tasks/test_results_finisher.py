import logging
from typing import Any

from sqlalchemy.orm import Session

from app import celery_app
from database.enums import ReportType
from database.models import Commit
from helpers.checkpoint_logger.flows import TestResultsFlow
from services.lock_manager import LockManager, LockRetry, LockType
from services.test_analytics.ta_finish_upload import ta_finish_upload
from shared.celery_config import test_results_finisher_task_name
from shared.django_apps.upload_breadcrumbs.models import Errors, Milestones
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask
from tasks.notify import notify_task_name

log = logging.getLogger(__name__)


class TestResultsFinisherTask(BaseCodecovTask, name=test_results_finisher_task_name):
    def run_impl(
        self,
        db_session: Session,
        _chain_result: bool,
        *,
        repoid: int,
        commitid: str,
        commit_yaml: dict,
        **kwargs,
    ):
        repoid = int(repoid)

        self.extra_dict: dict[str, Any] = {
            "commit_yaml": commit_yaml,
        }
        log.info("Starting test results finisher task", extra=self.extra_dict)

        bc_kwargs = {
            "commit_sha": commitid,
            "repo_id": repoid,
            "task_name": self.name,
            "parent_task_id": self.request.parent_id,
        }

        self._call_upload_breadcrumb_task(
            milestone=Milestones.NOTIFICATIONS_TRIGGERED, **bc_kwargs
        )

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            report_type=ReportType.COVERAGE,
            lock_timeout=max(80, self.hard_time_limit_task),
            blocking_timeout=None,
        )

        self._call_upload_breadcrumb_task(
            milestone=Milestones.LOCK_ACQUIRING, **bc_kwargs
        )

        try:
            # this needs to be the coverage notification lock
            # since both tests post/edit the same comment
            with lock_manager.locked(
                LockType.NOTIFICATION,
                max_retries=5,
                retry_num=self.attempts,
            ):
                self._call_upload_breadcrumb_task(
                    milestone=Milestones.LOCK_ACQUIRED, **bc_kwargs
                )
                finisher_result = self.process_impl_within_lock(
                    db_session=db_session,
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=UserYaml.from_dict(commit_yaml),
                    **kwargs,
                )
            self._call_upload_breadcrumb_task(
                milestone=Milestones.LOCK_RELEASED, **bc_kwargs
            )

            if finisher_result["queue_notify"]:
                self.app.tasks[notify_task_name].apply_async(
                    args=None,
                    kwargs={
                        "repoid": repoid,
                        "commitid": commitid,
                        "current_yaml": commit_yaml,
                    },
                )

            self._call_upload_breadcrumb_task(
                milestone=Milestones.NOTIFICATIONS_SENT, **bc_kwargs
            )

            return finisher_result

        except LockRetry as retry:
            self._call_upload_breadcrumb_task(
                error=Errors.INTERNAL_LOCK_ERROR, **bc_kwargs
            )
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
                return {"queue_notify": False}
            self.retry(max_retries=5, countdown=retry.countdown)

    def process_impl_within_lock(
        self,
        *,
        db_session: Session,
        repoid: int,
        commitid: str,
        commit_yaml: UserYaml,
        **kwargs,
    ):
        log.info("Running test results finishers", extra=self.extra_dict)
        TestResultsFlow.log(TestResultsFlow.TEST_RESULTS_FINISHER_BEGIN)

        commit: Commit = (
            db_session.query(Commit).filter_by(repoid=repoid, commitid=commitid).first()
        )
        assert commit, "commit not found"
        repo = commit.repository

        return ta_finish_upload(db_session, repo, commit, commit_yaml)


RegisteredTestResultsFinisherTask = celery_app.register_task(TestResultsFinisherTask())
test_results_finisher_task = celery_app.tasks[RegisteredTestResultsFinisherTask.name]
