from __future__ import annotations

import logging

from app import celery_app
from services.processing.types import UploadArguments
from services.test_analytics.ta_processor import ta_processor
from shared.celery_config import test_results_processor_task_name
from shared.django_apps.upload_breadcrumbs.models import Errors, Milestones
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class TestResultsProcessorTask(BaseCodecovTask, name=test_results_processor_task_name):
    __test__ = False

    def run_impl(
        self,
        db_session,
        previous_result: bool,
        *args,
        repoid: int,
        commitid: str,
        commit_yaml,
        arguments_list: list[UploadArguments],
        **kwargs,
    ) -> bool:
        upload_ids = [arg["upload_id"] for arg in arguments_list if "upload_id" in arg]

        bc_kwargs = {
            "commit_sha": commitid,
            "repo_id": repoid,
            "upload_ids": upload_ids,
            "task_name": self.name,
            "parent_task_id": self.request.parent_id,
        }

        self._call_upload_breadcrumb_task(
            milestone=Milestones.PROCESSING_UPLOAD, **bc_kwargs
        )

        try:
            for argument in arguments_list:
                ta_processor(
                    repoid=repoid,
                    commitid=commitid,
                    commit_yaml=commit_yaml,
                    argument=argument,
                    update_state=True,
                )
        except Exception:
            self._call_upload_breadcrumb_task(
                error=Errors.UNKNOWN,
                error_text="Unhandled exception during test results processing",
                **bc_kwargs,
            )
            raise

        self._call_upload_breadcrumb_task(
            milestone=Milestones.UPLOAD_COMPLETE, **bc_kwargs
        )

        return True


RegisteredTestResultsProcessorTask = celery_app.register_task(
    TestResultsProcessorTask()
)
test_results_processor_task = celery_app.tasks[RegisteredTestResultsProcessorTask.name]
