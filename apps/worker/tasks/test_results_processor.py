from __future__ import annotations

import logging

from app import celery_app
from services.processing.types import UploadArguments
from services.test_analytics.ta_processor import ta_processor
from shared.celery_config import test_results_processor_task_name
from shared.django_apps.upload_breadcrumbs.models import Errors, Milestones, ReportTypes
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
        upload_ids = [
            arg.get("upload_id") for arg in arguments_list if arg.get("upload_id")
        ]

        # Log breadcrumb for processing start
        self._call_upload_breadcrumb_task(
            commit_sha=commitid,
            repo_id=repoid,
            milestone=Milestones.PROCESSING_UPLOAD,
            upload_ids=upload_ids,
            report_type=ReportTypes.TEST_RESULTS,
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
            return True
        except Exception as e:
            log.exception(
                "Error processing test results",
                extra={"repoid": repoid, "commitid": commitid},
            )
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.PROCESSING_UPLOAD,
                upload_ids=upload_ids,
                error=Errors.UNKNOWN,
                error_text=repr(e),
                report_type=ReportTypes.TEST_RESULTS,
            )
            raise


RegisteredTestResultsProcessorTask = celery_app.register_task(
    TestResultsProcessorTask()
)
test_results_processor_task = celery_app.tasks[RegisteredTestResultsProcessorTask.name]
