from __future__ import annotations

import logging

from app import celery_app
from services.processing.types import UploadArguments
from services.test_analytics.ta_processor import ta_processor
from shared.celery_config import test_results_processor_task_name
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
        for argument in arguments_list:
            ta_processor(
                repoid=repoid,
                commitid=commitid,
                commit_yaml=commit_yaml,
                argument=argument,
            )
        return True


RegisteredTestResultsProcessorTask = celery_app.register_task(
    TestResultsProcessorTask()
)
test_results_processor_task = celery_app.tasks[RegisteredTestResultsProcessorTask.name]
