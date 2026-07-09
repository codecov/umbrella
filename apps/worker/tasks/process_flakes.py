import logging
from typing import Any

import sentry_sdk
from sqlalchemy.orm import Session

from app import celery_app
from services.test_analytics.ta_process_flakes import process_flakes_for_repo
from shared.celery_config import process_flakes_task_name
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class ProcessFlakesTask(BaseCodecovTask, name=process_flakes_task_name):
    """
    This task is currently called in the test results finisher task and in the sync pulls task
    """

    def run_impl(
        self,
        _db_session: Session,
        *,
        repo_id: int,
        **kwargs: Any,
    ):
        log.info("Received process flakes task for repo %s", repo_id)

        try:
            success, has_more = process_flakes_for_repo(repo_id)
            if has_more:
                log.info(
                    "Commit backlog not fully drained for repo %s, re-queuing task",
                    repo_id,
                )
                process_flakes_task.apply_async(kwargs={"repo_id": repo_id})
            return {"successful": success}
        except Exception as e:
            log.error("Error processing flakes for repo %s: %s", repo_id, str(e))
            sentry_sdk.capture_exception(e)
            return {"successful": False}


RegisteredProcessFlakesTask = celery_app.register_task(ProcessFlakesTask())
process_flakes_task = celery_app.tasks[RegisteredProcessFlakesTask.name]
