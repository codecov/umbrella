import logging
from typing import Any

import sentry_sdk
from django.db import InterfaceError, OperationalError
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
            process_flakes_for_repo(repo_id)
            return {"successful": True}
        except (OperationalError, InterfaceError) as e:
            # Transient database connectivity problems (e.g. pgbouncer briefly not
            # resolvable). Unprocessed commits have been requeued, so retry later.
            log.warning(
                "Database unavailable while processing flakes for repo %s, retrying: %s",
                repo_id,
                str(e),
            )
            self.safe_retry(exc=e)
            # safe_retry only returns when max retries were exceeded (and reports it)
            log.error("Error processing flakes for repo %s: %s", repo_id, str(e))
            sentry_sdk.capture_exception(e)
            return {"successful": False}
        except Exception as e:
            log.error("Error processing flakes for repo %s: %s", repo_id, str(e))
            sentry_sdk.capture_exception(e)
            return {"successful": False}


RegisteredProcessFlakesTask = celery_app.register_task(ProcessFlakesTask())
process_flakes_task = celery_app.tasks[RegisteredProcessFlakesTask.name]
