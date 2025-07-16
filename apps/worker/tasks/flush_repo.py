import logging

from celery.exceptions import SoftTimeLimitExceeded

from app import celery_app
from services.cleanup.repository import cleanup_repo
from services.cleanup.utils import CleanupSummary
from shared.celery_config import flush_repo_task_name
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class FlushRepoTask(BaseCodecovTask, name=flush_repo_task_name):
    acks_late = True  # retry the task when the worker dies for whatever reason
    max_retries = None  # aka, no limit on retries

    def run_impl(self, _db_session, repoid: int) -> CleanupSummary:
        try:
            return cleanup_repo(repoid)
        except SoftTimeLimitExceeded:
            raise self.retry()


FlushRepo = celery_app.register_task(FlushRepoTask())
flush_repo = celery_app.tasks[FlushRepo.name]
