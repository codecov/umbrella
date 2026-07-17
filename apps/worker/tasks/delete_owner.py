import logging

from app import celery_app
from services.cleanup.owner import cleanup_owner
from services.cleanup.utils import CleanupSummary
from shared.celery_config import delete_owner_task_name
from shared.django_apps.core.models import Repository
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class DeleteOwnerTask(BaseCodecovTask, name=delete_owner_task_name):
    acks_late = True  # retry the task when the worker dies for whatever reason
    max_retries = None  # aka, no limit on retries

    def run_impl(
        self,
        _db_session,
        ownerid: int,
        refs_cleared: bool = False,
    ) -> CleanupSummary:
        # Purposefully not catching errors since this task runs on a cron.
        #
        # To avoid exceeding the Celery hard-timeout for owners with many
        # repositories, `cleanup_owner` processes one repository per call and
        # returns early.  When there are still repositories left we re-enqueue
        # this task so the next repo is handled in a fresh invocation.
        summary = cleanup_owner(ownerid, refs_cleared=refs_cleared)

        remaining = Repository.objects.filter(author_id=ownerid).exists()
        if remaining:
            log.info(
                "Owner still has repositories after cleanup step; re-enqueueing",
                extra={"ownerid": ownerid},
            )
            self.apply_async(
                kwargs={"ownerid": ownerid, "refs_cleared": True},
            )

        return summary


RegisteredDeleteOwnerTask = celery_app.register_task(DeleteOwnerTask())
delete_owner_task = celery_app.tasks[DeleteOwnerTask.name]
