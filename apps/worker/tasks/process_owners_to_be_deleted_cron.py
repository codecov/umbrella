import logging
from typing import Any

from django.conf import settings

from app import celery_app
from shared.celery_config import (
    delete_owner_task_name,
    process_owners_to_be_deleted_cron_task_name,
)
from shared.django_apps.codecov_auth.models import OwnerToBeDeleted
from tasks.crontasks import CodecovCronTask

log = logging.getLogger(__name__)


class ProcessOwnersToBeDeletedCronTask(
    CodecovCronTask, name=process_owners_to_be_deleted_cron_task_name
):
    """
    Cron task that scans the OwnerToBeDeleted table and starts DeleteOwnerTask instances
    for each owner ID that needs to be deleted.

    This task runs periodically to process owners that have been marked for deletion
    but haven't been processed yet. It limits the number of tasks started to prevent
    overwhelming the system.
    """

    @classmethod
    def get_min_seconds_interval_between_executions(cls) -> int:
        """
        This task should run every hour to process owners marked for deletion.
        """
        return 60 * 60  # 1 hour

    def run_cron_task(self, db_session, *args, **kwargs) -> dict[str, Any]:
        """
        Scan the OwnerToBeDeleted table and start DeleteOwnerTask instances.

        Returns:
            Dict containing the results of the operation
        """
        # Get the maximum number of owners to process in one run
        # This prevents starting too many tasks at once
        max_owners_per_run = getattr(
            settings,
            "MAX_OWNERS_TO_DELETE_PER_CRON_RUN",
            10,  # Default to 10 owners per run
        )

        log.info(
            f"Starting to process owners marked for deletion. Max per run: {max_owners_per_run}"
        )

        # Get owners that need to be deleted, limited by max_owners_per_run
        owners_to_delete = list(OwnerToBeDeleted.objects.all()[:max_owners_per_run])

        if not owners_to_delete:
            log.info("No owners found in OwnerToBeDeleted table")
            return {
                "owners_processed": 0,
                "tasks_started": 0,
                "message": "No owners to process",
            }

        log.info(f"Found {len(owners_to_delete)} owners to process")

        owners_processed = 0

        for owner_record in owners_to_delete:
            try:
                owner_id = owner_record.owner_id
                log.info(f"Starting DeleteOwnerTask for owner ID: {owner_id}")

                self.app.tasks[delete_owner_task_name].apply_async(
                    kwargs={"ownerid": owner_id}
                )
                owners_processed += 1

                log.info(
                    f"Successfully started DeleteOwnerTask for owner ID: {owner_id}"
                )

            except Exception as e:
                log.error(
                    f"Failed to start DeleteOwnerTask for owner ID {owner_record.owner_id}: {str(e)}",
                    exc_info=True,
                )
                # Don't remove the owner from the table if we failed to start the task
                # It will be retried in the next cron run

        log.info(
            f"Completed processing deletion of owners. "
            f"Owners processed: {owners_processed}"
        )

        return {
            "owners_processed": owners_processed,
            "message": f"Processed {owners_processed} owners",
        }


RegisteredProcessOwnersToBeDeletedCronTask = celery_app.register_task(
    ProcessOwnersToBeDeletedCronTask()
)
process_owners_to_be_deleted_cron_task = celery_app.tasks[
    ProcessOwnersToBeDeletedCronTask.name
]
