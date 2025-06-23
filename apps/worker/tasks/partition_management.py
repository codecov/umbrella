import logging
import sys
from io import StringIO

from django.core.management import call_command

from app import celery_app
from shared.celery_config import partition_management_task_name
from tasks.crontasks import CodecovCronTask

log = logging.getLogger(__name__)


class PartitionManagementTask(CodecovCronTask, name=partition_management_task_name):
    """
    Task to manage database partitions by running the pgpartition command.
    This handles creating future partitions and deleting old partitions
    based on the configuration in partitioning.py.
    """

    @classmethod
    def get_min_seconds_interval_between_executions(cls):
        return 18000  # 5h

    def run_cron_task(self, db_session, *args, **kwargs):
        log.info("Starting partition management task")
        # Using the stdout and stderr arguments to call_command
        # does not work for Celery tasks, so we redirect sys.stdout
        prev, sys.stdout = sys.stdout, StringIO()
        try:
            call_command("pgpartition", "--yes")
        except Exception as e:
            log.exception(
                "Error running partition management",
                extra={"exception": str(e), "task_output": sys.stdout.getvalue()},
            )
            return {
                "successful": False,
                "reason": "Failed with exception",
                "exception": str(e),
                "task_output": sys.stdout.getvalue(),
            }
        finally:
            out = sys.stdout.getvalue()
            sys.stdout = prev

        log.info(
            "Partition management completed successfully",
            extra={"task_output": out},
        )
        return {"successful": True}


RegisteredPartitionManagementTask = celery_app.register_task(PartitionManagementTask())
partition_management_task = celery_app.tasks[RegisteredPartitionManagementTask.name]
