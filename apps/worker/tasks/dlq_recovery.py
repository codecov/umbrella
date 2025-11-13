"""
Dead Letter Queue (DLQ) Recovery Task

This task allows manual recovery of tasks that were saved to the DLQ after
exhausting all retries. Tasks can be re-queued for processing after investigation.
"""

import logging
from typing import Any

import orjson

from app import celery_app
from shared.celery_config import DLQ_KEY_PREFIX, dlq_recovery_task_name
from shared.helpers.redis import get_redis_connection
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


class DLQRecoveryTask(BaseCodecovTask, name=dlq_recovery_task_name):
    """
    Task to recover tasks from Dead Letter Queue.

    This task allows operators to:
    1. List tasks in DLQ
    2. Re-queue specific tasks for retry
    3. Delete tasks from DLQ after recovery

    Args:
        dlq_key: Full DLQ key (e.g., "task_dlq/app.tasks.upload.Upload/{repoid}/{commitid}")
        action: Action to perform - "list", "recover", or "delete"
        task_name_filter: Optional filter to only show/recover tasks matching this name
    """

    def run_impl(
        self,
        db_session,
        dlq_key: str | None = None,
        action: str = "list",
        task_name_filter: str | None = None,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Run DLQ recovery operations.

        Actions:
        - "list": List all DLQ keys (optionally filtered by task_name_filter)
        - "recover": Re-queue tasks from a specific DLQ key
        - "delete": Delete tasks from a specific DLQ key
        """
        redis_conn = get_redis_connection()

        if action == "list":
            return self._list_dlq_keys(redis_conn, task_name_filter)
        elif action == "recover":
            if not dlq_key:
                return {
                    "success": False,
                    "error": "dlq_key required for recover action",
                }
            return self._recover_tasks(redis_conn, dlq_key)
        elif action == "delete":
            if not dlq_key:
                return {"success": False, "error": "dlq_key required for delete action"}
            return self._delete_tasks(redis_conn, dlq_key)
        else:
            return {"success": False, "error": f"Unknown action: {action}"}

    def _list_dlq_keys(
        self, redis_conn, task_name_filter: str | None
    ) -> dict[str, Any]:
        """List all DLQ keys, optionally filtered by task name."""
        pattern = f"{DLQ_KEY_PREFIX}/*"
        if task_name_filter:
            pattern = f"{DLQ_KEY_PREFIX}/{task_name_filter}/*"

        keys = []
        for key in redis_conn.scan_iter(match=pattern):
            key_str = key.decode("utf-8") if isinstance(key, bytes) else key
            length = redis_conn.llen(key_str)
            ttl = redis_conn.ttl(key_str)
            keys.append({"key": key_str, "count": length, "ttl_seconds": ttl})

        return {
            "success": True,
            "action": "list",
            "keys": keys,
            "total_keys": len(keys),
        }

    def _recover_tasks(self, redis_conn, dlq_key: str) -> dict[str, Any]:
        """
        Recover tasks from DLQ by re-queueing them.

        This reads all task data from the DLQ key and attempts to re-queue
        each task using its original task name and arguments.
        """
        if not redis_conn.exists(dlq_key):
            return {"success": False, "error": f"DLQ key not found: {dlq_key}"}

        recovered_count = 0
        failed_count = 0
        errors = []

        # Read all tasks from DLQ
        while True:
            task_data_str = redis_conn.lpop(dlq_key)
            if not task_data_str:
                break

            try:
                # Deserialize task data
                task_data = orjson.loads(task_data_str)

                # Extract task name and arguments
                task_name = task_data.get("task_name")
                task_args = task_data.get("args", [])
                task_kwargs = task_data.get("kwargs", {})

                if not task_name:
                    failed_count += 1
                    errors.append("Task data missing task_name")
                    continue

                # Re-queue the task
                celery_app.send_task(task_name, args=task_args, kwargs=task_kwargs)
                recovered_count += 1

                log.info(
                    f"Recovered task from DLQ: {task_name}",
                    extra={"dlq_key": dlq_key, "task_name": task_name},
                )
            except Exception as e:
                failed_count += 1
                error_msg = f"Failed to recover task: {str(e)}"
                errors.append(error_msg)
                log.exception(
                    "Failed to recover task from DLQ",
                    extra={"dlq_key": dlq_key, "error": str(e)},
                )

        return {
            "success": True,
            "action": "recover",
            "dlq_key": dlq_key,
            "recovered_count": recovered_count,
            "failed_count": failed_count,
            "errors": errors[:10],  # Limit errors to first 10
        }

    def _delete_tasks(self, redis_conn, dlq_key: str) -> dict[str, Any]:
        """Delete all tasks from a DLQ key."""
        if not redis_conn.exists(dlq_key):
            return {"success": False, "error": f"DLQ key not found: {dlq_key}"}

        count = redis_conn.llen(dlq_key)
        redis_conn.delete(dlq_key)

        log.info(
            f"Deleted DLQ key: {dlq_key}", extra={"dlq_key": dlq_key, "count": count}
        )

        return {
            "success": True,
            "action": "delete",
            "dlq_key": dlq_key,
            "deleted_count": count,
        }


RegisteredDLQRecoveryTask = celery_app.register_task(DLQRecoveryTask())
dlq_recovery_task = celery_app.tasks[RegisteredDLQRecoveryTask.name]
