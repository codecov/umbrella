"""
Django management command for Dead Letter Queue (DLQ) operations.

This command provides a CLI interface to the DLQ recovery task for easier
inspection and recovery of failed tasks.
"""

from django.core.management.base import BaseCommand

from app import celery_app
from shared.celery_config import dlq_recovery_task_name


class Command(BaseCommand):
    help = "Manage Dead Letter Queue (DLQ) - list, recover, or delete failed tasks"

    def add_arguments(self, parser):
        parser.add_argument(
            "action",
            type=str,
            choices=["list", "recover", "delete"],
            help="Action to perform: list DLQ keys, recover tasks, or delete tasks",
        )
        parser.add_argument(
            "--dlq-key",
            type=str,
            help="DLQ key to recover or delete (required for recover/delete actions)",
        )
        parser.add_argument(
            "--task-name-filter",
            type=str,
            help="Filter DLQ keys by task name (for list action)",
        )

    def handle(self, *args, **options):
        action = options["action"]
        dlq_key = options.get("dlq_key")
        task_name_filter = options.get("task_name_filter")

        # Get the DLQ recovery task
        dlq_task = celery_app.tasks[dlq_recovery_task_name]

        # Call the task synchronously for CLI
        result = dlq_task.run(
            dlq_key=dlq_key,
            action=action,
            task_name_filter=task_name_filter,
        )

        if result.get("success"):
            self.stdout.write(self.style.SUCCESS(f"Action '{action}' completed successfully"))
            if action == "list":
                keys = result.get("keys", [])
                self.stdout.write(f"\nFound {result.get('total_keys', 0)} DLQ key(s):")
                for key_info in keys:
                    self.stdout.write(
                        f"  - {key_info['key']}: {key_info['count']} task(s), "
                        f"TTL: {key_info['ttl_seconds']}s"
                    )
            elif action == "recover":
                self.stdout.write(
                    f"Recovered {result.get('recovered_count', 0)} task(s) from {dlq_key}"
                )
                if result.get("failed_count", 0) > 0:
                    self.stdout.write(
                        self.style.WARNING(
                            f"Failed to recover {result.get('failed_count', 0)} task(s)"
                        )
                    )
                    for error in result.get("errors", [])[:5]:
                        self.stdout.write(self.style.ERROR(f"  - {error}"))
            elif action == "delete":
                self.stdout.write(
                    f"Deleted {result.get('deleted_count', 0)} task(s) from {dlq_key}"
                )
        else:
            self.stdout.write(
                self.style.ERROR(f"Action '{action}' failed: {result.get('error', 'Unknown error')}")
            )

