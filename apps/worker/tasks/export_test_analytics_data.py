import logging
import pathlib
import tempfile
from datetime import datetime
from typing import Any

import sentry_sdk
from google.cloud import storage
from sqlalchemy.orm import Session

from app import celery_app
from shared.celery_config import export_test_analytics_data_task_name
from shared.django_apps.codecov_auth.models import Owner, Service
from shared.django_apps.core.models import Repository
from shared.django_apps.ta_timeseries.models import Testrun
from shared.storage.data_exporter import _Archiver
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


def serialize_test_run(test_run: dict) -> list:
    """
    Convert a test run dict to compact list format. This is done
    instead of django serializers/dictionaries because it saves
    space, and we're likely dealing with a lot of data.

    Args:
        test_run: Dictionary containing test run data

    Returns:
        List of values in a consistent field order
    """
    return [
        test_run.get("filename"),
        test_run["timestamp"].isoformat() if test_run.get("timestamp") else None,
        test_run.get("testsuite"),
        test_run.get("outcome"),
        test_run.get("duration_seconds"),
        test_run.get("failure_message"),
        test_run.get("framework"),
        test_run.get("commit_sha"),
        test_run.get("branch"),
        test_run.get("flags"),
    ]


class ExportTestAnalyticsDataTask(
    BaseCodecovTask, name=export_test_analytics_data_task_name
):
    """
    This task exports test analytics data to a tarfile and uploads it to GCP.
    """

    # Override the global task_ignore_result=True setting
    # so we can track this task's status and result
    ignore_result = False

    def run_impl(
        self,
        _db_session: Session,
        integration_name: str,
        gcp_project_id: str,
        destination_bucket: str,
        destination_prefix: str,
        **kwargs: Any,
    ):
        log.info(
            "Received export test analytics data task",
            extra={
                "integration_name": integration_name,
            },
        )

        try:
            owner = Owner.objects.get(name=integration_name, service=Service.GITHUB)
        except Owner.DoesNotExist:
            log.warning(
                f"Owner with name {integration_name} and service {Service.GITHUB} not found"
            )
            return {
                "successful": False,
                "error": f"Owner with name {integration_name} and service {Service.GITHUB} not found",
            }

        repo_id_to_name = dict(
            Repository.objects.filter(
                author=owner, test_analytics_enabled=True
            ).values_list("repoid", "name")
        )

        if not repo_id_to_name:
            log.warning(f"No repositories found for owner {integration_name}")
            return {
                "successful": False,
                "error": f"No repositories found for owner {integration_name}",
            }

        # Initialize GCS client and bucket
        gcs_client = storage.Client(project=gcp_project_id)
        bucket = gcs_client.bucket(destination_bucket)

        fields = [
            "filename",
            "timestamp",
            "testsuite",
            "outcome",
            "duration_seconds",
            "failure_message",
            "framework",
            "commit_sha",
            "branch",
            "flags",
        ]

        repositories_succeeded = []
        repositories_failed = []

        # Process each repository and upload the data as tar.gz files using the archiver
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = pathlib.Path(temp_dir)
            prefix = pathlib.PurePosixPath(destination_prefix)

            with _Archiver(temp_path, bucket, prefix) as archiver:
                for repo_id, repo_name in repo_id_to_name.items():
                    try:
                        start_time = datetime.now()
                        log.info(f"Processing repository: {repo_name} (ID: {repo_id})")

                        test_runs_qs = (
                            Testrun.objects.filter(repo_id=repo_id)
                            .order_by("-timestamp")
                            .values(*fields)
                        )
                        test_runs_data = [
                            serialize_test_run(tr) for tr in list(test_runs_qs)
                        ]

                        repo_data = {"fields": fields, "data": test_runs_data}
                        blob_name = f"{integration_name}/{repo_name}.json"
                        archiver.upload_json(blob_name, repo_data)

                        repositories_succeeded.append({"name": repo_name})

                        end_time = datetime.now()
                        duration = (end_time - start_time).total_seconds()
                        log.info(
                            f"Successfully processed repository {repo_name}: {len(test_runs_data)} test runs in {duration:.2f}s"
                        )
                    except Exception as e:
                        log.error(
                            f"Failed to process repository {repo_name} (ID: {repo_id}): {str(e)}",
                            exc_info=True,
                        )
                        sentry_sdk.capture_exception(e)
                        repositories_failed.append({"name": repo_name, "error": str(e)})

        log.info(
            "Export test analytics data task completed",
            extra={
                "integration_name": integration_name,
                "repositories_succeeded": repositories_succeeded,
                "repositories_failed": repositories_failed,
            },
        )

        return {
            "message": "Export test analytics data task completed",
            "integration_name": integration_name,
            "repositories_succeeded": repositories_succeeded,
            "repositories_failed": repositories_failed,
        }


RegisteredExportTestAnalyticsDataTask = celery_app.register_task(
    ExportTestAnalyticsDataTask()
)
export_test_analytics_data_task = celery_app.tasks[
    RegisteredExportTestAnalyticsDataTask.name
]
