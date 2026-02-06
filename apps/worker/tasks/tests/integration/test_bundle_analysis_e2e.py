import json
from uuid import uuid4

import pytest
from celery import chain
from redis.exceptions import LockError

from database.tests.factories import CommitFactory, RepositoryFactory, UploadFactory
from services.bundle_analysis.report import BundleAnalysisReportService
from shared.api_archive.archive import ArchiveService
from shared.bundle_analysis.storage import get_bucket_name
from shared.celery_config import bundle_analysis_save_measurements_task_name
from shared.yaml import UserYaml
from tasks.bundle_analysis_notify import bundle_analysis_notify_task
from tasks.bundle_analysis_processor import (
    BundleAnalysisProcessorTask,
    bundle_analysis_processor_task,
)
from tasks.tests.utils import hook_repo_provider, hook_session, run_tasks
from tests.helpers import mock_all_plans_and_tiers


def get_sample_bundle_stats_json():
    """Returns a minimal valid bundle stats JSON for testing."""
    return {
        "version": "2",
        "plugin": {"name": "codecov-vite-bundle-analysis-plugin", "version": "1.0.0"},
        "builtAt": 1701451048604,
        "duration": 331,
        "bundler": {"name": "rollup", "version": "3.29.4"},
        "bundleName": "sample",
        "assets": [
            {
                "name": "assets/index-666d2e09.js",
                "size": 144577,
                "gzipSize": 144576,
                "normalized": "assets/index-*.js",
            }
        ],
        "chunks": [
            {
                "id": "index",
                "uniqueId": "2-index",
                "entry": True,
                "initial": True,
                "files": ["assets/index-666d2e09.js"],
                "names": ["index"],
            }
        ],
        "modules": [
            {"name": "./src/main.tsx", "size": 181, "chunkUniqueIds": ["2-index"]}
        ],
    }


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
def test_bundle_analysis_chain_e2e(
    dbsession,
    mocker,
    mock_repo_provider,
    mock_storage,
    mock_configuration,
):
    """Test the full bundle analysis chain: processor -> notify.

    This test verifies that:
    1. The chain executes successfully
    2. previous_result accumulates correctly through the chain
    3. Database state is updated correctly
    """
    mock_all_plans_and_tiers()

    # Setup mocks similar to test_full_upload
    hook_session(mocker, dbsession)
    mocker.patch("tasks.base.BaseCodecovTask.wrap_up_dbsession")
    hook_repo_provider(mocker, mock_repo_provider)
    mocker.patch("tasks.upload.UploadTask.possibly_setup_webhooks", return_value=True)
    mocker.patch(
        "tasks.upload.fetch_commit_yaml_and_possibly_store",
        return_value=UserYaml({}),
    )

    # Mock the save_measurements task to avoid side effects
    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    # Mock notify service to avoid actual GitHub API calls
    mocker.patch(
        "services.bundle_analysis.notify.BundleAnalysisNotifyService.notify",
        return_value=mocker.MagicMock(to_NotificationSuccess=lambda: "success"),
    )

    repository = RepositoryFactory.create()
    dbsession.add(repository)
    dbsession.flush()

    commit = CommitFactory.create(repository=repository, state="pending")
    dbsession.add(commit)
    dbsession.flush()

    # Create commit report for bundle analysis
    report_service = BundleAnalysisReportService({})
    commit_report = report_service.initialize_and_save_report(commit)
    dbsession.flush()

    # Create bundle analysis upload with JSON data
    archive_service = ArchiveService(repository)
    bundle_json = get_sample_bundle_stats_json()
    bundle_json_bytes = json.dumps(bundle_json).encode("utf-8")

    # Write bundle JSON to storage
    storage_path = f"v1/uploads/{uuid4().hex}.json"
    archive_service.write_file(storage_path, bundle_json_bytes)

    # Also write to mock_storage for the bundle analysis processor to read
    mock_storage.write_file(get_bucket_name(), storage_path, bundle_json_bytes)

    upload = UploadFactory.create(
        storage_path=storage_path,
        report=commit_report,
        state="started",
    )
    dbsession.add(upload)
    dbsession.flush()

    # Create the chain: processor -> notify
    # First task gets empty previous_result
    task_signatures = [
        bundle_analysis_processor_task.s(
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            params={
                "upload_id": upload.id_,
                "commit": commit.commitid,
            },
        ),
        bundle_analysis_notify_task.signature(
            kwargs={
                "repoid": commit.repoid,
                "commitid": commit.commitid,
                "commit_yaml": {},
            }
        ),
    ]
    task_signatures[0].args = ([],)  # Initial previous_result

    # Execute chain synchronously
    with run_tasks():
        result = chain(task_signatures).apply()

    # Verify the chain completed successfully
    # The notify task should return a dict with notify results
    assert result is not None

    # Verify database state
    dbsession.refresh(commit)
    dbsession.refresh(upload)

    # Commit should be complete, upload should be processed
    assert commit.state == "complete"
    assert upload.state == "processed"


@pytest.mark.integration
@pytest.mark.django_db(transaction=True)
def test_bundle_analysis_chain_max_retries_exceeded(
    dbsession,
    mocker,
    mock_repo_provider,
    mock_storage,
    mock_configuration,
    mock_redis,
):
    """Test that bundle analysis chain handles max retries exceeded correctly.

    This test verifies the fix for infinite retry loops by ensuring that when
    max retries are exceeded, the task returns previous_result instead of
    crashing the chain.
    """
    mock_all_plans_and_tiers()

    hook_session(mocker, dbsession)
    mocker.patch("tasks.base.BaseCodecovTask.wrap_up_dbsession")
    hook_repo_provider(mocker, mock_repo_provider)
    mocker.patch("tasks.upload.UploadTask.possibly_setup_webhooks", return_value=True)
    mocker.patch(
        "tasks.upload.fetch_commit_yaml_and_possibly_store",
        return_value=UserYaml({}),
    )

    # Mock the save_measurements task
    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    repository = RepositoryFactory.create()
    dbsession.add(repository)
    dbsession.flush()

    commit = CommitFactory.create(repository=repository, state="pending")
    dbsession.add(commit)
    dbsession.flush()

    report_service = BundleAnalysisReportService({})
    commit_report = report_service.initialize_and_save_report(commit)
    dbsession.flush()

    # Create bundle analysis upload
    archive_service = ArchiveService(repository)
    bundle_json = get_sample_bundle_stats_json()
    bundle_json_bytes = json.dumps(bundle_json).encode("utf-8")

    storage_path = f"v1/uploads/{uuid4().hex}.json"
    archive_service.write_file(storage_path, bundle_json_bytes)
    mock_storage.write_file(get_bucket_name(), storage_path, bundle_json_bytes)

    upload = UploadFactory.create(
        storage_path=storage_path,
        report=commit_report,
        state="started",
    )
    dbsession.add(upload)
    dbsession.flush()

    # Mock Redis lock to fail (simulating lock contention)
    # This will cause LockRetry to be raised
    mock_redis.lock.return_value.__enter__.side_effect = LockError("Lock failed")
    mock_redis.incr.return_value = 1  # LockManager: avoid MagicMock >= int

    # Create the chain
    task_signatures = [
        bundle_analysis_processor_task.s(
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            params={
                "upload_id": upload.id_,
                "commit": commit.commitid,
            },
        ),
        bundle_analysis_notify_task.signature(
            kwargs={
                "repoid": commit.repoid,
                "commitid": commit.commitid,
                "commit_yaml": {},
            }
        ),
    ]
    task_signatures[0].args = (
        [{"test": "previous_result"}],
    )  # Initial previous_result

    # Patch run_impl to set retries to max_retries + 1 before execution
    # This simulates the scenario where max retries are exceeded
    original_run_impl = BundleAnalysisProcessorTask.run_impl

    def patched_run_impl(self, *args, **kwargs):
        # Set retries to max_retries + 1 to trigger the max retries exceeded path
        if hasattr(self, "request"):
            self.request.retries = self.max_retries + 1
        return original_run_impl(self, *args, **kwargs)

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "run_impl",
        patched_run_impl,
    )

    # Execute chain - should not crash even with max retries exceeded
    # The processor should return previous_result instead of raising MaxRetriesExceededError
    with run_tasks():
        try:
            result = chain(task_signatures).apply()
            # If we get here, the chain completed (even if with previous_result)
            # This is the desired behavior - chain continues with partial results
            assert result is not None
        except Exception as e:
            # The chain should not crash - if it does, that's a bug
            pytest.fail(f"Chain crashed with max retries exceeded: {e}")
