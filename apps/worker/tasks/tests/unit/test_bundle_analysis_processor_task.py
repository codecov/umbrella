import pytest
from celery.exceptions import Retry
from redis.exceptions import LockError

from database.enums import ReportType
from database.models import CommitReport, Upload
from database.tests.factories import CommitFactory, RepositoryFactory, UploadFactory
from services.bundle_analysis.report import ProcessingError, ProcessingResult
from services.lock_manager import LockManager
from shared.api_archive.archive import ArchiveService
from shared.bundle_analysis.storage import get_bucket_name
from shared.celery_config import BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES
from shared.django_apps.bundle_analysis.models import CacheConfig
from shared.storage.exceptions import PutRequestRateLimitError
from tasks.bundle_analysis_processor import BundleAnalysisProcessorTask
from tasks.bundle_analysis_save_measurements import (
    bundle_analysis_save_measurements_task_name,
)


class MockBundleReport:
    def __init__(self, bundle_name, size):
        self.bundle_name = bundle_name
        self.size = size

    @property
    def name(self):
        return self.bundle_name


class MockBundleAnalysisReport:
    def bundle_reports(self):
        return [
            MockBundleReport("BundleA", 1111),
        ]

    def ingest(self, path, compare_sha: str | None = None):
        return 123, "BundleA"

    def cleanup(self):
        pass

    def delete_bundle_by_name(self, bundle_name):
        pass

    def update_is_cached(self, d):
        pass

    def associate_previous_assets(self, prev_bar):
        pass

    def metadata(self):
        return {}


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_task_success(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": 123,
            "upload_id": upload.id_,
            "bundle_name": "bundle1",
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"


def test_bundle_analysis_processor_task_error(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        storage_path="invalid-storage-path", report=commit_report
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    retry = mocker.patch.object(task, "retry")
    task.request.retries = 0
    task.request.headers = {}

    result = task.run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": {
                "code": "file_not_in_storage",
                "params": {"location": "invalid-storage-path"},
            },
            "session_id": None,
            "upload_id": upload.id_,
            "bundle_name": None,
        },
    ]

    assert commit.state == "error"
    assert upload.state == "error"
    retry.assert_called_once()
    assert retry.call_args[1]["max_retries"] == task.max_retries
    expected_countdown = 30 * (2**task.request.retries)
    assert retry.call_args[1]["countdown"] == expected_countdown


def test_bundle_analysis_processor_task_general_error(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    process_upload = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService.process_upload"
    )
    process_upload.side_effect = Exception()

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path="invalid-storage-path",
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    retry = mocker.patch.object(task, "retry")

    previous_result = [{"previous": "result"}]
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert result == previous_result
    assert upload.state == "error"
    assert not retry.called


def test_bundle_analysis_process_upload_general_error(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.side_effect = Exception()

    task = BundleAnalysisProcessorTask()
    retry = mocker.patch.object(task, "retry")

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert result == [
        {"previous": "result"},
        {
            "error": {
                "code": "parser_error",
                "params": {
                    "location": "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite",
                    "plugin_name": "unknown",
                },
            },
            "session_id": None,
            "upload_id": upload.id_,
            "bundle_name": None,
        },
    ]

    assert not retry.called
    assert upload.state == "error"
    assert commit.state == "error"


def test_bundle_analysis_processor_task_locked(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """Test that bundle analysis processor retries when lock cannot be acquired."""
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )
    mock_redis.lock.return_value.__enter__.side_effect = LockError()
    mock_redis.incr.return_value = (
        1  # LockManager: attempts < max_retries so it raises LockRetry(countdown)
    )

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    task.request.retries = 0  # Will retry (below max_retries)

    # Task should raise Retry (from self.retry()) when lock cannot be acquired
    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            [{"previous": "result"}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            params={
                "upload_id": upload.id_,
                "commit": commit.commitid,
            },
        )

    assert upload.state == "started"


def test_bundle_analysis_processor_task_max_retries_exceeded_raises_error(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """Test that bundle analysis processor returns previous_result when max retries exceeded.

    This test verifies the fix for infinite retry loops by ensuring that when max retries
    are exceeded, the task returns previous_result instead of retrying infinitely.
    This preserves chain behavior while preventing infinite loops.
    """
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )
    # Mock Redis to simulate lock failure - this will cause LockManager to raise LockRetry
    mock_redis.lock.return_value.__enter__.side_effect = LockError()
    # So LockManager sees attempts >= max_retries and raises LockRetry(max_retries_exceeded=True)
    mock_redis.incr.return_value = BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    # Set retries to max_retries to simulate max attempts exceeded scenario
    # Our code checks if retries >= max_retries, so retries = max_retries should exceed
    # This tests the real retry logic without mocking safe_retry or _has_exceeded_max_attempts
    task.request.retries = BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES
    task.request.headers = {}

    previous_result = [{"previous": "result"}]
    # Task should return previous_result instead of retrying infinitely
    # This preserves chain behavior while preventing infinite loops
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == previous_result


def test_bundle_analysis_processor_task_uses_default_blocking_timeout(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """
    Test that BundleAnalysisProcessorTask uses default blocking_timeout (not None).

    This test verifies that the task doesn't use blocking_timeout=None, which would
    cause indefinite blocking and disable retry logic. The task should use the
    default blocking_timeout to enable proper retry behavior.
    """
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    # Track what blocking_timeout was passed to Redis
    blocking_timeouts = []

    def track_and_raise(*args, **kwargs):
        blocking_timeouts.append(kwargs.get("blocking_timeout"))
        raise LockError()

    mock_redis.lock.side_effect = track_and_raise
    mock_redis.incr.return_value = 1  # LockManager: avoid MagicMock >= int

    task = BundleAnalysisProcessorTask()
    task.request.retries = 0
    task.request.headers = {}

    # Task should raise Retry when lock cannot be acquired
    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            [{"previous": "result"}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            params={
                "upload_id": upload.id_,
                "commit": commit.commitid,
            },
        )

    assert None not in blocking_timeouts, (
        "blocking_timeout=None was used! This causes indefinite blocking "
        "and disables retry logic. Use default blocking_timeout instead."
    )
    assert blocking_timeouts[0] == 30, (
        f"Expected blocking_timeout=30 for bundle analysis, got {blocking_timeouts[0]}"
    )


def test_bundle_analysis_processor_task_uses_ba_retry_countdown(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """
    Test that BundleAnalysisProcessorTask uses base_retry_countdown=10s
    instead of the default 200s.

    This verifies the optimization for high-concurrency bundle analysis scenarios.
    """
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    original_init = LockManager.__init__
    captured_kwargs = {}

    def capture_init(self, *args, **kwargs):
        captured_kwargs.update(kwargs)
        return original_init(self, *args, **kwargs)

    mocker.patch.object(LockManager, "__init__", capture_init)
    mock_redis.lock.return_value.__enter__.side_effect = LockError()

    task = BundleAnalysisProcessorTask()
    task.request.retries = 0
    task.request.headers = {}

    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            [{"previous": "result"}],
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            params={
                "upload_id": upload.id_,
                "commit": commit.commitid,
            },
        )

    assert captured_kwargs.get("base_retry_countdown") == 10, (
        f"Expected base_retry_countdown=10 for bundle analysis, "
        f"got {captured_kwargs.get('base_retry_countdown')}"
    )
    assert captured_kwargs.get("blocking_timeout") == 30, (
        f"Expected blocking_timeout=30 for bundle analysis, "
        f"got {captured_kwargs.get('blocking_timeout')}"
    )


def test_bundle_analysis_process_upload_rate_limit_error(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    retry = mocker.patch.object(task, "retry")
    task.request.retries = 0
    task.request.headers = {}

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.side_effect = PutRequestRateLimitError()

    result = task.run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": {
                "code": "rate_limit_error",
                "params": {
                    "location": "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
                },
            },
            "session_id": None,
            "upload_id": upload.id_,
            "bundle_name": None,
        },
    ]

    assert commit.state == "error"
    assert upload.state == "error"
    retry.assert_called_once()
    assert retry.call_args[1]["max_retries"] == task.max_retries
    expected_countdown = 30 * (2**task.request.retries)
    assert retry.call_args[1]["countdown"] == expected_countdown


def test_bundle_analysis_process_associate_no_parent_commit_id(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    parent_commit = CommitFactory.create(state="complete")
    dbsession.add(parent_commit)
    dbsession.flush()

    commit = CommitFactory.create(state="pending", parent_commit_id=None)
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert commit.state == "complete"
    assert upload.state == "processed"


def test_bundle_analysis_process_associate_no_parent_commit_object(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    parent_commit = CommitFactory.create(state="complete")

    commit = CommitFactory.create(
        state="pending", parent_commit_id=parent_commit.commitid
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert commit.state == "complete"
    assert upload.state == "processed"


def test_bundle_analysis_process_associate_no_parent_commit_report_object(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    parent_commit = CommitFactory.create(state="complete")
    dbsession.add(parent_commit)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending",
        parent_commit_id=parent_commit.commitid,
        repository=parent_commit.repository,
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert commit.state == "complete"
    assert upload.state == "processed"


def test_bundle_analysis_process_associate_called(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    parent_commit = CommitFactory.create(state="complete")
    dbsession.add(parent_commit)
    dbsession.flush()

    parent_commit_report = CommitReport(
        commit_id=parent_commit.id_, report_type="bundle_analysis"
    )
    dbsession.add(parent_commit_report)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending",
        parent_commit_id=parent_commit.commitid,
        repository=parent_commit.repository,
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert commit.state == "complete"
    assert upload.state == "processed"


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_process_associate_called_two(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    parent_commit = CommitFactory.create(state="complete")
    dbsession.add(parent_commit)
    dbsession.flush()

    parent_commit_report = CommitReport(
        commit_id=parent_commit.id_, report_type="bundle_analysis"
    )
    dbsession.add(parent_commit_report)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending",
        parent_commit_id=parent_commit.commitid,
        repository=parent_commit.repository,
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    associate = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReport.associate_previous_assets"
    )
    associate.return_value = None

    prev_bundle_report = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService._previous_bundle_analysis_report"
    )
    prev_bundle_report.side_effect = [None, MockBundleAnalysisReport()]

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert commit.state == "complete"
    assert upload.state == "processed"
    associate.assert_called_once()


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_associate_custom_compare_sha(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    parent_commit = CommitFactory.create(state="complete")
    dbsession.add(parent_commit)
    dbsession.flush()

    parent_commit_report = CommitReport(
        commit_id=parent_commit.id_, report_type="bundle_analysis"
    )
    dbsession.add(parent_commit_report)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending",
        parent_commit_id=parent_commit.commitid,
        repository=parent_commit.repository,
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    _get_parent_commit = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService._get_parent_commit"
    )
    _get_parent_commit.side_effect = [None, None]

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert commit.state == "complete"
    assert upload.state == "processed"

    assert _get_parent_commit.call_count == 2
    args = _get_parent_commit.call_args_list

    assert args[0][1]["head_commit"] == commit
    assert args[1][1]["head_commit"] == commit

    assert args[0][1]["head_bundle_report"] is None
    assert args[1][1]["head_bundle_report"] is not None


def test_bundle_analysis_processor_task_cache_config_not_saved(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.load",
        return_value=MockBundleAnalysisReport(),
    )

    bundle_load_mock_save = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.save",
        return_value=MockBundleAnalysisReport(),
    )
    bundle_load_mock_save.return_value = None

    bundle_config_mock = mocker.patch(
        "shared.django_apps.bundle_analysis.service.bundle_analysis.BundleAnalysisCacheConfigService.update_cache_option"
    )

    commit = CommitFactory.create(state="pending")

    # Using main branch as default and commit is in feat
    commit.branch = "feat"
    commit.repository.branch = "main"

    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": 123,
            "upload_id": upload.id_,
            "bundle_name": "BundleA",
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"

    bundle_config_mock.assert_not_called()


def test_bundle_analysis_processor_task_cache_config_saved(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.load",
        return_value=MockBundleAnalysisReport(),
    )

    bundle_load_mock_save = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.save",
        return_value=MockBundleAnalysisReport(),
    )
    bundle_load_mock_save.return_value = None

    bundle_config_mock = mocker.patch(
        "shared.django_apps.bundle_analysis.service.bundle_analysis.BundleAnalysisCacheConfigService.create_if_not_exists"
    )

    commit = CommitFactory.create(state="pending")

    # Using main branch as default and commit is in main
    commit.branch = "main"
    commit.repository.branch = "main"

    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": 123,
            "upload_id": upload.id_,
            "bundle_name": "BundleA",
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"

    bundle_config_mock.assert_called_with(commit.repository.repoid, "BundleA")


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_not_caching_previous_report(
    mocker,
    dbsession,
    mock_storage,
):
    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    repository = RepositoryFactory()

    prev_commit = CommitFactory.create(repository=repository)
    dbsession.add(prev_commit)
    dbsession.flush()

    prev_report = CommitReport(
        commit_id=prev_commit.id_, report_type=ReportType.BUNDLE_ANALYSIS.value
    )
    dbsession.add(prev_report)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending", repository=repository, parent_commit_id=prev_commit.commitid
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    repo_key = ArchiveService.get_archive_hash(prev_commit.repository)
    storage_path = (
        f"v1/repos/{repo_key}/{prev_report.external_id}/bundle_report.sqlite",
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    loader_mock = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.load",
    )
    loader_mock.side_effect = [None, MockBundleAnalysisReport(), None]

    saver_mock = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.save",
    )
    saver_mock.return_value = None

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": 123,
            "upload_id": upload.id_,
            "bundle_name": "BundleA",
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_not_caching_previous_report_two(
    mocker,
    dbsession,
    mock_storage,
):
    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    repository = RepositoryFactory()

    prev_commit = CommitFactory.create(repository=repository)
    dbsession.add(prev_commit)
    dbsession.flush()

    prev_report = CommitReport(
        commit_id=prev_commit.id_, report_type=ReportType.BUNDLE_ANALYSIS.value
    )
    dbsession.add(prev_report)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending", repository=repository, parent_commit_id=prev_commit.commitid
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    CacheConfig.objects.create(
        repo_id=commit.repoid, bundle_name="BundleA", is_caching=False
    )

    repo_key = ArchiveService.get_archive_hash(prev_commit.repository)
    storage_path = (
        f"v1/repos/{repo_key}/{prev_report.external_id}/bundle_report.sqlite",
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    loader_mock = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.load",
    )
    loader_mock.side_effect = [None, MockBundleAnalysisReport(), None]

    saver_mock = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.save",
    )
    saver_mock.return_value = None

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": 123,
            "upload_id": upload.id_,
            "bundle_name": "BundleA",
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_caching_previous_report(
    mocker,
    dbsession,
    mock_storage,
):
    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    repository = RepositoryFactory()

    prev_commit = CommitFactory.create(repository=repository)
    dbsession.add(prev_commit)
    dbsession.flush()

    prev_report = CommitReport(
        commit_id=prev_commit.id_, report_type=ReportType.BUNDLE_ANALYSIS.value
    )
    dbsession.add(prev_report)
    dbsession.flush()

    commit = CommitFactory.create(
        state="pending", repository=repository, parent_commit_id=prev_commit.commitid
    )
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    CacheConfig.objects.create(
        repo_id=commit.repoid, bundle_name="BundleA", is_caching=True
    )

    repo_key = ArchiveService.get_archive_hash(prev_commit.repository)
    storage_path = (
        f"v1/repos/{repo_key}/{prev_report.external_id}/bundle_report.sqlite",
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    loader_mock = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.load",
    )
    loader_mock.side_effect = [None, MockBundleAnalysisReport(), None]

    saver_mock = mocker.patch(
        "shared.bundle_analysis.BundleAnalysisReportLoader.save",
    )
    saver_mock.return_value = None

    ingest = mocker.patch("shared.bundle_analysis.BundleAnalysisReport.ingest")
    ingest.return_value = (123, "bundle1")  # session_id

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": 123,
            "upload_id": upload.id_,
            "bundle_name": "BundleA",
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_task_no_upload(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    result = BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": None,
            "commit": commit.commitid,
        },
    )

    commit_report = dbsession.query(CommitReport).filter_by(commit_id=commit.id).first()
    assert commit_report is not None

    upload = dbsession.query(Upload).filter_by(report_id=commit_report.id).first()
    assert upload is not None

    assert result == [
        {"previous": "result"},
        {
            "error": None,
            "session_id": None,
            "upload_id": upload.id_,
            "bundle_name": None,
        },
    ]

    assert commit.state == "complete"
    assert upload.state == "processed"
    assert upload.upload_type == "carriedforward"


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_task_carryforward(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(
        commit_id=commit.id_, report_type=ReportType.BUNDLE_ANALYSIS.value
    )
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        storage_path=storage_path, report=commit_report, state="processed"
    )
    dbsession.add(upload)
    dbsession.flush()

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": None,
            "commit": commit.commitid,
        },
    )

    # A new upload wasn't created because the caching was skipped
    total_uploads = (
        dbsession.query(Upload).filter_by(report_id=commit_report.id).count()
    )
    assert total_uploads == 1

    # A new report wasn't created either
    total_ba_reports = (
        dbsession.query(CommitReport).filter_by(commit_id=commit.id).count()
    )
    assert total_ba_reports == 1


@pytest.mark.django_db(databases={"default", "timeseries"})
def test_bundle_analysis_processor_task_carryforward_error(
    mocker,
    dbsession,
    mock_storage,
):
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(
        commit_id=commit.id_, report_type=ReportType.BUNDLE_ANALYSIS.value
    )
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        storage_path=storage_path, report=commit_report, state="error"
    )
    dbsession.add(upload)
    dbsession.flush()

    BundleAnalysisProcessorTask().run_impl(
        dbsession,
        [{"previous": "result"}],
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": None,
            "commit": commit.commitid,
        },
    )

    # A new upload was created because all the previous uploads were in error states
    total_uploads = (
        dbsession.query(Upload).filter_by(report_id=commit_report.id).count()
    )
    assert total_uploads == 2

    # There should still only be 1 BA report
    total_ba_reports = (
        dbsession.query(CommitReport).filter_by(commit_id=commit.id).count()
    )
    assert total_ba_reports == 1


def test_bundle_analysis_processor_task_max_retries_exceeded_lock(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """Test that when max retries are exceeded during lock acquisition, task returns previous_result."""
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )
    mock_redis.lock.return_value.__enter__.side_effect = LockError()
    # So LockManager sees attempts >= max_retries and raises LockRetry(max_retries_exceeded=True)
    mock_redis.incr.return_value = BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    # Set retries to max_retries to exceed limit (using self.request.retries, not self.attempts)
    task.request.retries = task.max_retries
    task.request.headers = {}

    previous_result = [{"previous": "result"}]
    # Should return previous_result when max retries exceeded (preserves chain behavior)
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == previous_result
    assert upload.state == "started"  # State should not change


def test_bundle_analysis_processor_task_safe_retry_fails(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """Test that when retries are below max, task raises Retry (not MaxRetriesExceededError).

    This test verifies that tasks below max retries will retry normally.
    Note: safe_retry() no longer exists - this tests the new retry behavior.
    """
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )
    mock_redis.lock.return_value.__enter__.side_effect = LockError()
    mock_redis.incr.return_value = (
        1  # LockManager: attempts < max so it raises LockRetry(countdown)
    )

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    task.request.retries = 0  # Below max_retries, so should retry
    task.request.headers = {}

    previous_result = [{"previous": "result"}]
    # Should raise Retry when retries are below max (not MaxRetriesExceededError)
    with pytest.raises(Retry):
        task.run_impl(
            dbsession,
            previous_result,
            repoid=commit.repoid,
            commitid=commit.commitid,
            commit_yaml={},
            params={
                "upload_id": upload.id_,
                "commit": commit.commitid,
            },
        )

    assert upload.state == "started"  # State should not change


def test_bundle_analysis_processor_task_max_retries_exceeded_processing(
    mocker,
    dbsession,
    mock_storage,
):
    """Test that when max retries are exceeded during processing with retryable error, upload is set to error."""
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    task.request.retries = task.max_retries
    task.request.headers = {"attempts": task.max_retries}  # >= max_retries → exceeded
    mocker.patch.object(task, "_has_exceeded_max_attempts", return_value=True)

    # Create a ProcessingResult with a retryable error
    retryable_error = ProcessingError(
        code="rate_limit_error",
        params={"location": storage_path},
        is_retryable=True,
    )
    processing_result = ProcessingResult(
        upload=upload,
        commit=commit,
        error=retryable_error,
    )

    process_upload = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService.process_upload"
    )
    process_upload.return_value = processing_result

    previous_result = [{"previous": "result"}]
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    # Should return previous_result (not append new result)
    assert result == previous_result
    # Upload should be set to error state
    dbsession.refresh(upload)
    assert upload.state == "error"
    assert commit.state == "error"


def test_bundle_analysis_processor_task_max_retries_exceeded_visibility_timeout(
    mocker,
    dbsession,
    mock_storage,
    mock_redis,
):
    """Test that task stops retrying when max attempts exceeded due to visibility timeout re-deliveries.

    This test verifies the fix for the bug where tasks would continue retrying after max retries
    when visibility timeout caused re-deliveries. The issue was that self.request.retries doesn't
    account for visibility timeout re-deliveries, but self.attempts does.

    Scenario:
    - self.request.retries = 5 (below max_retries of 10)
    - self.attempts = 11 (exceeds max_retries 10 due to visibility timeout re-deliveries)
    - Task should stop retrying and return previous_result
    """
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )
    # Mock Redis to simulate lock failure - this will cause LockManager to raise LockRetry
    mock_redis.lock.return_value.__enter__.side_effect = LockError()
    # LockManager gets attempt count from Redis; use 1 so it raises LockRetry(max_retries_exceeded=False).
    # Task then uses self.attempts (from headers) and returns because attempts >= max_retries.
    mock_redis.incr.return_value = 1

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    # Simulate visibility timeout re-delivery scenario:
    # - request.retries is low (5) because intentional retries haven't exceeded max
    # - attempts header is high (11) due to visibility timeout re-deliveries
    # This simulates the bug where tasks kept retrying after max attempts
    task.request.retries = 5  # Below max_retries (10)
    task.request.headers = {"attempts": 11}  # Exceeds max_retries 10 (11 >= 10)

    previous_result = [{"previous": "result"}]
    # Task should return previous_result when max attempts exceeded (via attempts header)
    # even though request.retries hasn't exceeded max_retries
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )
    assert result == previous_result
    assert upload.state == "started"  # State should not change


def test_bundle_analysis_processor_task_max_retries_exceeded_commit_failure(
    mocker,
    dbsession,
    mock_storage,
):
    """Test that when max attempts are exceeded and commit fails, fallback error handling works."""
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    task.request.retries = task.max_retries
    # Exceed when attempts >= max_retries
    task.request.headers = {"attempts": task.max_retries}
    mocker.patch.object(task, "_has_exceeded_max_attempts", return_value=True)

    # Create a ProcessingResult with a retryable error
    retryable_error = ProcessingError(
        code="rate_limit_error",
        params={"location": storage_path},
        is_retryable=True,
    )
    processing_result = ProcessingResult(
        upload=upload,
        commit=commit,
        error=retryable_error,
    )

    process_upload = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService.process_upload"
    )
    process_upload.return_value = processing_result

    # Mock commit to fail first time, succeed second time (fallback)
    commit_mock = mocker.patch.object(dbsession, "commit")
    commit_mock.side_effect = [
        Exception("Commit failed"),  # First commit fails
        None,  # Fallback commit succeeds
    ]

    previous_result = [{"previous": "result"}]
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    # Should return previous_result
    assert result == previous_result
    # Upload should be set to error state via fallback
    dbsession.refresh(upload)
    assert upload.state == "error"


def test_bundle_analysis_processor_task_general_error_commit_failure(
    mocker,
    dbsession,
    mock_storage,
):
    """Test that when general exception occurs and commit fails, task returns (does not re-raise) and sets upload to error."""
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    process_upload = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService.process_upload"
    )
    process_upload.side_effect = ValueError("Processing failed")

    commit = CommitFactory.create()
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(
        state="started",
        storage_path=storage_path,
        report=commit_report,
    )
    dbsession.add(upload)
    dbsession.flush()

    task = BundleAnalysisProcessorTask()
    retry = mocker.patch.object(task, "retry")

    # Mock commit to fail so we exercise the "set upload error and commit" path
    mocker.patch.object(dbsession, "commit", side_effect=Exception("Commit failed"))

    previous_result = [{"previous": "result"}]
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    # Task returns processing_results (not re-raises) to ack and avoid unbounded retries
    assert result == previous_result
    assert upload.state == "error"
    assert not retry.called


def test_bundle_analysis_processor_task_cleanup_with_none_result(
    mocker,
    dbsession,
    mock_storage,
):
    """Test that cleanup handles None result gracefully when process_upload raises."""
    storage_path = (
        "v1/repos/testing/ed1bdd67-8fd2-4cdb-ac9e-39b99e4a3892/bundle_report.sqlite"
    )
    mock_storage.write_file(get_bucket_name(), storage_path, "test-content")

    mocker.patch.object(
        BundleAnalysisProcessorTask,
        "app",
        tasks={
            bundle_analysis_save_measurements_task_name: mocker.MagicMock(),
        },
    )

    commit = CommitFactory.create(state="pending")
    dbsession.add(commit)
    dbsession.flush()

    commit_report = CommitReport(commit_id=commit.id_)
    dbsession.add(commit_report)
    dbsession.flush()

    upload = UploadFactory.create(storage_path=storage_path, report=commit_report)
    dbsession.add(upload)
    dbsession.flush()

    # Mock process_upload to raise exception before result is set
    process_upload = mocker.patch(
        "services.bundle_analysis.report.BundleAnalysisReportService.process_upload"
    )
    process_upload.side_effect = ValueError("Processing failed")

    task = BundleAnalysisProcessorTask()

    # Task returns (does not re-raise) to ack and avoid unbounded retries; cleanup runs in finally
    previous_result = [{"previous": "result"}]
    result = task.run_impl(
        dbsession,
        previous_result,
        repoid=commit.repoid,
        commitid=commit.commitid,
        commit_yaml={},
        params={
            "upload_id": upload.id_,
            "commit": commit.commitid,
        },
    )

    assert result == previous_result
    assert upload.state == "error"
