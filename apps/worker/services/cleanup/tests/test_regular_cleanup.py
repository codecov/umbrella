import pytest

from services.cleanup.regular import run_regular_cleanup
from services.cleanup.utils import CleanupResult, CleanupSummary
from shared.api_archive.archive import ArchiveService
from shared.django_apps.core.models import Pull, PullStates
from shared.django_apps.core.tests.factories import (
    CommitFactory,
    PullFactory,
    RepositoryFactory,
)
from shared.django_apps.reports.models import CommitReport
from shared.django_apps.reports.models import ReportSession as Upload
from shared.django_apps.reports.tests.factories import (
    CommitReportFactory,
    UploadFactory,
)
from shared.django_apps.staticanalysis.models import StaticAnalysisSingleFileSnapshot
from shared.django_apps.staticanalysis.tests.factories import (
    StaticAnalysisSingleFileSnapshotFactory,
)
from shared.storage.exceptions import FileNotInStorageError


@pytest.mark.django_db
def test_runs_regular_cleanup(mock_storage):
    repo = RepositoryFactory()
    archive_service = ArchiveService(repo)

    filesnapshot = StaticAnalysisSingleFileSnapshotFactory()
    archive_service.write_file(filesnapshot.content_location, "some content")

    commit = CommitFactory(repository=repo)

    commit_report = CommitReportFactory(
        commit=commit, report_type=CommitReport.ReportType.COVERAGE.value
    )
    upload = UploadFactory(report=commit_report, storage_path="regular-upload")

    archive_service.write_chunks(commit.commitid, "regular-upload-chunks_data")
    archive_service.write_file(upload.storage_path, "regular-upload_data")

    commit_report = CommitReportFactory(
        commit=commit,
        report_type=CommitReport.ReportType.COVERAGE.value,
        code="local-upload",
    )
    upload = UploadFactory(report=commit_report, storage_path="upload")

    archive_service.write_chunks(
        commit.commitid, "local-upload-chunks_data", report_code="local-upload"
    )
    archive_service.write_file(upload.storage_path, "local-upload_data")

    archive = mock_storage.storage["archive"]

    assert len(archive) == 5

    summary = run_regular_cleanup()

    assert summary == CleanupSummary(
        CleanupResult(3, 3),
        {
            Upload: CleanupResult(1, 1),
            StaticAnalysisSingleFileSnapshot: CleanupResult(1, 1),
            CommitReport: CleanupResult(1, 1),
        },
    )
    assert len(archive) == 2


@pytest.mark.django_db
def test_flare_cleanup_in_regular_cleanup(
    transactional_db, mocker, mock_archive_storage
):
    mock_logs = mocker.patch("logging.Logger.info")
    # Directly mock the storage service's delete_file method instead of the base class
    mock_delete = mocker.patch.object(
        mock_archive_storage, "delete_file", return_value=True
    )

    archive_value_for_flare = {"some": "data"}
    local_value_for_flare = {"test": "test"}

    # Create an open pull with local flare
    open_pull_with_local_flare = PullFactory(
        state=PullStates.OPEN.value,
        _flare=local_value_for_flare,
        repository=RepositoryFactory(),
    )
    # Verify initial state
    assert open_pull_with_local_flare.flare == local_value_for_flare
    assert open_pull_with_local_flare._flare == local_value_for_flare
    assert open_pull_with_local_flare._flare_storage_path is None

    # Create a closed pull with local flare
    closed_pull_with_local_flare = PullFactory(
        state=PullStates.CLOSED.value,
        _flare=local_value_for_flare,
        repository=RepositoryFactory(),
    )
    # Verify initial state
    assert closed_pull_with_local_flare.flare == local_value_for_flare
    assert closed_pull_with_local_flare._flare == local_value_for_flare
    assert closed_pull_with_local_flare._flare_storage_path is None

    # Create an open pull with archive flare
    open_pull_with_archive_flare = PullFactory(
        state=PullStates.OPEN.value,
        _flare=None,
        repository=RepositoryFactory(),
    )
    open_pull_with_archive_flare.flare = archive_value_for_flare
    open_pull_with_archive_flare.save()
    open_pull_with_archive_flare.refresh_from_db()
    # Verify initial state
    assert open_pull_with_archive_flare.flare == archive_value_for_flare
    assert open_pull_with_archive_flare._flare is None
    assert open_pull_with_archive_flare._flare_storage_path is not None

    # Create a merged pull with archive flare
    merged_pull_with_archive_flare = PullFactory(
        state=PullStates.MERGED.value,
        _flare=None,
        repository=RepositoryFactory(),
    )
    merged_pull_with_archive_flare.flare = archive_value_for_flare
    merged_pull_with_archive_flare.save()
    merged_pull_with_archive_flare.refresh_from_db()
    # Verify initial state
    assert merged_pull_with_archive_flare.flare == archive_value_for_flare
    assert merged_pull_with_archive_flare._flare is None
    assert merged_pull_with_archive_flare._flare_storage_path is not None

    # Run the regular cleanup, which should now include flare cleanup
    run_regular_cleanup()

    # Verify flare cleanup logs were included
    mock_logs.assert_any_call("Starting flare cleanup")
    mock_logs.assert_any_call("Flare cleanup cleared 1 database flares")
    # Use the new log message format
    mock_logs.assert_any_call(
        "Flare cleanup: processed 1 archive flares, 1 successfully deleted"
    )

    # Verify the delete_file was called for the merged pull's flare
    storage_path = merged_pull_with_archive_flare._flare_storage_path
    mock_delete.assert_called_with(mocker.ANY, storage_path)

    # There is a cache for flare on the object (all ArchiveFields have this),
    # so get a fresh copy of each object without the cached value
    open_pull_with_local_flare = Pull.objects.get(id=open_pull_with_local_flare.id)
    closed_pull_with_local_flare = Pull.objects.get(id=closed_pull_with_local_flare.id)
    open_pull_with_archive_flare = Pull.objects.get(id=open_pull_with_archive_flare.id)
    merged_pull_with_archive_flare = Pull.objects.get(
        id=merged_pull_with_archive_flare.id
    )

    # Check that the open pulls still have their flare data
    assert open_pull_with_local_flare.flare == local_value_for_flare
    assert open_pull_with_local_flare._flare == local_value_for_flare
    assert open_pull_with_local_flare._flare_storage_path is None

    assert open_pull_with_archive_flare.flare == archive_value_for_flare
    assert open_pull_with_archive_flare._flare is None
    assert open_pull_with_archive_flare._flare_storage_path is not None

    # Check that the non-open pulls have had their flare data cleaned
    assert closed_pull_with_local_flare.flare == {}
    assert closed_pull_with_local_flare._flare is None
    assert closed_pull_with_local_flare._flare_storage_path is None

    assert merged_pull_with_archive_flare.flare == {}
    assert merged_pull_with_archive_flare._flare is None
    assert merged_pull_with_archive_flare._flare_storage_path is None

    # Verify that running cleanup again doesn't process the already cleaned pulls
    mock_logs.reset_mock()
    mock_delete.reset_mock()

    # Run cleanup again
    run_regular_cleanup()

    # Verify the logs show no flares were cleaned
    mock_logs.assert_any_call("Flare cleanup cleared 0 database flares")
    mock_logs.assert_any_call(
        "Flare cleanup: processed 0 archive flares, 0 successfully deleted"
    )

    # Verify no delete calls were made for already processed pulls
    mock_delete.assert_not_called()


@pytest.mark.django_db
def test_flare_cleanup_failed_deletion(transactional_db, mocker, mock_archive_storage):
    """Test that when file deletion fails, the _flare_storage_path remains intact."""
    # Directly mock the storage service's delete_file method to return False (failed deletion)
    mock_delete = mocker.patch.object(
        mock_archive_storage, "delete_file", return_value=False
    )

    archive_value_for_flare = {"some": "data"}

    # Create a merged pull with archive flare
    failed_deletion_pull = PullFactory(
        state=PullStates.MERGED.value,
        _flare=None,
        repository=RepositoryFactory(),
    )
    failed_deletion_pull.flare = archive_value_for_flare
    failed_deletion_pull.save()
    failed_deletion_pull.refresh_from_db()
    original_storage_path = failed_deletion_pull._flare_storage_path

    # Verify initial state
    assert failed_deletion_pull.flare == archive_value_for_flare
    assert failed_deletion_pull._flare is None
    assert failed_deletion_pull._flare_storage_path is not None

    # Run cleanup
    run_regular_cleanup()

    # Verify the delete_file was called
    storage_path = failed_deletion_pull._flare_storage_path
    mock_delete.assert_called_with(mocker.ANY, storage_path)

    # Refresh and verify that the storage path was NOT cleared since deletion failed
    failed_deletion_pull = Pull.objects.get(id=failed_deletion_pull.id)
    assert failed_deletion_pull._flare_storage_path == original_storage_path


@pytest.mark.django_db
def test_flare_cleanup_file_not_in_storage(
    transactional_db, mocker, mock_archive_storage
):
    """Test that FileNotInStorageError is treated as a successful deletion."""
    # Mock delete_file to raise FileNotInStorageError
    mock_delete = mocker.patch.object(
        mock_archive_storage,
        "delete_file",
        side_effect=FileNotInStorageError("File not found in storage"),
    )

    archive_value_for_flare = {"some": "data"}

    # Create a merged pull with archive flare
    file_not_in_storage_pull = PullFactory(
        state=PullStates.MERGED.value,
        _flare=None,
        repository=RepositoryFactory(),
    )
    file_not_in_storage_pull.flare = archive_value_for_flare
    file_not_in_storage_pull.save()
    file_not_in_storage_pull.refresh_from_db()

    # Verify initial state
    assert file_not_in_storage_pull.flare == archive_value_for_flare
    assert file_not_in_storage_pull._flare is None
    assert file_not_in_storage_pull._flare_storage_path is not None

    # Run cleanup
    run_regular_cleanup()

    # Verify the delete_file was called
    storage_path = file_not_in_storage_pull._flare_storage_path
    mock_delete.assert_called_with(mocker.ANY, storage_path)

    # Refresh and verify that the storage path WAS cleared since FileNotInStorageError is treated as success
    file_not_in_storage_pull = Pull.objects.get(id=file_not_in_storage_pull.id)
    assert file_not_in_storage_pull.flare == {}
    assert file_not_in_storage_pull._flare is None
    assert file_not_in_storage_pull._flare_storage_path is None


@pytest.mark.django_db
def test_flare_cleanup_general_exception(
    transactional_db, mocker, mock_archive_storage
):
    """Test that any other exception is captured by Sentry and the file isn't marked as deleted."""
    # Mock delete_file to raise a general exception
    mock_delete = mocker.patch.object(
        mock_archive_storage,
        "delete_file",
        side_effect=Exception("Something went wrong"),
    )

    # Mock sentry_sdk.capture_exception
    mock_sentry_capture = mocker.patch("sentry_sdk.capture_exception")

    # Mock logger.error
    mock_log_error = mocker.patch("logging.Logger.error")

    archive_value_for_flare = {"some": "data"}

    # Create a merged pull with archive flare
    exception_pull = PullFactory(
        state=PullStates.MERGED.value,
        _flare=None,
        repository=RepositoryFactory(),
    )
    exception_pull.flare = archive_value_for_flare
    exception_pull.save()
    exception_pull.refresh_from_db()
    original_storage_path = exception_pull._flare_storage_path

    # Verify initial state
    assert exception_pull.flare == archive_value_for_flare
    assert exception_pull._flare is None
    assert exception_pull._flare_storage_path is not None

    # Run cleanup
    run_regular_cleanup()

    # Verify the delete_file was called
    storage_path = exception_pull._flare_storage_path
    mock_delete.assert_called_with(mocker.ANY, storage_path)

    # Verify that sentry_sdk.capture_exception was called
    mock_sentry_capture.assert_called_once()

    # Refresh and verify that the storage path was NOT cleared since an exception occurred
    exception_pull = Pull.objects.get(id=exception_pull.id)
    assert exception_pull._flare_storage_path == original_storage_path
