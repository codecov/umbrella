import pytest

from services.cleanup.regular import run_regular_cleanup
from services.cleanup.utils import CleanupResult, CleanupSummary
from shared.api_archive.archive import ArchiveService
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
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
