from unittest.mock import patch

import pytest
from shared.api_archive.archive import ArchiveService
from shared.bundle_analysis import BundleAnalysisReport as SharedBundleAnalysisReport
from shared.bundle_analysis import (
    BundleAnalysisReportLoader,
    BundleChange,
    StoragePaths,
)
from shared.bundle_analysis.storage import get_bucket_name
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory

from reports.models import CommitReport
from reports.tests.factories import CommitReportFactory
from services.bundle_analysis import (
    BundleAnalysisComparison,
    BundleAnalysisReport,
    BundleComparison,
    BundleReport,
    load_report,
)


@pytest.mark.django_db
def test_load_report(mock_storage):
    repo = RepositoryFactory()
    commit = CommitFactory(repository=repo)

    # no commit report record
    assert load_report(commit) is None

    commit_report = CommitReportFactory(
        commit=commit, report_type=CommitReport.ReportType.BUNDLE_ANALYSIS
    )

    storage_path = StoragePaths.bundle_report.path(
        repo_key=ArchiveService.get_archive_hash(repo),
        report_key=commit_report.external_id,
    )

    # nothing in storage
    assert load_report(commit) is None

    with open("./services/tests/samples/bundle_report.sqlite", "rb") as f:
        mock_storage.write_file(get_bucket_name(), storage_path, f)

    report = load_report(commit)
    assert report is not None
    assert isinstance(report, SharedBundleAnalysisReport)


@patch("services.bundle_analysis.SharedBundleChange")
def test_bundle_comparison(mock_shared_bundle_change):
    mock_shared_bundle_change = BundleChange(
        bundle_name="bundle1",
        change_type=BundleChange.ChangeType.ADDED,
        size_delta=1000000,
        percentage_delta=0.0,
    )

    bundle_comparison = BundleComparison(
        mock_shared_bundle_change,
        7654321,
    )

    assert bundle_comparison.bundle_name == "bundle1"
    assert bundle_comparison.change_type == "added"
    assert bundle_comparison.size_delta == 1000000
    assert bundle_comparison.size_total == 7654321


@pytest.mark.django_db
def test_bundle_analysis_comparison(mock_storage):
    repo = RepositoryFactory()

    base_commit = CommitFactory(repository=repo)
    base_commit_report = CommitReportFactory(
        commit=base_commit, report_type=CommitReport.ReportType.BUNDLE_ANALYSIS
    )

    head_commit = CommitFactory(repository=repo)
    head_commit_report = CommitReportFactory(
        commit=head_commit, report_type=CommitReport.ReportType.BUNDLE_ANALYSIS
    )

    with open("./services/tests/samples/base_bundle_report.sqlite", "rb") as f:
        storage_path = StoragePaths.bundle_report.path(
            repo_key=ArchiveService.get_archive_hash(repo),
            report_key=base_commit_report.external_id,
        )
        mock_storage.write_file(get_bucket_name(), storage_path, f)

    with open("./services/tests/samples/head_bundle_report.sqlite", "rb") as f:
        storage_path = StoragePaths.bundle_report.path(
            repo_key=ArchiveService.get_archive_hash(repo),
            report_key=head_commit_report.external_id,
        )
        mock_storage.write_file(get_bucket_name(), storage_path, f)

    loader = BundleAnalysisReportLoader(head_commit.repository)

    bac = BundleAnalysisComparison(
        loader, base_commit_report.external_id, head_commit_report.external_id, repo
    )

    assert len(bac.bundles) == 5
    assert bac.size_delta == 36555
    assert bac.size_total == 201720


def test_bundle_report():
    class MockSharedBundleReport:
        def __init__(self, db_path, bundle_name):
            self.bundle_name = bundle_name

        def total_size(self):
            return 7654321

        @property
        def name(self):
            return self.bundle_name

    bundle_comparison = BundleReport(MockSharedBundleReport("123abc", "bundle1"))

    assert bundle_comparison.name == "bundle1"
    assert bundle_comparison.size_total == 7654321


@pytest.mark.django_db
def test_bundle_analysis_report(mock_storage):
    repo = RepositoryFactory()

    commit = CommitFactory(repository=repo)
    commit_report = CommitReportFactory(
        commit=commit, report_type=CommitReport.ReportType.BUNDLE_ANALYSIS
    )

    with open("./services/tests/samples/head_bundle_report.sqlite", "rb") as f:
        storage_path = StoragePaths.bundle_report.path(
            repo_key=ArchiveService.get_archive_hash(repo),
            report_key=commit_report.external_id,
        )
        mock_storage.write_file(get_bucket_name(), storage_path, f)

    loader = BundleAnalysisReportLoader(commit.repository)

    bar = BundleAnalysisReport(loader.load(commit_report.external_id))

    assert len(bar.bundles) == 4
    assert bar.size_total == 201720
