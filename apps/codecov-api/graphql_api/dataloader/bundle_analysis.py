from typing import Union

from core.models import Commit
from graphql_api.types.comparison.comparison import MissingBaseReport, MissingHeadReport
from reports.models import CommitReport
from services.bundle_analysis import BundleAnalysisComparison, BundleAnalysisReport
from shared.bundle_analysis import (
    BundleAnalysisReportLoader,
    MissingBaseReportError,
    MissingHeadReportError,
)


def load_bundle_analysis_comparison(
    base_commit: Commit, head_commit: Commit
) -> Union[BundleAnalysisComparison, MissingHeadReport, MissingBaseReport]:
    head_report = CommitReport.objects.filter(
        report_type=CommitReport.ReportType.BUNDLE_ANALYSIS, commit=head_commit
    ).first()
    if head_report is None:
        return MissingHeadReport()

    base_report = CommitReport.objects.filter(
        report_type=CommitReport.ReportType.BUNDLE_ANALYSIS, commit=base_commit
    ).first()
    if base_report is None:
        return MissingBaseReport()

    loader = BundleAnalysisReportLoader(head_commit.repository)

    try:
        return BundleAnalysisComparison(
            loader=loader,
            base_report_key=base_report.external_id,
            head_report_key=head_report.external_id,
            repository=head_commit.repository,
        )
    except MissingBaseReportError:
        return MissingBaseReport()
    except MissingHeadReportError:
        return MissingHeadReport()


def load_bundle_analysis_report(
    commit: Commit,
) -> Union[BundleAnalysisReport, MissingHeadReport, MissingBaseReport]:
    report = CommitReport.objects.filter(
        report_type=CommitReport.ReportType.BUNDLE_ANALYSIS, commit=commit
    ).first()
    if report is None:
        return MissingHeadReport()

    loader = BundleAnalysisReportLoader(commit.repository)
    report = loader.load(report.external_id)
    if report is None:
        return MissingHeadReport()

    return BundleAnalysisReport(report)
