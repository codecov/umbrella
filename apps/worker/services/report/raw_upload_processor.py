import logging

import sentry_sdk

from helpers.exceptions import ReportEmptyError, ReportExpiredException
from services.path_fixer import PathFixer
from services.report.parser.types import ParsedRawReport
from services.report.report_builder import ReportBuilder
from services.report.report_processor import process_report
from shared.reports.resources import Report
from shared.utils.sessions import Session

log = logging.getLogger(__name__)


@sentry_sdk.trace
def process_raw_upload(
    commit_yaml,
    raw_reports: ParsedRawReport,
    session: Session,
) -> Report:
    # ----------------------
    # Extract `git ls-files`
    # ----------------------
    toc = []
    if raw_reports.has_toc():
        toc = raw_reports.get_toc()

    if raw_reports.has_env():
        env = raw_reports.get_env()
        session.env = dict([e.split("=", 1) for e in env.split("\n") if "=" in e])

    path_fixer = PathFixer.init_from_user_yaml(
        commit_yaml=commit_yaml, toc=toc, flags=session.flags
    )

    # ------------------
    # Extract bash fixes
    # ------------------
    ignored_lines = {}
    if raw_reports.has_report_fixes():
        ignored_lines = raw_reports.get_report_fixes(path_fixer)

    # [javascript] check for both coverage.json and coverage/coverage.lcov
    skip_files = set()
    for report_file in raw_reports.get_uploaded_files():
        if report_file.filename == "coverage/coverage.json":
            skip_files.add("coverage/coverage.lcov")

    report = Report()
    sessionid = session.id = report.next_session_number()

    # ---------------
    # Process reports
    # ---------------
    for report_file in raw_reports.get_uploaded_files():
        current_filename = report_file.filename
        if current_filename in skip_files or not report_file.contents:
            continue

        path_fixer_to_use = path_fixer.get_relative_path_aware_pathfixer(
            current_filename
        )
        report_builder_to_use = ReportBuilder(
            commit_yaml, sessionid, ignored_lines, path_fixer_to_use
        )

        try:
            report_from_file = process_report(
                report=report_file, report_builder=report_builder_to_use
            )
        except ReportExpiredException as r:
            r.filename = current_filename
            raise

        if not report_from_file:
            continue
        if report.is_empty():
            # if the initial report is empty, we can avoid a costly merge operation
            report = report_from_file
        else:
            # merging the smaller report into the larger one is faster,
            # so swap the two reports in that case.
            if len(report_from_file._files) > len(report._files):
                report_from_file, report = report, report_from_file

            report.merge(report_from_file)

    if not report:
        raise ReportEmptyError("No files found in report.")

    _sessionid, session = report.add_session(session, use_id_from_session=True)
    session.totals = report.totals

    return report
