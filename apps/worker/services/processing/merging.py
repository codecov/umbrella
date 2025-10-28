import functools
import logging
from copy import copy
from decimal import Decimal

import orjson
import sentry_sdk
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session as DbSession

from database.models.reports import Upload, UploadError, UploadLevelTotals
from helpers.number import precise_round
from services.report import delete_uploads_by_sessionid
from services.yaml.reader import read_yaml_field
from shared.reports.enums import UploadState
from shared.reports.resources import Report, ReportTotals
from shared.utils.sessions import SessionType
from shared.yaml import UserYaml

from .types import IntermediateReport, MergeResult, ProcessingResult

log = logging.getLogger(__name__)


def _compare_reports(original_report: Report, optimized_report: Report):
    """
    Compare two reports to ensure they produce identical coverage data.
    Raises an exception if the reports differ.
    """
    # Compare sessions
    if set(original_report.sessions.keys()) != set(optimized_report.sessions.keys()):
        raise ValueError(
            f"Shadow mode validation failed: Session IDs differ. "
            f"Original: {set(original_report.sessions.keys())}, "
            f"Optimized: {set(optimized_report.sessions.keys())}"
        )

    # Compare files
    original_files = set(original_report.files)
    optimized_files = set(optimized_report.files)
    if original_files != optimized_files:
        raise ValueError(
            f"Shadow mode validation failed: File lists differ. "
            f"Missing in optimized: {original_files - optimized_files}, "
            f"Extra in optimized: {optimized_files - original_files}"
        )

    # Compare totals
    original_totals = original_report.totals
    optimized_totals = optimized_report.totals
    if original_totals.astuple() != optimized_totals.astuple():
        raise ValueError(
            f"Shadow mode validation failed: Report totals differ. "
            f"Original: {original_totals.asdict()}, "
            f"Optimized: {optimized_totals.asdict()}"
        )

    # Compare each file in detail
    for filename in original_files:
        original_file = original_report.get(filename)
        optimized_file = optimized_report.get(filename)

        # Compare file totals
        if original_file.totals.astuple() != optimized_file.totals.astuple():
            raise ValueError(
                f"Shadow mode validation failed: File totals differ for '{filename}'. "
                f"Original: {original_file.totals.asdict()}, "
                f"Optimized: {optimized_file.totals.asdict()}"
            )

        # Compare line-by-line coverage
        original_lines = dict(original_file.lines)
        optimized_lines = dict(optimized_file.lines)

        if set(original_lines.keys()) != set(optimized_lines.keys()):
            raise ValueError(
                f"Shadow mode validation failed: Line numbers differ for '{filename}'. "
                f"Original has {len(original_lines)} lines, optimized has {len(optimized_lines)} lines"
            )

        line_nums = original_lines.keys()
        for line_num in line_nums:
            original_line = original_lines[line_num]
            optimized_line = optimized_lines[line_num]

            # Compare coverage data for each line
            if str(original_line) != str(optimized_line):
                raise ValueError(
                    f"Shadow mode validation failed: Line coverage differs for '{filename}' line {line_num}. "
                    f"Original: {original_line}, "
                    f"Optimized: {optimized_line}"
                )

    log.info("merge_reports: Shadow mode validation passed - reports are identical!")


@sentry_sdk.trace
def merge_reports(
    commit_yaml: UserYaml,
    master_report: Report,
    intermediate_reports: list[IntermediateReport],
) -> tuple[Report, MergeResult]:
    session_mapping: dict[int, int] = {}

    log.info("merge_reports: Clearing all carryforward sessions")
    deleted_sessions = clear_all_carryforward_sessions(
        commit_yaml, master_report, intermediate_reports
    )

    # Save the initial state for shadow mode testing
    log.info("merge_reports: Saving initial state for shadow mode testing")

    # Save master_report state
    if not master_report.is_empty():
        initial_report_json, initial_chunks, initial_totals = master_report.serialize()
        initial_report_data = orjson.loads(initial_report_json)
        # Need to save initial sessions separately since they'll be modified
        initial_sessions = {
            sid: copy(session) for sid, session in master_report.sessions.items()
        }
    else:
        initial_report_json = None
        initial_chunks = None
        initial_totals = None
        initial_report_data = None
        initial_sessions = {}

    # Save copies of all intermediate reports BEFORE they get modified
    saved_intermediate_reports = []
    for intermediate_report in intermediate_reports:
        if not intermediate_report.report.is_empty():
            report_json, chunks, totals = intermediate_report.report.serialize()
            saved_intermediate_reports.append(
                {
                    "upload_id": intermediate_report.upload_id,
                    "report_json": report_json,
                    "chunks": chunks,
                    "totals": totals,
                    "report_data": orjson.loads(report_json),
                    "sessions": {
                        sid: copy(session)
                        for sid, session in intermediate_report.report.sessions.items()
                    },
                }
            )
        else:
            saved_intermediate_reports.append(
                {
                    "upload_id": intermediate_report.upload_id,
                    "report_json": None,
                    "chunks": None,
                    "totals": None,
                    "report_data": None,
                    "sessions": {},
                }
            )

    # ============================================================================
    # ORIGINAL PATH: Using is_disjoint=False (current production behavior)
    # ============================================================================
    log.info("merge_reports: Merging intermediate reports (original path)")
    for intermediate_report in intermediate_reports:
        report = intermediate_report.report
        if report.is_empty():
            log.info("merge_reports: Empty report, skipping")
            continue

        old_sessionid = next(iter(report.sessions))
        new_sessionid = master_report.next_session_number()
        session_mapping[intermediate_report.upload_id] = new_sessionid

        log.info(
            "merge_reports: Merging report",
            extra={"old_sessionid": old_sessionid, "new_sessionid": new_sessionid},
        )

        if master_report.is_empty() and old_sessionid == new_sessionid:
            log.info(
                "merge_reports: Master report is empty, skipping merge",
                extra={"old_sessionid": old_sessionid, "new_sessionid": new_sessionid},
            )
            # if the master report is empty, we can avoid a costly merge operation
            master_report = report
            continue

        log.info(
            "merge_reports: Changing sessionid",
            extra={"old_sessionid": old_sessionid, "new_sessionid": new_sessionid},
        )

        report.change_sessionid(old_sessionid, new_sessionid)
        session = report.sessions[new_sessionid]

        log.info(
            "merge_reports: Adding session",
            extra={"session_id": session.id},
        )

        _session_id, session = master_report.add_session(
            session, use_id_from_session=True
        )

        joined = get_joined_flag(commit_yaml, session.flags or [])
        log.info(
            "merge_reports: Merging report",
            extra={"joined": joined},
        )
        master_report.merge(report, joined, is_disjoint=False)

    # ============================================================================
    # OPTIMIZED PATH: Using is_disjoint=True (shadow mode testing)
    # ============================================================================
    log.info("merge_reports: Starting shadow mode testing with is_disjoint=True")

    # Recreate the initial state
    if initial_report_data is not None:
        optimized_report = Report(
            files=initial_report_data.get("files"),
            sessions=initial_sessions,
            totals=initial_totals,
            chunks=initial_chunks.decode(),
        )
    else:
        optimized_report = Report()

    # Re-run the merge with optimized path using saved copies
    for saved_report in saved_intermediate_reports:
        # Recreate the report from saved state
        if saved_report["report_data"] is None:
            continue

        report_copy = Report(
            files=saved_report["report_data"].get("files"),
            sessions=saved_report["sessions"],
            totals=saved_report["totals"],
            chunks=saved_report["chunks"].decode(),
        )

        old_sessionid = next(iter(report_copy.sessions))
        new_sessionid = session_mapping[saved_report["upload_id"]]

        if optimized_report.is_empty() and old_sessionid == new_sessionid:
            optimized_report = report_copy
            continue

        report_copy.change_sessionid(old_sessionid, new_sessionid)
        session_copy = report_copy.sessions[new_sessionid]
        optimized_report.add_session(session_copy, use_id_from_session=True)

        joined = get_joined_flag(commit_yaml, session_copy.flags or [])
        optimized_report.merge(report_copy, joined, is_disjoint=True)

    log.info("merge_reports: Finishing merge of disjoint records (optimized path)")
    optimized_report.finish_merge()

    # ============================================================================
    # COMPARISON: Validate that both paths produce the same result
    # ============================================================================
    log.info("merge_reports: Comparing original and optimized reports")
    _compare_reports(master_report, optimized_report)

    log.info("merge_reports: Returning merge result")
    return master_report, MergeResult(session_mapping, deleted_sessions)


@sentry_sdk.trace
def update_uploads(
    db_session: DbSession,
    commit_yaml: UserYaml,
    processing_results: list[ProcessingResult],
    intermediate_reports: list[IntermediateReport],
    merge_result: MergeResult,
):
    """
    Updates all the `Upload` records with the `MergeResult`.
    In particular, this updates the `order_number` to match the new `session_id`,
    and it deletes all the `Upload` records matching removed carry-forwarded `Session`s.
    """

    # first, delete removed sessions, as report merging can reuse deleted `session_id`s.
    if merge_result.deleted_sessions:
        any_upload_id = next(iter(merge_result.session_mapping.keys()))
        report_id = (
            db_session.query(Upload.report_id)
            .filter(Upload.id_ == any_upload_id)
            .first()[0]
        )

        delete_uploads_by_sessionid(
            db_session, report_id, merge_result.deleted_sessions
        )

    precision: int = read_yaml_field(commit_yaml, ("coverage", "precision"), 2)
    rounding: str = read_yaml_field(commit_yaml, ("coverage", "round"), "nearest")
    make_totals = functools.partial(make_upload_totals, precision, rounding)

    reports = {ir.upload_id: ir.report for ir in intermediate_reports}

    # then, update all the `Upload`s with their state, and the final `order_number`,
    # as well as add a `UploadLevelTotals` or `UploadError`s where appropriate.
    all_errors: list[UploadError] = []
    all_totals: list[dict] = []
    all_upload_updates: list[dict] = []
    for result in processing_results:
        upload_id = result["upload_id"]

        if result["successful"]:
            update = {
                "state_id": UploadState.PROCESSED.db_id,
                "state": "processed",
            }
            report = reports.get(upload_id)
            if report is not None:
                all_totals.append(make_totals(upload_id, report.totals))
        elif result["error"]:
            update = {
                "state_id": UploadState.ERROR.db_id,
                "state": "error",
            }
            error = UploadError(
                upload_id=upload_id,
                error_code=result["error"]["code"],
                error_params=result["error"]["params"],
            )
            all_errors.append(error)

        update["id_"] = upload_id
        order_number = merge_result.session_mapping.get(upload_id)
        update["order_number"] = order_number
        all_upload_updates.append(update)

    db_session.bulk_update_mappings(Upload, all_upload_updates)
    db_session.bulk_save_objects(all_errors)

    if all_totals:
        # the `UploadLevelTotals` have a unique constraint for the `upload`,
        # so we have to use a manual `insert` statement:
        stmt = (
            insert(UploadLevelTotals.__table__)
            .values(all_totals)
            .on_conflict_do_nothing()
        )
        db_session.execute(stmt)

    db_session.flush()


# TODO(swatinem): we should eventually remove `UploadLevelTotals` completely
def make_upload_totals(
    precision: int, rounding: str, upload_id: int, totals: ReportTotals
) -> dict:
    if totals.coverage is not None:
        coverage = precise_round(Decimal(totals.coverage), precision, rounding)
    else:
        coverage = Decimal(0)

    return {
        "upload_id": upload_id,
        "branches": totals.branches,
        "coverage": coverage,
        "hits": totals.hits,
        "lines": totals.lines,
        "methods": totals.methods,
        "misses": totals.misses,
        "partials": totals.partials,
        "files": totals.files,
    }


def get_joined_flag(commit_yaml: UserYaml, flags: list[str]) -> bool:
    for flag in flags:
        if read_yaml_field(commit_yaml, ("flags", flag, "joined")) is False:
            log.info(
                "Customer is using joined=False feature", extra={"flag_used": flag}
            )
            sentry_sdk.capture_message(
                "Customer is using joined=False feature", tags={"flag_used": flag}
            )
            return False

    return True


def clear_all_carryforward_sessions(
    commit_yaml: UserYaml,
    master_report: Report,
    intermediate_reports: list[IntermediateReport],
) -> set[int]:
    if master_report.is_empty():
        return set()

    all_flags: set[str] = set()
    for intermediate_report in intermediate_reports:
        report = intermediate_report.report
        if report.is_empty():
            continue

        sessionid = next(iter(report.sessions))
        session = report.sessions[sessionid]
        all_flags.update(session.flags or [])

    if not all_flags:
        return set()

    return clear_carryforward_sessions(master_report, all_flags, commit_yaml)


@sentry_sdk.trace
def clear_carryforward_sessions(
    report: Report, flags: set[str], current_yaml: UserYaml
) -> set[int]:
    carryforward_flags = {f for f in flags if current_yaml.flag_has_carryfoward(f)}
    if not carryforward_flags:
        return set()

    sessions_to_delete = set()
    for session_id, session in report.sessions.items():
        if session.session_type == SessionType.carriedforward and session.flags:
            if any(f in carryforward_flags for f in session.flags):
                sessions_to_delete.add(session_id)

    if sessions_to_delete:
        report.delete_multiple_sessions(sessions_to_delete)

    return sessions_to_delete
