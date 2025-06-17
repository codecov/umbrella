import functools
import logging
from decimal import Decimal

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


@sentry_sdk.trace
def merge_reports(
    commit_yaml: UserYaml,
    master_report: Report,
    intermediate_reports: list[IntermediateReport],
) -> tuple[Report, MergeResult]:
    session_mapping: dict[int, int] = {}

    deleted_sessions = clear_all_carryforward_sessions(
        commit_yaml, master_report, intermediate_reports
    )

    for intermediate_report in intermediate_reports:
        report = intermediate_report.report
        if report.is_empty():
            continue

        old_sessionid = next(iter(report.sessions))
        new_sessionid = master_report.next_session_number()
        session_mapping[intermediate_report.upload_id] = new_sessionid

        if master_report.is_empty() and old_sessionid == new_sessionid:
            # if the master report is empty, we can avoid a costly merge operation
            master_report = report
            continue

        report.change_sessionid(old_sessionid, new_sessionid)
        session = report.sessions[new_sessionid]

        _session_id, session = master_report.add_session(
            session, use_id_from_session=True
        )

        joined = get_joined_flag(commit_yaml, session.flags or [])
        master_report.merge(report, joined)

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
