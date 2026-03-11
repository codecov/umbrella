import logging
import re
import time
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from enum import Enum

import sentry_sdk
from asgiref.sync import async_to_sync
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy import func

from app import celery_app
from celery_config import notify_error_task_name
from database.enums import CommitErrorTypes, ReportType
from database.models import Commit, Pull
from database.models.core import GITHUB_APP_INSTALLATION_DEFAULT_NAME
from database.models.reports import CommitReport, Upload
from helpers.checkpoint_logger.flows import UploadFlow
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.github_installation import get_installation_name_for_owner_for_task
from helpers.save_commit_error import save_commit_error
from services.comparison import get_or_create_comparison
from services.lock_manager import LockManager, LockRetry, LockType
from services.processing.intermediate import (
    cleanup_intermediate_reports,
    load_intermediate_reports,
)
from services.processing.merging import merge_reports, update_uploads
from services.processing.types import ProcessingResult
from services.report import ReportService
from services.repository import get_repo_provider_service
from services.timeseries import repository_datasets_query
from services.yaml import read_yaml_field
from shared.celery_config import (
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    compute_comparison_task_name,
    notify_task_name,
    pulls_task_name,
    timeseries_save_commit_measurements_task_name,
    upload_merger_task_name,
)
from shared.helpers.cache import cache
from shared.helpers.redis import get_redis_connection
from shared.metrics import Counter, Histogram
from shared.reports.enums import UploadState
from shared.reports.resources import Report
from shared.timeseries.helpers import is_timeseries_enabled
from shared.torngit.exceptions import TorngitError
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

# --- Constants ---

MERGE_BATCH_SIZE = 10
MERGE_TIME_BUDGET_SECONDS = 200
MERGER_GATE_KEY_PREFIX = "upload_merger_lock"
MERGER_GATE_TTL = 900

regexp_ci_skip = re.compile(r"\[(ci|skip| |-){3,}\]")

# --- Prometheus Metrics ---

UPLOAD_E2E_DURATION = Histogram(
    "upload_pipeline_e2e_duration_seconds",
    "Duration from oldest upload created_at to notify triggered.",
    ["path", "trigger"],
    buckets=[5, 15, 30, 60, 120, 300, 600, 1200, 3600],
)

UPLOAD_MERGE_DURATION = Histogram(
    "upload_merge_duration_seconds",
    "Wall clock time of the merge phase.",
    ["path", "trigger"],
    buckets=[1, 5, 10, 30, 60, 120, 300, 600],
)

UPLOAD_MERGE_COUNT = Histogram(
    "upload_merge_uploads_total",
    "Number of uploads merged in one merger run.",
    ["path", "trigger"],
    buckets=[1, 5, 10, 25, 50, 100, 200, 500],
)

UPLOAD_MERGE_RESULT = Counter(
    "upload_merge_result_total",
    "Outcome of merge task.",
    ["path", "outcome", "trigger"],
)

# --- Helper types ---

RemainingUploads = namedtuple("RemainingUploads", ["uploaded", "processed"])


class ShouldCallNotifyResult(Enum):
    DO_NOT_NOTIFY = "do_not_notify"
    NOTIFY_ERROR = "notify_error"
    NOTIFY = "notify"


# --- Key helpers ---


def merger_gate_key(repoid: int, commitid: str) -> str:
    return f"{MERGER_GATE_KEY_PREFIX}_{repoid}_{commitid}"


def delete_merger_gate(redis, repoid: int, commitid: str):
    redis.delete(merger_gate_key(repoid, commitid))


# --- Scheduling helpers ---


def schedule_continuation(task, repoid, commitid, commit_yaml):
    task.app.tasks[upload_merger_task_name].apply_async(
        kwargs={
            "repoid": repoid,
            "commitid": commitid,
            "commit_yaml": commit_yaml.to_dict(),
            "trigger": "continuation",
        }
    )


def schedule_sweep(task, repoid, commitid, commit_yaml, countdown=30):
    task.app.tasks[upload_merger_task_name].apply_async(
        kwargs={
            "repoid": repoid,
            "commitid": commitid,
            "commit_yaml": commit_yaml.to_dict(),
            "trigger": "sweep",
        },
        countdown=countdown,
    )


def schedule_watchdog(task, repoid, commitid, commit_yaml, countdown):
    task.app.tasks[upload_merger_task_name].apply_async(
        kwargs={
            "repoid": repoid,
            "commitid": commitid,
            "commit_yaml": commit_yaml.to_dict(),
            "trigger": "watchdog",
        },
        countdown=countdown,
    )


# --- DB query helpers ---


def get_processed_uploads(db_session, commit: Commit, limit: int) -> list[Upload]:
    return (
        db_session.query(Upload)
        .join(CommitReport, Upload.report_id == CommitReport.id_)
        .filter(
            CommitReport.commit_id == commit.id_,
            (CommitReport.report_type == None)  # noqa: E711
            | (CommitReport.report_type == ReportType.COVERAGE.value),
            Upload.state_id == UploadState.PROCESSED.db_id,
        )
        .limit(limit)
        .all()
    )


def count_unfinished_uploads(db_session, commit: Commit) -> RemainingUploads:
    row = (
        db_session.query(
            func.count(Upload.id_).filter(
                Upload.state_id == UploadState.UPLOADED.db_id
            ),
            func.count(Upload.id_).filter(
                Upload.state_id == UploadState.PROCESSED.db_id
            ),
        )
        .join(CommitReport, Upload.report_id == CommitReport.id_)
        .filter(
            CommitReport.commit_id == commit.id_,
            (CommitReport.report_type == None)  # noqa: E711
            | (CommitReport.report_type == ReportType.COVERAGE.value),
        )
        .one()
    )
    return RemainingUploads(uploaded=row[0], processed=row[1])


def get_oldest_upload_created_at(db_session, commit: Commit) -> datetime | None:
    result = (
        db_session.query(func.min(Upload.created_at))
        .join(CommitReport, Upload.report_id == CommitReport.id_)
        .filter(
            CommitReport.commit_id == commit.id_,
            (CommitReport.report_type == None)  # noqa: E711
            | (CommitReport.report_type == ReportType.COVERAGE.value),
        )
        .scalar()
    )
    return result


# --- Merge helpers ---


def merge_batch(
    db_session,
    report_service: ReportService,
    master_report: Report,
    commit: Commit,
    commit_yaml: UserYaml,
    batch: list[Upload],
    diff: dict | None,
) -> tuple[Report, list[ProcessingResult]]:
    """Load intermediate reports for a batch, merge into master report,
    and mark uploads as MERGED in the session (not yet committed)."""
    upload_ids = [u.id_ for u in batch]
    intermediate_reports = load_intermediate_reports(upload_ids)

    processing_results: list[ProcessingResult] = []
    for upload in batch:
        has_report = any(
            ir.upload_id == upload.id_ and not ir.report.is_empty()
            for ir in intermediate_reports
        )
        processing_results.append(
            {
                "upload_id": upload.id_,
                "arguments": {
                    "commit": commit.commitid,
                    "upload_id": upload.id_,
                    "version": "v4",
                    "reportid": str(upload.report.external_id),
                },
                "successful": has_report,
            }
        )

    master_report, merge_result = merge_reports(
        commit_yaml, master_report, intermediate_reports
    )

    update_uploads(
        db_session,
        commit_yaml,
        processing_results,
        intermediate_reports,
        merge_result,
    )

    return master_report, processing_results


def save_and_commit(
    db_session,
    report_service: ReportService,
    commit: Commit,
    master_report: Report,
    merged_upload_ids: list[int],
    diff: dict | None,
):
    """Persist the master report and commit staged upload updates."""
    if diff:
        master_report.apply_diff(diff)

    report_service.save_report(commit, master_report)
    db_session.commit()
    cleanup_intermediate_reports(merged_upload_ids)


# --- Cache invalidation ---


def invalidate_caches(redis_connection, commit: Commit):
    redis_connection.delete(f"cache/{commit.repoid}/tree/{commit.branch}")
    redis_connection.delete(f"cache/{commit.repoid}/tree/{commit.commitid}")
    repository = commit.repository
    key = ":".join((repository.service, repository.author.username, repository.name))
    if commit.branch:
        redis_connection.hdel("badge", (f"{key}:{commit.branch}").lower())
        if commit.branch == repository.branch:
            redis_connection.hdel("badge", (f"{key}:").lower())


# --- Post-processing ---


def should_call_notifications(
    commit: Commit,
    commit_yaml: UserYaml,
    all_processing_results: list[ProcessingResult],
    db_session,
) -> ShouldCallNotifyResult:
    remaining_uploads = (
        db_session.query(Upload)
        .join(CommitReport, Upload.report_id == CommitReport.id_)
        .filter(
            CommitReport.commit_id == commit.id_,
            (CommitReport.report_type == None)  # noqa: E711
            | (CommitReport.report_type == ReportType.COVERAGE.value),
            Upload.state_id == UploadState.UPLOADED.db_id,
        )
        .count()
    )
    if remaining_uploads > 0:
        return ShouldCallNotifyResult.DO_NOT_NOTIFY

    manual_trigger = read_yaml_field(
        commit_yaml, ("codecov", "notify", "manual_trigger")
    )
    if manual_trigger:
        return ShouldCallNotifyResult.DO_NOT_NOTIFY

    after_n_builds = (
        read_yaml_field(commit_yaml, ("codecov", "notify", "after_n_builds")) or 0
    )
    if after_n_builds > 0:
        report = ReportService(commit_yaml).get_existing_report_for_commit(commit)
        number_sessions = len(report.sessions) if report is not None else 0
        if after_n_builds > number_sessions:
            return ShouldCallNotifyResult.DO_NOT_NOTIFY

    processing_successes = [x["successful"] for x in all_processing_results]

    if read_yaml_field(
        commit_yaml,
        ("codecov", "notify", "notify_error"),
        _else=False,
    ):
        if len(processing_successes) == 0 or not all(processing_successes):
            return ShouldCallNotifyResult.NOTIFY_ERROR
    else:
        if not any(processing_successes):
            return ShouldCallNotifyResult.DO_NOT_NOTIFY

    return ShouldCallNotifyResult.NOTIFY


def post_process(
    db_session,
    commit: Commit,
    commit_yaml: UserYaml,
    task,
    all_processing_results: list[ProcessingResult],
) -> dict:
    """Run notifications, repo update, timeseries, and PR comparison."""
    repoid = commit.repoid
    commitid = commit.commitid
    repository = commit.repository

    notifications_called = False
    if not regexp_ci_skip.search(commit.message or ""):
        notify_result = should_call_notifications(
            commit, commit_yaml, all_processing_results, db_session
        )
        match notify_result:
            case ShouldCallNotifyResult.NOTIFY:
                notifications_called = True
                notify_kwargs = {
                    "repoid": repoid,
                    "commitid": commitid,
                    "current_yaml": commit_yaml.to_dict(),
                }
                notify_kwargs = UploadFlow.save_to_kwargs(notify_kwargs)
                task.app.tasks[notify_task_name].apply_async(kwargs=notify_kwargs)

                if commit.pullid:
                    pull = (
                        db_session.query(Pull)
                        .filter_by(repoid=commit.repoid, pullid=commit.pullid)
                        .first()
                    )
                    if pull:
                        head = pull.get_head_commit()
                        if head is None or head.timestamp <= commit.timestamp:
                            pull.head = commit.commitid
                        if pull.head == commit.commitid:
                            db_session.commit()
                            task.app.tasks[pulls_task_name].apply_async(
                                kwargs={
                                    "repoid": repoid,
                                    "pullid": pull.pullid,
                                    "should_send_notifications": False,
                                }
                            )
                            compared_to = pull.get_comparedto_commit()
                            if compared_to:
                                comparison = get_or_create_comparison(
                                    db_session, compared_to, commit
                                )
                                db_session.commit()
                                task.app.tasks[
                                    compute_comparison_task_name
                                ].apply_async(kwargs={"comparison_id": comparison.id})
            case ShouldCallNotifyResult.NOTIFY_ERROR:
                notify_error_kwargs = {
                    "repoid": repoid,
                    "commitid": commitid,
                    "current_yaml": commit_yaml.to_dict(),
                }
                notify_error_kwargs = UploadFlow.save_to_kwargs(notify_error_kwargs)
                task.app.tasks[notify_error_task_name].apply_async(
                    kwargs=notify_error_kwargs
                )
            case ShouldCallNotifyResult.DO_NOT_NOTIFY:
                pass
    else:
        commit.state = "skipped"

    if is_timeseries_enabled():
        dataset_names = [
            dataset.name for dataset in repository_datasets_query(repository)
        ]
        if dataset_names:
            task.app.tasks[timeseries_save_commit_measurements_task_name].apply_async(
                kwargs={
                    "commitid": commitid,
                    "repoid": repoid,
                    "dataset_names": dataset_names,
                }
            )

    now = datetime.now(tz=UTC)
    threshold = now - timedelta(minutes=60)
    if not repository.updatestamp or repository.updatestamp < threshold:
        repository.updatestamp = now
        db_session.commit()

    UploadFlow.log(UploadFlow.PROCESSING_COMPLETE)
    if not notifications_called:
        UploadFlow.log(UploadFlow.SKIPPING_NOTIFICATION)

    return {"notifications_called": notifications_called}


# --- Diff loading (reused from finisher) ---


@sentry_sdk.trace
@cache.cache_function(ttl=60 * 60)
def load_commit_diff(commit: Commit, task_name: str | None = None) -> dict | None:
    repository = commit.repository
    commitid = commit.commitid
    try:
        installation_name_to_use = (
            get_installation_name_for_owner_for_task(task_name, repository.author)
            if task_name
            else GITHUB_APP_INSTALLATION_DEFAULT_NAME
        )
        repository_service = get_repo_provider_service(
            repository, installation_name_to_use=installation_name_to_use
        )
        return async_to_sync(repository_service.get_commit_diff)(commitid)
    except TorngitError:
        log.warning(
            "Could not apply diff to report because there was an error fetching diff from provider",
            exc_info=True,
        )
    except RepositoryWithoutValidBotError:
        save_commit_error(
            commit,
            error_code=CommitErrorTypes.REPO_BOT_INVALID.value,
        )
        log.warning(
            "Could not apply diff to report because there is no valid bot found for that repo",
            exc_info=True,
        )
    return None


# === Main Task ===


class UploadMergerTask(BaseCodecovTask, name=upload_merger_task_name):
    """Merge processed uploads into the master report.

    Replaces the chord-based upload_finisher with a single-task-per-commit
    model. Features: time-boxed batch merging, self-scheduling continuations,
    sweep for late-arriving uploads, watchdog-first crash recovery.
    """

    def run_impl(
        self,
        db_session,
        *,
        repoid: int,
        commitid: str,
        commit_yaml,
        trigger: str = "processor",
        **kwargs,
    ):
        repoid = int(repoid)
        commit_yaml = UserYaml(commit_yaml)
        redis = get_redis_connection()

        # --- Gate key check: terminate watchdog chain if work is done ---
        if not redis.exists(merger_gate_key(repoid, commitid)):
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="nothing_to_do", trigger=trigger
            ).inc()
            return {"nothing_to_do": True}

        commit = (
            db_session.query(Commit)
            .filter(Commit.repoid == repoid, Commit.commitid == commitid)
            .first()
        )
        assert commit, "Commit not found in database."

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            lock_timeout=self.get_lock_timeout(DEFAULT_LOCK_TIMEOUT_SECONDS),
            blocking_timeout=5,
        )

        # --- Watchdog-first: schedule safety net BEFORE crash-prone work ---
        watchdog_countdown = self.get_lock_timeout(DEFAULT_LOCK_TIMEOUT_SECONDS) + 30
        schedule_watchdog(
            self, repoid, commitid, commit_yaml, countdown=watchdog_countdown
        )

        try:
            with lock_manager.locked(LockType.UPLOAD_PROCESSING):
                return self._merge_under_lock(
                    db_session, commit, commit_yaml, trigger, lock_manager
                )
        except LockRetry:
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="lock_retry", trigger=trigger
            ).inc()
            return {"lock_acquired": False}
        except SoftTimeLimitExceeded:
            log.warning(
                "Merger soft time limit exceeded",
                extra={"repoid": repoid, "commitid": commitid, "trigger": trigger},
            )
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="error", trigger=trigger
            ).inc()
            return {"error": "Soft time limit exceeded"}

    def _merge_under_lock(
        self,
        db_session,
        commit: Commit,
        commit_yaml: UserYaml,
        trigger: str,
        lock_manager: LockManager,
    ):
        repoid = commit.repoid
        commitid = commit.commitid

        db_session.refresh(commit)
        report_service = ReportService(commit_yaml)
        master_report = report_service.get_existing_report_for_commit(commit)
        if master_report is None:
            master_report = Report()
        diff = load_commit_diff(commit, self.name)

        merge_start = time.monotonic()
        uploads_merged = 0
        merged_upload_ids: list[int] = []
        all_processing_results: list[ProcessingResult] = []

        # --- Time-boxed merge loop ---
        while time.monotonic() - merge_start < MERGE_TIME_BUDGET_SECONDS:
            batch = get_processed_uploads(db_session, commit, MERGE_BATCH_SIZE)
            if not batch:
                break
            master_report, batch_results = merge_batch(
                db_session,
                report_service,
                master_report,
                commit,
                commit_yaml,
                batch,
                diff,
            )
            all_processing_results.extend(batch_results)
            merged_upload_ids.extend(u.id_ for u in batch)
            uploads_merged += len(batch)
        else:
            # Budget exhausted, more work may remain
            save_and_commit(
                db_session,
                report_service,
                commit,
                master_report,
                merged_upload_ids,
                diff,
            )
            UPLOAD_MERGE_DURATION.labels(path="merger", trigger=trigger).observe(
                time.monotonic() - merge_start
            )
            UPLOAD_MERGE_COUNT.labels(path="merger", trigger=trigger).observe(
                uploads_merged
            )
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="continuation", trigger=trigger
            ).inc()
            schedule_continuation(self, repoid, commitid, commit_yaml)
            return {"continuation_scheduled": True, "uploads_merged": uploads_merged}

        merge_elapsed = time.monotonic() - merge_start
        UPLOAD_MERGE_DURATION.labels(path="merger", trigger=trigger).observe(
            merge_elapsed
        )
        UPLOAD_MERGE_COUNT.labels(path="merger", trigger=trigger).observe(
            uploads_merged
        )

        # --- Early exit: nothing was merged ---
        if uploads_merged == 0:
            remaining = count_unfinished_uploads(db_session, commit)
            if remaining.uploaded > 0:
                UPLOAD_MERGE_RESULT.labels(
                    path="merger", outcome="sweep", trigger=trigger
                ).inc()
                schedule_sweep(self, repoid, commitid, commit_yaml, countdown=30)
                return {"sweep_scheduled": True}
            delete_merger_gate(lock_manager.redis_connection, repoid, commitid)
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="nothing_to_do", trigger=trigger
            ).inc()
            return {"nothing_to_do": True}

        # --- Save report + commit DB + cleanup intermediates ---
        save_and_commit(
            db_session,
            report_service,
            commit,
            master_report,
            merged_upload_ids,
            diff,
        )

        # --- Check remaining work ---
        remaining = count_unfinished_uploads(db_session, commit)

        if remaining.uploaded > 0:
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="sweep", trigger=trigger
            ).inc()
            schedule_sweep(self, repoid, commitid, commit_yaml, countdown=30)
            return {"sweep_scheduled": True, "uploads_merged": uploads_merged}

        if remaining.processed > 0:
            UPLOAD_MERGE_RESULT.labels(
                path="merger", outcome="continuation", trigger=trigger
            ).inc()
            schedule_continuation(self, repoid, commitid, commit_yaml)
            return {"continuation_scheduled": True, "uploads_merged": uploads_merged}

        # --- All done: post-process ---
        report_totals = master_report.totals.asdict() if master_report.totals else {}
        log.info(
            "Merge complete",
            extra={
                "repoid": repoid,
                "commitid": commitid,
                "report_totals": report_totals,
                "uploads_merged": uploads_merged,
            },
        )

        result = post_process(
            db_session, commit, commit_yaml, self, all_processing_results
        )

        oldest_upload = get_oldest_upload_created_at(db_session, commit)
        if oldest_upload:
            e2e = (
                datetime.now(tz=UTC) - oldest_upload.replace(tzinfo=UTC)
            ).total_seconds()
            UPLOAD_E2E_DURATION.labels(path="merger", trigger=trigger).observe(e2e)

        invalidate_caches(lock_manager.redis_connection, commit)
        delete_merger_gate(lock_manager.redis_connection, repoid, commitid)
        UPLOAD_MERGE_RESULT.labels(
            path="merger", outcome="completed", trigger=trigger
        ).inc()
        return {**result, "uploads_merged": uploads_merged}


RegisteredUploadMergerTask = celery_app.register_task(UploadMergerTask())
upload_merger_task = celery_app.tasks[RegisteredUploadMergerTask.name]
