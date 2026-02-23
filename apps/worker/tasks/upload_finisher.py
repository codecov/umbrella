import logging
import re
from datetime import UTC, datetime, timedelta
from enum import Enum

import sentry_sdk
from asgiref.sync import async_to_sync
from celery.exceptions import Retry, SoftTimeLimitExceeded

from app import celery_app
from celery_config import notify_error_task_name
from database.enums import CommitErrorTypes, ReportType
from database.models import Commit, Pull
from database.models.core import GITHUB_APP_INSTALLATION_DEFAULT_NAME
from database.models.reports import Upload
from helpers.checkpoint_logger.flows import UploadFlow
from helpers.exceptions import RepositoryWithoutValidBotError
from helpers.github_installation import get_installation_name_for_owner_for_task
from helpers.save_commit_error import save_commit_error
from services.comparison import get_or_create_comparison
from services.lock_manager import LockManager, LockRetry, LockType
from services.processing.intermediate import (
    cleanup_intermediate_reports,
    intermediate_report_key,
    load_intermediate_reports,
)
from services.processing.merging import merge_reports, update_uploads
from services.processing.state import ProcessingState
from services.processing.types import ProcessingResult
from services.report import ReportService
from services.repository import get_repo_provider_service
from services.timeseries import repository_datasets_query
from services.yaml import read_yaml_field
from shared.celery_config import (
    DEFAULT_LOCK_TIMEOUT_SECONDS,
    UPLOAD_PROCESSOR_MAX_RETRIES,
    compute_comparison_task_name,
    notify_task_name,
    pulls_task_name,
    timeseries_save_commit_measurements_task_name,
    upload_finisher_task_name,
)
from shared.django_apps.upload_breadcrumbs.models import Errors, Milestones
from shared.helpers.cache import cache
from shared.helpers.redis import get_redis_connection
from shared.metrics import Counter, inc_counter
from shared.reports.enums import UploadState
from shared.reports.resources import Report
from shared.timeseries.helpers import is_timeseries_enabled
from shared.torngit.exceptions import TorngitError
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

UPLOAD_FINISHER_ALREADY_COMPLETED_COUNTER = Counter(
    "upload_finisher_already_completed",
    "Number of times finisher skipped work because uploads were already in final state",
)

regexp_ci_skip = re.compile(r"\[(ci|skip| |-){3,}\]")


class ShouldCallNotifyResult(Enum):
    DO_NOT_NOTIFY = "do_not_notify"
    NOTIFY_ERROR = "notify_error"
    NOTIFY = "notify"


class UploadFinisherTask(BaseCodecovTask, name=upload_finisher_task_name):
    """This is the third task of the series of tasks designed to process an `upload` made
    by the user

    To see more about the whole picture, see `tasks.upload.UploadTask`

    This task does the finishing steps after a group of uploads is processed

    The steps are:
        - Schedule the set_pending task, depending on the case
        - Schedule notification tasks, depending on the case
        - Invalidating whatever cache is done
    """

    max_retries = UPLOAD_PROCESSOR_MAX_RETRIES

    def _find_started_uploads_with_reports(
        self, db_session, commit: Commit
    ) -> set[int]:
        """Find uploads in "started" state that have intermediate reports in Redis.

        This is the fallback when Redis ProcessingState has expired (TTL: PROCESSING_STATE_TTL).
        We check the database for uploads that were processed but never finalized,
        and verify they have intermediate reports before including them.
        """
        # Query for uploads in "started" state for this commit
        started_uploads = (
            db_session.query(Upload)
            .join(Upload.report)
            .filter(
                Upload.report.has(commit=commit),
                Upload.state == "started",
                Upload.state_id == UploadState.UPLOADED.db_id,
            )
            .all()
        )

        if not started_uploads:
            return set()

        log.info(
            "Found uploads in started state, checking for intermediate reports",
            extra={
                "upload_ids": [u.id_ for u in started_uploads],
                "count": len(started_uploads),
            },
        )

        # Check which uploads have intermediate reports (confirms they were processed)
        redis_connection = get_redis_connection()
        upload_ids_with_reports = set()

        for upload in started_uploads:
            report_key = intermediate_report_key(upload.id_)
            if redis_connection.exists(report_key):
                upload_ids_with_reports.add(upload.id_)
            else:
                log.warning(
                    "Upload in started state but no intermediate report found (may have expired)",
                    extra={"upload_id": upload.id_},
                )

        return upload_ids_with_reports

    def _reconstruct_processing_results(
        self, db_session, state: ProcessingState, commit: Commit
    ) -> list[ProcessingResult]:
        """Reconstruct processing_results from ProcessingState when finisher is triggered
        outside of a chord (e.g., from orphaned upload recovery).

        This ensures ALL uploads that were marked as processed in Redis are included
        in the final merged report, even if they completed via retry/recovery.

        If Redis state has expired (TTL: PROCESSING_STATE_TTL), falls back to database
        to find uploads in "started" state that have intermediate reports, preventing data loss.
        """

        # Get all upload IDs that are ready to be merged (in "processed" set)
        upload_ids = state.get_uploads_for_merging()

        if not upload_ids:
            log.warning(
                "No uploads found in Redis processed set, checking database for started uploads",
                extra={"repoid": commit.repoid, "commitid": commit.commitid},
            )
            # Fallback: Redis state expired (TTL: PROCESSING_STATE_TTL), check DB for uploads
            # in "started" state that might have been processed but never finalized
            upload_ids = self._find_started_uploads_with_reports(db_session, commit)

            if not upload_ids:
                log.warning(
                    "No started uploads with intermediate reports found in database",
                    extra={"repoid": commit.repoid, "commitid": commit.commitid},
                )
                return []

            log.info(
                "Found started uploads with intermediate reports (Redis state expired)",
                extra={
                    "upload_ids": list(upload_ids),
                    "count": len(upload_ids),
                },
            )

        log.info(
            "Reconstructing processing results from ProcessingState",
            extra={"upload_ids": list(upload_ids), "count": len(upload_ids)},
        )

        # Load Upload records from database to get arguments
        uploads = db_session.query(Upload).filter(Upload.id_.in_(upload_ids)).all()

        # Check which uploads have intermediate reports in Redis
        redis_connection = get_redis_connection()

        processing_results = []
        for upload in uploads:
            # Check if intermediate report exists (indicates successful processing)
            report_key = intermediate_report_key(upload.id_)
            has_report = redis_connection.exists(report_key)

            processing_result: ProcessingResult = {
                "upload_id": upload.id_,
                "arguments": {
                    "commit": commit.commitid,
                    "upload_id": upload.id_,
                    "version": "v4",  # Assume v4 for recovered uploads
                    "reportid": str(upload.report.external_id),
                },
                "successful": bool(has_report),
            }

            if not has_report:
                log.warning(
                    "Upload in processed set but no intermediate report found",
                    extra={"upload_id": upload.id_},
                )
                processing_result["error"] = {
                    "code": "missing_intermediate_report",
                    "params": {},
                }

            processing_results.append(processing_result)

        log.info(
            "Reconstructed processing results",
            extra={
                "total_uploads": len(processing_results),
                "successful": sum(1 for r in processing_results if r["successful"]),
                "failed": sum(1 for r in processing_results if not r["successful"]),
            },
        )

        return processing_results

    def run_impl(
        self,
        db_session,
        processing_results: list[ProcessingResult] | None = None,
        *args,
        repoid: int,
        commitid: str,
        commit_yaml,
        **kwargs,
    ):
        try:
            UploadFlow.log(UploadFlow.BATCH_PROCESSING_COMPLETE)
        except ValueError as e:
            log.warning("CheckpointLogger failed to log/submit", extra={"error": e})

        milestone = Milestones.UPLOAD_COMPLETE

        log.info(
            "Received upload_finisher task",
            extra={
                "processing_results": processing_results,
            },
        )

        repoid = int(repoid)
        commit_yaml = UserYaml(commit_yaml)

        log.info("run_impl: Getting commit")

        commit = (
            db_session.query(Commit)
            .filter(Commit.repoid == repoid, Commit.commitid == commitid)
            .first()
        )
        assert commit, "Commit not found in database."

        log.info("run_impl: Got commit")

        state = ProcessingState(repoid, commitid)

        # If processing_results not provided (e.g., from orphaned upload recovery),
        # reconstruct it from ProcessingState to ensure ALL uploads are included
        if processing_results is None:
            log.info(
                "run_impl: processing_results not provided, reconstructing from ProcessingState"
            )
            processing_results = self._reconstruct_processing_results(
                db_session, state, commit
            )
            log.info(
                "run_impl: Reconstructed processing results",
                extra={
                    "upload_count": len(processing_results),
                    "upload_ids": [r["upload_id"] for r in processing_results],
                },
            )

        upload_ids = [upload["upload_id"] for upload in processing_results]

        # Idempotency check: Skip if all uploads are already processed
        # This prevents wasted work if multiple finishers are triggered (e.g., from
        # visibility timeout re-queuing) or if finisher is manually retried
        if upload_ids:
            uploads_in_db = (
                db_session.query(Upload).filter(Upload.id_.in_(upload_ids)).all()
            )
            # Only skip if ALL uploads exist in DB and ALL are in final states
            if len(uploads_in_db) == len(upload_ids):
                all_already_processed = all(
                    upload.state in ("processed", "error") for upload in uploads_in_db
                )
                if all_already_processed:
                    log.info(
                        "All uploads already in final state, skipping finisher work",
                        extra={
                            "upload_ids": upload_ids,
                            "states": [u.state for u in uploads_in_db],
                        },
                    )
                    inc_counter(UPLOAD_FINISHER_ALREADY_COMPLETED_COUNTER)
                    return {
                        "already_completed": True,
                        "upload_ids": upload_ids,
                    }

        try:
            log.info("run_impl: Processing reports with lock")

            self._process_reports_with_lock(
                db_session,
                commit,
                commit_yaml,
                processing_results,
                milestone,
                upload_ids,
                state,
            )

            # Check if there are still unprocessed coverage uploads in the database
            # Use DB as source of truth - if any coverage uploads are still in UPLOADED state,
            # another finisher will process them and we shouldn't send notifications yet
            remaining_uploads = (
                db_session.query(Upload)
                .join(Upload.report)
                .filter(
                    Upload.report.has(commit=commit),
                    Upload.report.has(report_type=ReportType.COVERAGE.value),
                    Upload.state_id == UploadState.UPLOADED.db_id,
                )
                .count()
            )

            if remaining_uploads > 0:
                log.info(
                    "run_impl: Postprocessing should not be triggered - uploads still pending",
                    extra={"remaining_uploads": remaining_uploads},
                )
                UploadFlow.log(UploadFlow.PROCESSING_COMPLETE)
                UploadFlow.log(UploadFlow.SKIPPING_NOTIFICATION)
                self._call_upload_breadcrumb_task(
                    commit_sha=commitid,
                    repo_id=repoid,
                    milestone=milestone,
                    upload_ids=upload_ids,
                )
                return

            log.info("run_impl: Handling finisher lock")

            return self._handle_finisher_lock(
                db_session,
                commit,
                commit_yaml,
                processing_results,
                milestone,
                upload_ids,
            )

        except Retry:
            raise

        except SoftTimeLimitExceeded:
            log.warning("run_impl: soft time limit exceeded")
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=milestone,
                upload_ids=upload_ids,
                error=Errors.TASK_TIMED_OUT,
            )
            return {
                "error": "Soft time limit exceeded",
                "upload_ids": upload_ids,
            }

        except Exception as e:
            log.exception("run_impl: unexpected error in upload finisher")
            sentry_sdk.capture_exception(e)
            log.exception(
                "Unexpected error in upload finisher",
                extra={"upload_ids": upload_ids},
            )
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=milestone,
                upload_ids=upload_ids,
                error=Errors.UNKNOWN,
                error_text=repr(e),
            )
            return {
                "error": str(e),
                "upload_ids": upload_ids,
            }

    def _process_reports_with_lock(
        self,
        db_session,
        commit: Commit,
        commit_yaml: UserYaml,
        processing_results: list[ProcessingResult],
        milestone: Milestones,
        upload_ids: list,
        state: ProcessingState,
    ):
        """Process reports with a lock to prevent concurrent modifications."""
        diff = load_commit_diff(commit, self.name)
        repoid = commit.repoid
        commitid = commit.commitid

        log.info("run_impl: Loaded commit diff")

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            lock_timeout=self.get_lock_timeout(DEFAULT_LOCK_TIMEOUT_SECONDS),
            blocking_timeout=None,
        )

        try:
            with lock_manager.locked(
                LockType.UPLOAD_PROCESSING,
                max_retries=UPLOAD_PROCESSOR_MAX_RETRIES,
                retry_num=self.attempts,
            ):
                db_session.refresh(commit)
                report_service = ReportService(commit_yaml)

                log.info("run_impl: Performing report merging")

                report = perform_report_merging(
                    report_service, commit_yaml, commit, processing_results
                )

                log.info(
                    "run_impl: Saving combined report",
                    extra={"processing_results": processing_results},
                )

                if diff:
                    log.info("run_impl: Applying diff to report")
                    report.apply_diff(diff)

                log.info("run_impl: Saving report")
                report_service.save_report(commit, report)

                db_session.commit()

                log.info("run_impl: Marking uploads as merged")
                state.mark_uploads_as_merged(upload_ids)

                log.info("run_impl: Cleaning up intermediate reports")
                cleanup_intermediate_reports(upload_ids)

                log.info("run_impl: Finished upload_finisher task")

        except LockRetry as retry:
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=milestone,
                upload_ids=upload_ids,
                error=Errors.INTERNAL_LOCK_ERROR,
            )
            if retry.max_retries_exceeded or self._has_exceeded_max_attempts(
                UPLOAD_PROCESSOR_MAX_RETRIES
            ):
                log.error(
                    "Upload finisher exceeded max retries",
                    extra={
                        "attempts": retry.retry_num
                        if retry.max_retries_exceeded
                        else self.attempts,
                        "commitid": commitid,
                        "max_retries": UPLOAD_PROCESSOR_MAX_RETRIES,
                        "repoid": repoid,
                    },
                )
                self._call_upload_breadcrumb_task(
                    commit_sha=commitid,
                    repo_id=repoid,
                    milestone=milestone,
                    upload_ids=upload_ids,
                    error=Errors.INTERNAL_OUT_OF_RETRIES,
                )
                return
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=milestone,
                upload_ids=upload_ids,
                error=Errors.INTERNAL_RETRYING,
            )
            self.retry(
                max_retries=UPLOAD_PROCESSOR_MAX_RETRIES,
                countdown=retry.countdown,
            )

    def _handle_finisher_lock(
        self,
        db_session,
        commit: Commit,
        commit_yaml: UserYaml,
        processing_results: list[ProcessingResult],
        milestone: Milestones,
        upload_ids: list,
    ):
        """Handle the finisher lock and post-processing tasks."""
        repoid = commit.repoid
        commitid = commit.commitid
        repository = commit.repository

        lock_manager = LockManager(
            repoid=repoid,
            commitid=commitid,
            lock_timeout=self.get_lock_timeout(DEFAULT_LOCK_TIMEOUT_SECONDS),
            blocking_timeout=None,
        )

        try:
            with lock_manager.locked(
                LockType.UPLOAD_FINISHER,
                max_retries=UPLOAD_PROCESSOR_MAX_RETRIES,
                retry_num=self.attempts,
            ):
                result = self.finish_reports_processing(
                    db_session, commit, commit_yaml, processing_results
                )

                log.info("handle_finisher_lock: Finished reports processing")

                if is_timeseries_enabled():
                    log.info("handle_finisher_lock: Saving commit measurements")
                    dataset_names = [
                        dataset.name
                        for dataset in repository_datasets_query(repository)
                    ]
                    if dataset_names:
                        self.app.tasks[
                            timeseries_save_commit_measurements_task_name
                        ].apply_async(
                            kwargs={
                                "commitid": commitid,
                                "repoid": repoid,
                                "dataset_names": dataset_names,
                            }
                        )

                # Mark the repository as updated so it will appear earlier in the list
                # of recently-active repositories
                now = datetime.now(tz=UTC)
                threshold = now - timedelta(minutes=60)
                if not repository.updatestamp or repository.updatestamp < threshold:
                    log.info("handle_finisher_lock: Updating repository")
                    repository.updatestamp = now
                    db_session.commit()
                else:
                    log.info(
                        "Skipping repository update because it was updated recently",
                        extra={
                            "repository": repository.name,
                            "updatestamp": repository.updatestamp,
                            "threshold": threshold,
                        },
                    )

                log.info("handle_finisher_lock: Invalidating caches")
                self.invalidate_caches(lock_manager.redis_connection, commit)
                self._call_upload_breadcrumb_task(
                    commit_sha=commitid,
                    repo_id=repoid,
                    milestone=milestone,
                    upload_ids=upload_ids,
                )
                log.info("handle_finisher_lock: Finished upload_finisher task")
                return result

        except LockRetry as retry:
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=milestone,
                upload_ids=upload_ids,
                error=Errors.INTERNAL_LOCK_ERROR,
            )
            UploadFlow.log(UploadFlow.FINISHER_LOCK_ERROR)
            if retry.max_retries_exceeded or self._has_exceeded_max_attempts(
                UPLOAD_PROCESSOR_MAX_RETRIES
            ):
                log.error(
                    "Upload finisher exceeded max retries (finisher lock)",
                    extra={
                        "attempts": retry.retry_num
                        if retry.max_retries_exceeded
                        else self.attempts,
                        "commitid": commitid,
                        "max_retries": UPLOAD_PROCESSOR_MAX_RETRIES,
                        "repoid": repoid,
                    },
                )
                self._call_upload_breadcrumb_task(
                    commit_sha=commitid,
                    repo_id=repoid,
                    milestone=milestone,
                    upload_ids=upload_ids,
                    error=Errors.INTERNAL_OUT_OF_RETRIES,
                )
                return
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=milestone,
                upload_ids=upload_ids,
                error=Errors.INTERNAL_RETRYING,
            )
            self.retry(
                max_retries=UPLOAD_PROCESSOR_MAX_RETRIES,
                countdown=retry.countdown,
            )

    def finish_reports_processing(
        self,
        db_session,
        commit: Commit,
        commit_yaml: UserYaml,
        processing_results: list[ProcessingResult],
    ):
        log.debug(f"In finish_reports_processing for commit: {commit}")
        commitid = commit.commitid
        repoid = commit.repoid

        # always notify, let the notify handle if it should submit
        notifications_called = False
        if not regexp_ci_skip.search(commit.message or ""):
            should_call_notifications = self.should_call_notifications(
                commit, commit_yaml, processing_results, db_session
            )
            log.info(
                "finish_reports_processing: should_call_notifications",
                extra={"should_call_notifications": should_call_notifications},
            )
            match should_call_notifications:
                case ShouldCallNotifyResult.NOTIFY:
                    notifications_called = True
                    notify_kwargs = {
                        "repoid": repoid,
                        "commitid": commitid,
                        "current_yaml": commit_yaml.to_dict(),
                    }
                    notify_kwargs = UploadFlow.save_to_kwargs(notify_kwargs)
                    task = self.app.tasks[notify_task_name].apply_async(
                        kwargs=notify_kwargs
                    )
                    self._call_upload_breadcrumb_task(
                        commit_sha=commitid,
                        repo_id=repoid,
                        milestone=Milestones.NOTIFICATIONS_TRIGGERED,
                        upload_ids=[
                            upload["upload_id"] for upload in processing_results
                        ],
                    )
                    log.info(
                        "Scheduling notify task",
                        extra={
                            "repoid": repoid,
                            "commit": commitid,
                            "commit_yaml": commit_yaml.to_dict(),
                            "processing_results": processing_results,
                            "notify_task_id": task.id,
                            "parent_task": self.request.parent_id,
                        },
                    )
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
                                self.app.tasks[pulls_task_name].apply_async(
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
                                    self.app.tasks[
                                        compute_comparison_task_name
                                    ].apply_async(
                                        kwargs={"comparison_id": comparison.id}
                                    )
                case ShouldCallNotifyResult.DO_NOT_NOTIFY:
                    notifications_called = False
                    log.info(
                        "Skipping notify task",
                        extra={
                            "repoid": repoid,
                            "commit": commitid,
                            "commit_yaml": commit_yaml.to_dict(),
                            "processing_results": processing_results,
                            "parent_task": self.request.parent_id,
                        },
                    )
                case ShouldCallNotifyResult.NOTIFY_ERROR:
                    notifications_called = False
                    notify_error_kwargs = {
                        "repoid": repoid,
                        "commitid": commitid,
                        "current_yaml": commit_yaml.to_dict(),
                    }
                    notify_error_kwargs = UploadFlow.save_to_kwargs(notify_error_kwargs)
                    task = self.app.tasks[notify_error_task_name].apply_async(
                        kwargs=notify_error_kwargs
                    )
        else:
            log.info(
                "Skipping notify because CI skip tag is present",
                extra={
                    "repoid": repoid,
                    "commit": commitid,
                },
            )
            commit.state = "skipped"

        UploadFlow.log(UploadFlow.PROCESSING_COMPLETE)
        if not notifications_called:
            UploadFlow.log(UploadFlow.SKIPPING_NOTIFICATION)

        return {"notifications_called": notifications_called}

    def should_call_notifications(
        self,
        commit: Commit,
        commit_yaml: UserYaml,
        processing_results: list[ProcessingResult],
        db_session=None,
    ) -> ShouldCallNotifyResult:
        extra_dict = {
            "repoid": commit.repoid,
            "commitid": commit.commitid,
            "commit_yaml": commit_yaml,
            "processing_results": processing_results,
            "parent_task": self.request.parent_id,
        }

        # Check if there are still pending uploads in the database
        # Use DB as source of truth for upload completion status
        if db_session:
            remaining_uploads = (
                db_session.query(Upload)
                .join(Upload.report)
                .filter(
                    Upload.report.has(commit=commit),
                    Upload.report.has(report_type=ReportType.COVERAGE.value),
                    Upload.state_id == UploadState.UPLOADED.db_id,
                )
                .count()
            )
            if remaining_uploads > 0:
                log.info(
                    "Not scheduling notify because there are still pending uploads",
                    extra={**extra_dict, "remaining_uploads": remaining_uploads},
                )
                return ShouldCallNotifyResult.DO_NOT_NOTIFY

        manual_trigger = read_yaml_field(
            commit_yaml, ("codecov", "notify", "manual_trigger")
        )
        if manual_trigger:
            log.info(
                "Not scheduling notify because manual trigger is used",
                extra=extra_dict,
            )
            return ShouldCallNotifyResult.DO_NOT_NOTIFY

        after_n_builds = (
            read_yaml_field(commit_yaml, ("codecov", "notify", "after_n_builds")) or 0
        )
        if after_n_builds > 0:
            report = ReportService(commit_yaml).get_existing_report_for_commit(commit)
            number_sessions = len(report.sessions) if report is not None else 0
            if after_n_builds > number_sessions:
                log.info(
                    "Not scheduling notify because `after_n_builds` is %s and we only found %s builds",
                    after_n_builds,
                    number_sessions,
                    extra=extra_dict,
                )
                return ShouldCallNotifyResult.DO_NOT_NOTIFY

        processing_successses = [x["successful"] for x in processing_results]

        if read_yaml_field(
            commit_yaml,
            ("codecov", "notify", "notify_error"),
            _else=False,
        ):
            if len(processing_successses) == 0 or not all(processing_successses):
                log.info(
                    "Not scheduling notify because there is a non-successful processing result",
                    extra=extra_dict,
                )

                return ShouldCallNotifyResult.NOTIFY_ERROR
        else:
            if not any(processing_successses):
                return ShouldCallNotifyResult.DO_NOT_NOTIFY

        return ShouldCallNotifyResult.NOTIFY

    def invalidate_caches(self, redis_connection, commit: Commit):
        redis_connection.delete(f"cache/{commit.repoid}/tree/{commit.branch}")
        redis_connection.delete(f"cache/{commit.repoid}/tree/{commit.commitid}")
        repository = commit.repository
        key = ":".join(
            (repository.service, repository.author.username, repository.name)
        )
        if commit.branch:
            redis_connection.hdel("badge", (f"{key}:{commit.branch}").lower())
            if commit.branch == repository.branch:
                redis_connection.hdel("badge", (f"{key}:").lower())


RegisteredUploadTask = celery_app.register_task(UploadFinisherTask())
upload_finisher_task = celery_app.tasks[RegisteredUploadTask.name]


@sentry_sdk.trace
def perform_report_merging(
    report_service: ReportService,
    commit_yaml: UserYaml,
    commit: Commit,
    processing_results: list[ProcessingResult],
) -> Report:
    log.info("perform_report_merging: Getting existing report")
    master_report = report_service.get_existing_report_for_commit(commit)
    if master_report is None:
        log.info("perform_report_merging: No existing report found, creating new one")
        master_report = Report()
    else:
        log.info("perform_report_merging: Existing report found")

    upload_ids = [
        upload["upload_id"] for upload in processing_results if upload["successful"]
    ]
    log.info(
        "perform_report_merging: Loading intermediate reports",
        extra={"upload_ids": upload_ids},
    )
    intermediate_reports = load_intermediate_reports(upload_ids)

    log.info(
        "perform_report_merging: Merging reports", extra={"upload_ids": upload_ids}
    )
    master_report, merge_result = merge_reports(
        commit_yaml, master_report, intermediate_reports
    )

    log.info(
        "perform_report_merging: Updating uploads", extra={"upload_ids": upload_ids}
    )
    # Update the `Upload` in the database with the final session_id
    # (aka `order_number`) and other statuses
    update_uploads(
        commit.get_db_session(),
        commit_yaml,
        processing_results,
        intermediate_reports,
        merge_result,
    )

    return master_report


@sentry_sdk.trace
@cache.cache_function(ttl=60 * 60)  # the commit diff is immutable
def load_commit_diff(commit: Commit, task_name: str | None = None) -> dict | None:
    log.info("load_commit_diff: Loading commit diff")
    repository = commit.repository
    commitid = commit.commitid
    try:
        installation_name_to_use = (
            get_installation_name_for_owner_for_task(task_name, repository.author)
            if task_name
            else GITHUB_APP_INSTALLATION_DEFAULT_NAME
        )
        log.info("load_commit_diff: Getting repository service")
        repository_service = get_repo_provider_service(
            repository, installation_name_to_use=installation_name_to_use
        )
        log.info("load_commit_diff: Getting commit diff")
        return async_to_sync(repository_service.get_commit_diff)(commitid)

    # TODO(swatinem): can we maybe get rid of all this logging?
    except TorngitError:
        # When this happens, we have that commit.totals["diff"] is not available.
        # Since there is no way to calculate such diff without the git commit,
        # then we assume having the rest of the report saved there is better than the
        # alternative of refusing an otherwise "good" report because of the lack of diff
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
