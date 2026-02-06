import logging
from typing import Any, cast

from celery.exceptions import CeleryError, SoftTimeLimitExceeded

from app import celery_app
from database.enums import ReportType
from database.models import Commit, CommitReport, Upload
from services.bundle_analysis.report import (
    BundleAnalysisReportService,
    ProcessingResult,
)
from services.lock_manager import (
    LockRetry,
    LockType,
    get_bundle_analysis_lock_manager,
)
from services.processing.types import UploadArguments
from shared.celery_config import (
    BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES,
    bundle_analysis_processor_task_name,
)
from shared.reports.enums import UploadState
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask
from tasks.bundle_analysis_save_measurements import (
    bundle_analysis_save_measurements_task_name,
)

log = logging.getLogger(__name__)


def _log_max_retries_exceeded(
    commitid: str,
    repoid: int,
    attempts: int,
    max_retries: int,
    retry_num: int | None = None,
) -> None:
    extra = {
        "attempts": attempts,
        "commitid": commitid,
        "max_retries": max_retries,
        "repoid": repoid,
    }
    if retry_num is not None:
        extra["retry_num"] = retry_num
    log.error("Bundle analysis processor exceeded max retries", extra=extra)


def _set_upload_error_and_commit(
    db_session,
    upload,
    commitid: str,
    repoid: int,
    log_suffix: str = "",
) -> None:
    upload.state_id = UploadState.ERROR.db_id
    upload.state = "error"
    try:
        db_session.commit()
    except Exception:
        log.exception(
            f"Failed to commit upload error state{log_suffix}",
            extra={"commit": commitid, "repoid": repoid, "upload_id": upload.id_},
        )


class BundleAnalysisProcessorTask(
    BaseCodecovTask, name=bundle_analysis_processor_task_name
):
    max_retries = BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES

    def run_impl(
        self,
        db_session,
        # Celery `chain` injects this argument - it's the list of processing results
        # from prior tasks in the chain (accumulated as each task executes)
        previous_result: list[dict[str, Any]],
        *args,
        repoid: int,
        commitid: str,
        commit_yaml: dict,
        params: UploadArguments,
        **kwargs,
    ):
        repoid = int(repoid)

        log.info(
            "Starting bundle analysis processor",
            extra={
                "repoid": repoid,
                "commit": commitid,
                "commit_yaml": commit_yaml,
                "params": params,
            },
        )

        lock_manager = get_bundle_analysis_lock_manager(
            repoid=repoid,
            commitid=commitid,
        )

        try:
            with lock_manager.locked(
                LockType.BUNDLE_ANALYSIS_PROCESSING,
                retry_num=self.attempts,
                max_retries=self.max_retries,
            ):
                return self.process_impl_within_lock(
                    db_session,
                    repoid,
                    commitid,
                    UserYaml.from_dict(commit_yaml),
                    params,
                    previous_result,
                )
        except LockRetry as retry:
            # Honor LockManager cap (Redis attempt count) so re-delivered messages stop.
            if retry.max_retries_exceeded or self._has_exceeded_max_attempts(
                self.max_retries
            ):
                _log_max_retries_exceeded(
                    commitid=commitid,
                    repoid=repoid,
                    attempts=(
                        retry.retry_num if retry.max_retries_exceeded else self.attempts
                    ),
                    max_retries=self.max_retries,
                    retry_num=self.request.retries,
                )
                return previous_result
            self.retry(max_retries=self.max_retries, countdown=retry.countdown)

    def process_impl_within_lock(
        self,
        db_session,
        repoid: int,
        commitid: str,
        commit_yaml: UserYaml,
        params: UploadArguments,
        previous_result: list[dict[str, Any]],
    ):
        log.info(
            "Running bundle analysis processor",
            extra={
                "commit_yaml": commit_yaml,
                "params": params,
                "parent_task": self.request.parent_id,
            },
        )

        commit = (
            db_session.query(Commit).filter_by(repoid=repoid, commitid=commitid).first()
        )
        assert commit, "commit not found"

        report_service = BundleAnalysisReportService(commit_yaml)

        # previous_result is the list of processing results from prior processor tasks
        # (they get accumulated as we execute each task in succession)
        processing_results = (
            previous_result if isinstance(previous_result, list) else []
        )

        # these are populated in the upload task
        # unless when this task is called on a non-BA upload then we have to create an empty upload
        upload_id, carriedforward = params.get("upload_id"), False
        if upload_id is not None:
            upload = db_session.query(Upload).filter_by(id_=upload_id).first()
        else:
            # This processor task handles caching for reports. When the 'upload' parameter is missing,
            # it indicates this task was triggered by a non-BA upload.
            #
            # To prevent redundant caching of the same parent report:
            # 1. We first check if a BA report already exists for this commit
            # 2. We then verify there are uploads associated with it that aren't in an error state
            #
            # If both conditions are met, we can exit the task early since the caching was likely
            # already handled. Otherwise, we need to:
            # 1. Create a new BA report and upload
            # 2. Proceed with caching data from the parent report
            commit_report = (
                db_session.query(CommitReport)
                .filter_by(
                    commit_id=commit.id,
                    report_type=ReportType.BUNDLE_ANALYSIS.value,
                )
                .first()
            )
            if commit_report:
                upload_states = [upload.state for upload in commit_report.uploads]
                if upload_states and any(
                    upload_state != "error" for upload_state in upload_states
                ):
                    log.info(
                        "Bundle analysis report already exists for commit, skipping carryforward",
                        extra={
                            "repoid": commit.repoid,
                            "commit": commit.commitid,
                        },
                    )
                    return processing_results
            else:
                # If the commit report does not exist, we will create a new one
                commit_report = report_service.initialize_and_save_report(commit)

            upload = report_service.create_report_upload({"url": ""}, commit_report)
            carriedforward = True

        assert upload is not None

        # Override base commit of comparisons with a custom commit SHA if applicable
        compare_sha: str | None = cast(
            str | None, params.get("bundle_analysis_compare_sha")
        )

        result: ProcessingResult | None = None
        try:
            log.info(
                "Processing bundle analysis upload",
                extra={
                    "repoid": repoid,
                    "commit": commitid,
                    "commit_yaml": commit_yaml,
                    "params": params,
                    "upload_id": upload.id_,
                    "parent_task": self.request.parent_id,
                    "compare_sha": compare_sha,
                },
            )
            assert params.get("commit") == commit.commitid

            result = report_service.process_upload(commit, upload, compare_sha)
            if result.error and result.error.is_retryable:
                if self._has_exceeded_max_attempts(self.max_retries):
                    _log_max_retries_exceeded(
                        commitid=commitid,
                        repoid=repoid,
                        attempts=self.attempts,
                        max_retries=self.max_retries,
                    )
                    try:
                        result.update_upload(carriedforward=carriedforward)
                        db_session.commit()
                    except Exception:
                        log.exception(
                            "Failed to update and commit upload error state after max retries exceeded",
                            extra={
                                "commit": commitid,
                                "repoid": repoid,
                                "upload_id": upload.id_,
                            },
                        )
                        _set_upload_error_and_commit(
                            db_session, upload, commitid, repoid, log_suffix=" fallback"
                        )
                    return previous_result
                log.warn(
                    "Attempting to retry bundle analysis upload",
                    extra={
                        "commitid": commitid,
                        "repoid": repoid,
                        "commit_yaml": commit_yaml,
                        "params": params,
                        "result": result.as_dict(),
                    },
                )
                self.retry(
                    max_retries=self.max_retries,
                    countdown=30 * (2**self.request.retries),
                )
            result.update_upload(carriedforward=carriedforward)
            db_session.commit()

            processing_results.append(result.as_dict())
        except (CeleryError, SoftTimeLimitExceeded):
            # This generally happens when the task needs to be retried because we attempt to access
            # the upload before it is saved to GCS. Anecdotally, it takes around 30s for it to be
            # saved, but if the BA processor runs before that we will error and need to retry.
            raise
        except Exception:
            log.exception(
                "Unable to process bundle analysis upload",
                extra={
                    "repoid": repoid,
                    "commit": commitid,
                    "commit_yaml": commit_yaml,
                    "params": params,
                    "upload_id": upload.id_,
                    "parent_task": self.request.parent_id,
                },
            )
            _set_upload_error_and_commit(db_session, upload, commitid, repoid)
            return processing_results
        finally:
            if result is not None and result.bundle_report:
                result.bundle_report.cleanup()
            if result is not None and result.previous_bundle_report:
                result.previous_bundle_report.cleanup()

        # Create task to save bundle measurements
        self.app.tasks[bundle_analysis_save_measurements_task_name].apply_async(
            kwargs={
                "commitid": commit.commitid,
                "repoid": commit.repoid,
                "uploadid": upload.id_,
                "commit_yaml": commit_yaml.to_dict(),
                "previous_result": processing_results,
            }
        )

        log.info(
            "Finished bundle analysis processor",
            extra={
                "repoid": repoid,
                "commit": commitid,
                "commit_yaml": commit_yaml,
                "params": params,
                "results": processing_results,
                "parent_task": self.request.parent_id,
            },
        )

        return processing_results


RegisteredBundleAnalysisProcessorTask = celery_app.register_task(
    BundleAnalysisProcessorTask()
)
bundle_analysis_processor_task = celery_app.tasks[
    RegisteredBundleAnalysisProcessorTask.name
]
