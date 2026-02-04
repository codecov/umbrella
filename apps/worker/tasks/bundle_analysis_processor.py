import logging
import os
import tempfile
from contextlib import contextmanager
from typing import Any, cast

from celery.exceptions import CeleryError, SoftTimeLimitExceeded

from app import celery_app
from database.enums import ReportType
from database.models import Commit, CommitReport, Upload
from services.bundle_analysis.report import (
    BundleAnalysisReportService,
    ProcessingResult,
)
from services.lock_manager import LockManager, LockRetry, LockType
from services.processing.types import UploadArguments
from shared.api_archive.archive import ArchiveService
from shared.bundle_analysis.storage import get_bucket_name
from shared.celery_config import (
    BUNDLE_ANALYSIS_PROCESSOR_MAX_RETRIES,
    bundle_analysis_processor_task_name,
)
from shared.reports.enums import UploadState
from shared.storage.exceptions import FileNotInStorageError
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask
from tasks.bundle_analysis_save_measurements import (
    bundle_analysis_save_measurements_task_name,
)

log = logging.getLogger(__name__)


@contextmanager
def temporary_upload_file(db_session, repoid: int, upload_params: UploadArguments):
    """
    Context manager that pre-downloads a bundle upload file to a temporary location.

    This optimization downloads the file from GCS before acquiring the commit-level
    lock, reducing lock contention by 30-50% per bundle. The temporary file is
    automatically cleaned up when exiting the context, regardless of success or failure.

    Args:
        db_session: Database session for querying upload records
        repoid: Repository ID for logging
        upload_params: Upload parameters containing the upload_id

    Yields:
        str | None: Path to the downloaded temporary file, or None if download failed
                   or upload_id is not available

    Example:
        with temporary_upload_file(db_session, repoid, params) as local_path:
            if local_path:
                # Process the pre-downloaded file
                process_upload(commit, upload, pre_downloaded_path=local_path)
            # File is automatically cleaned up here
    """
    local_path = None

    try:
        upload_id = upload_params.get("upload_id")
        if upload_id is None:
            yield None
            return

        upload = db_session.query(Upload).filter_by(id_=upload_id).first()
        if upload is None or not upload.storage_path:
            yield None
            return

        # Get storage service from the repository
        commit = upload.report.commit
        archive_service = ArchiveService(commit.repository)
        storage_service = archive_service.storage

        # Download the upload file to a temporary location
        _, local_path = tempfile.mkstemp()

        try:
            with open(local_path, "wb") as f:
                storage_service.read_file(
                    get_bucket_name(), upload.storage_path, file_obj=f
                )

            log.info(
                "Pre-downloaded upload file before lock acquisition",
                extra={
                    "repoid": repoid,
                    "upload_id": upload_id,
                    "local_path": local_path,
                },
            )
            yield local_path

        except FileNotInStorageError:
            # File not yet available in storage, will retry inside lock
            log.info(
                "Upload file not yet available in storage for pre-download",
                extra={"repoid": repoid, "upload_id": upload_id},
            )
            yield None

        except Exception as e:
            log.warning(
                "Failed to pre-download upload file",
                extra={"repoid": repoid, "upload_id": upload_id, "error": str(e)},
            )
            yield None

    finally:
        # Ensure temporary file is always cleaned up
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
                log.debug(
                    "Cleaned up temporary upload file",
                    extra={"local_path": local_path},
                )
            except OSError as e:
                log.warning(
                    "Failed to clean up temporary file",
                    extra={"local_path": local_path, "error": str(e)},
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

        # Pre-download the upload file before acquiring lock to reduce lock contention.
        # This allows the GCS download to happen while another worker may hold the lock.
        # The context manager ensures automatic cleanup of the temporary file.
        # Note: We still use per-commit locking because all bundles share the same
        # SQLite report file, which requires serialized access to prevent data loss.
        with temporary_upload_file(db_session, repoid, params) as pre_downloaded_path:
            lock_manager = LockManager(
                repoid=repoid,
                commitid=commitid,
                report_type=ReportType.BUNDLE_ANALYSIS,
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
                        pre_downloaded_path=pre_downloaded_path,
                    )
            except LockRetry as retry:
                # Check max retries using self.attempts (includes visibility timeout re-deliveries)
                # This prevents infinite retry loops when max retries are exceeded
                if self._has_exceeded_max_attempts(self.max_retries):
                    max_attempts = (
                        self.max_retries + 1 if self.max_retries is not None else None
                    )
                    log.error(
                        "Bundle analysis processor exceeded max retries",
                        extra={
                            "attempts": self.attempts,
                            "commitid": commitid,
                            "max_attempts": max_attempts,
                            "max_retries": self.max_retries,
                            "repoid": repoid,
                            "retry_num": self.request.retries,
                        },
                    )
                    # Return previous_result to preserve chain behavior when max retries exceeded
                    # This allows the chain to continue with partial results rather than failing entirely
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
        pre_downloaded_path: str | None = None,
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

            result = report_service.process_upload(
                commit, upload, compare_sha, pre_downloaded_path
            )
            if result.error and result.error.is_retryable:
                if self._has_exceeded_max_attempts(self.max_retries):
                    attempts = self.attempts
                    max_attempts = self.max_retries + 1
                    log.error(
                        "Bundle analysis processor exceeded max retries",
                        extra={
                            "attempts": attempts,
                            "commitid": commitid,
                            "max_attempts": max_attempts,
                            "max_retries": self.max_retries,
                            "repoid": repoid,
                        },
                    )
                    # Update upload state to "error" before returning
                    try:
                        result.update_upload(carriedforward=carriedforward)
                        db_session.commit()
                    except Exception:
                        # Log commit failure but don't raise - we've already set the error state
                        log.exception(
                            "Failed to update and commit upload error state after max retries exceeded",
                            extra={
                                "commit": commitid,
                                "repoid": repoid,
                                "upload_id": upload.id_,
                            },
                        )
                        # Manually set upload state to error as fallback
                        upload.state_id = UploadState.ERROR.db_id
                        upload.state = "error"
                        try:
                            db_session.commit()
                        except Exception:
                            log.exception(
                                "Failed to commit upload error state fallback",
                                extra={
                                    "commit": commitid,
                                    "repoid": repoid,
                                    "upload_id": upload.id_,
                                },
                            )
                    return processing_results
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
            upload.state_id = UploadState.ERROR.db_id
            upload.state = "error"
            try:
                db_session.commit()
            except Exception:
                # Log commit failure but preserve original exception
                log.exception(
                    "Failed to commit upload error state",
                    extra={
                        "commit": commitid,
                        "repoid": repoid,
                        "upload_id": upload.id_,
                    },
                )
            raise
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
