import logging

import sentry_sdk
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy.orm import Session as DbSession

from app import celery_app
from services.processing.processing import UploadArguments, process_upload
from services.report import ProcessingError
from shared.celery_config import upload_processor_task_name
from shared.config import get_config
from shared.django_apps.upload_breadcrumbs.models import Errors, Milestones
from shared.upload.constants import UploadErrorCode
from shared.yaml import UserYaml
from tasks.base import BaseCodecovTask, clamp_retry_countdown

log = logging.getLogger(__name__)

# Maximum retries for file-not-found errors before giving up
MAX_FILE_NOT_FOUND_RETRIES = 1
# Base delay (seconds) for exponential backoff retry strategy
FIRST_RETRY_DELAY = 20


def UPLOAD_PROCESSING_LOCK_NAME(repoid: int, commitid: str) -> str:
    """Generate the Redis lock name for upload processing.

    Only one processing task can hold this lock at a time since merging reports
    requires exclusive access. Used by Upload, Notify, and Processor tasks to
    coordinate processing for a given commit.
    """
    return f"upload_processing_lock_{repoid}_{commitid}"


class UploadProcessorTask(BaseCodecovTask, name=upload_processor_task_name):
    """Processes user uploads and saves results to database and storage.

    This is the second task in the upload processing pipeline (see `tasks.upload.UploadTask`).

    Processing steps:
    1. Fetch uploaded report from storage (MinIO or Redis)
    2. Run through language processors to generate coverage reports
    3. Merge generated reports with existing commit reports
    4. Save results to database

    Handles multiple reports per task but typically receives a small number of uploads.
    """

    acks_late = get_config("setup", "tasks", "upload", "acks_late", default=False)

    def run_impl(
        self,
        db_session: DbSession,
        *args,
        repoid: int,
        commitid: str,
        commit_yaml: dict,
        arguments: UploadArguments,
        **kwargs,
    ):
        log.info(
            "Received upload processor task",
            extra={"arguments": arguments, "commit_yaml": commit_yaml},
        )

        self._call_upload_breadcrumb_task(
            commit_sha=commitid,
            repo_id=repoid,
            milestone=Milestones.PROCESSING_UPLOAD,
            upload_ids=[arguments["upload_id"]],
        )

        def on_processing_error(error: ProcessingError):
            """Handle processing errors by logging breadcrumbs and retrying if applicable."""
            match error.code:
                case UploadErrorCode.FILE_NOT_IN_STORAGE:
                    ub_error = Errors.FILE_NOT_IN_STORAGE
                case UploadErrorCode.REPORT_EXPIRED:
                    ub_error = Errors.REPORT_EXPIRED
                case UploadErrorCode.REPORT_EMPTY:
                    ub_error = Errors.REPORT_EMPTY
                case UploadErrorCode.PROCESSING_TIMEOUT:
                    ub_error = Errors.TASK_TIMED_OUT
                case (
                    UploadErrorCode.UNSUPPORTED_FILE_FORMAT
                    | UploadErrorCode.UNKNOWN_PROCESSING
                ):
                    ub_error = Errors.UNSUPPORTED_FORMAT
                case _:
                    ub_error = Errors.UNKNOWN

            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.PROCESSING_UPLOAD,
                upload_ids=[arguments["upload_id"]],
                error=ub_error,
                error_text=error.error_text,
            )

            # Retry with exponential backoff if error is retryable and under max retries
            if error.is_retryable and self.request.retries < error.max_retries:
                countdown = FIRST_RETRY_DELAY * (2**self.request.retries)
                log.info(
                    "Scheduling a retry due to retryable error",
                    extra={
                        "countdown": countdown,
                        "max_retries": error.max_retries,
                        "retry_count": self.request.retries,
                        "error": error.as_dict(),
                    },
                )
                self._call_upload_breadcrumb_task(
                    commit_sha=commitid,
                    repo_id=repoid,
                    milestone=Milestones.PROCESSING_UPLOAD,
                    upload_ids=[arguments["upload_id"]],
                    error=Errors.INTERNAL_RETRYING,
                )
                self.retry(
                    max_retries=error.max_retries,
                    countdown=clamp_retry_countdown(countdown),
                )
            elif error.is_retryable and self.request.retries >= error.max_retries:
                sentry_sdk.capture_exception(
                    error.error_class(error.error_text),
                    contexts={
                        "upload_details": {
                            "commitid": commitid,
                            "error_code": error.code,
                            "repoid": repoid,
                            "retry_count": self.request.retries,
                            "storage_location": error.params.get("location"),
                            "upload_id": arguments.get("upload_id"),
                        }
                    },
                )

        try:
            return process_upload(
                on_processing_error,
                db_session,
                int(repoid),
                commitid,
                UserYaml(commit_yaml),
                arguments,
            )
        except SoftTimeLimitExceeded as e:
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.PROCESSING_UPLOAD,
                upload_ids=[arguments["upload_id"]],
                error=Errors.TASK_TIMED_OUT,
            )
            raise
        except Exception as e:
            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.PROCESSING_UPLOAD,
                upload_ids=[arguments["upload_id"]],
                error=Errors.UNKNOWN,
                error_text=repr(e),
            )
            raise


RegisteredUploadTask = celery_app.register_task(UploadProcessorTask())
upload_processor_task = celery_app.tasks[RegisteredUploadTask.name]
