import logging

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
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

MAX_RETRIES = 5
FIRST_RETRY_DELAY = 20


def UPLOAD_PROCESSING_LOCK_NAME(repoid: int, commitid: str) -> str:
    """The upload_processing_lock.
    Only a single processing task may possess this lock at a time, because merging
    reports requires exclusive access to the report.

    This is used by the Upload and Notify tasks as well to verify if an upload
    for the commit is currently being processed.
    """
    return f"upload_processing_lock_{repoid}_{commitid}"


class UploadProcessorTask(BaseCodecovTask, name=upload_processor_task_name):
    """This is the second task of the series of tasks designed to process an `upload` made
    by the user

    To see more about the whole picture, see `tasks.upload.UploadTask`

    This task processes each user `upload`, and saves the results to db and minio storage

    The steps are:
        - Fetching the user uploaded report (from minio, or sometimes redis)
        - Running them through the language processors, and obtaining reports from that
        - Merging the generated reports to the already existing commit processed reports
        - Saving all that info to the database

    This task doesn't limit how many individual reports it receives for processing. It deals
        with as many as possible. But it is not expected that this task will receive a big
        number of `uploads` to be processed
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
            ub_error = None
            ub_error_text = None
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
                    ub_error_text = str(error.params)

            self._call_upload_breadcrumb_task(
                commit_sha=commitid,
                repo_id=repoid,
                milestone=Milestones.PROCESSING_UPLOAD,
                upload_ids=[arguments["upload_id"]],
                error=ub_error,
                error_text=ub_error_text,
            )

            # the error is only retried on the first pass
            if error.is_retryable and self.request.retries == 0:
                log.info(
                    "Scheduling a retry due to retryable error",
                    extra={"error": error.as_dict()},
                )
                self._call_upload_breadcrumb_task(
                    commit_sha=commitid,
                    repo_id=repoid,
                    milestone=Milestones.PROCESSING_UPLOAD,
                    upload_ids=[arguments["upload_id"]],
                    error=Errors.INTERNAL_RETRYING,
                )
                self.retry(max_retries=MAX_RETRIES, countdown=FIRST_RETRY_DELAY)

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
                error_text=str(e),
            )
            raise


RegisteredUploadTask = celery_app.register_task(UploadProcessorTask())
upload_processor_task = celery_app.tasks[RegisteredUploadTask.name]
