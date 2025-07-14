import logging

from sqlalchemy.orm import Session

from app import celery_app
from shared.celery_config import upload_breadcrumb_task_name
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)
from shared.metrics import Counter, inc_counter
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)

UPLOAD_BREADCRUMB_ENDPOINT_COUNTER = Counter(
    "upload_breadcrumbs_endpoint_total",
    "Number of upload breadcrumbs by endpoint",
    ["endpoint"],
)

UPLOAD_BREADCRUMB_MILESTONE_COUNTER = Counter(
    "upload_breadcrumbs_milestone_total",
    "Number of upload breadcrumbs by milestone",
    ["milestone"],
)

UPLOAD_BREADCRUMB_ERROR_COUNTER = Counter(
    "upload_breadcrumbs_error_total",
    "Number of upload breadcrumbs by error",
    ["error"],
)


class UploadBreadcrumbTask(BaseCodecovTask, name=upload_breadcrumb_task_name):
    """
    This task is for creating upload breadcrumbs and is triggered by events
    in the upload flow from both API and worker.
    """

    def run_impl(
        self,
        _db_session: Session,
        *,
        commit_sha: str,
        repo_id: int,
        breadcrumb_data: BreadcrumbData,
        upload_ids: list[str] = [],
        sentry_trace_id: str | None = None,
        **kwargs,
    ):
        if breadcrumb_data.endpoint:
            inc_counter(
                UPLOAD_BREADCRUMB_ENDPOINT_COUNTER,
                labels={"endpoint": Endpoints(breadcrumb_data.endpoint).label},
            )

        if breadcrumb_data.error:
            inc_counter(
                UPLOAD_BREADCRUMB_ERROR_COUNTER,
                labels={
                    "error": Errors(breadcrumb_data.error).label,
                },
            )
        if breadcrumb_data.milestone:
            inc_counter(
                UPLOAD_BREADCRUMB_MILESTONE_COUNTER,
                labels={"milestone": Milestones(breadcrumb_data.milestone).label},
            )

        UploadBreadcrumb.objects.create(
            commit_sha=commit_sha,
            repo_id=repo_id,
            breadcrumb_data=breadcrumb_data.model_dump(),
            upload_ids=upload_ids,
            sentry_trace_id=sentry_trace_id,
        )
        return {"successful": True}


RegisteredUploadBreadcrumbTask = celery_app.register_task(UploadBreadcrumbTask())
upload_breadcrumb_task = celery_app.tasks[RegisteredUploadBreadcrumbTask.name]
