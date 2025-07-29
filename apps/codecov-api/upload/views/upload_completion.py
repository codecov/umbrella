import logging
from collections.abc import Callable
from typing import Any

from rest_framework import status
from rest_framework.generics import CreateAPIView
from rest_framework.request import Request
from rest_framework.response import Response

from codecov_auth.authentication.repo_auth import (
    GitHubOIDCTokenAuthentication,
    GlobalTokenAuthentication,
    OrgLevelTokenAuthentication,
    RepositoryLegacyTokenAuthentication,
    UploadTokenRequiredAuthenticationCheck,
    repo_auth_custom_exception_handler,
)
from reports.models import ReportSession
from services.task import TaskService
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
)
from shared.metrics import inc_counter
from upload.helpers import (
    generate_upload_prometheus_metrics_labels,
    upload_breadcrumb_context,
)
from upload.metrics import API_UPLOAD_COUNTER
from upload.views.base import GetterMixin
from upload.views.uploads import CanDoCoverageUploadsPermission

log = logging.getLogger(__name__)


class UploadCompletionView(GetterMixin, CreateAPIView):
    permission_classes = [CanDoCoverageUploadsPermission]
    authentication_classes = [
        UploadTokenRequiredAuthenticationCheck,
        GlobalTokenAuthentication,
        OrgLevelTokenAuthentication,
        GitHubOIDCTokenAuthentication,
        RepositoryLegacyTokenAuthentication,
    ]

    def get_exception_handler(
        self,
    ) -> Callable[[Exception, dict[str, Any]], Response | None]:
        return repo_auth_custom_exception_handler

    def post(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        endpoint = Endpoints.UPLOAD_COMPLETION
        milestone = Milestones.NOTIFICATIONS_TRIGGERED
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="upload_complete",
                request=self.request,
                is_shelter_request=self.is_shelter_request(),
                position="start",
            ),
        )
        repo = self.get_repo()

        with upload_breadcrumb_context(
            initial_breadcrumb=False,
            commit_sha=self.kwargs.get("commit_sha"),
            repo_id=repo.repoid,
            milestone=milestone,
            endpoint=endpoint,
            error=Errors.COMMIT_NOT_FOUND,
        ):
            commit = self.get_commit(repo)

        uploads_queryset = ReportSession.objects.filter(
            report__commit=commit,
            report__code=None,
        )
        uploads_count = uploads_queryset.count()
        if not uploads_queryset or uploads_count == 0:
            log.info(
                "Cannot trigger notifications as we didn't find any uploads for the provided commit",
                extra={
                    "repo": repo.name,
                    "commit": commit.commitid,
                    "pullid": commit.pullid,
                },
            )
            TaskService().upload_breadcrumb(
                commit_sha=commit.commitid,
                repo_id=repo.repoid,
                breadcrumb_data=BreadcrumbData(
                    milestone=milestone,
                    endpoint=endpoint,
                    error=Errors.UPLOAD_NOT_FOUND,
                ),
            )
            return Response(
                data={
                    "uploads_total": 0,
                    "uploads_success": 0,
                    "uploads_processing": 0,
                    "uploads_error": 0,
                },
                status=status.HTTP_404_NOT_FOUND,
            )

        in_progress_uploads = 0
        errored_uploads = 0
        for upload in uploads_queryset:
            # upload is still processing
            if not upload.state:
                in_progress_uploads += 1
            elif upload.state == "error":
                errored_uploads += 1

        TaskService().manual_upload_completion_trigger(repo.repoid, commit.commitid)
        TaskService().upload_breadcrumb(
            commit_sha=commit.commitid,
            repo_id=repo.repoid,
            breadcrumb_data=BreadcrumbData(
                milestone=milestone,
                endpoint=endpoint,
            ),
        )
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="upload_complete",
                request=self.request,
                repository=repo,
                is_shelter_request=self.is_shelter_request(),
                position="end",
            ),
        )
        return Response(
            data={
                "uploads_total": uploads_count,
                "uploads_success": uploads_count
                - in_progress_uploads
                - errored_uploads,
                "uploads_processing": in_progress_uploads,
                "uploads_error": errored_uploads,
            },
            status=status.HTTP_200_OK,
        )
