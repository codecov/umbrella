import logging
from collections.abc import Callable
from typing import Any, cast

from django.conf import settings
from rest_framework import status
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from codecov_auth.authentication.repo_auth import (
    GitHubOIDCTokenAuthentication,
    GlobalTokenAuthentication,
    OrgLevelTokenAuthentication,
    RepositoryLegacyTokenAuthentication,
    TokenlessAuthentication,
    UploadTokenRequiredAuthenticationCheck,
    repo_auth_custom_exception_handler,
)
from services.task.task import TaskService
from shared.api_archive.archive import ArchiveService
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
)
from shared.metrics import inc_counter
from upload.helpers import (
    generate_upload_prometheus_metrics_labels,
    get_cli_uploader_string,
)
from upload.metrics import API_UPLOAD_COUNTER
from upload.serializers import (
    CommitReportSerializer,
    CommitSerializer,
    UploadSerializer,
)
from upload.throttles import UploadsPerCommitThrottle, UploadsPerWindowThrottle
from upload.views.base import GetterMixin
from upload.views.commits import create_commit
from upload.views.reports import create_report
from upload.views.uploads import (
    CanDoCoverageUploadsPermission,
    create_upload,
    get_token_for_analytics,
)

log = logging.getLogger(__name__)


class UploadCoverageView(GetterMixin, APIView):
    permission_classes = [CanDoCoverageUploadsPermission]
    authentication_classes = [
        UploadTokenRequiredAuthenticationCheck,
        GlobalTokenAuthentication,
        OrgLevelTokenAuthentication,
        GitHubOIDCTokenAuthentication,
        RepositoryLegacyTokenAuthentication,
        TokenlessAuthentication,
    ]
    throttle_classes = [UploadsPerCommitThrottle, UploadsPerWindowThrottle]

    def get_exception_handler(
        self,
    ) -> Callable[[Exception, dict[str, Any]], Response | None]:
        return repo_auth_custom_exception_handler

    def emit_metrics(self, position: str) -> None:
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="upload_coverage",
                request=self.request,
                is_shelter_request=self.is_shelter_request(),
                position=position,
            ),
        )

    def post(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        self.emit_metrics(position="start")
        endpoint = Endpoints.UPLOAD_COVERAGE
        uploader = get_cli_uploader_string(request)

        # Create commit
        create_commit_data = {
            "branch": request.data.get("branch"),
            "commitid": request.data.get("commitid"),
            "parent_commit_id": request.data.get("parent_commit_id"),
            "pullid": request.data.get("pullid"),
        }
        commit_serializer = CommitSerializer(data=create_commit_data)
        if not commit_serializer.is_valid():
            return Response(
                commit_serializer.errors, status=status.HTTP_400_BAD_REQUEST
            )

        repository = self.get_repo()
        self.emit_metrics(position="create_commit")
        commit = create_commit(commit_serializer, repository, endpoint, uploader)

        log.info(
            "Request to create new coverage upload",
            extra={
                "repo": repository.name,
                "commit": commit.commitid,
            },
        )

        # Create report
        TaskService().upload_breadcrumb(
            commit_sha=commit.commitid,
            repo_id=repository.repoid,
            breadcrumb_data=BreadcrumbData(
                milestone=Milestones.PREPARING_FOR_REPORT,
                endpoint=endpoint,
                uploader=uploader,
            ),
        )
        commit_report_data = {
            "code": request.data.get("code"),
        }
        commit_report_serializer = CommitReportSerializer(data=commit_report_data)
        if not commit_report_serializer.is_valid():
            TaskService().upload_breadcrumb(
                commit_sha=commit.commitid,
                repo_id=repository.repoid,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.PREPARING_FOR_REPORT,
                    endpoint=endpoint,
                    uploader=uploader,
                    error=Errors.BAD_REQUEST,
                ),
            )
            return Response(
                commit_report_serializer.errors, status=status.HTTP_400_BAD_REQUEST
            )

        self.emit_metrics(position="create_report")
        report = create_report(
            commit_report_serializer, repository, commit, endpoint, uploader
        )

        # Do upload
        upload_data = {
            "ci_service": request.data.get("ci_service"),
            "ci_url": request.data.get("ci_url"),
            "env": request.data.get("env"),
            "flags": request.data.get("flags"),
            "job_code": request.data.get("job_code"),
            "name": request.data.get("name"),
            "version": request.data.get("version"),
        }

        if self.is_shelter_request():
            upload_data["storage_path"] = request.data.get("storage_path")

        upload_serializer = UploadSerializer(data=upload_data)
        if not upload_serializer.is_valid():
            TaskService().upload_breadcrumb(
                commit_sha=commit.commitid,
                repo_id=repository.repoid,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.WAITING_FOR_COVERAGE_UPLOAD,
                    endpoint=endpoint,
                    uploader=uploader,
                    error=Errors.BAD_REQUEST,
                ),
            )
            return Response(
                upload_serializer.errors, status=status.HTTP_400_BAD_REQUEST
            )

        self.emit_metrics(position="create_upload")
        upload = create_upload(
            upload_serializer,
            repository,
            commit,
            report,
            self.is_shelter_request(),
            get_token_for_analytics(commit, self.request),
            endpoint,
            uploader,
        )

        self.emit_metrics(position="end")

        if not upload:
            return Response(
                upload_serializer.errors, status=status.HTTP_400_BAD_REQUEST
            )

        commitid = upload.report.commit.commitid
        upload_repository = upload.report.commit.repository
        url = f"{settings.CODECOV_DASHBOARD_URL}/{upload_repository.author.service}/{upload_repository.author.username}/{upload_repository.name}/commit/{commitid}"
        archive_service = ArchiveService(upload_repository)
        raw_upload_location = archive_service.create_presigned_put(
            cast(str, upload.storage_path)
        )

        TaskService().upload_breadcrumb(
            commit_sha=commit.commitid,
            repo_id=repository.repoid,
            breadcrumb_data=BreadcrumbData(
                milestone=Milestones.WAITING_FOR_COVERAGE_UPLOAD,
                endpoint=endpoint,
                uploader=uploader,
            ),
        )

        return Response(
            {
                "external_id": str(upload.external_id),
                "created_at": upload.created_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "raw_upload_location": raw_upload_location,
                "url": url,
            },
            status=status.HTTP_201_CREATED,
        )
