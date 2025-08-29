import logging
from collections.abc import Callable
from typing import Any, cast

from rest_framework import status
from rest_framework.generics import CreateAPIView
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.serializers import BaseSerializer
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
from core.models import Commit, Repository
from reports.models import CommitReport
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
    get_cli_uploader_string,
    upload_breadcrumb_context,
    validate_activated_repo,
)
from upload.metrics import API_UPLOAD_COUNTER
from upload.serializers import CommitReportSerializer
from upload.views.base import GetterMixin
from upload.views.uploads import CanDoCoverageUploadsPermission

log = logging.getLogger(__name__)


def create_report(
    serializer: CommitReportSerializer,
    repository: Repository,
    commit: Commit,
    endpoint: Endpoints,
    uploader: str,
) -> CommitReport:
    code = serializer.validated_data.get("code")
    if code == "default":
        serializer.validated_data["code"] = None
    instance, was_created = serializer.save(
        commit_id=commit.id,
        report_type=CommitReport.ReportType.COVERAGE,
    )
    if was_created:
        TaskService().preprocess_upload(repository.repoid, commit.commitid)
    else:
        TaskService().upload_breadcrumb(
            commit_sha=commit.commitid,
            repo_id=repository.repoid,
            breadcrumb_data=BreadcrumbData(
                milestone=Milestones.READY_FOR_REPORT,
                endpoint=endpoint,
                uploader=uploader,
            ),
        )
    return instance


class ReportViews(GetterMixin, CreateAPIView):
    serializer_class = CommitReportSerializer
    permission_classes = [CanDoCoverageUploadsPermission]
    authentication_classes = [
        UploadTokenRequiredAuthenticationCheck,
        GlobalTokenAuthentication,
        OrgLevelTokenAuthentication,
        GitHubOIDCTokenAuthentication,
        RepositoryLegacyTokenAuthentication,
        TokenlessAuthentication,
    ]

    def get_exception_handler(
        self,
    ) -> Callable[[Exception, dict[str, Any]], Response | None]:
        return repo_auth_custom_exception_handler

    def perform_create(self, serializer: BaseSerializer) -> None:
        endpoint = Endpoints.CREATE_REPORT
        uploader = get_cli_uploader_string(self.request)
        milestone = Milestones.PREPARING_FOR_REPORT
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="create_report",
                request=self.request,
                is_shelter_request=self.is_shelter_request(),
                position="start",
            ),
        )
        repository = self.get_repo()

        with upload_breadcrumb_context(
            initial_breadcrumb=True,
            commit_sha=self.kwargs.get("commit_sha"),
            repo_id=repository.repoid,
            milestone=milestone,
            endpoint=endpoint,
            uploader=uploader,
            error=Errors.REPO_DEACTIVATED,
        ):
            validate_activated_repo(repository)

        with upload_breadcrumb_context(
            initial_breadcrumb=False,
            commit_sha=self.kwargs.get("commit_sha"),
            repo_id=repository.repoid,
            milestone=milestone,
            endpoint=endpoint,
            uploader=uploader,
            error=Errors.COMMIT_NOT_FOUND,
        ):
            commit = self.get_commit(repository)

        log.info(
            "Request to create new report",
            extra={"repo": repository.name, "commit": commit.commitid},
        )
        create_report(
            cast(CommitReportSerializer, serializer),
            repository,
            commit,
            endpoint,
            uploader,
        )

        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="create_report",
                request=self.request,
                repository=repository,
                is_shelter_request=self.is_shelter_request(),
                position="end",
            ),
        )


EMPTY_RESPONSE = {
    "state": "completed",
    "result": {
        "state": "deprecated",
        "message": 'The "local upload" functionality has been deprecated.',
    },
}


class ReportResultsView(APIView, GetterMixin):
    permission_classes = [CanDoCoverageUploadsPermission]
    authentication_classes = [
        UploadTokenRequiredAuthenticationCheck,
        GlobalTokenAuthentication,
        OrgLevelTokenAuthentication,
        GitHubOIDCTokenAuthentication,
        RepositoryLegacyTokenAuthentication,
        TokenlessAuthentication,
    ]

    def get_exception_handler(
        self,
    ) -> Callable[[Exception, dict[str, Any]], Response | None]:
        return repo_auth_custom_exception_handler

    def get(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return Response(EMPTY_RESPONSE)

    def post(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return Response(EMPTY_RESPONSE, status=status.HTTP_201_CREATED)
