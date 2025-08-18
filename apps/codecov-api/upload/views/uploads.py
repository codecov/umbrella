import logging
import uuid
from collections.abc import Callable
from typing import Any, cast

from django.utils import timezone
from rest_framework.generics import CreateAPIView
from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.serializers import BaseSerializer
from rest_framework.views import APIView

from codecov_auth.authentication.repo_auth import (
    GitHubOIDCTokenAuthentication,
    GlobalTokenAuthentication,
    OIDCTokenRepositoryAuth,
    OrgLevelTokenAuthentication,
    OrgLevelTokenRepositoryAuth,
    RepositoryLegacyTokenAuthentication,
    TokenlessAuth,
    TokenlessAuthentication,
    UploadTokenRequiredAuthenticationCheck,
    repo_auth_custom_exception_handler,
)
from codecov_auth.authentication.types import RepositoryAuthInterface
from codecov_auth.models import OrganizationLevelToken
from core.models import Commit, Repository
from reports.models import CommitReport, ReportSession
from services.analytics import AnalyticsService
from services.task.task import TaskService
from shared.api_archive.archive import ArchiveService, MinioEndpoints
from shared.django_apps.reports.models import ReportType
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
)
from shared.events.amplitude import UNKNOWN_USER_OWNERID, AmplitudeEventPublisher
from shared.helpers.redis import get_redis_connection
from shared.metrics import inc_counter
from shared.upload.types import UploaderType
from shared.upload.utils import insert_coverage_measurement
from upload.helpers import (
    dispatch_upload_task,
    generate_upload_prometheus_metrics_labels,
    upload_breadcrumb_context,
    validate_activated_repo,
)
from upload.metrics import API_UPLOAD_COUNTER
from upload.serializers import UploadSerializer
from upload.throttles import UploadsPerCommitThrottle, UploadsPerWindowThrottle
from upload.views.base import GetterMixin

log = logging.getLogger(__name__)


def create_upload(
    serializer: UploadSerializer,
    repository: Repository,
    commit: Commit,
    report: CommitReport,
    is_shelter_request: bool,
    analytics_token: str,
    endpoint: Endpoints,
) -> ReportSession:
    version: str | None = (
        serializer.validated_data["version"]
        if "version" in serializer.validated_data
        else None
    )
    with upload_breadcrumb_context(
        initial_breadcrumb=False,
        commit_sha=commit.commitid,
        repo_id=repository.repoid,
        milestone=Milestones.WAITING_FOR_COVERAGE_UPLOAD,
        endpoint=endpoint,
    ):
        archive_service = ArchiveService(repository)
        # only Shelter requests are allowed to set their own `storage_path`

        if not serializer.validated_data.get("storage_path") or not is_shelter_request:
            serializer.validated_data["external_id"] = uuid.uuid4()
            path = MinioEndpoints.raw_with_upload_id.get_path(
                version="v4",
                date=timezone.now().strftime("%Y-%m-%d"),
                repo_hash=archive_service.storage_hash,
                commit_sha=commit.commitid,
                reportid=report.external_id,
                uploadid=serializer.validated_data["external_id"],
            )
            serializer.validated_data["storage_path"] = path
        # Create upload record
        instance: ReportSession = serializer.save(
            repo_id=repository.repoid,
            report_id=report.id,
            upload_extras={"format_version": "v1"},
            state="started",
        )

        # Inserts mirror upload record into measurements table. CLI hits this endpoint
        insert_coverage_measurement(
            owner_id=repository.author.ownerid,
            repo_id=repository.repoid,
            commit_id=commit.id,
            upload_id=instance.id,
            uploader_used=UploaderType.CLI.value,
            private_repo=repository.private,
            report_type=cast(ReportType, report.report_type),
        )

        trigger_upload_task(repository, commit.commitid, instance, report)
        activate_repo(repository, commit)
        send_analytics_data(commit, instance, version, analytics_token)
        return instance


def trigger_upload_task(
    repository: Repository, commit_sha: str, upload: ReportSession, report: CommitReport
) -> None:
    log.info(
        "Triggering upload task",
        extra={"repo": repository.name, "commit": commit_sha, "upload_id": upload.id},
    )
    redis = get_redis_connection()
    task_arguments = {
        "commit": commit_sha,
        "upload_id": upload.id,
        "version": "v4",
        "reportid": str(report.external_id),
    }
    dispatch_upload_task(redis, repository.repoid, task_arguments)


def activate_repo(repository: Repository, commit: Commit) -> None:
    # Only update the fields if needed
    if (
        repository.activated
        and repository.active
        and not repository.deleted
        and repository.coverage_enabled
    ):
        return
    repository.activated = True
    repository.active = True
    repository.deleted = False
    repository.coverage_enabled = True
    repository.save(
        update_fields=[
            "activated",
            "active",
            "deleted",
            "coverage_enabled",
            "updatestamp",
        ]
    )
    AmplitudeEventPublisher().publish(
        "Repository Activated",
        {
            "user_ownerid": commit.author.ownerid
            if commit.author
            else UNKNOWN_USER_OWNERID,
            "ownerid": repository.author.ownerid,
            "repoid": repository.repoid,
            "commitid": commit.id,  # Not commit.commitid, we do not want a commit SHA here.
            "pullid": commit.pullid,
            "upload_type": "Coverage report",
        },
    )


def send_analytics_data(
    commit: Commit, upload: ReportSession, version: str | None, analytics_token: str
) -> None:
    analytics_upload_data = {
        "commit": commit.commitid,
        "branch": commit.branch,
        "pr": commit.pullid,
        "repo": commit.repository.name,
        "repository_name": commit.repository.name,
        "repository_id": commit.repository.repoid,
        "service": commit.repository.service,
        "build": upload.build_code,
        "build_url": upload.build_url,
        # we were previously using upload.flag_names here, and this query might not be optimized
        # we weren't doing it in the legacy endpoint, but in the new one we are, and it may be causing problems
        # therefore we are removing this for now to see if it is the source of the issue
        "flags": "",
        "owner": commit.repository.author.ownerid,
        "token": str(analytics_token),
        "version": version,
        "uploader_type": "CLI",
    }
    AnalyticsService().account_uploaded_coverage_report(
        commit.repository.author.ownerid, analytics_upload_data
    )


def get_token_for_analytics(commit: Commit, request: Request) -> str:
    repo: Repository = commit.repository
    if isinstance(request.auth, TokenlessAuth):
        analytics_token = "tokenless_upload"
    elif isinstance(request.auth, OrgLevelTokenRepositoryAuth):
        org_token_obj = OrganizationLevelToken.objects.filter(owner=repo.author).first()
        analytics_token = str(org_token_obj.token) if org_token_obj else ""
    elif isinstance(request.auth, OIDCTokenRepositoryAuth):
        analytics_token = "oidc_token_upload"
    else:
        analytics_token = str(repo.upload_token)
    return analytics_token


class CanDoCoverageUploadsPermission(BasePermission):
    def has_permission(self, request: Request, view: APIView) -> bool:
        assert isinstance(view, GetterMixin), "View must be an instance of GetterMixin"
        repository = view.get_repo()
        return (
            isinstance(request.auth, RepositoryAuthInterface)
            and "upload" in request.auth.get_scopes()
            and request.auth.allows_repo(repository)
        )


class UploadViews(GetterMixin, CreateAPIView):
    serializer_class = UploadSerializer
    permission_classes = [
        CanDoCoverageUploadsPermission,
    ]
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

    def perform_create(self, serializer: BaseSerializer) -> None:
        endpoint = Endpoints.DO_UPLOAD
        milestone = Milestones.WAITING_FOR_COVERAGE_UPLOAD
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="create_upload",
                request=self.request,
                is_shelter_request=self.is_shelter_request(),
                position="start",
            ),
        )
        repository: Repository = self.get_repo()

        with upload_breadcrumb_context(
            initial_breadcrumb=False,
            commit_sha=self.kwargs.get("commit_sha"),
            repo_id=repository.repoid,
            milestone=milestone,
            endpoint=endpoint,
            error=Errors.REPO_DEACTIVATED,
        ):
            validate_activated_repo(repository)

        with upload_breadcrumb_context(
            initial_breadcrumb=False,
            commit_sha=self.kwargs.get("commit_sha"),
            repo_id=repository.repoid,
            milestone=milestone,
            endpoint=endpoint,
            error=Errors.COMMIT_NOT_FOUND,
        ):
            commit: Commit = self.get_commit(repository)

        with upload_breadcrumb_context(
            initial_breadcrumb=False,
            commit_sha=commit.commitid,
            repo_id=repository.repoid,
            milestone=milestone,
            endpoint=endpoint,
            error=Errors.REPORT_NOT_FOUND,
        ):
            report: CommitReport = self.get_report(commit)

        log.info(
            "Request to create new upload",
            extra={
                "repo": repository.name,
                "commit": commit.commitid,
                "cli_version": serializer.validated_data["version"]
                if "version" in serializer.validated_data
                else None,
            },
        )

        create_upload(
            cast(UploadSerializer, serializer),
            repository,
            commit,
            report,
            self.is_shelter_request(),
            get_token_for_analytics(commit, self.request),
            endpoint,
        )

        TaskService().upload_breadcrumb(
            commit_sha=commit.commitid,
            repo_id=repository.repoid,
            breadcrumb_data=BreadcrumbData(
                milestone=milestone,
                endpoint=endpoint,
            ),
        )

        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="create_upload",
                request=self.request,
                repository=repository,
                is_shelter_request=self.is_shelter_request(),
                position="end",
            ),
        )
