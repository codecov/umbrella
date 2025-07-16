import logging
from collections.abc import Callable
from typing import Any

from django.db.models import QuerySet
from rest_framework import serializers
from rest_framework.exceptions import NotAuthenticated
from rest_framework.generics import ListCreateAPIView
from rest_framework.request import Request
from rest_framework.response import Response

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
from shared.django_apps.upload_breadcrumbs.models import (
    Endpoints,
    Errors,
    Milestones,
)
from shared.metrics import inc_counter
from upload.helpers import (
    generate_upload_prometheus_metrics_labels,
    upload_breadcrumb_context,
    validate_activated_repo,
)
from upload.metrics import API_UPLOAD_COUNTER
from upload.serializers import CommitSerializer
from upload.views.base import GetterMixin
from upload.views.uploads import CanDoCoverageUploadsPermission

log = logging.getLogger(__name__)


def create_commit(
    serializer: serializers.ModelSerializer, repository: Repository, endpoint: Endpoints
) -> Commit:
    with upload_breadcrumb_context(
        initial_breadcrumb=True,
        commit_sha=serializer.validated_data.get("commitid"),
        repo_id=repository.repoid,
        milestone=Milestones.FETCHING_COMMIT_DETAILS,
        endpoint=endpoint,
        error=Errors.REPO_DEACTIVATED,
    ):
        validate_activated_repo(repository)

    return serializer.save(repository=repository)


class CommitViews(ListCreateAPIView, GetterMixin):
    serializer_class = CommitSerializer
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

    def get_queryset(self) -> QuerySet:
        repository = self.get_repo()
        return Commit.objects.filter(repository=repository)

    def list(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        repository = self.get_repo()
        if repository.private and isinstance(
            self.request.auth, TokenlessAuthentication
        ):
            raise NotAuthenticated()
        return super().list(request, *args, **kwargs)

    def create(self, request: Request, *args: Any, **kwargs: Any) -> Response:
        return super().create(request, *args, **kwargs)

    def perform_create(self, serializer) -> None:
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="create_commit",
                request=self.request,
                is_shelter_request=self.is_shelter_request(),
                position="start",
            ),
        )
        repository = self.get_repo()
        commit = create_commit(serializer, repository, Endpoints.CREATE_COMMIT)

        log.info(
            "Request to create new commit",
            extra={"repo": repository.name, "commit": commit.commitid},
        )

        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="coverage",
                endpoint="create_commit",
                request=self.request,
                repository=repository,
                is_shelter_request=self.is_shelter_request(),
                position="end",
            ),
        )
