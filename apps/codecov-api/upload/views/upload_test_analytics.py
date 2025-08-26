import logging
import uuid

from django.utils import timezone
from rest_framework import serializers, status
from rest_framework.exceptions import NotFound
from rest_framework.permissions import BasePermission
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from codecov_auth.authentication.repo_auth import (
    GitHubOIDCTokenAuthentication,
    RepositoryLegacyTokenAuthentication,
)
from codecov_auth.authentication.types import RepositoryAsUser, RepositoryAuthInterface
from services.task.task import TaskService
from shared.api_archive.archive import ArchiveService, MinioEndpoints
from shared.metrics import inc_counter
from shared.upload.types import TAUploadContext
from upload.helpers import (
    generate_upload_prometheus_metrics_labels,
)
from upload.metrics import API_UPLOAD_COUNTER
from upload.serializers import FlagListField
from upload.views.base import ShelterMixin

log = logging.getLogger(__name__)


class TAUploadPermission(BasePermission):
    def has_permission(self, request: Request, view: APIView) -> bool:
        return (
            isinstance(request.auth, RepositoryAuthInterface)
            and "upload" in request.auth.get_scopes()
        )


class TAUploadSerializer(serializers.Serializer):
    commit = serializers.CharField(required=True, allow_null=False)
    slug = serializers.CharField(required=True, allow_null=False)
    service = serializers.CharField(required=False, allow_null=True)  # git_service
    build = serializers.CharField(required=False, allow_null=True)
    buildUrl = serializers.CharField(required=False, allow_null=True)
    code = serializers.CharField(required=False, allow_null=True)
    flags = FlagListField(required=False, allow_null=True)
    pr = serializers.CharField(required=False, allow_null=True)
    branch = serializers.CharField(required=False, allow_null=True)
    storage_path = serializers.CharField(required=False)
    file_not_found = serializers.BooleanField(required=False)


class TAUploadView(
    ShelterMixin,
    APIView,
):
    permission_classes = [TAUploadPermission]
    authentication_classes = [
        GitHubOIDCTokenAuthentication,
        RepositoryLegacyTokenAuthentication,
    ]

    def post(self, request: Request) -> Response:
        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="ta_upload",
                endpoint="ta_upload",
                request=request,
                is_shelter_request=self.is_shelter_request(),
                position="start",
            ),
        )
        serializer = TAUploadSerializer(data=request.data)
        if not serializer.is_valid():
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
        data = serializer.validated_data

        commit_sha = data.get("commit")
        assert commit_sha is not None

        assert isinstance(request.user, RepositoryAsUser)
        repo = request.user._repository

        if repo is None:
            raise NotFound("Repository not found.")

        update_fields = []
        if not repo.active or not repo.activated:
            repo.active = True
            repo.activated = True
            update_fields += ["active", "activated"]

        if not repo.test_analytics_enabled:
            repo.test_analytics_enabled = True
            update_fields += ["test_analytics_enabled"]

        if update_fields:
            repo.save(update_fields=update_fields)

        inc_counter(
            API_UPLOAD_COUNTER,
            labels=generate_upload_prometheus_metrics_labels(
                action="test_results",
                endpoint="test_results",
                request=request,
                repository=repo,
                is_shelter_request=self.is_shelter_request(),
                position="end",
            ),
        )

        url = None
        storage_path = data.get("storage_path")
        if storage_path is None or not self.is_shelter_request():
            archive_service = ArchiveService(repo)
            storage_path = MinioEndpoints.test_results.get_path(
                date=timezone.now().strftime("%Y-%m-%d"),
                repo_hash=archive_service.get_archive_hash(repo),
                commit_sha=commit_sha,
                uploadid=str(uuid.uuid4()),
            )

            url = archive_service.create_presigned_put(storage_path)

        ta_upload_context: TAUploadContext = {
            "branch": data.get("branch"),
            "commit_sha": commit_sha,
            "flags": data.get("flags"),
            "merged": data.get("pr") is None and data.get("branch") == repo.branch,
            "pull_id": data.get("pr"),
            "storage_path": storage_path,
        }

        TaskService().queue_ta_upload(
            repo_id=repo.repoid,
            upload_context=ta_upload_context,
        )

        if url is None:
            return Response(status=201)
        else:
            return Response({"raw_upload_location": url}, status=201)
