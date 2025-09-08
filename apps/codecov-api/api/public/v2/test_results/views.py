import django_filters
from django.http import JsonResponse
from django_filters.rest_framework import DjangoFilterBackend
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import mixins, status, viewsets
from rest_framework.authentication import BasicAuthentication, SessionAuthentication

from api.shared.mixins import RepoPropertyMixin
from api.shared.permissions import RepositoryArtifactPermissions, SuperTokenPermissions
from codecov_auth.authentication import (
    SuperTokenAuthentication,
    UserTokenAuthentication,
)
from shared.django_apps.ta_timeseries.models import Testrun

from .serializers import TestrunSerializer


class TestResultsFilters(django_filters.FilterSet):
    commit_id = django_filters.CharFilter(field_name="commit_sha")
    outcome = django_filters.CharFilter(field_name="outcome")
    duration_min = django_filters.NumberFilter(
        field_name="duration_seconds", lookup_expr="gte"
    )
    duration_max = django_filters.NumberFilter(
        field_name="duration_seconds", lookup_expr="lte"
    )
    branch = django_filters.CharFilter(field_name="branch")

    class Meta:
        model = Testrun
        fields = ["commit_id", "outcome", "duration_min", "duration_max", "branch"]


@extend_schema(
    parameters=[
        OpenApiParameter(
            "commit_id",
            OpenApiTypes.STR,
            OpenApiParameter.QUERY,
            description="Commit SHA for which to return test results",
        ),
        OpenApiParameter(
            "outcome",
            OpenApiTypes.STR,
            OpenApiParameter.QUERY,
            description="Status of the test (failure, skip, error, pass)",
        ),
        OpenApiParameter(
            "duration_min",
            OpenApiTypes.INT,
            OpenApiParameter.QUERY,
            description="Minimum duration of the test in seconds",
        ),
        OpenApiParameter(
            "duration_max",
            OpenApiTypes.INT,
            OpenApiParameter.QUERY,
            description="Maximum duration of the test in seconds",
        ),
        OpenApiParameter(
            "branch",
            OpenApiTypes.STR,
            OpenApiParameter.QUERY,
            description="Branch name for which to return test results",
        ),
    ],
    tags=["Test Results"],
    summary="Retrieve test results",
)
class TestResultsView(
    viewsets.GenericViewSet,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
    RepoPropertyMixin,
):
    authentication_classes = [
        SuperTokenAuthentication,
        UserTokenAuthentication,
        BasicAuthentication,
        SessionAuthentication,
    ]

    serializer_class = TestrunSerializer
    permission_classes = [SuperTokenPermissions | RepositoryArtifactPermissions]
    filter_backends = [DjangoFilterBackend]
    filterset_class = TestResultsFilters

    def get_queryset(self):
        # TestInstance model has been removed, this endpoint is deprecated
        return Testrun.objects.none()

    def dispatch(self, request, *args, **kwargs):
        return JsonResponse(
            {
                "detail": "This endpoint has been deprecated. Please use /test-analytics/ instead.",
                "new_endpoint": "https://api.codecov.io/api/v2/{service}/{owner_username}/repos/{repo_name}/test-analytics/",
            },
            status=status.HTTP_301_MOVED_PERMANENTLY,
        )


class TestAnalyticsFilters(django_filters.FilterSet):
    commit_sha = django_filters.CharFilter(field_name="commit_sha")
    branch = django_filters.CharFilter(field_name="branch")
    outcome = django_filters.CharFilter(field_name="outcome")
    duration_min = django_filters.NumberFilter(
        field_name="duration_seconds", lookup_expr="gte"
    )
    duration_max = django_filters.NumberFilter(
        field_name="duration_seconds", lookup_expr="lte"
    )

    class Meta:
        model = Testrun
        fields = ["commit_sha", "branch", "outcome", "duration_min", "duration_max"]


@extend_schema(
    parameters=[
        OpenApiParameter(
            "commit_sha",
            OpenApiTypes.STR,
            OpenApiParameter.QUERY,
            description="Commit SHA for which to return test analytics",
        ),
        OpenApiParameter(
            "branch",
            OpenApiTypes.STR,
            OpenApiParameter.QUERY,
            description="Branch name for which to return test analytics",
        ),
        OpenApiParameter(
            "outcome",
            OpenApiTypes.STR,
            OpenApiParameter.QUERY,
            description="Status of the test (failure, skip, error, pass)",
        ),
        OpenApiParameter(
            "duration_min",
            OpenApiTypes.INT,
            OpenApiParameter.QUERY,
            description="Minimum duration of the test in seconds",
        ),
        OpenApiParameter(
            "duration_max",
            OpenApiTypes.INT,
            OpenApiParameter.QUERY,
            description="Maximum duration of the test in seconds",
        ),
    ],
    tags=["Test Analytics"],
    summary="Retrieve test analytics",
)
class TestAnalyticsView(
    viewsets.GenericViewSet,
    mixins.ListModelMixin,
    RepoPropertyMixin,
):
    authentication_classes = [
        SuperTokenAuthentication,
        UserTokenAuthentication,
        BasicAuthentication,
        SessionAuthentication,
    ]

    serializer_class = TestrunSerializer
    permission_classes = [SuperTokenPermissions | RepositoryArtifactPermissions]
    filter_backends = [DjangoFilterBackend]
    filterset_class = TestAnalyticsFilters

    def get_queryset(self):
        return Testrun.objects.filter(repo_id=self.repo.repoid).order_by("-timestamp")

    @extend_schema(summary="Test analytics list")
    def list(self, request, *args, **kwargs):
        """
        Returns a list of test analytics for the specified repository
        """
        return super().list(request, *args, **kwargs)
