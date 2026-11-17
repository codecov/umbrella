import django_filters
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import mixins
from rest_framework.authentication import BasicAuthentication, SessionAuthentication
from rest_framework.response import Response

from api.public.v2.schema import repo_parameters
from api.shared.pagination import PaginationMixin
from api.shared.permissions import RepositoryArtifactPermissions, SuperTokenPermissions
from api.shared.pull.mixins import PullViewSetMixin
from codecov_auth.authentication import (
    SuperTokenAuthentication,
    UserTokenAuthentication,
)
from core.models import Pull, PullStates
from services.comparison import CommitComparisonService

from .serializers import PullSerializer


class PullFilters(django_filters.FilterSet):
    state = django_filters.ChoiceFilter(choices=PullStates.choices)
    start_date = django_filters.DateTimeFilter(method="filter_start_date")

    def filter_start_date(self, queryset, name, value):
        return queryset.filter(updatestamp__gte=value)


@extend_schema(parameters=repo_parameters, tags=["Pulls"])
class PullViewSet(
    PaginationMixin,
    PullViewSetMixin,
    mixins.ListModelMixin,
    mixins.RetrieveModelMixin,
):
    authentication_classes = [
        SuperTokenAuthentication,
        UserTokenAuthentication,
        BasicAuthentication,
        SessionAuthentication,
    ]

    permission_classes = [SuperTokenPermissions | RepositoryArtifactPermissions]

    serializer_class = PullSerializer
    queryset = Pull.objects.none()
    filterset_class = PullFilters

    def get_queryset(self):
        return super().get_queryset().select_related("author")

    @extend_schema(
        summary="Pull list",
        parameters=[
            OpenApiParameter(
                "state",
                OpenApiTypes.STR,
                OpenApiParameter.QUERY,
                description="the state of the pull (open/merged/closed)",
            ),
            OpenApiParameter(
                "start_date",
                OpenApiTypes.DATETIME,
                OpenApiParameter.QUERY,
                description="only return pulls with updatestamp on or after this date",
            ),
        ],
    )
    def list(self, request, *args, **kwargs):
        """
        Returns a paginated list of pulls for the specified repository

        Optionally filterable by:
        * `state`
        * `start_date`

        Orderable by:
        * `pullid`
        """
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        items = page if page is not None else queryset

        # Batch-load all CommitComparison records for this page in a single query
        # to avoid the N+1 pattern in PullSerializer.get_patch()
        keys = [
            (pull.compared_to, pull.head)
            for pull in items
            if pull.compared_to and pull.head
        ]
        commit_comparisons = {}
        if keys:
            repo_id = self.repo.repoid
            for cc in CommitComparisonService.fetch_precomputed(repo_id, keys):
                commit_comparisons[(cc.base_commitid, cc.compare_commitid)] = cc

        serializer = self.get_serializer(
            items,
            many=True,
            context={**self.get_serializer_context(), "commit_comparisons": commit_comparisons},
        )
        if page is not None:
            return self.get_paginated_response(serializer.data)
        return Response(serializer.data)

    @extend_schema(
        summary="Pull detail",
        parameters=[
            OpenApiParameter(
                "pullid",
                OpenApiTypes.STR,
                OpenApiParameter.PATH,
                description="pull ID",
            ),
        ],
    )
    def retrieve(self, request, *args, **kwargs):
        """
        Returns a single pull by ID
        """
        return super().retrieve(request, *args, **kwargs)
