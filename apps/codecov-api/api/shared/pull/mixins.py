from django.db.models import OuterRef, Subquery
from django.shortcuts import get_object_or_404
from django_filters.rest_framework import DjangoFilterBackend
from rest_framework import filters, viewsets

from api.shared.mixins import RepoPropertyMixin
from api.shared.permissions import RepositoryArtifactPermissions
from compare.models import CommitComparison
from core.models import Commit


class PullViewSetMixin(
    viewsets.GenericViewSet,
    RepoPropertyMixin,
):
    filter_backends = [DjangoFilterBackend, filters.OrderingFilter]
    filterset_fields = ["state"]
    ordering = ["-pullid"]
    ordering_fields = ["pullid"]
    permission_classes = [RepositoryArtifactPermissions]
    lookup_field = "pullid"

    def get_object(self):
        pullid = self.kwargs.get("pullid")
        return get_object_or_404(self.get_queryset(), pullid=pullid)

    def get_queryset(self):
        return self.repo.pull_requests.annotate(
            base_totals=Subquery(
                Commit.objects.filter(
                    commitid=OuterRef("compared_to"), repository=OuterRef("repository")
                ).values("totals")[:1]
            ),
            head_totals=Subquery(
                Commit.objects.filter(
                    commitid=OuterRef("head"), repository=OuterRef("repository")
                ).values("totals")[:1]
            ),
            ci_passed=Subquery(
                Commit.objects.filter(
                    commitid=OuterRef("head"), repository=OuterRef("repository")
                ).values("ci_passed")[:1]
            ),
            commit_comparison_state=Subquery(
                CommitComparison.objects.filter(
                    base_commit__commitid=OuterRef("compared_to"),
                    base_commit__repository=OuterRef("repository"),
                    compare_commit__commitid=OuterRef("head"),
                    compare_commit__repository=OuterRef("repository"),
                ).values("state")[:1]
            ),
            commit_comparison_patch_totals=Subquery(
                CommitComparison.objects.filter(
                    base_commit__commitid=OuterRef("compared_to"),
                    base_commit__repository=OuterRef("repository"),
                    compare_commit__commitid=OuterRef("head"),
                    compare_commit__repository=OuterRef("repository"),
                ).values("patch_totals")[:1]
            ),
        )
