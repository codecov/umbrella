from typing import TypedDict

import django_filters
from django.http import JsonResponse
from django_filters.rest_framework import DjangoFilterBackend
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import OpenApiParameter, extend_schema
from rest_framework import viewsets
from rest_framework.decorators import action

from api.public.v2.schema import repo_parameters
from api.shared.mixins import RepoPropertyMixin
from api.shared.permissions import RepositoryArtifactPermissions
from rollouts import READ_NEW_EVALS
from shared.django_apps.db_settings import TA_TIMESERIES_ENABLED
from shared.django_apps.ta_timeseries.models import Testrun


class EvalsSummary(TypedDict):
    avgDurationSeconds: float
    avgCost: float
    totalItems: int
    passedItems: int
    failedItems: int
    scores: dict[str, dict[str, float]]


class EvalsFilters(django_filters.FilterSet):
    commit = django_filters.CharFilter(field_name="commit_sha")
    classname = django_filters.CharFilter(field_name="classname")

    class Meta:
        model = Testrun
        fields = ["commit", "classname"]


class EvalsPermissions(RepositoryArtifactPermissions):
    """
    Permissions class for evals endpoints. Extends RepositoryArtifactPermissions
    to add a check for the READ_NEW_EVALS feature flag.
    """

    def has_permission(self, request, view):
        # First check if the user has basic repository access
        has_basic_permission = super().has_permission(request, view)
        if not has_basic_permission:
            return False

        # Then check if the repository has the feature flag enabled
        if not READ_NEW_EVALS.check_value(identifier=str(view.repo.repoid)):
            return False

        # Finally, check if the environment has TA_TIMESERIES_ENABLED
        if not TA_TIMESERIES_ENABLED:
            return False

        return True


@extend_schema(
    parameters=repo_parameters,
    tags=["Evaluations"],
)
class EvalsViewSet(viewsets.GenericViewSet, RepoPropertyMixin):
    permission_classes = [EvalsPermissions]
    filter_backends = [DjangoFilterBackend]
    filterset_class = EvalsFilters

    def get_queryset(self):
        return Testrun.objects.filter(
            repo_id=self.repo.repoid, properties__isnull=False
        )

    def _aggregate_testruns(self, testruns) -> EvalsSummary:
        """
        Aggregate metrics from a list of testruns.
        Returns a dict with aggregated metrics and scores.
        """
        # TODO: This function loads all testruns into memory.
        # If possible we should offload the calculation to postgres.
        # (although if it ever get's out of the POC I'd expect the rollup to exist in a separate table)

        total_items = len(testruns)
        passed_items = sum(1 for t in testruns if t.outcome == "pass")
        failed_items = total_items - passed_items

        avg_duration = (
            sum(t.duration_seconds or 0 for t in testruns) / total_items
            if total_items > 0
            else 0
        )

        # Calculate score sums and averages for all items with scores
        score_agg_data: dict[str, tuple[float, int]] = {}
        cost_acc = 0
        items_with_cost = 0

        for testrun in testruns:
            eval_data = testrun.properties.get("eval", {})
            scores = eval_data.get("scores", [])
            cost = eval_data.get("cost")
            if cost:
                cost_acc += cost
                items_with_cost += 1

            # Consider scores from all items (not just passed ones)
            for score in scores:
                name = score.get("name")
                if name:
                    score_value = score.get("value") or score.get("score")
                    if isinstance(score_value, int | float):
                        if name not in score_agg_data:
                            score_agg_data[name] = (0, 0)
                        score_agg_data[name] = (
                            score_agg_data[name][0] + score_value,
                            score_agg_data[name][1] + 1,
                        )

        # Create score aggregation dicts with both sum and avg
        scores = {
            name: {"sum": _sum, "avg": _sum / count if count > 0 else 0}
            for name, (_sum, count) in score_agg_data.items()
        }

        return {
            "avgDurationSeconds": avg_duration,
            "avgCost": cost_acc / items_with_cost if items_with_cost > 0 else 0,
            "totalItems": total_items,
            "passedItems": passed_items,
            "failedItems": failed_items,
            "scores": scores,
        }

    @extend_schema(
        summary="Evaluation summary",
        parameters=[
            OpenApiParameter(
                "commit",
                OpenApiTypes.STR,
                OpenApiParameter.QUERY,
                description="commit SHA for which to return evaluation summary",
            ),
            # "classname" is a terrible name but that's the name of the field in the testrun model
            # it is the name of the class that the test belongs to, or `describe` block in vitest
            # for langfuse it is the name of the run
            OpenApiParameter(
                "classname",
                OpenApiTypes.STR,
                OpenApiParameter.QUERY,
                description="class name the test belongs to, or `describe` block in vitest, or run name in langfuse",
            ),
        ],
    )
    @action(detail=False, methods=["get"])
    def summary(self, request, *args, **kwargs):
        """
        Returns a summary of evaluations for the specified repository and commit
        """
        queryset = self.filter_queryset(self.get_queryset())
        testruns = list(queryset)
        return JsonResponse(self._aggregate_testruns(testruns))

    @extend_schema(
        summary="Evaluation compare",
        parameters=[
            OpenApiParameter(
                "base_sha",
                OpenApiTypes.STR,
                OpenApiParameter.QUERY,
                description="base commit SHA to compare from",
            ),
            OpenApiParameter(
                "head_sha",
                OpenApiTypes.STR,
                OpenApiParameter.QUERY,
                description="head commit SHA to compare to",
            ),
        ],
    )
    @action(detail=False, methods=["get"])
    def compare(self, request, *args, **kwargs):
        """
        Returns a comparison of evaluations between two commits
        """
        base_sha = request.query_params.get("base_sha")
        head_sha = request.query_params.get("head_sha")

        if not base_sha or not head_sha:
            return JsonResponse(
                {"error": "Both base_sha and head_sha are required"}, status=400
            )

        # Get testruns for both commits
        base_testruns = list(self.get_queryset().filter(commit_sha=base_sha))
        head_testruns = list(self.get_queryset().filter(commit_sha=head_sha))

        base_data = self._aggregate_testruns(base_testruns)
        head_data = self._aggregate_testruns(head_testruns)

        # Calculate differences
        def calculate_diff(base, head):
            if base == 0:
                return 0 if head == 0 else 100
            return ((head - base) / base) * 100

        # Compare scores
        score_diffs = {}
        all_score_names = set(base_data["scores"].keys()) | set(
            head_data["scores"].keys()
        )
        for score_name in all_score_names:
            base_score_data = base_data["scores"].get(score_name, {"sum": 0, "avg": 0})
            head_score_data = head_data["scores"].get(score_name, {"sum": 0, "avg": 0})

            score_diffs[score_name] = {
                "sum": calculate_diff(base_score_data["sum"], head_score_data["sum"]),
                "avg": calculate_diff(base_score_data["avg"], head_score_data["avg"]),
            }

        return JsonResponse(
            {
                "base": base_data,
                "head": head_data,
                "diff": {
                    "avgDurationSeconds": calculate_diff(
                        base_data["avgDurationSeconds"], head_data["avgDurationSeconds"]
                    ),
                    "avgCost": calculate_diff(
                        base_data["avgCost"], head_data["avgCost"]
                    ),
                    "totalItems": head_data["totalItems"] - base_data["totalItems"],
                    "passedItems": head_data["passedItems"] - base_data["passedItems"],
                    "failedItems": head_data["failedItems"] - base_data["failedItems"],
                    "scores": score_diffs,
                },
            }
        )
