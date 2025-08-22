from datetime import datetime
from typing import Literal, TypeIs

from django.db.models import Aggregate, Func

from shared.django_apps.prevent_timeseries import models as prevent_ts_models
from shared.django_apps.ta_timeseries import models as ta_ts_models

PRECOMPUTED_BRANCHES: tuple[str, str, str] = ("main", "master", "develop")


def _should_use_precomputed_aggregates(
    branch: str | None,
) -> TypeIs[Literal["main", "master", "develop"] | None]:
    return branch in PRECOMPUTED_BRANCHES or branch is None


class ArrayMergeDedupe(Aggregate):
    function = "array_merge_dedup_agg"
    template = "%(function)s(%(expressions)s)"


class Last(Aggregate):
    """
    Custom aggregate for TimescaleDB's last() function.

    Requires two arguments: value column and time column.
    Usage: Last('value_column', 'time_column', output_field=FloatField())
    """

    function = "last"
    template = "%(function)s(%(expressions)s)"


class Cardinality(Func):
    function = "cardinality"
    template = "%(function)s(%(expressions)s)"


def _calculate_slow_test_num(total_tests: int) -> int:
    return min(100, max(total_tests // 20, 1)) if total_tests else 0


def _pct_change(current: int | float | None, past: int | float | None) -> float:
    if past is None or past == 0:
        return 0.0
    if current is None:
        current = 0

    return (current - past) / past


def get_daily_aggregate_querysets(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
    *,
    use_prevent: bool = False,
):
    models = prevent_ts_models if use_prevent else ta_ts_models

    if branch is None:
        test_data = models.TestAggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )
        repo_data = models.AggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )
    else:
        test_data = models.BranchTestAggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            branch=branch,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )
        repo_data = models.BranchAggregateDaily.objects.filter(  # type: ignore[attr-defined]
            repo_id=repoid,
            branch=branch,
            bucket_daily__gt=start_date,
            bucket_daily__lte=end_date,
        )

    return test_data, repo_data
