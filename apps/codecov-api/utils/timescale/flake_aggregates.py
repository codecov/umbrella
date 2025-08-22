from datetime import datetime
from typing import Literal

from django.db.models import Case, Count, F, FloatField, Sum, Value, When

from shared.metrics import Histogram
from utils.ta_types import FlakeAggregates

from .timescale_utils import (
    _pct_change,
    _should_use_precomputed_aggregates,
    get_daily_aggregate_querysets,
)


def get_flake_aggregates_via_ca(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
):
    test_data, repo_data = get_daily_aggregate_querysets(
        repoid, branch, start_date, end_date
    )

    daily_aggregates = repo_data.aggregate(
        total_count=Sum(
            F("pass_count") + F("fail_count") + F("flaky_fail_count"),
            output_field=FloatField(),
        ),
        flake_rate=Case(
            When(
                total_count=0,
                then=Value(0.0),
            ),
            default=Sum("flaky_fail_count", output_field=FloatField())
            / F("total_count"),
        ),
    )

    flake_count = test_data.filter(flaky_fail_count__gt=0).aggregate(
        flake_count=Count("computed_name", distinct=True),
    )

    return {
        **daily_aggregates,
        "flake_count": flake_count["flake_count"] or 0,
    }


get_flake_aggregates_histogram = Histogram(
    "get_flake_aggregates_timescale",
    "Time it takes to get the flake aggregates from the database",
)


@get_flake_aggregates_histogram.time()
def get_flake_aggregates_from_timescale(
    repoid: int, branch: str | None, start_date: datetime, end_date: datetime
) -> FlakeAggregates | None:
    if not _should_use_precomputed_aggregates(branch):
        return None

    interval_duration = end_date - start_date
    comparison_start_date = start_date - interval_duration
    comparison_end_date = start_date

    curr_aggregates = get_flake_aggregates_via_ca(repoid, branch, start_date, end_date)

    if curr_aggregates["flake_count"] is None:
        return None

    past_aggregates = get_flake_aggregates_via_ca(
        repoid, branch, comparison_start_date, comparison_end_date
    )

    return FlakeAggregates(
        flake_count=int(curr_aggregates["flake_count"] or 0),
        flake_rate=curr_aggregates["flake_rate"] or 0,
        flake_count_percent_change=_pct_change(
            curr_aggregates["flake_count"], past_aggregates["flake_count"]
        ),
        flake_rate_percent_change=_pct_change(
            curr_aggregates["flake_rate"], past_aggregates["flake_rate"]
        ),
    )
