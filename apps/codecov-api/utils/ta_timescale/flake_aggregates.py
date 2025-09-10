from datetime import datetime
from typing import Literal

from django.db.models import Case, Count, F, FloatField, Sum, Value, When

from shared.django_apps.ta_timeseries.models import Testrun
from shared.metrics import Histogram
from utils.ta_timescale.types import FlakeAggregates
from utils.ta_timescale.utils import (
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


def get_flake_aggregates_via_testrun(
    repoid: int,
    branch: str,
    start_date: datetime,
    end_date: datetime,
):
    test_data = Testrun.objects.filter(
        repo_id=repoid,
        timestamp__gt=start_date,
        timestamp__lte=end_date,
    )
    if branch is not None:
        test_data = test_data.filter(branch=branch)

    aggregates = test_data.aggregate(
        total_count=Sum(
            Case(
                When(outcome__in=["pass", "failure", "flaky_fail"], then=Value(1)),
                default=Value(0),
            ),
            output_field=FloatField(),
        ),
        flaky_fail_count=Sum(
            Case(When(outcome="flaky_fail", then=Value(1)), default=Value(0)),
            output_field=FloatField(),
        ),
    )

    total_count = aggregates["total_count"] or 0.0
    flaky_fail_count = aggregates["flaky_fail_count"] or 0.0

    flake_rate = flaky_fail_count / total_count if total_count > 0 else 0.0

    flake_count = test_data.filter(outcome="flaky_fail").aggregate(
        flake_count=Count("test_id", distinct=True),
    )

    return {
        "total_count": total_count,
        "flake_rate": flake_rate,
        "flake_count": flake_count["flake_count"] or 0,
    }


get_flake_aggregates_histogram = Histogram(
    "get_flake_aggregates_timescale",
    "Time it takes to get the flake aggregates from the database",
)


@get_flake_aggregates_histogram.time()
def get_flake_aggregates(
    repoid: int,
    branch: str | None,
    start_date: datetime,
    end_date: datetime,
) -> FlakeAggregates | None:
    interval_duration = end_date - start_date
    comparison_start_date = start_date - interval_duration
    comparison_end_date = start_date

    if _should_use_precomputed_aggregates(branch):
        curr_aggregates = get_flake_aggregates_via_ca(
            repoid, branch, start_date, end_date
        )

        if curr_aggregates["flake_count"] is None:
            return FlakeAggregates(
                flake_count=0,
                flake_rate=0,
                flake_count_percent_change=0,
                flake_rate_percent_change=0,
            )

        past_aggregates = get_flake_aggregates_via_ca(
            repoid,
            branch,
            comparison_start_date,
            comparison_end_date,
        )
    else:
        curr_aggregates = get_flake_aggregates_via_testrun(
            repoid, branch, start_date, end_date
        )

        if curr_aggregates["flake_count"] is None:
            return FlakeAggregates(
                flake_count=0,
                flake_rate=0,
                flake_count_percent_change=0,
                flake_rate_percent_change=0,
            )

        past_aggregates = get_flake_aggregates_via_testrun(
            repoid,
            branch,
            comparison_start_date,
            comparison_end_date,
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
