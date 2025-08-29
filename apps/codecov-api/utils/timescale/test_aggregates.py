from datetime import datetime
from typing import Literal

from django.db.models import Count, F, FloatField, Sum, Window
from django.db.models.functions import RowNumber

from shared.metrics import Histogram
from utils.ta_types import TestResultsAggregates

from .timescale_utils import (
    _calculate_slow_test_num,
    _pct_change,
    _should_use_precomputed_aggregates,
    get_daily_aggregate_querysets,
)


def get_repo_aggregates_via_ca(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
):
    test_data, repo_data = get_daily_aggregate_querysets(
        repoid, branch, start_date, end_date
    )

    daily_aggregates = repo_data.aggregate(
        total_duration=Sum("total_duration_seconds", output_field=FloatField()),
        fails=Sum(F("fail_count") + F("flaky_fail_count")),
        skips=Sum("skip_count"),
    )

    unique_test_count = (
        test_data.aggregate(unique_test_count=Count("computed_name", distinct=True))[
            "unique_test_count"
        ]
        or 0
    )

    slow_test_num = _calculate_slow_test_num(unique_test_count)

    slow_tests = (
        test_data.values("test_id")
        .annotate(
            total_duration=Sum(
                F("avg_duration_seconds")
                * (F("pass_count") + F("fail_count") + F("flaky_fail_count")),
                output_field=FloatField(),
            ),
            row_num=Window(
                expression=RowNumber(),
                order_by=F("total_duration").desc(),
            ),
        )
        .filter(row_num__lte=slow_test_num)
    )

    result = slow_tests.aggregate(slowest_tests_duration=Sum("total_duration"))

    slowest_tests_duration = result["slowest_tests_duration"] or 0.0

    return daily_aggregates, slowest_tests_duration, slow_test_num


get_test_result_aggregates_histogram = Histogram(
    "get_test_result_aggregates_timescale",
    "Time it takes to get the test result aggregates from the database",
)


@get_test_result_aggregates_histogram.time()
def get_test_results_aggregates_from_timescale(
    repoid: int,
    branch: str | None,
    start_date: datetime,
    end_date: datetime,
) -> TestResultsAggregates | None:
    if not _should_use_precomputed_aggregates(branch):
        return None

    interval_duration = end_date - start_date
    comparison_start_date = start_date - interval_duration
    comparison_end_date = start_date

    curr_aggregates, curr_slow_test_duration, curr_slow_test_num = (
        get_repo_aggregates_via_ca(repoid, branch, start_date, end_date)
    )
    past_aggregates, past_slow_test_duration, past_slow_test_num = (
        get_repo_aggregates_via_ca(
            repoid,
            branch,
            comparison_start_date,
            comparison_end_date,
        )
    )
    return TestResultsAggregates(
        total_duration=curr_aggregates["total_duration"] or 0,
        fails=int(curr_aggregates["fails"] or 0),
        skips=int(curr_aggregates["skips"] or 0),
        total_slow_tests=curr_slow_test_num,
        slowest_tests_duration=curr_slow_test_duration or 0.0,
        total_duration_percent_change=_pct_change(
            curr_aggregates["total_duration"], past_aggregates["total_duration"]
        ),
        fails_percent_change=_pct_change(
            curr_aggregates["fails"], past_aggregates["fails"]
        ),
        skips_percent_change=_pct_change(
            curr_aggregates["skips"], past_aggregates["skips"]
        ),
        slowest_tests_duration_percent_change=_pct_change(
            curr_slow_test_duration, past_slow_test_duration
        ),
        total_slow_tests_percent_change=_pct_change(
            curr_slow_test_num, past_slow_test_num
        ),
    )
