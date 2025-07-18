from datetime import datetime
from typing import Literal

from django.db import connections
from django.db.models import (
    Aggregate,
    Case,
    Count,
    F,
    FloatField,
    Max,
    Q,
    Sum,
    Value,
    When,
)

from shared.django_apps.ta_timeseries.models import (
    TestrunBranchSummary,
)
from shared.metrics import Histogram
from utils.ta_types import (
    FlakeAggregates,
    TestResultsAggregates,
)

get_test_result_aggregates_histogram = Histogram(
    "get_test_result_aggregates_timescale",
    "Time it takes to get the test result aggregates from the database",
)

get_flake_aggregates_histogram = Histogram(
    "get_flake_aggregates_timescale",
    "Time it takes to get the flake aggregates from the database",
)


class ArrayMergeDedupe(Aggregate):
    function = "array_merge_dedup_agg"
    template = "%(function)s(%(expressions)s)"


def get_test_results_queryset(
    repoid: int,
    start_date: datetime,
    end_date: datetime,
    branch: str,
    parameter: Literal["flaky_tests", "failed_tests", "slowest_tests", "skipped_tests"]
    | None = None,
    testsuites: list[str] | None = None,
    flags: list[str] | None = None,
    term: str | None = None,
):
    base_queryset = TestrunBranchSummary.objects.filter(
        repo_id=repoid,
        branch=branch,
        timestamp_bin__gte=start_date,
        timestamp_bin__lt=end_date,
    )

    aggregated_queryset = base_queryset.values("computed_name", "testsuite").annotate(
        total_pass_count=Sum("pass_count"),
        total_fail_count=Sum("fail_count"),
        total_flaky_fail_count=Sum("flaky_fail_count"),
        total_skip_count=Sum("skip_count"),
        commits_where_fail=Sum("failing_commits"),
        total_count=Sum(
            F("pass_count") + F("fail_count") + F("flaky_fail_count"),
            output_field=FloatField(),
        ),
        failure_rate=Case(
            When(
                Q(total_count=0),
                then=Value(0.0),
            ),
            default=(Sum("fail_count") + Sum("flaky_fail_count")) / F("total_count"),
            output_field=FloatField(),
        ),
        flake_rate=Case(
            When(
                Q(total_count=0),
                then=Value(0.0),
            ),
            default=Sum("flaky_fail_count") / F("total_count"),
            output_field=FloatField(),
        ),
        total_duration=Sum(
            F("avg_duration_seconds")
            * (F("pass_count") + F("fail_count") + F("flaky_fail_count")),
            output_field=FloatField(),
        ),
        avg_duration=Case(
            When(
                Q(total_count=0),
                then=Value(0.0),
            ),
            default=F("total_duration") / F("total_count"),
            output_field=FloatField(),
        ),
        last_duration=Max("last_duration_seconds"),
        updated_at=Max("updated_at"),
        flags=ArrayMergeDedupe("flags"),
        name=F("computed_name"),
    )

    match parameter:
        case "failed_tests":
            aggregated_queryset = aggregated_queryset.filter(total_fail_count__gt=0)
        case "flaky_tests":
            aggregated_queryset = aggregated_queryset.filter(
                total_flaky_fail_count__gt=0
            )
        case "skipped_tests":
            aggregated_queryset = aggregated_queryset.filter(
                total_skip_count__gt=0, total_pass_count=0
            )

    if term:
        aggregated_queryset = aggregated_queryset.filter(computed_name__icontains=term)
    if testsuites:
        aggregated_queryset = aggregated_queryset.filter(testsuite__in=testsuites)
    if flags:
        aggregated_queryset = aggregated_queryset.filter(flags__overlap=flags)

    return aggregated_queryset


def _pct_change(current: int | float | None, past: int | float | None) -> float:
    if past is None or past == 0:
        return 0.0
    if current is None:
        current = 0

    return (current - past) / past


def get_slowest_tests_duration(
    repoid: int,
    branch: str,
    start_date: datetime,
    end_date: datetime,
    unique_test_count: int,
) -> tuple[float, int]:
    slow_test_num = min(100, max(unique_test_count // 20, 1))

    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            """
            SELECT SUM(total_duration) as slowest_tests_duration FROM (
                SELECT SUM(avg_duration_seconds * (pass_count + fail_count + flaky_fail_count)) as total_duration
                FROM ta_timeseries_testrun_branch_summary_1day
                WHERE repo_id = %s
                    AND branch = %s
                    AND timestamp_bin >= %s
                    AND timestamp_bin < %s
                GROUP BY computed_name, testsuite
                ORDER BY total_duration DESC
                LIMIT %s
            ) as slow_tests
            """,
            (repoid, branch, start_date, end_date, slow_test_num),
        )
        if result := cursor.fetchone():
            return result[0] or 0.0, slow_test_num
        else:
            return 0.0, 0


@get_test_result_aggregates_histogram.time()
def get_test_results_aggregates_from_timescale(
    repoid: int, branch: str, start_date: datetime, end_date: datetime
) -> TestResultsAggregates | None:
    interval_duration = end_date - start_date
    comparison_start_date = start_date - interval_duration
    comparison_end_date = start_date

    def get_aggregates(
        repoid: int, branch: str, start_date: datetime, end_date: datetime
    ):
        return TestrunBranchSummary.objects.filter(
            repo_id=repoid,
            branch=branch,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        ).aggregate(
            total_duration=Sum(
                F("avg_duration_seconds")
                * (F("pass_count") + F("fail_count") + F("flaky_fail_count")),
                output_field=FloatField(),
            ),
            fails=Sum(F("fail_count") + F("flaky_fail_count")),
            skips=Sum("skip_count"),
            unique_test_count=Count("computed_name", distinct=True),
        )

    curr_aggregates = get_aggregates(repoid, branch, start_date, end_date)

    if curr_aggregates["total_duration"] is None:
        return None

    curr_slow_test_duration, curr_slow_test_num = get_slowest_tests_duration(
        repoid, branch, start_date, end_date, curr_aggregates["unique_test_count"]
    )

    past_aggregates = get_aggregates(
        repoid, branch, comparison_start_date, comparison_end_date
    )

    past_slow_test_duration, past_slow_test_num = get_slowest_tests_duration(
        repoid,
        branch,
        comparison_start_date,
        comparison_end_date,
        past_aggregates["unique_test_count"],
    )

    return TestResultsAggregates(
        total_duration=curr_aggregates["total_duration"] or 0,
        fails=curr_aggregates["fails"] or 0,
        skips=curr_aggregates["skips"] or 0,
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


@get_flake_aggregates_histogram.time()
def get_flake_aggregates_from_timescale(
    repoid: int, branch: str, start_date: datetime, end_date: datetime
) -> FlakeAggregates | None:
    interval_duration = end_date - start_date
    comparison_start_date = start_date - interval_duration
    comparison_end_date = start_date

    def get_branch_aggregates(
        start: datetime,
        end: datetime,
    ):
        return TestrunBranchSummary.objects.filter(
            repo_id=repoid,
            branch=branch,
            timestamp_bin__gte=start,
            timestamp_bin__lt=end,
        ).aggregate(
            total_count=Sum(
                "pass_count",
                output_field=FloatField(),
            )
            + Sum("fail_count", output_field=FloatField())
            + Sum("flaky_fail_count", output_field=FloatField()),
            flake_count=Count(
                "computed_name", distinct=True, filter=Q(flaky_fail_count__gt=0)
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

    curr_aggregates = get_branch_aggregates(start_date, end_date)
    if curr_aggregates["flake_count"] is None:
        return None

    past_aggregates = get_branch_aggregates(comparison_start_date, comparison_end_date)

    return FlakeAggregates(
        flake_count=curr_aggregates["flake_count"] or 0,
        flake_rate=curr_aggregates["flake_rate"] or 0,
        flake_count_percent_change=_pct_change(
            curr_aggregates["flake_count"], past_aggregates["flake_count"]
        ),
        flake_rate_percent_change=_pct_change(
            curr_aggregates["flake_rate"], past_aggregates["flake_rate"]
        ),
    )
