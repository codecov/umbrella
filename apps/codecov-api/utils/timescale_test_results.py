from datetime import datetime
from typing import Literal, TypeIs

from django.db.models import (
    Aggregate,
    Case,
    Count,
    DateTimeField,
    Exists,
    F,
    FloatField,
    Max,
    OuterRef,
    Q,
    Subquery,
    Sum,
    Value,
    When,
    Window,
)
from django.db.models.functions import RowNumber, Trunc

from shared.django_apps.ta_timeseries.models import (
    AggregateDaily,
    BranchAggregateDaily,
    Testrun,
    TestrunBranchSummary,
    TestrunSummary,
)
from shared.metrics import Histogram
from utils.ta_types import (
    FlakeAggregates,
    TestResultsAggregates,
)

PRECOMPUTED_BRANCHES = ("main", "master", "develop")


def _should_use_precomputed_aggregates(
    branch: str | None,
) -> TypeIs[Literal["main", "master", "develop"] | None]:
    return branch in PRECOMPUTED_BRANCHES or branch is None


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


class Last(Aggregate):
    """
    Custom aggregate for TimescaleDB's last() function.

    Requires two arguments: value column and time column.
    Usage: Last('value_column', 'time_column', output_field=FloatField())
    """

    function = "last"
    template = "%(function)s(%(expressions)s)"


def _calculate_slow_test_num(total_tests: int) -> int:
    return min(100, max(total_tests // 20, 1)) if total_tests else 0


def get_test_data_queryset_via_ca(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
    parameter: Literal["flaky_tests", "failed_tests", "slowest_tests", "skipped_tests"]
    | None = None,
):
    if branch:
        test_data = TestrunBranchSummary.objects.filter(
            repo_id=repoid,
            branch=branch,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        )
    else:
        test_data = TestrunSummary.objects.filter(
            repo_id=repoid,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        )

    test_data = test_data.values("computed_name", "testsuite").annotate(  # type: ignore[assignment]
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
        last_duration=Last(
            "last_duration_seconds", "updated_at", output_field=FloatField()
        ),
        updated_at=Max("updated_at"),
        flags=ArrayMergeDedupe("flags"),
        name=F("computed_name"),
    )

    match parameter:
        case "failed_tests":
            test_data = test_data.filter(
                Q(total_fail_count__gt=0) | Q(total_flaky_fail_count__gt=0)
            )
        case "flaky_tests":
            test_data = test_data.filter(total_flaky_fail_count__gt=0)
        case "slowest_tests":
            total_tests = test_data.count()
            slow_test_num = _calculate_slow_test_num(total_tests)
            if slow_test_num:
                test_data = test_data.annotate(
                    row_number=Window(
                        expression=RowNumber(),
                        order_by=F("total_duration").desc(),
                    )
                ).filter(row_number__lte=slow_test_num)
        case "skipped_tests":
            if branch is not None:
                latest_ts_subquery = Subquery(
                    TestrunBranchSummary.objects.filter(
                        repo_id=repoid,
                        branch=branch,
                        computed_name=OuterRef("computed_name"),
                        testsuite=OuterRef("testsuite"),
                        timestamp_bin__gte=start_date,
                        timestamp_bin__lt=end_date,
                    )
                    .order_by("-timestamp_bin")
                    .values("timestamp_bin")[:1],
                    output_field=DateTimeField(),
                )

                has_skip_latest = Exists(
                    TestrunBranchSummary.objects.filter(
                        repo_id=repoid,
                        branch=branch,
                        computed_name=OuterRef("computed_name"),
                        testsuite=OuterRef("testsuite"),
                        timestamp_bin=latest_ts_subquery,
                        skip_count__gt=0,
                    )
                )

                test_data = test_data.annotate(has_skip_latest=has_skip_latest).filter(
                    has_skip_latest=True
                )
        case _:
            pass

    return test_data


def get_test_data_queryset_via_testrun(
    repoid: int,
    branch: str,
    start_date: datetime,
    end_date: datetime,
    parameter: Literal["flaky_tests", "failed_tests", "slowest_tests", "skipped_tests"]
    | None = None,
):
    test_data = Testrun.objects.filter(
        repo_id=repoid,
        branch=branch,
        timestamp__gte=start_date,
        timestamp__lt=end_date,
    )

    test_data = test_data.values("computed_name", "testsuite").annotate(  # type: ignore[assignment]
        total_pass_count=Sum(
            Case(When(outcome="pass", then=Value(1)), default=Value(0))
        ),
        total_fail_count=Sum(
            Case(When(outcome="failure", then=Value(1)), default=Value(0))
        ),
        total_skip_count=Sum(
            Case(When(outcome="skip", then=Value(1)), default=Value(0))
        ),
        total_flaky_fail_count=Sum(
            Case(When(outcome="flaky_fail", then=Value(1)), default=Value(0))
        ),
        commits_where_fail=Count(
            "commit_sha",
            filter=Q(outcome__in=["failure", "flaky_fail"]),
            distinct=True,
        ),
        total_count=(
            F("total_pass_count")
            + F("total_fail_count")
            + F("total_skip_count")
            + F("total_flaky_fail_count")
        ),
        failure_rate=Case(
            When(
                Q(total_count=0),
                then=Value(0.0),
            ),
            default=(F("total_fail_count") + F("total_flaky_fail_count"))
            / F("total_count"),
            output_field=FloatField(),
        ),
        flake_rate=Case(
            When(
                Q(total_count=0),
                then=Value(0.0),
            ),
            default=F("total_flaky_fail_count") / F("total_count"),
            output_field=FloatField(),
        ),
        total_duration=Sum(F("duration_seconds"), output_field=FloatField()),
        avg_duration=Case(
            When(
                Q(total_count=0),
                then=Value(0.0),
            ),
            default=F("total_duration") / F("total_count"),
            output_field=FloatField(),
        ),
        last_duration=Last("duration_seconds", "timestamp", output_field=FloatField()),
        updated_at=Max("timestamp"),
        flags=ArrayMergeDedupe("flags"),
        name=F("computed_name"),
    )

    match parameter:
        case "failed_tests":
            test_data = test_data.filter(
                Q(total_fail_count__gt=0) | Q(total_flaky_fail_count__gt=0)
            )
        case "flaky_tests":
            test_data = test_data.filter(total_flaky_fail_count__gt=0)
        case "slowest_tests":
            total_tests = test_data.count()
            slow_test_num = _calculate_slow_test_num(total_tests)
            if slow_test_num:
                test_data = test_data.annotate(
                    row_number=Window(
                        expression=RowNumber(),
                        order_by=F("total_duration").desc(),
                    )
                ).filter(row_number__lte=slow_test_num)
        case "skipped_tests":
            latest_bucket_subquery = Subquery(
                Testrun.objects.filter(
                    repo_id=repoid,
                    branch=branch,
                    computed_name=OuterRef("computed_name"),
                    testsuite=OuterRef("testsuite"),
                    timestamp__gte=start_date,
                    timestamp__lt=end_date,
                )
                .annotate(bucket=Trunc("timestamp", "day"))
                .order_by("-bucket")
                .values("bucket")[:1],
                output_field=DateTimeField(),
            )

            has_skip_latest = Exists(
                Testrun.objects.filter(
                    repo_id=repoid,
                    branch=branch,
                    computed_name=OuterRef("computed_name"),
                    testsuite=OuterRef("testsuite"),
                    outcome="skip",
                )
                .annotate(bucket=Trunc("timestamp", "day"))
                .filter(bucket=latest_bucket_subquery)
            )

            test_data = test_data.annotate(has_skip_latest=has_skip_latest).filter(
                has_skip_latest=True
            )
        case _:
            pass

    return test_data


def get_test_results_queryset(
    repoid: int,
    start_date: datetime,
    end_date: datetime,
    branch: str | None,
    parameter: Literal["flaky_tests", "failed_tests", "slowest_tests", "skipped_tests"]
    | None = None,
    testsuites: list[str] | None = None,
    flags: list[str] | None = None,
    term: str | None = None,
):
    if _should_use_precomputed_aggregates(branch):
        test_data = get_test_data_queryset_via_ca(
            repoid, branch, start_date, end_date, parameter
        )
    else:
        test_data = get_test_data_queryset_via_testrun(
            repoid, branch, start_date, end_date, parameter
        )

    match parameter:
        case "failed_tests":
            test_data = test_data.filter(total_fail_count__gt=0)
        case "flaky_tests":
            test_data = test_data.filter(total_flaky_fail_count__gt=0)
        case "skipped_tests":
            test_data = test_data.filter(total_skip_count__gt=0, total_pass_count=0)
        case _:
            pass

    if term:
        test_data = test_data.filter(computed_name__icontains=term)
    if testsuites:
        test_data = test_data.filter(testsuite__in=testsuites)
    if flags:
        test_data = test_data.filter(flags__overlap=flags)

    return test_data


def _pct_change(current: int | float | None, past: int | float | None) -> float:
    if past is None or past == 0:
        return 0.0
    if current is None:
        current = 0

    return (current - past) / past


def get_repo_aggregates_via_ca(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
):
    if branch is None:
        test_data = TestrunSummary.objects.filter(
            repo_id=repoid,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        )
        repo_data = AggregateDaily.objects.filter(
            repo_id=repoid,
            bucket_daily__gte=start_date,
            bucket_daily__lt=end_date,
        )
    elif branch in PRECOMPUTED_BRANCHES:
        test_data = TestrunBranchSummary.objects.filter(
            repo_id=repoid,
            branch=branch,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        )
        repo_data = BranchAggregateDaily.objects.filter(
            repo_id=repoid,
            branch=branch,
            bucket_daily__gte=start_date,
            bucket_daily__lt=end_date,
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
        test_data.values("computed_name", "testsuite")
        .annotate(
            total_duration=Sum(
                F("avg_duration_seconds")
                * (F("pass_count") + F("fail_count") + F("flaky_fail_count")),
                output_field=FloatField(),
            )
        )
        .order_by("-total_duration")[:slow_test_num]
    )

    result = slow_tests.aggregate(slowest_tests_duration=Sum("total_duration"))

    slowest_tests_duration = result["slowest_tests_duration"] or 0.0

    return daily_aggregates, slowest_tests_duration, slow_test_num


@get_test_result_aggregates_histogram.time()
def get_test_results_aggregates_from_timescale(
    repoid: int, branch: str | None, start_date: datetime, end_date: datetime
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
            repoid, branch, comparison_start_date, comparison_end_date
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


def get_flake_aggregates_via_ca(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
):
    if branch is None:
        test_data = TestrunSummary.objects.filter(
            repo_id=repoid,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        )
        repo_data = AggregateDaily.objects.filter(
            repo_id=repoid,
            bucket_daily__gte=start_date,
            bucket_daily__lt=end_date,
        )
    else:
        test_data = TestrunBranchSummary.objects.filter(
            repo_id=repoid,
            branch=branch,
            timestamp_bin__gte=start_date,
            timestamp_bin__lt=end_date,
        )
        repo_data = BranchAggregateDaily.objects.filter(
            repo_id=repoid,
            branch=branch,
            bucket_daily__gte=start_date,
            bucket_daily__lt=end_date,
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
