from datetime import datetime
from typing import Literal

from django.db.models import (
    Aggregate,
    Case,
    Count,
    F,
    FloatField,
    Func,
    IntegerField,
    Max,
    Min,
    Q,
    Sum,
    TextField,
    Value,
    When,
    Window,
)
from django.db.models.functions import Coalesce, RowNumber

from shared.django_apps.ta_timeseries.models import Testrun
from utils.ta_timescale.utils import (
    _calculate_slow_test_num,
    _should_use_precomputed_aggregates,
    get_daily_aggregate_querysets,
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


class Cardinality(Func):
    function = "cardinality"
    template = "%(function)s(%(expressions)s)"


def get_test_data_queryset_via_ca(
    repoid: int,
    branch: Literal["main", "master", "develop"] | None,
    start_date: datetime,
    end_date: datetime,
    parameter: Literal["flaky_tests", "failed_tests", "slowest_tests", "skipped_tests"]
    | None = None,
    merge_arrays: bool = False,
):
    test_data, _ = get_daily_aggregate_querysets(repoid, branch, start_date, end_date)

    test_data = test_data.values("test_id").annotate(  # type: ignore[assignment]
        total_pass_count=Sum("pass_count"),
        total_fail_count=Sum("fail_count"),
        total_flaky_fail_count=Sum("flaky_fail_count"),
        total_skip_count=Sum("skip_count"),
        total_count=Sum(
            F("pass_count") + F("fail_count") + F("flaky_fail_count"),
            output_field=FloatField(),
        ),
        runs_failed=Sum(F("fail_count") + F("flaky_fail_count")),
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
        last_outcome=Last("last_outcome", "updated_at", output_field=TextField()),
        updated_at=Max("updated_at"),
        computed_name=Min("computed_name"),
        name=F("computed_name"),
        testsuite=Min("testsuite"),
    )

    if merge_arrays:
        test_data = test_data.annotate(
            commits_where_fail=Coalesce(
                Cardinality(ArrayMergeDedupe("failing_commits")),
                0,
                output_field=IntegerField(),
            ),
            flags=ArrayMergeDedupe("flags"),
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
            test_data = test_data.filter(last_outcome="skip")
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
    merge_arrays: bool = False,
):
    test_data = Testrun.objects.filter(
        repo_id=repoid,
        branch=branch,
        timestamp__gt=start_date,
        timestamp__lte=end_date,
    )

    test_data = test_data.values("test_id").annotate(  # type: ignore[assignment]
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
        runs_failed=Sum(
            Case(
                When(outcome__in=["failure", "flaky_fail"], then=Value(1)),
                default=Value(0),
            )
        ),
        total_count=(
            F("total_pass_count") + F("total_fail_count") + F("total_flaky_fail_count")
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
        last_outcome=Last("outcome", "timestamp", output_field=TextField()),
        updated_at=Max("timestamp"),
        computed_name=Min("computed_name"),
        name=F("computed_name"),
        testsuite=Min("testsuite"),
    )

    if merge_arrays:
        test_data = test_data.annotate(
            commits_where_fail=Count(
                "commit_sha",
                filter=Q(outcome__in=["failure", "flaky_fail"]),
                distinct=True,
            ),
            flags=ArrayMergeDedupe("flags"),
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
            test_data = test_data.filter(last_outcome="skip")
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
    merge_arrays: bool = False,
):
    if _should_use_precomputed_aggregates(branch):
        test_data = get_test_data_queryset_via_ca(
            repoid, branch, start_date, end_date, parameter, merge_arrays
        )
    else:
        test_data = get_test_data_queryset_via_testrun(
            repoid, branch, start_date, end_date, parameter, merge_arrays
        )

    if term:
        test_data = test_data.filter(computed_name__icontains=term)
    if testsuites:
        test_data = test_data.filter(testsuite__in=testsuites)
    if flags:
        assert merge_arrays, "flags are only supported when merge_arrays is True"
        test_data = test_data.filter(flags__overlap=flags)

    return test_data
