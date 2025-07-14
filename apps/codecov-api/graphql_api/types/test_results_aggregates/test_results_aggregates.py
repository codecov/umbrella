import polars as pl
from ariadne import ObjectType
from graphql import GraphQLResolveInfo

from graphql_api.types.enums.enum_types import MeasurementInterval
from utils.ta_types import TestResultsAggregates
from utils.test_results import get_results, use_new_impl


def calculate_aggregates(repoid: int, table: pl.DataFrame) -> pl.DataFrame:
    if use_new_impl(repoid):
        total_tests = (
            pl.col("total_pass_count")
            + pl.col("total_fail_count")
            + pl.col("total_flaky_fail_count")
        )
    else:
        total_tests = pl.col("total_pass_count") + pl.col("total_fail_count")

    total_duration = (
        (pl.col("avg_duration") * total_tests).sum().alias("total_duration")
    )

    num_slow_tests = min(100, max(table.height // 20, 1))

    slowest_tests_duration = (
        (pl.col("avg_duration") * total_tests)
        .top_k(num_slow_tests)
        .sum()
        .alias("slowest_tests_duration")
    )

    return table.with_columns(pl.col("avg_duration")).select(
        total_duration,
        slowest_tests_duration,
        (pl.col("total_skip_count").sum()).alias("skips"),
        (pl.col("total_fail_count").sum()).alias("fails"),
        pl.lit(num_slow_tests).alias("total_slow_tests"),
    )


def test_results_aggregates_from_table(
    repoid: int,
    table: pl.DataFrame,
) -> TestResultsAggregates:
    aggregates = calculate_aggregates(repoid, table).row(0, named=True)
    return TestResultsAggregates(**aggregates)


def test_results_aggregates_with_percentage(
    repoid: int,
    curr_results: pl.DataFrame,
    past_results: pl.DataFrame,
) -> TestResultsAggregates:
    curr_aggregates = calculate_aggregates(repoid, curr_results)
    past_aggregates = calculate_aggregates(repoid, past_results)

    merged_results: pl.DataFrame = pl.concat([past_aggregates, curr_aggregates])

    merged_results = merged_results.with_columns(
        pl.all()
        .pct_change()
        .replace([float("inf"), float("-inf")], None)
        .fill_nan(0)
        .name.suffix("_percent_change")
    )
    aggregates = merged_results.row(1, named=True)

    return TestResultsAggregates(**aggregates)


def generate_test_results_aggregates(
    repoid: int, branch: str, interval: MeasurementInterval
) -> TestResultsAggregates | None:
    curr_results = get_results(repoid, branch, interval.value)
    if curr_results is None:
        return None
    past_results = get_results(repoid, branch, interval.value * 2, interval.value)
    if past_results is None:
        return test_results_aggregates_from_table(repoid, curr_results)
    else:
        return test_results_aggregates_with_percentage(
            repoid, curr_results, past_results
        )


test_results_aggregates_bindable = ObjectType("TestResultsAggregates")


@test_results_aggregates_bindable.field("totalDuration")
def resolve_total_duration(obj: TestResultsAggregates, _: GraphQLResolveInfo) -> float:
    return obj.total_duration


@test_results_aggregates_bindable.field("totalDurationPercentChange")
def resolve_total_duration_percent_change(
    obj: TestResultsAggregates, _: GraphQLResolveInfo
) -> float | None:
    return obj.total_duration_percent_change


@test_results_aggregates_bindable.field("slowestTestsDuration")
def resolve_slowest_tests_duration(
    obj: TestResultsAggregates, _: GraphQLResolveInfo
) -> float:
    return obj.slowest_tests_duration


@test_results_aggregates_bindable.field("slowestTestsDurationPercentChange")
def resolve_slowest_tests_duration_percent_change(
    obj: TestResultsAggregates, _: GraphQLResolveInfo
) -> float | None:
    return obj.slowest_tests_duration_percent_change


@test_results_aggregates_bindable.field("totalSlowTests")
def resolve_total_slow_tests(obj: TestResultsAggregates, _: GraphQLResolveInfo) -> int:
    return obj.total_slow_tests


@test_results_aggregates_bindable.field("totalSlowTestsPercentChange")
def resolve_total_slow_tests_percent_change(
    obj: TestResultsAggregates, _: GraphQLResolveInfo
) -> float | None:
    return obj.total_slow_tests_percent_change


@test_results_aggregates_bindable.field("totalFails")
def resolve_total_fails(obj: TestResultsAggregates, _: GraphQLResolveInfo) -> int:
    return obj.fails


@test_results_aggregates_bindable.field("totalFailsPercentChange")
def resolve_total_fails_percent_change(
    obj: TestResultsAggregates, _: GraphQLResolveInfo
) -> float | None:
    return obj.fails_percent_change


@test_results_aggregates_bindable.field("totalSkips")
def resolve_total_skips(obj: TestResultsAggregates, _: GraphQLResolveInfo) -> int:
    return obj.skips


@test_results_aggregates_bindable.field("totalSkipsPercentChange")
def resolve_total_skips_percent_change(
    obj: TestResultsAggregates, _: GraphQLResolveInfo
) -> float | None:
    return obj.skips_percent_change
