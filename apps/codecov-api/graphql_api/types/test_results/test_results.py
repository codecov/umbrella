from datetime import datetime
from typing import Any

from ariadne import ObjectType
from graphql import GraphQLResolveInfo

from graphql_api.types.test_analytics.test_analytics import TestResultsRow

test_result_bindable = ObjectType("TestResult")


@test_result_bindable.field("name")
def resolve_name(test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo) -> str:
    if isinstance(test, dict):
        return test["computed_name"].replace("\x1f", " ")
    return test.name.replace("\x1f", " ")


@test_result_bindable.field("updatedAt")
def resolve_updated_at(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> datetime:
    if isinstance(test, dict):
        return test["updated_at"]
    return test.updated_at


@test_result_bindable.field("commitsFailed")
def resolve_commits_failed(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> int:
    if isinstance(test, dict):
        return test["commits_where_fail"]
    return test.commits_where_fail


@test_result_bindable.field("failureRate")
def resolve_failure_rate(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> float:
    if isinstance(test, dict):
        return test["failure_rate"]
    return test.failure_rate


@test_result_bindable.field("flakeRate")
def resolve_flake_rate(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> float:
    if isinstance(test, dict):
        return test["flake_rate"]
    return test.flake_rate


@test_result_bindable.field("avgDuration")
def resolve_avg_duration(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> float:
    if isinstance(test, dict):
        return test["avg_duration"]
    return test.avg_duration


@test_result_bindable.field("totalDuration")
def resolve_total_duration(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> float:
    if isinstance(test, dict):
        return test["total_duration"]
    return test.total_duration


@test_result_bindable.field("lastDuration")
def resolve_last_duration(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> float:
    if isinstance(test, dict):
        return test["last_duration"]
    return test.last_duration


@test_result_bindable.field("totalFailCount")
def resolve_total_fail_count(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> int:
    if isinstance(test, dict):
        return test["total_fail_count"]
    return test.total_fail_count


@test_result_bindable.field("totalFlakyFailCount")
def resolve_total_flaky_fail_count(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> int:
    if isinstance(test, dict):
        return test["total_flaky_fail_count"]
    return test.total_flaky_fail_count


@test_result_bindable.field("totalSkipCount")
def resolve_total_skip_count(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> int:
    if isinstance(test, dict):
        return test["total_skip_count"]
    return test.total_skip_count


@test_result_bindable.field("totalPassCount")
def resolve_total_pass_count(
    test: TestResultsRow | dict[str, Any], _: GraphQLResolveInfo
) -> int:
    if isinstance(test, dict):
        return test["total_pass_count"]
    return test.total_pass_count
