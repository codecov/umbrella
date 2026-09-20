from typing import Any

from ariadne import ObjectType
from graphql import GraphQLResolveInfo

test_result_bindable = ObjectType("TestResult")
test_result_bindable.set_alias("commitsFailed", "commits_where_fail")


@test_result_bindable.field("name")
def resolve_name(testrun: dict[str, Any], _: GraphQLResolveInfo) -> str:
    return testrun["computed_name"].replace("\x1f", " ")


@test_result_bindable.field("avgDuration")
def resolve_avg_duration(testrun: dict[str, Any], _: GraphQLResolveInfo) -> float:
    return testrun.get("avg_duration") or 0.0
