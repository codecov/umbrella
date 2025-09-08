import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, TypedDict

from ariadne import ObjectType
from asgiref.sync import sync_to_async
from django.core.exceptions import ValidationError
from graphql.type.definition import GraphQLResolveInfo

from graphql_api.helpers.connection import queryset_to_connection
from graphql_api.types.enums import (
    OrderingDirection,
    TestResultsFilterParameter,
    TestResultsOrderingParameter,
)
from graphql_api.types.enums.enum_types import MeasurementInterval
from shared.django_apps.core.models import Repository
from shared.metrics import Histogram
from utils.ta_timescale.flake_aggregates import get_flake_aggregates
from utils.ta_timescale.metadata import get_flags, get_test_suites
from utils.ta_timescale.test_aggregates import (
    get_test_results_aggregates,
)
from utils.ta_timescale.test_results import get_test_results_queryset
from utils.ta_timescale.types import FlakeAggregates, TestResultsAggregates

log = logging.getLogger(__name__)

INTERVAL_30_DAY = 30
INTERVAL_7_DAY = 7
INTERVAL_1_DAY = 1


@dataclass
class TestResultConnection:
    __test__ = False

    edges: list[dict[str, Any]]
    page_info: dict
    total_count: int


def validate(
    interval: int,
    ordering: TestResultsOrderingParameter,
    ordering_direction: OrderingDirection,
    after: str | None,
    before: str | None,
    first: int | None,
    last: int | None,
) -> None:
    if interval not in {INTERVAL_1_DAY, INTERVAL_7_DAY, INTERVAL_30_DAY}:
        raise ValidationError(f"Invalid interval: {interval}")

    if not isinstance(ordering_direction, OrderingDirection):
        raise ValidationError(f"Invalid ordering direction: {ordering_direction}")

    if not isinstance(ordering, TestResultsOrderingParameter):
        raise ValidationError(f"Invalid ordering field: {ordering}")

    if first is not None and last is not None:
        raise ValidationError("First and last can not be used at the same time")

    if after is not None and before is not None:
        raise ValidationError("After and before can not be used at the same time")


class GQLTestResultsOrdering(TypedDict):
    parameter: TestResultsOrderingParameter
    direction: OrderingDirection


class GQLTestResultsFilters(TypedDict):
    parameter: TestResultsFilterParameter | None
    interval: MeasurementInterval
    branch: str | None
    test_suites: list[str] | None
    flags: list[str] | None
    term: str | None


# Bindings for GraphQL types
test_analytics_bindable: ObjectType = ObjectType("TestAnalytics")

test_results_histogram = Histogram(
    "get_test_results_timescale",
    "Time it takes to get the test results",
)


@test_analytics_bindable.field("testResults")
async def resolve_test_results(
    repository: Repository,
    info: GraphQLResolveInfo,
    ordering: GQLTestResultsOrdering | None = None,
    filters: GQLTestResultsFilters | None = None,
    first: int | None = None,
    after: str | None = None,
    last: int | None = None,
    before: str | None = None,
) -> TestResultConnection:
    with test_results_histogram.time():
        measurement_interval = (
            filters.get("interval", MeasurementInterval.INTERVAL_30_DAY)
            if filters
            else MeasurementInterval.INTERVAL_30_DAY
        )
        end_date = datetime.now(UTC)
        start_date = end_date - timedelta(days=measurement_interval.value)
        ordering_param = (
            ordering.get("parameter", TestResultsOrderingParameter.AVG_DURATION)
            if ordering
            else TestResultsOrderingParameter.AVG_DURATION
        )
        ordering_direction = (
            ordering.get("direction", OrderingDirection.DESC)
            if ordering
            else OrderingDirection.DESC
        )

        filters = filters or {}
        branch = filters.get("branch")
        parameter_enum = filters.get("parameter")
        testsuites = filters.get("test_suites")
        flags = filters.get("flags")
        term = filters.get("term")

        parameter = parameter_enum.value if parameter_enum else None
        aggregated_queryset = await sync_to_async(get_test_results_queryset)(
            repoid=repository.repoid,
            start_date=start_date,
            end_date=end_date,
            branch=branch,
            parameter=parameter,
            testsuites=testsuites,
            flags=flags,
            term=term,
        )

        connection = await queryset_to_connection(
            aggregated_queryset,
            ordering=(ordering_param, "name"),
            ordering_direction=(ordering_direction, OrderingDirection.ASC),
            first=first,
            after=after,
            last=last,
            before=before,
        )

        # so we can measure the time it takes to get the edges
        connection.edges

    return connection


@test_analytics_bindable.field("testResultsAggregates")
async def resolve_test_results_aggregates(
    repository: Repository,
    info: GraphQLResolveInfo,
    branch: str | None = None,
    interval: MeasurementInterval | None = None,
    **_: Any,
) -> TestResultsAggregates | None:
    measurement_interval = interval if interval else MeasurementInterval.INTERVAL_30_DAY
    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=measurement_interval.value)
    return await sync_to_async(get_test_results_aggregates)(
        repoid=repository.repoid,
        branch=branch,
        start_date=start_date,
        end_date=end_date,
    )


@test_analytics_bindable.field("flakeAggregates")
async def resolve_flake_aggregates(
    repository: Repository,
    info: GraphQLResolveInfo,
    branch: str | None = None,
    interval: MeasurementInterval | None = None,
    **_: Any,
) -> FlakeAggregates | None:
    measurement_interval = interval if interval else MeasurementInterval.INTERVAL_30_DAY
    end_date = datetime.now(UTC)
    start_date = end_date - timedelta(days=measurement_interval.value)
    return await sync_to_async(get_flake_aggregates)(
        repoid=repository.repoid,
        branch=branch,
        start_date=start_date,
        end_date=end_date,
    )


@test_analytics_bindable.field("testSuites")
async def resolve_test_suites(
    repository: Repository, info: GraphQLResolveInfo, term: str | None = None, **_: Any
) -> list[str]:
    result = await sync_to_async(get_test_suites)(repository, info, term)
    return sorted(result)


@test_analytics_bindable.field("flags")
async def resolve_flags(
    repository: Repository, info: GraphQLResolveInfo, term: str | None = None, **_: Any
) -> list[str]:
    result = await sync_to_async(get_flags)(repository, info, term)
    return sorted(result)
