from datetime import UTC, datetime, timedelta

import pytest
from django.db import connections

from graphql_api.tests.test_test_analytics import base_gql_query
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.ta_timeseries.models import Testrun, calc_test_id

from .helper import GraphQLTestHelper


@pytest.fixture(autouse=True)
def repository():
    owner = OwnerFactory(username="codecov-user")
    repo = RepositoryFactory(author=owner, name="testRepoName", active=True)
    return repo


@pytest.fixture
def new_ta_enabled(mocker):
    mocker.patch(
        "graphql_api.types.test_analytics.test_analytics.READ_NEW_TA.check_value",
        return_value=True,
    )


@pytest.fixture
def populate_timescale_flake_aggregates(repository):
    # Create testruns with different flaky behavior patterns
    today = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    thirty_days_ago = today - timedelta(days=30)

    # Recent testruns (today's data) - some flaky tests
    recent_testruns = [
        Testrun(
            repo_id=repository.repoid,
            test_id=calc_test_id(f"flaky_test_{i}", "", f"testsuite{i}"),
            timestamp=today - timedelta(days=1) + timedelta(minutes=i * 60),
            testsuite="testsuite1",
            classname="",
            name=f"flaky_test_{i}",
            computed_name=f"flaky_test_{i}",
            outcome="flaky_fail" if i == 0 else "pass",
            duration_seconds=10.0,
            commit_sha=f"commit{i + 1}",
            flags=["flag1"],
            branch="main",
        )
        for i in range(2)
    ]

    # Old testruns (30 days ago data) - different flaky pattern for comparison
    old_testruns = [
        Testrun(
            test_id=calc_test_id(f"flaky_test_{i}", "", f"testsuite{i}"),
            repo_id=repository.repoid,
            timestamp=thirty_days_ago - timedelta(days=1) + timedelta(minutes=i * 60),
            testsuite=f"testsuite{i}",
            classname="",
            name=f"flaky_test_{i}",
            computed_name=f"flaky_test_{i}",
            outcome="flaky_fail",
            duration_seconds=15.0 + (i),
            commit_sha=f"commit {i}",
            flags=[f"flag{i}"],
            branch="main",
        )
        for i in range(2, 5)
    ]

    testruns = recent_testruns + old_testruns

    Testrun.objects.bulk_create(testruns)

    # Refresh the continuous aggregate to ensure the repo summary is updated
    min_timestamp = datetime.now(UTC) - timedelta(days=60)
    max_timestamp = datetime.now(UTC)

    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('ta_timeseries_testrun_branch_summary_1day', %s, %s)",
            [min_timestamp, max_timestamp],
        )


@pytest.mark.usefixtures("new_ta_enabled")
@pytest.mark.django_db(databases=["default", "ta_timeseries"], transaction=True)
class TestFlakeAggregatesTimescale(GraphQLTestHelper):
    def test_gql_query_flake_aggregates(
        self, repository, populate_timescale_flake_aggregates, snapshot
    ):
        query = base_gql_query % (
            repository.author.username,
            repository.name,
            """
            flakeAggregates {
                flakeRate
                flakeCount
                flakeRatePercentChange
                flakeCountPercentChange
            }
            """,
        )
        result = self.gql_request(query, owner=repository.author)
        flake_aggregates = result["owner"]["repository"]["testAnalytics"][
            "flakeAggregates"
        ]

        # Verify the structure is returned (exact values may vary based on continuous aggregate calculation)
        assert "flakeRate" in flake_aggregates
        assert "flakeCount" in flake_aggregates
        assert "flakeRatePercentChange" in flake_aggregates
        assert "flakeCountPercentChange" in flake_aggregates

        assert snapshot("json") == flake_aggregates
