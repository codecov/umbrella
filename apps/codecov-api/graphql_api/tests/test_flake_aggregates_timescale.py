from datetime import UTC, datetime, timedelta

import pytest
from django.db import connections

from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.ta_timeseries.models import Testrun, calc_test_id
from utils.timescale_test_results import get_flake_aggregates_from_timescale

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
        result = get_flake_aggregates_from_timescale(
            repository.repoid,
            "main",
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=30),
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0),
        )
        assert result is not None
        assert result.flake_rate == 0.5
        assert result.flake_count == 1
        assert result.flake_rate_percent_change == -0.5
        assert result.flake_count_percent_change == -0.6666666666666666

    def test_gql_query_flake_aggregates_timescale(
        self, repository, populate_timescale_flake_aggregates, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                flakeAggregates {{
                                    flakeRate
                                    flakeCount
                                    flakeRatePercentChange
                                    flakeCountPercentChange
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result
