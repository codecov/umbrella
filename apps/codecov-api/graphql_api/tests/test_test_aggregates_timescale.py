from datetime import UTC, datetime, timedelta

import pytest

from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.ta_timeseries.models import Testrun, calc_test_id
from utils.ta_timescale.test_aggregates import get_test_results_aggregates
from utils.ta_timescale.utils import refresh_ta_timeseries_aggregates

from .helper import GraphQLTestHelper


@pytest.fixture(autouse=True)
def repository():
    owner = OwnerFactory(username="codecov-user")
    repo = RepositoryFactory(
        author=owner, name="testRepoName", active=True, branch="main"
    )
    return repo


@pytest.fixture
def populate_timescale_test_results_aggregates(repository):
    now_utc = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    thirty_days_ago = now_utc - timedelta(days=30)

    recent_testruns = [
        Testrun(
            repo_id=repository.repoid,
            test_id=calc_test_id(f"test_{i}", "", f"testsuite{i}"),
            timestamp=now_utc - timedelta(days=1) + timedelta(minutes=i * 60),
            testsuite="testsuite1",
            classname="",
            name=f"test_{i}",
            computed_name=f"test_{i}",
            outcome="pass" if i % 2 == 0 else "failure",
            duration_seconds=10.0,
            commit_sha=f"commit{i + 1}",
            flags=["flag1"],
            branch="main",
        )
        for i in range(2)
    ]

    old_testruns = [
        Testrun(
            test_id=calc_test_id(f"test_{i}", "", f"testsuite{i}"),
            repo_id=repository.repoid,
            timestamp=thirty_days_ago - timedelta(days=1) + timedelta(minutes=i * 60),
            testsuite=f"testsuite{i}",
            classname="",
            name=f"test_{i}",
            computed_name=f"test_{i}",
            outcome="pass" if i == 2 else "failure" if i == 3 else "skip",
            duration_seconds=15.0 + (i) if i != 4 else 0.0,
            commit_sha=f"commit {i}",
            flags=[f"flag{i}"],
            branch="feature-branch",
        )
        for i in range(2, 5)
    ]

    testruns = recent_testruns + old_testruns

    Testrun.objects.bulk_create(testruns)

    min_timestamp = datetime.now(UTC) - timedelta(days=60)
    max_timestamp = datetime.now(UTC)

    # Refresh all continuous aggregates
    refresh_ta_timeseries_aggregates(min_timestamp, max_timestamp)


@pytest.mark.django_db(databases=["default", "ta_timeseries"], transaction=True)
class TestTestResultsAggregatesTimescale(GraphQLTestHelper):
    def test_test_results_aggregates_timescale(
        self, repository, populate_timescale_test_results_aggregates
    ):
        result = get_test_results_aggregates(
            repository.repoid,
            "main",
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=30),
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0),
        )

        assert result is not None
        assert result.total_duration == 20.0
        assert result.fails == 1
        assert result.skips == 0
        assert result.slowest_tests_duration == 10.0
        assert result.total_slow_tests == 1
        assert result.slowest_tests_duration_percent_change == 0.0
        assert result.total_duration_percent_change == 0.0
        assert result.fails_percent_change == 0.0
        assert result.skips_percent_change == 0.0
        assert result.total_slow_tests_percent_change == 0.0

    def test_gql_query_test_results_aggregates_timescale(
        self, repository, populate_timescale_test_results_aggregates, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResultsAggregates {{
                                    totalDuration
                                    slowestTestsDuration
                                    totalFails
                                    totalSkips
                                    totalSlowTests
                                    totalDurationPercentChange
                                    slowestTestsDurationPercentChange
                                    totalFailsPercentChange
                                    totalSkipsPercentChange
                                    totalSlowTestsPercentChange
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_test_results_aggregates_timescale_branch(
        self, repository, populate_timescale_test_results_aggregates, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResultsAggregates(branch: "main") {{
                                    totalDuration
                                    slowestTestsDuration
                                    totalFails
                                    totalSkips
                                    totalSlowTests
                                    totalDurationPercentChange
                                    slowestTestsDurationPercentChange
                                    totalFailsPercentChange
                                    totalSkipsPercentChange
                                    totalSlowTestsPercentChange
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_test_results_aggregates_timescale_non_precomputed_branch(
        self, repository, populate_timescale_test_results_aggregates, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResultsAggregates(branch: "feature-branch") {{
                                    totalDuration
                                    slowestTestsDuration
                                    totalFails
                                    totalSkips
                                    totalSlowTests
                                    totalDurationPercentChange
                                    slowestTestsDurationPercentChange
                                    totalFailsPercentChange
                                    totalSkipsPercentChange
                                    totalSlowTestsPercentChange
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result
