from datetime import UTC, datetime, timedelta

import pytest
from django.db import connections

from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.ta_timeseries.models import (
    Testrun,
    calc_test_id,
)
from utils.timescale.test_results import get_test_results_queryset

from .helper import GraphQLTestHelper


@pytest.fixture(autouse=True)
def repository():
    owner = OwnerFactory(username="codecov-user")
    repo = RepositoryFactory(
        repoid=1, author=owner, name="testRepoName", active=True, branch="main"
    )

    return repo


@pytest.fixture
def new_ta_enabled(mocker):
    mocker.patch(
        "graphql_api.types.test_analytics.test_analytics.READ_NEW_TA.check_value",
        return_value=True,
    )


@pytest.fixture
def populate_timescale(repository):
    Testrun.objects.bulk_create(
        [
            Testrun(
                repo_id=repository.repoid,
                timestamp=datetime.now(UTC) - timedelta(days=10 - i),
                testsuite=f"testsuite{i}",
                classname="",
                name=f"name{i}",
                computed_name=f"name{i}",
                test_id=calc_test_id(f"name{i}", "", f"testsuite{i}"),
                outcome="pass" if i % 2 == 0 else "failure",
                duration_seconds=i * 2,
                commit_sha=f"test_commit {i}",
                flags=["flag1", "flag2"] if i % 2 == 0 else ["flag3"],
                branch="feature",
            )
            for i in range(5)
        ]
    )

    Testrun.objects.bulk_create(
        [
            Testrun(
                repo_id=repository.repoid,
                timestamp=datetime.now(UTC) - timedelta(days=5 - i),
                testsuite=f"testsuite{i}",
                classname="",
                name=f"name{i}",
                computed_name=f"name{i}",
                test_id=calc_test_id(f"name{i}", "", f"testsuite{i}"),
                outcome="pass" if i % 2 == 0 else "failure",
                duration_seconds=i,
                commit_sha=f"test_commit {i}",
                flags=["flag1", "flag2"] if i % 2 == 0 else ["flag3"],
                branch="main",
            )
            for i in range(5)
        ]
    )

    Testrun.objects.bulk_create(
        [
            Testrun(
                repo_id=repository.repoid,
                timestamp=datetime.now(UTC) - timedelta(days=1),
                testsuite="testsuite5",
                classname="",
                name="name5",
                computed_name="name5",
                test_id=calc_test_id("name5", "", "testsuite5"),
                outcome="skip",
                duration_seconds=0,
                commit_sha="test_commit_skip",
                flags=None,
                branch="main",
            )
        ]
    )

    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('ta_timeseries_branch_test_aggregate_hourly', %s, %s)",
            [
                (datetime.now(UTC) - timedelta(days=60)),
                None,
            ],
        )
        cursor.execute(
            "CALL refresh_continuous_aggregate('ta_timeseries_branch_test_aggregate_daily', %s, %s)",
            [
                (datetime.now(UTC) - timedelta(days=60)),
                None,
            ],
        )
        cursor.execute(
            "CALL refresh_continuous_aggregate('ta_timeseries_test_aggregate_hourly', %s, %s)",
            [
                (datetime.now(UTC) - timedelta(days=60)),
                None,
            ],
        )
        cursor.execute(
            "CALL refresh_continuous_aggregate('ta_timeseries_test_aggregate_daily', %s, %s)",
            [
                (datetime.now(UTC) - timedelta(days=60)),
                None,
            ],
        )


@pytest.mark.usefixtures("new_ta_enabled")
@pytest.mark.django_db(databases=["default", "ta_timeseries"], transaction=True)
class TestAnalyticsTestCaseNew(GraphQLTestHelper):
    def test_gql_query(self, repository, populate_timescale, new_ta_enabled, snapshot):
        result = get_test_results_queryset(
            repository.repoid,
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=30),
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0),
            "main",
        )

        assert result.count() == 6
        assert snapshot("json") == [
            {
                k: (
                    bytes(v).hex()
                    if k == "test_id" and isinstance(v, memoryview)
                    else v
                )
                for k, v in row.items()
                if k != "updated_at"
            }
            for row in result
        ]

    def test_gql_query_test_results_timescale(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResults {{
                                    totalCount
                                    edges {{
                                        cursor
                                        node {{
                                            name
                                            failureRate
                                            flakeRate
                                            avgDuration
                                            totalDuration
                                            totalFailCount
                                            totalFlakyFailCount
                                            totalPassCount
                                            totalSkipCount
                                            commitsFailed
                                            lastDuration
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_test_results_timescale_branch(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResults(filters: {{branch: "main"}}) {{
                                    totalCount
                                    edges {{
                                        cursor
                                        node {{
                                            name
                                            failureRate
                                            flakeRate
                                            avgDuration
                                            totalDuration
                                            totalFailCount
                                            totalFlakyFailCount
                                            totalPassCount
                                            totalSkipCount
                                            commitsFailed
                                            lastDuration
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_test_results_timescale_non_precomputed_branch(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResults(filters: {{branch: "feature"}}) {{
                                    totalCount
                                    edges {{
                                        cursor
                                        node {{
                                            name
                                            failureRate
                                            flakeRate
                                            avgDuration
                                            totalDuration
                                            totalFailCount
                                            totalFlakyFailCount
                                            totalPassCount
                                            totalSkipCount
                                            commitsFailed
                                            lastDuration
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_test_results_timescale_skipped_parameter(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResults(filters: {{branch: "main", parameter: SKIPPED_TESTS}}) {{
                                    totalCount
                                    edges {{
                                        cursor
                                        node {{
                                            name
                                            failureRate
                                            flakeRate
                                            avgDuration
                                            totalDuration
                                            totalFailCount
                                            totalFlakyFailCount
                                            totalPassCount
                                            totalSkipCount
                                            commitsFailed
                                            lastDuration
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_test_results_timescale_slowest_parameter(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testResults(filters: {{parameter: SLOWEST_TESTS}}) {{
                                    totalCount
                                    edges {{
                                        cursor
                                        node {{
                                            name
                                            failureRate
                                            flakeRate
                                            avgDuration
                                            totalDuration
                                            totalFailCount
                                            totalFlakyFailCount
                                            totalPassCount
                                            totalSkipCount
                                            commitsFailed
                                            lastDuration
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_testsuites_and_flags_resolvers(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testSuites
                                flags
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result

    def test_gql_query_testsuites_and_flags_resolvers_with_term(
        self, repository, populate_timescale, snapshot
    ):
        query = f"""
            query {{
                owner(username: "{repository.author.username}") {{
                    repository(name: "{repository.name}") {{
                        ... on Repository {{
                            testAnalytics {{
                                testSuites(term: "testsuite1")
                                flags(term: "flag1")
                            }}
                        }}
                    }}
                }}
            }}
        """

        result = self.gql_request(query, owner=repository.author)

        assert snapshot("json") == result
