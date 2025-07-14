from datetime import UTC, datetime, timedelta

import pytest
from django.db import connections

from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
from shared.django_apps.ta_timeseries.models import Testrun
from utils.timescale_test_results import get_test_results_queryset

from .helper import GraphQLTestHelper


@pytest.fixture(autouse=True)
def repository():
    owner = OwnerFactory(username="codecov-user")
    repo = RepositoryFactory(author=owner, name="testRepoName", active=True)

    return repo


@pytest.fixture
def populate_timescale(repository):
    Testrun.objects.bulk_create(
        [
            Testrun(
                repo_id=repository.repoid,
                timestamp=datetime.now(UTC) - timedelta(days=5 - i),
                testsuite=f"testsuite{i}",
                classname="",
                name=f"name{i}",
                computed_name=f"name{i}",
                outcome="pass" if i % 2 == 0 else "failure",
                duration_seconds=i,
                commit_sha=f"test_commit {i}",
                flags=["flag1", "flag2"] if i % 2 == 0 else ["flag3"],
                branch="main",
            )
            for i in range(5)
        ]
    )

    with connections["ta_timeseries"].cursor() as cursor:
        cursor.execute(
            "CALL refresh_continuous_aggregate('ta_timeseries_testrun_branch_summary_1day', %s, %s)",
            [
                (datetime.now(UTC) - timedelta(days=10)),
                datetime.now(UTC),
            ],
        )


class TestAnalyticsTestCaseNew(GraphQLTestHelper):
    @pytest.mark.django_db(databases=["default", "ta_timeseries"], transaction=True)
    def test_gql_query(self, repository, populate_timescale, snapshot):
        result = get_test_results_queryset(
            repository.repoid,
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
            - timedelta(days=30),
            datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0),
            "main",
        )

        assert result.count() == 5
        assert snapshot("json") == [
            {k: v for k, v in row.items() if k != "updated_at"} for row in result
        ]
