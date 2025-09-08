import pytest
from django.urls import reverse
from rest_framework import status

from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
from shared.django_apps.ta_timeseries.tests.factories import TestrunFactory
from utils.test_utils import APIClient


@pytest.fixture
def org():
    return OwnerFactory()


@pytest.fixture
def repo(org):
    return RepositoryFactory(author=org)


@pytest.fixture
def current_owner(repo, org):
    return OwnerFactory(permission=[repo.repoid], organizations=[org.ownerid])


@pytest.fixture
def api_client(current_owner):
    client = APIClient()
    client.force_login_owner(current_owner)
    return client


@pytest.fixture
def testruns(repo):
    return [
        TestrunFactory(
            repo_id=repo.repoid,
            commit_sha="abc123def",
            branch="main",
            outcome="pass",
            duration_seconds=1.5,
        ),
        TestrunFactory(
            repo_id=repo.repoid,
            commit_sha="def456ghi",
            branch="feature-branch",
            outcome="failure",
            duration_seconds=2.3,
        ),
    ]


@pytest.mark.django_db(databases=["default", "ta_timeseries"])
def test_list_test_analytics(org, repo, api_client, testruns, snapshot):
    url = reverse(
        "api-v2-test-analytics-list",
        kwargs={
            "service": org.service,
            "owner_username": org.username,
            "repo_name": repo.name,
        },
    )
    res = api_client.get(url)
    assert res.status_code == status.HTTP_200_OK

    data = res.json()

    for result in data.get("results", []):
        result.pop("timestamp", None)

    assert snapshot("json") == data
