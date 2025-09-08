from django.urls import reverse
from rest_framework import status

from codecov.tests.base_test import InternalAPITest
from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
from shared.django_apps.ta_timeseries.tests.factories import TestrunFactory
from utils.test_utils import APIClient


class TestAnalyticsViewsetTests(InternalAPITest):
    databases = {"default", "ta_timeseries"}

    def setUp(self):
        self.org = OwnerFactory()
        self.repo = RepositoryFactory(author=self.org)
        self.current_owner = OwnerFactory(
            permission=[self.repo.repoid], organizations=[self.org.ownerid]
        )

        self.client = APIClient()
        self.client.force_login_owner(self.current_owner)

        # Create test data
        self.testruns = [
            TestrunFactory(
                repo_id=self.repo.repoid,
                commit_sha="abc123def",
                branch="main",
                outcome="pass",
                duration_seconds=1.5,
            ),
            TestrunFactory(
                repo_id=self.repo.repoid,
                commit_sha="def456ghi",
                branch="feature-branch",
                outcome="failure",
                duration_seconds=2.3,
            ),
        ]

    def test_list_test_analytics(self):
        """Test that the new test-analytics endpoint returns test data"""
        url = reverse(
            "api-v2-test-analytics-list",
            kwargs={
                "service": self.org.service,
                "owner_username": self.org.username,
                "repo_name": self.repo.name,
            },
        )
        res = self.client.get(url)
        assert res.status_code == status.HTTP_200_OK

        data = res.json()
        assert "count" in data
        assert "results" in data
        assert data["count"] == 2
        assert len(data["results"]) == 2

        # Check that results contain expected fields
        result = data["results"][0]
        assert "test_id" in result
        assert "name" in result
        assert "outcome" in result
        assert "duration_seconds" in result
        assert "commit_sha" in result
        assert "branch" in result
        assert "repo_id" in result
