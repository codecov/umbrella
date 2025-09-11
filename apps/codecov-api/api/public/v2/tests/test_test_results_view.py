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

        # Remove timestamps from results for consistent comparison
        for result in data["results"]:
            result.pop("timestamp", None)

        # Sort results by test_id for consistent ordering
        data["results"].sort(key=lambda x: x["test_id"])

        expected_data = {
            "count": 2,
            "next": None,
            "previous": None,
            "total_pages": 1,
            "results": [
                {
                    "test_id": b"test_0".hex(),
                    "name": "test_0",
                    "classname": "class_0",
                    "testsuite": "suite_0",
                    "computed_name": "class_0::test_0",
                    "outcome": "pass",
                    "duration_seconds": 1.5,
                    "failure_message": None,
                    "framework": "Pytest",
                    "filename": "test_0.py",
                    "repo_id": self.repo.repoid,
                    "commit_sha": "abc123def",
                    "branch": "main",
                    "flags": [],
                    "upload_id": 1,
                    "properties": None,
                },
                {
                    "test_id": b"test_1".hex(),
                    "name": "test_1",
                    "classname": "class_1",
                    "testsuite": "suite_1",
                    "computed_name": "class_1::test_1",
                    "outcome": "failure",
                    "duration_seconds": 2.3,
                    "failure_message": "failure_message_failure",
                    "framework": "Pytest",
                    "filename": "test_1.py",
                    "repo_id": self.repo.repoid,
                    "commit_sha": "def456ghi",
                    "branch": "feature-branch",
                    "flags": [],
                    "upload_id": 1,
                    "properties": None,
                },
            ],
        }

        assert data == expected_data
