from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from django.test import override_settings
from django.urls import reverse
from django.utils.http import urlencode
from rest_framework.test import APITestCase

from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
from shared.django_apps.ta_timeseries.models import Testrun
from utils.test_utils import APIClient

FAKE_SUPER_TOKEN = "testaxs3o76rdcdpfzexuccx3uatui2nw73r"


class EvalsViewSetTestCase(APITestCase):
    databases = {"default", "ta_timeseries"}

    def setUp(self):
        self.service = "github"
        self.org = OwnerFactory(username="codecov", service=self.service)
        self.repo = RepositoryFactory(author=self.org, name="test-repo", active=True)
        self.current_owner = OwnerFactory(
            username="codecov-user",
            service="github",
            organizations=[self.org.ownerid],
            permission=[self.repo.repoid],
        )
        self.client = APIClient()
        self.client.force_login_owner(self.current_owner)

    def _request(self, url_name, user_token=None, method="get", **params):
        if user_token:
            self.client.logout()

        url = reverse(
            url_name,
            kwargs={
                "service": self.service,
                "owner_username": self.org.username,
                "repo_name": self.repo.name,
            },
        )

        qs = urlencode(params)
        url = f"{url}?{qs}"
        request_method = getattr(self.client, method.lower())
        return (
            request_method(url, HTTP_AUTHORIZATION=f"Bearer {user_token}")
            if user_token
            else request_method(url)
        )

    def _request_summary(self, user_token=None, **params):
        return self._request("api-v2-evals-summary", user_token, **params)

    def _request_compare(self, user_token=None, **params):
        return self._request("api-v2-evals-compare", user_token, **params)

    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    @patch("api.shared.permissions.SuperTokenPermissions.has_permission")
    def test_no_summary_if_unauthenticated_token_request(
        self,
        super_token_permissions_has_permission,
        repository_artifact_permissions_has_permission,
    ):
        repository_artifact_permissions_has_permission.return_value = False
        super_token_permissions_has_permission.return_value = False

        res = self._request_summary()
        assert res.status_code == 403

    @override_settings(SUPER_API_TOKEN=FAKE_SUPER_TOKEN)
    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    def test_no_summary_if_not_super_token_nor_user_token(
        self, repository_artifact_permissions_has_permission
    ):
        repository_artifact_permissions_has_permission.return_value = False
        res = self._request_summary("73c8d301-2e0b-42c0-9ace-95eef6b68e86")
        assert res.status_code == 401
        assert res.data["detail"] == "Invalid token."

    @override_settings(SUPER_API_TOKEN=FAKE_SUPER_TOKEN)
    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    def test_no_summary_if_super_token_but_no_GET_request(
        self, repository_artifact_permissions_has_permission
    ):
        repository_artifact_permissions_has_permission.return_value = False
        res = self._request_summary(FAKE_SUPER_TOKEN, method="post")
        assert res.status_code == 401
        assert res.data["detail"] == "Invalid token."

    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    @patch("rollouts.READ_NEW_EVALS.check_value")
    def test_no_summary_if_feature_flag_disabled(
        self, mock_feature_flag_check, repository_artifact_permissions_has_permission
    ):
        repository_artifact_permissions_has_permission.return_value = True
        mock_feature_flag_check.return_value = False

        res = self._request_summary()
        assert res.status_code == 403

    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    @patch("api.shared.permissions.SuperTokenPermissions.has_permission")
    def test_no_compare_if_unauthenticated_token_request(
        self,
        super_token_permissions_has_permission,
        repository_artifact_permissions_has_permission,
    ):
        repository_artifact_permissions_has_permission.return_value = False
        super_token_permissions_has_permission.return_value = False

        res = self._request_compare()
        assert res.status_code == 403

    @override_settings(SUPER_API_TOKEN=FAKE_SUPER_TOKEN)
    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    def test_no_compare_if_not_super_token_nor_user_token(
        self, repository_artifact_permissions_has_permission
    ):
        repository_artifact_permissions_has_permission.return_value = False
        res = self._request_compare("73c8d301-2e0b-42c0-9ace-95eef6b68e86")
        assert res.status_code == 401
        assert res.data["detail"] == "Invalid token."

    @override_settings(SUPER_API_TOKEN=FAKE_SUPER_TOKEN)
    @patch("api.shared.permissions.RepositoryArtifactPermissions.has_permission")
    def test_no_compare_if_super_token_but_no_GET_request(
        self, repository_artifact_permissions_has_permission
    ):
        repository_artifact_permissions_has_permission.return_value = False
        res = self._request_compare(FAKE_SUPER_TOKEN, method="post")
        assert res.status_code == 401
        assert res.data["detail"] == "Invalid token."

    def _make_testruns(self):
        # Create 3 testruns: 2 pass, 1 fail, with different durations, costs, and scores
        base_time = datetime.now(UTC)
        Testrun.objects.using("ta_timeseries").create(
            timestamp=base_time,
            test_id=b"id1",
            name="test1",
            classname="ClassA",
            outcome="pass",
            duration_seconds=10.0,
            repo_id=self.repo.repoid,
            commit_sha="abc123",
            properties={
                "eval": {
                    "cost": 5.0,
                    "scores": [
                        {"name": "accuracy", "score": 0.9},
                        {"name": "f1", "score": 0.8},
                    ],
                }
            },
        )
        Testrun.objects.using("ta_timeseries").create(
            timestamp=base_time + timedelta(seconds=1),
            test_id=b"id2",
            name="test2",
            classname="ClassA",
            outcome="pass",
            duration_seconds=20.0,
            repo_id=self.repo.repoid,
            commit_sha="abc123",
            properties={
                "eval": {
                    "cost": 7.0,
                    "scores": [
                        {"name": "accuracy", "score": 0.7},
                        {"name": "f1", "score": 0.6},
                    ],
                }
            },
        )
        Testrun.objects.using("ta_timeseries").create(
            timestamp=base_time + timedelta(seconds=2),
            test_id=b"id3",
            name="test3",
            classname="ClassB",
            outcome="fail",
            duration_seconds=30.0,
            repo_id=self.repo.repoid,
            commit_sha="abc123",
            properties={
                "eval": {
                    "cost": 3.0,
                    "scores": [
                        {"name": "accuracy", "score": 0.1},
                        {"name": "f1", "score": 0.2},
                    ],
                }
            },
        )

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_summary_aggregation_and_filtering_no_filter(self, mock_feature_flag):
        """
        Create Testrun instances and verify the summary endpoint aggregates and filters correctly.
        """
        self._make_testruns()
        res = self._request_summary({})  # no filter
        assert res.status_code == 200
        data = res.json()

        assert data["totalItems"] == 3
        assert data["passedItems"] == 2
        assert data["failedItems"] == 1
        assert data["avgDurationSeconds"] == (10.0 + 20.0 + 30.0) / 3
        assert data["avgCost"] == (5.0 + 7.0 + 3.0) / 3
        assert data["scores"]["accuracy"]["sum"] == pytest.approx(0.9 + 0.7 + 0.1)
        assert data["scores"]["accuracy"]["avg"] == pytest.approx((0.9 + 0.7 + 0.1) / 3)
        assert data["scores"]["f1"]["sum"] == pytest.approx(0.8 + 0.6 + 0.2)
        assert data["scores"]["f1"]["avg"] == pytest.approx((0.8 + 0.6 + 0.2) / 3)

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_summary_aggregation_and_filtering_filter_classA(self, mock_feature_flag):
        self._make_testruns()
        res = self._request_summary(classname="ClassA")
        assert res.status_code == 200
        data = res.json()

        assert data["totalItems"] == 2
        assert data["passedItems"] == 2
        assert data["failedItems"] == 0
        assert data["avgDurationSeconds"] == (10.0 + 20.0) / 2
        assert data["avgCost"] == (5.0 + 7.0) / 2
        assert data["scores"] == {
            "accuracy": {"sum": (0.9 + 0.7), "avg": (0.9 + 0.7) / 2},
            "f1": {"sum": (0.8 + 0.6), "avg": (0.8 + 0.6) / 2},
        }

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_summary_aggregation_and_filtering_filter_classB(self, mock_feature_flag):
        self._make_testruns()
        res = self._request_summary(classname="ClassB")
        assert res.status_code == 200
        data = res.json()

        assert data["totalItems"] == 1
        assert data["passedItems"] == 0
        assert data["failedItems"] == 1
        assert data["avgDurationSeconds"] == 30.0
        assert data["avgCost"] == 3.0
        assert data["scores"] == {
            "accuracy": {"sum": 0.1, "avg": 0.1},
            "f1": {"sum": 0.2, "avg": 0.2},
        }

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_compare_missing_parameters(self, mock_feature_flag):
        """Test that compare endpoint returns 400 when missing required parameters"""
        response = self._request_compare()
        assert response.status_code == 400
        assert response.json() == {"error": "Both base_sha and head_sha are required"}

        response = self._request_compare(base_sha="abc123")
        assert response.status_code == 400
        assert response.json() == {"error": "Both base_sha and head_sha are required"}

        response = self._request_compare(head_sha="def456")
        assert response.status_code == 400
        assert response.json() == {"error": "Both base_sha and head_sha are required"}

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_compare_no_data(self, mock_feature_flag):
        """Test compare endpoint when no data exists for either commit"""
        response = self._request_compare(base_sha="abc123", head_sha="def456")
        assert response.status_code == 200
        data = response.json()

        # Both commits should have zero values
        for commit_data in [data["base"], data["head"]]:
            assert commit_data["totalItems"] == 0
            assert commit_data["passedItems"] == 0
            assert commit_data["failedItems"] == 0
            assert commit_data["avgDurationSeconds"] == 0
            assert commit_data["avgCost"] == 0
            assert commit_data["scores"] == {}

        # Diffs should be zero
        assert data["diff"]["totalItems"] == 0
        assert data["diff"]["passedItems"] == 0
        assert data["diff"]["failedItems"] == 0
        assert data["diff"]["avgDurationSeconds"] == 0
        assert data["diff"]["avgCost"] == 0
        assert data["diff"]["scores"] == {}

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_compare_with_data(self, mock_feature_flag):
        """Test compare endpoint with actual test data"""
        base_time = datetime(2025, 6, 17, 9, 15, 43, 150189, tzinfo=UTC)
        head_time = base_time + timedelta(hours=1)

        # Create base commit data
        Testrun.objects.using("ta_timeseries").create(
            timestamp=base_time,
            test_id=b"id1",
            name="test1",
            classname="ClassA",
            outcome="pass",
            duration_seconds=10.0,
            repo_id=self.repo.repoid,
            commit_sha="abc123",
            properties={
                "eval": {
                    "cost": 5.0,
                    "scores": [
                        {"name": "accuracy", "score": 0.9},
                        {"name": "f1", "score": 0.8},
                    ],
                }
            },
        )

        # Create head commit data with improved scores
        Testrun.objects.using("ta_timeseries").create(
            timestamp=head_time,
            test_id=b"id1",
            name="test1",
            classname="ClassA",
            outcome="pass",
            duration_seconds=8.0,  # Faster
            repo_id=self.repo.repoid,
            commit_sha="def456",
            properties={
                "eval": {
                    "cost": 4.0,  # Cheaper
                    "scores": [
                        {"name": "accuracy", "score": 0.95},  # Better
                        {"name": "f1", "score": 0.85},  # Better
                    ],
                }
            },
        )

        response = self._request_compare(base_sha="abc123", head_sha="def456")
        assert response.status_code == 200
        data = response.json()

        # Check base data
        assert data["base"]["totalItems"] == 1
        assert data["base"]["passedItems"] == 1
        assert data["base"]["failedItems"] == 0
        assert data["base"]["avgDurationSeconds"] == 10.0
        assert data["base"]["avgCost"] == 5.0
        assert data["base"]["scores"]["accuracy"] == {"sum": 0.9, "avg": 0.9}
        assert data["base"]["scores"]["f1"] == {"sum": 0.8, "avg": 0.8}

        # Check head data
        assert data["head"]["totalItems"] == 1
        assert data["head"]["passedItems"] == 1
        assert data["head"]["failedItems"] == 0
        assert data["head"]["avgDurationSeconds"] == 8.0
        assert data["head"]["avgCost"] == 4.0
        assert data["head"]["scores"]["accuracy"] == {"sum": 0.95, "avg": 0.95}
        assert data["head"]["scores"]["f1"] == {"sum": 0.85, "avg": 0.85}

        # Check diffs
        assert data["diff"]["totalItems"] == 0  # Same number of tests
        assert data["diff"]["passedItems"] == 0  # Same number of passes
        assert data["diff"]["failedItems"] == 0  # Same number of failures
        assert data["diff"]["avgDurationSeconds"] == -20.0  # 20% faster
        assert data["diff"]["avgCost"] == -20.0  # 20% cheaper
        assert data["diff"]["scores"]["accuracy"] == {
            "sum": pytest.approx(5.555555555555),
            "avg": pytest.approx(5.555555555555),
        }  # ~5.56% better
        assert data["diff"]["scores"]["f1"] == {
            "sum": pytest.approx(6.25),
            "avg": pytest.approx(6.25),
        }  # 6.25% better

    @patch("rollouts.READ_NEW_EVALS.check_value", return_value=True)
    def test_compare_with_mixed_outcomes(self, mock_feature_flag):
        """Test compare endpoint with mixed pass/fail outcomes"""
        base_time = datetime(2025, 6, 17, 9, 15, 43, 150189, tzinfo=UTC)
        head_time = base_time + timedelta(hours=1)

        # Create base commit data with one pass and one fail
        Testrun.objects.using("ta_timeseries").create(
            timestamp=base_time,
            test_id=b"id1",
            name="test1",
            classname="ClassA",
            outcome="pass",
            duration_seconds=10.0,
            repo_id=self.repo.repoid,
            commit_sha="abc123",
            properties={
                "eval": {
                    "cost": 5.0,
                    "scores": [{"name": "accuracy", "score": 0.9}],
                }
            },
        )
        Testrun.objects.using("ta_timeseries").create(
            timestamp=base_time,
            test_id=b"id2",
            name="test2",
            classname="ClassA",
            outcome="fail",
            duration_seconds=5.0,
            repo_id=self.repo.repoid,
            commit_sha="abc123",
            properties={"eval": {"cost": 2.0}},
        )

        # Create head commit data with both passing
        Testrun.objects.using("ta_timeseries").create(
            timestamp=head_time,
            test_id=b"id1",
            name="test1",
            classname="ClassA",
            outcome="pass",
            duration_seconds=8.0,
            repo_id=self.repo.repoid,
            commit_sha="def456",
            properties={
                "eval": {
                    "cost": 4.0,
                    "scores": [{"name": "accuracy", "score": 0.95}],
                }
            },
        )
        Testrun.objects.using("ta_timeseries").create(
            timestamp=head_time,
            test_id=b"id2",
            name="test2",
            classname="ClassA",
            outcome="pass",
            duration_seconds=4.0,
            repo_id=self.repo.repoid,
            commit_sha="def456",
            properties={
                "eval": {
                    "cost": 1.5,
                    "scores": [{"name": "accuracy", "score": 0.85}],
                }
            },
        )

        response = self._request_compare(base_sha="abc123", head_sha="def456")
        assert response.status_code == 200
        data = response.json()

        # Check base data
        assert data["base"]["totalItems"] == 2
        assert data["base"]["passedItems"] == 1
        assert data["base"]["failedItems"] == 1
        assert data["base"]["avgDurationSeconds"] == 7.5  # (10 + 5) / 2
        assert data["base"]["avgCost"] == 3.5  # (5 + 2) / 2
        assert data["base"]["scores"]["accuracy"]["sum"] == pytest.approx(0.9)
        assert data["base"]["scores"]["accuracy"]["avg"] == pytest.approx(0.9)

        # Check head data
        assert data["head"]["totalItems"] == 2
        assert data["head"]["passedItems"] == 2
        assert data["head"]["failedItems"] == 0
        assert data["head"]["avgDurationSeconds"] == 6.0  # (8 + 4) / 2
        assert data["head"]["avgCost"] == 2.75  # (4 + 1.5) / 2
        assert data["head"]["scores"]["accuracy"]["sum"] == pytest.approx(1.8)
        assert data["head"]["scores"]["accuracy"]["avg"] == pytest.approx(0.9)

        # Check diffs
        assert data["diff"]["totalItems"] == 0  # Same number of tests
        assert data["diff"]["passedItems"] == 1  # One more pass
        assert data["diff"]["failedItems"] == -1  # One less failure
        assert data["diff"]["avgDurationSeconds"] == -20.0  # 20% faster
        assert data["diff"]["avgCost"] == pytest.approx(
            -21.428571428571427
        )  # ~21.43% cheaper
        assert data["diff"]["scores"]["accuracy"]["sum"] == pytest.approx(99.999999999)
        assert data["diff"]["scores"]["accuracy"]["avg"] == pytest.approx(0.0)
