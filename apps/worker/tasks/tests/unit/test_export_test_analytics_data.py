from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from shared.django_apps.codecov_auth.models import Service
from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory
from tasks.export_test_analytics_data import (
    ExportTestAnalyticsDataTask,
    serialize_test_run,
)


@pytest.fixture
def test_owner():
    owner = OwnerFactory.create(
        name="test_owner", service=Service.GITHUB.value, username="test_owner"
    )
    return owner


@pytest.fixture
def test_repo(test_owner):
    """Create a Django Repository model using factory"""
    repo = RepositoryFactory.create(
        author=test_owner, name="test_repo", test_analytics_enabled=True
    )
    return repo


@pytest.fixture
def mock_gcs_and_archiver():
    with (
        patch("tasks.export_test_analytics_data.storage.Client") as mock_storage_client,
        patch("tasks.export_test_analytics_data._Archiver") as mock_archiver,
    ):
        mock_bucket = MagicMock()
        mock_storage_client.return_value.bucket.return_value = mock_bucket

        mock_archiver_instance = MagicMock()
        mock_archiver.return_value.__enter__.return_value = mock_archiver_instance
        mock_archiver.return_value.__exit__.return_value = False

        yield {
            "storage_client": mock_storage_client,
            "archiver": mock_archiver,
            "archiver_instance": mock_archiver_instance,
            "bucket": mock_bucket,
        }


@pytest.fixture
def sample_test_run_data():
    return [
        {
            "filename": "test_file.py",
            "timestamp": datetime(2024, 1, 15, 10, 30, 0),
            "testsuite": "TestSuite",
            "outcome": "passed",
            "duration_seconds": 1.5,
            "failure_message": None,
            "framework": "pytest",
            "commit_sha": "abc123",
            "branch": "main",
            "flags": ["unit"],
        }
    ]


@pytest.fixture
def multiple_test_runs_data():
    return [
        {
            "filename": "test_file1.py",
            "timestamp": datetime(2024, 1, 15, 10, 30, 0),
            "testsuite": "TestSuite1",
            "outcome": "passed",
            "duration_seconds": 1.5,
            "failure_message": None,
            "framework": "pytest",
            "commit_sha": "abc123",
            "branch": "main",
            "flags": ["unit"],
        },
        {
            "filename": "test_file2.py",
            "timestamp": datetime(2024, 1, 15, 10, 31, 0),
            "testsuite": "TestSuite2",
            "outcome": "failed",
            "duration_seconds": 2.0,
            "failure_message": "Assertion error",
            "framework": "pytest",
            "commit_sha": "abc123",
            "branch": "main",
            "flags": ["integration"],
        },
        {
            "filename": "test_file3.py",
            "timestamp": datetime(2024, 1, 15, 10, 32, 0),
            "testsuite": "TestSuite3",
            "outcome": "passed",
            "duration_seconds": 0.8,
            "failure_message": None,
            "framework": "pytest",
            "commit_sha": "abc123",
            "branch": "main",
            "flags": ["unit"],
        },
    ]


class TestSerializeTestRun:
    def test_serialize_test_run_all_fields(self):
        timestamp = datetime(2024, 1, 15, 10, 30, 0)
        test_run = {
            "filename": "test_file.py",
            "timestamp": timestamp,
            "testsuite": "TestSuite",
            "outcome": "passed",
            "duration_seconds": 1.5,
            "failure_message": None,
            "framework": "pytest",
            "commit_sha": "abc123",
            "branch": "main",
            "flags": ["unit"],
        }

        result = serialize_test_run(test_run)

        assert result == [
            "test_file.py",
            "2024-01-15T10:30:00",
            "TestSuite",
            "passed",
            1.5,
            None,
            "pytest",
            "abc123",
            "main",
            ["unit"],
        ]


@pytest.mark.django_db
class TestExportTestAnalyticsDataTask:
    def test_owner_does_not_exist(self, dbsession):
        integration_name = "nonexistent_owner"

        result = ExportTestAnalyticsDataTask().run_impl(
            dbsession,
            integration_name=integration_name,
            gcp_project_id="test-project",
            destination_bucket="test-bucket",
            destination_prefix="test-prefix",
        )

        assert result["successful"] is False
        assert (
            "Owner with name nonexistent_owner and service github not found"
            in result["error"]
        )

    def test_owner_has_no_repos(self, dbsession, test_owner):
        # Create a repo without test_analytics_enabled
        repo = RepositoryFactory.create(
            author=test_owner, name="test_repo", test_analytics_enabled=False
        )

        result = ExportTestAnalyticsDataTask().run_impl(
            dbsession,
            integration_name="test_owner",
            gcp_project_id="test-project",
            destination_bucket="test-bucket",
            destination_prefix="test-prefix",
        )

        assert result["successful"] is False
        assert "No repositories found for owner test_owner" in result["error"]

    @patch("tasks.export_test_analytics_data.Testrun")
    def test_successful_export_test_run_data(
        self,
        mock_testrun,
        dbsession,
        test_repo,
        mock_gcs_and_archiver,
        sample_test_run_data,
    ):
        mock_testrun.objects.filter.return_value.order_by.return_value.values.return_value = sample_test_run_data

        result = ExportTestAnalyticsDataTask().run_impl(
            dbsession,
            integration_name="test_owner",
            gcp_project_id="test-project",
            destination_bucket="test-bucket",
            destination_prefix="test-prefix",
        )

        assert result["integration_name"] == "test_owner"
        assert len(result["repositories_succeeded"]) == 1
        assert result["repositories_succeeded"][0]["name"] == "test_repo"
        assert len(result["repositories_failed"]) == 0

        archiver_instance = mock_gcs_and_archiver["archiver_instance"]
        archiver_instance.upload_json.assert_called_once()
        call_args = archiver_instance.upload_json.call_args
        assert call_args[0][0] == "test_owner/test_repo.json"
        assert "fields" in call_args[0][1]
        assert "data" in call_args[0][1]

    @patch("tasks.export_test_analytics_data.Testrun")
    @patch("tasks.export_test_analytics_data.sentry_sdk")
    def test_failed_export_test_run_data(
        self, mock_sentry, mock_testrun, dbsession, test_repo, mock_gcs_and_archiver
    ):
        mock_testrun.objects.filter.side_effect = Exception("Database error")

        result = ExportTestAnalyticsDataTask().run_impl(
            dbsession,
            integration_name="test_owner",
            gcp_project_id="test-project",
            destination_bucket="test-bucket",
            destination_prefix="test-prefix",
        )

        assert result["integration_name"] == "test_owner"
        assert len(result["repositories_succeeded"]) == 0
        assert len(result["repositories_failed"]) == 1
        assert result["repositories_failed"][0]["name"] == "test_repo"
        assert "Database error" in result["repositories_failed"][0]["error"]
        mock_sentry.capture_exception.assert_called_once()

    @patch("tasks.export_test_analytics_data.Testrun")
    @patch("tasks.export_test_analytics_data.sentry_sdk")
    def test_mixed_success_and_failure(
        self,
        mock_sentry,
        mock_testrun,
        dbsession,
        test_owner,
        mock_gcs_and_archiver,
        sample_test_run_data,
    ):
        repo1 = RepositoryFactory.create(
            author=test_owner, name="successful_repo", test_analytics_enabled=True
        )
        repo2 = RepositoryFactory.create(
            author=test_owner, name="failing_repo", test_analytics_enabled=True
        )
        repo3 = RepositoryFactory.create(
            author=test_owner,
            name="another_successful_repo",
            test_analytics_enabled=True,
        )

        # Mock test run data - make it fail for repo2 only
        def filter_side_effect(*args, **kwargs):
            repo_id = kwargs.get("repo_id")
            if repo_id == repo2.repoid:
                raise Exception("Processing error for failing_repo")
            else:
                mock_result = MagicMock()
                mock_result.order_by.return_value.values.return_value = (
                    sample_test_run_data
                )
                return mock_result

        mock_testrun.objects.filter.side_effect = filter_side_effect

        result = ExportTestAnalyticsDataTask().run_impl(
            dbsession,
            integration_name="test_owner",
            gcp_project_id="test-project",
            destination_bucket="test-bucket",
            destination_prefix="test-prefix",
        )

        assert result["integration_name"] == "test_owner"
        assert len(result["repositories_succeeded"]) == 2
        assert len(result["repositories_failed"]) == 1

        successful_names = [repo["name"] for repo in result["repositories_succeeded"]]
        assert "successful_repo" in successful_names
        assert "another_successful_repo" in successful_names

        assert result["repositories_failed"][0]["name"] == "failing_repo"
        assert "Processing error" in result["repositories_failed"][0]["error"]
        assert mock_sentry.capture_exception.call_count == 1

        archiver_instance = mock_gcs_and_archiver["archiver_instance"]
        assert archiver_instance.upload_json.call_count == 2

    @patch("tasks.export_test_analytics_data.Testrun")
    def test_export_with_multiple_test_runs(
        self,
        mock_testrun,
        dbsession,
        test_repo,
        mock_gcs_and_archiver,
        multiple_test_runs_data,
    ):
        mock_testrun.objects.filter.return_value.order_by.return_value.values.return_value = multiple_test_runs_data

        result = ExportTestAnalyticsDataTask().run_impl(
            dbsession,
            integration_name="test_owner",
            gcp_project_id="test-project",
            destination_bucket="test-bucket",
            destination_prefix="test-prefix",
        )

        assert result["integration_name"] == "test_owner"
        assert len(result["repositories_succeeded"]) == 1
        assert len(result["repositories_failed"]) == 0

        archiver_instance = mock_gcs_and_archiver["archiver_instance"]
        archiver_instance.upload_json.assert_called_once()
        call_args = archiver_instance.upload_json.call_args
        uploaded_data = call_args[0][1]

        assert len(uploaded_data["data"]) == 3
        assert uploaded_data["fields"] == [
            "filename",
            "timestamp",
            "testsuite",
            "outcome",
            "duration_seconds",
            "failure_message",
            "framework",
            "commit_sha",
            "branch",
            "flags",
        ]
