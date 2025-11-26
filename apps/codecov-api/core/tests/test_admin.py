import uuid
from unittest.mock import MagicMock, patch

from django.contrib.admin.sites import AdminSite
from django.test import TestCase

from core.admin import CommitAdmin, RepositoryAdmin, RepositoryAdminForm, get_repo_yaml
from core.models import Commit, Repository
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
from shared.django_apps.reports.models import ReportType
from shared.django_apps.reports.tests.factories import (
    CommitReportFactory,
    UploadFactory,
)
from shared.reports.enums import UploadState
from utils.test_utils import Client


class AdminTest(TestCase):
    def setUp(self):
        self.user = UserFactory()
        self.repo_admin = RepositoryAdmin(Repository, AdminSite)
        self.client = Client()

    def test_staff_can_access_admin(self):
        self.user.is_staff = True
        self.user.save()

        self.client.force_login(self.user)
        response = self.client.get("/admin/")
        self.assertEqual(response.status_code, 200)

    def test_non_staff_cannot_access_admin(self):
        self.client.force_login(self.user)
        response = self.client.get("/admin/")
        self.assertEqual(response.status_code, 302)

    @patch("core.admin.admin.ModelAdmin.log_change")
    def test_prev_and_new_values_in_log_entry(self, mocked_super_log_change):
        repo = RepositoryFactory(using_integration=True)
        repo.save()
        repo.using_integration = False
        form = MagicMock()
        form.changed_data = ["using_integration"]
        self.repo_admin.save_model(
            request=MagicMock, new_obj=repo, form=form, change=True
        )
        assert (
            repo.changed_fields["using_integration"]
            == "prev value: True, new value: False"
        )

        message = []
        message.append({"changed": {"fields": ["using_integration"]}})
        self.repo_admin.log_change(MagicMock, repo, message)
        mocked_super_log_change.assert_called_once()
        assert message == [
            {"changed": {"fields": ["using_integration"]}},
            {"using_integration": "prev value: True, new value: False"},
        ]


class RepositoryAdminTests(AdminTest):
    def test_webhook_secret_nullable(self):
        repo = RepositoryFactory(
            webhook_secret=str(uuid.uuid4()),
        )
        self.assertIsNotNone(repo.webhook_secret)
        data = {
            "webhook_secret": "",
            # all the required fields have to be filled out in the form even though they aren't changed
            "name": repo.name,
            "author": repo.author,
            "service_id": repo.service_id,
            "upload_token": repo.upload_token,
            "image_token": repo.image_token,
            "branch": repo.branch,
        }

        form = RepositoryAdminForm(data=data, instance=repo)
        self.assertTrue(form.is_valid())
        updated_instance = form.save()
        self.assertIsNone(updated_instance.webhook_secret)

        repo.refresh_from_db()
        self.assertIsNone(repo.webhook_secret)

    def test_get_search_results_with_integer_search_term(self):
        """Test searching by repoid when search term is a valid integer."""
        # Create repositories with known repoids
        repo1 = RepositoryFactory()
        repo2 = RepositoryFactory()
        repo3 = RepositoryFactory()

        request = MagicMock()
        queryset = Repository.objects.all()
        search_term = str(repo2.repoid)  # Search by repoid

        result_queryset, may_have_duplicates = self.repo_admin.get_search_results(
            request, queryset, search_term
        )

        # Should find the repository with the matching repoid
        self.assertIn(repo2, result_queryset)
        # Should also include results from parent search (by author username)
        self.assertGreaterEqual(len(result_queryset), 1)

    def test_get_search_results_with_non_integer_search_term(self):
        """Test that non-integer search terms don't trigger repoid search."""
        repo1 = RepositoryFactory()
        repo2 = RepositoryFactory()

        request = MagicMock()
        queryset = Repository.objects.all()
        search_term = "not_a_number"

        result_queryset, may_have_duplicates = self.repo_admin.get_search_results(
            request, queryset, search_term
        )

        # Should not find any repositories by repoid since search term is not an integer
        # The result should only include what the parent search returns (by author username)
        self.assertEqual(len(result_queryset), 0)


class CommitAdminTests(TestCase):
    """Tests for CommitAdmin reprocessing functionality."""

    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.commit_admin = CommitAdmin(Commit, AdminSite())
        self.repo = RepositoryFactory()

    def test_has_no_add_permission(self):
        """Test that add permission is disabled."""
        request = MagicMock()
        self.assertFalse(self.commit_admin.has_add_permission(request))

    def test_has_no_delete_permission(self):
        """Test that delete permission is disabled."""
        request = MagicMock()
        self.assertFalse(self.commit_admin.has_delete_permission(request))

    def test_short_commitid(self):
        """Test that short_commitid returns first 7 characters."""
        commit = CommitFactory(repository=self.repo, commitid="abcdef1234567890")
        result = self.commit_admin.short_commitid(commit)
        self.assertEqual(result, "abcdef1")

    def test_short_commitid_empty(self):
        """Test that short_commitid handles empty commitid."""
        commit = MagicMock()
        commit.commitid = ""
        result = self.commit_admin.short_commitid(commit)
        self.assertEqual(result, "")

    def test_get_urls_includes_custom_urls(self):
        """Test that get_urls includes all reprocessing URL patterns."""
        urls = self.commit_admin.get_urls()
        url_names = [url.name for url in urls if hasattr(url, "name")]

        self.assertIn("core_commit_reprocess_coverage", url_names)
        self.assertIn("core_commit_reprocess_test_analytics", url_names)
        self.assertIn("core_commit_reprocess_bundle_analysis", url_names)
        self.assertIn("core_commit_trigger_notifications", url_names)

    def test_reprocess_actions_no_pk(self):
        """Test reprocess_actions returns empty string for unsaved commit."""
        commit = MagicMock()
        commit.pk = None
        result = self.commit_admin.reprocess_actions(commit)
        self.assertEqual(result, "")

    def test_reprocess_actions_with_coverage(self):
        """Test reprocess_actions shows coverage button when coverage exists."""
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)

        result = self.commit_admin.reprocess_actions(commit)

        self.assertIn("Reprocess Coverage", result)
        self.assertIn("Trigger Notifications", result)

    def test_reprocess_actions_with_test_analytics(self):
        """Test reprocess_actions shows test analytics button."""
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory(commit=commit, report_type=ReportType.TEST_RESULTS)

        result = self.commit_admin.reprocess_actions(commit)

        self.assertIn("Reprocess Test Analytics", result)
        self.assertIn("Trigger Notifications", result)

    def test_reprocess_actions_with_bundle_analysis(self):
        """Test reprocess_actions shows bundle analysis button."""
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory(commit=commit, report_type=ReportType.BUNDLE_ANALYSIS)

        result = self.commit_admin.reprocess_actions(commit)

        self.assertIn("Reprocess Bundle Analysis", result)
        self.assertIn("Trigger Notifications", result)

    def test_reprocess_actions_always_shows_notifications(self):
        """Test reprocess_actions always shows notifications trigger."""
        commit = CommitFactory(repository=self.repo)

        result = self.commit_admin.reprocess_actions(commit)

        self.assertIn("Trigger Notifications", result)


class CommitAdminReprocessViewsTests(TestCase):
    """Tests for CommitAdmin reprocessing view methods."""

    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.commit_admin = CommitAdmin(Commit, AdminSite())
        self.repo = RepositoryFactory()

    def test_reprocess_coverage_view_commit_not_found(self):
        """Test reprocess_coverage_view handles missing commit."""
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=None),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_coverage_view(request, "999")

        self.assertEqual(response.status_code, 302)

    def test_reprocess_coverage_view_no_coverage_data(self):
        """Test reprocess_coverage_view handles commit without coverage."""
        commit = CommitFactory(repository=self.repo)
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_coverage_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)

    def test_reprocess_coverage_view_no_uploads(self):
        """Test reprocess_coverage_view handles commit with report but no uploads."""
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_coverage_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)

    @patch("core.admin.dispatch_upload_task")
    @patch("core.admin.get_redis_connection")
    def test_reprocess_coverage_view_success(self, mock_redis, mock_dispatch):
        """Test reprocess_coverage_view successfully queues reprocessing."""
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)
        upload = UploadFactory(
            report=report,
            storage_path="v4/raw/test-path",
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )

        request = MagicMock()
        request.user = self.user

        mock_redis.return_value = MagicMock()

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_coverage_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)
        mock_dispatch.assert_called_once()

        # Verify upload state was reset
        upload.refresh_from_db()
        self.assertEqual(upload.state, "started")
        self.assertEqual(upload.state_id, UploadState.UPLOADED.db_id)

    def test_reprocess_coverage_view_skips_uploads_without_storage_path(self):
        """Test reprocess_coverage_view skips uploads without storage_path."""
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)
        # Upload without storage_path
        UploadFactory(report=report, storage_path=None)

        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_coverage_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)

    def test_reprocess_test_analytics_view_commit_not_found(self):
        """Test reprocess_test_analytics_view handles missing commit."""
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=None),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_test_analytics_view(request, "999")

        self.assertEqual(response.status_code, 302)

    def test_reprocess_test_analytics_view_no_test_data(self):
        """Test reprocess_test_analytics_view handles commit without test data."""
        commit = CommitFactory(repository=self.repo)
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_test_analytics_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)

    @patch("core.admin.Testrun")
    @patch("core.admin.dispatch_upload_task")
    @patch("core.admin.get_redis_connection")
    def test_reprocess_test_analytics_view_success(
        self, mock_redis, mock_dispatch, mock_testrun
    ):
        """Test reprocess_test_analytics_view successfully queues reprocessing."""
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.TEST_RESULTS)
        upload = UploadFactory(
            report=report,
            storage_path="v4/raw/test-path",
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )

        request = MagicMock()
        request.user = self.user

        mock_redis.return_value = MagicMock()
        mock_testrun.objects.using.return_value.filter.return_value.delete.return_value = (
            0,
            {},
        )

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_test_analytics_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)
        mock_dispatch.assert_called_once()

        # Verify upload state was reset
        upload.refresh_from_db()
        self.assertEqual(upload.state, "started")

    def test_reprocess_bundle_analysis_view_commit_not_found(self):
        """Test reprocess_bundle_analysis_view handles missing commit."""
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=None),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_bundle_analysis_view(request, "999")

        self.assertEqual(response.status_code, 302)

    def test_reprocess_bundle_analysis_view_no_bundle_data(self):
        """Test reprocess_bundle_analysis_view handles commit without bundle data."""
        commit = CommitFactory(repository=self.repo)
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_bundle_analysis_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)

    @patch("core.admin.TaskService")
    @patch("core.admin.get_repo_yaml")
    def test_reprocess_bundle_analysis_view_success(
        self, mock_get_yaml, mock_task_service
    ):
        """Test reprocess_bundle_analysis_view successfully queues reprocessing."""
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory(commit=commit, report_type=ReportType.BUNDLE_ANALYSIS)

        request = MagicMock()
        request.user = self.user

        mock_yaml = MagicMock()
        mock_yaml.to_dict.return_value = {"coverage": {"status": "patch"}}
        mock_get_yaml.return_value = mock_yaml

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_bundle_analysis_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)
        mock_task_service.return_value.schedule_task.assert_called_once()

    def test_trigger_notifications_view_commit_not_found(self):
        """Test trigger_notifications_view handles missing commit."""
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.commit_admin, "get_object", return_value=None),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.trigger_notifications_view(request, "999")

        self.assertEqual(response.status_code, 302)

    @patch("core.admin.TaskService")
    @patch("core.admin.get_repo_yaml")
    def test_trigger_notifications_view_success(self, mock_get_yaml, mock_task_service):
        """Test trigger_notifications_view successfully queues notification."""
        commit = CommitFactory(repository=self.repo)

        request = MagicMock()
        request.user = self.user

        mock_yaml = MagicMock()
        mock_yaml.to_dict.return_value = {"coverage": {"status": "patch"}}
        mock_get_yaml.return_value = mock_yaml

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.trigger_notifications_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)
        mock_task_service.return_value.schedule_task.assert_called_once()


class GetRepoYamlTests(TestCase):
    """Tests for get_repo_yaml helper function."""

    def test_get_repo_yaml_returns_user_yaml(self):
        """Test that get_repo_yaml returns a UserYaml object."""
        repo = RepositoryFactory()

        result = get_repo_yaml(repo)

        # Should return a UserYaml or similar object
        self.assertIsNotNone(result)
