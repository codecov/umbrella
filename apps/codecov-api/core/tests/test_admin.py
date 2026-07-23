import uuid
from unittest.mock import MagicMock, patch

from django.contrib.admin.sites import AdminSite
from django.test import TestCase
from django.urls import reverse

from core.admin import (
    CommitAdmin,
    CommitNotificationInline,
    CommitNotificationStatusFilter,
    CommitReportInline,
    RepositoryAdmin,
    RepositoryAdminForm,
    get_repo_yaml,
    notification_failure_reason,
)
from core.models import Commit, CommitNotification, Repository
from shared.django_apps.codecov_auth.tests.factories import (
    GithubAppInstallationFactory,
    UserFactory,
)
from shared.django_apps.core.tests.factories import (
    CommitFactory,
    CommitNotificationFactory,
    RepositoryFactory,
)
from shared.django_apps.reports.models import ReportResults, ReportType
from shared.django_apps.reports.tests.factories import (
    CommitReportFactory,
    ReportResultsFactory,
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

    def test_integrations_display_no_installations(self):
        repo = RepositoryFactory()
        self.assertEqual(self.repo_admin.integrations(repo), "-")

    def test_integrations_display_all_repos_installation(self):
        repo = RepositoryFactory()
        GithubAppInstallationFactory(
            owner=repo.author,
            name="my-app",
            app_id=42,
            installation_id=99,
            repository_service_ids=None,
        )

        # List column: compact comma-separated summary.
        summary = self.repo_admin.integrations(repo)
        self.assertIn("my-app (installation 99)", summary)

        # Detail view: full table with app id and coverage scope.
        table = self.repo_admin.integrations_table(repo)
        self.assertIn("<table>", table)
        self.assertIn("my-app", table)
        self.assertIn("42", table)
        self.assertIn("all repos", table)

    def test_integrations_display_scoped_installation(self):
        repo = RepositoryFactory()
        # Covers this repo (by service_id).
        GithubAppInstallationFactory(
            owner=repo.author,
            name="scoped-app",
            repository_service_ids=[repo.service_id],
        )
        # Same owner, but scoped to a different repo -> excluded.
        GithubAppInstallationFactory(
            owner=repo.author,
            name="other-app",
            repository_service_ids=["some-other-service-id"],
        )

        summary = self.repo_admin.integrations(repo)
        self.assertIn("scoped-app", summary)
        self.assertNotIn("other-app", summary)

        table = self.repo_admin.integrations_table(repo)
        self.assertIn("scoped-app", table)
        self.assertIn("scoped repos", table)
        self.assertNotIn("other-app", table)


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

    def test_default_ordering_is_timestamp_descending(self):
        self.assertEqual(self.commit_admin.ordering, ("-timestamp",))

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

    def test_repository_link(self):
        commit = CommitFactory(repository=self.repo)
        result = self.commit_admin.repository_link(commit)
        expected_label = (
            f"{self.repo.author.service}:{self.repo.author.username}/{self.repo.name}"
        )
        self.assertIn(expected_label, result)
        self.assertIn(
            reverse("admin:core_repository_change", args=[self.repo.repoid]), result
        )

    def test_repository_link_without_author(self):
        commit = CommitFactory(repository=self.repo)
        commit.repository.author = None
        result = self.commit_admin.repository_link(commit)
        self.assertIn(self.repo.name, result)
        self.assertIn(
            reverse("admin:core_repository_change", args=[self.repo.repoid]), result
        )

    def test_parent_commit_link(self):
        parent = CommitFactory(repository=self.repo, commitid="a" * 40)
        commit = CommitFactory(repository=self.repo, parent_commit_id=parent.commitid)
        result = self.commit_admin.parent_commit_link(commit)
        self.assertIn(parent.commitid, result)
        self.assertIn(reverse("admin:core_commit_change", args=[parent.pk]), result)

    def test_parent_commit_link_missing_parent(self):
        commit = CommitFactory(repository=self.repo, parent_commit_id="b" * 40)
        result = self.commit_admin.parent_commit_link(commit)
        self.assertEqual(result, "b" * 40)

    def test_parent_commit_link_empty(self):
        commit = CommitFactory(repository=self.repo, parent_commit_id=None)
        result = self.commit_admin.parent_commit_link(commit)
        self.assertEqual(result, "-")

    def _get_annotated_commit(self, commit):
        request = MagicMock()
        request.user = self.user
        return self.commit_admin.get_queryset(request).get(pk=commit.pk)

    def test_report_count_display(self):
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory.create_batch(2, commit=commit)
        annotated = self._get_annotated_commit(commit)
        self.assertEqual(self.commit_admin.report_count_display(annotated), 2)

    def test_reports_status_no_reports(self):
        commit = CommitFactory(repository=self.repo)
        annotated = self._get_annotated_commit(commit)
        self.assertIsNone(self.commit_admin.reports_status(annotated))

    def test_reports_status_all_processed(self):
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)
        UploadFactory(report=report, state="processed")
        annotated = self._get_annotated_commit(commit)
        self.assertTrue(self.commit_admin.reports_status(annotated))

    def test_reports_status_pending_upload(self):
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)
        UploadFactory(report=report, state="uploaded")
        annotated = self._get_annotated_commit(commit)
        self.assertIsNone(self.commit_admin.reports_status(annotated))

    def test_reports_status_error_upload(self):
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.COVERAGE)
        UploadFactory(report=report, state="error")
        annotated = self._get_annotated_commit(commit)
        self.assertFalse(self.commit_admin.reports_status(annotated))

    def test_reports_status_error_report_results(self):
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(commit=commit, report_type=ReportType.TEST_RESULTS)
        ReportResultsFactory(
            report=report, state=ReportResults.ReportResultsStates.ERROR
        )
        annotated = self._get_annotated_commit(commit)
        self.assertFalse(self.commit_admin.reports_status(annotated))

    def test_notification_count_display(self):
        commit = CommitFactory(repository=self.repo)
        CommitNotificationFactory.create_batch(2, commit=commit)
        annotated = self._get_annotated_commit(commit)
        self.assertEqual(self.commit_admin.notification_count_display(annotated), 2)

    def test_notifications_successful_true(self):
        commit = CommitFactory(repository=self.repo)
        CommitNotificationFactory(
            commit=commit, state=CommitNotification.States.SUCCESS
        )
        annotated = self._get_annotated_commit(commit)
        self.assertTrue(self.commit_admin.notifications_successful(annotated))

    def test_notifications_successful_false(self):
        commit = CommitFactory(repository=self.repo)
        CommitNotificationFactory(commit=commit, state=CommitNotification.States.ERROR)
        annotated = self._get_annotated_commit(commit)
        self.assertFalse(self.commit_admin.notifications_successful(annotated))

    def test_notifications_successful_none_without_notifications(self):
        commit = CommitFactory(repository=self.repo)
        annotated = self._get_annotated_commit(commit)
        self.assertIsNone(self.commit_admin.notifications_successful(annotated))

    def test_notification_status_filter_all_passed(self):
        passed = CommitFactory(repository=self.repo)
        CommitNotificationFactory(
            commit=passed, state=CommitNotification.States.SUCCESS
        )
        failed = CommitFactory(repository=self.repo)
        CommitNotificationFactory(commit=failed, state=CommitNotification.States.ERROR)
        CommitFactory(repository=self.repo)

        request = MagicMock()
        request.user = self.user
        qs = self.commit_admin.get_queryset(request)
        filter_instance = CommitNotificationStatusFilter(
            request, {"notification_status": "all_passed"}, Commit, self.commit_admin
        )
        filtered = filter_instance.queryset(request, qs)
        self.assertEqual(list(filtered.values_list("pk", flat=True)), [passed.pk])

    def test_notification_status_filter_has_failure(self):
        passed = CommitFactory(repository=self.repo)
        CommitNotificationFactory(
            commit=passed, state=CommitNotification.States.SUCCESS
        )
        failed = CommitFactory(repository=self.repo)
        CommitNotificationFactory(commit=failed, state=CommitNotification.States.ERROR)
        pending = CommitFactory(repository=self.repo)
        CommitNotificationFactory(
            commit=pending, state=CommitNotification.States.PENDING
        )
        CommitFactory(repository=self.repo)

        request = MagicMock()
        request.user = self.user
        qs = self.commit_admin.get_queryset(request)
        filter_instance = CommitNotificationStatusFilter(
            request, {"notification_status": "has_failure"}, Commit, self.commit_admin
        )
        filtered = filter_instance.queryset(request, qs)
        self.assertCountEqual(
            list(filtered.values_list("pk", flat=True)), [failed.pk, pending.pk]
        )

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


class CommitAdminInlineTests(TestCase):
    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.commit_admin = CommitAdmin(Commit, AdminSite())
        self.repo = RepositoryFactory()
        self.commit = CommitFactory(repository=self.repo)

    def test_commit_admin_includes_inlines(self):
        request = MagicMock()
        request.user = self.user
        inlines = self.commit_admin.get_inline_instances(request, self.commit)
        self.assertEqual(len(inlines), 2)
        self.assertIsInstance(inlines[0], CommitReportInline)
        self.assertIsInstance(inlines[1], CommitNotificationInline)


class CommitReportInlineTests(TestCase):
    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.commit_admin = CommitAdmin(Commit, AdminSite())
        self.repo = RepositoryFactory()
        self.commit = CommitFactory(repository=self.repo)

    def test_report_inline_is_read_only(self):
        request = MagicMock()
        request.user = self.user
        inline = CommitReportInline(Commit, AdminSite())
        self.assertFalse(inline.has_add_permission(request, self.commit))
        self.assertFalse(inline.has_change_permission(request, self.commit))
        self.assertFalse(inline.has_delete_permission(request, self.commit))

    def test_commit_change_page_shows_reports(self):
        CommitReportFactory(
            commit=self.commit,
            report_type=ReportType.COVERAGE,
            code="default",
        )
        url = reverse("admin:core_commit_change", args=[self.commit.pk])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Commit reports")
        self.assertContains(response, "coverage")
        self.assertContains(response, "default")


class CommitNotificationInlineTests(TestCase):
    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.commit_admin = CommitAdmin(Commit, AdminSite())
        self.repo = RepositoryFactory()
        self.commit = CommitFactory(repository=self.repo)

    def test_notification_inline_is_read_only(self):
        request = MagicMock()
        request.user = self.user
        inline = CommitNotificationInline(Commit, AdminSite())
        self.assertFalse(inline.has_add_permission(request, self.commit))
        self.assertFalse(inline.has_change_permission(request, self.commit))
        self.assertFalse(inline.has_delete_permission(request, self.commit))

    def test_commit_change_page_shows_notifications(self):
        CommitNotificationFactory(
            commit=self.commit,
            notification_type="comment",
            decoration_type="standard",
            state="success",
        )
        url = reverse("admin:core_commit_change", args=[self.commit.pk])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Commit notifications")
        self.assertContains(response, "comment")
        self.assertContains(response, "standard")
        self.assertContains(response, "success")

    def test_failure_reason_delivery_failed(self):
        notification = CommitNotificationFactory(
            commit=self.commit,
            state=CommitNotification.States.ERROR,
            decoration_type=CommitNotification.DecorationTypes.STANDARD,
        )
        self.assertEqual(notification_failure_reason(notification), "Delivery failed")

    def test_failure_reason_from_decoration_type(self):
        notification = CommitNotificationFactory(
            commit=self.commit,
            state=CommitNotification.States.ERROR,
            decoration_type=CommitNotification.DecorationTypes.UPLOAD_LIMIT,
        )
        self.assertEqual(notification_failure_reason(notification), "Upload limit")

    def test_failure_reason_none_for_success(self):
        notification = CommitNotificationFactory(
            commit=self.commit,
            state=CommitNotification.States.SUCCESS,
        )
        self.assertIsNone(notification_failure_reason(notification))

    def test_commit_change_page_shows_notification_failure_reason(self):
        CommitNotificationFactory(
            commit=self.commit,
            notification_type="checks_project",
            decoration_type=CommitNotification.DecorationTypes.UPLOAD_LIMIT,
            state=CommitNotification.States.ERROR,
        )
        url = reverse("admin:core_commit_change", args=[self.commit.pk])
        response = self.client.get(url)
        self.assertEqual(response.status_code, 200)
        self.assertContains(response, "Failure reason")
        self.assertContains(response, "Upload limit")


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

    def test_reprocess_bundle_analysis_view_no_uploads(self):
        """Test reprocess_bundle_analysis_view handles commit with report but no uploads."""
        commit = CommitFactory(repository=self.repo)
        CommitReportFactory(commit=commit, report_type=ReportType.BUNDLE_ANALYSIS)
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

    def test_reprocess_bundle_analysis_view_skips_uploads_without_storage_path(self):
        """Test reprocess_bundle_analysis_view skips uploads without storage_path."""
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(
            commit=commit, report_type=ReportType.BUNDLE_ANALYSIS
        )
        # Upload without storage_path
        UploadFactory(report=report, storage_path=None)

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

    @patch("core.admin.Measurement")
    @patch("core.admin.dispatch_upload_task")
    @patch("core.admin.get_redis_connection")
    def test_reprocess_bundle_analysis_view_success(
        self, mock_redis, mock_dispatch, mock_measurement
    ):
        """Test reprocess_bundle_analysis_view successfully queues reprocessing."""
        commit = CommitFactory(repository=self.repo)
        report = CommitReportFactory(
            commit=commit, report_type=ReportType.BUNDLE_ANALYSIS
        )
        upload = UploadFactory(
            report=report,
            storage_path="v4/raw/test-path",
            state="processed",
            state_id=UploadState.PROCESSED.db_id,
        )

        request = MagicMock()
        request.user = self.user

        mock_redis.return_value = MagicMock()
        mock_measurement.objects.using.return_value.filter.return_value.delete.return_value = (
            0,
            {},
        )

        with (
            patch.object(self.commit_admin, "get_object", return_value=commit),
            patch.object(self.commit_admin, "message_user"),
        ):
            response = self.commit_admin.reprocess_bundle_analysis_view(
                request, str(commit.pk)
            )

        self.assertEqual(response.status_code, 302)
        mock_dispatch.assert_called_once()

        # Verify upload state was reset
        upload.refresh_from_db()
        self.assertEqual(upload.state, "started")
        self.assertEqual(upload.state_id, UploadState.UPLOADED.db_id)

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
