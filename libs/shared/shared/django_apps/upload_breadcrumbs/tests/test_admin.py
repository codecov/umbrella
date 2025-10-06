import json
from unittest.mock import MagicMock, patch

from django.conf import settings
from django.contrib.admin.sites import AdminSite
from django.test import Client, TestCase, override_settings
from django.utils.safestring import SafeString

from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
from shared.django_apps.reports.tests.factories import (
    CommitReportFactory,
    UploadFactory,
)
from shared.django_apps.upload_breadcrumbs.admin import (
    EndpointFilter,
    ErrorFilter,
    MilestoneFilter,
    PresentDataFilter,
    UploadBreadcrumbAdmin,
)
from shared.django_apps.upload_breadcrumbs.models import (
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)
from shared.django_apps.upload_breadcrumbs.tests.factories import (
    UploadBreadcrumbFactory,
)


class UploadBreadcrumbAdminTest(TestCase):
    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.admin = UploadBreadcrumbAdmin(UploadBreadcrumb, AdminSite())
        self.repo = RepositoryFactory()

    def test_permissions_disabled(self):
        """Test that all permissions are disabled for upload breadcrumbs."""
        request = MagicMock()
        self.assertFalse(self.admin.has_delete_permission(request))
        self.assertFalse(self.admin.has_add_permission(request))
        self.assertFalse(self.admin.has_change_permission(request))

    @override_settings(DJANGO_ADMIN_URL="random_admin_url")
    def test_formatted_repo_id_with_existing_repo(self):
        """Test formatted_repo_id displays repo info correctly."""
        breadcrumb = UploadBreadcrumbFactory(repo_id=self.repo.repoid)

        result = self.admin.formatted_repo_id(breadcrumb)

        self.assertIsInstance(result, SafeString)
        self.assertIn(str(self.repo.repoid), result)
        self.assertIn(self.repo.author.username, result)
        self.assertIn(self.repo.name, result)
        self.assertIn(f"/{settings.DJANGO_ADMIN_URL}/core/repository/", result)

    def test_formatted_repo_id_with_nonexistent_repo(self):
        """Test formatted_repo_id handles missing related repo object gracefully."""
        breadcrumb = UploadBreadcrumbFactory(repo_id=999999)

        result = self.admin.formatted_repo_id(breadcrumb)

        self.assertEqual(result, "999999")

    def test_formatted_repo_id_with_no_repo_id(self):
        """Test formatted_repo_id handles None repo_id."""
        # Create a breadcrumb with a valid repo_id first, then set it to None to bypass validation
        breadcrumb = UploadBreadcrumbFactory(repo_id=self.repo.repoid)
        breadcrumb.repo_id = None

        result = self.admin.formatted_repo_id(breadcrumb)

        self.assertEqual(result, "-")

    def test_formatted_commit_sha(self):
        """Test commit SHA formatting with various input lengths."""
        test_cases = [
            ("abcdefghijklmnop1234567890", "abcdefg"),  # Long SHA truncated to 7
            ("abc123", "abc123"),  # Short SHA not truncated
            ("", "-"),  # Empty SHA returns dash
        ]

        for commit_sha, expected_result in test_cases:
            with self.subTest(commit_sha=commit_sha):
                breadcrumb = UploadBreadcrumbFactory(commit_sha=commit_sha)
                result = self.admin.formatted_commit_sha(breadcrumb)
                self.assertEqual(result, expected_result)

    def test_formatted_breadcrumb_data_with_all_fields(self):
        """Test breadcrumb data formatting with all fields present."""
        breadcrumb_data = {
            "milestone": Milestones.COMMIT_PROCESSED.value,
            "endpoint": Endpoints.CREATE_COMMIT.value,
            "uploader": "test_uploader",
            "error": Errors.BAD_REQUEST.value,
            "error_text": "This is a test error message that is very long and should be truncated",
        }
        breadcrumb = UploadBreadcrumbFactory(breadcrumb_data=breadcrumb_data)

        result = self.admin.formatted_breadcrumb_data(breadcrumb)

        self.assertIsInstance(result, SafeString)
        self.assertIn("📍", result)  # milestone emoji
        self.assertIn("🔗", result)  # endpoint emoji
        self.assertIn("⬆️", result)  # uploader emoji
        self.assertIn("❌", result)  # error emoji
        self.assertIn("💬", result)  # error text emoji
        self.assertIn(str(Milestones.COMMIT_PROCESSED.label), result)
        self.assertIn(str(Endpoints.CREATE_COMMIT.label), result)
        self.assertIn(str(Errors.BAD_REQUEST.label), result)
        self.assertIn("This is a test error message that is very long ...", result)

    def test_formatted_breadcrumb_data_empty(self):
        """Test breadcrumb data formatting with no data."""
        breadcrumb = UploadBreadcrumbFactory(breadcrumb_data={})

        result = self.admin.formatted_breadcrumb_data(breadcrumb)

        self.assertEqual(result, "-")

    def test_formatted_breadcrumb_data_individual_fields(self):
        """Test breadcrumb data formatting with individual fields."""
        test_cases = [
            (
                {"milestone": Milestones.COMMIT_PROCESSED.value},
                "📍",
                str(Milestones.COMMIT_PROCESSED.label),
                ["🔗", "⬆️", "❌", "💬"],
            ),
            (
                {"endpoint": Endpoints.CREATE_COMMIT.value},
                "🔗",
                str(Endpoints.CREATE_COMMIT.label),
                ["📍", "⬆️", "❌", "💬"],
            ),
            (
                {"uploader": "test_uploader"},
                "⬆️",
                "test_uploader",
                ["📍", "🔗", "❌", "💬"],
            ),
            (
                {"error": Errors.BAD_REQUEST.value},
                "❌",
                str(Errors.BAD_REQUEST.label),
                ["📍", "🔗", "⬆️", "💬"],
            ),
            (
                {"error_text": "Short error"},
                "💬",
                "Short error",
                ["📍", "🔗", "⬆️", "❌"],
            ),
        ]

        for (
            breadcrumb_data,
            expected_emoji,
            expected_content,
            not_expected_emojis,
        ) in test_cases:
            with self.subTest(breadcrumb_data=breadcrumb_data):
                breadcrumb = UploadBreadcrumbFactory(breadcrumb_data=breadcrumb_data)
                result = self.admin.formatted_breadcrumb_data(breadcrumb)

                self.assertIsInstance(result, SafeString)
                self.assertIn(expected_emoji, result)
                self.assertIn(expected_content, result)
                for emoji in not_expected_emojis:
                    self.assertNotIn(emoji, result)

    def test_formatted_breadcrumb_data_detail(self):
        """Test detailed breadcrumb data formatting."""
        breadcrumb_data = {
            "milestone": Milestones.COMMIT_PROCESSED.value,
            "endpoint": Endpoints.CREATE_COMMIT.value,
            "uploader": "test_uploader",
            "error": Errors.BAD_REQUEST.value,
            "error_text": "Test error",
        }
        breadcrumb = UploadBreadcrumbFactory(breadcrumb_data=breadcrumb_data)

        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)

        self.assertIsInstance(result, SafeString)
        self.assertIn("📍 Milestone:", result)
        self.assertIn("🔗 Endpoint:", result)
        self.assertIn("⬆️ Uploader:", result)
        self.assertIn("❌ Error:", result)
        self.assertIn("💬 Error Text:", result)
        self.assertIn("Raw JSON Data", result)
        self.assertIn(json.dumps(breadcrumb_data, indent=2), result)

    def test_formatted_breadcrumb_data_detail_empty(self):
        """Test detailed breadcrumb data formatting with no data."""
        breadcrumb = UploadBreadcrumbFactory(breadcrumb_data={})

        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)

        self.assertEqual(result, "-")

    def test_formatted_breadcrumb_data_detail_none(self):
        """Test detailed breadcrumb data formatting with None data."""
        breadcrumb = MagicMock()
        breadcrumb.breadcrumb_data = None

        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)

        self.assertEqual(result, "-")

    def test_formatted_breadcrumb_data_detail_individual_fields(self):
        """Test detailed breadcrumb data formatting with individual fields."""
        # Test milestone only
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data={"milestone": Milestones.COMMIT_PROCESSED.value}
        )
        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)
        self.assertIn("📍 Milestone:", result)
        self.assertIn(str(Milestones.COMMIT_PROCESSED.label), result)

        # Test endpoint only
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data={"endpoint": Endpoints.CREATE_COMMIT.value}
        )
        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)
        self.assertIn("🔗 Endpoint:", result)
        self.assertIn(str(Endpoints.CREATE_COMMIT.label), result)
        self.assertIn(Endpoints.CREATE_COMMIT.name, result)

        # Test uploader only
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data={"uploader": "test_uploader"}
        )
        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)
        self.assertIn("⬆️ Uploader:", result)
        self.assertIn("test_uploader", result)

        # Test error only
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data={"error": Errors.BAD_REQUEST.value}
        )
        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)
        self.assertIn("❌ Error:", result)
        self.assertIn(str(Errors.BAD_REQUEST.label), result)

        # Test error_text only
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data={"error_text": "Custom error message"}
        )
        result = self.admin.formatted_breadcrumb_data_detail(breadcrumb)
        self.assertIn("💬 Error Text:", result)
        self.assertIn("Custom error message", result)

    def test_formatted_upload_ids(self):
        """Test upload IDs formatting for list view with various inputs."""
        test_cases = [
            ([123, 456, 789], "123, 456, 789"),  # Few IDs
            ([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], "1, 2, 3, ... (+7 more)"),  # Many IDs
            (None, "-"),  # None IDs
            ([], "-"),  # Empty list
        ]

        for upload_ids, expected_result in test_cases:
            with self.subTest(upload_ids=upload_ids):
                breadcrumb = UploadBreadcrumbFactory(upload_ids=upload_ids)
                result = self.admin.formatted_upload_ids(breadcrumb)
                self.assertEqual(result, expected_result)

    def test_formatted_upload_ids_detail_empty_cases(self):
        """Test detailed upload IDs formatting with empty cases."""
        test_cases = [
            (None, "-"),
            ([], "-"),
        ]

        for upload_ids, expected_result in test_cases:
            with self.subTest(upload_ids=upload_ids):
                breadcrumb = UploadBreadcrumbFactory(upload_ids=upload_ids)
                result = self.admin.formatted_upload_ids_detail(breadcrumb)
                self.assertEqual(result, expected_result)

    def test_formatted_upload_ids_detail(self):
        """Test detailed upload IDs formatting."""
        upload_ids = [123, 456, 789]
        breadcrumb = UploadBreadcrumbFactory(upload_ids=upload_ids)

        result = self.admin.formatted_upload_ids_detail(breadcrumb)

        self.assertIsInstance(result, SafeString)
        self.assertIn("Upload IDs (3 total):", result)
        self.assertIn("• 123", result)
        self.assertIn("• 456", result)
        self.assertIn("• 789", result)

    def test_formatted_sentry_trace_id(self):
        """Test Sentry trace ID formatting with various inputs."""
        test_cases = [
            ("abcdef1234567890", "abcdef12...", True),  # Long trace with ellipsis
            ("abc123", "abc123", False),  # Short trace without ellipsis
            (None, "-", False),  # None trace returns dash
        ]

        for trace_id, expected_display, should_have_ellipsis in test_cases:
            with self.subTest(trace_id=trace_id):
                breadcrumb = UploadBreadcrumbFactory(sentry_trace_id=trace_id)
                result = self.admin.formatted_sentry_trace_id(breadcrumb)

                if trace_id is None:
                    self.assertEqual(result, "-")
                else:
                    self.assertIsInstance(result, SafeString)
                    self.assertIn(expected_display, result)
                    if should_have_ellipsis:
                        self.assertIn("...", result)
                        self.assertIn(
                            f"https://test.sentry.io/explore/traces/trace/{trace_id}",
                            result,
                        )
                        self.assertIn('target="_blank"', result)
                    else:
                        self.assertNotIn("...", result)

    def test_formatted_sentry_trace_id_detail(self):
        """Test detailed Sentry trace ID formatting."""
        test_cases = [
            ("abcdef1234567890", "formatted"),  # Valid trace ID
            (None, "-"),  # None trace returns dash
        ]

        for trace_id, expected_result in test_cases:
            with self.subTest(trace_id=trace_id):
                breadcrumb = UploadBreadcrumbFactory(sentry_trace_id=trace_id)
                result = self.admin.formatted_sentry_trace_id_detail(breadcrumb)

                if expected_result == "-":
                    self.assertEqual(result, "-")
                else:
                    self.assertIsInstance(result, SafeString)
                    self.assertIn(f"Trace ID:</strong> {trace_id}", result)
                    self.assertIn(
                        f"https://test.sentry.io/explore/traces/trace/{trace_id}",
                        result,
                    )

    def test_log_links_gcp_disabled(self):
        """Test log links generation when GCP logging is disabled."""
        commit_sha = "abcdef123"
        trace_id = "trace123"
        breadcrumb = UploadBreadcrumbFactory(
            commit_sha=commit_sha, sentry_trace_id=trace_id
        )

        with patch(
            "shared.django_apps.upload_breadcrumbs.admin.get_config"
        ) as mock_get_config:
            mock_get_config.return_value = False
            result = self.admin.log_links(breadcrumb)

        self.assertEqual(result, "-")

    def test_log_links(self):
        """Test log links generation."""
        commit_sha = "abcdef123"
        trace_id = "trace123"
        breadcrumb = UploadBreadcrumbFactory(
            commit_sha=commit_sha, sentry_trace_id=trace_id
        )

        with patch(
            "shared.django_apps.upload_breadcrumbs.admin.get_config"
        ) as mock_get_config:
            mock_get_config.return_value = True
            result = self.admin.log_links(breadcrumb)

        self.assertIsInstance(result, SafeString)
        self.assertIn("GCP Commit SHA Logs", result)
        self.assertIn("GCP Sentry Trace Logs", result)
        self.assertIn("console.cloud.google.com", result)
        self.assertIn(commit_sha, result)
        self.assertIn(trace_id, result)

    def test_log_links_empty(self):
        """Test log links with no commit SHA or trace ID."""
        breadcrumb = UploadBreadcrumbFactory(commit_sha="", sentry_trace_id=None)

        with patch(
            "shared.django_apps.upload_breadcrumbs.admin.get_config"
        ) as mock_get_config:
            mock_get_config.return_value = True
            result = self.admin.log_links(breadcrumb)

        self.assertEqual(result, "-")

    def test_log_links_only_commit_sha(self):
        """Test log links with only commit SHA."""
        commit_sha = "abcdef123"
        breadcrumb = UploadBreadcrumbFactory(
            commit_sha=commit_sha, sentry_trace_id=None
        )

        with patch(
            "shared.django_apps.upload_breadcrumbs.admin.get_config"
        ) as mock_get_config:
            mock_get_config.return_value = True
            result = self.admin.log_links(breadcrumb)

        self.assertIsInstance(result, SafeString)
        self.assertIn("GCP Commit SHA Logs", result)
        self.assertNotIn("GCP Sentry Trace Logs", result)
        self.assertIn(commit_sha, result)

    def test_changelist_view_includes_info(self):
        """Test that changelist view includes breadcrumb information."""
        request = MagicMock()

        # Mock the super().changelist_view call to capture what extra_context is passed to it
        with patch("django.contrib.admin.ModelAdmin.changelist_view") as mock_super:
            mock_super.return_value = MagicMock()

            # Call the method with None extra_context (the typical case)
            self.admin.changelist_view(request, None)

            # Verify the parent method was called
            mock_super.assert_called_once()

            # Get the extra_context that was passed to the parent method
            call_args = mock_super.call_args
            self.assertEqual(len(call_args[0]), 2)  # request and extra_context
            passed_extra_context = call_args[0][1]

            # Verify extra_context contains breadcrumb_info
            self.assertIn("breadcrumb_info", passed_extra_context)
            self.assertIsInstance(passed_extra_context["breadcrumb_info"], SafeString)
            self.assertIn(
                "Upload Breadcrumbs Information",
                passed_extra_context["breadcrumb_info"],
            )
            self.assertIn("Milestone:", passed_extra_context["breadcrumb_info"])

            # Check that milestone list is included
            for milestone in Milestones:
                self.assertIn(
                    str(milestone.label), passed_extra_context["breadcrumb_info"]
                )


class PresentDataFilterTest(TestCase):
    def setUp(self):
        self.filter = PresentDataFilter(
            None, {}, UploadBreadcrumb, UploadBreadcrumbAdmin
        )

    def test_lookups(self):
        """Test filter lookups are correct."""
        request = MagicMock()
        model_admin = MagicMock()

        lookups = self.filter.lookups(request, model_admin)

        expected = [
            ("has_milestone", "Has Milestone"),
            ("has_endpoint", "Has Endpoint"),
            ("has_uploader", "Has Uploader"),
            ("has_error", "Has Error"),
            ("has_error_text", "Has Error Text"),
            ("has_upload_ids", "Has Upload IDs"),
            ("has_sentry_trace", "Has Sentry Trace"),
        ]
        self.assertEqual(lookups, expected)

    def test_queryset_no_value(self):
        """Test queryset returns unchanged when no filter value."""
        request = MagicMock()
        queryset = UploadBreadcrumb.objects.all()

        with patch.object(self.filter, "value", return_value=None):
            result = self.filter.queryset(request, queryset)

        self.assertEqual(result, queryset)

    def test_queryset_multiple_filters(self):
        """Test multiple filters combined."""
        request = MagicMock()
        queryset = UploadBreadcrumb.objects.all()

        with patch.object(
            self.filter,
            "value",
            return_value="has_milestone,has_endpoint,has_uploader,has_error,has_error_text,has_upload_ids,has_sentry_trace",
        ):
            result = self.filter.queryset(request, queryset)

        # Check that the filter was applied
        self.assertNotEqual(result, queryset)

    def test_choices_multiselect(self):
        """Test multiselect choices functionality."""
        changelist = MagicMock()
        changelist.get_query_string.return_value = "test_query_string"

        with patch.object(self.filter, "value", return_value="has_milestone,has_error"):
            choices = list(self.filter.choices(changelist))

        # Should have "All" option plus all filter options
        self.assertEqual(len(choices), 8)  # 1 "All" + 7 filter options

        # Check that selected items have checkmarks
        milestone_choice = next(c for c in choices if "Has Milestone" in c["display"])
        error_choice = next(c for c in choices if "Has Error" in c["display"])
        endpoint_choice = next(c for c in choices if "Has Endpoint" in c["display"])

        self.assertTrue(milestone_choice["selected"])
        self.assertTrue(error_choice["selected"])
        self.assertFalse(endpoint_choice["selected"])
        self.assertIn("✓", milestone_choice["display"])
        self.assertIn("✓", error_choice["display"])
        self.assertNotIn("✓", endpoint_choice["display"])

    def test_queryset_individual_filters(self):
        """Test each filter type individually."""
        request = MagicMock()
        queryset = UploadBreadcrumb.objects.all()

        # Test each filter type individually
        filter_types = [
            "has_milestone",
            "has_endpoint",
            "has_uploader",
            "has_error",
            "has_error_text",
            "has_upload_ids",
            "has_sentry_trace",
        ]

        for filter_type in filter_types:
            with self.subTest(filter_type=filter_type):
                with patch.object(self.filter, "value", return_value=filter_type):
                    result = self.filter.queryset(request, queryset)
                    # Check that the filter was applied (queryset changed)
                    self.assertNotEqual(result, queryset)

    def test_queryset_with_unknown_filter(self):
        """Test with unknown filter type"""
        request = MagicMock()
        queryset = UploadBreadcrumb.objects.all()

        with patch.object(self.filter, "value", return_value="unknown_filter"):
            result = self.filter.queryset(request, queryset)
            self.assertEqual(result, queryset)


class MilestoneFilterTest(TestCase):
    def setUp(self):
        self.filter = MilestoneFilter(None, {}, UploadBreadcrumb, UploadBreadcrumbAdmin)

    def test_lookups(self):
        """Test milestone filter lookups."""
        request = MagicMock()
        model_admin = MagicMock()

        lookups = self.filter.lookups(request, model_admin)

        # Should have all milestone choices
        self.assertEqual(len(lookups), len(Milestones))
        for choice in Milestones:
            self.assertIn((choice.value, str(choice.label)), lookups)

    def test_queryset(self):
        """Test milestone filter queryset behavior."""
        test_cases = [
            (Milestones.COMMIT_PROCESSED.value, True),
            (None, False),
        ]

        for filter_value, should_change_queryset in test_cases:
            with self.subTest(filter_value=filter_value):
                request = MagicMock()
                queryset = UploadBreadcrumb.objects.all()

                with patch.object(self.filter, "value", return_value=filter_value):
                    result = self.filter.queryset(request, queryset)

                if should_change_queryset:
                    self.assertNotEqual(result, queryset)
                else:
                    self.assertEqual(result, queryset)


class EndpointFilterTest(TestCase):
    def setUp(self):
        self.filter = EndpointFilter(None, {}, UploadBreadcrumb, UploadBreadcrumbAdmin)

    def test_lookups(self):
        """Test endpoint filter lookups use enum names."""
        request = MagicMock()
        model_admin = MagicMock()

        lookups = self.filter.lookups(request, model_admin)

        # Should have all endpoint choices with names (not labels)
        self.assertEqual(len(lookups), len(Endpoints))
        for choice in Endpoints:
            self.assertIn((choice.value, choice.name), lookups)

    def test_queryset(self):
        """Test endpoint filter queryset behavior."""
        test_cases = [
            (Endpoints.CREATE_COMMIT.value, True),
            (None, False),
        ]

        for filter_value, should_change_queryset in test_cases:
            with self.subTest(filter_value=filter_value):
                request = MagicMock()
                queryset = UploadBreadcrumb.objects.all()

                with patch.object(self.filter, "value", return_value=filter_value):
                    result = self.filter.queryset(request, queryset)

                if should_change_queryset:
                    self.assertNotEqual(result, queryset)
                else:
                    self.assertEqual(result, queryset)


class ErrorFilterTest(TestCase):
    def setUp(self):
        self.filter = ErrorFilter(None, {}, UploadBreadcrumb, UploadBreadcrumbAdmin)

    def test_lookups(self):
        """Test error filter lookups use enum names."""
        request = MagicMock()
        model_admin = MagicMock()

        lookups = self.filter.lookups(request, model_admin)

        # Should have all error choices with names (not labels)
        self.assertEqual(len(lookups), len(Errors))
        for choice in Errors:
            self.assertIn((choice.value, choice.name), lookups)

    def test_queryset(self):
        """Test error filter queryset behavior with different values."""
        test_cases = [
            (Errors.BAD_REQUEST.value, True),
            (None, False),
        ]

        for filter_value, should_change_queryset in test_cases:
            with self.subTest(filter_value=filter_value):
                request = MagicMock()
                queryset = UploadBreadcrumb.objects.all()

                with patch.object(self.filter, "value", return_value=filter_value):
                    result = self.filter.queryset(request, queryset)

                if should_change_queryset:
                    # Check that the filter was applied
                    self.assertNotEqual(result, queryset)
                else:
                    # Should return original queryset
                    self.assertEqual(result, queryset)


class UploadBreadcrumbAdminSearchTest(TestCase):
    """Test cases for the custom search functionality in UploadBreadcrumbAdmin."""

    def setUp(self):
        self.admin = UploadBreadcrumbAdmin(UploadBreadcrumb, AdminSite())
        self.request = MagicMock()

        self.repo1 = RepositoryFactory()
        self.repo2 = RepositoryFactory()

        self.breadcrumb1 = UploadBreadcrumbFactory(
            repo_id=12345,
            commit_sha="cf36f4c203c44887afbf251974c30d26ec98f269",
            sentry_trace_id="abc123def456789012345678901234ab",
        )
        self.breadcrumb2 = UploadBreadcrumbFactory(
            repo_id=67890,
            commit_sha="abcdef1234567890abcdef1234567890abcdef12",
            sentry_trace_id="def456789012345678901234567890cd",
        )
        self.breadcrumb3 = UploadBreadcrumbFactory(
            repo_id=12345,
            commit_sha="1234567890abcdef1234567890abcdef12345678",
            sentry_trace_id=None,
        )

    def test_get_search_results_empty_search_term(self):
        """Test that empty search terms return the original queryset."""
        queryset = UploadBreadcrumb.objects.all()

        # Empty string
        result, use_distinct = self.admin.get_search_results(self.request, queryset, "")
        self.assertEqual(result, queryset)
        self.assertFalse(use_distinct)

        # Whitespace only
        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "   "
        )
        self.assertEqual(result, queryset)
        self.assertFalse(use_distinct)

    def test_get_search_results_repo_id_only(self):
        """Test searching by repo_id only."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "12345"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        expected_ids = [
            self.breadcrumb1.id,
            self.breadcrumb3.id,
        ]
        self.assertCountEqual(result_ids, expected_ids)

    def test_get_search_results_full_commit_sha(self):
        """Test searching by full commit SHA (40 characters)."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "cf36f4c203c44887afbf251974c30d26ec98f269"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb1.id])

    def test_get_search_results_sentry_trace_id(self):
        """Test searching by sentry trace ID (32 characters)."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "abc123def456789012345678901234ab"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb1.id])

    def test_get_search_results_combined_repo_and_commit(self):
        """Test searching by both repo_id and commit SHA (AND logic)."""
        queryset = UploadBreadcrumb.objects.all()

        # Search for repo_id AND specific commit SHA
        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "67890 abcdef1234567890abcdef1234567890abcdef12"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb2.id])

        # Different repo_id
        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "12345 abcdef1234567890abcdef1234567890abcdef12"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [])

    def test_get_search_results_combined_repo_and_sentry(self):
        """Test searching by repo_id and sentry trace ID."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "12345 abc123def456789012345678901234ab"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb1.id])

    def test_get_search_results_combined_commit_and_sentry(self):
        """Test searching by commit SHA and sentry trace ID."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request,
            queryset,
            "cf36f4c203c44887afbf251974c30d26ec98f269 abc123def456789012345678901234ab",
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb1.id])

    def test_get_search_results_three_fields(self):
        """Test searching by all three fields."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request,
            queryset,
            "12345 cf36f4c203c44887afbf251974c30d26ec98f269 abc123def456789012345678901234ab",
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb1.id])

    def test_get_search_results_non_matching_repo_id(self):
        """Test searching for a repo_id that doesn't exist."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "99999"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [])

    def test_get_search_results_non_matching_commit_sha(self):
        """Test searching for a commit SHA that doesn't exist."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [])

    def test_get_search_results_non_matching_sentry_trace(self):
        """Test searching for a sentry trace ID that doesn't exist."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "ffffffffffffffffffffffffffffffff"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [])

    def test_get_search_results_mixed_case_hex(self):
        """Test that hex string search requires exact case match."""
        queryset = UploadBreadcrumb.objects.all()

        # Should not match due to case sensitivity
        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "CF36F4C203C44887AFBF251974C30D26EC98F269"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [])

    def test_get_search_results_invalid_patterns(self):
        """Test that invalid patterns return no results."""
        queryset = UploadBreadcrumb.objects.all()

        invalid_patterns = [
            "12345abc",  # Mixed digit and non-digit but too short
            "abc123def456789012345678901234abc",  # 33 chars (not 32 or 40)
            "abcdef1234567890abcdef12345678901234567890123456",  # Too long
            "not-hexadecimal",
        ]

        for pattern in invalid_patterns:
            with self.subTest(pattern=pattern):
                result, use_distinct = self.admin.get_search_results(
                    self.request, queryset, pattern
                )
                # Invalid patterns should return (queryset.none(), False)
                self.assertFalse(use_distinct)
                self.assertEqual(list(result), [])

    def test_get_search_results_very_large_number(self):
        """Test searching with a very large number."""
        queryset = UploadBreadcrumb.objects.all()

        # Should still work as repo_id is BigIntegerField
        large_number = "999999999999999999999"
        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, large_number
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [])

    def test_get_search_results_multiple_spaces(self):
        """Test that multiple spaces between search terms are handled correctly."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "12345    cf36f4c203c44887afbf251974c30d26ec98f269"
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertEqual(result_ids, [self.breadcrumb1.id])

    def test_get_search_results_leading_trailing_spaces(self):
        """Test that leading and trailing spaces are handled correctly."""
        queryset = UploadBreadcrumb.objects.all()

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "  12345  "
        )

        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        expected_ids = [self.breadcrumb1.id, self.breadcrumb3.id]
        self.assertCountEqual(result_ids, expected_ids)

    def test_get_search_results_all_digits(self):
        """Test that all-digit strings are validated correctly."""
        queryset = UploadBreadcrumb.objects.all()

        test_breadcrumb = UploadBreadcrumbFactory(
            repo_id=99999,
            commit_sha="1111111111111111111111111111111111111111",
            sentry_trace_id="22222222222222222222222222222222",
        )

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "1111111111111111111111111111111111111111"
        )
        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertIn(test_breadcrumb.id, result_ids)

        result, use_distinct = self.admin.get_search_results(
            self.request, queryset, "22222222222222222222222222222222"
        )
        self.assertTrue(use_distinct)
        result_ids = list(result.values_list("id", flat=True))
        self.assertIn(test_breadcrumb.id, result_ids)


@override_settings(ROOT_URLCONF="shared.django_apps.upload_breadcrumbs.tests.test_urls")
class UploadBreadcrumbAdminResendTest(TestCase):
    """Test cases for the resend upload functionality in UploadBreadcrumbAdmin."""

    def setUp(self):
        self.user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(self.user)
        self.admin = UploadBreadcrumbAdmin(UploadBreadcrumb, AdminSite())
        self.repo = RepositoryFactory()

    def test_is_failed_upload_scenarios(self):
        """Test _is_failed_upload with various scenarios."""
        test_cases = [
            # (breadcrumb_data, expected_result, description)
            (
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                    "error": Errors.FILE_NOT_IN_STORAGE.value,
                },
                True,
                "processing upload with FILE_NOT_IN_STORAGE error",
            ),
            (
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                    "error": Errors.REPORT_EXPIRED.value,
                },
                True,
                "processing upload with REPORT_EXPIRED error",
            ),
            (
                {
                    "milestone": Milestones.COMPILING_UPLOADS.value,
                    "error": Errors.REPORT_EXPIRED.value,
                },
                True,
                "compiling uploads with REPORT_EXPIRED error",
            ),
            (
                {
                    "milestone": Milestones.COMPILING_UPLOADS.value,
                    "error": Errors.UNKNOWN.value,
                },
                True,
                "compiling uploads with UNKNOWN error",
            ),
            (
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                },
                False,
                "processing upload without error",
            ),
            (
                {
                    "milestone": Milestones.COMPILING_UPLOADS.value,
                },
                False,
                "compiling uploads without error",
            ),
            (
                {
                    "milestone": Milestones.UPLOAD_COMPLETE.value,
                    "error": Errors.BAD_REQUEST.value,
                },
                False,
                "non-processing/compiling milestone with error",
            ),
            (
                {
                    "milestone": Milestones.WAITING_FOR_COVERAGE_UPLOAD.value,
                    "error": Errors.FILE_NOT_IN_STORAGE.value,
                },
                False,
                "non-processing/compiling milestone with error",
            ),
            (None, False, "None breadcrumb_data"),
            ({}, False, "empty breadcrumb_data"),
        ]

        for breadcrumb_data, expected_result, description in test_cases:
            with self.subTest(description=description):
                if breadcrumb_data is None:
                    # Handle None case with a mock since the DB doesn't allow null breadcrumb_data
                    breadcrumb = MagicMock()
                    breadcrumb.breadcrumb_data = None
                else:
                    breadcrumb = UploadBreadcrumbFactory(
                        breadcrumb_data=breadcrumb_data
                    )
                result = self.admin._is_failed_upload(breadcrumb)
                self.assertEqual(result, expected_result, f"Failed for: {description}")

    def test_resend_upload_button_scenarios(self):
        """Test resend_upload_button with various scenarios."""
        test_cases = [
            # (task_service_available, breadcrumb_data, expected_contains, expected_type, description)
            (
                True,
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                    "error": Errors.FILE_NOT_IN_STORAGE.value,
                },
                ["🔄 Resend", 'class="button"', "onclick="],
                SafeString,
                "failed upload with TaskService available",
            ),
            (
                True,
                {
                    "milestone": Milestones.COMPILING_UPLOADS.value,
                    "error": Errors.REPORT_EXPIRED.value,
                },
                ["🔄 Resend", 'class="button"', "onclick="],
                SafeString,
                "failed compiling upload with TaskService available",
            ),
            (
                False,
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                    "error": Errors.FILE_NOT_IN_STORAGE.value,
                },
                None,
                str,
                "failed upload with TaskService unavailable",
            ),
            (
                True,
                {
                    "milestone": Milestones.UPLOAD_COMPLETE.value,
                },
                None,
                str,
                "successful upload",
            ),
            (
                False,
                {
                    "milestone": Milestones.UPLOAD_COMPLETE.value,
                },
                None,
                str,
                "successful upload with TaskService unavailable",
            ),
        ]

        for (
            task_service_available,
            breadcrumb_data,
            expected_contains,
            expected_type,
            description,
        ) in test_cases:
            with self.subTest(description=description):
                with patch(
                    "shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE",
                    task_service_available,
                ):
                    breadcrumb = UploadBreadcrumbFactory(
                        breadcrumb_data=breadcrumb_data
                    )
                    result = self.admin.resend_upload_button(breadcrumb)

                    if expected_contains is None:
                        self.assertEqual(result, "-", f"Failed for: {description}")
                    else:
                        self.assertIsInstance(
                            result, expected_type, f"Failed for: {description}"
                        )
                        for expected_content in expected_contains:
                            self.assertIn(
                                expected_content, result, f"Failed for: {description}"
                            )

    def test_resend_upload_action_scenarios(self):
        """Test resend_upload_action with various scenarios."""
        test_cases = [
            # (task_service_available, breadcrumb_data, commit_sha, has_pk, expected_contains, description)
            (
                True,
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                    "error": Errors.REPORT_EXPIRED.value,
                },
                "abcdef1234567890",
                True,
                ["🔄 Resend Upload", 'class="button default"', "abcdef1", "⚠️ Note:"],
                "failed upload with TaskService available",
            ),
            (
                True,
                {
                    "milestone": Milestones.COMPILING_UPLOADS.value,
                    "error": Errors.FILE_NOT_IN_STORAGE.value,
                },
                "1234567890abcdef",
                True,
                ["🔄 Resend Upload", 'class="button default"', "1234567", "⚠️ Note:"],
                "failed compiling upload with TaskService available",
            ),
            (
                False,
                {
                    "milestone": Milestones.PROCESSING_UPLOAD.value,
                    "error": Errors.FILE_NOT_IN_STORAGE.value,
                },
                "abcdef1234567890",
                True,
                ["⚠️ Task service not available", "Resend functionality is disabled"],
                "failed upload with TaskService unavailable",
            ),
            (
                True,
                {
                    "milestone": Milestones.UPLOAD_COMPLETE.value,
                },
                "abcdef1234567890",
                True,
                [
                    "✅ This upload does not appear to have failed",
                    "Resend option is not available",
                ],
                "successful upload",
            ),
            (
                False,
                {
                    "milestone": Milestones.UPLOAD_COMPLETE.value,
                },
                "abcdef1234567890",
                True,
                ["⚠️ Task service not available", "Resend functionality is disabled"],
                "successful upload with TaskService unavailable",
            ),
            (True, {}, "abcdef1234567890", False, None, "new object without pk"),
        ]

        for (
            task_service_available,
            breadcrumb_data,
            commit_sha,
            has_pk,
            expected_contains,
            description,
        ) in test_cases:
            with self.subTest(description=description):
                with patch(
                    "shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE",
                    task_service_available,
                ):
                    if has_pk:
                        breadcrumb = UploadBreadcrumbFactory(
                            breadcrumb_data=breadcrumb_data, commit_sha=commit_sha
                        )
                    else:
                        breadcrumb = UploadBreadcrumb()  # No pk

                    result = self.admin.resend_upload_action(breadcrumb)

                    if expected_contains is None:
                        self.assertEqual(result, "-", f"Failed for: {description}")
                    else:
                        self.assertIsInstance(
                            result, SafeString, f"Failed for: {description}"
                        )
                        for expected_content in expected_contains:
                            self.assertIn(
                                expected_content, result, f"Failed for: {description}"
                            )

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", False)
    @patch("django.contrib.messages.error")
    def test_resend_upload_view_with_task_service_unavailable(
        self, mock_messages_error
    ):
        """Test resend_upload_view handles TaskService unavailability."""
        breadcrumb = UploadBreadcrumbFactory()
        request = MagicMock()
        request.user = self.user

        with patch.object(self.admin, "get_object", return_value=breadcrumb):
            response = self.admin.resend_upload_view(request, str(breadcrumb.id))

        mock_messages_error.assert_called_once_with(
            request, "Task service not available. Resend functionality is disabled."
        )

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("django.contrib.messages.error")
    def test_resend_upload_view_with_nonexistent_breadcrumb(self, mock_messages_error):
        """Test resend_upload_view handles nonexistent breadcrumb."""
        request = MagicMock()
        request.user = self.user

        with patch.object(self.admin, "get_object", return_value=None):
            response = self.admin.resend_upload_view(request, "999")

        mock_messages_error.assert_called_once_with(
            request, "Upload breadcrumb not found."
        )

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("django.contrib.messages.error")
    def test_resend_upload_view_with_non_failed_upload(self, mock_messages_error):
        """Test resend_upload_view handles non-failed uploads."""
        breadcrumb_data = {
            "milestone": Milestones.UPLOAD_COMPLETE.value,
        }
        breadcrumb = UploadBreadcrumbFactory(breadcrumb_data=breadcrumb_data)
        request = MagicMock()
        request.user = self.user

        with patch.object(self.admin, "get_object", return_value=breadcrumb):
            response = self.admin.resend_upload_view(request, str(breadcrumb.id))

        mock_messages_error.assert_called_once_with(
            request, "This upload does not appear to have failed."
        )

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("django.contrib.messages.success")
    def test_resend_upload_view_successful_resend(self, mock_messages_success):
        """Test resend_upload_view handles successful resend."""
        breadcrumb_data = {
            "milestone": Milestones.PROCESSING_UPLOAD.value,
            "error": Errors.FILE_NOT_IN_STORAGE.value,
        }
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data=breadcrumb_data, commit_sha="abcdef1234567890"
        )
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.admin, "get_object", return_value=breadcrumb),
            patch.object(self.admin, "_resend_upload", return_value=True),
        ):
            response = self.admin.resend_upload_view(request, str(breadcrumb.id))

        mock_messages_success.assert_called_once()
        success_message = mock_messages_success.call_args[0][1]
        self.assertIn("Upload resend triggered successfully", success_message)
        self.assertIn("abcdef1", success_message)

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("django.contrib.messages.error")
    def test_resend_upload_view_failed_resend(self, mock_messages_error):
        """Test resend_upload_view handles failed resend."""
        breadcrumb_data = {
            "milestone": Milestones.PROCESSING_UPLOAD.value,
            "error": Errors.FILE_NOT_IN_STORAGE.value,
        }
        breadcrumb = UploadBreadcrumbFactory(
            breadcrumb_data=breadcrumb_data, commit_sha="abcdef1234567890"
        )
        request = MagicMock()
        request.user = self.user

        with (
            patch.object(self.admin, "get_object", return_value=breadcrumb),
            patch.object(self.admin, "_resend_upload", return_value=False),
        ):
            response = self.admin.resend_upload_view(request, str(breadcrumb.id))

        mock_messages_error.assert_called_once()
        error_message = mock_messages_error.call_args[0][1]
        self.assertIn("Failed to resend upload", error_message)
        self.assertIn("abcdef1", error_message)

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("django.contrib.messages.error")
    def test_resend_upload_view_exception_handling(self, mock_messages_error):
        """Test resend_upload_view handles exceptions gracefully."""
        request = MagicMock()
        request.user = self.user

        with patch.object(
            self.admin, "get_object", side_effect=Exception("Test error")
        ):
            response = self.admin.resend_upload_view(request, "123")

        mock_messages_error.assert_called_once()
        error_message = mock_messages_error.call_args[0][1]
        self.assertIn("Error resending upload: Test error", error_message)

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", False)
    def test_resend_upload_with_task_service_unavailable(self):
        """Test _resend_upload returns False when TaskService unavailable."""
        breadcrumb = UploadBreadcrumbFactory()

        result = self.admin._resend_upload(breadcrumb, self.user)

        self.assertFalse(result)

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("shared.django_apps.upload_breadcrumbs.admin.TaskService")
    @patch("shared.django_apps.upload_breadcrumbs.admin.get_redis_connection")
    def test_resend_upload_successful(
        self,
        mock_redis_connection,
        mock_task_service_class,
    ):
        """Test _resend_upload successfully triggers upload task.

        The method checks several things in sequence:
        1. Repository.objects.get() - verify repo exists (uses real DB)
        2. Commit.objects.get() - verify commit exists (uses real DB)
        3. Redis connection - test with ping() (mocked)
        4. ReportSession.objects.filter() - get upload data (uses real DB)
        5. Store data in Redis (mocked)
        6. Call TaskService.upload() (mocked)

        We use real database objects for Repository, Commit, and ReportSession
        to ensure the actual database queries work correctly.
        """
        # Create a real commit in the database
        commit = CommitFactory(repository=self.repo, commitid="abcdef1234567890")
        commit.save()  # Ensure it's saved

        # Create a real CommitReport and Upload (ReportSession)
        commit_report = CommitReportFactory(commit=commit)
        commit_report.save()

        upload = UploadFactory(
            report=commit_report,
            external_id=1234567890,
            storage_path="v4/raw/test-path",
            build_code="test-build",
            build_url="https://example.com/build",
            job_code="test-job",
            provider="github",
        )
        upload.save()

        # Mock Redis
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [1, True, True]
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis_connection.return_value = mock_redis

        # Mock TaskService
        mock_task_service = MagicMock()
        mock_task_service_class.return_value = mock_task_service

        breadcrumb = UploadBreadcrumbFactory(
            repo_id=self.repo.repoid,
            commit_sha="abcdef1234567890",
            upload_ids=[upload.id],  # Use real upload ID from database
            breadcrumb_data={"endpoint": Endpoints.CREATE_COMMIT.value},
        )

        result = self.admin._resend_upload(breadcrumb, self.user)
        self.assertTrue(result, "Resend should succeed")

        # Verify TaskService.upload was called with correct parameters
        mock_task_service.upload.assert_called_once()
        call_kwargs = mock_task_service.upload.call_args[1]
        self.assertEqual(call_kwargs["repoid"], self.repo.repoid)
        self.assertEqual(call_kwargs["commitid"], "abcdef1234567890")
        self.assertEqual(call_kwargs["report_type"], "coverage")
        self.assertEqual(call_kwargs["arguments"], {})
        self.assertGreaterEqual(call_kwargs["countdown"], 4)

    @patch("shared.django_apps.upload_breadcrumbs.admin.TASK_SERVICE_AVAILABLE", True)
    @patch("shared.django_apps.upload_breadcrumbs.admin.TaskService")
    @patch("shared.django_apps.upload_breadcrumbs.admin.get_redis_connection")
    @patch("shared.django_apps.upload_breadcrumbs.admin.log")
    def test_resend_upload_task_service_exception(
        self, mock_log, mock_redis_connection, mock_task_service_class
    ):
        """Test _resend_upload handles exceptions and logs them."""
        # Create real database objects
        commit = CommitFactory(repository=self.repo, commitid="abcdef1234567890")
        commit_report = CommitReportFactory(commit=commit)
        upload = UploadFactory(
            report=commit_report,
            external_id=1234567890,
            storage_path="v4/raw/test-path",
            build_code="test-build",
            build_url="https://example.com/build",
            job_code="test-job",
            provider="github",
        )

        # Mock Redis
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_pipeline = MagicMock()
        mock_pipeline.execute.return_value = [1, True, True]
        mock_redis.pipeline.return_value.__enter__.return_value = mock_pipeline
        mock_redis_connection.return_value = mock_redis

        # Mock TaskService to raise exception during upload dispatch
        mock_task_service = MagicMock()
        mock_task_service.upload.side_effect = Exception("Upload failed")
        mock_task_service_class.return_value = mock_task_service

        breadcrumb = UploadBreadcrumbFactory(
            repo_id=self.repo.repoid,
            commit_sha="abcdef1234567890",
            upload_ids=[upload.id],
        )

        result = self.admin._resend_upload(breadcrumb, self.user)

        # Should return False when exception occurs
        self.assertFalse(result)

        # Verify exception was logged with correct details
        # NOTE: TaskService.upload() exceptions are caught by inner handler at line 890
        # which uses log.error(), not log.exception(). The outer log.exception() handler
        # only catches exceptions from other parts of the method (Redis, DB queries, etc.)
        mock_log.error.assert_called_with(
            "Failed to dispatch upload task - likely Celery broker connection issue",
            extra={
                "error": "Upload failed",
                "error_type": "Exception",
                "breadcrumb_id": breadcrumb.id,
            },
            exc_info=True,
        )

    def test_get_urls_includes_resend_url(self):
        """Test that get_urls includes the resend upload URL pattern."""
        urls = self.admin.get_urls()

        # Find the resend upload URL pattern
        resend_url_pattern = None
        for url_pattern in urls:
            if (
                hasattr(url_pattern, "name")
                and url_pattern.name
                == "upload_breadcrumbs_uploadbreadcrumb_resend_upload"
            ):
                resend_url_pattern = url_pattern
                break

        self.assertIsNotNone(resend_url_pattern)
        self.assertIn("resend-upload/", str(resend_url_pattern.pattern))
