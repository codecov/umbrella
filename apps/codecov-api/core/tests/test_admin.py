import uuid
from unittest.mock import MagicMock, patch

from django.contrib.admin.sites import AdminSite
from django.test import TestCase

from core.admin import RepositoryAdmin, RepositoryAdminForm
from core.models import Repository
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
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
