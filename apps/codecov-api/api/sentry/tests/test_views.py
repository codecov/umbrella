import json
from unittest.mock import patch

import jwt
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from codecov_auth.models import Account
from shared.django_apps.codecov_auth.models import GithubAppInstallation, Owner
from shared.django_apps.codecov_auth.tests.factories import AccountFactory, OwnerFactory
from shared.plan.constants import PlanName


class AccountLinkViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.url = reverse("account-link")
        self.unlink_url = reverse("account-unlink")

        # Sample valid data
        self.valid_data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

    def _make_authenticated_request(self, url=None, data=None, jwt_payload=None):
        if data is None:
            data = self.valid_data
        if url is None:
            url = self.url

        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.return_value = jwt_payload or {
                "g_p": "github",
                "g_o": "test-org",
            }
            return self.client.post(
                url, data=json.dumps(data), content_type="application/json"
            )

    def test_account_link_success_new_account(self):
        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)
        self.assertEqual(account.name, "Test Sentry Org")

        owner = Owner.objects.get(service_id="456789123", service="github")
        self.assertEqual(owner.account, account)
        self.assertEqual(owner.name, "test-org")
        self.assertEqual(owner.username, "test-org")

        installation = GithubAppInstallation.objects.get(installation_id="987654321")
        self.assertEqual(installation.owner, owner)

    def test_account_link_success_existing_account(self):
        existing_account = AccountFactory(
            sentry_org_id="123456789",
            name="Existing Sentry Org",
            plan=PlanName.SENTRY_MERGE_PLAN.value,
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.id, existing_account.id)
        self.assertEqual(account.name, "Test Sentry Org")
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)

        owner = Owner.objects.get(service_id="456789123", service="github")
        self.assertEqual(owner.account, existing_account)

    def test_account_link_success_existing_owner(self):
        existing_owner = OwnerFactory(
            service_id="456789123",
            service="github",
            name="existing-org",
            username="existing-org",
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)

        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.account_id, account.id)
        self.assertEqual(existing_owner.name, "existing-org")
        self.assertEqual(existing_owner.username, "existing-org")

    def test_account_link_success_existing_owner_with_different_account(self):
        old_account = AccountFactory(name="Old Account")
        existing_owner = OwnerFactory(
            service_id="456789123",
            service="github",
            account=old_account,
            name="existing-org",
            username="existing-org",
        )

        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "New Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        new_account = Account.objects.get(sentry_org_id="123456789")

        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.account, new_account)
        self.assertNotEqual(existing_owner.account, old_account)

    def test_account_link_success_existing_installation(self):
        existing_account = AccountFactory(sentry_org_id="123456789")
        existing_owner = OwnerFactory(
            service_id="456789123", service="github", account=existing_account
        )
        existing_installation = GithubAppInstallation.objects.create(
            installation_id="987654321",
            owner=existing_owner,
            name="existing-app",
            app_id="12345",
        )

        response = self._make_authenticated_request(
            data={
                "sentry_org_id": "123456789",
                "sentry_org_name": "Test Sentry Org",
                "organizations": [
                    {
                        "installation_id": "987654321",
                        "service_id": "456789123",
                        "slug": "test-org",
                        "provider": "github",
                    },
                ],
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        installations = GithubAppInstallation.objects.filter(installation_id=987654321)
        self.assertEqual(installations.count(), 1)

    def test_account_link_authentication_failure(self):
        response = self.client.post(
            self.url, data=json.dumps(self.valid_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_account_link_invalid_jwt(self):
        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.side_effect = PermissionError("Invalid JWT")

            response = self.client.post(
                self.url,
                data=json.dumps(self.valid_data),
                content_type="application/json",
            )

            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_account_link_expired_jwt(self):
        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.side_effect = jwt.ExpiredSignatureError("Token expired")

            response = self.client.post(
                self.url,
                data=json.dumps(self.valid_data),
                content_type="application/json",
            )

            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_account_link_missing_sentry_org_id(self):
        data = {
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("sentry_org_id", response.data)

    def test_account_link_missing_sentry_org_name(self):
        data = {
            "sentry_org_id": "123456789",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("sentry_org_name", response.data)

    def test_account_link_missing_organizations(self):
        data = {"sentry_org_id": "123456789", "sentry_org_name": "Test Sentry Org"}

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("organizations", response.data)

    def test_account_link_invalid_organization_data(self):
        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    # Missing service_id, slug, provider
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("service_id", response.data["organizations"][0])
        self.assertIn("slug", response.data["organizations"][0])
        self.assertIn("provider", response.data["organizations"][0])

    def test_account_link_invalid_json(self):
        response = self._make_authenticated_request(data="invalid json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_account_link_settings_integration(self):
        with (
            patch("django.conf.settings.GITHUB_SENTRY_APP_NAME", "test-app-name"),
            patch("django.conf.settings.GITHUB_SENTRY_APP_ID", "12345"),
        ):
            response = self._make_authenticated_request(data=self.valid_data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

            installation = GithubAppInstallation.objects.get(
                installation_id="987654321"
            )
            self.assertEqual(installation.name, "test-app-name")
            self.assertEqual(installation.app_id, 12345)

    def test_account_link_skips_non_github_organizations(self):
        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "github-org",
                    "provider": "github",
                },
                {
                    "installation_id": "987654322",
                    "service_id": "456789124",
                    "slug": "gitlab-org",
                    "provider": "gitlab",
                },
                {
                    "installation_id": "987654323",
                    "service_id": "456789125",
                    "slug": "bitbucket-org",
                    "provider": "bitbucket",
                },
            ],
        }

        with patch("api.sentry.views.log") as mock_log:
            response = self._make_authenticated_request(data=data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

            account = Account.objects.get(sentry_org_id="123456789")

            owners = Owner.objects.filter(account=account)
            self.assertEqual(owners.count(), 1)
            owner = owners.first()
            self.assertIsNotNone(owner)
            self.assertEqual(owner.service, "github")
            self.assertEqual(owner.name, "github-org")

            installations = GithubAppInstallation.objects.filter(owner__account=account)
            self.assertEqual(installations.count(), 1)
            installation = installations.first()
            self.assertIsNotNone(installation)
            self.assertEqual(installation.installation_id, 987654321)

            self.assertEqual(mock_log.warning.call_count, 2)

            warning_calls = mock_log.warning.call_args_list
            self.assertIn("gitlab-org", str(warning_calls[0]))
            self.assertIn("bitbucket-org", str(warning_calls[1]))
            self.assertIn("not a GitHub organization", str(warning_calls[0]))
            self.assertIn("not a GitHub organization", str(warning_calls[1]))

    def test_account_link_only_github_organizations(self):
        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "github-org-1",
                    "provider": "github",
                },
                {
                    "installation_id": "987654322",
                    "service_id": "456789124",
                    "slug": "github-org-2",
                    "provider": "github",
                },
                {
                    "installation_id": "987654323",
                    "service_id": "456789125",
                    "slug": "gitlab-org",
                    "provider": "gitlab",
                },
            ],
        }

        with patch("api.sentry.views.log") as mock_log:
            response = self._make_authenticated_request(data=data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

            account = Account.objects.get(sentry_org_id="123456789")

            owners = Owner.objects.filter(account=account)
            self.assertEqual(owners.count(), 2)

            owner_names = [owner.name for owner in owners]
            self.assertIn("github-org-1", owner_names)
            self.assertIn("github-org-2", owner_names)

            installations = GithubAppInstallation.objects.filter(owner__account=account)
            self.assertEqual(installations.count(), 2)

            installation_ids = [inst.installation_id for inst in installations]
            self.assertIn(987654321, installation_ids)
            self.assertIn(987654322, installation_ids)

            mock_log.warning.assert_called_once()
            warning_call = mock_log.warning.call_args[0][0]
            self.assertIn("gitlab-org", warning_call)
            self.assertIn("not a GitHub organization", warning_call)

    def test_reactivation_of_inactive_account(self):
        account = AccountFactory(
            sentry_org_id="123456789",
            name="Original Name",
            plan=PlanName.SENTRY_MERGE_PLAN.value,
            is_active=True,
        )
        OwnerFactory(
            service_id="456789123",
            service="github",
            account=account,
            name="test-org",
            username="test-org",
        )

        unlink_data = {"sentry_org_ids": ["123456789"]}
        unlink_response = self._make_authenticated_request(
            url=self.unlink_url, data=unlink_data
        )
        self.assertEqual(unlink_response.status_code, status.HTTP_200_OK)
        self.assertEqual(unlink_response.data["message"], "Unlinked 1 of 1 accounts")
        self.assertEqual(unlink_response.data["successfully_unlinked"], 1)
        self.assertEqual(unlink_response.data["total_requested"], 1)

        account.refresh_from_db()
        self.assertFalse(account.is_active)

        reactivation_data = self.valid_data.copy()
        reactivation_data["sentry_org_name"] = "Reactivated Name"

        link_response = self._make_authenticated_request(
            url=self.url, data=reactivation_data
        )
        self.assertEqual(link_response.status_code, status.HTTP_200_OK)

        reactivated_account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(reactivated_account.id, account.id)
        self.assertTrue(reactivated_account.is_active)
        self.assertEqual(reactivated_account.name, "Reactivated Name")
        self.assertEqual(str(reactivated_account.sentry_org_id), "123456789")

    def test_conflict_with_active_account(self):
        existing_account = AccountFactory(
            sentry_org_id="999999999",
            name="Existing Active Account",
            plan=PlanName.SENTRY_MERGE_PLAN.value,
            is_active=True,
        )
        OwnerFactory(
            service_id="456789123",
            service="github",
            account=existing_account,
            name="test-org",
            username="test-org",
        )

        conflict_data = self.valid_data.copy()
        conflict_data["sentry_org_id"] = "123456789"

        response = self._make_authenticated_request(url=self.url, data=conflict_data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn(
            "already linked to an active Sentry account", response.data["message"]
        )

        existing_account.refresh_from_db()
        self.assertTrue(existing_account.is_active)
        self.assertEqual(str(existing_account.sentry_org_id), "999999999")

    def test_account_link_preserves_highest_seat_count_from_multiple_owners(self):
        OwnerFactory(
            service_id="456789123",
            service="github",
            name="owner1",
            username="owner1",
            plan_user_count=5,
        )
        OwnerFactory(
            service_id="456789124",
            service="github",
            name="owner2",
            username="owner2",
            plan_user_count=15,
        )
        OwnerFactory(
            service_id="456789125",
            service="github",
            name="owner3",
            username="owner3",
            plan_user_count=8,
        )

        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "service_id": "456789123",
                    "slug": "test-org-1",
                    "provider": "github",
                },
                {
                    "installation_id": "987654322",
                    "service_id": "456789124",
                    "slug": "test-org-2",
                    "provider": "github",
                },
                {
                    "installation_id": "987654323",
                    "service_id": "456789125",
                    "slug": "test-org-3",
                    "provider": "github",
                },
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        account = Account.objects.get(sentry_org_id="123456789")

        self.assertEqual(account.plan_seat_count, 15)


class AccountUnlinkViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.url = reverse("account-unlink")

        self.valid_data = {"sentry_org_ids": ["123456789"]}

    def _make_authenticated_request(self, data, jwt_payload=None):
        if data is None:
            data = self.valid_data

        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.return_value = jwt_payload or {
                "g_p": "github",
                "g_o": "test-org",
            }
            return self.client.post(
                self.url, data=json.dumps(data), content_type="application/json"
            )

    def test_account_unlink_success(self):
        account = AccountFactory(
            sentry_org_id="123456789", name="Test Sentry Org", is_active=True
        )
        owner = OwnerFactory(
            service_id="456789123",
            service="github",
            account=account,
            name="test-org",
            username="test-org",
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "Unlinked 1 of 1 accounts")
        self.assertEqual(response.data["successfully_unlinked"], 1)
        self.assertEqual(response.data["total_requested"], 1)

        account.refresh_from_db()
        self.assertFalse(account.is_active)
        self.assertEqual(str(account.sentry_org_id), "123456789")
        self.assertEqual(account.name, "Test Sentry Org")

        owner.refresh_from_db()
        self.assertEqual(owner.account, account)

    def test_account_unlink_not_found(self):
        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "Unlinked 0 of 1 accounts")
        self.assertEqual(response.data["successfully_unlinked"], 0)
        self.assertEqual(response.data["total_requested"], 1)

    def test_account_unlink_mixed_existing_and_nonexisting(self):
        account = AccountFactory(
            sentry_org_id="123456789", name="Test Sentry Org", is_active=True
        )
        owner = OwnerFactory(
            service_id="456789123",
            service="github",
            account=account,
            name="test-org",
            username="test-org",
        )

        unlink_data = {"sentry_org_ids": ["123456789", "999999999", "888888888"]}

        with patch("api.sentry.views.log") as mock_log:
            response = self._make_authenticated_request(data=unlink_data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response.data["message"], "Unlinked 1 of 3 accounts")
            self.assertEqual(response.data["successfully_unlinked"], 1)
            self.assertEqual(response.data["total_requested"], 3)

            account.refresh_from_db()
            self.assertFalse(account.is_active)

            owner.refresh_from_db()
            self.assertEqual(owner.account, account)

            self.assertEqual(mock_log.warning.call_count, 2)
            warning_calls = mock_log.warning.call_args_list
            self.assertIn("999999999", str(warning_calls[0]))
            self.assertIn("888888888", str(warning_calls[1]))
            self.assertIn("not found", str(warning_calls[0]))
            self.assertIn("not found", str(warning_calls[1]))

    def test_account_unlink_multiple_organizations_success(self):
        account1 = AccountFactory(
            sentry_org_id="123456789", name="Test Sentry Org 1", is_active=True
        )
        account2 = AccountFactory(
            sentry_org_id="987654321", name="Test Sentry Org 2", is_active=True
        )
        account3 = AccountFactory(
            sentry_org_id="555666777", name="Test Sentry Org 3", is_active=True
        )

        unlink_data = {"sentry_org_ids": ["123456789", "987654321", "555666777"]}
        response = self._make_authenticated_request(data=unlink_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "Unlinked 3 of 3 accounts")
        self.assertEqual(response.data["successfully_unlinked"], 3)
        self.assertEqual(response.data["total_requested"], 3)

        account1.refresh_from_db()
        account2.refresh_from_db()
        account3.refresh_from_db()

        self.assertFalse(account1.is_active)
        self.assertFalse(account2.is_active)
        self.assertFalse(account3.is_active)

    def test_account_unlink_authentication_failure(self):
        response = self.client.post(
            self.url, data=json.dumps(self.valid_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
