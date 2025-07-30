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


class AccountLinkViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.url = reverse("account-link")

        # Sample valid data
        self.valid_data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "external_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

    def _make_authenticated_request(self, data, jwt_payload=None):
        """Helper method to make an authenticated request with JWT payload"""
        if data is None:
            data = self.valid_data

        # Mock the JWT authentication by setting the payload on the request
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

    def test_account_link_success_new_account(self):
        """Test successful account linking with new account creation"""
        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify Account was created
        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.name, "Test Sentry Org")

        # Verify Owner was created
        owner = Owner.objects.get(service_id="456789123", service="github")
        self.assertEqual(owner.account, account)
        self.assertEqual(owner.name, "test-org")
        self.assertEqual(owner.username, "test-org")

        # Verify GithubAppInstallation was created
        installation = GithubAppInstallation.objects.get(installation_id="987654321")
        self.assertEqual(installation.owner, owner)

    def test_account_link_success_existing_account(self):
        """Test successful account linking with existing account"""
        # Create existing account
        existing_account = AccountFactory(
            sentry_org_id="123456789", name="Existing Sentry Org"
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify existing account was used
        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.id, existing_account.id)
        self.assertEqual(account.name, "Existing Sentry Org")  # Name should not change

        # Verify Owner was created and linked to existing account
        owner = Owner.objects.get(service_id="456789123", service="github")
        self.assertEqual(owner.account, existing_account)

    def test_account_link_success_existing_owner(self):
        """Test successful account linking with existing owner"""
        # Create existing owner
        existing_owner = OwnerFactory(
            service_id="456789123",
            service="github",
            name="existing-org",
            username="existing-org",
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify account was created
        account = Account.objects.get(sentry_org_id="123456789")

        # Verify existing owner was updated to link to new account
        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.account_id, account.id)
        self.assertEqual(existing_owner.name, "existing-org")
        self.assertEqual(existing_owner.username, "existing-org")

    def test_account_link_success_existing_owner_with_different_account(self):
        """Test successful account linking when owner has different account"""
        # Create existing account and owner
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
                    "external_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify new account was created
        new_account = Account.objects.get(sentry_org_id="123456789")

        # Verify existing owner was updated to link to new account
        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.account, new_account)
        self.assertNotEqual(existing_owner.account, old_account)

    def test_account_link_success_existing_installation(self):
        """Test successful account linking with existing installation"""
        # Create existing installation
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
                        "external_id": "456789123",
                        "slug": "test-org",
                        "provider": "github",
                    },
                ],
            }
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify installation was not duplicated
        installations = GithubAppInstallation.objects.filter(installation_id=987654321)
        self.assertEqual(installations.count(), 1)

    def test_account_link_authentication_failure(self):
        """Test account linking fails without proper authentication"""
        response = self.client.post(
            self.url, data=json.dumps(self.valid_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_account_link_invalid_jwt(self):
        """Test account linking fails with invalid JWT"""
        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.side_effect = PermissionError("Invalid JWT")

            response = self.client.post(
                self.url,
                data=json.dumps(self.valid_data),
                content_type="application/json",
            )

            self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_account_link_expired_jwt(self):
        """Test account linking fails with expired JWT"""
        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.side_effect = jwt.ExpiredSignatureError("Token expired")

            response = self.client.post(
                self.url,
                data=json.dumps(self.valid_data),
                content_type="application/json",
            )

            self.assertEqual(response.status_code, status.HTTP_401_UNAUTHORIZED)

    def test_account_link_missing_sentry_org_id(self):
        """Test account linking fails with missing sentry_org_id"""
        data = {
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "external_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("sentry_org_id", response.data)

    def test_account_link_missing_sentry_org_name(self):
        """Test account linking fails with missing sentry_org_name"""
        data = {
            "sentry_org_id": "123456789",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "external_id": "456789123",
                    "slug": "test-org",
                    "provider": "github",
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("sentry_org_name", response.data)

    def test_account_link_missing_organizations(self):
        """Test account linking fails with missing organizations"""
        data = {"sentry_org_id": "123456789", "sentry_org_name": "Test Sentry Org"}

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("organizations", response.data)

    def test_account_link_invalid_organization_data(self):
        """Test account linking fails with invalid organization data"""
        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    # Missing external_id, slug, provider
                }
            ],
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("external_id", response.data["organizations"][0])
        self.assertIn("slug", response.data["organizations"][0])
        self.assertIn("provider", response.data["organizations"][0])

    def test_account_link_invalid_json(self):
        """Test account linking fails with invalid JSON"""
        response = self._make_authenticated_request(data="invalid json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_account_link_settings_integration(self):
        """Test that the endpoint uses the correct settings for GithubAppInstallation"""
        with (
            patch("django.conf.settings.GITHUB_SENTRY_APP_NAME", "test-app-name"),
            patch("django.conf.settings.GITHUB_SENTRY_APP_ID", "12345"),
        ):
            response = self._make_authenticated_request(data=self.valid_data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

            # Verify installation was created with correct settings
            installation = GithubAppInstallation.objects.get(
                installation_id="987654321"
            )
            self.assertEqual(installation.name, "test-app-name")
            self.assertEqual(installation.app_id, 12345)

    def test_account_link_skips_non_github_organizations(self):
        """Test that non-GitHub organizations are skipped and logged"""
        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "external_id": "456789123",
                    "slug": "github-org",
                    "provider": "github",
                },
                {
                    "installation_id": "987654322",
                    "external_id": "456789124",
                    "slug": "gitlab-org",
                    "provider": "gitlab",
                },
                {
                    "installation_id": "987654323",
                    "external_id": "456789125",
                    "slug": "bitbucket-org",
                    "provider": "bitbucket",
                },
            ],
        }

        with patch("api.sentry.views.log") as mock_log:
            response = self._make_authenticated_request(data=data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

            # Verify only GitHub organization was processed
            account = Account.objects.get(sentry_org_id="123456789")

            # Should only have one owner (GitHub)
            owners = Owner.objects.filter(account=account)
            self.assertEqual(owners.count(), 1)
            self.assertEqual(owners.first().service, "github")
            self.assertEqual(owners.first().name, "github-org")

            # Should only have one installation (GitHub)
            installations = GithubAppInstallation.objects.filter(owner__account=account)
            self.assertEqual(installations.count(), 1)
            self.assertEqual(installations.first().installation_id, 987654321)

            # Verify warning logs were called for non-GitHub orgs
            self.assertEqual(mock_log.warning.call_count, 2)

            # Check the warning messages
            warning_calls = mock_log.warning.call_args_list
            self.assertIn("gitlab-org", str(warning_calls[0]))
            self.assertIn("bitbucket-org", str(warning_calls[1]))
            self.assertIn("not a GitHub organization", str(warning_calls[0]))
            self.assertIn("not a GitHub organization", str(warning_calls[1]))

    def test_account_link_only_github_organizations(self):
        """Test that only GitHub organizations are processed when mixed with others"""
        data = {
            "sentry_org_id": "123456789",
            "sentry_org_name": "Test Sentry Org",
            "organizations": [
                {
                    "installation_id": "987654321",
                    "external_id": "456789123",
                    "slug": "github-org-1",
                    "provider": "github",
                },
                {
                    "installation_id": "987654322",
                    "external_id": "456789124",
                    "slug": "github-org-2",
                    "provider": "github",
                },
                {
                    "installation_id": "987654323",
                    "external_id": "456789125",
                    "slug": "gitlab-org",
                    "provider": "gitlab",
                },
            ],
        }

        with patch("api.sentry.views.log") as mock_log:
            response = self._make_authenticated_request(data=data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)

            # Verify account was created
            account = Account.objects.get(sentry_org_id="123456789")

            # Should have two owners (both GitHub)
            owners = Owner.objects.filter(account=account)
            self.assertEqual(owners.count(), 2)

            owner_names = [owner.name for owner in owners]
            self.assertIn("github-org-1", owner_names)
            self.assertIn("github-org-2", owner_names)

            # Should have two installations (both GitHub)
            installations = GithubAppInstallation.objects.filter(owner__account=account)
            self.assertEqual(installations.count(), 2)

            installation_ids = [inst.installation_id for inst in installations]
            self.assertIn(987654321, installation_ids)
            self.assertIn(987654322, installation_ids)

            # Verify warning log was called for non-GitHub org
            mock_log.warning.assert_called_once()
            warning_call = mock_log.warning.call_args[0][0]
            self.assertIn("gitlab-org", warning_call)
            self.assertIn("not a GitHub organization", warning_call)
