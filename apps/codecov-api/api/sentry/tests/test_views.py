import json
from unittest.mock import patch

import jwt
from django.test import TestCase
from django.urls import reverse
from rest_framework import status
from rest_framework.test import APIClient

from codecov_auth.models import Account
from shared.django_apps.codecov_auth.models import (
    GithubAppInstallation,
    Owner,
)
from shared.django_apps.codecov_auth.tests.factories import (
    AccountFactory,
    OwnerFactory,
    PlanFactory,
    TierFactory,
)
from shared.plan.constants import PlanName, TierName


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
        """Helper method to make an authenticated request with JWT payload"""
        if data is None:
            data = self.valid_data
        if url is None:
            url = self.url

        # Mock the JWT authentication by setting the payload on the request
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
        """Test successful account linking with new account creation"""
        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify Account was created
        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)
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
            sentry_org_id="123456789",
            name="Existing Sentry Org",
            plan=PlanName.SENTRY_MERGE_PLAN.value,
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify existing account was used and updated
        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.id, existing_account.id)
        self.assertEqual(
            account.name, "Test Sentry Org"
        )  # Name should be updated from request
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)

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
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)

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
                    "service_id": "456789123",
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
                        "service_id": "456789123",
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

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

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

            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

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

            self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_account_link_missing_sentry_org_id(self):
        """Test account linking fails with missing sentry_org_id"""
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
        """Test account linking fails with missing sentry_org_name"""
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

            # Verify only GitHub organization was processed
            account = Account.objects.get(sentry_org_id="123456789")

            # Should only have one owner (GitHub)
            owners = Owner.objects.filter(account=account)
            self.assertEqual(owners.count(), 1)
            owner = owners.first()
            self.assertIsNotNone(owner)
            self.assertEqual(owner.service, "github")
            self.assertEqual(owner.name, "github-org")

            # Should only have one installation (GitHub)
            installations = GithubAppInstallation.objects.filter(owner__account=account)
            self.assertEqual(installations.count(), 1)
            installation = installations.first()
            self.assertIsNotNone(installation)
            self.assertEqual(installation.installation_id, 987654321)

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

    def test_account_link_blocks_pro_monthly_plan(self):
        """Test that account linking is blocked for Pro Monthly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.CODECOV_PRO_MONTHLY.value, TierName.PRO.value, True
        )

    def test_account_link_blocks_pro_yearly_plan(self):
        """Test that account linking is blocked for Pro Yearly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.CODECOV_PRO_YEARLY.value, TierName.PRO.value, True
        )

    def test_account_link_blocks_team_monthly_plan(self):
        """Test that account linking is blocked for Team Monthly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.TEAM_MONTHLY.value, TierName.TEAM.value, True
        )

    def test_account_link_blocks_team_yearly_plan(self):
        """Test that account linking is blocked for Team Yearly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.TEAM_YEARLY.value, TierName.TEAM.value, True
        )

    def test_account_link_blocks_enterprise_monthly_plan(self):
        """Test that account linking is blocked for Enterprise Monthly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.ENTERPRISE_CLOUD_MONTHLY.value, TierName.ENTERPRISE.value, True
        )

    def test_account_link_blocks_enterprise_yearly_plan(self):
        """Test that account linking is blocked for Enterprise Yearly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.ENTERPRISE_CLOUD_YEARLY.value, TierName.ENTERPRISE.value, True
        )

    def test_account_link_blocks_sentry_monthly_plan(self):
        """Test that account linking is blocked for Sentry Monthly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.SENTRY_MONTHLY.value, TierName.SENTRY.value, True
        )

    def test_account_link_blocks_sentry_yearly_plan(self):
        """Test that account linking is blocked for Sentry Yearly plan"""
        self._test_account_link_blocks_paid_plan(
            PlanName.SENTRY_YEARLY.value, TierName.SENTRY.value, True
        )

    def _test_account_link_blocks_paid_plan(self, plan_name, tier_name, paid_plan):
        """Helper method to test that account linking is blocked for paid plans"""
        # Create plan and tier
        tier = TierFactory(tier_name=tier_name)
        plan = PlanFactory(
            name=plan_name,
            tier=tier,
            paid_plan=paid_plan,
            marketing_name=f"{tier_name.title()} Plan",
        )

        # Derive org name from plan name
        org_name = f"{plan_name.replace('users-', '').replace('-', '_')}_org"

        # Create owner with the plan
        existing_owner = OwnerFactory(
            service_id="456789123",
            service="github",
            plan=plan_name,
            name=org_name,
            username=org_name,
        )

        # Update test data to use the correct org name
        test_data = self.valid_data.copy()
        test_data["organizations"][0]["slug"] = org_name

        response = self._make_authenticated_request(data=test_data)

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("already has an active paid plan", response.data["message"])
        self.assertIn(org_name, response.data["message"])
        self.assertIn(plan_name, response.data["message"])

        # Verify no account was created
        self.assertFalse(Account.objects.filter(sentry_org_id="123456789").exists())

        # Verify owner plan was not changed
        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.plan, plan_name)

    def test_account_link_allows_developer_plan(self):
        """Test that account linking is allowed for Developer plan"""
        self._test_account_link_allows_compatible_plan(
            PlanName.USERS_DEVELOPER.value, TierName.BASIC.value, False
        )

    def test_account_link_allows_free_plan(self):
        """Test that account linking is allowed for Free plan"""
        self._test_account_link_allows_compatible_plan(
            PlanName.FREE_PLAN_NAME.value, TierName.BASIC.value, False
        )

    def _test_account_link_allows_compatible_plan(
        self, plan_name, tier_name, paid_plan
    ):
        """Helper method to test that account linking is allowed for compatible plans"""
        # Create plan and tier
        tier = TierFactory(tier_name=tier_name)
        plan = PlanFactory(
            name=plan_name,
            tier=tier,
            paid_plan=paid_plan,
            marketing_name=f"{tier_name.title()} Plan",
        )

        # Derive org name from plan name
        org_name = f"{plan_name.replace('users-', '').replace('-', '_')}_org"

        # Create owner with the plan
        existing_owner = OwnerFactory(
            service_id="456789123",
            service="github",
            plan=plan_name,
            name=org_name,
            username=org_name,
        )

        # Update test data to use the correct org name
        test_data = self.valid_data.copy()
        test_data["organizations"][0]["slug"] = org_name

        response = self._make_authenticated_request(data=test_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify account was created
        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)

        # Verify owner was linked to new account
        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.account, account)

    def test_account_link_allows_plan_doesnt_exist(self):
        """If the owner doesn't have a Plan object, account linking should succeed"""

        # Create owner with the plan
        existing_owner = OwnerFactory(
            service_id="456789123",
            service="github",
            plan=PlanName.SENTRY_YEARLY.value,
            name="test-org",
            username="test-org",
        )

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # Verify account was created
        account = Account.objects.get(sentry_org_id="123456789")
        self.assertEqual(account.plan, PlanName.SENTRY_MERGE_PLAN.value)

        # Verify owner was linked to new account
        existing_owner.refresh_from_db()
        self.assertEqual(existing_owner.account, account)


class AccountUnlinkViewTests(TestCase):
    def setUp(self):
        self.client = APIClient()
        self.url = reverse("account-unlink")

        # Sample valid data for unlinking
        self.valid_data = {"sentry_org_ids": ["123456789"]}

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

    def test_account_unlink_success(self):
        """Test successful account unlinking"""
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
        self.assertEqual(str(account.sentry_org_id), "123456789")  # Should be preserved
        self.assertEqual(account.name, "Test Sentry Org")

        # Verify owner relationship is still intact
        owner.refresh_from_db()
        self.assertEqual(owner.account, account)

    def test_account_unlink_not_found(self):
        """Test unlinking when account doesn't exist (should succeed with warning log)"""
        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "Unlinked 0 of 1 accounts")
        self.assertEqual(response.data["successfully_unlinked"], 0)
        self.assertEqual(response.data["total_requested"], 1)

    def test_account_unlink_mixed_existing_and_nonexisting(self):
        """Test unlinking mix of existing and non-existing organizations"""
        # Create only one account
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

        # Try to unlink existing and non-existing organizations
        unlink_data = {"sentry_org_ids": ["123456789", "999999999", "888888888"]}

        with patch("api.sentry.views.log") as mock_log:
            response = self._make_authenticated_request(data=unlink_data)

            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response.data["message"], "Unlinked 1 of 3 accounts")
            self.assertEqual(response.data["successfully_unlinked"], 1)
            self.assertEqual(response.data["total_requested"], 3)

            # Verify existing account is now inactive
            account.refresh_from_db()
            self.assertFalse(account.is_active)

            # Verify owner relationship is still intact
            owner.refresh_from_db()
            self.assertEqual(owner.account, account)

            # Verify warning logs were called for non-existing accounts
            self.assertEqual(mock_log.warning.call_count, 2)
            warning_calls = mock_log.warning.call_args_list
            self.assertIn("999999999", str(warning_calls[0]))
            self.assertIn("888888888", str(warning_calls[1]))
            self.assertIn("not found", str(warning_calls[0]))
            self.assertIn("not found", str(warning_calls[1]))

    def test_account_unlink_multiple_organizations_success(self):
        """Test successful unlinking of multiple organizations"""
        # Create multiple accounts
        account1 = AccountFactory(
            sentry_org_id="123456789", name="Test Sentry Org 1", is_active=True
        )
        account2 = AccountFactory(
            sentry_org_id="987654321", name="Test Sentry Org 2", is_active=True
        )
        account3 = AccountFactory(
            sentry_org_id="555666777", name="Test Sentry Org 3", is_active=True
        )

        # Unlink multiple organizations
        unlink_data = {"sentry_org_ids": ["123456789", "987654321", "555666777"]}
        response = self._make_authenticated_request(data=unlink_data)

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["message"], "Unlinked 3 of 3 accounts")
        self.assertEqual(response.data["successfully_unlinked"], 3)
        self.assertEqual(response.data["total_requested"], 3)

        # Verify all accounts are now inactive
        account1.refresh_from_db()
        account2.refresh_from_db()
        account3.refresh_from_db()

        self.assertFalse(account1.is_active)
        self.assertFalse(account2.is_active)
        self.assertFalse(account3.is_active)

    def test_account_unlink_authentication_failure(self):
        """Test account unlinking fails without proper authentication"""
        response = self.client.post(
            self.url, data=json.dumps(self.valid_data), content_type="application/json"
        )

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class CreateTestAnalyticsExportTests(TestCase):
    """Tests for the create_ta_export endpoint"""

    def setUp(self):
        self.client = APIClient()
        self.url = reverse("create-ta-export")
        self.valid_data = {
            "integration_names": ["test-integration-1", "test-integration-2"],
            "gcp_project_id": "test-project-123",
            "destination_bucket": "test-bucket",
            "destination_prefix": "test/prefix/path",
        }

    def _make_authenticated_request(self, data=None, jwt_payload=None):
        """Helper method to make an authenticated request with JWT payload"""
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

    def test_create_ta_export_authentication_failure(self):
        """Test create_ta_export fails without proper authentication"""
        response = self.client.post(
            self.url, data=json.dumps(self.valid_data), content_type="application/json"
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_create_ta_export_invalid_inputs_missing_required_fields(self):
        response = self._make_authenticated_request(data={})
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("integration_names", response.data)
        self.assertIn("gcp_project_id", response.data)
        self.assertIn("destination_bucket", response.data)
        self.assertIn("destination_prefix", response.data)

    def test_create_ta_export_no_integration_names_provided(self):
        """Test create_ta_export with empty integration_names list"""
        data = {
            "integration_names": [],
            "gcp_project_id": "test-project-123",
            "destination_bucket": "test-bucket",
            "destination_prefix": "test/prefix/path",
        }
        response = self._make_authenticated_request(data=data)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertIn("integration_names", response.data)

    @patch("api.sentry.views.TaskService")
    def test_create_ta_export_successful_scheduling(self, mock_task_service_class):
        mock_task_service = mock_task_service_class.return_value
        mock_result_1 = patch("celery.result.AsyncResult").start()
        mock_result_1.id = "task-id-1"
        mock_result_1.status = "PENDING"

        mock_result_2 = patch("celery.result.AsyncResult").start()
        mock_result_2.id = "task-id-2"
        mock_result_2.status = "PENDING"

        mock_task_service.schedule_task.side_effect = [mock_result_1, mock_result_2]

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(response.data["total_tasks"], 2)
        self.assertEqual(response.data["successfully_scheduled"], 2)
        self.assertEqual(len(response.data["tasks"]), 2)

        # Verify first task
        task1 = response.data["tasks"][0]
        self.assertEqual(task1["integration_name"], "test-integration-1")
        self.assertEqual(task1["task_id"], "task-id-1")
        self.assertEqual(task1["status"], "PENDING")

        # Verify second task
        task2 = response.data["tasks"][1]
        self.assertEqual(task2["integration_name"], "test-integration-2")
        self.assertEqual(task2["task_id"], "task-id-2")
        self.assertEqual(task2["status"], "PENDING")

        # Verify schedule_task was called correctly
        self.assertEqual(mock_task_service.schedule_task.call_count, 2)

        # Verify first call
        call_kwargs_1 = mock_task_service.schedule_task.call_args_list[0][1]
        self.assertEqual(
            call_kwargs_1["kwargs"]["integration_name"], "test-integration-1"
        )
        self.assertEqual(call_kwargs_1["kwargs"]["gcp_project_id"], "test-project-123")
        self.assertEqual(call_kwargs_1["kwargs"]["destination_bucket"], "test-bucket")
        self.assertEqual(
            call_kwargs_1["kwargs"]["destination_prefix"], "test/prefix/path"
        )

    @patch("api.sentry.views.TaskService")
    def test_create_ta_export_failed_schedule_task(self, mock_task_service_class):
        mock_task_service = mock_task_service_class.return_value
        mock_task_service.schedule_task.side_effect = Exception(
            "Failed to schedule task"
        )

        data = {
            "integration_names": ["failing-integration"],
            "gcp_project_id": "test-project-123",
            "destination_bucket": "test-bucket",
            "destination_prefix": "test/prefix/path",
        }

        response = self._make_authenticated_request(data=data)

        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(response.data["total_tasks"], 1)
        self.assertEqual(response.data["successfully_scheduled"], 0)
        self.assertEqual(len(response.data["tasks"]), 1)

        # Verify failed task
        task = response.data["tasks"][0]
        self.assertEqual(task["integration_name"], "failing-integration")
        self.assertEqual(task["error"], "Failed to schedule task")
        self.assertEqual(task["status"], "FAILED_TO_SCHEDULE")
        self.assertNotIn("task_id", task)

    @patch("api.sentry.views.TaskService")
    def test_create_ta_export_mixed_success_and_failure(self, mock_task_service_class):
        mock_task_service = mock_task_service_class.return_value
        mock_result = patch("celery.result.AsyncResult").start()
        mock_result.id = "task-id-success"
        mock_result.status = "PENDING"

        mock_task_service.schedule_task.side_effect = [
            mock_result,
            Exception("Failed to schedule second task"),
        ]

        response = self._make_authenticated_request(data=self.valid_data)

        self.assertEqual(response.status_code, status.HTTP_202_ACCEPTED)
        self.assertEqual(response.data["total_tasks"], 2)
        self.assertEqual(response.data["successfully_scheduled"], 1)
        self.assertEqual(len(response.data["tasks"]), 2)

        # Verify successful task
        task1 = response.data["tasks"][0]
        self.assertEqual(task1["integration_name"], "test-integration-1")
        self.assertEqual(task1["task_id"], "task-id-success")
        self.assertEqual(task1["status"], "PENDING")

        # Verify failed task
        task2 = response.data["tasks"][1]
        self.assertEqual(task2["integration_name"], "test-integration-2")
        self.assertEqual(task2["error"], "Failed to schedule second task")
        self.assertEqual(task2["status"], "FAILED_TO_SCHEDULE")
        self.assertNotIn("task_id", task2)


class GetTestAnalyticsExportTests(TestCase):
    """Tests for the get_ta_export endpoint"""

    def setUp(self):
        self.client = APIClient()
        self.task_id = "test-task-id-123"
        self.url = reverse("get-ta-export", kwargs={"task_id": self.task_id})

    def _make_authenticated_request(self, task_id=None, jwt_payload=None):
        """Helper method to make an authenticated request with JWT payload"""
        if task_id:
            url = reverse("get-ta-export", kwargs={"task_id": task_id})
        else:
            url = self.url

        with patch(
            "codecov_auth.permissions.get_sentry_jwt_payload"
        ) as mock_get_payload:
            mock_get_payload.return_value = jwt_payload or {
                "g_p": "github",
                "g_o": "test-org",
            }
            return self.client.get(url)

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_successful_with_success_result(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "SUCCESS"
        mock_result.successful.return_value = True
        mock_result.result = {
            "successful": True,
            "integration_name": "test-integration",
            "exported_files": ["file1.json", "file2.json"],
            "total_records": 1000,
        }

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "SUCCESS")
        self.assertIn("result", response.data)
        self.assertTrue(response.data["result"]["successful"])
        self.assertEqual(
            response.data["result"]["integration_name"], "test-integration"
        )
        self.assertEqual(response.data["result"]["total_records"], 1000)

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_successful_but_reported_failure(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "SUCCESS"
        mock_result.successful.return_value = True
        mock_result.result = {
            "successful": False,
            "integration_name": "test-integration",
            "error": "Failed to export data: Connection timeout",
        }

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "SUCCESS")
        self.assertIn("result", response.data)
        self.assertFalse(response.data["result"]["successful"])
        self.assertEqual(
            response.data["result"]["integration_name"], "test-integration"
        )
        self.assertEqual(
            response.data["result"]["error"],
            "Failed to export data: Connection timeout",
        )

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_failed_with_error(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "FAILURE"
        mock_result.successful.return_value = False
        mock_result.failed.return_value = True

        test_exception = ValueError("Database connection failed")
        mock_result.info = test_exception

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "FAILURE")
        self.assertIn("error", response.data)
        self.assertEqual(
            response.data["error"]["message"], "Database connection failed"
        )
        self.assertEqual(response.data["error"]["type"], "ValueError")

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_in_progress_pending(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "PENDING"
        mock_result.successful.return_value = False
        mock_result.failed.return_value = False

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "PENDING")
        self.assertNotIn("result", response.data)
        self.assertNotIn("error", response.data)

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_in_progress_retry(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "RETRY"
        mock_result.successful.return_value = False
        mock_result.failed.return_value = False

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "RETRY")
        self.assertNotIn("result", response.data)
        self.assertNotIn("error", response.data)

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_successful_with_non_dict_result(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "SUCCESS"
        mock_result.successful.return_value = True
        mock_result.result = "Simple string result"

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "SUCCESS")
        self.assertIn("result", response.data)
        self.assertEqual(response.data["result"], "Simple string result")

    @patch("api.sentry.views.AsyncResult")
    def test_get_ta_export_failed_with_no_error_info(self, mock_async_result):
        mock_result = mock_async_result.return_value
        mock_result.status = "FAILURE"
        mock_result.successful.return_value = False
        mock_result.failed.return_value = True
        mock_result.info = None

        response = self._make_authenticated_request()

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data["task_id"], self.task_id)
        self.assertEqual(response.data["status"], "FAILURE")
        self.assertIn("error", response.data)
        self.assertEqual(response.data["error"]["type"], "Unknown")
