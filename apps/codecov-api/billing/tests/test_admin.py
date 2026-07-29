from django.test import TestCase
from django.urls import reverse

from shared.django_apps.codecov_auth.tests.factories import (
    AccountFactory,
    OwnerFactory,
    UserFactory,
)
from shared.plan.constants import PlanName
from utils.test_utils import Client


class StripeBillingAdminTest(TestCase):
    def setUp(self):
        self.staff_user = UserFactory(is_staff=True)
        self.client = Client()
        self.client.force_login(user=self.staff_user)

        self.account = AccountFactory(
            name="Acme Corp",
            plan=PlanName.CODECOV_PRO_YEARLY.value,
            plan_seat_count=10,
            free_seat_count=2,
        )
        self.owner = OwnerFactory(
            account=self.account,
            username="acme-org",
            stripe_customer_id="cus_lookup_123",
            stripe_subscription_id="sub_lookup_456",
        )
        self.associated_owner = OwnerFactory(
            account=self.account,
            username="acme-labs",
        )
        self.unrelated_owner = OwnerFactory(
            username="not-acme",
            plan=PlanName.BASIC_PLAN_NAME.value,
            plan_user_count=5,
            free=1,
            plan_activated_users=[],
            stripe_customer_id="cus_someone_else",
        )
        self.owner_without_customer = OwnerFactory(username="no-stripe-customer")

        self.changelist_url = reverse("admin:billing_stripebilling_changelist")
        self.detail_url = reverse(
            "admin:billing_stripebilling_change", args=[self.owner.ownerid]
        )

    def test_billing_section_listed_on_admin_index(self):
        response = self.client.get(reverse("admin:index"))

        assert response.status_code == 200
        self.assertContains(response, "Billing")
        self.assertContains(response, self.changelist_url)

    def test_changelist_uses_owner_stripe_customer_data(self):
        response = self.client.get(self.changelist_url)

        assert response.status_code == 200
        self.assertContains(response, "cus_lookup_123")
        self.assertContains(response, "sub_lookup_456")
        self.assertContains(response, "acme-org")
        self.assertContains(response, "Acme Corp")
        self.assertNotContains(response, self.owner_without_customer.username)

    def test_lookup_by_stripe_customer_id(self):
        response = self.client.get(self.changelist_url, {"q": "cus_lookup_123"})

        assert response.status_code == 200
        self.assertContains(response, "cus_lookup_123")
        self.assertNotContains(response, self.unrelated_owner.stripe_customer_id)

    def test_partial_customer_id_search(self):
        response = self.client.get(self.changelist_url, {"q": "lookup_123"})

        assert response.status_code == 200
        self.assertContains(response, "cus_lookup_123")
        self.assertNotContains(response, self.unrelated_owner.stripe_customer_id)

    def test_search_bar_is_rendered(self):
        response = self.client.get(self.changelist_url)

        assert response.status_code == 200
        self.assertContains(response, 'name="q"')
        self.assertContains(response, "Search by Stripe customer id")

    def test_changelist_shows_plan_and_seats(self):
        response = self.client.get(self.changelist_url)

        assert response.status_code == 200
        self.assertContains(response, PlanName.CODECOV_PRO_YEARLY.value)
        self.assertContains(response, "0 / 12")
        self.assertContains(response, PlanName.BASIC_PLAN_NAME.value)
        self.assertContains(response, "0 / 6")

    def test_lookup_by_subscription_owner_and_account(self):
        for term in ("sub_lookup_456", "acme-org", "Acme"):
            response = self.client.get(self.changelist_url, {"q": term})
            assert response.status_code == 200
            self.assertContains(response, "cus_lookup_123")

    def test_non_numeric_search_does_not_error(self):
        response = self.client.get(
            self.changelist_url, {"q": "definitely-not-a-number"}
        )

        assert response.status_code == 200

    def test_numeric_search_matches_owner_and_account_ids(self):
        for identifier in (self.owner.ownerid, self.account.id):
            response = self.client.get(self.changelist_url, {"q": str(identifier)})
            assert response.status_code == 200
            self.assertContains(response, "cus_lookup_123")

    def test_detail_links_to_owner_and_account(self):
        response = self.client.get(self.detail_url)

        assert response.status_code == 200
        self.assertContains(
            response,
            reverse("admin:codecov_auth_owner_change", args=[self.owner.ownerid]),
        )
        self.assertContains(
            response,
            reverse("admin:codecov_auth_account_change", args=[self.account.id]),
        )

    def test_detail_lists_all_owners_associated_with_account(self):
        response = self.client.get(self.detail_url)

        assert response.status_code == 200
        for owner in (self.owner, self.associated_owner):
            self.assertContains(
                response,
                reverse("admin:codecov_auth_owner_change", args=[owner.ownerid]),
            )
            self.assertContains(response, owner.username)
        self.assertNotContains(response, self.unrelated_owner.username)

    def test_standalone_owner_lists_itself(self):
        url = reverse(
            "admin:billing_stripebilling_change",
            args=[self.unrelated_owner.ownerid],
        )

        response = self.client.get(url)

        assert response.status_code == 200
        self.assertContains(response, self.unrelated_owner.username)
        self.assertNotContains(
            response,
            reverse("admin:codecov_auth_account_change", args=[self.account.id]),
        )

    def test_detail_links_out_to_stripe(self):
        response = self.client.get(self.detail_url)

        self.assertContains(
            response, "https://dashboard.stripe.com/customers/cus_lookup_123"
        )
        self.assertContains(
            response, "https://dashboard.stripe.com/subscriptions/sub_lookup_456"
        )

    def test_view_is_read_only(self):
        assert self.client.get(f"{self.changelist_url}add/").status_code == 403

        response = self.client.post(
            self.detail_url, {"stripe_customer_id": "cus_tampered"}
        )

        assert response.status_code == 403
        self.owner.refresh_from_db()
        assert self.owner.stripe_customer_id == "cus_lookup_123"

    def test_owner_count_column(self):
        response = self.client.get(self.changelist_url)

        assert response.status_code == 200
        self.assertContains(response, "field-owner_count")
