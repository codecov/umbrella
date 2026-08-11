from unittest.mock import patch

import pytest
from asgiref.sync import sync_to_async
from django.test import TestCase

from codecov.commands.exceptions import (
    NotFound,
    Unauthenticated,
    Unauthorized,
    ValidationError,
)
from shared.django_apps.codecov_auth.tests.factories import (
    AccountFactory,
    StripeBillingFactory,
)
from shared.django_apps.core.tests.factories import OwnerFactory
from shared.plan.constants import PlanName

from ..delete_owner import DeleteOwnerInteractor


class DeleteOwnerInteractorTest(TestCase):
    def setUp(self):
        self.current_owner = OwnerFactory(username="current-user", service="github")
        self.org = OwnerFactory(
            username="some-org",
            service="github",
            admins=[self.current_owner.ownerid],
        )
        # make the current owner a member + admin of the org
        self.current_owner.organizations = [self.org.ownerid]
        self.current_owner.save()

        self.member_owner = OwnerFactory(
            username="member-user",
            service="github",
            organizations=[self.org.ownerid],
        )
        self.random_owner = OwnerFactory(username="random-user", service="github")

    def execute(self, current_owner, username):
        service = current_owner.service if current_owner else "github"
        return DeleteOwnerInteractor(current_owner, service).execute(username)

    @sync_to_async
    def _update_current_owner(self, **attrs):
        for key, value in attrs.items():
            setattr(self.current_owner, key, value)
        self.current_owner.save()

    @sync_to_async
    def _setup_account_stripe_billing(self):
        account = AccountFactory()
        self.current_owner.account = account
        self.current_owner.plan = PlanName.CODECOV_PRO_MONTHLY.value
        self.current_owner.save()
        StripeBillingFactory(account=account, is_active=True)

    async def test_when_unauthenticated_raises(self):
        with pytest.raises(Unauthenticated):
            await self.execute(None, self.current_owner.username)

    async def test_when_owner_not_found_raises(self):
        with pytest.raises(NotFound):
            await self.execute(self.current_owner, "does-not-exist")

    async def test_when_not_admin_of_org_raises(self):
        with pytest.raises(Unauthorized):
            await self.execute(self.member_owner, self.org.username)

    async def test_when_not_part_of_org_raises(self):
        with pytest.raises(Unauthorized):
            await self.execute(self.random_owner, self.org.username)

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    async def test_delete_personal_account(self, task_service_mock):
        await self.execute(self.current_owner, self.current_owner.username)
        task_service_mock.return_value.delete_owner.assert_called_once_with(
            ownerid=self.current_owner.ownerid
        )

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    async def test_delete_org_as_admin(self, task_service_mock):
        await self.execute(self.current_owner, self.org.username)
        task_service_mock.return_value.delete_owner.assert_called_once_with(
            ownerid=self.org.ownerid
        )

    async def test_when_invoice_billed_raises(self):
        await self._update_current_owner(uses_invoice=True)
        with pytest.raises(ValidationError):
            await self.execute(self.current_owner, self.current_owner.username)

    async def test_when_github_marketplace_paid_raises(self):
        await self._update_current_owner(
            plan=PlanName.CODECOV_PRO_MONTHLY.value,
            plan_provider="github",
        )
        with pytest.raises(ValidationError):
            await self.execute(self.current_owner, self.current_owner.username)

    async def test_when_stripe_subscription_raises(self):
        await self._update_current_owner(
            plan=PlanName.CODECOV_PRO_MONTHLY.value,
            stripe_subscription_id="sub_123",
        )
        with pytest.raises(ValidationError):
            await self.execute(self.current_owner, self.current_owner.username)

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    async def test_when_free_plan_allows_deletion(self, task_service_mock):
        await self._update_current_owner(
            plan=PlanName.USERS_DEVELOPER.value,
            stripe_subscription_id=None,
        )
        await self.execute(self.current_owner, self.current_owner.username)
        task_service_mock.return_value.delete_owner.assert_called_once_with(
            ownerid=self.current_owner.ownerid
        )

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    async def test_when_account_stripe_billing_raises(self, task_service_mock):
        await self._setup_account_stripe_billing()
        with pytest.raises(ValidationError):
            await self.execute(self.current_owner, self.current_owner.username)
        task_service_mock.return_value.delete_owner.assert_not_called()
