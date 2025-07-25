from django.test import TestCase, override_settings

from billing.helpers import on_enterprise_plan
from billing.tests.mocks import mock_all_plans_and_tiers
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.plan.constants import PlanName


class HelpersTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        mock_all_plans_and_tiers()

    @override_settings(IS_ENTERPRISE=True)
    def test_on_enterprise_plan_on_prem(self):
        owner = OwnerFactory()
        assert on_enterprise_plan(owner) == True

    def test_on_enterprise_plan_enterprise_cloud(self):
        plan_names = [
            PlanName.ENTERPRISE_CLOUD_MONTHLY.value,
            PlanName.ENTERPRISE_CLOUD_YEARLY.value,
        ]

        for plan in plan_names:
            owner = OwnerFactory(plan=plan)
            assert on_enterprise_plan(owner) == True

    def test_on_enterprise_plan_cloud(self):
        owner = OwnerFactory()
        assert on_enterprise_plan(owner) == False
