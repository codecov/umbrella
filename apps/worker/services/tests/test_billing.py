import pytest
from django.test import override_settings

from database.tests.factories import OwnerFactory
from shared.plan.constants import PlanName
from shared.plan.service import PlanService
from tests.helpers import mock_all_plans_and_tiers


class TestBillingServiceTestCase:
    """
    BillingService is deprecated - use PlanService instead.
    """

    @pytest.fixture(autouse=True)
    def setup(self):
        mock_all_plans_and_tiers()

    @pytest.mark.django_db
    def test_pr_author_plan_check(self, request, dbsession, with_sql_functions):
        owner = OwnerFactory.create(service="github", plan="users-pr-inappm")
        dbsession.add(owner)
        dbsession.flush()
        plan = PlanService(owner)
        assert plan.is_pr_billing_plan

    @pytest.mark.django_db
    @override_settings(IS_ENTERPRISE=True)
    def test_pr_author_enterprise_plan_check(
        self, request, dbsession, with_sql_functions
    ):
        """Enterprise deployments always use PR billing."""
        owner = OwnerFactory.create(service="github")
        dbsession.add(owner)
        dbsession.flush()

        plan = PlanService(owner)

        assert plan.is_pr_billing_plan

    @pytest.mark.django_db
    def test_plan_not_pr_author(self, request, dbsession, with_sql_functions):
        owner = OwnerFactory.create(
            service="github", plan=PlanName.CODECOV_PRO_MONTHLY_LEGACY.value
        )
        dbsession.add(owner)
        dbsession.flush()

        plan = PlanService(owner)

        assert not plan.is_pr_billing_plan
