from datetime import timedelta

import pytest
from django.test import TestCase, override_settings
from django.utils import timezone
from freezegun import freeze_time

from billing.tests.mocks import mock_all_plans_and_tiers
from shared.django_apps.codecov_auth.tests.factories import AccountFactory
from shared.django_apps.core.tests.factories import OwnerFactory
from shared.plan.constants import PlanName, TrialStatus

from .helper import GraphQLTestHelper


class TestPlanType(GraphQLTestHelper, TestCase):
    @pytest.fixture(scope="function", autouse=True)
    def inject_mocker(request, mocker):
        request.mocker = mocker

    def setUp(self):
        mock_all_plans_and_tiers()
        self.current_org = OwnerFactory(
            username="random-plan-user",
            service="github",
            trial_start_date=timezone.now(),
            trial_end_date=timezone.now() + timedelta(days=14),
        )

    @freeze_time("2023-06-19")
    def test_owner_plan_data_when_trialing(self):
        now = timezone.now()
        later = timezone.now() + timedelta(days=14)
        current_org = OwnerFactory(
            username="random-plan-user",
            service="github",
            plan=PlanName.TRIAL_PLAN_NAME.value,
            trial_start_date=now,
            trial_end_date=later,
            trial_status=TrialStatus.ONGOING.value,
            pretrial_users_count=234,
            plan_user_count=123,
        )
        query = f"""{{
            owner(username: "{current_org.username}") {{
                plan {{
                    trialStatus
                    trialEndDate
                    trialStartDate
                    trialTotalDays
                    marketingName
                    value
                    tierName
                    billingRate
                    baseUnitPrice
                    benefits
                    monthlyUploadLimit
                    pretrialUsersCount
                    planUserCount
                    isEnterprisePlan
                    isFreePlan
                    isProPlan
                    isSentryPlan
                    isTeamPlan
                    isTrialPlan
                }}
            }}
        }}
        """
        data = self.gql_request(query, owner=current_org)
        assert data["owner"]["plan"] == {
            "trialStatus": "ONGOING",
            "trialEndDate": "2023-07-03T00:00:00",
            "trialStartDate": "2023-06-19T00:00:00",
            "trialTotalDays": 14,
            "marketingName": "Developer",
            "value": "users-trial",
            "tierName": "trial",
            "billingRate": None,
            "baseUnitPrice": 0,
            "benefits": [
                "Configurable # of users",
                "Unlimited public repositories",
                "Unlimited private repositories",
                "Priority Support",
            ],
            "monthlyUploadLimit": None,
            "pretrialUsersCount": 234,
            "planUserCount": 123,
            "isEnterprisePlan": False,
            "isFreePlan": False,
            "isProPlan": False,
            "isSentryPlan": False,
            "isTeamPlan": False,
            "isTrialPlan": True,
        }

    def test_owner_plan_data_with_account(self):
        self.current_org.account = AccountFactory(
            plan=PlanName.CODECOV_PRO_YEARLY.value,
            plan_seat_count=25,
            free_seat_count=4,
        )
        self.current_org.save()
        query = f"""{{
                owner(username: "{self.current_org.username}") {{
                    plan {{
                        marketingName
                        value
                        tierName
                        billingRate
                        baseUnitPrice
                        planUserCount
                        freeSeatCount
                        isEnterprisePlan
                        isFreePlan
                        isProPlan
                        isSentryPlan
                        isTeamPlan
                        isTrialPlan
                    }}
                }}
            }}
            """
        data = self.gql_request(query, owner=self.current_org)
        assert data["owner"]["plan"] == {
            "marketingName": "Pro",
            "value": "users-pr-inappy",
            "tierName": "pro",
            "billingRate": "annually",
            "baseUnitPrice": 10,
            "planUserCount": 29,
            "freeSeatCount": 4,
            "isEnterprisePlan": False,
            "isFreePlan": False,
            "isProPlan": True,
            "isSentryPlan": False,
            "isTeamPlan": False,
            "isTrialPlan": False,
        }

    def test_owner_plan_data_has_seats_left(self):
        current_org = OwnerFactory(
            username="random-plan-user",
            service="github",
            plan=PlanName.TRIAL_PLAN_NAME.value,
            trial_status=TrialStatus.ONGOING.value,
            plan_user_count=2,
            plan_activated_users=[],
        )
        query = f"""{{
            owner(username: "{current_org.username}") {{
                plan {{
                    hasSeatsLeft
                }}
            }}
        }}
        """
        data = self.gql_request(query, owner=current_org)
        assert data["owner"]["plan"] == {"hasSeatsLeft": True}

    @override_settings(IS_ENTERPRISE=True)
    def test_plan_user_count_for_enterprise_org(self):
        """
        Enterprise deployments have unlimited users (plan_user_count = 0).
        """
        enterprise_org = OwnerFactory(
            username="random-plan-user",
            service="github",
            plan=PlanName.CODECOV_PRO_YEARLY.value,
            plan_user_count=1,
            plan_activated_users=[],
        )
        for i in range(4):
            new_owner = OwnerFactory()
            enterprise_org.plan_activated_users.append(new_owner.ownerid)
        enterprise_org.save()

        query = f"""{{
                    owner(username: "{enterprise_org.username}") {{
                        plan {{
                            planUserCount
                            hasSeatsLeft
                        }}
                    }}
                }}
                """
        data = self.gql_request(query, owner=enterprise_org)
        # Enterprise has unlimited users (0) and always has seats left
        assert data["owner"]["plan"]["planUserCount"] == 0
        assert data["owner"]["plan"]["hasSeatsLeft"] == True

    @override_settings(IS_ENTERPRISE=True)
    def test_plan_for_enterprise_org_always_has_seats(self):
        """
        Enterprise deployments always have seats available.
        """
        enterprise_org = OwnerFactory(
            username="random-plan-user",
            service="github",
            plan=PlanName.CODECOV_PRO_YEARLY.value,
            plan_user_count=1,
            plan_activated_users=[],
        )
        query = f"""{{
                        owner(username: "{enterprise_org.username}") {{
                            plan {{
                                planUserCount
                                hasSeatsLeft
                            }}
                        }}
                    }}
                    """
        data = self.gql_request(query, owner=enterprise_org)
        # Enterprise has unlimited users and always has seats left
        assert data["owner"]["plan"]["planUserCount"] == 0
        assert data["owner"]["plan"]["hasSeatsLeft"] == True

    def test_owner_plan_data_when_trial_status_is_none(self):
        now = timezone.now()
        later = now + timedelta(days=14)
        current_org = OwnerFactory(
            username="random-plan-user",
            service="github",
            plan=PlanName.TRIAL_PLAN_NAME.value,
            trial_start_date=now,
            trial_end_date=later,
            trial_status=None,
        )
        query = f"""{{
            owner(username: "{current_org.username}") {{
                plan {{
                    trialStatus
                }}
            }}
        }}
        """
        data = self.gql_request(query, owner=current_org)
        assert data["owner"]["plan"]["trialStatus"] == "NOT_STARTED"
