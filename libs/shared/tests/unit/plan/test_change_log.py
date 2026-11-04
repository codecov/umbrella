import logging

import pytest

from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.plan.constants import (
    DEFAULT_FREE_PLAN,
    TRIAL_PLAN_SEATS,
    PlanName,
    TrialStatus,
)
from shared.plan.service import PlanService
from tests.helper import mock_all_plans_and_tiers

LOGGER = "shared.plan.change_log"


def _plan_change_records(caplog):
    return [r for r in caplog.records if r.message == "Owner plan changed"]


@pytest.fixture
def plans(db):
    mock_all_plans_and_tiers()


def test_logs_stripe_upgrade(plans, caplog):
    owner = OwnerFactory(
        plan=DEFAULT_FREE_PLAN,
        plan_user_count=1,
        stripe_customer_id="cus_123",
        stripe_subscription_id="sub_123",
    )
    caplog.set_level(logging.INFO, logger=LOGGER)

    PlanService(current_org=owner).update_plan(
        name=PlanName.CODECOV_PRO_MONTHLY.value, user_count=10
    )

    (record,) = _plan_change_records(caplog)
    assert record.ownerid == owner.ownerid
    assert record.previous_plan == DEFAULT_FREE_PLAN
    assert record.new_plan == PlanName.CODECOV_PRO_MONTHLY.value
    assert record.previous_plan_user_count == 1
    assert record.new_plan_user_count == 10
    assert record.billing_provider == "stripe"
    assert record.stripe_customer_id == "cus_123"
    assert record.previous_stripe_subscription_id == "sub_123"
    assert record.new_stripe_subscription_id == "sub_123"
    assert not hasattr(record, "github_marketplace_account_id")


def test_logs_seat_only_change(plans, caplog):
    owner = OwnerFactory(
        plan=PlanName.CODECOV_PRO_MONTHLY.value,
        plan_user_count=5,
        stripe_subscription_id="sub_123",
    )
    caplog.set_level(logging.INFO, logger=LOGGER)

    PlanService(current_org=owner).update_plan(
        name=PlanName.CODECOV_PRO_MONTHLY.value, user_count=7
    )

    (record,) = _plan_change_records(caplog)
    assert record.previous_plan == record.new_plan
    assert record.previous_plan_user_count == 5
    assert record.new_plan_user_count == 7


def test_logs_stripe_cancellation(plans, caplog):
    owner = OwnerFactory(
        plan=PlanName.CODECOV_PRO_MONTHLY.value,
        plan_user_count=5,
        stripe_customer_id="cus_123",
        stripe_subscription_id="sub_123",
    )
    caplog.set_level(logging.INFO, logger=LOGGER)

    PlanService(current_org=owner).set_default_plan_data()

    (record,) = _plan_change_records(caplog)
    assert record.new_plan == DEFAULT_FREE_PLAN
    assert record.new_plan_user_count == 1
    assert record.billing_provider == "stripe"
    assert record.previous_stripe_subscription_id == "sub_123"
    assert record.new_stripe_subscription_id is None


def test_logs_trial_start_and_cancel(plans, caplog):
    owner = OwnerFactory(plan=DEFAULT_FREE_PLAN, plan_user_count=1)
    caplog.set_level(logging.INFO, logger=LOGGER)

    PlanService(current_org=owner).start_trial(current_owner=owner)
    PlanService(current_org=owner).cancel_trial()

    start, cancel = _plan_change_records(caplog)
    assert start.previous_plan == DEFAULT_FREE_PLAN
    assert start.new_plan == PlanName.TRIAL_PLAN_NAME.value
    assert start.new_plan_user_count == TRIAL_PLAN_SEATS
    assert start.previous_trial_status == TrialStatus.NOT_STARTED.value
    assert start.new_trial_status == TrialStatus.ONGOING.value
    assert start.billing_provider is None

    assert cancel.previous_plan == PlanName.TRIAL_PLAN_NAME.value
    assert cancel.new_plan == DEFAULT_FREE_PLAN
    assert cancel.previous_trial_status == TrialStatus.ONGOING.value
    assert cancel.new_trial_status == TrialStatus.EXPIRED.value


def test_logs_free_seat_change(plans, caplog):
    owner = OwnerFactory(free=0)
    caplog.set_level(logging.INFO, logger=LOGGER)

    owner.free = 3
    owner.save()

    (record,) = _plan_change_records(caplog)
    assert record.previous_free_seats == 0
    assert record.new_free_seats == 3


def test_logs_github_marketplace_identifier(plans, caplog):
    owner = OwnerFactory(
        service_id="98765",
        plan=PlanName.GHM_PLAN_NAME.value,
        plan_provider="github",
        plan_user_count=10,
    )
    caplog.set_level(logging.INFO, logger=LOGGER)

    owner.plan = DEFAULT_FREE_PLAN
    owner.plan_user_count = 1
    owner.save()

    (record,) = _plan_change_records(caplog)
    assert record.billing_provider == "github_marketplace"
    assert record.github_marketplace_account_id == "98765"


def test_github_marketplace_to_stripe_is_labeled_stripe(plans, caplog):
    owner = OwnerFactory(
        plan=PlanName.GHM_PLAN_NAME.value,
        plan_provider="github",
        plan_user_count=10,
    )
    owner.stripe_customer_id = "cus_123"
    owner.stripe_subscription_id = "sub_123"
    owner.save()
    caplog.set_level(logging.INFO, logger=LOGGER)

    owner.plan = PlanName.CODECOV_PRO_MONTHLY.value
    owner.save()

    (record,) = _plan_change_records(caplog)
    assert record.previous_plan == PlanName.GHM_PLAN_NAME.value
    assert record.billing_provider == "stripe"
    assert record.new_stripe_subscription_id == "sub_123"


def test_trial_upgrade_in_one_save_logs_once(plans, caplog):
    owner = OwnerFactory(plan=DEFAULT_FREE_PLAN, plan_user_count=3)
    PlanService(current_org=owner).start_trial(current_owner=owner)
    caplog.set_level(logging.INFO, logger=LOGGER)

    plan_service = PlanService(current_org=owner)
    plan_service.expire_trial_when_upgrading(save=False)
    plan_service.update_plan(name=PlanName.CODECOV_PRO_MONTHLY.value, user_count=8)

    (record,) = _plan_change_records(caplog)
    assert record.previous_plan == PlanName.TRIAL_PLAN_NAME.value
    assert record.new_plan == PlanName.CODECOV_PRO_MONTHLY.value
    assert record.previous_plan_user_count == TRIAL_PLAN_SEATS
    assert record.new_plan_user_count == 8
    assert record.previous_trial_status == TrialStatus.ONGOING.value
    assert record.new_trial_status == TrialStatus.EXPIRED.value


def test_update_fields_only_logs_persisted_fields(plans, caplog):
    owner = OwnerFactory(plan=DEFAULT_FREE_PLAN, plan_user_count=1)
    caplog.set_level(logging.INFO, logger=LOGGER)

    owner.plan = PlanName.CODECOV_PRO_MONTHLY.value
    owner.plan_user_count = 5
    owner.name = "new name"
    owner.save(update_fields=["name"])
    assert _plan_change_records(caplog) == []

    owner.save(update_fields=["plan"])
    (record,) = _plan_change_records(caplog)
    assert record.new_plan == PlanName.CODECOV_PRO_MONTHLY.value
    assert record.previous_plan_user_count == 1
    assert record.new_plan_user_count == 1

    owner.save()
    first, second = _plan_change_records(caplog)
    assert second.previous_plan == second.new_plan
    assert second.previous_plan_user_count == 1
    assert second.new_plan_user_count == 5


def test_logs_change_to_deferred_field(plans, caplog):
    owner = OwnerFactory(plan=DEFAULT_FREE_PLAN, plan_user_count=2)
    caplog.set_level(logging.INFO, logger=LOGGER)

    deferred = Owner.objects.only("ownerid", "username").get(pk=owner.pk)
    deferred.plan_user_count = 9
    deferred.save()

    (record,) = _plan_change_records(caplog)
    assert record.previous_plan_user_count == 2
    assert record.new_plan_user_count == 9
    assert record.previous_plan == record.new_plan == DEFAULT_FREE_PLAN


def test_refresh_from_db_resets_snapshot(plans, caplog):
    owner = OwnerFactory(plan=DEFAULT_FREE_PLAN, plan_user_count=2)
    Owner.objects.filter(pk=owner.pk).update(plan_user_count=7)
    caplog.set_level(logging.INFO, logger=LOGGER)

    owner.refresh_from_db()
    owner.save()

    assert _plan_change_records(caplog) == []


def test_no_log_for_unrelated_changes(plans, caplog):
    owner = OwnerFactory()
    caplog.set_level(logging.INFO, logger=LOGGER)

    owner.name = "new name"
    owner.stripe_subscription_id = "sub_123"
    owner.save()
    OwnerFactory()

    assert _plan_change_records(caplog) == []
