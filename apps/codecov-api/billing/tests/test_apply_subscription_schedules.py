from datetime import datetime
from io import StringIO
from unittest.mock import MagicMock, patch

import pytest
from django.core.management import call_command

from shared.django_apps.codecov_auth.tests.factories import OwnerFactory, PlanFactory


@pytest.fixture
def target_plan(db):
    """Plan used as --target-plan."""
    return PlanFactory(
        name="users-pr-inappy",
        stripe_id="price_target_123",
    )


@pytest.fixture
def current_plan_in_stripe(db):
    """Plan with stripe_id matching mock subscription default (price_current_456). Used when testing --target-plan omitted."""
    return PlanFactory(
        name="users-current",
        stripe_id="price_current_456",
    )


@pytest.fixture
def owner_with_subscription(db):
    """Owner with Stripe subscription (no schedule)."""
    return OwnerFactory(
        stripe_subscription_id="sub_123",
        stripe_customer_id="cus_123",
    )


class _MockSubscription:
    """Mock Stripe subscription that supports both dict and attribute access (command uses subscription['items'] and subscription.schedule)."""

    def __init__(self, data):
        self._data = data
        self.schedule = data.get("schedule")

    def __getitem__(self, key):
        return self._data[key]


def _make_mock_subscription(
    *,
    plan_id="price_current_456",
    quantity=5,
    current_period_start=1000000,
    current_period_end=2000000,
    schedule=None,
):
    """Build a mock Stripe subscription (supports subscription['key'] and subscription.schedule)."""
    data = {
        "id": "sub_123",
        "items": {
            "data": [
                {
                    "id": "si_123",
                    "plan": {"id": plan_id},
                    "quantity": quantity,
                }
            ]
        },
        "current_period_start": current_period_start,
        "current_period_end": current_period_end,
        "schedule": schedule,
    }
    return _MockSubscription(data)


def _make_mock_schedule(phases=None, metadata=None):
    """Build a mock Stripe SubscriptionSchedule (object with id, phases, metadata)."""
    s = MagicMock()
    s.id = "sub_sched_123"
    s.phases = phases or [
        {"start_date": 1000000, "end_date": 2000000},
    ]
    s.metadata = metadata or {}
    return s


@pytest.mark.django_db
@patch("stripe.SubscriptionSchedule.modify")
@patch("stripe.SubscriptionSchedule.create")
@patch("stripe.Subscription.retrieve")
def test_apply_subscription_schedules_creates_schedule(
    mock_sub_retrieve,
    mock_schedule_create,
    mock_schedule_modify,
    owner_with_subscription,
    target_plan,
):
    """Creating a new schedule calls create then modify with two phases."""
    mock_sub_retrieve.return_value = _make_mock_subscription(
        plan_id="price_current_456",
        quantity=5,
        current_period_start=1000000,
        current_period_end=2000000,
        schedule=None,
    )
    mock_schedule_create.return_value = MagicMock(id="sub_sched_new")

    out = StringIO()
    err = StringIO()
    end_date = "2025-12-31"

    call_command(
        "apply_subscription_schedules",
        "--target-plan=users-pr-inappy",
        f"--end-date={end_date}",
        "--owner-ids",
        str(owner_with_subscription.ownerid),
        "--limit=10",
        stdout=out,
        stderr=err,
    )

    mock_sub_retrieve.assert_called_once_with(
        owner_with_subscription.stripe_subscription_id
    )
    mock_schedule_create.assert_called_once()
    assert (
        mock_schedule_create.call_args.kwargs["from_subscription"]
        == owner_with_subscription.stripe_subscription_id
    )
    assert (
        mock_schedule_create.call_args.kwargs["metadata"]["task_signature"]
        == "cancel_task"
    )
    assert mock_schedule_create.call_args.kwargs["metadata"]["end_date"] == end_date

    mock_schedule_modify.assert_called_once()
    modify_kw = mock_schedule_modify.call_args.kwargs
    assert modify_kw["end_behavior"] == "cancel"
    assert len(modify_kw["phases"]) == 2
    assert modify_kw["phases"][0]["items"][0]["plan"] == "price_current_456"
    assert modify_kw["phases"][1]["items"][0]["plan"] == target_plan.stripe_id

    assert "Created schedule" in out.getvalue()
    assert "scheduled" in out.getvalue().lower() or "Scheduled" in out.getvalue()


@pytest.mark.django_db
@pytest.mark.usefixtures("target_plan")
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.Subscription.retrieve"
)
def test_apply_subscription_schedules_dry_run_no_stripe_writes(
    mock_sub_retrieve,
    owner_with_subscription,
):
    """Dry run only retrieves subscription; no create/modify."""
    mock_sub_retrieve.return_value = _make_mock_subscription(schedule=None)

    out = StringIO()
    call_command(
        "apply_subscription_schedules",
        "--target-plan=users-pr-inappy",
        "--end-date=2025-12-31",
        "--owner-ids",
        str(owner_with_subscription.ownerid),
        "--dry-run",
        stdout=out,
        stderr=StringIO(),
    )

    mock_sub_retrieve.assert_called_once()
    assert "DRY RUN" in out.getvalue()
    assert "Would" in out.getvalue() or "Created" not in out.getvalue()


@pytest.mark.django_db
@pytest.mark.usefixtures("target_plan")
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.SubscriptionSchedule.retrieve"
)
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.Subscription.retrieve"
)
def test_apply_subscription_schedules_skips_when_has_schedule_with_same_end_date(
    mock_sub_retrieve,
    mock_schedule_retrieve,
    owner_with_subscription,
):
    """When schedule already exists with same end date, command skips."""
    mock_sub_retrieve.return_value = _make_mock_subscription(schedule="sub_sched_123")
    mock_schedule_retrieve.return_value = _make_mock_schedule(
        phases=[{"start_date": 1000000, "end_date": 2000000}],
        metadata={
            "task_signature": "cancel_task",
            "end_date": "2025-12-31",
        },
    )

    out = StringIO()
    call_command(
        "apply_subscription_schedules",
        "--target-plan=users-pr-inappy",
        "--end-date=2025-12-31",
        "--owner-ids",
        str(owner_with_subscription.ownerid),
        stdout=out,
        stderr=StringIO(),
    )

    assert "SKIPPED" in out.getvalue()
    assert "already has schedule" in out.getvalue().lower()


@pytest.mark.django_db
@pytest.mark.usefixtures("target_plan")
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.SubscriptionSchedule.modify"
)
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.SubscriptionSchedule.retrieve"
)
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.Subscription.retrieve"
)
def test_apply_subscription_schedules_updates_existing_cancellation_schedule(
    mock_sub_retrieve,
    mock_schedule_retrieve,
    _mock_schedule_modify,  # not used but needed so owner_with_subscription is the fixture in correct order
    owner_with_subscription,
):
    """When schedule exists with different end date, command updates last phase."""
    mock_sub_retrieve.return_value = _make_mock_subscription(schedule="sub_sched_123")
    existing = _make_mock_schedule(
        phases=[
            {"start_date": 1000000, "end_date": 2000000},
            {"start_date": 2000000, "end_date": 3000000},
        ],
        metadata={"task_signature": "cancel_task", "end_date": "2025-06-30"},
    )
    existing.phases = [
        {"start_date": 1000000, "end_date": 2000000},
        {"start_date": 2000000, "end_date": 3000000},
    ]
    mock_schedule_retrieve.return_value = existing

    out = StringIO()
    call_command(
        "apply_subscription_schedules",
        "--target-plan=users-pr-inappy",
        "--end-date=2025-12-31",
        "--owner-ids",
        str(owner_with_subscription.ownerid),
        stdout=out,
        stderr=StringIO(),
    )

    # Command calls existing_schedule.modify(), not stripe.SubscriptionSchedule.modify
    existing.modify.assert_called_once()
    phases = existing.modify.call_args.kwargs["phases"]
    assert len(phases) == 2
    # Command passes end_date (datetime) into the new last phase
    expected_end = datetime.fromisoformat("2025-12-31")
    assert phases[1]["end_date"] == expected_end
    assert "Updated schedule" in out.getvalue()


@pytest.mark.django_db
@pytest.mark.usefixtures("target_plan")
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.SubscriptionSchedule.modify"
)
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.SubscriptionSchedule.retrieve"
)
@patch(
    "billing.management.commands.apply_subscription_schedules.stripe.Subscription.retrieve"
)
def test_apply_subscription_schedules_adds_end_phase_to_existing_schedule(
    mock_sub_retrieve,
    mock_schedule_retrieve,
    _mock_schedule_modify,
    owner_with_subscription,
    target_plan,
):
    """When schedule exists without our task signature, command appends end phase."""
    mock_sub_retrieve.return_value = _make_mock_subscription(
        schedule="sub_sched_123",
        quantity=3,
    )
    existing_phases = [
        {"start_date": 1000000, "end_date": 2000000},
    ]
    existing = _make_mock_schedule(
        phases=existing_phases,
        metadata={"task_signature": "other_scheduler"},
    )
    existing.phases = existing_phases
    mock_schedule_retrieve.return_value = existing

    out = StringIO()
    call_command(
        "apply_subscription_schedules",
        "--target-plan=users-pr-inappy",
        "--end-date=2025-12-31",
        owner_ids=str(owner_with_subscription.ownerid),
        stdout=out,
        stderr=StringIO(),
    )

    # we check existing_schedule.modify() instead of stripe.SubscriptionSchedule.modify
    existing.modify.assert_called_once()
    phases = existing.modify.call_args.kwargs["phases"]
    # Original phase(s) + new end phase
    assert len(phases) == 2
    assert phases[0]["start_date"] == 1000000
    assert phases[0]["end_date"] == 2000000
    assert phases[1]["end_date"] == datetime.fromisoformat("2025-12-31")
    assert phases[1]["items"][0]["plan"] == target_plan.stripe_id
    assert phases[1]["items"][0]["quantity"] == 3
    assert (
        existing.modify.call_args.kwargs["metadata"]["task_signature"] == "cancel_task"
    )
    assert "Added schedule phase" in out.getvalue()


@pytest.mark.django_db
@patch("stripe.SubscriptionSchedule.modify")
@patch("stripe.SubscriptionSchedule.create")
@patch("stripe.Subscription.retrieve")
def test_apply_subscription_schedules_uses_current_plan_when_target_plan_omitted(
    mock_sub_retrieve,
    mock_schedule_create,
    mock_schedule_modify,
    owner_with_subscription,
    current_plan_in_stripe,
):
    """When --target-plan is omitted, command uses subscription's current plan (looked up by Stripe price ID)."""
    mock_sub_retrieve.return_value = _make_mock_subscription(
        plan_id="price_current_456",
        quantity=5,
        current_period_start=1000000,
        current_period_end=2000000,
        schedule=None,
    )
    mock_schedule_create.return_value = MagicMock(id="sub_sched_new")

    out = StringIO()
    call_command(
        "apply_subscription_schedules",
        "--end-date=2025-12-31",
        "--owner-ids",
        str(owner_with_subscription.ownerid),
        "--limit=10",
        stdout=out,
        stderr=StringIO(),
    )

    mock_schedule_create.assert_called_once()
    modify_kw = mock_schedule_modify.call_args.kwargs
    assert (
        modify_kw["phases"][1]["items"][0]["plan"] == current_plan_in_stripe.stripe_id
    )
    assert "Created schedule" in out.getvalue()
    assert (
        "use current plan" in out.getvalue().lower()
        or "Target plan: (use current" in out.getvalue()
    )


@pytest.mark.django_db
@patch("stripe.Subscription.retrieve")
def test_apply_subscription_schedules_skips_when_no_plan_for_stripe_price(
    mock_sub_retrieve,
    owner_with_subscription,
):
    """When --target-plan is omitted and no Plan exists for subscription's Stripe price, command skips with skipped_no_plan."""
    # Mock subscription has a price ID that does not match any Plan in DB
    mock_sub_retrieve.return_value = _make_mock_subscription(
        plan_id="price_unknown_nonexistent",
        schedule=None,
    )

    out = StringIO()
    call_command(
        "apply_subscription_schedules",
        "--end-date=2025-12-31",
        "--owner-ids",
        str(owner_with_subscription.ownerid),
        stdout=out,
        stderr=StringIO(),
    )

    assert "SKIPPED" in out.getvalue()
    assert "No Plan found" in out.getvalue()
    assert "price_unknown_nonexistent" in out.getvalue()
    assert "Skipped (no Plan for Stripe price)" in out.getvalue()
