from types import SimpleNamespace

from shared.plan.change_log import (
    build_plan_change_row,
    collect_model_plan_change,
    plan_change_context,
    record_owner_bulk_update,
    record_plan_change,
)
from shared.plan.constants import DEFAULT_FREE_PLAN


class _Tracker:
    def __init__(self, previous):
        self._previous = previous

    def has_changed(self, field):
        return field in self._previous

    def previous(self, field):
        return self._previous[field]


def test_build_plan_change_row_keeps_old_plan_new_plan_and_actor():
    with plan_change_context(
        source="start_trial",
        actor_owner_id=15,
        actor_username="ada",
        actor_email="ada@codecov.io",
    ):
        row = build_plan_change_row(
            entity="owner",
            entity_id=42,
            old_plan="users-developer",
            new_plan="users-trial",
            changes={
                "plan": {"old": "users-developer", "new": "users-trial"},
                "plan_user_count": {"old": 1, "new": 1000},
                "trial_status": {"old": "not_started", "new": "ongoing"},
            },
            snapshot={
                "service": "github",
                "username": "codecov",
                "plan": "users-trial",
                "plan_user_count": 1000,
                "stripe_subscription_id": "sub_123",
                "trial_fired_by": 15,
            },
        )

    assert row["entity"] == "owner"
    assert row["owner_id"] == 42
    assert row["account_id"] is None
    assert row["old_plan"] == "users-developer"
    assert row["new_plan"] == "users-trial"
    assert row["service"] == "github"
    assert row["username"] == "codecov"
    assert row["source"] == "start_trial"
    assert row["actor"]["actor_owner_id"] == 15
    assert row["actor"]["actor_email"] == "ada@codecov.io"
    assert row["changes"]["plan_user_count"] == {"old": 1, "new": 1000}
    assert row["snapshot"]["stripe_subscription_id"] == "sub_123"
    assert row["snapshot"]["trial_fired_by"] == 15
    assert row["caller"]


def test_collect_model_plan_change_diffs_tracked_fields():
    owner = SimpleNamespace(
        pk=7,
        plan="users-pr-inappm",
        plan_user_count=9,
        service="github",
        username="acme",
        tracker=_Tracker({"plan": "users-developer", "plan_user_count": 1}),
    )

    pending = collect_model_plan_change(
        owner,
        entity="owner",
        trigger_fields=("plan", "plan_user_count"),
        snapshot_fields=("plan", "plan_user_count", "service", "username"),
    )

    assert pending["old_plan"] == "users-developer"
    assert pending["new_plan"] == "users-pr-inappm"
    assert pending["changes"]["plan_user_count"]["old"] == 1
    assert pending["snapshot"]["username"] == "acme"


def test_collect_skips_unchanged_and_unpersisted_fields():
    owner = SimpleNamespace(
        pk=7,
        plan="users-pr-inappm",
        plan_user_count=9,
        tracker=_Tracker({"plan": "users-developer", "plan_user_count": 1}),
    )

    pending = collect_model_plan_change(
        owner,
        entity="owner",
        trigger_fields=("plan", "plan_user_count"),
        snapshot_fields=("plan",),
        update_fields=["plan_user_count"],
    )

    assert "plan" not in pending["changes"]
    assert pending["old_plan"] == "users-pr-inappm"
    assert pending["changes"]["plan_user_count"] == {"old": 1, "new": 9}


def test_record_plan_change_inserts_via_django(mocker):
    bulk_create = mocker.patch(
        "shared.django_apps.codecov_auth.models.PlanChange.objects.bulk_create"
    )

    with plan_change_context(source="stripe_webhook", stripe_obo_owner_id=9):
        record_plan_change(
            entity="owner",
            entity_id=3,
            old_plan="users-trial",
            new_plan="users-pr-inappy",
            changes={"plan": {"old": "users-trial", "new": "users-pr-inappy"}},
            snapshot={
                "username": "org",
                "service": "github",
                "plan": "users-pr-inappy",
            },
        )

    bulk_create.assert_called_once()
    row = bulk_create.call_args.args[0][0]
    assert row.old_plan == "users-trial"
    assert row.new_plan == "users-pr-inappy"
    assert row.owner_id == 3
    assert row.source == "stripe_webhook"
    assert row.actor["stripe_obo_owner_id"] == 9
    assert row.username == "org"


def test_record_owner_bulk_update_skips_unchanged_rows(mocker):
    bulk_create = mocker.patch(
        "shared.django_apps.codecov_auth.models.PlanChange.objects.bulk_create"
    )
    record_owner_bulk_update(
        [
            {"ownerid": 1, "plan": "users-developer", "delinquent": False},
            {"ownerid": 2, "plan": "users-pr-inappm", "delinquent": True},
        ],
        {"delinquent": True},
    )

    bulk_create.assert_called_once()
    rows = bulk_create.call_args.args[0]
    assert len(rows) == 1
    assert rows[0].owner_id == 1
    assert rows[0].old_plan == "users-developer"
    assert rows[0].changes["delinquent"] == {"old": False, "new": True}


def test_new_default_plan_is_not_recorded():
    owner = SimpleNamespace(
        pk=None,
        plan=DEFAULT_FREE_PLAN,
        plan_provider=None,
        stripe_subscription_id=None,
        uses_invoice=False,
        trial_status="not_started",
    )
    assert (
        collect_model_plan_change(
            owner,
            entity="owner",
            trigger_fields=("plan",),
            snapshot_fields=("plan",),
        )
        is None
    )
