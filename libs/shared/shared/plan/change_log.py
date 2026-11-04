import logging
from collections.abc import Iterable
from typing import Any

from shared.plan.constants import PlanName

log = logging.getLogger(__name__)

PLAN_CHANGE_FIELDS = ("plan", "plan_user_count", "free")
PLAN_CONTEXT_FIELDS = (
    "trial_status",
    "plan_provider",
    "stripe_customer_id",
    "stripe_subscription_id",
)
PLAN_TRACKED_FIELDS = PLAN_CHANGE_FIELDS + PLAN_CONTEXT_FIELDS

GITHUB_PLAN_PROVIDER = "github"


def _is_github_marketplace(values: dict[str, Any]) -> bool:
    # plan_provider is never cleared after a marketplace cancellation, so it's
    # only meaningful while the owner is still on the marketplace plan.
    return (
        values["plan_provider"] == GITHUB_PLAN_PROVIDER
        and values["plan"] == PlanName.GHM_PLAN_NAME.value
    )


def _billing_provider(previous: dict[str, Any], current: dict[str, Any]) -> str | None:
    # The new state decides; the old state only matters for cancellations.
    for values in (current, previous):
        if _is_github_marketplace(values):
            return "github_marketplace"
        if values["stripe_subscription_id"]:
            return "stripe"
    return None


def has_plan_change(changed_fields: Iterable[str]) -> bool:
    return any(field in PLAN_CHANGE_FIELDS for field in changed_fields)


def log_owner_plan_change(
    owner: Any, previous: dict[str, Any], current: dict[str, Any]
) -> None:
    """Log an owner's plan/seat change; `owner` may be a Django or SQLAlchemy Owner."""
    if all(previous[field] == current[field] for field in PLAN_CHANGE_FIELDS):
        return

    billing_provider = _billing_provider(previous, current)

    extra = {
        "ownerid": owner.ownerid,
        "username": owner.username,
        "service": owner.service,
        "service_id": owner.service_id,
        "previous_plan": previous["plan"],
        "new_plan": current["plan"],
        "previous_plan_user_count": previous["plan_user_count"],
        "new_plan_user_count": current["plan_user_count"],
        "previous_free_seats": previous["free"],
        "new_free_seats": current["free"],
        "previous_trial_status": previous["trial_status"],
        "new_trial_status": current["trial_status"],
        "billing_provider": billing_provider,
        "stripe_customer_id": current["stripe_customer_id"]
        or previous["stripe_customer_id"],
        "previous_stripe_subscription_id": previous["stripe_subscription_id"],
        "new_stripe_subscription_id": current["stripe_subscription_id"],
    }
    if billing_provider == "github_marketplace":
        extra["github_marketplace_account_id"] = owner.service_id

    log.info("Owner plan changed", extra=extra)


PLAN_SNAPSHOT_ATTR = "_plan_snapshot"


def update_plan_snapshot(instance: Any, fields: Iterable[str] | None = None) -> None:
    """Record the persisted plan fields of a Django Owner loaded into memory."""
    # Plain dict reads instead of FieldTracker: tracked fields make every
    # attribute read scan the model's deferred fields, which is costly on Owner.
    snapshot = instance.__dict__.setdefault(PLAN_SNAPSHOT_ATTR, {})
    for field in PLAN_TRACKED_FIELDS if fields is None else fields:
        if field in PLAN_TRACKED_FIELDS and field in instance.__dict__:
            snapshot[field] = instance.__dict__[field]


def load_missing_plan_snapshot(
    sender: Any,
    instance: Any,
    raw: bool = False,
    update_fields: Iterable[str] | None = None,
    **kwargs: Any,
) -> None:
    """Fetch persisted values for plan fields assigned without being loaded (e.g. deferred)."""
    if raw or instance.pk is None:
        return
    saved_fields = PLAN_TRACKED_FIELDS if update_fields is None else update_fields
    snapshot = instance.__dict__.setdefault(PLAN_SNAPSHOT_ATTR, {})
    missing = [
        field
        for field in saved_fields
        if field in PLAN_TRACKED_FIELDS
        and field in instance.__dict__
        and field not in snapshot
    ]
    if missing:
        row = sender._base_manager.filter(pk=instance.pk).values(*missing).first()
        snapshot.update(row or {})


def log_django_owner_plan_change(
    sender: Any,
    instance: Any,
    created: bool,
    raw: bool = False,
    update_fields: Iterable[str] | None = None,
    **kwargs: Any,
) -> None:
    saved_fields = PLAN_TRACKED_FIELDS if update_fields is None else update_fields
    snapshot = instance.__dict__.get(PLAN_SNAPSHOT_ATTR, {})
    changed_fields = {
        field
        for field in saved_fields
        if field in snapshot
        and field in instance.__dict__
        and instance.__dict__[field] != snapshot[field]
    }

    if not (created or raw) and has_plan_change(changed_fields):
        previous = {}
        current = {}
        for field in PLAN_TRACKED_FIELDS:
            if field in changed_fields:
                previous[field] = snapshot[field]
                current[field] = instance.__dict__[field]
            else:
                # Unsaved in-memory edits aren't persisted, so report the saved value.
                previous[field] = current[field] = (
                    snapshot[field] if field in snapshot else getattr(instance, field)
                )
        log_owner_plan_change(instance, previous, current)

    update_plan_snapshot(instance, saved_fields)
