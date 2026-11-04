import logging
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


def _billing_provider(before: dict[str, Any], current: dict[str, Any]) -> str | None:
    # Marketplace wins: a marketplace purchase cancels any Stripe subscription.
    if _is_github_marketplace(before) or _is_github_marketplace(current):
        return "github_marketplace"
    if before["stripe_subscription_id"] or current["stripe_subscription_id"]:
        return "stripe"
    return None


def log_owner_plan_change(owner: Any, previous: dict[str, Any]) -> None:
    """Log an owner's plan/seat change; `owner` may be a Django or SQLAlchemy Owner."""
    current = {field: getattr(owner, field) for field in PLAN_TRACKED_FIELDS}
    before = {**current, **previous}
    if all(before[field] == current[field] for field in PLAN_CHANGE_FIELDS):
        return

    billing_provider = _billing_provider(before, current)

    extra = {
        "ownerid": owner.ownerid,
        "username": owner.username,
        "service": owner.service,
        "service_id": owner.service_id,
        "previous_plan": before["plan"],
        "new_plan": current["plan"],
        "previous_plan_user_count": before["plan_user_count"],
        "new_plan_user_count": current["plan_user_count"],
        "previous_free_seats": before["free"],
        "new_free_seats": current["free"],
        "previous_trial_status": before["trial_status"],
        "new_trial_status": current["trial_status"],
        "billing_provider": billing_provider,
        "stripe_customer_id": current["stripe_customer_id"]
        or before["stripe_customer_id"],
        "previous_stripe_subscription_id": before["stripe_subscription_id"],
        "new_stripe_subscription_id": current["stripe_subscription_id"],
    }
    if billing_provider == "github_marketplace":
        extra["github_marketplace_account_id"] = owner.service_id

    log.info("Owner plan changed", extra=extra)


def log_django_owner_plan_change(
    sender: Any, instance: Any, created: bool, **kwargs: Any
) -> None:
    if created:
        return
    previous = {
        field: instance.tracker.previous(field)
        for field in PLAN_TRACKED_FIELDS
        if instance.tracker.has_changed(field)
    }
    if previous:
        log_owner_plan_change(instance, previous)
