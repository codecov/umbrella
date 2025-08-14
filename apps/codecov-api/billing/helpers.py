import logging

from django.conf import settings
from django.db.models import QuerySet

from codecov_auth.models import Owner, Plan
from shared.plan.constants import TierName

log = logging.getLogger(__name__)


def on_enterprise_plan(owner: Owner) -> bool:
    plan = Plan.objects.select_related("tier").get(name=owner.plan)
    return settings.IS_ENTERPRISE or (plan.tier.tier_name == TierName.ENTERPRISE.value)


def update_single_owner_admins(owner: Owner) -> None:
    if not owner.admins:
        return

    admins = Owner.objects.filter(pk__in=owner.admins)
    valid_admin_ids = set()

    for admin in admins:
        if admin.organizations and owner.ownerid in admin.organizations:
            valid_admin_ids.add(admin.ownerid)
        else:
            log.warning(
                "Suppressing billing email to admin not in organization",
                extra={
                    "org_owner_id": owner.ownerid,
                    "admin_owner_id": admin.ownerid,
                },
            )

    if valid_admin_ids != set(owner.admins):
        owner.admins = list(valid_admin_ids)
        owner.save(update_fields=["admins"])


def update_org_admins(owners: QuerySet[Owner]) -> None:
    for owner in owners:
        update_single_owner_admins(owner)


def get_admins_for_owners(owners: QuerySet[Owner]) -> list[Owner]:
    owner_ids: set[int] = set()
    for owner in owners:
        owner_ids.add(owner.ownerid)
        owner_ids.update(owner.admins)

    if not owner_ids:
        return []

    owners_qs = Owner.objects.filter(ownerid__in=owner_ids)
    return list(owners_qs)
