import logging

from django.conf import settings
from django.db.models import QuerySet

from codecov_auth.models import Owner, Plan
from shared.plan.constants import TierName

log = logging.getLogger(__name__)


def on_enterprise_plan(owner: Owner) -> bool:
    plan = Plan.objects.select_related("tier").get(name=owner.plan)
    return settings.IS_ENTERPRISE or (plan.tier.tier_name == TierName.ENTERPRISE.value)


def get_admins_for_owners(owners: QuerySet[Owner]) -> list[Owner]:
    owner_ids: set[int] = set()
    for owner in owners:
        owner_ids.add(owner.ownerid)
        owner_ids.update(owner.admins)

    if not owner_ids:
        return []

    owners_qs = Owner.objects.filter(ownerid__in=owner_ids)
    return list(owners_qs)
