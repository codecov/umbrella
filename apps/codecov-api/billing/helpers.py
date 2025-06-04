from django.conf import settings
from django.db.models import QuerySet

from codecov_auth.models import Owner, Plan
from shared.plan.constants import TierName


def on_enterprise_plan(owner: Owner) -> bool:
    plan = Plan.objects.select_related("tier").get(name=owner.plan)
    return settings.IS_ENTERPRISE or (plan.tier.tier_name == TierName.ENTERPRISE.value)


def get_all_admins_for_owners(owners: QuerySet[Owner]):
    admin_ids = set()
    for owner in owners:
        if owner.admins:
            admin_ids.update(owner.admins)

        # Add the owner's email as well - for user owners, admins is empty.
        if owner.email:
            admin_ids.add(owner.ownerid)

    admins: QuerySet[Owner] = Owner.objects.filter(pk__in=admin_ids)
    return admins
