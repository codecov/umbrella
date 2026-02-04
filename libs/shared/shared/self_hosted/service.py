import operator
from functools import reduce

from django.conf import settings
from django.db import transaction
from django.db.models import F, Func, Q, QuerySet

from shared.config import get_config
from shared.django_apps.codecov_auth.models import Owner


def admin_owners() -> QuerySet:
    """
    Returns a queryset of admin owners based on the YAML config:

        setup:
          admins:
            - service: <provider>
              username: <username>
            - ...
    """
    admins = get_config("setup", "admins", default=[])

    filters = [
        Q(service=admin["service"], username=admin["username"])
        for admin in admins
        if "service" in admin and "username" in admin
    ]

    if len(filters) == 0:
        return Owner.objects.none()
    else:
        return Owner.objects.filter(reduce(operator.or_, filters))


def is_admin_owner(owner: Owner | None) -> bool:
    """
    Returns true if the given owner is an admin.
    """
    return owner is not None and admin_owners().filter(pk=owner.pk).exists()


def activated_owners() -> QuerySet:
    """
    Returns all owners that are activated in ANY org's `plan_activated_users`
    across the entire instance.
    """
    owner_ids = (
        Owner.objects.annotate(
            plan_activated_owner_ids=Func(
                F("plan_activated_users"),
                function="unnest",
            )
        )
        .values_list("plan_activated_owner_ids", flat=True)
        .distinct()
    )
    return Owner.objects.filter(pk__in=owner_ids)


def is_activated_owner(owner: Owner) -> bool:
    """
    Returns true if the given owner is activated in this instance.
    """
    return activated_owners().filter(pk=owner.pk).exists()


def enterprise_has_seats_left() -> bool:
    """
    Returns True - enterprise deployments have unlimited seats.
    """
    return True


def can_activate_owner(owner: Owner) -> bool:
    """
    Returns True - enterprise deployments can always activate users.
    """
    return True


@transaction.atomic
def activate_owner(owner: Owner):
    """
    Activate the given owner in ALL orgs that the owner is a part of.
    """
    if not settings.IS_ENTERPRISE:
        raise Exception("activate_owner is only available in self-hosted environments")

    Owner.objects.filter(pk__in=owner.organizations).update(
        plan_activated_users=Func(
            owner.pk,
            function="array_append_unique",
            template="%(function)s(plan_activated_users, %(expressions)s)",
        )
    )


def deactivate_owner(owner: Owner):
    """
    Deactivate the given owner across ALL orgs.
    """
    if not settings.IS_ENTERPRISE:
        raise Exception(
            "deactivate_owner is only available in self-hosted environments"
        )

    Owner.objects.filter(
        plan_activated_users__contains=Func(
            owner.pk,
            function="array",
            template="%(function)s[%(expressions)s]",
        )
    ).update(
        plan_activated_users=Func(
            owner.pk,
            function="array_remove",
            template="%(function)s(plan_activated_users, %(expressions)s)",
        )
    )


def enable_autoactivation():
    """
    Enable auto-activation for the entire instance.

    There's no good place to store this instance-wide so we're just saving this
    for all owners.
    """
    Owner.objects.all().update(plan_auto_activate=True)


def disable_autoactivation():
    """
    Disable auto-activation for the entire instance.

    There's no good place to store this instance-wide so we're just saving this
    for all owners.
    """
    Owner.objects.all().update(plan_auto_activate=False)


def is_autoactivation_enabled():
    """
    Returns true if ANY org has auto-activation enabled.
    """
    return Owner.objects.filter(plan_auto_activate=True).exists()
