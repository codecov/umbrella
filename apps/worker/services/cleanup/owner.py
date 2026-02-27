import logging

from django.db import transaction
from django.db.models import Q

from services.cleanup.cleanup import cleanup_queryset
from services.cleanup.utils import CleanupSummary, cleanup_context
from shared.django_apps.codecov_auth.models import Owner, OwnerProfile, OwnerToBeDeleted
from shared.django_apps.core.models import Commit, Pull, Repository

log = logging.getLogger(__name__)

CLEAR_ARRAY_FIELDS = ["plan_activated_users", "organizations", "admins"]


def cleanup_owner(owner_id: int) -> CleanupSummary:
    log.info("Started/Continuing Owner cleanup", extra={"ownerid": owner_id})

    clear_owner_references(owner_id)
    owner_query = Owner.objects.filter(ownerid=owner_id)
    with cleanup_context() as context:
        cleanup_queryset(owner_query, context)
        summary = context.summary

    OwnerToBeDeleted.objects.filter(owner_id=owner_id).delete()

    log.info("Owner cleanup finished", extra={"ownerid": owner_id, "summary": summary})
    return summary


# TODO: maybe turn this into a `MANUAL_CLEANUP`?
def clear_owner_references(owner_id: int):
    """
    This clears the `ownerid` from various DB arrays where it is being referenced.
    """

    OwnerProfile.objects.filter(default_org=owner_id).update(default_org=None)
    Owner.objects.filter(bot=owner_id).update(bot=None)
    Repository.objects.filter(bot=owner_id).update(bot=None)
    Commit.objects.filter(author=owner_id).update(author=None)
    Pull.objects.filter(author=owner_id).update(author=None)

    # This uses a transaction / `select_for_update` to ensure consistency when
    # modifying these `ArrayField`s in python.
    # I don’t think we have such consistency anyplace else in the codebase, so
    # if this is causing lock contention issues, its also fair to avoid this.
    with transaction.atomic():
        filter = Q()
        for field in CLEAR_ARRAY_FIELDS:
            filter = filter | Q(**{f"{field}__contains": [owner_id]})

        owners_with_reference = Owner.objects.select_for_update().filter(filter)
        for owner in owners_with_reference:
            updated_fields = set()
            for field in CLEAR_ARRAY_FIELDS:
                array = getattr(owner, field)
                if array:
                    updated_fields.add(field)
                    setattr(owner, field, [x for x in array if x != owner_id])

            if updated_fields:
                owner.save(update_fields=updated_fields)
