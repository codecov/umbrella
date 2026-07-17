import logging

from django.db import transaction
from django.db.models import Q

from services.cleanup.cleanup import cleanup_queryset
from services.cleanup.utils import CleanupSummary, cleanup_context
from shared.django_apps.codecov_auth.models import Owner, OwnerProfile, OwnerToBeDeleted
from shared.django_apps.core.models import Commit, Pull, Repository

log = logging.getLogger(__name__)

CLEAR_ARRAY_FIELDS = ["plan_activated_users", "organizations", "admins"]


def cleanup_owner_repo(owner_id: int, repoid: int) -> CleanupSummary:
    """
    Cleans up a single repository belonging to an owner.
    Called once per repo so each invocation stays within the task time limit.
    """
    log.info(
        "Cleaning up repository for owner",
        extra={"ownerid": owner_id, "repoid": repoid},
    )
    repo_query = Repository.objects.filter(repoid=repoid, author_id=owner_id)
    with cleanup_context() as context:
        cleanup_queryset(repo_query, context)
        summary = context.summary
    log.info(
        "Repository cleanup finished",
        extra={"ownerid": owner_id, "repoid": repoid, "summary": summary},
    )
    return summary


def cleanup_owner(owner_id: int, refs_cleared: bool = False) -> CleanupSummary:
    """
    Cleans up an owner.  To avoid hitting the Celery hard-timeout when an owner
    has many repositories, this function only cleans one repository per call.
    The caller (DeleteOwnerTask) is responsible for re-enqueuing itself until
    all repositories are gone, then calling this function a final time with no
    repositories remaining so the owner record itself is removed.
    """
    log.info(
        "Started/Continuing Owner cleanup",
        extra={"ownerid": owner_id, "refs_cleared": refs_cleared},
    )

    if not refs_cleared:
        clear_owner_references(owner_id)

    # Process one repository at a time to stay within the task time limit.
    next_repo = Repository.objects.filter(author_id=owner_id).first()
    if next_repo is not None:
        log.info(
            "Owner still has repositories; cleaning one and rescheduling",
            extra={"ownerid": owner_id, "repoid": next_repo.repoid},
        )
        return cleanup_owner_repo(owner_id, next_repo.repoid)

    # All repositories cleaned — now remove the owner record itself.
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
