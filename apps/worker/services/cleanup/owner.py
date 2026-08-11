import logging

from django.db import transaction
from django.db.models import Q

from services.cleanup.cleanup import cleanup_queryset
from services.cleanup.utils import CleanupSummary, cleanup_context
from shared.config import get_config
from shared.django_apps.codecov_auth.models import Owner, OwnerProfile, OwnerToBeDeleted
from shared.django_apps.core.models import Commit, Pull, Repository

log = logging.getLogger(__name__)

CLEAR_ARRAY_FIELDS = ["plan_activated_users", "organizations", "admins"]


def _get_repo_chunk_size() -> int:
    return get_config("cleanup", "repo_chunk_size", default=10)


def cleanup_owner(owner_id: int) -> CleanupSummary:
    log.info("Started/Continuing Owner cleanup", extra={"ownerid": owner_id})

    clear_owner_references(owner_id)

    # Fetch all repo IDs for this owner upfront so we can process them in
    # small chunks. Without chunking, a large owner (200+ repos, 100k+ commits)
    # causes `simplified_lookup` to fall back to nested subqueries at every
    # level (Commit → CommitReport → ReportSession), producing a 3-level deep
    # subquery that runs for 15+ minutes and gets killed by PostgreSQL.
    repo_ids = list(
        Repository.objects.filter(author_id=owner_id).values_list("repoid", flat=True)
    )
    chunk_size = _get_repo_chunk_size()
    repo_chunks = [repo_ids[i : i + chunk_size] for i in range(0, len(repo_ids), chunk_size)]

    with cleanup_context() as context:
        for idx, chunk in enumerate(repo_chunks):
            log.info(
                "cleanup_owner: processing repo chunk %d/%d",
                idx + 1,
                len(repo_chunks),
                extra={"ownerid": owner_id, "chunk": idx + 1, "total_chunks": len(repo_chunks), "repo_ids": chunk},
            )
            repo_query = Repository.objects.filter(repoid__in=chunk)
            cleanup_queryset(repo_query, context)

        # Clean up the owner record itself after all repos are gone.
        owner_query = Owner.objects.filter(ownerid=owner_id)
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
