"""Reconnect (undelete) an obfuscated owner that was queued for deletion.

When an owner is marked for deletion its identity fields are obfuscated and its
``service`` is set to ``to_be_deleted``. If the real user logs back in before the
deletion cron runs, OAuth keys on ``(service, service_id)`` and therefore creates
a brand-new owner instead of matching the obfuscated one. This module merges that
returning owner back into the original: it repoints references, restores the
original owner's identity from the returning owner, and removes the deletion row.
"""

import logging
from dataclasses import dataclass

from django.db import transaction

from codecov_auth.models import Owner
from shared.django_apps.codecov_auth.models import OwnerToBeDeleted

log = logging.getLogger(__name__)

# Identity copied from the returning (source) owner back onto the original.
IDENTITY_FIELDS = (
    "service",
    "service_id",
    "username",
    "name",
    "email",
    "business_email",
    "oauth_token",
    "private_access",
)

# Owner array fields that store ownerids.
OWNER_ID_ARRAY_FIELDS = ("plan_activated_users", "organizations", "admins")


@dataclass
class ReconnectPreview:
    original: Owner
    source: Owner
    identity_changes: list[tuple[str, object, object]]
    related_counts: dict[str, int]


def build_reconnect_preview(original: Owner, source: Owner) -> ReconnectPreview:
    """Describe what a reconnect would change, for a confirmation screen."""
    identity_changes = [
        (field, getattr(original, field), getattr(source, field))
        for field in IDENTITY_FIELDS
    ]

    related_counts: dict[str, int] = {}
    for relation in Owner._meta.related_objects:
        if relation.many_to_many:
            continue
        model = relation.related_model
        field_name = relation.field.name
        count = model.objects.filter(**{field_name: source}).count()
        if count:
            related_counts[f"{model.__name__}.{field_name}"] = count

    return ReconnectPreview(
        original=original,
        source=source,
        identity_changes=identity_changes,
        related_counts=related_counts,
    )


def _repoint_fk_relations(source: Owner, target: Owner) -> None:
    """Move every direct foreign-key reference from ``source`` to ``target``."""
    for relation in Owner._meta.related_objects:
        if relation.many_to_many:
            continue
        model = relation.related_model
        field_name = relation.field.name
        source_rows = model.objects.filter(**{field_name: source})

        if relation.one_to_one:
            # e.g. OwnerProfile: at most one per owner. Keep the original's if it
            # already has one, otherwise adopt the source's.
            if model.objects.filter(**{field_name: target}).exists():
                source_rows.delete()
            else:
                source_rows.update(**{field_name: target})
        else:
            source_rows.update(**{field_name: target})


def _repoint_id_references(source_id: int, target_id: int) -> None:
    """Swap ownerid references stored in Owner array fields."""
    for field in OWNER_ID_ARRAY_FIELDS:
        owners = Owner.objects.select_for_update().filter(
            **{f"{field}__contains": [source_id]}
        )
        for owner in owners:
            array = getattr(owner, field) or []
            swapped = [target_id if value == source_id else value for value in array]
            deduped = list(dict.fromkeys(swapped))
            setattr(owner, field, deduped)
            owner.save(update_fields=[field])

    Owner.objects.filter(trial_fired_by=source_id).update(trial_fired_by=target_id)


@transaction.atomic
def reconnect_owner(
    *, original_ownerid: int, source_ownerid: int, actor_user_id: int | None = None
) -> Owner:
    """Merge the returning ``source`` owner into the ``original`` and undelete it.

    Repoints references from source to original, restores the original owner's
    identity from the source, deletes the (now redundant) source owner, and
    removes the deletion queue row.
    """
    if original_ownerid == source_ownerid:
        raise ValueError("Cannot reconnect an owner to itself")

    original = Owner.objects.select_for_update().get(ownerid=original_ownerid)
    source = Owner.objects.select_for_update().get(ownerid=source_ownerid)

    _repoint_fk_relations(source, original)
    _repoint_id_references(source.ownerid, original.ownerid)

    identity = {field: getattr(source, field) for field in IDENTITY_FIELDS}
    source_user = source.user

    # Delete the source first so it releases the unique (service, service_id),
    # (service, username), external_id and sentry_user_id slots before we
    # restore those values onto the original.
    source.delete()

    for field, value in identity.items():
        setattr(original, field, value)
    if original.user_id is None and source_user is not None:
        original.user = source_user
    original.save()

    OwnerToBeDeleted.objects.filter(owner_id=original.ownerid).delete()

    log.info(
        "Owner reconnected (undeleted) via admin",
        extra={
            "original_ownerid": original.ownerid,
            "source_ownerid": source_ownerid,
            "actor_user_id": actor_user_id,
        },
    )
    return original
