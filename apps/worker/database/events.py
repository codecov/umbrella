import json
import logging

from google.cloud import pubsub_v1
from sqlalchemy import event, inspect, select

from database.models.core import Account, Owner, Repository
from helpers.environment import is_enterprise
from shared.config import get_config
from shared.plan.change_log import (
    ACCOUNT_PLAN_FIELDS,
    OWNER_PLAN_FIELDS,
    record_plan_change,
)
from shared.plan.constants import DEFAULT_FREE_PLAN

_pubsub_publisher = None

log = logging.getLogger(__name__)


def _is_shelter_enabled():
    return get_config(
        "setup", "shelter", "enabled", default=False if is_enterprise() else True
    )


def _get_pubsub_publisher():
    global _pubsub_publisher  # noqa: PLW0603
    if not _pubsub_publisher:
        _pubsub_publisher = pubsub_v1.PublisherClient()
    return _pubsub_publisher


def _publish_shelter_sync(sync_type: str, entity_id: int) -> None:
    try:
        pubsub_project_id = get_config("setup", "shelter", "pubsub_project_id")
        pubsub_topic_id = get_config("setup", "shelter", "sync_repo_topic_id")

        if pubsub_project_id and pubsub_topic_id:
            publisher = _get_pubsub_publisher()
            topic_path = publisher.topic_path(pubsub_project_id, pubsub_topic_id)
            publisher.publish(
                topic_path,
                json.dumps(
                    {
                        "type": sync_type,
                        "sync": "one",
                        "id": entity_id,
                    }
                ).encode("utf-8"),
            )
        log.info(
            "Message published for shelter sync",
            extra={"sync_type": sync_type, "entity_id": entity_id},
        )
    except Exception as e:
        log.warning(
            "Failed to publish shelter sync message",
            extra={"sync_type": sync_type, "entity_id": entity_id, "error": e},
        )


def _sync_repo(repository: Repository):
    log.info(f"Signal triggered for repository {repository.repoid}")
    _publish_shelter_sync("repo", repository.repoid)


def _sync_owner(owner: Owner):
    log.info(
        "Signal triggered for owner",
        extra={"ownerid": owner.ownerid},
    )
    _publish_shelter_sync("owner", owner.ownerid)


@event.listens_for(Repository, "after_insert")
def after_insert_repo(mapper, connection, target: Repository):
    if not _is_shelter_enabled():
        log.debug("Shelter is not enabled, skipping after_insert signal")
        return

    # Send to shelter service
    log.info("After insert signal", extra={"repoid": target.repoid})
    _sync_repo(target)


@event.listens_for(Repository, "after_update")
def after_update_repo(mapper, connection, target: Repository):
    if not _is_shelter_enabled():
        log.debug("Shelter is not enabled, skipping after_update signal")
        return

    # Send to shelter service
    state = inspect(target)

    for attr in state.attrs:
        if attr.key in ["name", "upload_token", "ownerid", "private"]:
            history = attr.history
            # Detects if there are changes and if said changes are different.
            # has_changes() is True when you update the an entry with the same value,
            # so we must ensure those values are different to trigger the signal
            if history.has_changes() and history.deleted and history.added:
                old_value = history.deleted[0]
                new_value = history.added[0]
                if old_value != new_value:
                    log.info("After update signal", extra={"repoid": target.repoid})
                    _sync_repo(target)
                    break


def _mapped_history_changes(target, fields: tuple[str, ...]) -> dict:
    state = inspect(target)
    changes = {}
    for field in fields:
        if field not in state.mapper.attrs:
            continue
        history = state.attrs[field].history
        if not history.has_changes():
            continue
        old = history.deleted[0] if history.deleted else None
        new = history.added[0] if history.added else getattr(target, field)
        if old != new:
            changes[field] = {"old": old, "new": new}
    return changes


def _record_orm_plan_change(
    target, entity: str, entity_id: int | None, changes: dict, bind
) -> None:
    if not changes:
        return
    state = inspect(target)
    snapshot = {}
    for field in (*OWNER_PLAN_FIELDS, *ACCOUNT_PLAN_FIELDS, "plan_activated_users"):
        if field in state.mapper.attrs:
            snapshot[field] = getattr(target, field, None)
    record_plan_change(
        entity=entity,
        entity_id=entity_id,
        old_plan=changes.get("plan", {}).get("old", snapshot.get("plan")),
        new_plan=changes.get("plan", {}).get("new", snapshot.get("plan")),
        changes=changes,
        snapshot=snapshot,
        extra={"actor": state.info.get("plan_change_actor")},
        bind=bind,
    )


def _inserted_plan_is_interesting(target) -> bool:
    plan = getattr(target, "plan", None)
    if plan and plan != DEFAULT_FREE_PLAN:
        return True
    if getattr(target, "plan_provider", None):
        return True
    if getattr(target, "stripe_subscription_id", None):
        return True
    trial_status = getattr(target, "trial_status", None)
    return bool(trial_status and trial_status != "not_started")


@event.listens_for(Owner, "after_insert")
def log_owner_plan_insert(mapper, connection, target: Owner):
    if not _inserted_plan_is_interesting(target):
        return
    changes = {}
    state = inspect(target)
    for field in OWNER_PLAN_FIELDS:
        if field not in state.mapper.attrs:
            continue
        value = getattr(target, field, None)
        if value is not None:
            changes[field] = {"old": None, "new": value}
    _record_orm_plan_change(target, "owner", target.ownerid, changes, connection)


@event.listens_for(Owner, "after_update")
def log_owner_plan_update(mapper, connection, target: Owner):
    _record_orm_plan_change(
        target,
        "owner",
        target.ownerid,
        _mapped_history_changes(target, OWNER_PLAN_FIELDS),
        connection,
    )


@event.listens_for(Account, "after_update")
def log_account_plan_update(mapper, connection, target: Account):
    _record_orm_plan_change(
        target,
        "account",
        target.id_,
        _mapped_history_changes(target, ACCOUNT_PLAN_FIELDS),
        connection,
    )


@event.listens_for(Owner, "after_update")
def after_update_owner(mapper, connection, target: Owner):
    if not _is_shelter_enabled():
        log.debug("Shelter is not enabled, skipping after_update signal")
        return

    state = inspect(target)

    for attr in state.attrs:
        if attr.key != "username":
            continue
        history = attr.history
        if history.has_changes() and history.deleted and history.added:
            old_value = history.deleted[0]
            new_value = history.added[0]
            if old_value != new_value:
                log.info(
                    "After owner username update signal",
                    extra={"ownerid": target.ownerid},
                )
                _sync_owner(target)
                repoids = connection.execute(
                    select(Repository.repoid).where(
                        Repository.ownerid == target.ownerid
                    )
                ).scalars()
                for repoid in repoids:
                    _publish_shelter_sync("repo", repoid)
                break
