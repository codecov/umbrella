import logging
import traceback
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    insert,
    inspect,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import InstanceState

from shared.plan.constants import DEFAULT_FREE_PLAN

log = logging.getLogger(__name__)

# Fields whose change is itself a plan/trial/billing change.
OWNER_PLAN_FIELDS = (
    "plan",
    "plan_user_count",
    "plan_auto_activate",
    "plan_provider",
    "free",
    "did_trial",
    "trial_start_date",
    "trial_end_date",
    "trial_status",
    "trial_fired_by",
    "pretrial_users_count",
    "stripe_customer_id",
    "stripe_subscription_id",
    "stripe_coupon_id",
    "delinquent",
    "uses_invoice",
    "invoice_details",
)

ACCOUNT_PLAN_FIELDS = (
    "plan",
    "plan_seat_count",
    "free_seat_count",
    "plan_auto_activate",
    "is_delinquent",
)

# Stored on every owner row so one record is enough to restore state.
OWNER_SNAPSHOT_FIELDS = OWNER_PLAN_FIELDS + (
    "plan_activated_users",
    "service",
    "username",
    "service_id",
    "account_id",
)

ACCOUNT_SNAPSHOT_FIELDS = ACCOUNT_PLAN_FIELDS + ("name", "sentry_org_id", "is_active")

_ACTOR: ContextVar[dict[str, Any] | None] = ContextVar(
    "plan_change_actor", default=None
)

# Raw table so the worker can insert on the SQLAlchemy connection that is
# already flushing the plan change. Django owns the schema.
PLAN_CHANGE_TABLE = Table(
    "codecov_auth_planchange",
    MetaData(),
    Column("id", BigInteger, primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("updated_at", DateTime(timezone=True), nullable=False),
    Column("entity", String(16), nullable=False),
    Column("owner_id", Integer),
    Column("account_id", BigInteger),
    Column("service", Text),
    Column("username", Text),
    Column("old_plan", Text),
    Column("new_plan", Text),
    Column("changes", JSONB, nullable=False),
    Column("snapshot", JSONB, nullable=False),
    Column("actor", JSONB, nullable=False),
    Column("source", Text),
    Column("caller", JSONB, nullable=False),
)

_MAX_TEXT = 500
_MAX_LIST = 100


def jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return int(value) if value == int(value) else float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return jsonable(value.value)
    if isinstance(value, str):
        if len(value) > _MAX_TEXT:
            return value[:_MAX_TEXT] + "...[truncated]"
        return value
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        items = [jsonable(item) for item in value[:_MAX_LIST]]
        if len(value) > _MAX_LIST:
            items.append(f"...[{len(value) - _MAX_LIST} more]")
        return items
    if isinstance(value, int | float | bool) or value is None:
        return value
    return str(value)


@contextmanager
def plan_change_context(**actor: Any):
    """Merge actor/source details onto plan-change rows written in this block."""
    current = dict(_ACTOR.get() or {})
    for key, value in actor.items():
        if value is not None:
            current[key] = jsonable(value)
    token = _ACTOR.set(current)
    try:
        yield
    finally:
        _ACTOR.reset(token)


def remember_plan_change_actor(instance: Any, **actor: Any) -> None:
    """Stash actor details on a SQLAlchemy instance until the session flushes.

    Worker tasks commit after ``run_impl`` returns, so a contextvar set only
    inside the task is gone by the time the ``after_update`` listener runs.
    """
    state: InstanceState = inspect(instance)
    stored = dict(state.info.get("plan_change_actor") or {})
    stored.update(_ACTOR.get() or {})
    for key, value in actor.items():
        if value is not None:
            stored[key] = jsonable(value)
    state.info["plan_change_actor"] = stored


def record_plan_change(
    *,
    entity: str,
    entity_id: int | None,
    old_plan: Any,
    new_plan: Any,
    changes: dict[str, dict[str, Any]],
    snapshot: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
    bind: Any = None,
) -> None:
    """Insert one plan-change row. Failures are logged and swallowed.

    ``bind`` is a SQLAlchemy session or connection. Pass it from the worker so
    the history row commits or rolls back with the plan update.
    """
    if not changes:
        return
    try:
        row = build_plan_change_row(
            entity=entity,
            entity_id=entity_id,
            old_plan=old_plan,
            new_plan=new_plan,
            changes=changes,
            snapshot=snapshot,
            extra=extra,
        )
        _persist_rows([row], bind=bind)
    except Exception:
        log.exception(
            "Failed to record plan change",
            extra={"entity": entity, "entity_id": entity_id},
        )


def collect_model_plan_change(
    instance: Any,
    *,
    entity: str,
    trigger_fields: Iterable[str],
    snapshot_fields: Iterable[str],
    update_fields: Iterable[str] | None = None,
) -> dict[str, Any] | None:
    """Diff a Django model against its tracked values. Call before ``save``."""
    adding = not instance.pk
    if adding and not _create_is_plan_event(instance):
        return None

    persisted = None if update_fields is None else set(update_fields)
    changes: dict[str, dict[str, Any]] = {}
    for field in trigger_fields:
        if persisted is not None and field not in persisted:
            continue
        new_value = getattr(instance, field)
        if adding:
            if new_value is None:
                continue
            changes[field] = {"old": None, "new": jsonable(new_value)}
            continue
        if not instance.tracker.has_changed(field):
            continue
        changes[field] = {
            "old": jsonable(instance.tracker.previous(field)),
            "new": jsonable(new_value),
        }
    if not changes:
        return None

    snapshot = {field: getattr(instance, field, None) for field in snapshot_fields}
    old_plan = changes.get("plan", {}).get("old", snapshot.get("plan"))
    new_plan = changes.get("plan", {}).get("new", snapshot.get("plan"))
    return {
        "entity": entity,
        "entity_id": instance.pk,
        "old_plan": old_plan,
        "new_plan": new_plan,
        "changes": changes,
        "snapshot": snapshot,
    }


def emit_collected_plan_change(instance: Any, pending: dict[str, Any] | None) -> None:
    if not pending:
        return
    pending["entity_id"] = pending.get("entity_id") or getattr(instance, "pk", None)
    record_plan_change(**pending)


def record_owner_bulk_update(
    rows: Iterable[Mapping[str, Any]], updates: Mapping[str, Any]
) -> None:
    """Record a ``QuerySet.update`` that bypasses ``Owner.save``."""
    try:
        pending = []
        for row in rows:
            changes = {}
            for field, new_value in updates.items():
                old_value = row.get(field)
                if old_value != new_value:
                    changes[field] = {
                        "old": jsonable(old_value),
                        "new": jsonable(new_value),
                    }
            if not changes:
                continue
            pending.append(
                build_plan_change_row(
                    entity="owner",
                    entity_id=row.get("ownerid"),
                    old_plan=row.get("plan"),
                    new_plan=updates.get("plan", row.get("plan")),
                    changes=changes,
                    snapshot=dict(row),
                )
            )
        if pending:
            _persist_rows(pending)
    except Exception:
        log.exception("Failed to record bulk plan change")


def build_plan_change_row(
    *,
    entity: str,
    entity_id: int | None,
    old_plan: Any,
    new_plan: Any,
    changes: dict[str, dict[str, Any]],
    snapshot: dict[str, Any] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    actor = dict(_ACTOR.get() or {})
    if extra:
        remembered = extra.get("actor")
        if isinstance(remembered, dict):
            actor.update(jsonable(remembered))
        for key, value in extra.items():
            if key != "actor" and value is not None:
                actor.setdefault(key, jsonable(value))
    stored_snapshot = jsonable(snapshot or {})
    now = datetime.now(UTC)
    return {
        "created_at": now,
        "updated_at": now,
        "entity": entity,
        "owner_id": entity_id if entity == "owner" else None,
        "account_id": entity_id if entity == "account" else None,
        "service": stored_snapshot.get("service"),
        "username": stored_snapshot.get("username") or stored_snapshot.get("name"),
        "old_plan": jsonable(old_plan),
        "new_plan": jsonable(new_plan),
        "changes": jsonable(changes),
        "snapshot": stored_snapshot,
        "actor": actor,
        "source": actor.get("source"),
        "caller": _caller_stack(),
    }


def _persist_rows(rows: list[dict[str, Any]], bind: Any = None) -> None:
    if bind is not None:
        bind.execute(insert(PLAN_CHANGE_TABLE), rows)
        return
    # Imported lazily: codecov_auth.models imports this module at load time.
    from shared.django_apps.codecov_auth.models import PlanChange  # noqa: PLC0415

    PlanChange.objects.bulk_create([PlanChange(**row) for row in rows])


def _create_is_plan_event(instance: Any) -> bool:
    plan = getattr(instance, "plan", None)
    if plan and plan != DEFAULT_FREE_PLAN:
        return True
    if getattr(instance, "plan_provider", None):
        return True
    if getattr(instance, "stripe_subscription_id", None):
        return True
    if getattr(instance, "uses_invoice", False):
        return True
    trial_status = getattr(instance, "trial_status", None)
    return bool(trial_status and trial_status != "not_started")


def _caller_stack() -> list[str]:
    frames: list[str] = []
    for frame in traceback.extract_stack():
        filename = frame.filename.replace("\\", "/")
        if filename.endswith("/plan/change_log.py"):
            continue
        if (
            "/django/" in filename
            or "/logging/" in filename
            or "/contextlib.py" in filename
        ):
            continue
        frames.append(f"{filename}:{frame.lineno} {frame.name}")
    return frames[-8:]
