"""Mutation services for `redis_admin` (M5).

Everything that *changes* Redis from the admin UI funnels through
`redis_delete()`. The admin's `ModelAdmin.delete_queryset` /
`delete_model` overrides are thin wrappers around this so we have a
single chokepoint for:

- Family-aware command dispatch (`DEL` for top-level keys, `LREM` /
  `SREM` / `HDEL` for individual list/set/hash items).
- Locks safety: any key whose family has `is_deletable=False` is
  refused, even when the queryset somehow surfaces it.
- Pipeline batching: `DELETE_BATCH_SIZE` ops per pipeline so a single
  delete action doesn't block Redis with one giant MULTI.
- Audit logging: every call (dry-run included) writes a
  `django.contrib.admin.LogEntry` so operators can reconstruct who
  cleared what and when.
- Dry-run mode: returns the same `DeleteResult` shape but performs
  zero mutations, used by the admin's "Dry-run" action to preview
  scope before committing.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

import sentry_sdk
from django.contrib.admin.models import DELETION, LogEntry
from django.contrib.contenttypes.models import ContentType

from . import conn as _conn
from . import settings as redis_admin_settings
from .families import ConnectionKind, find_family
from .models import RedisLock, RedisQueue, RedisQueueItem

log = logging.getLogger(__name__)

# Hard sample cap on what we record in the LogEntry change_message;
# operators just need a few representative names, not the whole list.
_AUDIT_SAMPLE_SIZE = 25


@dataclass(frozen=True)
class DeleteResult:
    """Outcome of a single `redis_delete()` call."""

    count: int
    sample: tuple[str, ...]
    families: tuple[str, ...]
    dry_run: bool
    refused: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class _PendingItem:
    """Internal: a single item-level delete (LREM/SREM/HDEL) request."""

    queue_name: str
    family: str
    redis_type: str
    selector: str  # raw value (LIST/SET) or field name (HASH)


def _classify_queues(
    queues: Iterable[RedisQueue],
) -> tuple[list[str], list[str], list[str]]:
    """Bucket `RedisQueue` rows into (deletable_keys, refused_keys, families)."""

    deletable: list[str] = []
    refused: list[str] = []
    families: set[str] = set()
    for queue in queues:
        family = find_family(queue.name)
        if family is None:
            refused.append(queue.name)
            continue
        if not family.is_deletable:
            refused.append(queue.name)
            continue
        deletable.append(queue.name)
        families.add(family.name)
    return deletable, refused, sorted(families)


def _classify_items(
    items: Iterable[RedisQueueItem],
) -> tuple[list[_PendingItem], list[str], list[str]]:
    """Bucket `RedisQueueItem` rows into (pending_ops, refused_keys, families)."""

    pending: list[_PendingItem] = []
    refused: list[str] = []
    families: set[str] = set()
    for item in items:
        family = find_family(item.queue_name)
        if family is None or not family.is_deletable:
            refused.append(f"{item.queue_name}#{item.index_or_field}")
            continue
        # Families with a `decode_value` transformer (today: only
        # `celery_broker`) round-trip the raw Redis bytes through a
        # display-decorator — `_celery_decode_value` prepends a
        # `[task=… repoid=… commit=…]` prefix so backed-up celery
        # queues are triagable from the admin. That same prefix
        # would land in `LREM`/`SREM` selectors and silently match
        # zero rows in Redis. We intentionally don't track an
        # un-decorated copy on the model (it'd defeat the
        # truncation budget for big payloads), so refuse item-
        # level deletion for these families: operators clear the
        # whole queue from `RedisQueueAdmin` instead. (LREM-by-
        # index isn't a Redis primitive; the only ways to drop a
        # single celery message are LREM by exact value or DEL the
        # whole queue.) HASH-typed families with `decode_value` are
        # safe — HDEL keys off `index_or_field`, not the value —
        # but no current family hits that combination.
        if family.redis_type in ("list", "set") and family.decode_value is not None:
            refused.append(f"{item.queue_name}#{item.index_or_field}")
            continue
        # The HASH selector is the field name; SET selector is the
        # member value; LIST uses the raw value (not the index) since
        # LREM is value-based — admin pagination already gave us the
        # decoded value as `raw_value`.
        if family.redis_type == "hash":
            selector = item.index_or_field
        else:
            selector = item.raw_value or ""
        pending.append(
            _PendingItem(
                queue_name=item.queue_name,
                family=family.name,
                redis_type=family.redis_type,
                selector=selector,
            )
        )
        families.add(family.name)
    return pending, refused, sorted(families)


def _batched(iterable: Sequence, size: int) -> Iterable[Sequence]:
    for i in range(0, len(iterable), size):
        yield iterable[i : i + size]


def _execute_key_deletes(redis, keys: Sequence[str]) -> int:
    """Run `DEL` for every key in `keys`, batched via pipelines.

    Returns the number of keys that existed and were deleted (sum of
    pipeline replies, since `DEL` returns 0 for missing keys).
    """

    if not keys:
        return 0
    deleted = 0
    batch_size = redis_admin_settings.DELETE_BATCH_SIZE
    for batch in _batched(list(keys), batch_size):
        pipe = redis.pipeline(transaction=False)
        for key in batch:
            pipe.delete(key)
        for reply in pipe.execute():
            try:
                deleted += int(reply or 0)
            except (TypeError, ValueError):
                pass
    return deleted


def _execute_item_deletes(redis, items: Sequence[_PendingItem]) -> int:
    """Run LREM/SREM/HDEL for every pending item, batched via pipelines."""

    if not items:
        return 0
    deleted = 0
    batch_size = redis_admin_settings.DELETE_BATCH_SIZE
    for batch in _batched(list(items), batch_size):
        pipe = redis.pipeline(transaction=False)
        for item in batch:
            if item.redis_type == "list":
                # `count=0` removes all matching values from the list,
                # which mirrors what an operator would expect when
                # picking a row out of the items changelist.
                pipe.lrem(item.queue_name, 0, item.selector)
            elif item.redis_type == "set":
                pipe.srem(item.queue_name, item.selector)
            elif item.redis_type == "hash":
                pipe.hdel(item.queue_name, item.selector)
            else:
                # Strings have no item-level delete; for STRING families
                # the queue-level DEL path handles removal. Refuse here
                # rather than silently no-op.
                log.warning(
                    "redis_admin: refusing item delete on STRING family %s key %s",
                    item.family,
                    item.queue_name,
                )
                pipe.echo("noop")
        for reply in pipe.execute():
            try:
                deleted += int(reply or 0)
            except (TypeError, ValueError):
                pass
    return deleted


def _record_audit(
    *,
    user,
    scope: str,
    count: int,
    refused: Sequence[str],
    sample: Sequence[str],
    families: Sequence[str],
    dry_run: bool,
) -> None:
    """Write a `LogEntry` row so operators can reconstruct what cleared what.

    Best-effort: a logging failure must never roll back the actual
    delete. The change_message is JSON for grep-ability.
    """

    try:
        ct = ContentType.objects.get_for_model(RedisQueue)
        message = json.dumps(
            {
                "scope": scope,
                "dry_run": dry_run,
                "count": count,
                "families": list(families),
                "sample": list(sample),
                "refused": list(refused)[:_AUDIT_SAMPLE_SIZE],
            }
        )
        user_id = getattr(user, "id", None) or getattr(user, "pk", None)
        if user_id is None:
            log.warning("redis_admin: skipping audit log; no user supplied")
            return
        LogEntry.objects.log_action(
            user_id=user_id,
            content_type_id=ct.id,
            object_id="",
            object_repr=f"redis_admin.{scope}",
            action_flag=DELETION,
            change_message=message,
        )
    except Exception:  # pragma: no cover - audit must never crash delete
        log.exception("redis_admin: failed to write audit LogEntry")


def redis_delete(
    targets: Iterable[RedisQueue | RedisQueueItem],
    *,
    user,
    dry_run: bool = False,
) -> DeleteResult:
    """Delete every item in `targets` from Redis, with safety + audit.

    `targets` may be a mix of `RedisQueue` (top-level DEL) and
    `RedisQueueItem` (LREM/SREM/HDEL) — the family registry decides
    which command to issue. Lock-flagged families (`is_deletable=False`)
    are refused regardless of how they were surfaced.

    `user` is the Django user driving the action; used to attribute the
    `LogEntry`. `dry_run=True` returns the same shape but issues no
    mutations.
    """

    span_name = "dry_run" if dry_run else "execute"
    with sentry_sdk.start_span(op="redis.admin.delete", name=span_name) as span:
        try:
            span.set_tag("redis_admin.dry_run", dry_run)
        except AttributeError:  # pragma: no cover - older sdks
            pass
        return _redis_delete(targets, user=user, dry_run=dry_run, span=span)


def _redis_delete(
    targets: Iterable[RedisQueue | RedisQueueItem],
    *,
    user,
    dry_run: bool,
    span,
) -> DeleteResult:
    queue_targets: list[RedisQueue] = []
    item_targets: list[RedisQueueItem] = []
    for target in targets:
        if isinstance(target, RedisQueueItem):
            item_targets.append(target)
        elif isinstance(target, RedisQueue | RedisLock):
            # `RedisLock` reaches us only via the URL-tampering safety
            # net described above; the family registry will refuse it
            # in `_classify_queues` because its family has
            # `is_deletable=False`. Cast to `RedisQueue` for the rest
            # of the pipeline since the field shape matches.
            queue_targets.append(target)
        else:
            log.warning(
                "redis_admin.redis_delete: ignoring unknown target type %r",
                type(target),
            )

    deletable_keys, refused_keys, queue_families = _classify_queues(queue_targets)
    pending_items, refused_items, item_families = _classify_items(item_targets)

    refused = tuple(refused_keys + refused_items)
    families = tuple(sorted(set(queue_families) | set(item_families)))

    sample_source = list(deletable_keys) + [
        f"{p.queue_name}#{p.selector}" for p in pending_items
    ]
    sample = tuple(sample_source[:_AUDIT_SAMPLE_SIZE])

    if dry_run:
        result = DeleteResult(
            count=len(deletable_keys) + len(pending_items),
            sample=sample,
            families=families,
            dry_run=True,
            refused=refused,
        )
    else:
        # Bucket deletes by the family's connection kind so each pipeline
        # is dispatched against the Redis instance that actually owns
        # those keys. In production, `celery_broker` lives on a separate
        # Memorystore from everything else (see `redis_admin.conn`); a
        # mixed selection (e.g. an admin "clear-by-scope" sweep across
        # `uploads` + `celery_broker`) needs both clients to fully apply.
        clients: dict[ConnectionKind, Any] = {}

        def _client_for(kind: ConnectionKind) -> Any:
            if kind not in clients:
                clients[kind] = _conn.get_connection(kind=kind)
            return clients[kind]

        keys_by_kind: dict[ConnectionKind, list[str]] = {}
        for name in deletable_keys:
            family = find_family(name)
            key_kind: ConnectionKind = (
                family.connection_kind if family is not None else "default"
            )
            keys_by_kind.setdefault(key_kind, []).append(name)

        items_by_kind: dict[ConnectionKind, list[_PendingItem]] = {}
        for pending in pending_items:
            family = find_family(pending.queue_name)
            item_kind: ConnectionKind = (
                family.connection_kind if family is not None else "default"
            )
            items_by_kind.setdefault(item_kind, []).append(pending)

        keys_deleted = sum(
            _execute_key_deletes(_client_for(kind), kind_keys)
            for kind, kind_keys in keys_by_kind.items()
        )
        items_deleted = sum(
            _execute_item_deletes(_client_for(kind), kind_items)
            for kind, kind_items in items_by_kind.items()
        )
        result = DeleteResult(
            count=keys_deleted + items_deleted,
            sample=sample,
            families=families,
            dry_run=False,
            refused=refused,
        )

    scope = (
        "queue+item"
        if queue_targets and item_targets
        else ("queue" if queue_targets else "item")
    )
    _record_audit(
        user=user,
        scope=scope,
        count=result.count,
        refused=refused,
        sample=sample,
        families=families,
        dry_run=dry_run,
    )
    try:
        span.set_data("redis_admin.count", result.count)
        span.set_data("redis_admin.refused_count", len(refused))
        span.set_data("redis_admin.families", list(families))
    except AttributeError:  # pragma: no cover - older sdks
        pass
    return result
