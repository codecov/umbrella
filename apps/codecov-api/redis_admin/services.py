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
import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

import sentry_sdk
from django.contrib.admin.models import DELETION, LogEntry
from django.contrib.contenttypes.models import ContentType

from . import conn as _conn
from . import settings as redis_admin_settings
from .families import ConnectionKind, find_family, parse_celery_envelope
from .families import _decode as _decode_value
from .models import CeleryBrokerQueue, RedisLock, RedisQueue, RedisQueueItem

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
    extra: dict | None = None,
) -> None:
    """Write a `LogEntry` row so operators can reconstruct what cleared what.

    Best-effort: a logging failure must never roll back the actual
    delete. The change_message is JSON for grep-ability.

    `extra` is merged into the change_message dict so callers can
    record scope-specific fields (e.g. `passes_run`, `total_drifted`
    for the streaming celery clear).
    """

    try:
        ct = ContentType.objects.get_for_model(RedisQueue)
        payload: dict = {
            "scope": scope,
            "dry_run": dry_run,
            "count": count,
            "families": list(families),
            "sample": list(sample),
            "refused": list(refused)[:_AUDIT_SAMPLE_SIZE],
        }
        if extra:
            payload.update(extra)
        message = json.dumps(payload)
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


# ---- Celery broker per-message clear (M6 + streaming) -----------------------
#
# `redis_delete()`'s `_classify_items` deliberately refuses item-level
# deletes for `celery_broker` because the family's `decode_value`
# transformer prepends a `[task=… repoid=… commit=…]` summary and the
# decorated string can't round-trip back to LREM (LREM matches on
# exact bytes). The new `CeleryBrokerQueueAdmin` solves this with the
# canonical LSET-tombstone idiom: replace each target index with a
# unique sentinel value, then LREM the sentinel in one pass. Race-safe
# under concurrent celery consumers since the sentinel only matches
# the slot we replaced — even if a worker BLPOPs from the head between
# our LSET and LREM, the popped (sentinel) message is recognised at
# task-decode time as garbage and we still end up with a consistent
# delete count.
#
# Streaming clear (PR #899):
#
# For 200–500k-deep queues the old eager-materialise-then-LSET path
# had two problems:
#
#   1. Memory: materialising the entire queue into Python before
#      clearing was O(N) in memory.
#   2. Race drift: the index a Celery consumer sees between our
#      `LRANGE` snapshot and our `LSET` call can shift if another
#      consumer BLPOPs from the head. The old code had no verify step.
#
# The new path (see `_streaming_celery_clear`) walks the **entire**
# queue in 10k-row LRANGE chunks every pass — the clear has no
# artificial cap because "Clear all" needs to drain whatever depth
# the queue happens to be at, even past the chart sampler's window
# (`CELERY_BROKER_SCAN_LIMIT`). For each candidate position it does
# a verify-before-LSET (LINDEX to confirm the raw bytes still match
# the snapshot row before writing the tombstone) and a
# repeat-until-stable loop (max 3 passes) to catch messages that
# drifted into our filter window between passes.

# Tombstone prefix is intentionally distinctive so a stray LREM
# attempt on the wrong queue couldn't accidentally match a real
# message. Each call appends a fresh UUID so simultaneous clears
# don't step on each other.
_CELERY_TOMBSTONE_PREFIX = "__redis_admin_celery_tombstone__"

# Chunk size for streaming LRANGE during clear.
_CELERY_CLEAR_CHUNK: int = 10_000

# Maximum passes in the repeat-until-stable loop.
_CELERY_MAX_PASSES: int = 3


def _celery_clear_tombstone() -> str:
    return f"{_CELERY_TOMBSTONE_PREFIX}:{uuid.uuid4().hex}"


@dataclass(frozen=True)
class _StreamingClearStats:
    """Per-run stats from `_streaming_celery_clear`."""

    total_lset: int
    total_drifted: int
    total_found: int
    first_match_index: int | None
    passes_run: int


def _streaming_celery_clear(
    redis,
    queue_name: str,
    filter_tuples: frozenset,
    tombstone: str,
    *,
    keep_one: bool = False,
    dry_run: bool = False,
) -> _StreamingClearStats:
    """Streaming LRANGE+verify-before-LSET clear for `queue_name`.

    Walks the **entire** queue in `_CELERY_CLEAR_CHUNK`-row chunks,
    identifies messages whose parsed `(task_name, repoid, commitid)`
    triple appears in `filter_tuples`, and for each match:

    1. Issues `LINDEX key idx` to re-read the raw bytes at that
       position.
    2. Compares LINDEX bytes to the LRANGE snapshot bytes. If they
       differ (drift — a consumer BLPOPed from the head, shifting
       indexes), the slot is skipped.
    3. Issues `LSET key idx tombstone` on a clean match.

    After each full pass, `LREM key 0 tombstone` sweeps all
    tombstones in one shot.

    The loop repeats up to `_CELERY_MAX_PASSES` times, stopping
    early when a pass finds zero matches (queue drained of matches)
    OR two consecutive passes tombstone the same count (stable
    point reached — pathological persistent-drift signal).

    `keep_one=True` skips the first matching message encountered in
    the first pass and on all subsequent passes, implementing the
    "clear all but first (lowest-index match)" semantic.

    On `dry_run=True` we walk the queue exactly once and accumulate
    `total_found` without issuing any LSET/LREM. The dry-run preview
    only needs the count, not race coverage, so multi-pass would
    just re-scan a quiet queue for no benefit (and on a 500k-deep
    queue that's a real cost).

    Returns a `_StreamingClearStats` carrying `total_lset` (slots
    tombstoned), `total_drifted` (slots skipped due to LRANGE/LINDEX
    drift), `total_found` (every match observed across passes —
    populated for the dry-run preview), and `passes_run`.
    """

    total_lset = 0
    total_drifted = 0
    total_found = 0
    # Tracks the lowest-index match seen across all passes. Pass 1
    # walks the queue in ascending index order, so the first match
    # we see IS the lowest. Used by the clear-by-filter preview to
    # render the "Clear all but first" callout (`kept_index`)
    # without a separate queryset materialisation.
    first_match_index: int | None = None
    prev_lset: int | None = None
    passes_run = 0

    # Track whether we've kept the "first" across all passes so that
    # repeat passes honour the same semantic (first by queue index).
    first_kept = not keep_one  # True means "no need to keep one"

    for pass_num in range(_CELERY_MAX_PASSES):
        passes_run = pass_num + 1
        matches_found = 0
        matches_lset = 0
        matches_drifted = 0
        # Reset first_kept for each pass so each pass independently
        # skips its first match. This ensures the lowest-index
        # surviving message is always kept, even as the queue shifts.
        pass_first_kept = first_kept

        # Walk the entire queue every pass: clearing has to traverse
        # the full depth so a "Clear all" on a 200k+ saturated queue
        # actually drains it. The chart's `_stream_frequency_aggregate`
        # caps at `CELERY_BROKER_SCAN_LIMIT` because it only needs a
        # representative sample to compute bucket proportions; the
        # clear path has no analogous bound — capping it meant that on
        # a queue dominated by one `(task, repoid, commitid)` triple,
        # pass 1 cleared the first `cap` positions, pass 2 cleared
        # another `cap` that shifted in from beyond, and the
        # `prev_lset == matches_lset` plateau exit then declared
        # convergence with most of the queue still in place.
        depth = int(redis.llen(queue_name) or 0)
        for chunk_start in range(0, depth, _CELERY_CLEAR_CHUNK):
            chunk_end = chunk_start + _CELERY_CLEAR_CHUNK - 1
            raw_chunk = redis.lrange(queue_name, chunk_start, chunk_end)
            for offset, raw in enumerate(raw_chunk):
                # Skip already-tombstoned slots from this or a
                # concurrent clear.
                if isinstance(raw, bytes) and raw.startswith(
                    _CELERY_TOMBSTONE_PREFIX.encode()
                ):
                    continue
                idx = chunk_start + offset
                decoded = _decode_value(raw)
                meta = parse_celery_envelope(decoded)
                key = (meta.task, meta.repoid, meta.commitid)
                if key not in filter_tuples:
                    continue
                matches_found += 1
                if first_match_index is None:
                    first_match_index = idx

                # keep_one: skip the first match across all chunks
                # (lowest index in current pass).
                if not pass_first_kept:
                    pass_first_kept = True
                    continue

                if dry_run:
                    continue

                # Verify-before-LSET: re-read the slot via LINDEX
                # and compare raw bytes to our LRANGE snapshot.
                current_raw = redis.lindex(queue_name, idx)
                if current_raw != raw:
                    matches_drifted += 1
                    continue

                try:
                    redis.lset(queue_name, idx, tombstone)
                    matches_lset += 1
                except Exception:
                    # Out-of-range: consumer drained this slot.
                    matches_drifted += 1

        if not dry_run:
            redis.lrem(queue_name, 0, tombstone)

        total_lset += matches_lset
        total_drifted += matches_drifted
        total_found += matches_found

        # Dry-run only needs the count; one full scan is exact for a
        # quiet queue and the streaming-clear semantics for the preview
        # don't need the verify-before-LSET race coverage that justifies
        # multi-pass on the executing path.
        if dry_run:
            break
        # Stability check: stop when no matches or lset count plateaued.
        if matches_found == 0:
            break
        if keep_one and matches_drifted == 0:
            # With keep_one we deliberately leave the first match in
            # place, so the only reason to loop again is to retry
            # drifted slots. When no drift occurred this pass, the
            # queue has converged (non-keeper matches are cleared,
            # keeper survives) — exit early to avoid an extra full
            # LRANGE scan at 500k depth. If drift did occur, fall
            # through to the plateau check so the next pass can
            # retry.
            break
        if prev_lset is not None and matches_lset == prev_lset:
            break
        prev_lset = matches_lset

    return _StreamingClearStats(
        total_lset=total_lset,
        total_drifted=total_drifted,
        total_found=total_found,
        first_match_index=first_match_index,
        passes_run=passes_run,
    )


def streaming_celery_count(
    queue_name: str,
    *,
    task_name: str | None = None,
    repoid: int | None = None,
    commitid: str | None = None,
) -> tuple[int, int | None]:
    """Streaming `(match_count, lowest_index_match)` for the
    `(queue_name, task_name?, repoid?, commitid?)` filter.

    Walks the full `LLEN(queue)` so the returned count agrees
    with what `celery_broker_clear` would actually tombstone.
    Used by `clear_by_filter_view` to render the preview page;
    the queryset path is bounded by `CELERY_BROKER_DISPLAY_LIMIT`
    and would underreport on deep queues.

    The filter compares envelope tuples by exact equality. The
    admin view passes `commitid` straight from the chart row
    (full 40-char hash), so this matches the
    `queryset.filter(commitid__startswith=...)` path the
    changelist uses for full-length input. Partial-prefix
    commits (rare; only reachable via manual URL editing) will
    count as zero — that's a documented limitation; the user
    can still browse via the changelist filters.
    """

    redis = _conn.get_connection(kind="broker")
    filter_tuples = frozenset({(task_name or None, repoid, commitid or None)})
    tombstone = _celery_clear_tombstone()
    stats = _streaming_celery_clear(
        redis,
        queue_name,
        filter_tuples,
        tombstone,
        keep_one=False,
        dry_run=True,
    )
    return stats.total_found, stats.first_match_index


def celery_broker_clear(
    targets: Iterable[CeleryBrokerQueue],
    *,
    user,
    dry_run: bool = False,
    keep_one: bool = False,
) -> DeleteResult:
    """Clear the given celery broker messages with the LSET-tombstone path.

    Parallels `redis_delete()` for the `RedisQueue` / `RedisQueueItem`
    surfaces but is celery-broker-specific: targets must already be
    `CeleryBrokerQueue` rows materialised from
    `CeleryBrokerQueueQuerySet`, which carry the per-queue index we
    need for LSET. Wraps in a sentry span and writes a `LogEntry`
    audit row with `scope="celery_broker_clear"` so the existing
    operator audit trail keeps capturing celery clears alongside
    family-level deletes.

    `keep_one=True` skips the first (lowest-index) match in each
    queue — implements the "clear all but first" action on the
    `clear-by-filter` page.
    """

    span_name = "celery_broker_dry_run" if dry_run else "celery_broker_execute"
    with sentry_sdk.start_span(op="redis.admin.delete", name=span_name) as span:
        try:
            span.set_tag("redis_admin.dry_run", dry_run)
            span.set_tag("redis_admin.scope", "celery_broker_clear")
        except AttributeError:  # pragma: no cover - older sdks
            pass
        return _celery_broker_clear(
            targets, user=user, dry_run=dry_run, keep_one=keep_one, span=span
        )


def _celery_broker_clear(
    targets: Iterable[CeleryBrokerQueue],
    *,
    user,
    dry_run: bool,
    keep_one: bool,
    span,
) -> DeleteResult:
    # Derive per-queue filter tuples from the pre-identified targets.
    # For each queue we collect the set of (task_name, repoid,
    # commitid) triples that the operator wants cleared and the sample
    # tokens for the audit log.
    by_queue_filters: dict[str, set] = {}
    sample_source: list[str] = []

    for target in targets:
        if not isinstance(target, CeleryBrokerQueue):
            log.warning(
                "redis_admin.celery_broker_clear: ignoring non-celery target %r",
                type(target),
            )
            continue
        if not target.queue_name:
            continue
        key = (target.task_name, target.repoid, target.commitid)
        by_queue_filters.setdefault(target.queue_name, set()).add(key)
        sample_source.append(f"{target.queue_name}#{target.index_in_queue}")

    sample = tuple(sample_source[:_AUDIT_SAMPLE_SIZE])
    families = ("celery_broker",) if by_queue_filters else ()

    total_lset = 0
    total_drifted = 0
    total_passes = 0

    if not by_queue_filters:
        # No celery_broker targets at all (operator submitted an empty
        # filter set, or all targets were the wrong polymorphic type).
        result_count = 0
    elif dry_run:
        # Walk the full queue per filter triple so the preview count
        # matches what the real clear would tombstone. Counts only —
        # `_streaming_celery_clear(..., dry_run=True)` skips LSET/LREM.
        # Without this, the preview was reporting `pending_count`
        # (the materialised target list, capped by
        # `CELERY_BROKER_DISPLAY_LIMIT`), so a 200k-deep queue with
        # 190k matches would show ~1.9k in the preview while the
        # actual clear then drained 190k.
        redis = _conn.get_connection(kind="broker")
        total_found = 0
        queues_with_matches = 0
        for queue_name, filter_set in by_queue_filters.items():
            tombstone = _celery_clear_tombstone()
            stats = _streaming_celery_clear(
                redis,
                queue_name,
                frozenset(filter_set),
                tombstone,
                keep_one=keep_one,
                dry_run=True,
            )
            total_found += stats.total_found
            if stats.total_found > 0:
                queues_with_matches += 1
            total_passes += stats.passes_run
        # `keep_one` semantics: the actual clear preserves one
        # message per queue *that has at least one match*. Subtract
        # only for those queues, not for every queue in the filter
        # set -- otherwise a queue that drained to zero matches
        # between the changelist render and this preview (e.g. a
        # consumer popped the last match) would shave one extra
        # off the count, underreporting what the real clear will
        # tombstone. Aligns the preview with the actual execution
        # path of `_streaming_celery_clear`.
        if keep_one and queues_with_matches > 0:
            total_found = max(0, total_found - queues_with_matches)
        result_count = total_found
    else:
        redis = _conn.get_connection(kind="broker")
        for queue_name, filter_set in by_queue_filters.items():
            tombstone = _celery_clear_tombstone()
            stats = _streaming_celery_clear(
                redis,
                queue_name,
                frozenset(filter_set),
                tombstone,
                keep_one=keep_one,
                dry_run=False,
            )
            total_lset += stats.total_lset
            total_drifted += stats.total_drifted
            total_passes += stats.passes_run
        result_count = total_lset

    result = DeleteResult(
        count=result_count,
        sample=sample,
        families=families,
        dry_run=dry_run,
        refused=(),
    )

    mode = (
        "dry-run" if dry_run else ("all-but-first" if keep_one else "all-from-bucket")
    )
    _record_audit(
        user=user,
        scope="celery_broker_clear",
        count=result.count,
        refused=(),
        sample=sample,
        families=families,
        dry_run=dry_run,
        extra={
            "mode": mode,
            "passes_run": total_passes,
            "total_lset": total_lset,
            "total_drifted": total_drifted,
        },
    )
    try:
        span.set_data("redis_admin.count", result.count)
        span.set_data("redis_admin.queues", sorted(by_queue_filters))
        span.set_data("redis_admin.passes_run", total_passes)
        span.set_data("redis_admin.total_drifted", total_drifted)
    except AttributeError:  # pragma: no cover - older sdks
        pass
    return result
