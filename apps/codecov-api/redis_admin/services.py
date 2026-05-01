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

import datetime as _dt
import json
import logging
import threading
import uuid
from collections.abc import Callable, Iterable, Sequence
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


class _FilterWildcard:
    """Sentinel for "this filter slot is unconstrained".

    Distinct from `None` so the per-message clear paths
    (`clear_selected`, `clear_dry_run`, `delete_model`,
    `delete_queryset`) keep exact-tuple-membership semantics for
    materialised rows whose envelope legitimately carries `None`
    in a slot (e.g. a `sync_repos` task with `repoid=None` and
    `commitid=None`). If `None` doubled as the wildcard value, a
    single-row clear of a `sync_repos` envelope would tombstone
    *every* `sync_repos` message in the queue rather than just
    the one the operator selected.

    Operator-input paths (`streaming_celery_count` and the
    `clear_by_filter_view` synthetic target) substitute this
    sentinel for any unset filter slot before building the
    `(task, repoid, commitid)` tuple.
    """

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return "FILTER_ANY"


_FILTER_ANY = _FilterWildcard()


def _envelope_matches_any_filter(meta, filter_tuples: frozenset) -> bool:
    """Return True if `meta` matches any tuple in `filter_tuples`.

    Each filter tuple is `(task_name, repoid, commitid)` where any
    slot may be the `_FILTER_ANY` sentinel to mean "wildcard / do
    not constrain on this field". `None` in a slot means "match
    only envelopes whose corresponding parsed field is `None`",
    which is what materialised per-message targets need so they
    never over-match envelopes whose envelope happens to carry the
    same task name but a different `(repoid, commitid)` shape.

    Mirrors the queryset's "filter by what's set" semantic only
    for callers that opt in by substituting `_FILTER_ANY` for
    unset slots before building the frozenset.
    """

    for ft_task, ft_repoid, ft_commitid in filter_tuples:
        if ft_task is not _FILTER_ANY and ft_task != meta.task:
            continue
        if ft_repoid is not _FILTER_ANY and ft_repoid != meta.repoid:
            continue
        if ft_commitid is not _FILTER_ANY and ft_commitid != meta.commitid:
            continue
        return True
    return False


@dataclass(frozen=True)
class _StreamingClearStats:
    """Per-run stats from `_streaming_celery_clear`."""

    total_lset: int
    total_drifted: int
    total_found: int
    first_match_index: int | None
    passes_run: int
    # Set by `_streaming_celery_clear` when a `progress_callback`
    # returned `True` to abort. The chunked background-job worker
    # uses this to flip the job hash's `status` to `"cancelled"`.
    # Synchronous callers (`celery_broker_clear`) never pass a
    # callback, so they always see `cancelled=False`.
    cancelled: bool = False


# Per-chunk progress payload handed to the optional
# `progress_callback` in `_streaming_celery_clear`. Kept as a small
# frozen dataclass (rather than a bare dict) so the callback signature
# is self-documenting and stable across the in-flight chunked-clear
# worker, future chunked callers, and any progress-snapshot test
# fixtures.
@dataclass(frozen=True)
class _ChunkProgress:
    pass_num: int  # 1-based pass index
    chunk_index: int  # 0-based chunk index within the current pass
    chunks_total: int  # number of chunks in the current pass
    processed_delta: int  # messages walked in this chunk (LRANGE size)
    matched_delta: int  # matches LSET-tombstoned in this chunk
    drifted_delta: int  # LRANGE/LINDEX drifts skipped in this chunk
    found_delta: int  # all matches observed in this chunk (incl. drift)
    total_lset: int  # cumulative across all passes/chunks so far
    total_drifted: int
    total_found: int


def _streaming_celery_clear(
    redis,
    queue_name: str,
    filter_tuples: frozenset,
    tombstone: str,
    *,
    keep_one: bool = False,
    dry_run: bool = False,
    chunk_size: int | None = None,
    progress_callback: Callable[[_ChunkProgress], bool] | None = None,
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

    `chunk_size`, when set, overrides `_CELERY_CLEAR_CHUNK` for the
    LRANGE step. The chunked background-job worker
    (`start_celery_broker_clear_job`) drops it to
    `_CELERY_CLEAR_CHUNK_JOB` (1k) so the progress bar updates and
    cancel checks happen at sub-second granularity even on multi-
    hundred-thousand-deep queues; synchronous callers leave it at
    the default 10k for fewer LRANGE round-trips.

    `progress_callback`, when supplied, is invoked at each chunk
    boundary with a `_ChunkProgress` snapshot (per-chunk deltas and
    running totals). Returning `True` from the callback aborts the
    clear: the in-flight tombstones for the current pass are
    LREMmed (so we leave a clean queue, not a partial graveyard) and
    `_StreamingClearStats.cancelled` is set so the chunked worker
    can flip the job hash to `"cancelled"`. Returning `False` (or
    `None`) lets the loop continue. Cancel only ever lands at chunk
    boundaries — never mid-LSET — so an LSET we already issued is
    always paired with the corresponding LREM at the next pass-end
    or cancel-drain.
    """

    chunk = chunk_size if chunk_size is not None else _CELERY_CLEAR_CHUNK

    total_lset = 0
    total_drifted = 0
    total_found = 0
    cancelled = False
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
        # Track chunk index + count for the progress callback so the
        # progress page can render "pass 2 / 3, chunk 47 / 213". The
        # `chunks_total` is computed off the snapshot depth — fine
        # for the progress UI; the loop itself walks the same range.
        chunks_total = (depth + chunk - 1) // chunk if chunk > 0 else 0
        chunk_index = 0
        for chunk_start in range(0, depth, chunk):
            chunk_end = chunk_start + chunk - 1
            raw_chunk = redis.lrange(queue_name, chunk_start, chunk_end)
            chunk_processed = len(raw_chunk)
            chunk_lset_before = matches_lset
            chunk_drifted_before = matches_drifted
            chunk_found_before = matches_found
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
                if not _envelope_matches_any_filter(meta, filter_tuples):
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

            chunk_index += 1
            if progress_callback is not None:
                snapshot = _ChunkProgress(
                    pass_num=passes_run,
                    chunk_index=chunk_index,
                    chunks_total=chunks_total,
                    processed_delta=chunk_processed,
                    matched_delta=matches_lset - chunk_lset_before,
                    drifted_delta=matches_drifted - chunk_drifted_before,
                    found_delta=matches_found - chunk_found_before,
                    total_lset=total_lset + matches_lset,
                    total_drifted=total_drifted + matches_drifted,
                    total_found=total_found + matches_found,
                )
                # Defensive: a misbehaving callback that itself
                # raises must not strand in-flight tombstones nor
                # crash the streaming clear. Treat it the same as
                # "no cancel requested" and keep going. The chunked-
                # job worker traps and reports its own callback
                # failures via the job hash's `error` field.
                try:
                    cancel_now = bool(progress_callback(snapshot))
                except Exception:  # pragma: no cover - defensive
                    log.exception(
                        "redis_admin._streaming_celery_clear: "
                        "progress_callback raised; treating as no-cancel"
                    )
                    cancel_now = False
                if cancel_now:
                    cancelled = True
                    if not dry_run:
                        # Drain in-flight tombstones for this pass
                        # so the queue is left in a clean state
                        # rather than a partial-graveyard. Mirrors
                        # the pass-end LREM below.
                        try:
                            redis.lrem(queue_name, 0, tombstone)
                        except Exception:  # pragma: no cover - best-effort
                            log.exception(
                                "redis_admin._streaming_celery_clear: "
                                "cancel-drain LREM failed for %r",
                                queue_name,
                            )
                    total_lset += matches_lset
                    total_drifted += matches_drifted
                    total_found += matches_found
                    return _StreamingClearStats(
                        total_lset=total_lset,
                        total_drifted=total_drifted,
                        total_found=total_found,
                        first_match_index=first_match_index,
                        passes_run=passes_run,
                        cancelled=True,
                    )

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
        cancelled=cancelled,
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
    # Substitute `_FILTER_ANY` (not `None`) for unset slots so the
    # streaming match treats them as wildcards. `None` would mean
    # "match only envelopes whose corresponding field is literally
    # `None`" which would zero-out the count for any operator
    # input that didn't pin every slot of the triple.
    filter_tuples = frozenset(
        {
            (
                task_name if task_name else _FILTER_ANY,
                repoid if repoid is not None else _FILTER_ANY,
                commitid if commitid else _FILTER_ANY,
            )
        }
    )
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


# ---- Chunked celery_broker clear jobs (background-thread variant) ----------
#
# Synchronous `celery_broker_clear` is fine for changelist actions and
# the dry-run preview because operators expect the action handler to
# block; but the "Clear all" path on a 200-500k-deep queue can run
# well past the gunicorn worker `--timeout` (api.sh's 600s default
# is comfortably above that, but upstream proxies may impose lower
# ceilings — Cloudflare's free-tier is 100s, GCP HTTPS LB defaults
# to 30s). The chunked variant here decouples the clear's wall-clock
# from the HTTP request lifetime: `start_celery_broker_clear_job`
# returns a `job_id` immediately, the actual clear runs in a daemon
# thread, and the operator polls a status hash for progress + drives
# cancellation through it.
#
# Why threading and not Celery: the queue we're clearing is the
# Celery broker. Submitting a control task to a broker we're
# emptying creates a circular dependency (the control task may sit
# behind the messages we're about to tombstone); the operator pod
# would also need to be in the worker fleet to pick the task up.
# Threading keeps the entire job within the api pod's process, so
# the lifecycle is "as long as gunicorn keeps the worker alive".
# That's a documented restart-safety trade-off (see README): a
# gunicorn worker killed mid-job leaves any in-flight tombstones
# (`__redis_admin_celery_tombstone__:<uuid>`) parked in the queue.
# Operator can re-run the clear (the new pass skips them) or sweep
# manually.

# Job state lives in the cache redis (`_conn.get_connection(kind=
# "default")`), NOT the broker — keeping the operator's job
# bookkeeping off the same Redis instance we're clearing means a
# clear that drains the broker doesn't accidentally also evict its
# own progress hash.
_CELERY_CLEAR_JOB_KEY_PREFIX: str = "redis_admin:celery_clear_job"

# Smaller LRANGE chunk for the chunked worker — finer-grained
# progress updates and sub-second cancel landings on multi-hundred-
# thousand-deep queues. The synchronous path keeps
# `_CELERY_CLEAR_CHUNK` (10k) so existing tests + call sites don't
# change behaviour.
_CELERY_CLEAR_CHUNK_JOB: int = 1_000

# Auto-expire abandoned job hashes so the cache redis doesn't
# accumulate zombie state (gunicorn reload, container restart,
# operator closed the tab and forgot). 24h gives the on-call
# enough headroom to come back the next morning and inspect a
# completed run via the status URL before it disappears.
_CELERY_CLEAR_JOB_TTL_SECONDS: int = 24 * 60 * 60


# Terminal states that release the polling client.
_CELERY_CLEAR_JOB_TERMINAL_STATES: frozenset[str] = frozenset(
    {"completed", "cancelled", "failed"}
)

# Test-only handle to the worker thread per job_id so tests can
# `.join(timeout=...)` deterministically. Cleared by the worker on
# exit. NEVER rely on this in production paths; production polls
# via the JSON status endpoint. Module-private with the leading
# underscore convention so it's clearly off-limits to callers.
_clear_job_threads: dict[str, threading.Thread] = {}
# A lock around the dict because the worker thread itself mutates
# it (clears its entry on exit) and production paths read it from
# the gunicorn HTTP thread. The dict is small (one entry per
# in-flight job per worker process) and contention is rare.
_clear_job_threads_lock = threading.Lock()


def _job_key(job_id: str) -> str:
    return f"{_CELERY_CLEAR_JOB_KEY_PREFIX}:{job_id}"


def _utc_now_iso() -> str:
    """Timestamp for job hash fields. Uses timezone-aware UTC ISO 8601
    (seconds resolution) so the progress page can render durations
    and the audit trail has a stable cross-deploy ordering.
    """
    return _dt.datetime.now(_dt.UTC).isoformat(timespec="seconds")


def _decode_job_hash(raw: dict[bytes | str, bytes | str]) -> dict[str, str]:
    """Decode a fakeredis / redis-py HGETALL result into a flat
    `{str: str}` mapping. Bytes get decoded as UTF-8; non-bytes pass
    through unchanged. Used by both `get_celery_broker_clear_job`
    and the worker's cancel-poll path so the JSON status view sees
    a consistent shape regardless of which Redis client was used.
    """
    decoded: dict[str, str] = {}
    for k, v in raw.items():
        kk = k.decode() if isinstance(k, bytes) else str(k)
        vv = v.decode() if isinstance(v, bytes) else str(v)
        decoded[kk] = vv
    return decoded


def start_celery_broker_clear_job(
    queue_name: str,
    *,
    user,
    task_name: str | None = None,
    repoid: int | None = None,
    commitid: str | None = None,
    keep_one: bool = False,
    dry_run: bool = False,
) -> str:
    """Spawn a background `celery_broker_clear` job; return its `job_id`.

    Initialises a hash at `redis_admin:celery_clear_job:<job_id>` in
    the cache redis with the operator's filter shape, snapshots the
    current `LLEN(queue)` as `total_estimated`, then launches a
    daemon thread that walks the queue in
    `_CELERY_CLEAR_CHUNK_JOB`-message chunks. The HTTP request that
    triggers this returns immediately so the gunicorn worker never
    sits on the actual clear; the operator polls a status JSON view
    for progress and can request cancellation (lands at the next
    chunk boundary).
    """

    # Hyphenated canonical form (8-4-4-4-12) so the value round-
    # trips through Django's `<uuid:>` URL converter — which only
    # accepts the canonical regex — without a separate normalising
    # layer in the view.
    job_id = str(uuid.uuid4())
    cache = _conn.get_connection(kind="default")
    # Snapshot LLEN once at submission so the progress bar has a
    # stable denominator for the operator's benefit. The actual
    # streaming clear re-LLENs on every pass (the queue may shift
    # during a long clear), but the "estimated total" rendered in
    # the UI is best-effort.
    try:
        broker = _conn.get_connection(kind="broker")
        total_estimated = int(broker.llen(queue_name) or 0)
    except Exception:  # pragma: no cover - broker outage path
        total_estimated = 0

    user_id = getattr(user, "id", None) or getattr(user, "pk", None) or 0
    now = _utc_now_iso()
    initial = {
        "status": "pending",
        "queue": queue_name,
        "filter_task": task_name or "",
        "filter_repoid": "" if repoid is None else str(repoid),
        "filter_commitid": commitid or "",
        "dry_run": "1" if dry_run else "0",
        "keep_one": "1" if keep_one else "0",
        "user_id": str(user_id),
        "started_at": now,
        "updated_at": now,
        "completed_at": "",
        "total_estimated": str(total_estimated),
        "processed": "0",
        "matched": "0",
        "drifted": "0",
        "passes_run": "0",
        "error": "",
        "cancel_requested": "0",
    }
    key = _job_key(job_id)
    pipe = cache.pipeline(transaction=False)
    pipe.hset(key, mapping=initial)
    pipe.expire(key, _CELERY_CLEAR_JOB_TTL_SECONDS)
    pipe.execute()

    thread = threading.Thread(
        target=_run_celery_broker_clear_job,
        kwargs={
            "job_id": job_id,
            "queue_name": queue_name,
            "task_name": task_name,
            "repoid": repoid,
            "commitid": commitid,
            "keep_one": keep_one,
            "dry_run": dry_run,
            "user": user,
        },
        # Truncated UUID so threadnames are easy to grep in pstacks
        # without becoming unreadable.
        name=f"redis_admin.clear_job.{job_id[:8]}",
        daemon=True,
    )
    with _clear_job_threads_lock:
        _clear_job_threads[job_id] = thread
    thread.start()
    return job_id


def get_celery_broker_clear_job(job_id: str) -> dict[str, str] | None:
    """Read the job's state hash from cache redis. Returns `None`
    when the hash is absent (unknown id, or the 24h TTL elapsed).
    Values are returned as strings — the JSON status view casts
    `processed` / `matched` / `total_estimated` to int before
    serialising.
    """

    cache = _conn.get_connection(kind="default")
    raw = cache.hgetall(_job_key(job_id))
    if not raw:
        return None
    return _decode_job_hash(raw)


def request_cancel_celery_broker_clear_job(job_id: str) -> bool:
    """Set `cancel_requested=1` on the job hash. Returns `True`
    when the hash existed (cancel will land at the next chunk
    boundary), `False` when the id is unknown or the hash already
    expired. Idempotent: re-cancelling a cancelled or terminal
    job is a no-op.
    """

    cache = _conn.get_connection(kind="default")
    key = _job_key(job_id)
    if not cache.exists(key):
        return False
    cache.hset(
        key,
        mapping={
            "cancel_requested": "1",
            "updated_at": _utc_now_iso(),
        },
    )
    return True


def _run_celery_broker_clear_job(
    *,
    job_id: str,
    queue_name: str,
    task_name: str | None,
    repoid: int | None,
    commitid: str | None,
    keep_one: bool,
    dry_run: bool,
    user,
) -> None:
    """Daemon-thread worker for `start_celery_broker_clear_job`.

    Owns the lifecycle of one chunked clear: it flips `status` to
    `running`, calls `_streaming_celery_clear` with a callback that
    updates the progress hash and polls `cancel_requested`, and on
    completion writes a final `status` (`completed` / `cancelled` /
    `failed`) plus the `_record_audit` row so the operator audit
    trail captures chunked-mode clears alongside synchronous ones.

    Exceptions are swallowed: a misbehaving streaming clear must
    not crash the gunicorn worker. We log to Sentry via
    `log.exception(...)` and surface the error string to the
    operator through the job hash's `error` field instead.
    """

    try:
        _run_celery_broker_clear_job_body(
            job_id=job_id,
            queue_name=queue_name,
            task_name=task_name,
            repoid=repoid,
            commitid=commitid,
            keep_one=keep_one,
            dry_run=dry_run,
            user=user,
        )
    finally:
        # Drop the test-handle on exit so a re-run with the same
        # job_id (operator clicked twice through some flake) starts
        # cleanly. Best-effort: a failure to remove leaves a stale
        # entry, which only matters for tests, not production.
        with _clear_job_threads_lock:
            _clear_job_threads.pop(job_id, None)


def _run_celery_broker_clear_job_body(
    *,
    job_id: str,
    queue_name: str,
    task_name: str | None,
    repoid: int | None,
    commitid: str | None,
    keep_one: bool,
    dry_run: bool,
    user,
) -> None:
    """Inner body of the worker, separated from `_run_celery_broker_clear_job`
    so the wrapper can own the test-handle cleanup in a `finally`
    block. The split keeps the cleanup correct even if a future
    refactor introduces a new early-return path inside the body.
    """

    cache = _conn.get_connection(kind="default")
    key = _job_key(job_id)
    cache.hset(key, "status", "running")
    cache.hset(key, "updated_at", _utc_now_iso())

    # Build the same `_FILTER_ANY`-substituted filter the
    # synchronous `streaming_celery_count` uses, so an unset
    # operator slot becomes a wildcard rather than an exact-`None`
    # match (which would zero-match for any envelope whose field
    # wasn't literally `None`).
    filter_tuples = frozenset(
        {
            (
                task_name if task_name else _FILTER_ANY,
                repoid if repoid is not None else _FILTER_ANY,
                commitid if commitid else _FILTER_ANY,
            )
        }
    )

    def _progress_callback(snapshot: _ChunkProgress) -> bool:
        """Per-chunk hook: pump `processed/matched/drifted` into
        the job hash via HINCRBY, refresh `passes_run` /
        `updated_at`, then check `cancel_requested`. Returning
        `True` aborts the loop in `_streaming_celery_clear` and
        triggers an in-flight tombstone drain.
        """

        try:
            pipe = cache.pipeline(transaction=False)
            if snapshot.processed_delta:
                pipe.hincrby(key, "processed", snapshot.processed_delta)
            if snapshot.matched_delta:
                pipe.hincrby(key, "matched", snapshot.matched_delta)
            if snapshot.drifted_delta:
                pipe.hincrby(key, "drifted", snapshot.drifted_delta)
            pipe.hset(
                key,
                mapping={
                    "passes_run": str(snapshot.pass_num),
                    "updated_at": _utc_now_iso(),
                },
            )
            pipe.execute()
        except Exception:  # pragma: no cover - cache outage shouldn't kill clear
            log.exception(
                "redis_admin.clear_job(%s): failed to write progress",
                job_id,
            )
        try:
            cancel_raw = cache.hget(key, "cancel_requested")
        except Exception:  # pragma: no cover - cache outage
            return False
        if cancel_raw is None:
            return False
        if isinstance(cancel_raw, bytes):
            return cancel_raw == b"1"
        return cancel_raw == "1"

    error_str: str | None = None
    stats: _StreamingClearStats | None = None
    try:
        broker = _conn.get_connection(kind="broker")
        tombstone = _celery_clear_tombstone()
        stats = _streaming_celery_clear(
            broker,
            queue_name,
            filter_tuples,
            tombstone,
            keep_one=keep_one,
            dry_run=dry_run,
            chunk_size=_CELERY_CLEAR_CHUNK_JOB,
            progress_callback=_progress_callback,
        )
    except Exception as exc:
        log.exception("redis_admin.clear_job(%s): worker failed", job_id)
        error_str = str(exc)

    finalise: dict[str, str] = {
        "completed_at": _utc_now_iso(),
        "updated_at": _utc_now_iso(),
    }
    if error_str is not None:
        finalise["status"] = "failed"
        finalise["error"] = error_str
    elif stats is not None and stats.cancelled:
        finalise["status"] = "cancelled"
    else:
        finalise["status"] = "completed"
    try:
        cache.hset(key, mapping=finalise)
    except Exception:  # pragma: no cover - cache outage at finalise
        log.exception(
            "redis_admin.clear_job(%s): failed to write terminal state",
            job_id,
        )

    # Audit at job-completion (not job-start). `result.count` here
    # mirrors the synchronous path: total tombstoned for live runs,
    # total_found for dry-run previews. Extra fields capture the
    # chunked-mode shape so log queries can distinguish chunked from
    # synchronous clears.
    if dry_run:
        mode = "chunked-dry-run"
    elif keep_one:
        mode = "chunked-all-but-first"
    else:
        mode = "chunked-all-from-bucket"
    sample = (f"{queue_name}#filter",)
    families = ("celery_broker",)
    extra: dict[str, Any] = {
        "mode": mode,
        "job_id": job_id,
        "cancelled": bool(stats and stats.cancelled),
    }
    if stats is not None:
        extra.update(
            {
                "passes_run": stats.passes_run,
                "total_lset": stats.total_lset,
                "total_drifted": stats.total_drifted,
                "total_found": stats.total_found,
            }
        )
        count = stats.total_found if dry_run else stats.total_lset
    else:
        count = 0
    if error_str is not None:
        extra["error"] = error_str
    _record_audit(
        user=user,
        scope="celery_broker_clear",
        count=count,
        refused=(),
        sample=sample,
        families=families,
        dry_run=dry_run,
        extra=extra,
    )
