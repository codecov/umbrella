"""Registry of Redis key "families" surfaced in the admin.

A `Family` describes a Redis key pattern owned by some part of the system:
how to recognise it, how to extract structured fields (repo / commit /
report type), how to build a tighter SCAN pattern when the admin user
narrows their query, and (optionally) how to decode a single item for
display.

Milestone 1 enumerated keys; M3 added repo/commit parsers + pushdown;
M4 extends the registry with the remaining Redis-backed surfaces:
`latest_upload`, `upload-processing-state`, `intermediate-report`, the
Celery broker queues (with kombu-envelope decoding), and a read-only
locks family.
"""

from __future__ import annotations

import base64
import fnmatch
import json
import logging
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

import sentry_sdk

from . import conn as _conn
from . import settings as redis_admin_settings

log = logging.getLogger(__name__)


RedisType = Literal["list", "set", "hash", "string"]

# Which Redis connection a family lives on. Re-exported from
# `redis_admin.conn` so callers can keep their `ConnectionKind`
# imports family-shaped (`from .families import ConnectionKind`)
# without spawning a duplicate definition that could silently drift
# from the canonical one in `conn.py`.
ConnectionKind = _conn.ConnectionKind


@dataclass(frozen=True)
class ParsedKey:
    """Structured view of a Redis key once we know which family it belongs to.

    Fields are optional because not every family encodes every dimension in
    its key (e.g. `ta_flake_key:{repoid}` has no commitid;
    `intermediate-report/{upload_id}` has only an upload_id).
    """

    family: str
    repoid: int | None = None
    commitid: str | None = None
    report_type: str | None = None
    upload_id: str | None = None
    state: str | None = None


# ---- Parsers ---------------------------------------------------------------


def _parse_uploads_key(key: str) -> ParsedKey:
    """`uploads/{repoid}/{commitid}` (coverage) or
    `uploads/{repoid}/{commitid}/{report_type}` (test_results, bundle_analysis).

    Mirrors what `upload.helpers.dispatch_upload_task` writes today:
    coverage drops the trailing report-type segment.
    """

    parts = key.split("/", 3)
    if len(parts) < 3 or parts[0] != "uploads":
        return ParsedKey(family="uploads")
    repoid: int | None
    try:
        repoid = int(parts[1])
    except ValueError:
        repoid = None
    commitid = parts[2] or None
    report_type = parts[3] if len(parts) >= 4 and parts[3] else "coverage"
    return ParsedKey(
        family="uploads",
        repoid=repoid,
        commitid=commitid,
        report_type=report_type,
    )


def _parse_ta_flake_key(key: str) -> ParsedKey:
    """`ta_flake_key:{repoid}` written by the test-analytics finisher."""

    if not key.startswith("ta_flake_key:"):
        return ParsedKey(family="ta_flake_key")
    tail = key[len("ta_flake_key:") :]
    repoid: int | None
    try:
        repoid = int(tail)
    except ValueError:
        repoid = None
    return ParsedKey(family="ta_flake_key", repoid=repoid)


def _parse_latest_upload_key(key: str) -> ParsedKey:
    """`latest_upload/{repoid}/{commitid}` (coverage default) or
    `latest_upload/{repoid}/{commitid}/{report_type}`.

    Mirrors the key shape written next to the matching `uploads/` queue
    by `upload.helpers.dispatch_upload_task`.
    """

    parts = key.split("/", 3)
    if len(parts) < 3 or parts[0] != "latest_upload":
        return ParsedKey(family="latest_upload")
    repoid: int | None
    try:
        repoid = int(parts[1])
    except ValueError:
        repoid = None
    commitid = parts[2] or None
    report_type = parts[3] if len(parts) >= 4 and parts[3] else "coverage"
    return ParsedKey(
        family="latest_upload",
        repoid=repoid,
        commitid=commitid,
        report_type=report_type,
    )


def _parse_upload_processing_state_key(key: str) -> ParsedKey:
    """`upload-processing-state/{repoid}/{commitsha}/{state}` written by
    `apps/worker/services/processing/state.py`. Each state lives in its
    own SET key; the trailing `{state}` segment is preserved on the
    parsed `state` field for context.
    """

    parts = key.split("/", 3)
    if len(parts) < 3 or parts[0] != "upload-processing-state":
        return ParsedKey(family="upload-processing-state")
    repoid: int | None
    try:
        repoid = int(parts[1])
    except ValueError:
        repoid = None
    commitid = parts[2] or None
    state = parts[3] if len(parts) >= 4 and parts[3] else None
    return ParsedKey(
        family="upload-processing-state",
        repoid=repoid,
        commitid=commitid,
        state=state,
    )


def _parse_intermediate_report_key(key: str) -> ParsedKey:
    """`intermediate-report/{upload_id}` written by
    `apps/worker/services/processing/intermediate.py`. The key shape
    only carries an `upload_id`; `repoid` / `commitid` are not derivable
    without a DB lookup we deliberately don't do here.
    """

    if not key.startswith("intermediate-report/"):
        return ParsedKey(family="intermediate-report")
    upload_id = key[len("intermediate-report/") :] or None
    return ParsedKey(family="intermediate-report", upload_id=upload_id)


def _parse_celery_broker_key(_: str) -> ParsedKey:
    """Celery broker queues are keyed by their queue name (`celery`,
    `healthcheck`, `enterprise_*`, etc.); none of them carry repo or
    commit info in the *queue* key. Per-item decoding is what surfaces
    `repoid`/`commitid` (see `_celery_decode_value`).
    """

    return ParsedKey(family="celery_broker")


def _resolve_celery_queue_names() -> tuple[str, ...]:
    """Best-effort enumeration of the well-known Celery queue names.

    Reads from the live `BaseCeleryConfig` so we pick up any queues an
    operator configured via `setup.tasks.*.queue`. Falls back to the two
    documented defaults if the import fails (e.g. during tests or in a
    minimal environment).
    """

    try:
        from shared.celery_config import BaseCeleryConfig  # noqa: PLC0415
    except Exception:  # pragma: no cover - defensive: any import error
        log.warning(
            "redis_admin: could not import BaseCeleryConfig; "
            "using ('celery', 'healthcheck') only"
        )
        return ("celery", "healthcheck")

    names: set[str] = set()
    names.add(getattr(BaseCeleryConfig, "task_default_queue", "celery") or "celery")
    names.add(
        getattr(BaseCeleryConfig, "health_check_default_queue", "healthcheck")
        or "healthcheck"
    )
    routes = getattr(BaseCeleryConfig, "task_routes", {}) or {}
    for spec in routes.values():
        if isinstance(spec, dict):
            queue_name = spec.get("queue")
            if isinstance(queue_name, str) and queue_name:
                names.add(queue_name)
    return tuple(sorted(names))


def _peek_celery_envelope(decoded: str) -> tuple[str | None, int | None, str | None]:
    """Pull `(task, repoid, commitid)` from a kombu JSON envelope.

    Returns `(None, None, None)` for anything we can't parse so the
    fallback is "show the raw payload" rather than "crash the admin".
    """

    try:
        envelope = json.loads(decoded)
    except (ValueError, TypeError):
        return (None, None, None)
    if not isinstance(envelope, dict):
        return (None, None, None)

    headers = envelope.get("headers") or {}
    task = headers.get("task") if isinstance(headers, dict) else None
    if not isinstance(task, str):
        task = None

    repoid: int | None = None
    commitid: str | None = None
    body_b64 = envelope.get("body")
    if isinstance(body_b64, str) and body_b64:
        try:
            body_bytes = base64.b64decode(body_b64, validate=False)
            body = json.loads(body_bytes.decode("utf-8", errors="replace"))
        except (ValueError, TypeError, UnicodeDecodeError):
            body = None
        if isinstance(body, list) and len(body) >= 2 and isinstance(body[1], dict):
            kwargs = body[1]
            rid = kwargs.get("repoid")
            if isinstance(rid, int):
                repoid = rid
            elif isinstance(rid, str) and rid.isdigit():
                repoid = int(rid)
            cid = kwargs.get("commitid")
            if isinstance(cid, str) and cid:
                commitid = cid
    return (task, repoid, commitid)


def _celery_decode_value(raw_decoded: str) -> str:
    """Prefix the kombu payload with a one-line summary if we can."""

    task, repoid, commitid = _peek_celery_envelope(raw_decoded)
    parts: list[str] = []
    if task:
        parts.append(f"task={task}")
    if repoid is not None:
        parts.append(f"repoid={repoid}")
    if commitid:
        short = commitid[:7]
        parts.append(f"commit={short}")
    if parts:
        return f"[{' '.join(parts)}] {raw_decoded}"
    return raw_decoded


def _celery_pattern(filters: Mapping[str, Any]) -> str | None:
    """Celery queue *keys* don't carry repoid/commitid; if those filters
    are set, every Celery queue would be dropped post-scan, so skip the
    family entirely.
    """

    if filters.get("repoid") is not None or filters.get("commitid_prefix"):
        return None
    return "enterprise_*"


# ---- Lock parsers (M4.3) ---------------------------------------------------
#
# Locks / fences / gates are read-only in the admin: deleting an active
# lock could cause race conditions in the worker tasks that own them.
# Their families are registered with `is_deletable=False`, surface only
# in the `RedisLock` admin (not `RedisQueue`), and never accept a
# `DELETE` from `services.redis_delete()` (M5).


def _split_lock_tail(tail: str) -> tuple[int | None, str | None, str | None]:
    """Pull `(repoid, commitid, report_type)` out of `<repoid>_<commit>[_<rt>]`.

    Used by every lock-family parser since the suffix shape is shared.
    """

    if not tail:
        return (None, None, None)
    parts = tail.split("_")
    repoid: int | None = None
    commitid: str | None = None
    report_type: str | None = None
    if parts and parts[0]:
        try:
            repoid = int(parts[0])
        except ValueError:
            repoid = None
    if len(parts) >= 2:
        commitid = parts[1] or None
    if len(parts) >= 3:
        report_type = "_".join(parts[2:]) or None
    return (repoid, commitid, report_type)


def _parse_coordination_lock_name(family_name: str, name: str) -> ParsedKey:
    """`<type>_lock_<repoid>_<commit>[_<report_type>]` per
    `apps/worker/services/lock_manager.py::LockManager.lock_name`.
    Shared by the `coordination_lock` family and `lock_attempts:` keys
    (which embed a coordination-lock name as their suffix).
    """

    sep = "_lock_"
    if sep not in name:
        return ParsedKey(family=family_name)
    _, _, tail = name.partition(sep)
    repoid, commitid, report_type = _split_lock_tail(tail)
    return ParsedKey(
        family=family_name,
        repoid=repoid,
        commitid=commitid,
        report_type=report_type,
    )


def _parse_coordination_lock_key(key: str) -> ParsedKey:
    return _parse_coordination_lock_name("coordination_lock", key)


def _parse_lock_attempts_key(key: str) -> ParsedKey:
    """`lock_attempts:{coordination_lock_name}` — STRING counter set by
    `lock_manager._track_lock_attempt`. The interesting fields live in
    the embedded coordination-lock name, so reuse that parser.
    """

    if not key.startswith("lock_attempts:"):
        return ParsedKey(family="lock_attempts")
    inner = key[len("lock_attempts:") :]
    parsed = _parse_coordination_lock_name("lock_attempts", inner)
    return ParsedKey(
        family="lock_attempts",
        repoid=parsed.repoid,
        commitid=parsed.commitid,
        report_type=parsed.report_type,
    )


def _parse_ta_flake_lock_key(key: str) -> ParsedKey:
    """`ta_flake_lock:{repoid}` per `ta_process_flakes.py`."""

    if not key.startswith("ta_flake_lock:"):
        return ParsedKey(family="ta_flake_lock")
    tail = key[len("ta_flake_lock:") :]
    try:
        repoid = int(tail)
    except ValueError:
        return ParsedKey(family="ta_flake_lock")
    return ParsedKey(family="ta_flake_lock", repoid=repoid)


def _parse_ta_notifier_fence_key(key: str) -> ParsedKey:
    """`ta_notifier_fence:{repoid}_{commitid}` per `test_analytics_notifier.py`."""

    if not key.startswith("ta_notifier_fence:"):
        return ParsedKey(family="ta_notifier_fence")
    repoid, commitid, _ = _split_lock_tail(key[len("ta_notifier_fence:") :])
    return ParsedKey(family="ta_notifier_fence", repoid=repoid, commitid=commitid)


def _parse_upload_finisher_gate_key(key: str) -> ParsedKey:
    """`upload_finisher_gate_{repoid}_{commitid}` per `finisher_gate.py`."""

    if not key.startswith("upload_finisher_gate_"):
        return ParsedKey(family="upload_finisher_gate")
    repoid, commitid, _ = _split_lock_tail(key[len("upload_finisher_gate_") :])
    return ParsedKey(family="upload_finisher_gate", repoid=repoid, commitid=commitid)


def _lock_pattern_factory(prefix: str, *, prefix_sep: str, has_commit: bool):
    """Build a `pattern_for` for a lock family whose key shape is
    either `<prefix><prefix_sep><repoid>` (when `has_commit=False`) or
    `<prefix><prefix_sep><repoid>_<commit>` (when `has_commit=True`).
    Pushes `repoid` (and `commitid_prefix` when applicable) into the
    SCAN; drops the family entirely when the user filtered by
    `commitid_prefix` against a `has_commit=False` key shape.

    `prefix_sep` is the literal between the family prefix and the
    repoid (`:` for `ta_flake_lock`/`ta_notifier_fence`, `_` for
    `upload_finisher_gate`). When `has_commit` is true the
    repoid-to-commit separator is hardcoded `_` because that's what
    every callsite in the worker uses.
    """

    def pattern_for(filters: Mapping[str, Any]) -> str | None:
        commitid = filters.get("commitid_prefix")
        if not has_commit and commitid:
            return None
        repoid = filters.get("repoid")
        if repoid is not None and has_commit and commitid:
            return f"{prefix}{prefix_sep}{repoid}_{commitid}*"
        if repoid is not None:
            tail = "_*" if has_commit else "*"
            return f"{prefix}{prefix_sep}{repoid}{tail}"
        return f"{prefix}{prefix_sep}*"

    return pattern_for


def _lock_attempts_pattern(filters: Mapping[str, Any]) -> str:
    """`pattern_for` for the `lock_attempts:` family whose keys are
    `lock_attempts:<coordination_lock_name>` and the coordination-
    lock-name in turn is `<type>_lock_<repoid>_<commit>...`. The
    repoid is buried inside the suffix, so a naive
    `lock_attempts:<repoid>*` pushdown matches zero keys (cf. the
    Bugbot review on PR #888). We push the embedded structure into
    the SCAN MATCH instead.

    `lock_attempts:*_lock_<repoid>_*` is still tighter than the
    full-keyspace fallback (`lock_attempts:*`) and keeps the SCAN
    targeted enough to be useful on busy clusters.
    """

    repoid = filters.get("repoid")
    commitid = filters.get("commitid_prefix")
    if repoid is not None and commitid:
        return f"lock_attempts:*_lock_{repoid}_{commitid}*"
    if repoid is not None:
        return f"lock_attempts:*_lock_{repoid}_*"
    # Commitid-only pushdown isn't safe (commitid can collide with
    # repoid digits at any position in the key); fall back to the
    # family wildcard and let the post-scan `parse_key` filter do
    # the work.
    return "lock_attempts:*"


# ---- SCAN pattern pushdown -------------------------------------------------


def _uploads_pattern(filters: Mapping[str, Any]) -> str:
    """Tighten `uploads/*` based on what the admin filter supplied.

    The trailing `*` covers both coverage (`uploads/{r}/{c}`) and the
    typed variants (`uploads/{r}/{c}/{report_type}`), so a commitid-prefix
    pushdown still sees both.
    """

    repoid = filters.get("repoid")
    commitid_prefix = filters.get("commitid_prefix") or ""
    if repoid is not None and commitid_prefix:
        return f"uploads/{repoid}/{commitid_prefix}*"
    if repoid is not None:
        return f"uploads/{repoid}/*"
    if commitid_prefix:
        return f"uploads/*/{commitid_prefix}*"
    return "uploads/*"


def _ta_flake_pattern(filters: Mapping[str, Any]) -> str | None:
    """`ta_flake_key:{repoid}` is exact when we know the repoid.

    Filtering by `commitid` short-circuits the family entirely since
    the key shape has no commit segment to match against.
    """

    if filters.get("commitid_prefix"):
        return None
    repoid = filters.get("repoid")
    if repoid is not None:
        return f"ta_flake_key:{repoid}"
    return "ta_flake_key:*"


def _latest_upload_pattern(filters: Mapping[str, Any]) -> str:
    """Same shape as `uploads/`: `latest_upload/{repoid}/{commitid}[/{rt}]`."""

    repoid = filters.get("repoid")
    commitid_prefix = filters.get("commitid_prefix") or ""
    if repoid is not None and commitid_prefix:
        return f"latest_upload/{repoid}/{commitid_prefix}*"
    if repoid is not None:
        return f"latest_upload/{repoid}/*"
    if commitid_prefix:
        return f"latest_upload/*/{commitid_prefix}*"
    return "latest_upload/*"


def _upload_processing_state_pattern(filters: Mapping[str, Any]) -> str:
    """`upload-processing-state/{repoid}/{commitsha}/{state}` — `*`
    matches across `/` so the wildcard tail covers the state suffix.
    """

    repoid = filters.get("repoid")
    commitid_prefix = filters.get("commitid_prefix") or ""
    if repoid is not None and commitid_prefix:
        return f"upload-processing-state/{repoid}/{commitid_prefix}*"
    if repoid is not None:
        return f"upload-processing-state/{repoid}/*"
    if commitid_prefix:
        return f"upload-processing-state/*/{commitid_prefix}*"
    return "upload-processing-state/*"


def _intermediate_report_pattern(filters: Mapping[str, Any]) -> str | None:
    """No repoid/commitid in the key shape; if either is being filtered
    on, every intermediate-report key would be dropped post-scan, so
    skip the family entirely instead of paying for the SCAN.
    """

    if filters.get("repoid") is not None or filters.get("commitid_prefix"):
        return None
    return "intermediate-report/*"


@dataclass(frozen=True)
class Family:
    name: str
    scan_pattern: str
    redis_type: RedisType
    parse_key: Callable[[str], ParsedKey]
    # `pattern_for` may return None to declare that the current filters
    # cannot match anything in this family (e.g. filtering by commitid
    # against a family whose key shape has no commit segment); the
    # iterator skips such families entirely instead of running a doomed
    # SCAN.
    pattern_for: Callable[[Mapping[str, Any]], str | None] | None = None
    fixed_keys: tuple[str, ...] = field(default_factory=tuple)
    # M5 tags: families that are safe to delete from. Locks (M4.3) leave
    # this False so the admin refuses to mutate them even by mistake.
    is_deletable: bool = True
    # Which top-level admin model surfaces this family — RedisQueue
    # (default) or RedisLock. Locks live in their own admin so an
    # operator can't accidentally clear them from the queues page.
    category: Literal["queue", "lock"] = "queue"
    # Optional per-item display transformer. Items queryset (M2/M4) calls
    # this on each LRANGE / SSCAN / HSCAN value before truncation, so
    # families like `celery_broker` can decorate their kombu envelope
    # with a `[task=... repoid=... commit=...]` prefix without
    # special-casing the queryset itself.
    decode_value: Callable[[str], str] | None = None
    # Which Redis instance owns this family's keys. Production splits
    # the Celery broker onto its own Memorystore (see
    # `redis_admin.conn`) — `celery_broker` is the only family that
    # opts in today, but keeping this as a per-family knob means future
    # split-Redis surfaces (e.g. a dedicated rate-limit cluster) can
    # plug in the same way.
    connection_kind: ConnectionKind = "default"

    def build_scan_pattern(self, filters: Mapping[str, Any]) -> str | None:
        if self.pattern_for is None:
            return self.scan_pattern
        return self.pattern_for(filters)


FAMILIES: tuple[Family, ...] = (
    Family(
        name="uploads",
        scan_pattern="uploads/*",
        redis_type="list",
        parse_key=_parse_uploads_key,
        pattern_for=_uploads_pattern,
    ),
    Family(
        name="ta_flake_key",
        scan_pattern="ta_flake_key:*",
        redis_type="list",
        parse_key=_parse_ta_flake_key,
        pattern_for=_ta_flake_pattern,
    ),
    Family(
        name="latest_upload",
        scan_pattern="latest_upload/*",
        redis_type="string",
        parse_key=_parse_latest_upload_key,
        pattern_for=_latest_upload_pattern,
    ),
    Family(
        name="upload-processing-state",
        scan_pattern="upload-processing-state/*",
        redis_type="set",
        parse_key=_parse_upload_processing_state_key,
        pattern_for=_upload_processing_state_pattern,
    ),
    Family(
        name="intermediate-report",
        scan_pattern="intermediate-report/*",
        redis_type="hash",
        parse_key=_parse_intermediate_report_key,
        pattern_for=_intermediate_report_pattern,
    ),
    Family(
        name="celery_broker",
        # Used as the SCAN pattern for ad-hoc enterprise_* queues; the
        # well-known names from BaseCeleryConfig come through `fixed_keys`.
        scan_pattern="enterprise_*",
        redis_type="list",
        parse_key=_parse_celery_broker_key,
        pattern_for=_celery_pattern,
        fixed_keys=_resolve_celery_queue_names(),
        decode_value=_celery_decode_value,
        # Celery queues live on the broker Redis (`services.celery_broker`),
        # which in production is a separate Memorystore from the cache Redis
        # the rest of the families use. See `redis_admin.conn` for details.
        connection_kind="broker",
    ),
    # ---- Locks (M4.3) -----------------------------------------------------
    # Order matters: more-specific prefixes go BEFORE the broad
    # `*_lock_*` coordination family so `find_family` correctly routes
    # `lock_attempts:upload_lock_…` to `lock_attempts` rather than
    # `coordination_lock`.
    Family(
        name="lock_attempts",
        scan_pattern="lock_attempts:*",
        redis_type="string",
        parse_key=_parse_lock_attempts_key,
        pattern_for=_lock_attempts_pattern,
        is_deletable=False,
        category="lock",
    ),
    Family(
        name="ta_flake_lock",
        scan_pattern="ta_flake_lock:*",
        redis_type="string",
        parse_key=_parse_ta_flake_lock_key,
        pattern_for=_lock_pattern_factory(
            "ta_flake_lock", prefix_sep=":", has_commit=False
        ),
        is_deletable=False,
        category="lock",
    ),
    Family(
        name="ta_notifier_fence",
        scan_pattern="ta_notifier_fence:*",
        redis_type="string",
        parse_key=_parse_ta_notifier_fence_key,
        pattern_for=_lock_pattern_factory(
            "ta_notifier_fence", prefix_sep=":", has_commit=True
        ),
        is_deletable=False,
        category="lock",
    ),
    Family(
        name="upload_finisher_gate",
        scan_pattern="upload_finisher_gate_*",
        redis_type="string",
        parse_key=_parse_upload_finisher_gate_key,
        pattern_for=_lock_pattern_factory(
            "upload_finisher_gate", prefix_sep="_", has_commit=True
        ),
        is_deletable=False,
        category="lock",
    ),
    Family(
        name="coordination_lock",
        scan_pattern="*_lock_*",
        redis_type="string",
        parse_key=_parse_coordination_lock_key,
        # Repoid pushdown isn't safe here because the `_lock_` infix can
        # appear inside the type prefix (e.g. `upload_lock_123_…`); the
        # post-scan repoid filter on the queryset takes care of it.
        is_deletable=False,
        category="lock",
    ),
)


def _decode(value: bytes | str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="replace")
    return value


def find_family(key: str) -> Family | None:
    """Return the `Family` whose `scan_pattern` matches `key`, or None."""

    for family in FAMILIES:
        if key in family.fixed_keys:
            return family
        if family.scan_pattern and fnmatch.fnmatchcase(key, family.scan_pattern):
            return family
    return None


def iter_keys(
    redis=None,
    *,
    family: str | None = None,
    repoid: int | None = None,
    commitid_prefix: str | None = None,
    category: Literal["queue", "lock"] | None = None,
) -> Iterator[tuple[str, Family]]:
    """Yield `(key, family)` for every known family, optionally narrowed.

    Filters that the family understands are pushed down into the SCAN
    pattern (see `_uploads_pattern` / `_ta_flake_pattern`); filters that
    don't apply are dropped, and post-scan filtering happens in
    `RedisQueueQuerySet`. `category` separates queue families (default
    `RedisQueue` audience) from lock families (`RedisLock` audience).
    Total work is bounded by `MAX_SCAN_KEYS`.

    When `redis` is `None` (the production path) each family is iterated
    against the connection it owns via `family.connection_kind` —
    `celery_broker` runs against the Celery broker Redis while everyone
    else runs against the cache Redis. Passing an explicit `redis`
    overrides routing and uses that single client for every family,
    which is the shape tests rely on (one fakeredis backs all kinds).
    """

    if redis is not None:

        def resolve_client(kind: ConnectionKind) -> Any:
            return redis
    else:
        # Cache one client per kind so iter_keys at most opens two
        # connections per call (default + broker), regardless of how
        # many families we sweep.
        client_cache: dict[ConnectionKind, Any] = {}

        def resolve_client(kind: ConnectionKind) -> Any:
            if kind not in client_cache:
                client_cache[kind] = _conn.get_connection(kind=kind)
            return client_cache[kind]

    cap = redis_admin_settings.MAX_SCAN_KEYS
    count = redis_admin_settings.SCAN_COUNT
    filters: dict[str, Any] = {
        "repoid": repoid,
        "commitid_prefix": commitid_prefix,
    }
    # Wrap the entire SCAN sweep in a span so an admin page that backs
    # itself onto a slow Redis surfaces as a discrete unit in Sentry.
    span_name = f"{category or 'all'}.{family or '*'}"
    with sentry_sdk.start_span(op="redis.admin.iter_keys", name=span_name) as span:
        try:
            span.set_tag("redis_admin.category", category or "all")
            span.set_tag("redis_admin.family", family or "*")
            if repoid is not None:
                span.set_tag("redis_admin.repoid", repoid)
            if commitid_prefix:
                span.set_tag("redis_admin.commitid_prefix", commitid_prefix)
        except AttributeError:  # pragma: no cover - older sentry sdks
            pass
        yield from _iter_keys_inner(
            resolve_client,
            family=family,
            category=category,
            cap=cap,
            count=count,
            filters=filters,
        )


def _iter_keys_inner(
    resolve_client: Callable[[ConnectionKind], Any],
    *,
    family: str | None,
    category: Literal["queue", "lock"] | None,
    cap: int,
    count: int,
    filters: Mapping[str, Any],
) -> Iterator[tuple[str, Family]]:
    """Body of `iter_keys`, factored out so the wrapping span lives in a
    plain function and the inner generator can `yield` freely without
    nested context-manager bookkeeping."""

    seen = 0
    # Track yielded keys so an overlapping pattern (e.g.
    # `lock_attempts:upload_lock_…` is matched by both `lock_attempts:*`
    # and the broader `*_lock_*`) doesn't double-count.
    yielded: set[str] = set()

    for fam in FAMILIES:
        if family and fam.name != family:
            continue
        if category is not None and fam.category != category:
            continue

        pattern = fam.build_scan_pattern(filters)
        # `pattern is None` means the family cannot match the requested
        # filters (e.g. filtering by repoid against `celery_broker`).
        # Skip both fixed_keys and the SCAN so we don't waste roundtrips
        # on guaranteed-empty matches.
        if pattern is None:
            continue

        client = resolve_client(fam.connection_kind)

        for key in fam.fixed_keys:
            if key in yielded:
                continue
            if client.exists(key):
                yielded.add(key)
                yield key, fam
                seen += 1
                if seen >= cap:
                    log.warning(
                        "redis_admin: hit MAX_SCAN_KEYS=%s while iterating fixed keys",
                        cap,
                    )
                    return

        if not pattern:
            continue

        for raw_key in client.scan_iter(match=pattern, count=count):
            key = _decode(raw_key)
            if key in yielded:
                continue
            # If a more-specific family registered earlier owns this key
            # (e.g. `lock_attempts:` matched it before the broader
            # `*_lock_*` pattern), it'll yield it on its own iteration —
            # don't surface it again here as a `coordination_lock`.
            owner = find_family(key)
            if owner is not None and owner.name != fam.name:
                continue
            yielded.add(key)
            yield key, fam
            seen += 1
            if seen >= cap:
                log.warning(
                    "redis_admin: hit MAX_SCAN_KEYS=%s while scanning %s",
                    cap,
                    pattern,
                )
                return
