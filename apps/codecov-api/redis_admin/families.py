"""Registry of Redis key "families" surfaced in the admin.

A `Family` describes a Redis key pattern owned by some part of the system:
how to recognise it, how to extract structured fields (repo / commit /
report type), and how to build a tighter SCAN pattern when the admin user
narrows their query.

Milestone 1 just enumerated keys; milestone 3 makes the parsers real and
adds the SCAN pushdown helpers used by the changelist's filter/search.
Milestone 4 will extend the registry with `latest_upload`,
`upload-processing-state`, `intermediate-report`, the Celery broker
queues, and the locks family.
"""

from __future__ import annotations

import fnmatch
import logging
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from . import conn as _conn
from . import settings as redis_admin_settings

log = logging.getLogger(__name__)


RedisType = Literal["list", "set", "hash", "string"]


@dataclass(frozen=True)
class ParsedKey:
    """Structured view of a Redis key once we know which family it belongs to.

    Fields are optional because not every family encodes every dimension in
    its key (e.g. `ta_flake_key:{repoid}` has no commitid).
    """

    family: str
    repoid: int | None = None
    commitid: str | None = None
    report_type: str | None = None
    upload_id: str | None = None


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


def _ta_flake_pattern(filters: Mapping[str, Any]) -> str:
    """`ta_flake_key:{repoid}` is exact when we know the repoid."""

    repoid = filters.get("repoid")
    if repoid is not None:
        return f"ta_flake_key:{repoid}"
    return "ta_flake_key:*"


@dataclass(frozen=True)
class Family:
    name: str
    scan_pattern: str
    redis_type: RedisType
    parse_key: Callable[[str], ParsedKey]
    pattern_for: Callable[[Mapping[str, Any]], str] | None = None
    fixed_keys: tuple[str, ...] = field(default_factory=tuple)

    def build_scan_pattern(self, filters: Mapping[str, Any]) -> str:
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
) -> Iterator[tuple[str, Family]]:
    """Yield `(key, family)` for every known family, optionally narrowed.

    Filters that the family understands are pushed down into the SCAN
    pattern (see `_uploads_pattern` / `_ta_flake_pattern`); filters that
    don't apply are dropped, and post-scan filtering happens in
    `RedisQueueQuerySet`. Total work is bounded by `MAX_SCAN_KEYS`.
    """

    redis = redis if redis is not None else _conn.get_connection()
    seen = 0
    cap = redis_admin_settings.MAX_SCAN_KEYS
    count = redis_admin_settings.SCAN_COUNT
    filters: dict[str, Any] = {
        "repoid": repoid,
        "commitid_prefix": commitid_prefix,
    }

    for fam in FAMILIES:
        if family and fam.name != family:
            continue

        for key in fam.fixed_keys:
            if redis.exists(key):
                yield key, fam
                seen += 1
                if seen >= cap:
                    log.warning(
                        "redis_admin: hit MAX_SCAN_KEYS=%s while iterating fixed keys",
                        cap,
                    )
                    return

        pattern = fam.build_scan_pattern(filters)
        if not pattern:
            continue

        for raw_key in redis.scan_iter(match=pattern, count=count):
            yield _decode(raw_key), fam
            seen += 1
            if seen >= cap:
                log.warning(
                    "redis_admin: hit MAX_SCAN_KEYS=%s while scanning %s",
                    cap,
                    pattern,
                )
                return
