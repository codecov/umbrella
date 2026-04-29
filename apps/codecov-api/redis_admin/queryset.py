"""Fake QuerySets that let Django admin render Redis keys and their items.

Inspired by `wolph/django-redis-admin`. The Django admin's changelist talks to
its model via a small set of QuerySet methods: `iterator`, `count`,
`__getitem__` (slicing for pagination), `_clone`/`all`/`none`/`using`, and
`order_by`. This module implements just those, dispatching to Redis under the
hood so the models need no real database table.

`RedisQueueQuerySet` — milestone 1, enumerates keys.
`RedisItemQuerySet`  — milestone 2, enumerates items inside a single keyed
                       container, gated on a `queue_name__exact=<key>` filter
                       so we never accidentally scan every item in Redis.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import sentry_sdk

from . import conn as _conn
from . import settings as redis_admin_settings
from .families import (
    CeleryEnvelopeMeta,
    Family,
    find_family,
    iter_keys,
    parse_celery_envelope,
)
from .families import _decode as _decode_value  # noqa: PLC2701 - shared helper
from .families import (
    _resolve_celery_queue_names as _celery_queue_names,  # noqa: PLC2701
)

_UNSET: Any = object()


def _ordering_key(obj: Any, attr: str) -> tuple:
    """Sort key tolerant of `None` on mixed-type columns.

    `list_display` columns like `report_type` are nullable strings:
    rows from the `uploads` family populate them with `"coverage"` /
    `"test_results"` while `ta_flake_key` rows leave them as `None`.
    Python 3 won't compare `str` and `int`, so the previous
    `getattr(obj, attr) or 0` sort key crashed with `TypeError` as
    soon as one row's column was `None` and another's was a string
    (Bugbot review on PR #887).

    Returning a 2-tuple `(is_none, value_or_blank)` keeps `None` rows
    grouped together (sorted last), and the inner `value_or_blank` is
    only ever compared between two non-None values of the same
    underlying field — so the comparison stays type-consistent.
    """

    value = getattr(obj, attr, None)
    if value is None:
        return (1, "")
    return (0, value)


def _ordering_tuple_for_int(value: int | None) -> tuple:
    """`None`-tolerant tie-break key for an int axis (used by chart sort)."""

    if value is None:
        return (1, 0)
    return (0, value)


def _ordering_tuple_for_str(value: str | None) -> tuple:
    """`None`-tolerant tie-break key for a str axis (used by chart sort)."""

    if value is None:
        return (1, "")
    return (0, value)


def _build_redis_queue(model, key: str, family: Family, redis) -> Any:
    """Materialize a single `RedisQueue` model instance from Redis."""

    redis_type = family.redis_type
    if redis_type == "list":
        depth = redis.llen(key)
    elif redis_type == "set":
        depth = redis.scard(key)
    elif redis_type == "hash":
        depth = redis.hlen(key)
    elif redis_type == "string":
        # For a string we report 0 (no items to consume) but keep the entry so
        # operators can see/clear it. STRLEN would be misleading as a "depth".
        depth = 0
    else:
        depth = 0

    ttl = redis.ttl(key)
    # redis-py returns -2 if the key does not exist and -1 if it has no TTL.
    # We surface "no TTL" as None in the admin to keep the column readable.
    ttl_seconds = None if ttl is None or ttl < 0 else int(ttl)

    parsed = family.parse_key(key)

    return model(
        name=key,
        family=family.name,
        redis_type=redis_type.upper(),
        depth=int(depth or 0),
        ttl_seconds=ttl_seconds,
        repoid=parsed.repoid,
        commitid=parsed.commitid,
        report_type=parsed.report_type,
    )


# Filter kwargs `RedisQueueQuerySet.filter()` understands; everything else
# raises NotImplementedError so missing functionality is loud, not silent.
#
# Note on commitid: bare `commitid=` and `commitid__startswith=` map to
# `commitid_prefix`, while `commitid__exact=` maps to its own
# `commitid_exact` bucket. Both still pushdown the supplied value into
# the SCAN MATCH (an exact value is a valid prefix), but the post-scan
# predicate enforces equality vs. prefix so `__exact` keeps its Django
# semantics — see Bugbot review on PR #887, where `__exact` was silently
# behaving as a prefix match.
_QUEUE_FILTER_KEYS: dict[str, str] = {
    "family": "family",
    "family__exact": "family",
    "repoid": "repoid",
    "repoid__exact": "repoid",
    "commitid": "commitid_prefix",
    "commitid__exact": "commitid_exact",
    "commitid__startswith": "commitid_prefix",
    "report_type": "report_type",
    "report_type__exact": "report_type",
    "depth__gte": "depth_gte",
    "name__icontains": "name_substring",
    "name__contains": "name_substring",
    # Admin bulk actions (delete_selected, custom actions) narrow the
    # queryset to selected pks via `filter(pk__in=[...])`. We treat that
    # as a direct-by-name lookup that bypasses iter_keys SCAN entirely.
    "pk": "name_exact",
    "pk__exact": "name_exact",
    "pk__in": "name_in",
    "name": "name_exact",
    "name__exact": "name_exact",
    "name__in": "name_in",
}


class RedisQueueQuerySet:
    """Quacks like a Django QuerySet for the admin changelist.

    Filter kwargs in `_QUEUE_FILTER_KEYS` are honoured: `family`, `repoid`,
    and `commitid` are pushed into a tighter SCAN MATCH; the rest
    (`depth__gte`, `name__icontains`, `report_type`) are applied as a
    post-scan filter on materialised rows.
    """

    def __init__(
        self,
        model,
        *,
        ordering: tuple[str, ...] = (),
        filters: dict[str, Any] | None = None,
        category: str = "queue",
        family_exclude: tuple[str, ...] = (),
    ) -> None:
        self.model = model
        self._ordering = ordering
        self._filters: dict[str, Any] = dict(filters or {})
        self._category = category
        # Families that should be hidden from the changelist regardless
        # of any inbound `family` filter. Set by `RedisQueueAdmin` to
        # exclude `celery_broker` so the queue surface (where one row =
        # one queue + scalar depth) doesn't compete with the new
        # `CeleryBrokerQueueAdmin` (where one row = one kombu message).
        self._family_exclude: tuple[str, ...] = tuple(family_exclude)
        self._result_cache: list | None = None

    # ---- Cloning helpers --------------------------------------------------

    def _clone(
        self,
        *,
        ordering: tuple[str, ...] | None = None,
        filters: dict[str, Any] | None = None,
        family_exclude: tuple[str, ...] | None = None,
    ) -> RedisQueueQuerySet:
        return RedisQueueQuerySet(
            self.model,
            ordering=self._ordering if ordering is None else ordering,
            filters={**self._filters, **(filters or {})},
            category=self._category,
            family_exclude=self._family_exclude
            if family_exclude is None
            else family_exclude,
        )

    def all(self) -> RedisQueueQuerySet:
        return self._clone()

    def family_exclude(self, *names: str) -> RedisQueueQuerySet:
        """Hide every key whose family is in `names` from this queryset.

        Used by `RedisQueueAdmin.get_queryset` to keep `celery_broker`
        out of the queue changelist now that those queues have their
        own per-message admin (`CeleryBrokerQueueAdmin`). Implemented
        as a dedicated builder rather than a generic `exclude` so the
        admin's filter pipeline can't accidentally clobber the
        exclusion via a stray `?family=celery_broker` query string —
        the exclusion always wins (`_post_scan_predicate` enforces it
        post-resolve, and `iter_keys` skips the family up front).
        """

        merged = tuple(dict.fromkeys((*self._family_exclude, *names)))
        return self._clone(family_exclude=merged)

    def none(self) -> RedisQueueQuerySet:
        empty = self._clone()
        empty._result_cache = []
        return empty

    def using(self, alias) -> RedisQueueQuerySet:
        # Database alias has no meaning for a Redis-backed queryset; the
        # admin still calls this so we accept and ignore it.
        return self

    # ---- Compatibility shims for django.contrib.admin.utils ---------------

    @property
    def verbose_name(self):
        # `delete_selected` calls `model_ngettext(queryset)` →
        # `model_format_dict(obj)`; since our queryset doesn't subclass
        # `django.db.models.QuerySet`, that helper falls into the
        # `else: opts = obj` branch and reads `verbose_name` directly.
        # Proxy to the model's Meta so the confirmation page renders.
        return self.model._meta.verbose_name

    @property
    def verbose_name_plural(self):
        return self.model._meta.verbose_name_plural

    # ---- Filtering -------------------------------------------------------

    def _interpret_filter_kwargs(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        new_filters: dict[str, Any] = {}
        unsupported: dict[str, Any] = {}
        for key, value in kwargs.items():
            target = _QUEUE_FILTER_KEYS.get(key)
            if target is None:
                unsupported[key] = value
                continue
            if value is None or value == "":
                continue
            if target == "repoid":
                try:
                    new_filters[target] = int(value)
                except (TypeError, ValueError):
                    # An int-only filter against a non-int value can never
                    # match; force the queryset empty rather than crashing.
                    new_filters["__empty__"] = True
            elif target == "depth_gte":
                try:
                    new_filters[target] = int(value)
                except (TypeError, ValueError):
                    new_filters["__empty__"] = True
            elif target == "name_substring":
                new_filters[target] = str(value).lower()
            elif target == "name_in":
                # `pk__in=[...]` from admin bulk actions; merge with any
                # previously-set name_in so chained `.filter()` calls
                # narrow the set rather than overwriting.
                names = list(value) if not isinstance(value, str) else [value]
                existing = self._filters.get("name_in")
                if existing is not None:
                    new_filters[target] = [n for n in names if n in set(existing)]
                else:
                    new_filters[target] = names
            elif target == "name_exact":
                new_filters[target] = str(value)
            else:
                new_filters[target] = str(value)
        if unsupported:
            raise NotImplementedError(
                "RedisQueueQuerySet.filter received unsupported kwargs: "
                f"{sorted(unsupported)!r}; supported: {sorted(_QUEUE_FILTER_KEYS)!r}"
            )
        return new_filters

    def filter(self, *args, **kwargs) -> RedisQueueQuerySet:
        if args:
            raise NotImplementedError(
                "RedisQueueQuerySet.filter does not accept positional Q objects"
            )
        if not kwargs:
            return self._clone()
        return self._clone(filters=self._interpret_filter_kwargs(kwargs))

    def exclude(self, *args, **kwargs) -> RedisQueueQuerySet:
        if not args and not kwargs:
            return self._clone()
        raise NotImplementedError("RedisQueueQuerySet.exclude is added in milestone 5")

    def get(self, *args, **kwargs):
        """Lookup a single queue by `pk=` / `name=` (or their `__exact` variants).

        The admin's `get_object` calls `queryset.get(name=<key>)` when the
        operator clicks a row on the changelist; we reuse the explicit-name
        fast path in `_fetch_all` so this never triggers a SCAN.
        """

        if args:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching positional args is not supported"
            )
        filtered = self.filter(**kwargs)
        items = filtered._fetch_all()
        if not items:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching {kwargs!r} does not exist"
            )
        if len(items) > 1:
            raise self.model.MultipleObjectsReturned(
                f"get() returned {len(items)} {self.model.__name__} rows"
            )
        return items[0]

    # ---- Ordering --------------------------------------------------------

    def order_by(self, *fields: str) -> RedisQueueQuerySet:
        return self._clone(ordering=tuple(fields))

    # ---- Materialization -------------------------------------------------

    def _post_scan_predicate(self, obj: Any) -> bool:
        f = self._filters
        # Family pushdown happens in `iter_keys`, but the `name_exact` /
        # `name_in` shortcut in `_fetch_all` skips that and resolves
        # ownership purely via `find_family(name)`. Without re-checking
        # here, a `family__exact=celery_broker name__exact=<queue>`
        # filter (the shape `CeleryQueueFilter` builds) would silently
        # surface the wrong row if some other family ever picked a
        # name colliding with a celery queue. Cheap belt-and-braces
        # check; harmless for the SCAN path where `iter_keys` already
        # filtered by family.
        family = f.get("family")
        if family and obj.family != family:
            return False
        if self._family_exclude and obj.family in self._family_exclude:
            # Belt-and-braces: `iter_keys` already skips excluded
            # families up front, but the `name_exact` / `name_in`
            # shortcut bypasses `iter_keys` entirely, so re-check here
            # to keep the shortcut honest.
            return False
        depth_gte = f.get("depth_gte")
        if depth_gte is not None and (obj.depth or 0) < depth_gte:
            return False
        name_substring = f.get("name_substring")
        if name_substring and name_substring not in (obj.name or "").lower():
            return False
        report_type = f.get("report_type")
        if report_type and (obj.report_type or "") != report_type:
            return False
        # Redis glob `*` matches `/`, so a `uploads/*/abc*` pushdown can
        # surface `uploads/<r>/<other>/abc-something`; double-check the
        # parsed commitid starts with the prefix the caller asked for.
        commitid_prefix = f.get("commitid_prefix")
        if commitid_prefix and not (obj.commitid or "").startswith(commitid_prefix):
            return False
        # `commitid__exact` shares the SCAN pushdown with `__startswith`
        # (an exact value is a valid prefix), but enforces equality
        # post-scan so its Django semantics are preserved.
        commitid_exact = f.get("commitid_exact")
        if commitid_exact and (obj.commitid or "") != commitid_exact:
            return False
        # Families without a repoid in their key shape (e.g. intermediate-
        # report) opt out of repoid SCAN pushdown by returning None from
        # `pattern_for`, but a defensive post-scan check guards future
        # families that *do* scan and need the value re-verified.
        repoid = f.get("repoid")
        if repoid is not None and obj.repoid != repoid:
            return False
        return True

    def _fetch_all(self) -> list:
        if self._result_cache is not None:
            return self._result_cache

        if self._filters.get("__empty__"):
            self._result_cache = []
            return []

        # Cache one client per connection-kind so families that share a
        # kind reuse a single Redis client across the page (the broker
        # kind is the only non-default kind today, but the cache scales
        # transparently if more get added).
        clients: dict[str, Any] = {}

        def _client_for(family: Family) -> Any:
            kind = family.connection_kind
            if kind not in clients:
                clients[kind] = _conn.get_connection(kind=kind)
            return clients[kind]

        # `pk__in=[...]` / `pk=name` from admin bulk actions: skip SCAN
        # and resolve each requested name directly against the family
        # registry. This keeps the bulk-delete confirmation page fast
        # even on a large keyspace, and avoids re-scanning for keys we
        # already have names for.
        explicit_names: list[str] | None = None
        name_in = self._filters.get("name_in")
        name_exact = self._filters.get("name_exact")
        if name_in is not None:
            explicit_names = list(name_in)
        elif name_exact is not None:
            explicit_names = [name_exact]

        if explicit_names is not None:
            items = []
            for name in explicit_names:
                family = find_family(name)
                if family is None:
                    continue
                if family.category != self._category:
                    continue
                redis = _client_for(family)
                if not redis.exists(name):
                    continue
                items.append(_build_redis_queue(self.model, name, family, redis))
        else:
            # Don't pass an explicit redis to iter_keys here so it routes
            # SCAN / EXISTS against the connection each family owns. The
            # builder picks the matching client per yielded family.
            #
            # `commitid__exact` value is a valid prefix for SCAN MATCH
            # purposes (an exact 40-char SHA tightens the pattern even
            # more than a prefix); the post-scan predicate is what
            # actually enforces the exact-vs-prefix semantic split.
            scan_commitid = self._filters.get("commitid_prefix") or self._filters.get(
                "commitid_exact"
            )
            items = [
                _build_redis_queue(self.model, key, family, _client_for(family))
                for key, family in iter_keys(
                    family=self._filters.get("family"),
                    family_exclude=self._family_exclude or None,
                    repoid=self._filters.get("repoid"),
                    commitid_prefix=scan_commitid,
                    category=self._category,
                )
            ]

        items = [obj for obj in items if self._post_scan_predicate(obj)]

        for field in reversed(self._ordering):
            reverse = field.startswith("-")
            attr = field.lstrip("-")
            items.sort(
                key=lambda obj, attr=attr: _ordering_key(obj, attr), reverse=reverse
            )

        self._result_cache = items
        return items

    def __iter__(self) -> Iterator:
        return iter(self._fetch_all())

    def iterator(self, chunk_size: int | None = None) -> Iterator:
        # The admin sometimes calls `.iterator()`; honour it but materialize
        # since the dataset is bounded by MAX_SCAN_KEYS.
        return iter(self._fetch_all())

    def __len__(self) -> int:
        return len(self._fetch_all())

    def count(self) -> int:
        return len(self._fetch_all())

    def __getitem__(self, item):
        return self._fetch_all()[item]

    def __bool__(self) -> bool:
        return bool(self._fetch_all())

    # ---- Mutations (M5) --------------------------------------------------

    def delete(self):
        raise NotImplementedError(
            "redis_admin.RedisQueueQuerySet.delete is added in milestone 5"
        )

    # ---- Admin compatibility shims ---------------------------------------

    @property
    def query(self):
        # The admin occasionally pokes at `.query` (e.g. for ordering hints);
        # returning a tiny stub keeps `ChangeList.get_ordering` happy.
        class _Query:
            order_by = self._ordering
            select_related = False
            distinct = False

        return _Query()

    @property
    def ordered(self) -> bool:
        return bool(self._ordering)

    @property
    def db(self) -> str:
        return "default"


def _truncate_for_display(value: str, *, cap: int | None = None) -> str:
    if cap is None:
        cap = redis_admin_settings.MAX_DECODE_BYTES
    if len(value) <= cap:
        return value
    truncated_count = len(value) - cap
    return f"{value[:cap]}\u2026 (+{truncated_count} chars truncated)"


class RedisItemQuerySet:
    """Fake QuerySet for items inside a single Redis-backed container.

    The admin URL `?queue_name__exact=<key>` is the only supported filter.
    Without it, this queryset is empty by design so we never accidentally
    fan out into every item in Redis.
    """

    def __init__(
        self,
        model,
        *,
        queue_name: str | None = None,
        ordering: tuple[str, ...] = (),
    ) -> None:
        self.model = model
        self.queue_name = queue_name
        self._ordering = ordering
        # Cached materialized snapshot for SET/HASH/STRING reads. LIST
        # reads remain LRANGE-paginated so we don't bother caching.
        self._snapshot: list | None = None

    # ---- Cloning helpers --------------------------------------------------

    def _clone(
        self,
        *,
        queue_name: str | None = _UNSET,
        ordering: tuple[str, ...] | None = None,
    ) -> RedisItemQuerySet:
        return RedisItemQuerySet(
            self.model,
            queue_name=self.queue_name if queue_name is _UNSET else queue_name,
            ordering=self._ordering if ordering is None else ordering,
        )

    def all(self) -> RedisItemQuerySet:
        return self._clone()

    def none(self) -> RedisItemQuerySet:
        return self._clone(queue_name=None)

    def using(self, alias) -> RedisItemQuerySet:
        return self

    # ---- Filtering -------------------------------------------------------

    def filter(self, *args, **kwargs) -> RedisItemQuerySet:
        kwargs = dict(kwargs)
        new_queue_name = self.queue_name
        for key in ("queue_name__exact", "queue_name"):
            if key in kwargs:
                new_queue_name = kwargs.pop(key)
        if args or kwargs:
            raise NotImplementedError(
                "RedisItemQuerySet.filter only supports queue_name[/__exact] in M2; "
                f"got args={args!r}, kwargs={list(kwargs)!r}"
            )
        return self._clone(queue_name=new_queue_name)

    def exclude(self, *args, **kwargs) -> RedisItemQuerySet:
        if not args and not kwargs:
            return self._clone()
        raise NotImplementedError("RedisItemQuerySet.exclude is added in milestone 5")

    def get(self, *args, **kwargs):
        """Resolve a single item by its `pk_token` (`<queue>#<locator>`).

        Used by Django admin's `get_object` when the operator clicks an
        item row to open the per-item inspector. Locator shape varies by
        Redis type:

        * list   → numeric index (e.g. `uploads/1/abc#0`)
        * set    → `m:<idx>` into the sorted snapshot
        * hash   → `f:<field>` for the literal field name
        * string → `v` (singleton)
        """

        if args:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} positional get() is not supported"
            )
        pk = (
            kwargs.pop("pk", None)
            or kwargs.pop("pk__exact", None)
            or kwargs.pop("pk_token", None)
            or kwargs.pop("pk_token__exact", None)
        )
        if kwargs:
            raise NotImplementedError(
                "RedisItemQuerySet.get only accepts pk/pk_token; "
                f"got {sorted(kwargs)!r}"
            )
        if pk is None:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching no kwargs does not exist"
            )
        return self._get_one(str(pk))

    def _get_one(self, pk_token: str):
        queue_name, sep, locator = pk_token.rpartition("#")
        if not sep or not queue_name or not locator:
            raise self.model.DoesNotExist(
                f"pk_token must look like 'queue#locator'; got {pk_token!r}"
            )

        bound = self._clone(queue_name=queue_name)
        redis = bound._connection()
        if not redis.exists(queue_name):
            raise self.model.DoesNotExist(f"Redis key {queue_name!r} does not exist")

        kind = bound._resolve_type(redis)

        if kind == "list":
            try:
                idx = int(locator)
            except ValueError as exc:
                raise self.model.DoesNotExist(
                    f"list pk_token must be 'queue#<index>'; got {locator!r}"
                ) from exc
            raw = redis.lindex(queue_name, idx)
            if raw is None:
                raise self.model.DoesNotExist(
                    f"index {idx} out of range for list {queue_name!r}"
                )
            return self.model(
                pk_token=pk_token,
                queue_name=queue_name,
                index_or_field=str(idx),
                raw_value=_truncate_for_display(bound._decode_with_family(raw)),
            )

        if kind == "set":
            if not locator.startswith("m:"):
                raise self.model.DoesNotExist(
                    f"set pk_token must be 'queue#m:<idx>'; got {locator!r}"
                )
            try:
                idx = int(locator[2:])
            except ValueError as exc:
                raise self.model.DoesNotExist(
                    f"set pk_token index must be int; got {locator!r}"
                ) from exc
            snapshot = bound._materialize_set(redis)
            if idx < 0 or idx >= len(snapshot):
                raise self.model.DoesNotExist(
                    f"set member index {idx} out of range for {queue_name!r}"
                )
            return snapshot[idx]

        if kind == "hash":
            if not locator.startswith("f:"):
                raise self.model.DoesNotExist(
                    f"hash pk_token must be 'queue#f:<field>'; got {locator!r}"
                )
            field = locator[2:]
            raw = redis.hget(queue_name, field)
            if raw is None:
                raise self.model.DoesNotExist(
                    f"hash field {field!r} does not exist on {queue_name!r}"
                )
            return self.model(
                pk_token=pk_token,
                queue_name=queue_name,
                index_or_field=field,
                raw_value=_truncate_for_display(_decode_value(raw)),
            )

        if kind == "string":
            if locator != "v":
                raise self.model.DoesNotExist(
                    f"string pk_token must be 'queue#v'; got {locator!r}"
                )
            snapshot = bound._materialize_string(redis)
            if not snapshot:
                raise self.model.DoesNotExist(f"string {queue_name!r} does not exist")
            return snapshot[0]

        raise self.model.DoesNotExist(
            f"unsupported Redis type {kind!r} for {queue_name!r}"
        )

    def order_by(self, *fields: str) -> RedisItemQuerySet:
        return self._clone(ordering=tuple(fields))

    # ---- Materialization -------------------------------------------------

    def _resolve_family(self) -> Family | None:
        if not self.queue_name:
            return None
        return find_family(self.queue_name)

    def _connection(self) -> Any:
        """Return the Redis client that owns this queryset's queue.

        Routes off the resolved family's `connection_kind` so a queue
        belonging to `celery_broker` reads from the broker Redis instead
        of the (default) cache Redis. Unknown families fall back to the
        default kind, which preserves prior behavior for any operator
        URL-tampering paths.
        """

        family = self._resolve_family()
        kind = family.connection_kind if family is not None else "default"
        return _conn.get_connection(kind=kind)

    def _resolve_type(self, redis) -> str | None:
        """Best-effort lookup of the Redis type for `self.queue_name`."""

        if not self.queue_name:
            return None
        family = self._resolve_family()
        if family is not None:
            return family.redis_type
        raw_type = redis.type(self.queue_name)
        kind = _decode_value(raw_type) if raw_type is not None else "none"
        if kind == "none":
            return None
        return kind

    def _decode_with_family(self, raw_value: bytes | str) -> str:
        decoded = _decode_value(raw_value)
        family = self._resolve_family()
        if family is not None and family.decode_value is not None:
            try:
                decoded = family.decode_value(decoded)
            except Exception:  # pragma: no cover - decoder bug shouldn't 500
                pass
        return decoded

    def _build_list_items(self, redis, start: int, raw_values: list) -> list:
        return [
            self.model(
                pk_token=f"{self.queue_name}#{start + offset}",
                queue_name=self.queue_name,
                index_or_field=str(start + offset),
                raw_value=_truncate_for_display(self._decode_with_family(value)),
            )
            for offset, value in enumerate(raw_values)
        ]

    @sentry_sdk.trace
    def _list_slice(self, redis, start: int, stop: int) -> list:
        if stop <= start:
            return []
        cap = redis_admin_settings.ITEM_PAGE_SIZE
        capped_stop = min(stop, start + cap)
        raw = redis.lrange(self.queue_name, start, capped_stop - 1)
        return self._build_list_items(redis, start, raw)

    # ---- SET / HASH / STRING readers (M4) --------------------------------
    #
    # SETs and HASHes are read with SSCAN/HSCAN bounded by MAX_ITEMS_PER_KEY
    # so a pathologically large container doesn't OOM the api process. The
    # full snapshot is sorted lexically and paginated in Python; this gives
    # the admin a stable page-to-page ordering that SSCAN cursors cannot
    # provide on their own. Snapshots are cached per-queryset so repeated
    # `count()` / `__getitem__` calls don't re-stream Redis.

    @sentry_sdk.trace
    def _materialize_set(self, redis) -> list:
        if self._snapshot is not None:
            return self._snapshot
        cap = redis_admin_settings.MAX_ITEMS_PER_KEY
        members: list[str] = []
        for raw in redis.sscan_iter(
            self.queue_name, count=redis_admin_settings.SCAN_COUNT
        ):
            members.append(_decode_value(raw))
            if len(members) >= cap:
                break
        members.sort()
        self._snapshot = [
            self.model(
                pk_token=f"{self.queue_name}#m:{idx}",
                queue_name=self.queue_name,
                index_or_field=member if len(member) <= 64 else member[:61] + "...",
                raw_value=_truncate_for_display(member),
            )
            for idx, member in enumerate(members)
        ]
        return self._snapshot

    @sentry_sdk.trace
    def _materialize_hash(self, redis) -> list:
        if self._snapshot is not None:
            return self._snapshot
        cap = redis_admin_settings.MAX_ITEMS_PER_KEY
        pairs: list[tuple[str, str]] = []
        for raw_field, raw_value in redis.hscan_iter(
            self.queue_name, count=redis_admin_settings.SCAN_COUNT
        ):
            pairs.append((_decode_value(raw_field), _decode_value(raw_value)))
            if len(pairs) >= cap:
                break
        pairs.sort(key=lambda fv: fv[0])
        self._snapshot = [
            self.model(
                pk_token=f"{self.queue_name}#f:{field}",
                queue_name=self.queue_name,
                index_or_field=field,
                raw_value=_truncate_for_display(value),
            )
            for field, value in pairs
        ]
        return self._snapshot

    @sentry_sdk.trace
    def _materialize_string(self, redis) -> list:
        if self._snapshot is not None:
            return self._snapshot
        raw = redis.get(self.queue_name)
        if raw is None:
            self._snapshot = []
            return self._snapshot
        self._snapshot = [
            self.model(
                pk_token=f"{self.queue_name}#v",
                queue_name=self.queue_name,
                index_or_field="(value)",
                raw_value=_truncate_for_display(_decode_value(raw)),
            )
        ]
        return self._snapshot

    def count(self) -> int:
        if not self.queue_name:
            return 0
        redis = self._connection()
        kind = self._resolve_type(redis)
        if kind == "list":
            return int(redis.llen(self.queue_name) or 0)
        if kind == "set":
            return int(redis.scard(self.queue_name) or 0)
        if kind == "hash":
            return int(redis.hlen(self.queue_name) or 0)
        if kind == "string":
            return 1 if redis.exists(self.queue_name) else 0
        return 0

    def __len__(self) -> int:
        return self.count()

    def __iter__(self) -> Iterator:
        if not self.queue_name:
            return iter(())
        return iter(self[: redis_admin_settings.ITEM_PAGE_SIZE])

    def iterator(self, chunk_size: int | None = None) -> Iterator:
        return iter(self)

    def __bool__(self) -> bool:
        return self.count() > 0

    def _slice(self, redis, kind: str | None, start: int, stop: int) -> list:
        if stop <= start:
            return []
        if kind == "list":
            return self._list_slice(redis, start, stop)
        if kind == "set":
            return self._materialize_set(redis)[start:stop]
        if kind == "hash":
            return self._materialize_hash(redis)[start:stop]
        if kind == "string":
            return self._materialize_string(redis)[start:stop]
        return []

    def __getitem__(self, item):
        if not self.queue_name:
            return [] if isinstance(item, slice) else None
        redis = self._connection()
        kind = self._resolve_type(redis)
        if kind not in ("list", "set", "hash", "string"):
            return [] if isinstance(item, slice) else None
        if isinstance(item, slice):
            start = item.start or 0
            stop = (
                item.stop
                if item.stop is not None
                else start + redis_admin_settings.ITEM_PAGE_SIZE
            )
            return self._slice(redis, kind, start, stop)
        if isinstance(item, int):
            page = self._slice(redis, kind, item, item + 1)
            if not page:
                raise IndexError(item)
            return page[0]
        raise TypeError(f"unsupported index type: {type(item)!r}")

    # ---- Mutations (M5) --------------------------------------------------

    def delete(self):
        raise NotImplementedError("RedisItemQuerySet.delete is added in milestone 5")

    # ---- Admin compatibility shims ---------------------------------------

    @property
    def query(self):
        ordering = self._ordering

        class _Query:
            order_by = ordering
            select_related = False
            distinct = False

        return _Query()

    @property
    def ordered(self) -> bool:
        return bool(self._ordering)

    @property
    def db(self) -> str:
        return "default"


# ---- Celery broker per-message queryset (M6) -------------------------------
#
# Backs `CeleryBrokerQueue`, the celery-broker-specific drill-down: one
# row per message inside one celery queue, with the kombu envelope
# already parsed into structured columns. Deliberately not a subclass of
# `RedisItemQuerySet` because field shape, pk conventions, and
# connection routing all differ — `RedisItemQuerySet` stays the
# generic-Redis-types fallback for non-celery families.

# Names of body kwargs that are routinely huge (megabytes of base64 YAML)
# and would blow past `MAX_DECODE_BYTES` if rendered verbatim in
# `payload_preview`. We replace them with a `<truncated: N chars>`
# placeholder so the preview stays scannable; operators who genuinely
# need the raw envelope can fall through to the existing `RedisQueueItem`
# admin.
_LARGE_KWARGS_KEYS: frozenset[str] = frozenset(
    {
        "commit_yaml",
        "commit_yaml_dict",
        "current_yaml",
        "processing_results",
        "report_json",
        "raw_upload",
    }
)

# Per-value cap inside `payload_preview` so a single oversized kwarg
# can't monopolise the line even when it isn't on the known-large list.
_PAYLOAD_PER_VALUE_CAP: int = 256


_CELERY_FILTER_KEYS: dict[str, str] = {
    "queue_name": "queue_name",
    "queue_name__exact": "queue_name",
    "repoid": "repoid",
    "repoid__exact": "repoid",
    "commitid": "commitid_prefix",
    "commitid__exact": "commitid_exact",
    "commitid__startswith": "commitid_prefix",
    "task_name": "task_name_exact",
    "task_name__exact": "task_name_exact",
    "task_name__icontains": "task_name_substring",
    "task_id": "task_id_exact",
    "task_id__exact": "task_id_exact",
    "ownerid": "ownerid",
    "ownerid__exact": "ownerid",
    "pullid": "pullid",
    "pullid__exact": "pullid",
    # Admin bulk-action `filter(pk__in=[...])` shortcut so
    # `clear_selected` confirmation pages can re-resolve rows by
    # pk_token. Each pk_token is `<queue>#<idx>`, so we cross-check
    # below that every selected pk shares the same queue.
    "pk__in": "pk_in",
}


def _summarise_kwargs_for_preview(
    kwargs: dict[str, Any] | None,
    *,
    cap: int | None = None,
) -> str:
    """Render kombu body kwargs as a one-line preview string.

    Known-large keys (`commit_yaml`, `processing_results`, …) are
    swapped for a `<truncated: N chars>` placeholder so the preview
    stays under the global `MAX_DECODE_BYTES` cap; everything else is
    JSON-serialised, with each value individually truncated so one
    oversized field can't monopolise the line.
    """

    if not kwargs:
        return ""
    summarised: dict[str, Any] = {}
    for key, value in kwargs.items():
        if key in _LARGE_KWARGS_KEYS:
            try:
                marshalled = json.dumps(value, default=str)
            except (TypeError, ValueError):
                marshalled = str(value)
            summarised[key] = f"<truncated: {len(marshalled)} chars>"
            continue
        try:
            marshalled = json.dumps(value, default=str)
        except (TypeError, ValueError):
            marshalled = str(value)
        if len(marshalled) > _PAYLOAD_PER_VALUE_CAP:
            summarised[key] = (
                f"{marshalled[:_PAYLOAD_PER_VALUE_CAP]}\u2026 "
                f"(+{len(marshalled) - _PAYLOAD_PER_VALUE_CAP} chars)"
            )
        else:
            summarised[key] = value
    try:
        rendered = json.dumps(summarised, default=str)
    except (TypeError, ValueError):
        rendered = str(summarised)
    return _truncate_for_display(rendered, cap=cap)


@dataclass(frozen=True)
class FrequencyBucket:
    """One row of the celery_broker drill-down frequency chart.

    A bucket aggregates messages by `(task_name, repoid, commitid)`.
    Grouping by `task_name` matters on shared queues like the default
    `celery` queue where multiple task classes coexist — without it,
    a "clear all messages for repo X commit Y" action would silently
    drop unrelated tasks routed through the same queue. Buckets where
    all three axes are `None` are skipped at construction time, since
    the chart's row-level click target maps directly to a
    `clear-by-filter/?queue_name=...&task_name=...&repoid=...&commitid=...`
    URL and an all-empty row would produce an empty-scope clear with
    no useful semantic.

    `pct` is the bucket's share of the total queue depth, computed
    against the snapshot the chart was built from (i.e. against the
    `LRANGE 0 MAX_ITEMS_PER_KEY-1` materialisation, not against
    `LLEN`). For queues larger than `MAX_ITEMS_PER_KEY` the chart
    template surfaces a banner so operators know the percentages
    are over the visible window.
    """

    task_name: str | None
    repoid: int | None
    commitid: str | None
    count: int
    pct: float


# Default cap for the chart's "top N" cut. Large enough to cover the
# head of any realistic distribution, small enough to keep the
# rendered table scannable above the changelist.
_FREQUENCY_TOP_DEFAULT: int = 20


class CeleryBrokerQueueQuerySet:
    """Fake QuerySet over messages inside one celery_broker queue.

    Required pre-filter: `queue_name__exact=<queue>`. Without it the
    queryset short-circuits to empty and the admin renders an info
    message — the alternative would be fanning out across every
    well-known celery queue, which is unbounded by design.

    Materialisation is `LRANGE 0 MAX_ITEMS_PER_KEY-1` once, parse
    each element via `parse_celery_envelope`, then apply Python-side
    filters and ordering. The result snapshot is cached on the
    queryset instance so repeated `count()` / `__getitem__` /
    `__iter__` calls during a single admin request reuse it.

    Connection routing is hard-coded to `kind="broker"` since this
    queryset is celery-only; no `find_family` round-trip needed.
    """

    def __init__(
        self,
        model,
        *,
        queue_name: str | None = None,
        ordering: tuple[str, ...] = (),
        filters: dict[str, Any] | None = None,
    ) -> None:
        self.model = model
        self.queue_name = queue_name
        self._ordering = ordering
        self._filters: dict[str, Any] = dict(filters or {})
        self._result_cache: list | None = None

    # ---- Cloning helpers --------------------------------------------------

    def _clone(
        self,
        *,
        queue_name: Any = _UNSET,
        ordering: tuple[str, ...] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> CeleryBrokerQueueQuerySet:
        return CeleryBrokerQueueQuerySet(
            self.model,
            queue_name=self.queue_name if queue_name is _UNSET else queue_name,
            ordering=self._ordering if ordering is None else ordering,
            filters=(
                {**self._filters, **(filters or {})} if filters else dict(self._filters)
            ),
        )

    def all(self) -> CeleryBrokerQueueQuerySet:
        return self._clone()

    def none(self) -> CeleryBrokerQueueQuerySet:
        empty = self._clone(queue_name=None)
        empty._result_cache = []
        return empty

    def using(self, alias) -> CeleryBrokerQueueQuerySet:
        return self

    # ---- Compatibility shims for django.contrib.admin.utils ---------------

    @property
    def verbose_name(self):
        return self.model._meta.verbose_name

    @property
    def verbose_name_plural(self):
        return self.model._meta.verbose_name_plural

    # ---- Filtering -------------------------------------------------------

    def _interpret_filter_kwargs(
        self, kwargs: dict[str, Any]
    ) -> tuple[Any, dict[str, Any]]:
        """Translate Django-style lookups into our internal filter buckets.

        `new_queue_name` stays `_UNSET` when no `queue_name`-shaped
        lookup was supplied so `_clone` can preserve the existing one.
        """

        new_queue_name: Any = _UNSET
        new_filters: dict[str, Any] = {}
        for key, value in kwargs.items():
            target = _CELERY_FILTER_KEYS.get(key)
            if target is None:
                raise NotImplementedError(
                    "CeleryBrokerQueueQuerySet.filter only supports "
                    f"{sorted(_CELERY_FILTER_KEYS)}; got {key!r}"
                )
            if target == "queue_name":
                new_queue_name = None if value is None or value == "" else str(value)
                continue
            if value is None or value == "":
                continue
            if target in ("repoid", "ownerid", "pullid"):
                try:
                    new_filters[target] = int(value)
                except (TypeError, ValueError):
                    new_filters["__empty__"] = True
            elif target in ("commitid_prefix", "commitid_exact"):
                new_filters[target] = str(value)
            elif target in ("task_name_exact", "task_id_exact"):
                new_filters[target] = str(value)
            elif target == "task_name_substring":
                new_filters[target] = str(value).lower()
            elif target == "pk_in":
                indexes: set[int] = set()
                queue_names: set[str] = set()
                bad = False
                for raw in value or ():
                    raw_str = str(raw)
                    queue, sep, idx = raw_str.rpartition("#")
                    if not sep or not queue or not idx:
                        bad = True
                        break
                    if idx == "summary":
                        # Summary pk_tokens identify a queue, not a
                        # message; the only bulk actions we expose are
                        # message-shaped clears, so a `pk__in=[<queue>#summary]`
                        # selection has no valid resolution. Force empty.
                        bad = True
                        break
                    try:
                        indexes.add(int(idx))
                    except ValueError:
                        bad = True
                        break
                    queue_names.add(queue)
                if bad or len(queue_names) > 1:
                    # `pk__in` spanning multiple queues would force a
                    # cross-queue fan-out and break the
                    # `queue_name`-required invariant.
                    new_filters["__empty__"] = True
                elif queue_names:
                    new_queue_name = next(iter(queue_names))
                    new_filters["pk_in_indexes"] = indexes
        return new_queue_name, new_filters

    def filter(self, *args, **kwargs) -> CeleryBrokerQueueQuerySet:
        if args:
            raise NotImplementedError(
                "CeleryBrokerQueueQuerySet.filter does not accept positional Q objects"
            )
        new_queue_name, new_filters = self._interpret_filter_kwargs(kwargs)
        return self._clone(queue_name=new_queue_name, filters=new_filters)

    def exclude(self, *args, **kwargs) -> CeleryBrokerQueueQuerySet:
        if not args and not kwargs:
            return self._clone()
        raise NotImplementedError(
            "CeleryBrokerQueueQuerySet.exclude is not implemented"
        )

    def order_by(self, *fields: str) -> CeleryBrokerQueueQuerySet:
        return self._clone(ordering=tuple(fields))

    def get(self, *args, **kwargs):
        """Resolve a single message by `pk_token = '<queue>#<index>'`."""

        if args:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} positional get() is not supported"
            )
        pk = (
            kwargs.pop("pk", None)
            or kwargs.pop("pk__exact", None)
            or kwargs.pop("pk_token", None)
            or kwargs.pop("pk_token__exact", None)
        )
        if kwargs:
            raise NotImplementedError(
                "CeleryBrokerQueueQuerySet.get only supports pk/pk_token; "
                f"got extra {list(kwargs)!r}"
            )
        if pk is None:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching no kwargs does not exist"
            )
        pk_str = str(pk)
        queue, sep, tail = pk_str.rpartition("#")
        if not sep or not queue or not tail:
            raise self.model.DoesNotExist(
                f"pk_token must look like 'queue#<index>' or 'queue#summary'; "
                f"got {pk_str!r}"
            )
        # Summary pk_tokens (`<queue>#summary`) are emitted by the
        # queue-list landing page; resolving one is rare (`get_object`
        # for the change view, which we don't expose) but kept
        # symmetric with message pks for admin sanity.
        if tail == "summary":
            redis = self._connection()
            depth = int(redis.llen(queue) or 0)
            return self._build_summary_row(queue, depth)
        try:
            idx = int(tail)
        except ValueError as exc:
            raise self.model.DoesNotExist(
                f"pk_token index must be int or 'summary'; got {tail!r}"
            ) from exc
        bound = self._clone(queue_name=queue)
        for row in bound._fetch_all():
            if row.index_in_queue == idx:
                return row
        raise self.model.DoesNotExist(
            f"index {idx} not found in queue {queue!r} "
            f"(may exceed MAX_ITEMS_PER_KEY or have been consumed)"
        )

    # ---- Materialisation -------------------------------------------------

    def _connection(self) -> Any:
        # Celery broker queues always live on the broker Redis; the
        # family registry routes `celery_broker` here too.
        return _conn.get_connection(kind="broker")

    def _build_row(self, idx: int, meta: CeleryEnvelopeMeta) -> Any:
        return self.model(
            pk_token=f"{self.queue_name}#{idx}",
            queue_name=self.queue_name,
            index_in_queue=idx,
            depth=None,
            task_name=meta.task,
            task_id=meta.task_id,
            repoid=meta.repoid,
            commitid=meta.commitid,
            ownerid=meta.ownerid,
            pullid=meta.pullid,
            payload_preview=_summarise_kwargs_for_preview(meta.kwargs),
        )

    def _build_summary_row(self, queue: str, depth: int) -> Any:
        return self.model(
            pk_token=f"{queue}#summary",
            queue_name=queue,
            index_in_queue=None,
            depth=depth,
            task_name=None,
            task_id=None,
            repoid=None,
            commitid=None,
            ownerid=None,
            pullid=None,
            payload_preview="",
        )

    def is_summary_mode(self) -> bool:
        """No `queue_name` filter → render the per-queue summary list.

        Routed off `queue_name` rather than a separate flag so that
        `.filter(queue_name__exact=...)` flips us into per-message mode
        automatically — same toggle the admin's `?queue_name__exact=`
        URL parameter drives.
        """

        return not self.queue_name

    @sentry_sdk.trace
    def _materialise_summary(self) -> list:
        """One row per known celery queue with `depth = LLEN(queue)`.

        Drives the `/admin/redis_admin/celerybrokerqueue/` landing
        page (no `queue_name` filter): operators see every queue and
        its current depth at a glance, then click through to the
        per-message drill-down. The set of queues is the same one
        `CeleryQueueFilter` enumerates, so the picker stays in sync
        with the admin's other surfaces.

        Empty queues are kept in the list so an operator who knows a
        queue exists but is currently drained doesn't lose the entry
        point — `LLEN` returns 0 for both empty and missing keys, and
        we treat them identically here.
        """

        redis = self._connection()
        rows: list = []
        for name in _celery_queue_names():
            try:
                depth = int(redis.llen(name) or 0)
            except Exception:  # pragma: no cover - defensive
                depth = 0
            rows.append(self._build_summary_row(name, depth))
        return rows

    @sentry_sdk.trace
    def _materialise(self) -> list:
        if self.is_summary_mode():
            return self._materialise_summary()
        redis = self._connection()
        if not redis.exists(self.queue_name):
            return []
        cap = redis_admin_settings.MAX_ITEMS_PER_KEY
        # `LRANGE 0 cap-1` rather than streaming because celery queues
        # are bounded in practice and a single round-trip beats paging
        # for the typical "operator filtered to one repoid" case where
        # filtering happens client-side anyway.
        raw_values = redis.lrange(self.queue_name, 0, cap - 1)
        rows: list = []
        for offset, raw in enumerate(raw_values):
            decoded = _decode_value(raw)
            meta = parse_celery_envelope(decoded)
            rows.append(self._build_row(offset, meta))
        return rows

    def _matches_filters(self, row: Any) -> bool:
        f = self._filters
        if f.get("__empty__"):
            return False
        # Summary rows don't carry message-shaped fields, so the
        # message-level predicates would always reject them. Skip the
        # message filters entirely in summary mode — the only
        # narrowing applied there is whatever `queue_name` the
        # caller already pinned.
        if row.depth is not None:
            return True
        repoid = f.get("repoid")
        if repoid is not None and row.repoid != repoid:
            return False
        ownerid = f.get("ownerid")
        if ownerid is not None and row.ownerid != ownerid:
            return False
        pullid = f.get("pullid")
        if pullid is not None and row.pullid != pullid:
            return False
        commit_exact = f.get("commitid_exact")
        if commit_exact and (row.commitid or "") != commit_exact:
            return False
        commit_prefix = f.get("commitid_prefix")
        if commit_prefix and not (row.commitid or "").startswith(commit_prefix):
            return False
        task_exact = f.get("task_name_exact")
        if task_exact and (row.task_name or "") != task_exact:
            return False
        task_substring = f.get("task_name_substring")
        if task_substring and task_substring not in (row.task_name or "").lower():
            return False
        task_id_exact = f.get("task_id_exact")
        if task_id_exact and (row.task_id or "") != task_id_exact:
            return False
        pk_in_indexes = f.get("pk_in_indexes")
        if pk_in_indexes is not None and row.index_in_queue not in pk_in_indexes:
            return False
        return True

    def _fetch_all(self) -> list:
        if self._result_cache is not None:
            return self._result_cache
        if self._filters.get("__empty__"):
            self._result_cache = []
            return []
        rows = self._materialise()
        rows = [r for r in rows if self._matches_filters(r)]
        for field in reversed(self._ordering):
            reverse = field.startswith("-")
            attr = field.lstrip("-")
            rows.sort(
                key=lambda obj, attr=attr: _ordering_key(obj, attr), reverse=reverse
            )
        self._result_cache = rows
        return rows

    def __iter__(self) -> Iterator:
        return iter(self._fetch_all())

    def iterator(self, chunk_size: int | None = None) -> Iterator:
        return iter(self._fetch_all())

    def __len__(self) -> int:
        return len(self._fetch_all())

    def count(self) -> int:
        return len(self._fetch_all())

    def __getitem__(self, item):
        return self._fetch_all()[item]

    def __bool__(self) -> bool:
        return bool(self._fetch_all())

    def delete(self):
        # Real deletion goes through `services.celery_broker_clear`
        # so the LSET-tombstone path runs and audit logs capture the
        # operation. The admin's `clear_selected` action calls that
        # service directly.
        raise NotImplementedError(
            "CeleryBrokerQueueQuerySet.delete is not supported; use "
            "redis_admin.services.celery_broker_clear"
        )

    # ---- Frequency aggregation (M6.1) ------------------------------------

    def frequency_by_task_repo_commit(
        self, *, top: int = _FREQUENCY_TOP_DEFAULT
    ) -> list[FrequencyBucket]:
        """Top `(task_name, repoid, commitid)` triples by message count.

        Drives the drill-down page's frequency chart. Pure aggregation
        over rows already materialised by `_fetch_all`, so calling this
        on a queryset that the changelist already iterated reuses the
        cached snapshot rather than re-issuing `LRANGE`.

        Buckets where all three axes are `None` are dropped — the
        chart's click target maps to
        `clear-by-filter/?task_name=...&repoid=...&commitid=...`, and
        an all-empty row would produce an empty-scope clear. Sort
        order is stable across ties: `(count desc, task_name asc,
        repoid asc, commitid asc)` so two runs against the same Redis
        snapshot produce identical row order (no flicker between page
        reloads).
        """

        if self.is_summary_mode():
            # Summary rows don't carry message-shaped fields.
            return []
        rows = self._fetch_all()
        total = len(rows)
        if total == 0:
            return []
        counter: Counter[tuple[str | None, int | None, str | None]] = Counter()
        for row in rows:
            if row.task_name is None and row.repoid is None and row.commitid is None:
                continue
            counter[(row.task_name, row.repoid, row.commitid)] += 1
        if not counter:
            return []
        # `Counter.most_common` doesn't guarantee tie-break order across
        # Python versions, so re-sort with explicit secondary keys
        # (None sorts last via `(is_none, value)` shape).
        ordered = sorted(
            counter.items(),
            key=lambda kv: (
                -kv[1],
                _ordering_tuple_for_str(kv[0][0]),
                _ordering_tuple_for_int(kv[0][1]),
                _ordering_tuple_for_str(kv[0][2]),
            ),
        )
        buckets: list[FrequencyBucket] = []
        for (task_name, repoid, commitid), count in ordered[:top]:
            pct = (count / total) * 100.0 if total else 0.0
            buckets.append(
                FrequencyBucket(
                    task_name=task_name,
                    repoid=repoid,
                    commitid=commitid,
                    count=count,
                    pct=pct,
                )
            )
        return buckets

    # ---- Admin compatibility shims ---------------------------------------

    @property
    def query(self):
        ordering = self._ordering

        class _Query:
            order_by = ordering
            select_related = False
            distinct = False

        return _Query()

    @property
    def ordered(self) -> bool:
        return bool(self._ordering)

    @property
    def db(self) -> str:
        return "default"
