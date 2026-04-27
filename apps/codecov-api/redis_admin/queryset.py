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

from collections.abc import Iterator
from typing import Any

from . import conn as _conn
from . import settings as redis_admin_settings
from .families import Family, find_family, iter_keys

_UNSET: Any = object()


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
_QUEUE_FILTER_KEYS: dict[str, str] = {
    "family": "family",
    "family__exact": "family",
    "repoid": "repoid",
    "repoid__exact": "repoid",
    "commitid": "commitid_prefix",
    "commitid__exact": "commitid_prefix",
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
    ) -> None:
        self.model = model
        self._ordering = ordering
        self._filters: dict[str, Any] = dict(filters or {})
        self._category = category
        self._result_cache: list | None = None

    # ---- Cloning helpers --------------------------------------------------

    def _clone(
        self,
        *,
        ordering: tuple[str, ...] | None = None,
        filters: dict[str, Any] | None = None,
    ) -> RedisQueueQuerySet:
        return RedisQueueQuerySet(
            self.model,
            ordering=self._ordering if ordering is None else ordering,
            filters={**self._filters, **(filters or {})},
            category=self._category,
        )

    def all(self) -> RedisQueueQuerySet:
        return self._clone()

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

        redis = _conn.get_connection()

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
                if not redis.exists(name):
                    continue
                items.append(_build_redis_queue(self.model, name, family, redis))
        else:
            items = [
                _build_redis_queue(self.model, key, family, redis)
                for key, family in iter_keys(
                    redis,
                    family=self._filters.get("family"),
                    repoid=self._filters.get("repoid"),
                    commitid_prefix=self._filters.get("commitid_prefix"),
                    category=self._category,
                )
            ]

        items = [obj for obj in items if self._post_scan_predicate(obj)]

        for field in reversed(self._ordering):
            reverse = field.startswith("-")
            attr = field.lstrip("-")
            items.sort(
                key=lambda obj, attr=attr: getattr(obj, attr) or 0, reverse=reverse
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


def _decode_value(value: bytes | str) -> str:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return value.decode("utf-8", errors="replace")
    return value


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
        raise NotImplementedError("RedisItemQuerySet.get is added in milestone 5")

    def order_by(self, *fields: str) -> RedisItemQuerySet:
        return self._clone(ordering=tuple(fields))

    # ---- Materialization -------------------------------------------------

    def _resolve_family(self) -> Family | None:
        if not self.queue_name:
            return None
        return find_family(self.queue_name)

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
        redis = _conn.get_connection()
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
        redis = _conn.get_connection()
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
