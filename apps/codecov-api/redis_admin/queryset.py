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

    return model(
        name=key,
        family=family.name,
        redis_type=redis_type.upper(),
        depth=int(depth or 0),
        ttl_seconds=ttl_seconds,
    )


class RedisQueueQuerySet:
    """Quacks like a Django QuerySet for the admin changelist."""

    def __init__(self, model, *, ordering: tuple[str, ...] = ()) -> None:
        self.model = model
        self._ordering = ordering
        self._result_cache: list | None = None

    # ---- Cloning helpers --------------------------------------------------

    def _clone(self, *, ordering: tuple[str, ...] | None = None) -> RedisQueueQuerySet:
        return RedisQueueQuerySet(
            self.model,
            ordering=self._ordering if ordering is None else ordering,
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

    # ---- Filtering / lookup (M3+) ----------------------------------------

    def filter(self, *args, **kwargs) -> RedisQueueQuerySet:
        if not args and not kwargs:
            return self._clone()
        raise NotImplementedError(
            "redis_admin.RedisQueueQuerySet.filter is added in milestone 3"
        )

    def exclude(self, *args, **kwargs) -> RedisQueueQuerySet:
        if not args and not kwargs:
            return self._clone()
        raise NotImplementedError(
            "redis_admin.RedisQueueQuerySet.exclude is added in milestone 3"
        )

    def get(self, *args, **kwargs):
        raise NotImplementedError(
            "redis_admin.RedisQueueQuerySet.get is added in milestone 3"
        )

    # ---- Ordering --------------------------------------------------------

    def order_by(self, *fields: str) -> RedisQueueQuerySet:
        return self._clone(ordering=tuple(fields))

    # ---- Materialization -------------------------------------------------

    def _fetch_all(self) -> list:
        if self._result_cache is not None:
            return self._result_cache

        redis = _conn.get_connection()
        items = [
            _build_redis_queue(self.model, key, family, redis)
            for key, family in iter_keys(redis)
        ]

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

    def _resolve_type(self, redis) -> str | None:
        """Best-effort lookup of the Redis type for `self.queue_name`."""

        if not self.queue_name:
            return None
        family = find_family(self.queue_name)
        if family is not None:
            return family.redis_type
        raw_type = redis.type(self.queue_name)
        kind = _decode_value(raw_type) if raw_type is not None else "none"
        if kind == "none":
            return None
        return kind

    def _build_list_items(self, redis, start: int, raw_values: list) -> list:
        return [
            self.model(
                pk_token=f"{self.queue_name}#{start + offset}",
                queue_name=self.queue_name,
                index_or_field=str(start + offset),
                raw_value=_truncate_for_display(_decode_value(value)),
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

    def count(self) -> int:
        if not self.queue_name:
            return 0
        redis = _conn.get_connection()
        kind = self._resolve_type(redis)
        if kind == "list":
            return int(redis.llen(self.queue_name) or 0)
        # SET/HASH/STRING land in M4 alongside their families. For now they
        # report 0 so the changelist renders cleanly.
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

    def __getitem__(self, item):
        if not self.queue_name:
            return [] if isinstance(item, slice) else None
        redis = _conn.get_connection()
        kind = self._resolve_type(redis)
        if kind != "list":
            return [] if isinstance(item, slice) else None
        if isinstance(item, slice):
            start = item.start or 0
            stop = (
                item.stop
                if item.stop is not None
                else start + redis_admin_settings.ITEM_PAGE_SIZE
            )
            return self._list_slice(redis, start, stop)
        if isinstance(item, int):
            page = self._list_slice(redis, item, item + 1)
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
