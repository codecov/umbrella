"""Fake QuerySet that lets Django admin render Redis keys.

Inspired by `wolph/django-redis-admin`. The Django admin's changelist talks to
its model via a small set of QuerySet methods: `iterator`, `count`,
`__getitem__` (slicing for pagination), `_clone`/`all`/`none`/`using`, and
`order_by`. This module implements just those, dispatching to Redis under the
hood so the model needs no real database table.

Milestone 1 keeps the surface small: enumeration, count, slicing, in-memory
ordering. `filter`/`exclude`/`get`/`delete` deliberately raise
`NotImplementedError` so future milestones know exactly what to wire in.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from . import conn as _conn
from .families import Family, iter_keys


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
