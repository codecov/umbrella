"""Read-only queryset backing the public-bot usage admin changelist."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from shared.helpers.redis import get_redis_connection
from shared.rate_limits.public_bot import RepoUsageRow, list_repo_usage


def _ordering_key(obj: Any, attr: str) -> tuple:
    value = getattr(obj, attr, None)
    if value is None:
        return (1, "")
    if isinstance(value, int | float):
        return (0, value)
    return (0, str(value))


class PublicBotUsageQuerySet:
    """Quacks like a Django QuerySet for the public-bot usage admin changelist."""

    def __init__(
        self,
        model: type,
        *,
        bot_filter: str | None = None,
        repo_filter: str | None = None,
        ordering: tuple[str, ...] = ("-hits",),
    ) -> None:
        self.model = model
        self.bot_filter = bot_filter
        self.repo_filter = repo_filter
        self._ordering = ordering
        self._result_cache: list[Any] | None = None

    def _clone(
        self,
        *,
        bot_filter: str | None = None,
        repo_filter: str | None = None,
        ordering: tuple[str, ...] | None = None,
    ) -> PublicBotUsageQuerySet:
        return PublicBotUsageQuerySet(
            self.model,
            bot_filter=self.bot_filter if bot_filter is None else bot_filter,
            repo_filter=self.repo_filter if repo_filter is None else repo_filter,
            ordering=self._ordering if ordering is None else ordering,
        )

    def all(self) -> PublicBotUsageQuerySet:
        return self._clone()

    def none(self) -> PublicBotUsageQuerySet:
        empty = self._clone()
        empty._result_cache = []
        return empty

    def using(self, alias) -> PublicBotUsageQuerySet:
        return self

    @property
    def verbose_name(self):
        return self.model._meta.verbose_name

    @property
    def verbose_name_plural(self):
        return self.model._meta.verbose_name_plural

    def _materialise(self) -> list[Any]:
        redis = get_redis_connection()
        rows = list_repo_usage(
            redis,
            bot_filter=self.bot_filter,
            repo_filter=self.repo_filter,
        )
        return [self.model.from_row(row) for row in rows]

    def _fetch_all(self) -> list[Any]:
        if self._result_cache is None:
            rows = self._materialise()
            for field in reversed(self._ordering):
                reverse = field.startswith("-")
                attr = field.lstrip("-")
                rows.sort(
                    key=lambda obj, attr=attr: _ordering_key(obj, attr),
                    reverse=reverse,
                )
            self._result_cache = rows
        return self._result_cache

    def order_by(self, *fields: str) -> PublicBotUsageQuerySet:
        return self._clone(ordering=fields or self._ordering)

    def get(self, **kwargs: Any) -> Any:
        """Retrieve a single object matching *kwargs* from the Redis-backed list.

        Raises ``model.DoesNotExist`` when no match is found and
        ``model.MultipleObjectsReturned`` when more than one match is found,
        mirroring the contract of a real Django QuerySet.
        """
        results = [
            obj
            for obj in self._fetch_all()
            if all(getattr(obj, k, None) == v for k, v in kwargs.items())
        ]
        if not results:
            raise self.model.DoesNotExist(
                f"{self.model.__name__} matching query does not exist."
            )
        if len(results) > 1:
            raise self.model.MultipleObjectsReturned(
                f"get() returned more than one {self.model.__name__} -- "
                f"it returned {len(results)}!"
            )
        return results[0]

    def filter(self, **kwargs: Any) -> PublicBotUsageQuerySet:
        bot_filter = kwargs.get("bot") or kwargs.get("bot__exact") or self.bot_filter
        repo_filter = kwargs.get("repo__icontains") or self.repo_filter
        return self._clone(bot_filter=bot_filter, repo_filter=repo_filter)

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

    @property
    def query(self):
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


def format_reset_at(reset_at: int | None) -> str:
    if reset_at is None:
        return "—"
    return datetime.fromtimestamp(reset_at, tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def row_to_context(row: RepoUsageRow) -> dict[str, Any]:
    return {
        "bot": row.bot,
        "repo": row.repo,
        "hits": row.hits,
        "budget": row.budget,
        "pct_budget": row.pct_budget,
        "reset_at": format_reset_at(row.reset_at),
    }
