"""Per-repo usage tracking for shared public-bot GitHub tokens.

Shared install-level bots (dedicated apps and YAML-configured bots) serve
uncovered public repos from a single GitHub rate-limit pool per bot. This
module records how many API calls each repo makes to each bot within the
current GitHub rate-limit window (derived from ``X-RateLimit-Reset``).
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from redis import Redis

# entity_name values for shared install-level bots (see shared/bots/helpers.py).
# Defined here explicitly to avoid importing torngit, which would circular-import
# through github.py.
PUBLIC_BOTS = frozenset(
    {
        "read",
        "tokenless",
        "commit",
        "pull",
        "comment",
        "status",
        "tokenless_bot",
    }
)

REPO_USAGE_KEY_PREFIX = "pbrl"
POOL_BUDGET_KEY_PREFIX = "public_bot_budget"
POOL_RESET_KEY_PREFIX = "public_bot_reset"
POOL_STATE_TTL_SECONDS = 120
BUDGET_FALLBACK = 15000
GITHUB_WINDOW_SECONDS = 3600
MAX_SHARE_PER_REPO = 0.05


def is_public_bot(entity_name: str | None) -> bool:
    return bool(entity_name and entity_name in PUBLIC_BOTS)


def repo_cap(budget: int) -> int:
    return max(0, int(budget * MAX_SHARE_PER_REPO))


def is_over_cap(hits: int, budget: int) -> bool:
    return hits >= repo_cap(budget)


# ---------------------------------------------------------------------------
# Data shapes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RepoUsageRow:
    bot: str
    repo: str
    hits: int
    budget: int
    pct_budget: float
    reset_at: int | None
    cap: int | None = None
    over_cap: bool = False


@dataclass(frozen=True)
class GitHubWindow:
    """The active GitHub hourly rate-limit window for one bot."""

    reset_ts: int | None

    @property
    def start_epoch(self) -> int:
        now = int(time.time())
        if self.reset_ts and self.reset_ts > now:
            return self.reset_ts - GITHUB_WINDOW_SECONDS
        return now - GITHUB_WINDOW_SECONDS

    def minute_buckets(self) -> list[int]:
        start_minute = self.start_epoch // 60
        end_minute = int(time.time() // 60)
        if end_minute < start_minute:
            return []
        return list(range(start_minute, end_minute + 1))

    def usage_key_ttl(self) -> int:
        now = int(time.time())
        if self.reset_ts and self.reset_ts > now:
            return max(60, self.reset_ts - now + 60)
        return GITHUB_WINDOW_SECONDS + 60

    def contains_minute(self, minute_bucket: int) -> bool:
        return minute_bucket * 60 >= self.start_epoch


# ---------------------------------------------------------------------------
# Redis store
# ---------------------------------------------------------------------------


def _usage_key(bot: str, repo: str, minute_bucket: int) -> str:
    return f"{REPO_USAGE_KEY_PREFIX}:{bot}:{repo}:{minute_bucket}"


def _parse_usage_key(key: str) -> tuple[str, str, int] | None:
    prefix = f"{REPO_USAGE_KEY_PREFIX}:"
    if not key.startswith(prefix):
        return None
    rest = key[len(prefix) :]
    bot, _, tail = rest.partition(":")
    repo, _, minute_str = tail.rpartition(":")
    try:
        return bot, repo, int(minute_str)
    except ValueError:
        return None


class PublicBotUsageStore:
    """Read and write public-bot pool state and per-repo usage counters."""

    def __init__(self, redis: Redis) -> None:
        self._redis = redis

    def window_for(self, bot: str, reset_ts: int | None = None) -> GitHubWindow:
        if reset_ts is None:
            reset_ts = self.reset_at(bot)
        return GitHubWindow(reset_ts)

    def budget(self, bot: str) -> int:
        value = self._redis.get(f"{POOL_BUDGET_KEY_PREFIX}:{bot}")
        return int(value) if value is not None else BUDGET_FALLBACK

    def reset_at(self, bot: str) -> int | None:
        value = self._redis.get(f"{POOL_RESET_KEY_PREFIX}:{bot}")
        return int(value) if value is not None else None

    def update_pool_from_headers(
        self,
        bot: str,
        remaining: int | str,
        limit: int | str,
        reset: int | str,
    ) -> None:
        self._redis.set(
            f"{POOL_BUDGET_KEY_PREFIX}:{bot}", int(limit), ex=POOL_STATE_TTL_SECONDS
        )
        self._redis.set(
            f"{POOL_RESET_KEY_PREFIX}:{bot}", int(reset), ex=POOL_STATE_TTL_SECONDS
        )

    def hits(self, bot: str, repo: str, *, reset_ts: int | None = None) -> int:
        buckets = self.window_for(bot, reset_ts).minute_buckets()
        if not buckets:
            return 0
        keys = [_usage_key(bot, repo, minute_bucket) for minute_bucket in buckets]
        values = self._redis.mget(keys)
        return sum(int(value) for value in values if value is not None)

    def record_hit(self, bot: str, repo: str, *, reset_ts: int | None = None) -> None:
        window = self.window_for(bot, reset_ts)
        minute_bucket = int(time.time() // 60)
        key = _usage_key(bot, repo, minute_bucket)
        pipeline = self._redis.pipeline()
        pipeline.incr(key)
        pipeline.expire(key, window.usage_key_ttl())
        pipeline.execute()

    def _discover_pairs(
        self,
        *,
        bot_filter: str | None,
        repo_filter: str | None,
        windows_by_bot: dict[str, GitHubWindow],
    ) -> set[tuple[str, str]]:
        match = f"{REPO_USAGE_KEY_PREFIX}:{bot_filter or '*'}:*"
        repo_filter_lower = repo_filter.lower() if repo_filter else None
        pairs: set[tuple[str, str]] = set()

        for key in self._redis.scan_iter(match=match, count=500):
            key_str = key.decode() if isinstance(key, bytes) else key
            parsed = _parse_usage_key(key_str)
            if parsed is None:
                continue
            bot, repo, minute_bucket = parsed
            if repo_filter_lower and repo_filter_lower not in repo.lower():
                continue
            window = windows_by_bot.get(bot, GitHubWindow(None))
            if window.contains_minute(minute_bucket):
                pairs.add((bot, repo))
        return pairs

    def list_rows(
        self,
        *,
        bot_filter: str | None = None,
        repo_filter: str | None = None,
    ) -> list[RepoUsageRow]:
        windows_by_bot = {
            bot: self.window_for(bot, self.reset_at(bot)) for bot in PUBLIC_BOTS
        }
        budgets_by_bot = {bot: self.budget(bot) for bot in PUBLIC_BOTS}
        resets_by_bot = {bot: self.reset_at(bot) for bot in PUBLIC_BOTS}

        rows: list[RepoUsageRow] = []
        for bot, repo in sorted(
            self._discover_pairs(
                bot_filter=bot_filter,
                repo_filter=repo_filter,
                windows_by_bot=windows_by_bot,
            )
        ):
            reset_ts = resets_by_bot.get(bot)
            hits = self.hits(bot, repo, reset_ts=reset_ts)
            if hits == 0:
                continue
            budget = budgets_by_bot.get(bot, BUDGET_FALLBACK)
            pct_budget = (hits / budget * 100) if budget else 0.0
            rows.append(
                RepoUsageRow(
                    bot=bot,
                    repo=repo,
                    hits=hits,
                    budget=budget,
                    pct_budget=pct_budget,
                    reset_at=reset_ts,
                    cap=repo_cap(budget),
                    over_cap=is_over_cap(hits, budget),
                )
            )
        rows.sort(key=lambda row: row.hits, reverse=True)
        return rows

    def top_rows(
        self,
        *,
        limit: int = 10,
        bot_filter: str | None = None,
        repo_filter: str | None = None,
    ) -> list[RepoUsageRow]:
        return self.list_rows(
            bot_filter=bot_filter,
            repo_filter=repo_filter,
        )[:limit]


# ---------------------------------------------------------------------------
# Module-level helpers (call-site convenience)
# ---------------------------------------------------------------------------


def _store(redis_connection: Redis) -> PublicBotUsageStore:
    return PublicBotUsageStore(redis_connection)


def get_pool_reset(redis_connection: Redis, bot: str) -> int | None:
    return _store(redis_connection).reset_at(bot)


def get_pool_budget(redis_connection: Redis, bot: str) -> int:
    return _store(redis_connection).budget(bot)


def record_pool_state(
    redis_connection: Redis,
    bot: str,
    remaining: int | str,
    limit: int | str,
    reset: int | str,
) -> None:
    _store(redis_connection).update_pool_from_headers(bot, remaining, limit, reset)


def get_repo_usage(
    redis_connection: Redis,
    bot: str,
    repo: str,
    reset_ts: int | None = None,
) -> int:
    return _store(redis_connection).hits(bot, repo, reset_ts=reset_ts)


def record_repo_request(
    redis_connection: Redis,
    bot: str,
    repo: str,
    reset_ts: int | None = None,
) -> None:
    _store(redis_connection).record_hit(bot, repo, reset_ts=reset_ts)


def list_repo_usage(
    redis_connection: Redis,
    *,
    bot_filter: str | None = None,
    repo_filter: str | None = None,
) -> list[RepoUsageRow]:
    return _store(redis_connection).list_rows(
        bot_filter=bot_filter, repo_filter=repo_filter
    )


def top_repos_across_bots(
    redis_connection: Redis,
    *,
    limit: int = 10,
    bot_filter: str | None = None,
    repo_filter: str | None = None,
) -> list[RepoUsageRow]:
    return _store(redis_connection).top_rows(
        limit=limit, bot_filter=bot_filter, repo_filter=repo_filter
    )
