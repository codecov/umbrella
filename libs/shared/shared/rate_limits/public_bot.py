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

from shared.torngit.base import TokenType

PUBLIC_BOTS = frozenset(
    member.value for member in TokenType if member is not TokenType.admin
) | {"tokenless_bot"}

REPO_USAGE_KEY_PREFIX = "pbrl"
POOL_BUDGET_KEY_PREFIX = "public_bot_budget"
POOL_RESET_KEY_PREFIX = "public_bot_reset"
POOL_STATE_TTL_SECONDS = 120
BUDGET_FALLBACK = 15000
GITHUB_WINDOW_SECONDS = 3600
MAX_SHARE_PER_REPO = 0.05


def is_public_bot(entity_name: str | None) -> bool:
    return bool(entity_name and entity_name in PUBLIC_BOTS)


def _repo_usage_key(bot: str, repo: str, minute_bucket: int) -> str:
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


def _window_start(reset_ts: int | None) -> int:
    now = int(time.time())
    if reset_ts and reset_ts > now:
        return reset_ts - GITHUB_WINDOW_SECONDS
    return now - GITHUB_WINDOW_SECONDS


def _minute_buckets_for_window(window_start: int) -> list[int]:
    start_minute = window_start // 60
    end_minute = int(time.time() // 60)
    if end_minute < start_minute:
        return []
    return list(range(start_minute, end_minute + 1))


def _usage_key_ttl(reset_ts: int | None) -> int:
    now = int(time.time())
    if reset_ts and reset_ts > now:
        return max(60, reset_ts - now + 60)
    return GITHUB_WINDOW_SECONDS + 60


def get_pool_reset(redis_connection: Redis, bot: str) -> int | None:
    value = redis_connection.get(f"{POOL_RESET_KEY_PREFIX}:{bot}")
    return int(value) if value is not None else None


def get_pool_budget(redis_connection: Redis, bot: str) -> int:
    value = redis_connection.get(f"{POOL_BUDGET_KEY_PREFIX}:{bot}")
    return int(value) if value is not None else BUDGET_FALLBACK


def record_pool_state(
    redis_connection: Redis,
    bot: str,
    remaining: int | str,
    limit: int | str,
    reset: int | str,
) -> None:
    redis_connection.set(
        f"{POOL_BUDGET_KEY_PREFIX}:{bot}", int(limit), ex=POOL_STATE_TTL_SECONDS
    )
    redis_connection.set(
        f"{POOL_RESET_KEY_PREFIX}:{bot}", int(reset), ex=POOL_STATE_TTL_SECONDS
    )


def get_repo_usage(
    redis_connection: Redis,
    bot: str,
    repo: str,
    reset_ts: int | None = None,
) -> int:
    if reset_ts is None:
        reset_ts = get_pool_reset(redis_connection, bot)
    buckets = _minute_buckets_for_window(_window_start(reset_ts))
    if not buckets:
        return 0
    keys = [_repo_usage_key(bot, repo, minute_bucket) for minute_bucket in buckets]
    values = redis_connection.mget(keys)
    return sum(int(value) for value in values if value is not None)


def record_repo_request(
    redis_connection: Redis,
    bot: str,
    repo: str,
    reset_ts: int | None = None,
) -> None:
    if reset_ts is None:
        reset_ts = get_pool_reset(redis_connection, bot)
    minute_bucket = int(time.time() // 60)
    key = _repo_usage_key(bot, repo, minute_bucket)
    pipeline = redis_connection.pipeline()
    pipeline.incr(key)
    pipeline.expire(key, _usage_key_ttl(reset_ts))
    pipeline.execute()


def repo_cap(budget: int) -> int:
    return max(0, int(budget * MAX_SHARE_PER_REPO))


def is_over_cap(hits: int, budget: int) -> bool:
    return hits >= repo_cap(budget)


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


def _scan_usage_keys(
    redis_connection: Redis,
    bot_filter: str | None = None,
    repo_filter: str | None = None,
) -> list[tuple[str, str, int]]:
    match = f"{REPO_USAGE_KEY_PREFIX}:{bot_filter or '*'}:*"
    keys: list[tuple[str, str, int]] = []
    repo_filter_lower = repo_filter.lower() if repo_filter else None
    for key in redis_connection.scan_iter(match=match, count=500):
        key_str = key.decode() if isinstance(key, bytes) else key
        parsed = _parse_usage_key(key_str)
        if parsed is None:
            continue
        bot, repo, minute_bucket = parsed
        if repo_filter_lower and repo_filter_lower not in repo.lower():
            continue
        keys.append((bot, repo, minute_bucket))
    return keys


def list_repo_usage(
    redis_connection: Redis,
    *,
    bot_filter: str | None = None,
    repo_filter: str | None = None,
) -> list[RepoUsageRow]:
    """Aggregate per-repo hit counts for the current GitHub window."""
    reset_by_bot = {bot: get_pool_reset(redis_connection, bot) for bot in PUBLIC_BOTS}
    budget_by_bot = {bot: get_pool_budget(redis_connection, bot) for bot in PUBLIC_BOTS}
    window_start_by_bot = {
        bot: _window_start(reset_ts) for bot, reset_ts in reset_by_bot.items()
    }

    pairs: set[tuple[str, str]] = set()
    for bot, repo, minute_bucket in _scan_usage_keys(
        redis_connection, bot_filter=bot_filter, repo_filter=repo_filter
    ):
        window_start = window_start_by_bot.get(bot, _window_start(None))
        if minute_bucket * 60 < window_start:
            continue
        pairs.add((bot, repo))

    rows: list[RepoUsageRow] = []
    for bot, repo in sorted(pairs):
        reset_ts = reset_by_bot.get(bot)
        hits = get_repo_usage(redis_connection, bot, repo, reset_ts=reset_ts)
        if hits == 0:
            continue
        budget = budget_by_bot.get(bot, BUDGET_FALLBACK)
        pct_budget = (hits / budget * 100) if budget else 0.0
        rows.append(
            RepoUsageRow(
                bot=bot,
                repo=repo,
                hits=hits,
                budget=budget,
                pct_budget=pct_budget,
                reset_at=reset_ts,
            )
        )
    rows.sort(key=lambda row: row.hits, reverse=True)
    return rows


def top_repos_across_bots(
    redis_connection: Redis,
    *,
    limit: int = 10,
    bot_filter: str | None = None,
    repo_filter: str | None = None,
) -> list[RepoUsageRow]:
    return list_repo_usage(
        redis_connection, bot_filter=bot_filter, repo_filter=repo_filter
    )[:limit]
