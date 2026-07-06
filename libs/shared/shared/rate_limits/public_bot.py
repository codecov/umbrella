"""Load-adaptive, per-repo rate limiting for the shared "public bot" tokens.

The shared dedicated-app tokens (e.g. ``commit_dedicated_app`` / ``pull_dedicated_app``)
are used as a fallback for public repos that are not covered by a GitHub App
installation. Because every uncovered public repo shares a single GitHub rate-limit
budget per token, a few high-volume repos can monopolize the pool.

This module implements a per-repo cap whose size *slides* with the shared token's
live GitHub utilization: near-unlimited when the pool is quiet, clamped to a small
guaranteed share when the pool is under pressure.

Configuration (install YAML, read via ``get_config``; defaults are safe/no-op)::

    github:
      public_bot_rate_limit:
        enabled: true          # master switch (default: false)
        enforce: false         # false = observe/metrics-only; true = actually drop
        bots: [commit, pull]   # token entity_names subject to limiting
        guaranteed_share: 0.02 # fair share of the pool when it is saturated
        max_share: 0.20        # burst share when the pool is idle
        util_low: 0.5          # utilization at/below which repos get max_share
        util_high: 0.9         # utilization at/above which repos get guaranteed_share
        window_seconds: 3600   # rolling window for per-repo usage
        budget_fallback: 15000 # used only if live GitHub headers are unavailable
"""

import math
import time
from dataclasses import dataclass

from redis import Redis

from shared.config import get_config

# Redis key prefixes
REPO_USAGE_KEY_PREFIX = "pbrl"  # pbrl:{bot}:{repo}:{epoch_minute}
POOL_UTIL_KEY_PREFIX = "public_bot_util"  # public_bot_util:{bot}
POOL_BUDGET_KEY_PREFIX = "public_bot_budget"  # public_bot_budget:{bot}

# How long the pool-utilization signal is trusted before we consider it stale.
POOL_UTIL_TTL_SECONDS = 120

DEFAULT_ENABLED = False
DEFAULT_ENFORCE = False
DEFAULT_BOTS = ["commit", "pull"]
DEFAULT_GUARANTEED_SHARE = 0.02
DEFAULT_MAX_SHARE = 0.20
DEFAULT_UTIL_LOW = 0.5
DEFAULT_UTIL_HIGH = 0.9
DEFAULT_WINDOW_SECONDS = 3600
DEFAULT_BUDGET_FALLBACK = 15000


@dataclass(frozen=True)
class PublicBotRateLimitConfig:
    enabled: bool
    enforce: bool
    bots: frozenset[str]
    guaranteed_share: float
    max_share: float
    util_low: float
    util_high: float
    window_seconds: int
    budget_fallback: int


@dataclass(frozen=True)
class PublicBotRateLimitDecision:
    """Result of evaluating a single request against the per-repo cap."""

    is_limited_bot: bool
    over_cap: bool
    usage: int
    cap: int
    utilization: float


def get_public_bot_rate_limit_config(service: str) -> PublicBotRateLimitConfig:
    """Loads the per-repo public-bot rate-limit config for a service."""
    raw = get_config(service, "public_bot_rate_limit", default={})
    return PublicBotRateLimitConfig(
        enabled=raw.get("enabled", DEFAULT_ENABLED),
        enforce=raw.get("enforce", DEFAULT_ENFORCE),
        bots=frozenset(raw.get("bots", DEFAULT_BOTS)),
        guaranteed_share=raw.get("guaranteed_share", DEFAULT_GUARANTEED_SHARE),
        max_share=raw.get("max_share", DEFAULT_MAX_SHARE),
        util_low=raw.get("util_low", DEFAULT_UTIL_LOW),
        util_high=raw.get("util_high", DEFAULT_UTIL_HIGH),
        window_seconds=raw.get("window_seconds", DEFAULT_WINDOW_SECONDS),
        budget_fallback=raw.get("budget_fallback", DEFAULT_BUDGET_FALLBACK),
    )


def _minute_buckets(window_seconds: int) -> list[int]:
    """Returns the epoch-minute bucket ids covering the trailing window."""
    current = int(time.time() // 60)
    num_buckets = max(1, math.ceil(window_seconds / 60))
    return [current - offset for offset in range(num_buckets)]


def _repo_usage_key(bot: str, repo: str, minute_bucket: int) -> str:
    return f"{REPO_USAGE_KEY_PREFIX}:{bot}:{repo}:{minute_bucket}"


def record_pool_utilization(
    redis_connection: Redis, bot: str, remaining: int | str, limit: int | str
) -> None:
    """Stores a shared bot's live GitHub utilization (1 - remaining/limit) and budget."""
    limit = int(limit)
    utilization = 1 - int(remaining) / limit
    redis_connection.set(
        f"{POOL_UTIL_KEY_PREFIX}:{bot}", utilization, ex=POOL_UTIL_TTL_SECONDS
    )
    redis_connection.set(
        f"{POOL_BUDGET_KEY_PREFIX}:{bot}", limit, ex=POOL_UTIL_TTL_SECONDS
    )


def get_pool_utilization(redis_connection: Redis, bot: str) -> float:
    """Reads the last-known utilization for a shared bot. Missing/stale -> 0.0."""
    value = redis_connection.get(f"{POOL_UTIL_KEY_PREFIX}:{bot}")
    return float(value) if value is not None else 0.0


def get_pool_budget(redis_connection: Redis, bot: str, budget_fallback: int) -> int:
    """Reads the last-known GitHub limit for a shared bot, else the config fallback."""
    value = redis_connection.get(f"{POOL_BUDGET_KEY_PREFIX}:{bot}")
    return int(value) if value is not None else budget_fallback


def sliding_repo_cap(
    utilization: float, budget: int, cfg: PublicBotRateLimitConfig
) -> int:
    """Per-repo request cap as a function of pool utilization.

    - utilization <= util_low  -> budget * max_share        (quiet pool, generous)
    - utilization >= util_high -> budget * guaranteed_share  (saturated, fair share)
    - between                  -> linear interpolation of the share
    """
    if utilization <= cfg.util_low:
        share = cfg.max_share
    elif utilization >= cfg.util_high:
        share = cfg.guaranteed_share
    else:
        frac = (utilization - cfg.util_low) / (cfg.util_high - cfg.util_low)
        share = cfg.max_share - frac * (cfg.max_share - cfg.guaranteed_share)
    return max(0, int(budget * share))


def get_repo_usage(
    redis_connection: Redis, bot: str, repo: str, window_seconds: int
) -> int:
    """Sums a repo's requests to a shared bot over the trailing window."""
    keys = [_repo_usage_key(bot, repo, b) for b in _minute_buckets(window_seconds)]
    values = redis_connection.mget(keys)
    return sum(int(v) for v in values if v is not None)


def record_repo_request(
    redis_connection: Redis, bot: str, repo: str, window_seconds: int
) -> None:
    """Increments a repo's usage of a shared bot for the current minute bucket."""
    key = _repo_usage_key(bot, repo, _minute_buckets(window_seconds)[0])
    pipeline = redis_connection.pipeline()
    pipeline.incr(key)
    pipeline.expire(key, window_seconds + 60)
    pipeline.execute()


def evaluate_public_bot_request(
    redis_connection: Redis, bot: str, repo: str, cfg: PublicBotRateLimitConfig
) -> PublicBotRateLimitDecision:
    """Decides whether this request would exceed the repo's sliding cap.

    Read-only: it does not increment the repo counter (callers do that only when a
    request is actually sent, via ``record_repo_request``).
    """
    if not cfg.enabled or bot not in cfg.bots:
        return PublicBotRateLimitDecision(
            is_limited_bot=False, over_cap=False, usage=0, cap=0, utilization=0.0
        )
    utilization = get_pool_utilization(redis_connection, bot)
    budget = get_pool_budget(redis_connection, bot, cfg.budget_fallback)
    cap = sliding_repo_cap(utilization, budget, cfg)
    usage = get_repo_usage(redis_connection, bot, repo, cfg.window_seconds)
    return PublicBotRateLimitDecision(
        is_limited_bot=True,
        over_cap=usage >= cap,
        usage=usage,
        cap=cap,
        utilization=utilization,
    )
