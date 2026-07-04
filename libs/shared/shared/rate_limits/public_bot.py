"""Load-adaptive, per-repo rate limiting for the shared "public bot" tokens.

The shared dedicated-app tokens (e.g. ``commit_dedicated_app`` / ``pull_dedicated_app``)
are used as a fallback for public repos that are not covered by a GitHub App
installation. Because every uncovered public repo shares a single GitHub rate-limit
budget per token, a few high-volume repos can monopolize the pool.

This module implements a per-repo cap whose size *slides* with the shared token's
live GitHub utilization: near-unlimited when the pool is quiet, clamped to a small
guaranteed share when the pool is under pressure. It is intentionally fail-open:
any Redis error results in "not limited" so we never block provider traffic on our
own bookkeeping.

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

Rollout (observe-first):

1. Ship with ``enabled: true, enforce: false``. This computes the decision and
   emits ``git_provider_public_bot_over_cap{...,mode="observe"}`` on every over-cap
   request but still sends it -- zero behavior change. Also populates
   ``git_provider_public_bot_utilization`` so live pool pressure is visible.
2. Watch ``git_provider_public_bot_over_cap`` for a few days to see which repos trip
   which bot, and when. Tune ``util_low`` / ``util_high`` / ``max_share`` /
   ``guaranteed_share`` so only genuine monopolizers trip during real pressure.
   Audit the endpoints those repos hit, since enforcing returns empty responses.
3. Flip ``enforce: true``. Over-cap requests are then silently dropped (synthetic
   204 -> ``None``, no exception) and ``mode="enforce"`` on the metric. Roll back by
   flipping ``enforce`` (or ``enabled``) back to false.
"""

import logging
import math
import time
from dataclasses import dataclass

from redis import Redis, RedisError

from shared.config import get_config

log = logging.getLogger(__name__)

# Redis key prefixes
REPO_USAGE_KEY_PREFIX = "pbrl"  # pbrl:{bot}:{repo}:{epoch_minute}
POOL_UTIL_KEY_PREFIX = "public_bot_util"  # public_bot_util:{bot}
POOL_BUDGET_KEY_PREFIX = "public_bot_budget"  # public_bot_budget:{bot}

# How long the pool-utilization signal is trusted before we consider it stale.
POOL_UTIL_TTL_SECONDS = 120

# Defaults (see plan). All overridable via install YAML under
# `<service>.public_bot_rate_limit.*`.
DEFAULT_ENABLED = False
DEFAULT_ENFORCE = False
DEFAULT_BOTS = ["commit", "pull"]
DEFAULT_GUARANTEED_SHARE = 0.02
DEFAULT_MAX_SHARE = 0.20
DEFAULT_UTIL_LOW = 0.5
DEFAULT_UTIL_HIGH = 0.9
DEFAULT_WINDOW_SECONDS = 3600
DEFAULT_BUDGET_FALLBACK = 15000


def _as_float(value: object) -> float | None:
    """Coerces a Redis value (bytes/str/int/float) to float, or None on failure."""
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode()
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: object) -> int | None:
    """Coerces a Redis value (bytes/str/int) to int, or None on failure."""
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode()
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


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
    """Loads the per-repo public-bot rate-limit config for a service (fail-safe)."""
    raw = get_config(service, "public_bot_rate_limit", default={}) or {}
    bots = raw.get("bots", DEFAULT_BOTS) or []
    return PublicBotRateLimitConfig(
        enabled=bool(raw.get("enabled", DEFAULT_ENABLED)),
        enforce=bool(raw.get("enforce", DEFAULT_ENFORCE)),
        bots=frozenset(str(b) for b in bots),
        guaranteed_share=float(raw.get("guaranteed_share", DEFAULT_GUARANTEED_SHARE)),
        max_share=float(raw.get("max_share", DEFAULT_MAX_SHARE)),
        util_low=float(raw.get("util_low", DEFAULT_UTIL_LOW)),
        util_high=float(raw.get("util_high", DEFAULT_UTIL_HIGH)),
        window_seconds=int(raw.get("window_seconds", DEFAULT_WINDOW_SECONDS)),
        budget_fallback=int(raw.get("budget_fallback", DEFAULT_BUDGET_FALLBACK)),
    )


def _minute_buckets(window_seconds: int, now: float | None = None) -> list[int]:
    """Returns the epoch-minute bucket ids covering the trailing window."""
    now = now if now is not None else time.time()
    current = int(now // 60)
    num_buckets = max(1, math.ceil(window_seconds / 60))
    return [current - offset for offset in range(num_buckets)]


def _repo_usage_key(bot: str, repo: str, minute_bucket: int) -> str:
    return f"{REPO_USAGE_KEY_PREFIX}:{bot}:{repo}:{minute_bucket}"


def record_pool_utilization(
    redis_connection: Redis,
    bot: str,
    remaining: int | str | None,
    limit: int | str | None,
) -> None:
    """Stores the live GitHub utilization for a shared bot from response headers.

    ``utilization`` is ``1 - remaining/limit`` in ``[0, 1]``. Fails open.
    """
    try:
        remaining_i = int(remaining)
        limit_i = int(limit)
    except (TypeError, ValueError):
        return
    if limit_i <= 0:
        return
    utilization = max(0.0, min(1.0, 1.0 - (remaining_i / limit_i)))
    try:
        redis_connection.set(
            f"{POOL_UTIL_KEY_PREFIX}:{bot}",
            utilization,
            ex=POOL_UTIL_TTL_SECONDS,
        )
        redis_connection.set(
            f"{POOL_BUDGET_KEY_PREFIX}:{bot}",
            limit_i,
            ex=POOL_UTIL_TTL_SECONDS,
        )
    except RedisError:
        log.warning(
            "Failed to record public bot pool utilization",
            extra={"bot": bot},
        )


def get_pool_utilization(redis_connection: Redis, bot: str) -> float:
    """Reads the last-known utilization for a shared bot. Missing/stale -> 0.0."""
    try:
        value = redis_connection.get(f"{POOL_UTIL_KEY_PREFIX}:{bot}")
    except RedisError:
        log.warning("Failed to read public bot pool utilization", extra={"bot": bot})
        return 0.0
    utilization = _as_float(value)
    if utilization is None:
        return 0.0
    return max(0.0, min(1.0, utilization))


def get_pool_budget(redis_connection: Redis, bot: str, budget_fallback: int) -> int:
    """Reads the last-known GitHub limit for a shared bot, else the config fallback."""
    try:
        value = redis_connection.get(f"{POOL_BUDGET_KEY_PREFIX}:{bot}")
    except RedisError:
        return budget_fallback
    budget = _as_int(value)
    if budget is None or budget <= 0:
        return budget_fallback
    return budget


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
    elif cfg.util_high <= cfg.util_low:
        # Degenerate config; fall back to the strict share.
        share = cfg.guaranteed_share
    else:
        frac = (utilization - cfg.util_low) / (cfg.util_high - cfg.util_low)
        share = cfg.max_share - frac * (cfg.max_share - cfg.guaranteed_share)
    return max(0, int(budget * share))


def get_repo_usage(
    redis_connection: Redis, bot: str, repo: str, window_seconds: int
) -> int:
    """Sums a repo's requests to a shared bot over the trailing window. Fails open."""
    buckets = _minute_buckets(window_seconds)
    keys = [_repo_usage_key(bot, repo, b) for b in buckets]
    try:
        values = redis_connection.mget(keys)
    except RedisError:
        log.warning(
            "Failed to read public bot repo usage",
            extra={"bot": bot, "repo": repo},
        )
        return 0
    total = 0
    for value in values:
        parsed = _as_int(value)
        if parsed is not None:
            total += parsed
    return total


def record_repo_request(
    redis_connection: Redis, bot: str, repo: str, window_seconds: int
) -> None:
    """Increments a repo's usage of a shared bot for the current minute. Fails open."""
    bucket = _minute_buckets(window_seconds)[0]
    key = _repo_usage_key(bot, repo, bucket)
    try:
        pipeline = redis_connection.pipeline()
        pipeline.incr(key)
        # Keep the bucket around for the whole window (+ small margin) so it is
        # still counted until it naturally ages out.
        pipeline.expire(key, window_seconds + 60)
        pipeline.execute()
    except RedisError:
        log.warning(
            "Failed to record public bot repo request",
            extra={"bot": bot, "repo": repo},
        )


def evaluate_public_bot_request(
    redis_connection: Redis,
    bot: str,
    repo: str,
    cfg: PublicBotRateLimitConfig,
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
