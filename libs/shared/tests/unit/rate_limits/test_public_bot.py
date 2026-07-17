from unittest.mock import MagicMock

import pytest

from shared.rate_limits.public_bot import (
    BUDGET_FALLBACK,
    MAX_SHARE_PER_REPO,
    POOL_BUDGET_KEY_PREFIX,
    POOL_RESET_KEY_PREFIX,
    PUBLIC_BOTS,
    REPO_USAGE_KEY_PREFIX,
    GitHubWindow,
    get_pool_budget,
    get_pool_reset,
    get_repo_usage,
    is_over_cap,
    is_public_bot,
    list_repo_usage,
    record_pool_state,
    record_repo_request,
    repo_cap,
    top_repos_across_bots,
)
from shared.torngit.base import TokenType


def test_public_bots_includes_all_shared_token_types():
    assert TokenType.commit.value in PUBLIC_BOTS
    assert TokenType.pull.value in PUBLIC_BOTS
    assert TokenType.read.value in PUBLIC_BOTS
    assert "tokenless_bot" in PUBLIC_BOTS
    assert TokenType.admin.value not in PUBLIC_BOTS


def test_is_public_bot():
    assert is_public_bot("commit")
    assert is_public_bot("tokenless_bot")
    assert not is_public_bot("254_123")
    assert not is_public_bot(None)


def test_record_and_read_pool_state():
    redis = MagicMock()
    record_pool_state(redis, "commit", remaining=7500, limit=15000, reset=4_000_000_000)
    assert redis.set.call_count == 2
    redis.get.side_effect = lambda key: {
        f"{POOL_BUDGET_KEY_PREFIX}:commit": b"15000",
        f"{POOL_RESET_KEY_PREFIX}:commit": b"4000000000",
    }.get(key)
    assert get_pool_budget(redis, "commit") == 15000
    assert get_pool_reset(redis, "commit") == 4_000_000_000


def test_get_pool_budget_falls_back():
    redis = MagicMock()
    redis.get.return_value = None
    assert get_pool_budget(redis, "commit") == BUDGET_FALLBACK


def test_record_and_sum_repo_usage(monkeypatch):
    fixed_now = 3_600_000
    monkeypatch.setattr("shared.rate_limits.public_bot.time.time", lambda: fixed_now)

    redis = MagicMock()
    reset_ts = fixed_now + 1800
    redis.get.side_effect = lambda key: {
        f"{POOL_RESET_KEY_PREFIX}:commit": str(reset_ts).encode(),
    }.get(key)

    minute_bucket = fixed_now // 60
    key = f"{REPO_USAGE_KEY_PREFIX}:commit:org/repo:{minute_bucket}"
    redis.mget.return_value = [b"3", b"2"]

    record_repo_request(redis, "commit", "org/repo", reset_ts=reset_ts)
    pipeline = redis.pipeline.return_value
    pipeline.incr.assert_called_once_with(key)
    pipeline.expire.assert_called_once()
    pipeline.execute.assert_called_once()

    usage = get_repo_usage(redis, "commit", "org/repo", reset_ts=reset_ts)
    assert usage == 5


def test_repo_cap_and_over_cap():
    budget = 15000
    cap = repo_cap(budget)
    assert cap == int(budget * MAX_SHARE_PER_REPO)
    assert not is_over_cap(cap - 1, budget)
    assert is_over_cap(cap, budget)


def test_contains_minute_matches_minute_buckets_when_not_aligned(monkeypatch):
    fixed_now = 3_600_001
    monkeypatch.setattr("shared.rate_limits.public_bot.time.time", lambda: fixed_now)

    reset_ts = fixed_now + 1800
    window = GitHubWindow(reset_ts)
    minute_bucket = fixed_now // 60

    assert minute_bucket in window.minute_buckets()
    assert window.contains_minute(minute_bucket)


def test_list_repo_usage_aggregates_scan_results(monkeypatch):
    fixed_now = 3_600_001
    monkeypatch.setattr("shared.rate_limits.public_bot.time.time", lambda: fixed_now)
    reset_ts = fixed_now + 1800
    minute_bucket = fixed_now // 60

    redis = MagicMock()
    redis.scan_iter.return_value = [
        f"{REPO_USAGE_KEY_PREFIX}:commit:org/a:{minute_bucket}",
        f"{REPO_USAGE_KEY_PREFIX}:pull:org/b:{minute_bucket}",
    ]
    redis.get.side_effect = lambda key: {
        f"{POOL_RESET_KEY_PREFIX}:commit": str(reset_ts).encode(),
        f"{POOL_RESET_KEY_PREFIX}:pull": str(reset_ts).encode(),
        f"{POOL_BUDGET_KEY_PREFIX}:commit": b"15000",
        f"{POOL_BUDGET_KEY_PREFIX}:pull": b"15000",
    }.get(key)
    redis.mget.side_effect = [
        [b"10"],
        [b"20"],
    ]

    rows = list_repo_usage(redis)
    assert len(rows) == 2
    assert rows[0].repo == "org/b"
    assert rows[0].hits == 20
    assert rows[0].pct_budget == pytest.approx(20 / 15000 * 100)


def test_top_repos_across_bots_limits_results(monkeypatch):
    fixed_now = 3_600_000
    monkeypatch.setattr("shared.rate_limits.public_bot.time.time", lambda: fixed_now)
    reset_ts = fixed_now + 1800
    minute_bucket = fixed_now // 60

    redis = MagicMock()
    keys = [
        f"{REPO_USAGE_KEY_PREFIX}:commit:org/{idx}:{minute_bucket}" for idx in range(12)
    ]
    redis.scan_iter.return_value = keys
    redis.get.return_value = str(reset_ts).encode()
    redis.mget.return_value = [b"1"]

    top = top_repos_across_bots(redis, limit=10)
    assert len(top) == 10
