from unittest.mock import MagicMock

import pytest
from redis import RedisError

from shared.rate_limits.public_bot import (
    DEFAULT_BOTS,
    POOL_BUDGET_KEY_PREFIX,
    POOL_UTIL_KEY_PREFIX,
    PublicBotRateLimitConfig,
    evaluate_public_bot_request,
    get_pool_budget,
    get_pool_utilization,
    get_public_bot_rate_limit_config,
    get_repo_usage,
    record_pool_utilization,
    record_repo_request,
    sliding_repo_cap,
)


def make_cfg(**overrides) -> PublicBotRateLimitConfig:
    base = {
        "enabled": True,
        "enforce": False,
        "bots": frozenset({"commit", "pull"}),
        "guaranteed_share": 0.02,
        "max_share": 0.20,
        "util_low": 0.5,
        "util_high": 0.9,
        "window_seconds": 3600,
        "budget_fallback": 15000,
    }
    base.update(overrides)
    return PublicBotRateLimitConfig(**base)


class TestSlidingRepoCap:
    def test_below_util_low_uses_max_share(self):
        assert sliding_repo_cap(0.3, 15000, make_cfg()) == 3000

    def test_at_util_low_uses_max_share(self):
        assert sliding_repo_cap(0.5, 15000, make_cfg()) == 3000

    def test_above_util_high_uses_guaranteed_share(self):
        assert sliding_repo_cap(0.95, 15000, make_cfg()) == 300

    def test_at_util_high_uses_guaranteed_share(self):
        assert sliding_repo_cap(0.9, 15000, make_cfg()) == 300

    def test_midpoint_interpolates(self):
        # U=0.7 -> share = 0.20 - 0.5*(0.20-0.02) = 0.11 -> 1650
        assert sliding_repo_cap(0.7, 15000, make_cfg()) == 1650

    def test_zero_utilization(self):
        assert sliding_repo_cap(0.0, 15000, make_cfg()) == 3000

    def test_full_utilization(self):
        assert sliding_repo_cap(1.0, 15000, make_cfg()) == 300

    def test_degenerate_thresholds_equal(self):
        cfg = make_cfg(util_low=0.9, util_high=0.9)
        # value at/below low -> max share
        assert sliding_repo_cap(0.5, 15000, cfg) == 3000
        # value strictly above -> guaranteed share
        assert sliding_repo_cap(0.95, 15000, cfg) == 300


class TestConfigLoader:
    def test_defaults_when_missing(self, mock_configuration):
        cfg = get_public_bot_rate_limit_config("github")
        assert cfg.enabled is False
        assert cfg.enforce is False
        assert cfg.bots == frozenset(DEFAULT_BOTS)
        assert cfg.guaranteed_share == 0.02
        assert cfg.max_share == 0.20
        assert cfg.window_seconds == 3600
        assert cfg.budget_fallback == 15000

    def test_reads_from_config(self, mock_configuration):
        mock_configuration.set_params(
            {
                "github": {
                    "public_bot_rate_limit": {
                        "enabled": True,
                        "enforce": True,
                        "bots": ["commit", "pull", "read"],
                        "guaranteed_share": 0.05,
                        "max_share": 0.5,
                        "util_low": 0.4,
                        "util_high": 0.8,
                        "window_seconds": 600,
                        "budget_fallback": 5000,
                    }
                }
            }
        )
        cfg = get_public_bot_rate_limit_config("github")
        assert cfg.enabled is True
        assert cfg.enforce is True
        assert cfg.bots == frozenset({"commit", "pull", "read"})
        assert cfg.guaranteed_share == 0.05
        assert cfg.max_share == 0.5
        assert cfg.util_low == 0.4
        assert cfg.util_high == 0.8
        assert cfg.window_seconds == 600
        assert cfg.budget_fallback == 5000


class TestPoolUtilization:
    def test_record_pool_utilization_sets_keys(self):
        redis = MagicMock()
        record_pool_utilization(redis, "commit", remaining=12000, limit=15000)
        # utilization = 1 - 12000/15000 = 0.2
        set_calls = {c.args[0]: c for c in redis.set.call_args_list}
        assert f"{POOL_UTIL_KEY_PREFIX}:commit" in set_calls
        assert f"{POOL_BUDGET_KEY_PREFIX}:commit" in set_calls
        util_call = set_calls[f"{POOL_UTIL_KEY_PREFIX}:commit"]
        assert util_call.args[1] == pytest.approx(0.2)

    def test_record_pool_utilization_ignores_bad_values(self):
        redis = MagicMock()
        record_pool_utilization(redis, "commit", remaining=None, limit=None)
        record_pool_utilization(redis, "commit", remaining=1, limit=0)
        redis.set.assert_not_called()

    def test_record_pool_utilization_fails_open(self):
        redis = MagicMock()
        redis.set.side_effect = RedisError
        # Should not raise
        record_pool_utilization(redis, "commit", remaining=1, limit=100)

    def test_get_pool_utilization_from_bytes(self):
        redis = MagicMock()
        redis.get.return_value = b"0.5"
        assert get_pool_utilization(redis, "commit") == 0.5

    def test_get_pool_utilization_missing_is_zero(self):
        redis = MagicMock()
        redis.get.return_value = None
        assert get_pool_utilization(redis, "commit") == 0.0

    def test_get_pool_utilization_fails_open(self):
        redis = MagicMock()
        redis.get.side_effect = RedisError
        assert get_pool_utilization(redis, "commit") == 0.0

    def test_get_pool_budget_from_redis(self):
        redis = MagicMock()
        redis.get.return_value = b"5000"
        assert get_pool_budget(redis, "commit", 15000) == 5000

    def test_get_pool_budget_fallback_when_missing(self):
        redis = MagicMock()
        redis.get.return_value = None
        assert get_pool_budget(redis, "commit", 15000) == 15000

    def test_get_pool_budget_fallback_on_error(self):
        redis = MagicMock()
        redis.get.side_effect = RedisError
        assert get_pool_budget(redis, "commit", 15000) == 15000


class TestRepoUsage:
    def test_get_repo_usage_sums_buckets(self):
        redis = MagicMock()
        redis.mget.return_value = [b"3", None, b"2", b"5"]
        assert get_repo_usage(redis, "commit", "org/repo", 3600) == 10

    def test_get_repo_usage_fails_open(self):
        redis = MagicMock()
        redis.mget.side_effect = RedisError
        assert get_repo_usage(redis, "commit", "org/repo", 3600) == 0

    def test_record_repo_request_increments_current_bucket(self):
        redis = MagicMock()
        pipeline = MagicMock()
        redis.pipeline.return_value = pipeline
        record_repo_request(redis, "commit", "org/repo", 3600)
        pipeline.incr.assert_called_once()
        pipeline.expire.assert_called_once()
        pipeline.execute.assert_called_once()

    def test_record_repo_request_fails_open(self):
        redis = MagicMock()
        redis.pipeline.side_effect = RedisError
        # Should not raise
        record_repo_request(redis, "commit", "org/repo", 3600)


class TestEvaluatePublicBotRequest:
    def test_disabled_config_is_not_limited(self):
        redis = MagicMock()
        cfg = make_cfg(enabled=False)
        decision = evaluate_public_bot_request(redis, "commit", "org/repo", cfg)
        assert decision.is_limited_bot is False
        assert decision.over_cap is False

    def test_bot_not_in_list_is_not_limited(self):
        redis = MagicMock()
        cfg = make_cfg()
        decision = evaluate_public_bot_request(
            redis, "installation_123", "org/repo", cfg
        )
        assert decision.is_limited_bot is False

    def test_under_cap(self):
        redis = MagicMock()

        def fake_get(key):
            if key.startswith(POOL_UTIL_KEY_PREFIX):
                return b"0.95"  # saturated -> cap = 2% of 15000 = 300
            if key.startswith(POOL_BUDGET_KEY_PREFIX):
                return b"15000"
            return None

        redis.get.side_effect = fake_get
        redis.mget.return_value = [b"100"]  # usage 100 < cap 300
        cfg = make_cfg()
        decision = evaluate_public_bot_request(redis, "commit", "org/repo", cfg)
        assert decision.is_limited_bot is True
        assert decision.over_cap is False
        assert decision.cap == 300
        assert decision.usage == 100
        assert decision.utilization == pytest.approx(0.95)

    def test_over_cap(self):
        redis = MagicMock()

        def fake_get(key):
            if key.startswith(POOL_UTIL_KEY_PREFIX):
                return b"0.95"  # saturated -> cap = 300
            if key.startswith(POOL_BUDGET_KEY_PREFIX):
                return b"15000"
            return None

        redis.get.side_effect = fake_get
        redis.mget.return_value = [b"400"]  # usage 400 >= cap 300
        cfg = make_cfg()
        decision = evaluate_public_bot_request(redis, "commit", "org/repo", cfg)
        assert decision.is_limited_bot is True
        assert decision.over_cap is True
        assert decision.cap == 300
        assert decision.usage == 400

    def test_quiet_pool_allows_large_usage(self):
        redis = MagicMock()

        def fake_get(key):
            if key.startswith(POOL_UTIL_KEY_PREFIX):
                return b"0.1"  # quiet -> cap = 20% of 15000 = 3000
            if key.startswith(POOL_BUDGET_KEY_PREFIX):
                return b"15000"
            return None

        redis.get.side_effect = fake_get
        redis.mget.return_value = [b"2000"]  # usage 2000 < cap 3000
        cfg = make_cfg()
        decision = evaluate_public_bot_request(redis, "commit", "org/repo", cfg)
        assert decision.over_cap is False
        assert decision.cap == 3000
