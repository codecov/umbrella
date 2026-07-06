from unittest.mock import MagicMock

import httpx
import pytest
from redis import RedisError

from shared.rate_limits.public_bot import (
    POOL_BUDGET_KEY_PREFIX,
    POOL_UTIL_KEY_PREFIX,
)
from shared.torngit.github import Github


@pytest.fixture
def public_bot_handler():
    handler = Github(
        repo={"name": "mongo-python-driver"},
        owner={"username": "mongodb"},
        token={"key": "some_key"},
        oauth_consumer_token={"key": "client_id", "secret": "client_secret"},
    )
    return handler


def _redis_with(util: str, budget: str, usage: str):
    redis = MagicMock()

    def fake_get(key):
        if key.startswith(POOL_UTIL_KEY_PREFIX):
            return util.encode()
        if key.startswith(POOL_BUDGET_KEY_PREFIX):
            return budget.encode()
        return None

    redis.get.side_effect = fake_get
    redis.mget.return_value = [usage.encode()]
    redis.pipeline.return_value = MagicMock()
    return redis


def _ok_response():
    return httpx.Response(
        status_code=200,
        headers={
            "X-RateLimit-Remaining": "750",
            "X-RateLimit-Limit": "15000",
            "Content-Type": "application/json",
        },
        json=[{"sha": "abc"}],
    )


COMMIT_TOKEN = {
    "key": "shared_key",
    "username": "commit_dedicated_app",
    "entity_name": "commit",
}


def _enable_config(mock_configuration, enforce: bool):
    mock_configuration.set_params(
        {
            "github": {
                "public_bot_rate_limit": {
                    "enabled": True,
                    "enforce": enforce,
                    "bots": ["commit", "pull"],
                    "guaranteed_share": 0.02,
                    "max_share": 0.20,
                    "util_low": 0.5,
                    "util_high": 0.9,
                    "window_seconds": 3600,
                    "budget_fallback": 15000,
                }
            }
        }
    )


class TestMakeHttpCallPublicBot:
    @pytest.mark.asyncio
    async def test_observe_over_cap_still_sends(
        self, public_bot_handler, mock_configuration, mocker
    ):
        _enable_config(mock_configuration, enforce=False)
        # util 0.95 -> cap 300; usage 400 -> over cap
        public_bot_handler._redis_connection = _redis_with("0.95", "15000", "400")
        inc = mocker.patch("shared.torngit.github.inc_counter")
        record = mocker.patch("shared.torngit.github.record_repo_request")

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use=COMMIT_TOKEN,
        )

        # In observe mode the real request is sent and returned
        client.request.assert_awaited_once()
        assert result is res
        # over-cap metric emitted with mode=observe
        over_cap_calls = [
            c
            for c in inc.call_args_list
            if c.args
            and getattr(c.args[0], "_name", "") == "git_provider_public_bot_over_cap"
        ]
        assert len(over_cap_calls) == 1
        assert over_cap_calls[0].args[1]["mode"] == "observe"
        assert over_cap_calls[0].args[1]["bot"] == "commit"
        # request was counted since it was sent
        record.assert_called_once()

    @pytest.mark.asyncio
    async def test_enforce_over_cap_drops_silently(
        self, public_bot_handler, mock_configuration, mocker
    ):
        _enable_config(mock_configuration, enforce=True)
        public_bot_handler._redis_connection = _redis_with("0.95", "15000", "400")
        record = mocker.patch("shared.torngit.github.record_repo_request")

        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=_ok_response()))

        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use=COMMIT_TOKEN,
        )

        # Request is dropped: no HTTP call, no exception, synthetic 204
        client.request.assert_not_called()
        assert result.status_code == 204
        # dropped requests are NOT counted against usage
        record.assert_not_called()

    @pytest.mark.asyncio
    async def test_enforce_over_cap_parses_to_none(
        self, public_bot_handler, mock_configuration, mocker
    ):
        _enable_config(mock_configuration, enforce=True)
        public_bot_handler._redis_connection = _redis_with("0.95", "15000", "400")

        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=_ok_response()))

        # api() wraps make_http_call and parses the response; 204 -> None
        result = await public_bot_handler.api(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token=COMMIT_TOKEN,
        )
        assert result is None
        client.request.assert_not_called()

    @pytest.mark.asyncio
    async def test_under_cap_sends_and_records(
        self, public_bot_handler, mock_configuration, mocker
    ):
        _enable_config(mock_configuration, enforce=True)
        # util 0.1 -> cap 3000; usage 100 -> under cap
        public_bot_handler._redis_connection = _redis_with("0.1", "15000", "100")
        record = mocker.patch("shared.torngit.github.record_repo_request")

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use=COMMIT_TOKEN,
        )
        assert result is res
        client.request.assert_awaited_once()
        record.assert_called_once()

    @pytest.mark.asyncio
    async def test_non_limited_bot_is_untouched(
        self, public_bot_handler, mock_configuration, mocker
    ):
        _enable_config(mock_configuration, enforce=True)
        redis = _redis_with("0.99", "15000", "99999")
        public_bot_handler._redis_connection = redis
        record = mocker.patch("shared.torngit.github.record_repo_request")

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        # installation token is not in the configured bots list
        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use={
                "key": "k",
                "username": "installation_123",
                "entity_name": "254_123",
            },
        )
        assert result is res
        client.request.assert_awaited_once()
        record.assert_not_called()

    @pytest.mark.asyncio
    async def test_redis_error_fails_open(
        self, public_bot_handler, mock_configuration, mocker
    ):
        _enable_config(mock_configuration, enforce=True)
        redis = MagicMock()
        redis.get.side_effect = RedisError
        public_bot_handler._redis_connection = redis

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        # A Redis failure must not block the provider request
        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use=COMMIT_TOKEN,
        )
        assert result is res
        client.request.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_disabled_config_no_enforcement(
        self, public_bot_handler, mock_configuration, mocker
    ):
        # config not set -> disabled by default
        redis = _redis_with("0.99", "15000", "99999")
        public_bot_handler._redis_connection = redis
        record = mocker.patch("shared.torngit.github.record_repo_request")

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use=COMMIT_TOKEN,
        )
        assert result is res
        client.request.assert_awaited_once()
        record.assert_not_called()
