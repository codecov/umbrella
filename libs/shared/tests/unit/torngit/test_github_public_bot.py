from unittest.mock import MagicMock

import httpx
import pytest
from redis import RedisError

from shared.torngit.github import Github


@pytest.fixture
def public_bot_handler():
    return Github(
        repo={"name": "mongo-python-driver"},
        owner={"username": "mongodb"},
        token={"key": "some_key"},
        oauth_consumer_token={"key": "client_id", "secret": "client_secret"},
    )


def _ok_response():
    return httpx.Response(
        status_code=200,
        headers={
            "X-RateLimit-Remaining": "7500",
            "X-RateLimit-Limit": "15000",
            "X-RateLimit-Reset": "4000000000",
            "Content-Type": "application/json",
        },
        json=[{"sha": "abc"}],
    )


COMMIT_TOKEN = {
    "key": "shared_key",
    "username": "commit_dedicated_app",
    "entity_name": "commit",
}


class TestMakeHttpCallPublicBotCollection:
    @pytest.mark.asyncio
    async def test_public_bot_records_usage(self, public_bot_handler, mocker):
        record_pool = mocker.patch("shared.torngit.github.record_pool_state")
        record_repo = mocker.patch("shared.torngit.github.record_repo_request")
        inc = mocker.patch("shared.torngit.github.inc_counter")
        public_bot_handler._redis_connection = MagicMock()

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        result = await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use=COMMIT_TOKEN,
        )

        assert result is res
        record_pool.assert_called_once()
        record_repo.assert_called_once()
        assert inc.call_count == 1

    @pytest.mark.asyncio
    async def test_non_public_bot_is_untouched(self, public_bot_handler, mocker):
        record_repo = mocker.patch("shared.torngit.github.record_repo_request")
        public_bot_handler._redis_connection = MagicMock()

        res = _ok_response()
        client = mocker.MagicMock(request=mocker.AsyncMock(return_value=res))

        await public_bot_handler.make_http_call(
            client,
            "GET",
            "/repos/mongodb/mongo-python-driver/commits",
            token_to_use={
                "key": "k",
                "username": "installation_123",
                "entity_name": "254_123",
            },
        )

        record_repo.assert_not_called()

    @pytest.mark.asyncio
    async def test_redis_error_fails_open(self, public_bot_handler, mocker):
        mocker.patch(
            "shared.torngit.github.record_repo_request",
            side_effect=RedisError,
        )
        public_bot_handler._redis_connection = MagicMock()

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
