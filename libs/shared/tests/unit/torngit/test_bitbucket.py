from urllib.parse import parse_qsl, urlparse

import httpx
import pytest
import respx

from shared.torngit.bitbucket import Bitbucket
from shared.torngit.exceptions import (
    TorngitClientError,
    TorngitClientGeneralError,
    TorngitObjectNotFoundError,
    TorngitServer5xxCodeError,
    TorngitServerUnreachableError,
)


@pytest.fixture
def respx_vcr():
    with respx.mock as v:
        yield v


@pytest.fixture
def valid_handler():
    return Bitbucket(
        repo={"name": "example-python"},
        owner={
            "username": "ThiagoCodecov",
            "service_id": "6ef29b63-aaaa-aaaa-aaaa-aaaa03f5cd49",
        },
        oauth_consumer_token={
            "key": "oauth_consumer_key_value",
            "secret": "oauth_consumer_token_secret_value",
        },
        token={"secret": "somesecret", "key": "somekey"},
    )


class TestUnitBitbucket:
    @pytest.mark.asyncio
    async def test_api_client_error_unreachable(self, valid_handler, mocker):
        client = mocker.MagicMock(
            request=mocker.AsyncMock(return_value=mocker.MagicMock(status_code=599))
        )
        method = "GET"
        url = "random_url"
        with pytest.raises(TorngitServerUnreachableError):
            await valid_handler.api(client, "2", method, url)

    @pytest.mark.asyncio
    async def test_api_client_error_connect_error(self, valid_handler, mocker):
        client = mocker.MagicMock(
            request=mocker.AsyncMock(
                side_effect=httpx.ConnectError("message", request="request")
            )
        )
        method = "GET"
        url = "random_url"
        with pytest.raises(TorngitServerUnreachableError):
            await valid_handler.api(client, "2", method, url)

    @pytest.mark.asyncio
    async def test_api_client_error_server_error(self, valid_handler, mocker):
        client = mocker.MagicMock(
            request=mocker.AsyncMock(return_value=mocker.MagicMock(status_code=503))
        )
        method = "GET"
        url = "random_url"
        with pytest.raises(TorngitServer5xxCodeError):
            await valid_handler.api(client, "2", method, url)

    @pytest.mark.asyncio
    async def test_api_client_error_client_error(self, valid_handler, mocker):
        client = mocker.MagicMock(
            request=mocker.AsyncMock(return_value=mocker.MagicMock(status_code=404))
        )
        method = "GET"
        url = "random_url"
        with pytest.raises(TorngitClientError):
            await valid_handler.api(client, "2", method, url)

    @pytest.mark.asyncio
    async def test_api_client_proper_params(self, valid_handler, mocker):
        client = mocker.MagicMock(
            request=mocker.AsyncMock(
                return_value=mocker.MagicMock(text="kowabunga", status_code=200)
            )
        )
        method = "GET"
        url = "/random_url"
        res = await valid_handler.api(client, "2", method, url)
        assert res == "kowabunga"
        assert client.request.call_count == 1
        args, kwargs = client.request.call_args
        assert len(args) == 2
        assert args[0] == "GET"
        built_url = args[1]
        parsed_url = urlparse(built_url)
        assert parsed_url.scheme == "https"
        assert parsed_url.netloc == "api.bitbucket.org"
        assert parsed_url.path == "/2.0/random_url"
        assert parsed_url.params == ""
        assert parsed_url.fragment == ""
        # OAuth 2.0: auth is via Bearer token in headers, not query params
        query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))
        assert query == {}
        assert kwargs["headers"]["Authorization"] == "Bearer somekey"

    def test_generate_redirect_url(self, valid_handler):
        url = valid_handler.generate_redirect_url(
            "https://codecov.io/login/bitbucket", state="teststate123"
        )
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))
        assert parsed.scheme == "https"
        assert parsed.netloc == "bitbucket.org"
        assert parsed.path == "/site/oauth2/authorize"
        assert query["client_id"] == "oauth_consumer_key_value"
        assert query["response_type"] == "code"
        assert query["redirect_uri"] == "https://codecov.io/login/bitbucket"
        assert query["state"] == "teststate123"

    def test_generate_redirect_url_without_state(self, valid_handler):
        url = valid_handler.generate_redirect_url("https://codecov.io/login/bitbucket")
        parsed = urlparse(url)
        query = dict(parse_qsl(parsed.query))
        assert "state" not in query

    def test_generate_access_token(self, valid_handler):
        with respx.mock:
            my_route = respx.post(
                "https://bitbucket.org/site/oauth2/access_token"
            ).mock(
                return_value=httpx.Response(
                    status_code=200,
                    json={
                        "access_token": "testss3hxhcfqf1h6g",
                        "refresh_token": "testrefreshtoken",
                        "token_type": "bearer",
                        "scopes": "account",
                    },
                )
            )
            v = valid_handler.generate_access_token(
                "auth_code_from_bitbucket", "https://codecov.io/login/bitbucket"
            )
            assert v == {
                "key": "testss3hxhcfqf1h6g",
                "secret": "testrefreshtoken",
            }
            assert my_route.call_count == 1

    def test_generate_access_token_4xx(self, valid_handler):
        with respx.mock:
            respx.post("https://bitbucket.org/site/oauth2/access_token").mock(
                return_value=httpx.Response(
                    status_code=400, json={"error": "invalid_grant"}
                )
            )
            with pytest.raises(TorngitClientGeneralError):
                valid_handler.generate_access_token(
                    "bad_code", "https://codecov.io/login/bitbucket"
                )

    def test_generate_access_token_5xx(self, valid_handler):
        with respx.mock:
            respx.post("https://bitbucket.org/site/oauth2/access_token").mock(
                return_value=httpx.Response(status_code=503)
            )
            with pytest.raises(TorngitServer5xxCodeError):
                valid_handler.generate_access_token(
                    "code", "https://codecov.io/login/bitbucket"
                )

    @pytest.mark.asyncio
    async def test_api_token_refresh_on_401(self, mocker):
        new_token = {"key": "new_access_token", "secret": "new_refresh_token"}
        on_token_refresh = mocker.AsyncMock()
        handler = Bitbucket(
            repo={"name": "example-python"},
            owner={"username": "ThiagoCodecov"},
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
            token={"key": "expired_token", "secret": "old_refresh_token"},
            on_token_refresh=on_token_refresh,
        )
        mocker.patch.object(handler, "refresh_token", return_value=new_token)
        response_401 = mocker.MagicMock(
            status_code=401,
            text="Unauthorized",
            headers={"Content-Type": "text/plain"},
        )
        response_200 = mocker.MagicMock(
            status_code=200,
            text="ok",
            headers={"Content-Type": "text/plain"},
        )
        client = mocker.MagicMock(
            request=mocker.AsyncMock(side_effect=[response_401, response_200])
        )
        result = await handler.api(client, "2", "GET", "/some/path")
        assert result == "ok"
        assert client.request.call_count == 2
        on_token_refresh.assert_awaited_once_with(new_token)
        # Second call should use the new token
        _, kwargs = client.request.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer new_access_token"

    @pytest.mark.asyncio
    async def test_api_no_refresh_without_callback(self, mocker):
        """Without on_token_refresh, a 401 should raise immediately."""
        handler = Bitbucket(
            repo={"name": "example-python"},
            owner={"username": "ThiagoCodecov"},
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
            token={"key": "expired_token", "secret": "old_refresh_token"},
        )
        client = mocker.MagicMock(
            request=mocker.AsyncMock(
                return_value=mocker.MagicMock(
                    status_code=401,
                    text="Unauthorized",
                    headers={"Content-Type": "text/plain"},
                )
            )
        )
        with pytest.raises(TorngitClientGeneralError):
            await handler.api(client, "2", "GET", "/some/path")
        assert client.request.call_count == 1

    @pytest.mark.asyncio
    async def test_refresh_token(self, valid_handler):
        new_token = {
            "access_token": "new_access_token",
            "refresh_token": "new_refresh_token",
            "token_type": "bearer",
        }
        valid_handler._token = {"key": "old_access", "secret": "old_refresh"}
        with respx.mock:
            respx.post("https://bitbucket.org/site/oauth2/access_token").mock(
                return_value=httpx.Response(status_code=200, json=new_token)
            )
            async with httpx.AsyncClient() as client:
                result = await valid_handler.refresh_token(client)
        assert result == {"key": "new_access_token", "secret": "new_refresh_token"}
        assert valid_handler.token == result

    @pytest.mark.asyncio
    async def test_refresh_token_no_refresh_token(self, valid_handler):
        """If no refresh_token stored, refresh_token() returns None."""
        valid_handler._token = {"key": "old_access", "secret": None}
        async with httpx.AsyncClient() as client:
            result = await valid_handler.refresh_token(client)
        assert result is None

    @pytest.mark.asyncio
    async def test_api_refresh_failure_raises_original_401(self, mocker):
        """If refresh_token() raises, the original 401 error is raised."""
        on_token_refresh = mocker.AsyncMock()
        handler = Bitbucket(
            repo={"name": "example-python"},
            owner={"username": "ThiagoCodecov"},
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
            token={"key": "expired_token", "secret": "old_refresh_token"},
            on_token_refresh=on_token_refresh,
        )
        mocker.patch.object(
            handler,
            "refresh_token",
            side_effect=TorngitClientGeneralError(400, {}, "invalid_grant"),
        )
        client = mocker.MagicMock(
            request=mocker.AsyncMock(
                return_value=mocker.MagicMock(
                    status_code=401,
                    text="Unauthorized",
                    reason_phrase="Unauthorized",
                    content=b"Unauthorized",
                    headers={"Content-Type": "text/plain"},
                )
            )
        )
        with pytest.raises(TorngitClientGeneralError):
            await handler.api(client, "2", "GET", "/some/path")
        assert client.request.call_count == 1
        on_token_refresh.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "has_write_access, expected_result",
        [(True, (True, True)), (False, (True, False))],
    )
    async def test_get_authenticated_private_200_status_some_permissions(
        self, mocker, respx_vcr, has_write_access, expected_result
    ):
        respx.get(
            "https://api.bitbucket.org/2.0/repositories/ThiagoCodecov/example-python"
        ).respond(status_code=200, json={})
        values = (
            [{"full_name": "ThiagoCodecov/example-python"}] if has_write_access else []
        )
        respx.get("https://api.bitbucket.org/2.0/repositories").respond(
            status_code=200,
            json={"pagelen": 10, "values": values, "page": 1},
        )
        handler = Bitbucket(
            repo={"name": "example-python", "private": True},
            owner={
                "username": "ThiagoCodecov",
                "service_id": "6ef29b63-aaaa-aaaa-aaaa-aaaa03f5cd49",
            },
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
            token={"secret": "somesecret", "key": "somekey"},
        )
        res = await handler.get_authenticated()
        assert res == expected_result

    @pytest.mark.asyncio
    async def test_get_authenticated_private_200_status_no_permission(
        self, mocker, respx_vcr
    ):
        respx.get(
            "https://api.bitbucket.org/2.0/repositories/ThiagoCodecov/example-python"
        ).respond(status_code=200, json={})
        respx.get("https://api.bitbucket.org/2.0/repositories").respond(
            status_code=200, json={"pagelen": 10, "values": [], "page": 1}
        )
        handler = Bitbucket(
            repo={"name": "example-python", "private": True},
            owner={
                "username": "ThiagoCodecov",
                "service_id": "6ef29b63-aaaa-aaaa-aaaa-aaaa03f5cd49",
            },
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
            token={"secret": "somesecret", "key": "somekey"},
        )
        res = await handler.get_authenticated()
        assert res == (True, False)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "permission_name, expected_result",
        [("read", (True, False)), ("write", (True, True)), ("admin", (True, True))],
    )
    async def test_get_authenticated_private_404_status(
        self, mocker, respx_vcr, permission_name, expected_result
    ):
        respx.get(
            "https://api.bitbucket.org/2.0/repositories/ThiagoCodecov/example-python"
        ).respond(status_code=404, json={})
        handler = Bitbucket(
            repo={"name": "example-python", "private": True},
            owner={
                "username": "ThiagoCodecov",
                "service_id": "6ef29b63-aaaa-aaaa-aaaa-aaaa03f5cd49",
            },
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
            token={"secret": "secret", "key": "key"},
        )
        with pytest.raises(TorngitClientError):
            await handler.get_authenticated()

    @pytest.mark.asyncio
    async def test_list_repos_exception_mid_call(self, valid_handler, respx_vcr):
        respx.get("https://api.bitbucket.org/2.0/user/workspaces").respond(
            status_code=200,
            json={
                "values": [
                    {
                        "type": "workspace_access",
                        "workspace": {
                            "type": "workspace_base",
                            "slug": "specialslug",
                            "uuid": "[uuid]",
                        },
                    },
                    {
                        "type": "workspace_access",
                        "workspace": {
                            "type": "workspace_base",
                            "slug": "anotherslug",
                            "uuid": "[abcdef]",
                        },
                    },
                ]
            },
        )
        respx.get("https://api.bitbucket.org/2.0/repositories").respond(
            status_code=200,
            json={"values": [{"full_name": "codecov/worker"}]},
        )
        respx.get("https://api.bitbucket.org/2.0/repositories/specialslug").respond(
            status_code=200, json={"values": []}
        )
        respx.get("https://api.bitbucket.org/2.0/repositories/ThiagoCodecov").respond(
            status_code=200, json={"values": []}
        )
        respx.get("https://api.bitbucket.org/2.0/repositories/anotherslug").respond(
            status_code=200,
            json={
                "values": [
                    {
                        "is_private": True,
                        "language": "python",
                        "uuid": "[haja]",
                        "full_name": "anotherslug/aaaa",
                        "owner": {"uuid": "[poilk]"},
                    },
                    {
                        "is_private": True,
                        "language": "python",
                        "uuid": "[haja]",
                        "full_name": "anotherslug/qwerty",
                        "owner": {"uuid": "[poilk]"},
                    },
                ]
            },
        )
        respx.get("https://api.bitbucket.org/2.0/repositories/codecov").respond(
            status_code=404, json={"values": []}
        )
        res = await valid_handler.list_repos()
        assert res == [
            {
                "owner": {"service_id": "poilk", "username": "anotherslug"},
                "repo": {
                    "service_id": "haja",
                    "name": "aaaa",
                    "language": "python",
                    "private": True,
                    "branch": "main",
                },
            },
            {
                "owner": {"service_id": "poilk", "username": "anotherslug"},
                "repo": {
                    "service_id": "haja",
                    "name": "qwerty",
                    "language": "python",
                    "private": True,
                    "branch": "main",
                },
            },
        ]

    @pytest.mark.asyncio
    async def test_get_compare(self, valid_handler, respx_vcr):
        diff = "\n".join(
            [
                "diff --git a/README.md b/README.md",
                "index 87f9baa..51c8a2d 100644",
                "--- a/README.md",
                "+++ b/README.md",
                "@@ -14,4 +14,2 @@ Truces luctuque cognovit, cum lanam ordine vereri relinquunt sit munere quidam.",
                " Solent **torvi clamare successit** ille memores rogum; serpens egi caelo,",
                "-moventem gelido volucrum reddidit fatalia, *in*. Abdit instant et, et hostis",
                "-amores, nec pater formosus mortis capiat eripui: ferarum extemplo. Inmeritas",
                " favilla qui Dauno portis Aello! Fluit inde magis vinci hastam amore, mihi fama",
            ]
        )
        base, head = "6ae5f17", "b92edba"
        respx.get(
            "https://api.bitbucket.org/2.0/repositories/ThiagoCodecov/example-python/diff/b92edba..6ae5f17",
            params__contains={"context": "1"},
        ).respond(status_code=200, content=diff, headers={"Content-Type": "aaaa"})
        expected_result = {
            "diff": {
                "files": {
                    "README.md": {
                        "type": "modified",
                        "before": None,
                        "segments": [
                            {
                                "header": ["14", "4", "14", "2"],
                                "lines": [
                                    " Solent **torvi clamare successit** ille memores rogum; serpens egi caelo,",
                                    "-moventem gelido volucrum reddidit fatalia, *in*. Abdit instant et, et hostis",
                                    "-amores, nec pater formosus mortis capiat eripui: ferarum extemplo. Inmeritas",
                                    " favilla qui Dauno portis Aello! Fluit inde magis vinci hastam amore, mihi fama",
                                ],
                            }
                        ],
                        "stats": {"added": 0, "removed": 2},
                    }
                }
            },
            "commits": [{"commitid": "b92edba"}, {"commitid": "6ae5f17"}],
        }
        res = await valid_handler.get_compare(base, head)
        assert sorted(res.keys()) == sorted(expected_result.keys())
        assert res == expected_result

    @pytest.mark.asyncio
    async def test_get_distance_in_commits(self):
        expected_result = {
            "behind_by": None,
            "behind_by_commit": None,
            "status": None,
            "ahead_by": None,
        }
        handler = Bitbucket(
            repo={"name": "example-python", "private": True},
        )
        res = await handler.get_distance_in_commits("branch", "commit")
        assert res == expected_result

    @pytest.mark.asyncio
    async def test_get_repo_languages(self):
        expected_result = ["javascript"]
        handler = Bitbucket(
            repo={"name": "example-python", "private": True},
        )
        res = await handler.get_repo_languages(None, "JavaScript")
        assert res == expected_result

    @pytest.mark.asyncio
    async def test_get_repo_no_languages(self):
        expected_result = []
        handler = Bitbucket(
            repo={"name": "example-python", "private": True},
        )
        res = await handler.get_repo_languages(None, None)
        assert res == expected_result

    @pytest.mark.asyncio
    async def test_get_pull_rquest_files(self, valid_handler):
        handler = Bitbucket(
            repo={"name": "test-repo"},
            owner={"username": "e2e-org"},
            token={"secret": "somesecret", "key": "somekey"},
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
        )
        with respx.mock:
            respx.get(
                "https://api.bitbucket.org/2.0/repositories/e2e-org/test-repo/pullrequests/1/diffstat"
            ).mock(
                return_value=httpx.Response(
                    status_code=200,
                    json={
                        "values": [
                            {
                                "type": "diffstat",
                                "lines_added": 1,
                                "lines_removed": 1,
                                "status": "modified",
                                "old": {
                                    "path": "README.md",
                                    "type": "commit_file",
                                    "escaped_path": "README.md",
                                    "links": {
                                        "self": {
                                            "href": "https://bitbucket.org/!api/2.0/repositories/e2e-org/test-repo/src/d32434b65381acce9709e11234c0ba5ce2a9f515/README.md"
                                        }
                                    },
                                },
                                "new": {
                                    "path": "README.md",
                                    "type": "commit_file",
                                    "escaped_path": "README.md",
                                    "links": {
                                        "self": {
                                            "href": "https://bitbucket.org/!api/2.0/repositories/e2e-org/test-repo/src/8ed929e26c3e9dd51bb7abefc89f4f5044ff28fe/README.md"
                                        }
                                    },
                                },
                            }
                        ],
                        "pagelen": 500,
                        "size": 1,
                        "page": 1,
                    },
                )
            )
            v = await handler.get_pull_request_files("1")
            assert v == [
                "README.md",
            ]

    @pytest.mark.asyncio
    async def test_get_pull_request_files_404(self):
        handler = Bitbucket(
            repo={"name": "test-repo"},
            owner={"username": "e2e-org"},
            token={"secret": "somesecret", "key": "somekey"},
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
        )
        with respx.mock:
            respx.get(
                "https://api.bitbucket.org/2.0/repositories/e2e-org/test-repo/pullrequests/4/diffstat"
            ).mock(
                return_value=httpx.Response(
                    status_code=404,
                    headers={
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": "1350085394",
                    },
                )
            )
            with pytest.raises(TorngitObjectNotFoundError) as excinfo:
                await handler.get_pull_request_files(4)
            assert excinfo.value.code == 404
            assert excinfo.value.message == "PR with id 4 does not exist"

    @pytest.mark.asyncio
    async def test_get_pull_request_files_403(self):
        handler = Bitbucket(
            repo={"name": "test-repo"},
            owner={"username": "e2e-org"},
            token={"secret": "somesecret", "key": "somekey"},
            oauth_consumer_token={
                "key": "oauth_consumer_key_value",
                "secret": "oauth_consumer_token_secret_value",
            },
        )
        with respx.mock:
            respx.get(
                "https://api.bitbucket.org/2.0/repositories/e2e-org/test-repo/pullrequests/4/diffstat"
            ).mock(
                return_value=httpx.Response(
                    status_code=403,
                    headers={
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": "1350085394",
                    },
                )
            )
            with pytest.raises(TorngitClientError) as excinfo:
                await handler.get_pull_request_files(4)
            assert excinfo.value.code == 403
            assert excinfo.value.message == "Bitbucket API: Forbidden"
