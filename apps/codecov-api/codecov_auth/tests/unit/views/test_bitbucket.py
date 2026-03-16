from unittest.mock import call, patch

from django.core import signing
from django.http.cookie import SimpleCookie
from django.test import TestCase
from django.urls import reverse

from codecov_auth.models import Owner
from codecov_auth.views.bitbucket import BitbucketLoginView
from shared.torngit.bitbucket import Bitbucket
from shared.torngit.exceptions import (
    TorngitClientGeneralError,
)


def test_get_bitbucket_redirect(client, settings, mocker):
    mocked_generate = mocker.patch.object(
        Bitbucket,
        "generate_redirect_url",
        return_value="https://bitbucket.org/site/oauth2/authorize?client_id=testqmo19ebdkseoby&response_type=code&redirect_uri=http%3A%2F%2Flocalhost&state=teststate",
    )
    settings.BITBUCKET_REDIRECT_URI = "http://localhost"
    settings.BITBUCKET_CLIENT_ID = "testqmo19ebdkseoby"
    settings.BITBUCKET_CLIENT_SECRET = "testfi8hzehvz453qj8mhv21ca4rf83f"
    url = reverse("bitbucket-login")
    res = client.get(url, SERVER_NAME="localhost:8000")
    assert res.status_code == 302

    assert "_bb_oauth_state" in res.cookies
    cookie = res.cookies["_bb_oauth_state"]
    assert cookie.value
    assert cookie.get("domain") == settings.COOKIES_DOMAIN
    assert cookie.get("secure")
    assert mocked_generate.call_count == 1
    # state kwarg was passed through
    _, kwargs = mocked_generate.call_args
    assert kwargs.get("state") is not None


def test_get_bitbucket_redirect_bitbucket_error(client, settings, mocker):
    mocker.patch.object(
        Bitbucket,
        "generate_redirect_url",
        side_effect=TorngitClientGeneralError(400, {}, "bad request"),
    )
    settings.BITBUCKET_REDIRECT_URI = "http://localhost"
    settings.BITBUCKET_CLIENT_ID = "testqmo19ebdkseoby"
    settings.BITBUCKET_CLIENT_SECRET = "testfi8hzehvz453qj8mhv21ca4rf83f"
    url = reverse("bitbucket-login")
    res = client.get(url, SERVER_NAME="localhost:8000")
    assert res.status_code == 302
    assert "_bb_oauth_state" not in res.cookies
    assert res.url == url


async def fake_get_authenticated_user():
    return {
        "username": "ThiagoCodecov",
        "has_2fa_enabled": None,
        "display_name": "Thiago Ramos",
        "account_id": "5bce04c759d0e84f8c7555e9",
        "links": {
            "hooks": {
                "href": "https://bitbucket.org/!api/2.0/users/%7B9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645%7D/hooks"
            },
            "self": {
                "href": "https://bitbucket.org/!api/2.0/users/%7B9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645%7D"
            },
            "repositories": {
                "href": "https://bitbucket.org/!api/2.0/repositories/%7B9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645%7D"
            },
            "html": {
                "href": "https://bitbucket.org/%7B9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645%7D/"
            },
            "avatar": {
                "href": "https://avatar-management--avatars.us-west-2.prod.public.atl-paas.net/initials/TR-6.png"
            },
            "snippets": {
                "href": "https://bitbucket.org/!api/2.0/snippets/%7B9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645%7D"
            },
        },
        "nickname": "thiago",
        "created_on": "2018-11-06T12:12:59.588751+00:00",
        "is_staff": False,
        "location": None,
        "account_status": "active",
        "type": "user",
        "uuid": "{9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645}",
    }


def test_get_bitbucket_already_token(client, settings, mocker, db, mock_redis):
    mocker.patch.object(
        Bitbucket, "get_authenticated_user", side_effect=fake_get_authenticated_user
    )

    async def fake_list_teams():
        return []

    mocker.patch.object(Bitbucket, "list_teams", side_effect=fake_list_teams)
    mocker.patch(
        "services.task.TaskService.refresh",
        return_value=mocker.MagicMock(
            as_tuple=mocker.MagicMock(return_value=("a", "b"))
        ),
    )
    mocked_get = mocker.patch.object(
        Bitbucket,
        "generate_access_token",
        return_value={
            "key": "test6tl3evq7c8vuyn",
            "secret": "testrefreshtoken",
        },
    )
    settings.BITBUCKET_REDIRECT_URI = "http://localhost"
    settings.BITBUCKET_CLIENT_ID = "testqmo19ebdkseoby"
    settings.BITBUCKET_CLIENT_SECRET = "testfi8hzehvz453qj8mhv21ca4rf83f"
    settings.CODECOV_DASHBOARD_URL = "dashboard.value"
    settings.COOKIE_SECRET = "aaaaa"

    state = "test_state_value_abc123"
    url = reverse("bitbucket-login")
    client.cookies = SimpleCookie(
        {
            "_bb_oauth_state": signing.get_cookie_signer(salt="_bb_oauth_state").sign(
                state
            )
        }
    )
    mock_create_user_onboarding_metric = mocker.patch(
        "shared.django_apps.codecov_metrics.service.codecov_metrics.UserOnboardingMetricsService.create_user_onboarding_metric"
    )

    res = client.get(
        url,
        {"code": "auth_code_from_bitbucket", "state": state},
        SERVER_NAME="localhost:8000",
    )
    assert res.status_code == 302
    assert res.url == "dashboard.value/bb"
    assert "_bb_oauth_state" in res.cookies
    cookie = res.cookies["_bb_oauth_state"]
    assert cookie.value == ""
    assert cookie.get("domain") == settings.COOKIES_DOMAIN
    mocked_get.assert_called_with(
        "auth_code_from_bitbucket", settings.BITBUCKET_REDIRECT_URI
    )
    owner = Owner.objects.get(username="ThiagoCodecov", service="bitbucket")
    expected_call = call(
        org_id=owner.ownerid,
        event="INSTALLED_APP",
        payload={"login": "bitbucket"},
    )
    assert mock_create_user_onboarding_metric.call_args_list == [expected_call]


def test_get_bitbucket_already_token_no_state_cookie(
    client, settings, mocker, db, mock_redis
):
    mocker.patch(
        "services.task.TaskService.refresh",
        return_value=mocker.MagicMock(
            as_tuple=mocker.MagicMock(return_value=("a", "b"))
        ),
    )
    mocked_get = mocker.patch.object(
        Bitbucket,
        "generate_access_token",
        return_value={
            "key": "test6tl3evq7c8vuyn",
            "secret": "testrefreshtoken",
        },
    )
    settings.BITBUCKET_REDIRECT_URI = "http://localhost"
    settings.BITBUCKET_CLIENT_ID = "testqmo19ebdkseoby"
    settings.BITBUCKET_CLIENT_SECRET = "testfi8hzehvz453qj8mhv21ca4rf83f"
    url = reverse("bitbucket-login")
    res = client.get(
        url,
        {"code": "auth_code_from_bitbucket", "state": "some_state"},
        SERVER_NAME="localhost:8000",
    )
    assert res.status_code == 302
    assert res.url == "/login/bitbucket"
    assert not mocked_get.called


def test_get_bitbucket_state_mismatch(client, settings, mocker, db, mock_redis):
    mocked_get = mocker.patch.object(
        Bitbucket,
        "generate_access_token",
        return_value={
            "key": "test6tl3evq7c8vuyn",
            "secret": "testrefreshtoken",
        },
    )
    settings.BITBUCKET_REDIRECT_URI = "http://localhost"
    settings.BITBUCKET_CLIENT_ID = "testqmo19ebdkseoby"
    settings.BITBUCKET_CLIENT_SECRET = "testfi8hzehvz453qj8mhv21ca4rf83f"
    settings.COOKIE_SECRET = "aaaaa"

    url = reverse("bitbucket-login")
    client.cookies = SimpleCookie(
        {
            "_bb_oauth_state": signing.get_cookie_signer(salt="_bb_oauth_state").sign(
                "legit_state"
            )
        }
    )
    res = client.get(
        url,
        {"code": "auth_code_from_bitbucket", "state": "attacker_injected_state"},
        SERVER_NAME="localhost:8000",
    )
    assert res.status_code == 302
    assert res.url == "/login/bitbucket"
    assert not mocked_get.called


class TestBitbucketLoginView(TestCase):
    def test_fetch_user_data(self):
        async def fake_list_teams():
            return []

        with patch.object(
            Bitbucket, "get_authenticated_user", side_effect=fake_get_authenticated_user
        ):
            with patch.object(Bitbucket, "list_teams", side_effect=fake_list_teams):
                view = BitbucketLoginView()
                token = {"key": "aaaa", "secret": "bbbb"}
                res = view.fetch_user_data(token)
                assert res == {
                    "has_private_access": True,
                    "is_student": False,
                    "orgs": [],
                    "user": {
                        "key": "aaaa",
                        "secret": "bbbb",
                        "id": "9a01f37b-b1b2-40c5-8c5e-1a39f4b5e645",
                        "login": "ThiagoCodecov",
                    },
                }
