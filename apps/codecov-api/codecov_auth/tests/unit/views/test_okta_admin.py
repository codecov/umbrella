from django.contrib import auth
from django.test import override_settings
from django.urls import reverse

from codecov_auth.models import OktaUser, User
from codecov_auth.views.okta_mixin import OktaIdTokenPayload
from shared.django_apps.codecov_auth.tests.factories import (
    OktaUserFactory,
    UserFactory,
)

_ADMIN_SETTINGS = {
    "OKTA_ISS": "https://example.okta.com",
    "OKTA_ADMIN_CLIENT_ID": "test-admin-client-id",
    "OKTA_ADMIN_CLIENT_SECRET": "test-admin-client-secret",
    "OKTA_ADMIN_REDIRECT_URL": "https://localhost:8000/login/okta-admin",
    "DJANGO_ADMIN_URL": "admin",
}


def _mock_token_post(mocker, status_code=200):
    return mocker.patch(
        "codecov_auth.views.okta_mixin.requests.post",
        return_value=mocker.MagicMock(
            status_code=status_code,
            json=mocker.MagicMock(
                return_value={
                    "access_token": "test-access-token",
                    "id_token": "test-id-token",
                },
            ),
        ),
    )


def _mock_validate_id_token(mocker):
    return mocker.patch(
        "codecov_auth.views.okta_admin.validate_id_token",
        return_value=OktaIdTokenPayload(
            sub="test-okta-id",
            email="admin@example.com",
            name="Admin User",
            iss="https://example.okta.com",
            aud="test-admin-client-id",
        ),
    )


@override_settings(OKTA_ISS=None)
def test_okta_admin_login_unconfigured(client, db):
    res = client.get(reverse("okta-admin-login"))
    assert res.status_code == 503
    assert b"Okta SSO is not configured" in res.content


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_redirect_to_authorize(client, db):
    res = client.get(
        reverse("okta-admin-login"),
        data={"next": "/admin/"},
    )
    state = client.session["okta_admin_oauth_state"]

    assert res.status_code == 302
    assert client.session["okta_admin_next"] == "/admin/"
    expected = (
        "https://example.okta.com/oauth2/v1/authorize"
        "?response_type=code&client_id=test-admin-client-id"
        "&scope=openid+email+profile"
        "&redirect_uri=https%3A%2F%2Flocalhost%3A8000%2Flogin%2Fokta-admin"
        f"&state={state}"
    )
    assert res.url == expected


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_creates_user_and_sets_staff(client, mocker, db):
    _mock_token_post(mocker)
    _mock_validate_id_token(mocker)

    state = "test-state"
    session = client.session
    session["okta_admin_oauth_state"] = state
    session["okta_admin_next"] = "/admin/"
    session.save()

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": state},
    )

    assert res.status_code == 302
    assert res.url == "/admin/"

    okta_user = OktaUser.objects.get(okta_id="test-okta-id")
    assert okta_user.email == "admin@example.com"
    assert okta_user.name == "Admin User"
    assert okta_user.access_token == "test-access-token"

    user = okta_user.user
    assert user is not None
    assert user.is_staff is True
    assert user.email == "admin@example.com"
    assert user.name == "Admin User"

    current_user = auth.get_user(client)
    assert current_user == user
    assert "okta_admin_oauth_state" not in client.session
    assert "okta_admin_next" not in client.session


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_existing_okta_user(client, mocker, db):
    _mock_token_post(mocker)
    _mock_validate_id_token(mocker)
    existing = OktaUserFactory(okta_id="test-okta-id")
    assert existing.user.is_staff is not True

    state = "test-state"
    session = client.session
    session["okta_admin_oauth_state"] = state
    session["okta_admin_next"] = "/admin/codecov_auth/user/"
    session.save()

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": state},
    )

    assert res.status_code == 302
    assert res.url == "/admin/codecov_auth/user/"

    existing.user.refresh_from_db()
    assert existing.user.is_staff is True
    assert OktaUser.objects.filter(okta_id="test-okta-id").count() == 1

    current_user = auth.get_user(client)
    assert current_user == existing.user


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_links_existing_user_by_email(client, mocker, db):
    _mock_token_post(mocker)
    _mock_validate_id_token(mocker)
    existing_user = UserFactory(email="admin@example.com", name="Pre-existing")

    state = "test-state"
    session = client.session
    session["okta_admin_oauth_state"] = state
    session["okta_admin_next"] = "/admin/"
    session.save()

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": state},
    )

    assert res.status_code == 302

    # No duplicate User is created; the OktaUser links to the existing one.
    assert User.objects.filter(email="admin@example.com").count() == 1
    okta_user = OktaUser.objects.get(okta_id="test-okta-id")
    assert okta_user.user == existing_user

    existing_user.refresh_from_db()
    assert existing_user.is_staff is True
    assert auth.get_user(client) == existing_user


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_invalid_state(client, db):
    session = client.session
    session["okta_admin_oauth_state"] = "expected-state"
    session.save()

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": "wrong-state"},
    )

    assert res.status_code == 302
    assert res.url == "/admin/login/"
    assert auth.get_user(client).is_anonymous


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_token_exchange_failure(client, mocker, db):
    _mock_token_post(mocker, status_code=401)

    state = "test-state"
    session = client.session
    session["okta_admin_oauth_state"] = state
    session.save()

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": state},
    )

    assert res.status_code == 302
    assert res.url == "/admin/login/"
    assert auth.get_user(client).is_anonymous
    assert OktaUser.objects.count() == 0


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_id_token_validation_failure(client, mocker, db):
    _mock_token_post(mocker)
    mocker.patch(
        "codecov_auth.views.okta_admin.validate_id_token",
        side_effect=ValueError("bad token"),
    )

    state = "test-state"
    session = client.session
    session["okta_admin_oauth_state"] = state
    session.save()

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": state},
    )

    assert res.status_code == 302
    assert res.url == "/admin/login/"
    assert auth.get_user(client).is_anonymous
    assert OktaUser.objects.count() == 0
