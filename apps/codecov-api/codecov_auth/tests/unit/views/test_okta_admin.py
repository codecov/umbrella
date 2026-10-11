import logging

from django.contrib import auth
from django.test import override_settings
from django.urls import reverse

from codecov_auth.models import OktaUser
from codecov_auth.views.okta_mixin import OktaIdTokenPayload
from shared.django_apps.codecov_auth.tests.factories import OktaUserFactory

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


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_logs_diagnostic_info_on_state_failure(
    client, mocker, db, caplog
):
    """Test that enhanced logging captures diagnostic info when state verification fails."""
    caplog.set_level(logging.WARNING, logger="codecov_auth.views.okta_admin")

    # Set up a session with a different state than what we'll send
    session = client.session
    session["okta_admin_oauth_state"] = "expected-state"
    session.save()
    session_key = client.session.session_key

    # Send a request with wrong state
    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": "wrong-state"},
    )

    # Verify redirect to login
    assert res.status_code == 302
    assert res.url == "/admin/login/"

    # Verify enhanced logging was captured
    assert len(caplog.records) == 1
    log_record = caplog.records[0]

    assert log_record.levelname == "WARNING"
    assert log_record.message == "Invalid state during Okta admin login callback"

    # Verify diagnostic info in extra fields
    assert "state_param" in log_record.__dict__
    assert log_record.state_param == "wrong-state"

    assert "has_session_state" in log_record.__dict__
    assert log_record.has_session_state is True  # Session has state, just wrong one

    assert "session_key" in log_record.__dict__
    assert log_record.session_key == session_key


@override_settings(**_ADMIN_SETTINGS)
def test_okta_admin_callback_logs_missing_session_state(client, mocker, db, caplog):
    """Test logging when session has no state stored (e.g., cookie blocked)."""
    caplog.set_level(logging.WARNING, logger="codecov_auth.views.okta_admin")

    # Don't set any state in session - simulates cookie being blocked
    # Session exists but has no okta_admin_oauth_state key
    session_key = client.session.session_key

    res = client.get(
        reverse("okta-admin-login"),
        data={"code": "test-code", "state": "some-state"},
    )

    assert res.status_code == 302
    assert len(caplog.records) == 1
    log_record = caplog.records[0]

    # Verify has_session_state is False when no state in session
    assert log_record.has_session_state is False
    assert log_record.state_param == "some-state"
