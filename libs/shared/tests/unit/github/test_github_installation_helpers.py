import pickle
from time import time
from unittest.mock import patch

from django.test import override_settings
from freezegun import freeze_time

# This import here avoids a circular import issue
from shared.github import (
    InvalidInstallationError,
    get_github_jwt_token,
    get_pem,
    get_pem_overrides,
)
from shared.utils.test_utils import mock_config_helper


def test_can_unpickle_invalid_installation_error():
    exception = InvalidInstallationError("permission_error")
    pickled = pickle.dumps(exception)
    unpickled = pickle.loads(pickled)
    assert str(unpickled) == str(exception)


@freeze_time("2024-02-21T00:00:00")
@patch("shared.github.jwt")
def test_get_github_jwt_token(mock_jwt, mocker):
    mock_jwt.encode.return_value = "encoded_jwt"
    configs = {"github.integration.id": 15000, "github.integration.expires": 300}
    file_configs = {"github.integration.pem": "--------BEGIN RSA PRIVATE KEY-----..."}
    mock_config_helper(mocker, configs, file_configs)
    token = get_github_jwt_token("github")
    assert token == "encoded_jwt"
    mock_jwt.encode.assert_called_with(
        {
            "iat": int(time()),
            "exp": int(time()) + 300,
            "iss": 15000,
        },
        "--------BEGIN RSA PRIVATE KEY-----...",
        algorithm="RS256",
    )


@freeze_time("2024-02-21T00:00:00")
@patch("shared.github.jwt")
@override_settings(GITHUB_SENTRY_APP_ID="test_sentry_app_id")
def test_get_github_jwt_token_with_override(mock_jwt, mocker):
    mock_jwt.encode.return_value = "encoded_jwt"
    configs = {"github.integration.expires": 300}
    file_configs = {
        "github.sentry_merge_app_pem": "--------BEGIN RSA PRIVATE KEY-----..."
    }
    mock_config_helper(mocker, configs, file_configs)

    token = get_github_jwt_token("github", app_id="test_sentry_app_id")
    assert token == "encoded_jwt"
    mock_jwt.encode.assert_called_with(
        {
            "iat": int(time()),
            "exp": int(time()) + 300,
            "iss": "test_sentry_app_id",
        },
        "--------BEGIN RSA PRIVATE KEY-----...",
        algorithm="RS256",
    )


@override_settings(GITHUB_SENTRY_APP_ID=None)
def test_get_pem_overrides_empty():
    result = get_pem_overrides("some_app_id")
    assert result is None


@override_settings(GITHUB_SENTRY_APP_ID="test_app_123")
def test_get_pem_overrides_with_settings():
    result = get_pem_overrides("test_app_123")
    assert result == ("github", "sentry_merge_app_pem")


@override_settings(
    GITHUB_SENTRY_APP_ID="test_app_123", GITHUB_SENTRY_APP_PEM="test_pem_content"
)
def test_get_pem_overrides_with_non_matching_app_id():
    result = get_pem_overrides("different_app_id")
    assert result is None


def test_get_pem_with_pem_path(mocker):
    file_configs = {"test.pem.path": "pem_content"}
    mock_config_helper(mocker, {}, file_configs)
    result = get_pem("app_id", "github", pem_path="yaml+file://test.pem.path")
    assert result == "pem_content"


@override_settings(GITHUB_SENTRY_APP_ID="test_sentry_app_id")
def test_get_pem_with_override(mocker):
    file_configs = {"github.sentry_merge_app_pem": "override_pem_content"}
    mock_config_helper(mocker, {}, file_configs)

    result = get_pem("test_sentry_app_id", "github")
    assert result == "override_pem_content"


@override_settings(GITHUB_SENTRY_APP_ID=None)
def test_get_pem_with_github_service(mocker):
    file_configs = {"github.integration.pem": "github_pem_content"}
    mock_config_helper(mocker, {}, file_configs)

    result = get_pem("app_id", "github")
    assert result == "github_pem_content"


@override_settings(GITHUB_SENTRY_APP_ID=None)
def test_get_pem_with_github_enterprise_service(mocker):
    file_configs = {
        "github_enterprise.integration.pem": "github_enterprise_pem_content"
    }
    mock_config_helper(mocker, {}, file_configs)

    result = get_pem("app_id", "github_enterprise")
    assert result == "github_enterprise_pem_content"


@override_settings(GITHUB_SENTRY_APP_ID=None)
def test_get_pem_no_pem_provided():
    try:
        get_pem("app_id", "")
        assert False, "Expected exception was not raised"
    except Exception as e:
        assert str(e) == "No PEM provided to get installation token"
