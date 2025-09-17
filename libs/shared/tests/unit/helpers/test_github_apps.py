from django.test import override_settings

from shared.helpers.github_apps import default_installation_app_ids, is_configured
from shared.utils.test_utils import mock_config_helper


@override_settings(GITHUB_SENTRY_APP_ID=None)
def test_default_installation_app_ids_empty(mocker):
    configs = {"github.integration.id": None}
    mock_config_helper(mocker, configs, {})
    result = default_installation_app_ids()
    assert result == set()


@override_settings(GITHUB_SENTRY_APP_ID=456)
def test_default_installation_app_ids_with_config(mocker):
    configs = {"github.integration.id": 12345}
    mock_config_helper(mocker, configs, {})
    result = default_installation_app_ids()
    assert result == {"12345", "456"}


@override_settings(GITHUB_SENTRY_APP_ID=None)
def test_default_installation_app_ids_only_config(mocker):
    configs = {"github.integration.id": 12345}
    mock_config_helper(mocker, configs, {})
    result = default_installation_app_ids()
    assert result == {"12345"}


@override_settings(GITHUB_SENTRY_APP_ID=456)
def test_default_installation_app_ids_only_sentry(mocker):
    configs = {"github.integration.id": None}
    mock_config_helper(mocker, configs, {})
    result = default_installation_app_ids()
    assert result == {"456"}


@override_settings(GITHUB_SENTRY_APP_ID=456)
def test_is_configured_default_app(mocker):
    configs = {"github.integration.id": 12345}
    mock_config_helper(mocker, configs, {})
    assert is_configured(12345) is True
    assert is_configured(456) is True


def test_is_configured_custom_app_with_pem():
    assert is_configured(789, "yaml+file://custom.app.pem") is True


def test_is_configured_custom_app_without_pem():
    assert is_configured(789) is False
    assert is_configured(789, None) is False


def test_is_configured_custom_app_empty_pem():
    assert is_configured(789, "") is True


def test_is_configured_none_app_id():
    assert is_configured(None, "yaml+file://some.path.to.pem") is False
