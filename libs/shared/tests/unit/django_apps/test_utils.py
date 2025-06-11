from unittest.mock import patch

import pytest

from shared.django_apps.utils.config import get_settings_module


class TestSettingsModule:
    @pytest.mark.parametrize(
        "run_env,expected",
        [
            ("DEV", "settings_dev"),
            ("STAGING", "settings_staging"),
            ("TESTING", "settings_test"),
            ("PRODUCTION", "settings_prod"),
            ("ENTERPRISE", "settings_enterprise"),
        ],
    )
    def test_get_settings_module(self, run_env, expected):
        with patch("shared.django_apps.utils.config.RUN_ENV", run_env):
            # test with default parent module
            with patch("shared.django_apps.utils.config._settings_module", None):
                assert get_settings_module() == f"codecov.{expected}"

            # test with custom parent module
            with patch("shared.django_apps.utils.config._settings_module", None):
                assert get_settings_module("custom") == f"custom.{expected}"

    @patch("shared.django_apps.utils.config._settings_module", "something")
    def test_get_settings_module_already_set(self):
        assert get_settings_module() == "something"
