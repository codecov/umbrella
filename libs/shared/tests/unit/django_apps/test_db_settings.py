import pytest

from shared.django_apps import db_settings


def test_ta_timeseries_connect_timeout_default():
    assert db_settings.TA_TIMESERIES_CONNECT_TIMEOUT == 5


def test_ta_timeseries_database_includes_connect_timeout_when_enabled():
    if not db_settings.TA_TIMESERIES_ENABLED:
        pytest.skip("TA timeseries disabled in this test environment")

    ta_db = db_settings.DATABASES["ta_timeseries"]
    assert (
        ta_db["OPTIONS"]["connect_timeout"] == db_settings.TA_TIMESERIES_CONNECT_TIMEOUT
    )
    assert "options" not in ta_db["OPTIONS"]
