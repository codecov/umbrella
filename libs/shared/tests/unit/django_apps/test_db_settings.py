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


def test_default_database_includes_connect_timeout():
    assert db_settings.DATABASE_CONNECT_TIMEOUT == 10
    default_db = db_settings.DATABASES["default"]
    assert (
        default_db["OPTIONS"]["connect_timeout"] == db_settings.DATABASE_CONNECT_TIMEOUT
    )


def test_default_read_database_includes_connect_timeout_when_enabled():
    if not db_settings.DATABASE_READ_REPLICA_ENABLED:
        pytest.skip("Read replica disabled in this test environment")

    read_db = db_settings.DATABASES["default_read"]
    assert (
        read_db["OPTIONS"]["connect_timeout"]
        == db_settings.DATABASE_READ_CONNECT_TIMEOUT
    )
