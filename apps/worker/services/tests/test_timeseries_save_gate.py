from unittest.mock import MagicMock

import pytest

from services.processing.timeseries_save_gate import (
    TIMESERIES_SAVE_COUNTDOWN_SECONDS,
    TIMESERIES_SAVE_GATE_TTL_SECONDS,
    release_timeseries_save_gate,
    timeseries_save_gate_key,
    try_acquire_timeseries_save_gate,
)


@pytest.fixture
def redis_connection(mocker):
    connection = MagicMock()
    mocker.patch(
        "services.processing.timeseries_save_gate.get_redis_connection",
        return_value=connection,
    )
    return connection


def test_gate_ttl_is_below_countdown():
    # A finisher arriving after the gate expires must enqueue a fresh task
    assert TIMESERIES_SAVE_GATE_TTL_SECONDS < TIMESERIES_SAVE_COUNTDOWN_SECONDS


def test_gate_key():
    assert timeseries_save_gate_key(123, "abc123") == "timeseries_save_123_abc123"


def test_try_acquire_success(redis_connection):
    redis_connection.set.return_value = True

    assert try_acquire_timeseries_save_gate(123, "abc123") is True
    redis_connection.set.assert_called_once_with(
        "timeseries_save_123_abc123",
        "1",
        nx=True,
        ex=TIMESERIES_SAVE_GATE_TTL_SECONDS,
    )


def test_try_acquire_already_held(redis_connection):
    redis_connection.set.return_value = None

    assert try_acquire_timeseries_save_gate(123, "abc123") is False


def test_try_acquire_fails_open_on_redis_error(redis_connection):
    redis_connection.set.side_effect = Exception("redis down")

    assert try_acquire_timeseries_save_gate(123, "abc123") is True


def test_release_deletes_key(redis_connection):
    release_timeseries_save_gate(123, "abc123")

    redis_connection.delete.assert_called_once_with("timeseries_save_123_abc123")


def test_release_swallows_redis_error(redis_connection):
    redis_connection.delete.side_effect = Exception("redis down")

    release_timeseries_save_gate(123, "abc123")
