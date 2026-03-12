from unittest.mock import MagicMock

from services.lock_manager import LockManager, LockType
from services.processing.finisher_gate import (
    FINISHER_GATE_KEY_PREFIX,
    FINISHER_GATE_TTL_SECONDS,
    delete_finisher_gate,
    finisher_gate_key,
    refresh_finisher_gate_ttl,
    try_acquire_finisher_gate,
)


def test_finisher_gate_key_uses_distinct_prefix():
    manager = LockManager(repoid=123, commitid="abc123")
    upload_finisher_lock_key = manager.lock_name(LockType.UPLOAD_FINISHER)
    gate_key = finisher_gate_key(123, "abc123")

    assert FINISHER_GATE_KEY_PREFIX == "upload_finisher_gate"
    assert gate_key == "upload_finisher_gate_123_abc123"
    assert gate_key != upload_finisher_lock_key


def test_try_acquire_finisher_gate_success(mocker):
    redis_connection = MagicMock()
    redis_connection.set.return_value = True
    mocker.patch(
        "services.processing.finisher_gate.get_redis_connection",
        return_value=redis_connection,
    )

    acquired = try_acquire_finisher_gate(123, "abc123")

    assert acquired is True
    redis_connection.set.assert_called_once_with(
        "upload_finisher_gate_123_abc123",
        "1",
        nx=True,
        ex=FINISHER_GATE_TTL_SECONDS,
    )


def test_try_acquire_finisher_gate_failure(mocker):
    redis_connection = MagicMock()
    redis_connection.set.return_value = None
    mocker.patch(
        "services.processing.finisher_gate.get_redis_connection",
        return_value=redis_connection,
    )

    acquired = try_acquire_finisher_gate(123, "abc123")

    assert acquired is False


def test_refresh_finisher_gate_ttl_returns_true_when_key_exists(mocker):
    redis_connection = MagicMock()
    redis_connection.expire.return_value = 1
    mocker.patch(
        "services.processing.finisher_gate.get_redis_connection",
        return_value=redis_connection,
    )

    refreshed = refresh_finisher_gate_ttl(123, "abc123")

    assert refreshed is True
    redis_connection.expire.assert_called_once_with(
        "upload_finisher_gate_123_abc123", FINISHER_GATE_TTL_SECONDS
    )


def test_refresh_finisher_gate_ttl_returns_false_when_key_missing(mocker):
    redis_connection = MagicMock()
    redis_connection.expire.return_value = 0
    mocker.patch(
        "services.processing.finisher_gate.get_redis_connection",
        return_value=redis_connection,
    )

    refreshed = refresh_finisher_gate_ttl(123, "abc123")

    assert refreshed is False


def test_delete_finisher_gate(mocker):
    redis_connection = MagicMock()
    mocker.patch(
        "services.processing.finisher_gate.get_redis_connection",
        return_value=redis_connection,
    )

    delete_finisher_gate(123, "abc123")

    redis_connection.delete.assert_called_once_with("upload_finisher_gate_123_abc123")
