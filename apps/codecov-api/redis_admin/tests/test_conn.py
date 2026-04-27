"""Unit tests for `redis_admin.conn` connection-kind routing.

The production deployment uses two distinct Redis instances — the
"cache" Memorystore reachable via `services.redis_url` and the Celery
broker Memorystore at `services.celery_broker`. These tests pin the
contract that `get_connection(kind=...)` honors that split, both for
real config-driven URLs and for the `REDIS_ADMIN_*_CONNECTION_FACTORY`
override hooks used by tests / operators.
"""

from __future__ import annotations

import fakeredis
import pytest
from django.test.utils import override_settings

from redis_admin import conn as redis_admin_conn

# Where this test module's fakeredis factories live so the dotted-path
# resolver can import them. The setting accepts any zero-arg callable.
_DEFAULT_FACTORY = "redis_admin.tests.test_conn._make_default_server"
_BROKER_FACTORY = "redis_admin.tests.test_conn._make_broker_server"

_default_server = fakeredis.FakeStrictRedis()
_broker_server = fakeredis.FakeStrictRedis()


def _make_default_server():
    return _default_server


def _make_broker_server():
    return _broker_server


def test_get_connection_default_kind_uses_default_factory():
    with override_settings(REDIS_ADMIN_CONNECTION_FACTORY=_DEFAULT_FACTORY):
        assert redis_admin_conn.get_connection() is _default_server
        assert redis_admin_conn.get_connection(kind="default") is _default_server


def test_get_connection_broker_kind_uses_broker_factory():
    with override_settings(
        REDIS_ADMIN_CONNECTION_FACTORY=_DEFAULT_FACTORY,
        REDIS_ADMIN_BROKER_CONNECTION_FACTORY=_BROKER_FACTORY,
    ):
        assert redis_admin_conn.get_connection(kind="broker") is _broker_server
        # Default kind unaffected by the broker setting.
        assert redis_admin_conn.get_connection(kind="default") is _default_server


def test_get_connection_broker_falls_back_to_default_when_broker_factory_unset(
    monkeypatch,
):
    """When `REDIS_ADMIN_BROKER_CONNECTION_FACTORY` is not configured we
    construct the broker client off `services.celery_broker` (or the
    default cache URL when that's also unset). Stub
    `_broker_connection` so the test doesn't try to open a real socket
    and assert it was the path taken."""

    sentinel = object()
    monkeypatch.setattr(redis_admin_conn, "_broker_connection", lambda: sentinel)
    with override_settings(REDIS_ADMIN_BROKER_CONNECTION_FACTORY=None):
        assert redis_admin_conn.get_connection(kind="broker") is sentinel


def _stub_config(monkeypatch, mapping: dict[tuple[str, ...], str | None]) -> None:
    """Make `redis_admin.conn.get_config` and `get_redis_url` deterministic.

    `mapping` is keyed by the args tuple `get_config(*args)` is called
    with so we can answer both `("services", "celery_broker")` and the
    fallback `("services", "redis_url")` from one place.
    """

    def fake_get_config(*args, **_kwargs):
        return mapping.get(args)

    monkeypatch.setattr(redis_admin_conn, "get_config", fake_get_config)
    # `_broker_connection` falls through to `get_redis_url()` when
    # `services.celery_broker` is unset; route that through the same
    # mapping so a single fixture covers both lookups.
    monkeypatch.setattr(
        redis_admin_conn,
        "get_redis_url",
        lambda: mapping.get(("services", "redis_url")) or "redis://default:6379",
    )


def test_broker_connection_falls_back_to_redis_url_when_celery_broker_unset(
    monkeypatch,
):
    """`_broker_connection()` mirrors `BaseCeleryConfig`: prefer
    `services.celery_broker`, fall back to `services.redis_url`. Single-
    Redis dev/enterprise deploys rely on this fallback so they don't
    have to set both URLs."""

    _stub_config(
        monkeypatch,
        {("services", "redis_url"): "redis://localhost:6379"},
    )

    client = redis_admin_conn._broker_connection()
    # `redis-py` exposes the resolved URL via the connection pool's
    # connection_kwargs; "host" is set from the URL.
    pool = client.connection_pool
    assert pool.connection_kwargs.get("host") == "localhost"


def test_broker_connection_prefers_celery_broker_over_redis_url(monkeypatch):
    _stub_config(
        monkeypatch,
        {
            ("services", "redis_url"): "redis://cache-host:6379",
            ("services", "celery_broker"): "redis://broker-host:6379",
        },
    )

    client = redis_admin_conn._broker_connection()
    assert client.connection_pool.connection_kwargs.get("host") == "broker-host"


def test_unknown_kind_raises():
    with pytest.raises(ValueError, match="unknown redis_admin connection kind"):
        redis_admin_conn.get_connection(kind="bogus")  # type: ignore[arg-type]


def test_factory_must_be_dotted_path():
    with override_settings(REDIS_ADMIN_CONNECTION_FACTORY="not_a_dotted_path"):
        with pytest.raises(ValueError, match="REDIS_ADMIN_CONNECTION_FACTORY"):
            redis_admin_conn.get_connection()


def test_broker_factory_must_be_dotted_path():
    with override_settings(REDIS_ADMIN_BROKER_CONNECTION_FACTORY="bare_name"):
        with pytest.raises(ValueError, match="REDIS_ADMIN_BROKER_CONNECTION_FACTORY"):
            redis_admin_conn.get_connection(kind="broker")
