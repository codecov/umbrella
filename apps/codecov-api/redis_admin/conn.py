"""Single entry point for Redis access from the redis_admin app.

We expose two named connections:

- ``"default"`` — the cache Redis at ``services.redis_url``, used by
  every family whose data lives on the main app Redis (uploads/, locks,
  intermediate-report, …). This is also what
  ``shared.helpers.redis.get_redis_connection`` returns.
- ``"broker"`` — the Celery broker at ``services.celery_broker``. In
  production this is a *different* Memorystore instance from the cache
  Redis (see ``terraform/codecov/cluster/secrets.tf`` —
  ``google_redis_instance.cache`` vs. ``google_redis_instance.celery``)
  and is exposed via a separate Secret Manager entry
  (``…-memorystore-celery-url``). The api pod already binds it to the
  ``SERVICES__CELERY_BROKER`` env var; without routing the
  ``celery_broker`` family through it the admin can't see any of the
  Celery queue keys (KEDA & Grafana monitor the broker Redis directly,
  which is why a 195k-deep ``bundles_analysis`` backlog showed up there
  but not in the admin changelist).

Dev / enterprise deploys typically run a single Redis. They leave
``services.celery_broker`` unset and the broker kind transparently falls
back to the default cache URL, mirroring what ``BaseCeleryConfig`` does.

Tests and operators can swap in custom factories per kind:

- ``REDIS_ADMIN_CONNECTION_FACTORY`` overrides ``"default"``.
- ``REDIS_ADMIN_BROKER_CONNECTION_FACTORY`` overrides ``"broker"``.

Each is a dotted path to a zero-arg callable returning a
``redis.Redis``-compatible instance.
"""

from collections.abc import Callable
from importlib import import_module
from typing import Any, Literal

from django.conf import settings
from redis import Redis

from shared.config import get_config
from shared.helpers.redis import get_redis_connection, get_redis_url

ConnectionKind = Literal["default", "broker"]

# Per-kind setting names so a single factory can't accidentally
# short-circuit both kinds at once (the broker variant ships separately
# so we can roll it out behind its own flag/factory if needed).
_FACTORY_SETTINGS: dict[str, str] = {
    "default": "REDIS_ADMIN_CONNECTION_FACTORY",
    "broker": "REDIS_ADMIN_BROKER_CONNECTION_FACTORY",
}


def _resolve_factory(dotted: str, *, setting_name: str) -> Callable[[], Any]:
    module_path, _, attr = dotted.rpartition(".")
    if not module_path:
        raise ValueError(f"{setting_name} must be a dotted path, got {dotted!r}")
    module = import_module(module_path)
    return getattr(module, attr)


def _broker_connection() -> Any:
    """Build a client for the Celery broker Redis.

    Falls back to ``services.redis_url`` when ``services.celery_broker``
    is unset so single-Redis deploys (dev, most enterprise) keep working
    without any extra config — same fallback ``BaseCeleryConfig`` uses.
    """

    url = get_config("services", "celery_broker") or get_redis_url()
    return Redis.from_url(url)


def get_connection(kind: ConnectionKind = "default") -> Any:
    """Return a Redis client for the requested connection ``kind``.

    The ``kind`` argument is keyword-defaulted so existing zero-arg
    callsites (and the simple ``lambda: server`` patches in tests) keep
    working unchanged.
    """

    setting_name = _FACTORY_SETTINGS.get(kind)
    if setting_name is None:
        raise ValueError(f"unknown redis_admin connection kind: {kind!r}")

    factory_path = getattr(settings, setting_name, None)
    if factory_path:
        factory = _resolve_factory(factory_path, setting_name=setting_name)
        return factory()

    if kind == "broker":
        return _broker_connection()
    return get_redis_connection()
