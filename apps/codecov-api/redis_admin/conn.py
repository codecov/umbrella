"""Single entry point for Redis access from the redis_admin app.

By default we hand out the same connection used everywhere else in the
codebase via `shared.helpers.redis.get_redis_connection`. Tests and operators
can swap in a custom factory by setting `REDIS_ADMIN_CONNECTION_FACTORY` to a
dotted path of a zero-arg callable returning a `redis.Redis`-compatible
instance.
"""

from collections.abc import Callable
from importlib import import_module
from typing import Any

from django.conf import settings

from shared.helpers.redis import get_redis_connection


def _resolve_factory(dotted: str) -> Callable[[], Any]:
    module_path, _, attr = dotted.rpartition(".")
    if not module_path:
        raise ValueError(
            f"REDIS_ADMIN_CONNECTION_FACTORY must be a dotted path, got {dotted!r}"
        )
    module = import_module(module_path)
    return getattr(module, attr)


def get_connection() -> Any:
    factory_path = getattr(settings, "REDIS_ADMIN_CONNECTION_FACTORY", None)
    if factory_path:
        factory = _resolve_factory(factory_path)
        return factory()
    return get_redis_connection()
