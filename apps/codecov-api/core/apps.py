import logging

from django.apps import AppConfig
from django.conf import settings

from shared.helpers.cache import RedisBackend, cache
from shared.helpers.redis import get_redis_connection

logger = logging.getLogger(__name__)


class CoreConfig(AppConfig):
    name = "core"

    def ready(self):
        import core.signals  # noqa: F401, PLC0415

        if settings.RUN_ENV not in ["DEV", "TESTING"]:
            cache_backend = RedisBackend(get_redis_connection())
            cache.configure(cache_backend)
