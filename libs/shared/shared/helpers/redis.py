from redis import Redis
from redis.sentinel import Sentinel

from shared.config import get_config


def get_redis_url() -> str:
    url = get_config("services", "redis_url")
    if url is not None:
        return url
    hostname = "redis"
    port = 6379
    return f"redis://{hostname}:{port}"


def get_redis_connection() -> Redis:
    """Return a Redis connection that always targets the writable primary.

    When ``services.redis_sentinel_service_name`` and
    ``services.redis_sentinel_nodes`` are configured, a Redis Sentinel client
    is used so that the connection automatically follows primary failovers.
    Falls back to a plain ``Redis.from_url`` connection otherwise.
    """
    sentinel_service = get_config("services", "redis_sentinel_service_name")
    sentinel_nodes = get_config("services", "redis_sentinel_nodes")

    if sentinel_service and sentinel_nodes:
        # sentinel_nodes is expected to be a list of [host, port] pairs, e.g.
        # [["sentinel-0", 26379], ["sentinel-1", 26379]]
        # socket_timeout ensures sentinel discovery fails fast on a network
        # partition rather than blocking indefinitely.
        sentinel = Sentinel(sentinel_nodes, socket_timeout=0.5)
        return sentinel.master_for(sentinel_service, socket_timeout=0.5)

    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
