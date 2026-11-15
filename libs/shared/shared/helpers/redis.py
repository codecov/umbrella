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
    """Return a Redis connection that always targets the primary node.

    If ``services.redis_sentinel_urls`` is configured (a list of
    ``[host, port]`` pairs) the connection is obtained via Redis Sentinel so
    that write operations always go to the current primary, even after a
    failover.  Otherwise falls back to the plain ``services.redis_url``.
    """
    sentinel_urls = get_config("services", "redis_sentinel_urls")
    if sentinel_urls:
        master_name = get_config(
            "services", "redis_sentinel_master_name", default="mymaster"
        )
        sentinel = Sentinel(sentinel_urls)
        return sentinel.master_for(master_name)

    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
