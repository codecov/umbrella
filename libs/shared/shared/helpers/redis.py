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
    """
    Return a Redis connection to the primary node.

    If Redis Sentinel is configured (via ``services.redis_sentinel_urls`` and
    ``services.redis_sentinel_service_name``), a Sentinel-managed master
    connection is returned so that writes automatically follow the current
    primary after a failover.  Otherwise falls back to the single static URL
    defined by ``services.redis_url``.

    Config keys (under ``services``):
        redis_sentinel_urls: list of "host:port" strings for the Sentinel nodes
        redis_sentinel_service_name: the Sentinel master name (e.g. "mymaster")
        redis_url: fallback single-node URL (used when Sentinel is not configured)
    """
    sentinel_urls = get_config("services", "redis_sentinel_urls")
    sentinel_service_name = get_config("services", "redis_sentinel_service_name")

    if sentinel_urls and sentinel_service_name:
        return _get_sentinel_master_connection(sentinel_urls, sentinel_service_name)

    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def _get_sentinel_master_connection(sentinel_urls: list, service_name: str) -> Redis:
    """
    Build a Sentinel client and return a master connection for *service_name*.

    ``sentinel_urls`` is expected to be a list of "host:port" strings or
    (host, port) tuples as stored in the application config.
    """
    hosts = []
    for entry in sentinel_urls:
        if isinstance(entry, (list, tuple)):
            host, port = entry[0], int(entry[1])
        else:
            host, port = entry.rsplit(":", 1)
            port = int(port)
        hosts.append((host, port))

    sentinel = Sentinel(hosts)
    return sentinel.master_for(service_name)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
