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
    """Return a Redis connection to the writable primary node.

    When ``services.redis_sentinel_hosts`` is configured, Redis Sentinel is
    used so the connection always resolves to the current primary even after a
    failover.  Otherwise falls back to the direct ``services.redis_url``
    connection for backwards compatibility.

    Config keys
    -----------
    services.redis_sentinel_hosts : list[str]
        Sentinel nodes as ``"host:port"`` strings,
        e.g. ``["sentinel-0:26379", "sentinel-1:26379", "sentinel-2:26379"]``.
    services.redis_sentinel_service_name : str
        The Sentinel master name (default: ``"mymaster"``).
    """
    sentinel_hosts = get_config("services", "redis_sentinel_hosts")
    if sentinel_hosts:
        service_name = (
            get_config("services", "redis_sentinel_service_name") or "mymaster"
        )
        parsed = []
        for entry in sentinel_hosts:
            host, _, port = str(entry).rpartition(":")
            parsed.append((host, int(port) if port else 26379))
        sentinel = Sentinel(parsed)
        return sentinel.master_for(service_name)
    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
