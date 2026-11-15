from redis import Redis

from shared.config import get_config


def get_redis_url() -> str:
    url = get_config("services", "redis_url")
    if url is not None:
        return url
    hostname = "redis"
    port = 6379
    return f"redis://{hostname}:{port}"


def get_redis_connection() -> Redis:
    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def get_redis_connection_for_writes() -> Redis:
    """Return a Redis connection pointed at the primary (write) node.

    Reads ``services.redis_url_primary`` from config and falls back to
    ``services.redis_url`` when the dedicated primary URL is not set.
    This ensures that write operations (e.g. distributed locks) are never
    routed to a read-only replica.
    """
    url = get_config("services", "redis_url_primary") or get_redis_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
