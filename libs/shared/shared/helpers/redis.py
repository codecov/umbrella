from redis import Redis

from shared.config import get_config


def get_redis_url() -> str:
    url = get_config("services", "redis_url")
    if url is not None:
        return url
    hostname = "redis"
    port = 6379
    return f"redis://{hostname}:{port}"


def get_redis_primary_url() -> str:
    """
    Returns the URL for the Redis primary (writable) node.

    Uses ``services.redis_primary_url`` when configured, and falls back to
    ``services.redis_url`` so that single-Redis deployments continue to work
    without any configuration change.
    """
    url = get_config("services", "redis_primary_url")
    if url is not None:
        return url
    return get_redis_url()


def get_redis_connection() -> Redis:
    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def get_redis_primary_connection() -> Redis:
    """
    Returns a Redis client connected to the primary (writable) node.

    Use this wherever write commands (SET, SADD, SREM, etc.) are issued.
    For read-only access ``get_redis_connection`` is sufficient.
    """
    url = get_redis_primary_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
