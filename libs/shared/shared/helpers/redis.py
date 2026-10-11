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

    Uses `services.redis_primary_url` if configured, otherwise falls back
    to `services.redis_url`. This distinction is important when `redis_url`
    points to a read-only replica — write operations must always use the primary.
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
    Returns a Redis connection to the primary (writable) node.

    Use this for any write operations (SET, HSET, SADD, SREM, DEL, EXPIRE, etc.)
    to avoid ReadOnlyError when `redis_url` is configured to a replica.
    """
    url = get_redis_primary_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
