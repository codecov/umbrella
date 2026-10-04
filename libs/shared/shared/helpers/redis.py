from redis import Redis

from shared.config import get_config


def get_redis_url() -> str:
    url = get_config("services", "redis_url")
    if url is not None:
        return url
    hostname = "redis"
    port = 6379
    return f"redis://{hostname}:{port}"


def get_redis_write_url() -> str:
    """
    Returns the Redis URL to use for write operations.
    Falls back to the general redis_url if redis_url_write is not configured.
    Configure redis_url_write to point to the Redis primary when reads and
    writes are served by different endpoints (e.g. replica vs. primary).
    """
    url = get_config("services", "redis_url_write")
    if url is not None:
        return url
    return get_redis_url()


def get_redis_connection() -> Redis:
    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def get_redis_write_connection() -> Redis:
    """
    Returns a Redis connection suitable for write operations.
    Uses redis_url_write config if set, otherwise falls back to redis_url.
    """
    url = get_redis_write_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
