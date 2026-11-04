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
    """Return the URL for the writable (primary) Redis instance.

    Falls back to ``redis_url`` when ``redis_primary_url`` is not configured so
    that environments without a read-replica split continue to work unchanged.
    """
    url = get_config("services", "redis_primary_url")
    if url is not None:
        return url
    return get_redis_url()


def get_redis_connection() -> Redis:
    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def get_redis_write_connection() -> Redis:
    """Return a Redis connection guaranteed to point at the writable primary."""
    url = get_redis_write_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
