from redis import Redis

from shared.config import get_config


def get_redis_url() -> str:
    url = get_config("services", "redis_url")
    if url is not None:
        return url
    hostname = "redis"
    port = 6379
    return f"redis://{hostname}:{port}"


def get_broker_redis_url() -> str:
    """Return the Redis URL for the Celery broker.

    Uses `services.celery_broker` if configured, otherwise falls back to the
    shared `services.redis_url`.  In production, `services.celery_broker`
    SHOULD point to a **dedicated** Redis instance that is sized for broker
    workload and not shared with application caching.  Sharing a single Redis
    instance for both the broker and application cache allows cache growth to
    exhaust broker memory, which triggers Redis OOM-prevention and causes
    Celery workers to crash with OutOfMemoryError on `unacked_index` writes.
    """
    url = get_config("services", "celery_broker")
    if url is not None:
        return url
    return get_redis_url()


def get_redis_connection() -> Redis:
    url = get_redis_url()
    return _get_redis_instance_from_url(url)


def get_broker_redis_connection() -> Redis:
    """Return a Redis connection for the Celery broker (see get_broker_redis_url)."""
    url = get_broker_redis_url()
    return _get_redis_instance_from_url(url)


def _get_redis_instance_from_url(url):
    return Redis.from_url(url)
