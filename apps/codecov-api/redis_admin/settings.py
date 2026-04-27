"""Default settings for the redis_admin app.

Override any of these by setting `REDIS_ADMIN_<NAME>` in your Django settings,
or expose a custom Redis client via `REDIS_ADMIN_CONNECTION_FACTORY` (a dotted
path to a callable that returns a `redis.Redis`-compatible instance).
"""

from django.conf import settings

# Hard cap on the number of keys we will visit during a single SCAN sweep across
# all family patterns. Protects Redis from runaway scans if the keyspace grows
# unexpectedly large.
MAX_SCAN_KEYS: int = getattr(settings, "REDIS_ADMIN_MAX_SCAN_KEYS", 10_000)

# COUNT hint passed to each Redis SCAN call. Larger values trade per-call CPU
# for fewer round trips; 500 is a balanced default for a single-shard instance.
SCAN_COUNT: int = getattr(settings, "REDIS_ADMIN_SCAN_COUNT", 500)

# Page size for the items-in-a-queue admin view (added in milestone 2).
ITEM_PAGE_SIZE: int = getattr(settings, "REDIS_ADMIN_ITEM_PAGE_SIZE", 100)

# Maximum bytes of a single Redis value rendered in the admin item view; values
# larger than this are truncated with a "(... N bytes truncated)" suffix.
MAX_DECODE_BYTES: int = getattr(settings, "REDIS_ADMIN_MAX_DECODE_BYTES", 4_096)
