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

# Hard cap on the number of items materialised from a single SET/HASH key during
# admin browsing (M4). LIST keys use bounded LRANGE windows so they aren't
# constrained here. SCAN-based readers stop streaming once they reach this cap
# so a runaway 10M-element SET can't OOM the api process.
MAX_ITEMS_PER_KEY: int = getattr(settings, "REDIS_ADMIN_MAX_ITEMS_PER_KEY", 20_000)

# Per-message rows materialised on the celery_broker changelist drill-down.
# Tighter than `MAX_ITEMS_PER_KEY` because each row holds a parsed envelope
# in memory (kept in the per-request LRANGE cache) and is rendered as a
# table cell — the operator only needs a representative slice, not the
# entire 100k-deep queue.
CELERY_BROKER_DISPLAY_LIMIT: int = getattr(
    settings, "REDIS_ADMIN_CELERY_BROKER_DISPLAY_LIMIT", 2_000
)

# Sample window used by deep scans of a celery_broker queue: the streaming
# frequency chart aggregator (`_stream_frequency_aggregate`) and the
# streaming clear (`services._streaming_celery_clear`). Both walk the queue
# in bounded chunks and discard payloads, so memory stays flat regardless
# of this cap; raising it just trades latency for accuracy. Keeping the
# chart and clear on the same window guarantees that anything the chart
# surfaces is reachable by the "Clear queue" button.
CELERY_BROKER_SCAN_LIMIT: int = getattr(
    settings, "REDIS_ADMIN_CELERY_BROKER_SCAN_LIMIT", 100_000
)

# Pipeline batch size for delete operations (M5). Keeps a single delete action
# from blocking Redis with a single oversized MULTI when an operator clears
# thousands of keys at once.
DELETE_BATCH_SIZE: int = getattr(settings, "REDIS_ADMIN_DELETE_BATCH_SIZE", 500)
