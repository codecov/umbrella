"""Unmanaged Django models that the admin renders from Redis.

These have `Meta.managed = False` so Django never tries to create or migrate
a database table for them. Field declarations exist purely to give the admin
something to introspect for `list_display`, ordering, and search machinery.
Instances are constructed by `redis_admin.queryset.RedisQueueQuerySet` from
Redis SCAN results and are never saved.
"""

from django.db import models

from .queryset import (
    CeleryBrokerQueueQuerySet,
    RedisItemQuerySet,
    RedisQueueQuerySet,
    UnackedQueueQuerySet,
)


class RedisQueueManager(models.Manager):
    def get_queryset(self) -> RedisQueueQuerySet:  # type: ignore[override]
        return RedisQueueQuerySet(self.model, category="queue")


class RedisLockManager(models.Manager):
    def get_queryset(self) -> RedisQueueQuerySet:  # type: ignore[override]
        return RedisQueueQuerySet(self.model, category="lock")


class RedisQueueItemManager(models.Manager):
    def get_queryset(self) -> RedisItemQuerySet:  # type: ignore[override]
        return RedisItemQuerySet(self.model)


class RedisQueue(models.Model):
    name = models.CharField(primary_key=True, max_length=512)
    family = models.CharField(max_length=64)
    redis_type = models.CharField(max_length=16)
    depth = models.PositiveIntegerField(default=0)
    ttl_seconds = models.IntegerField(null=True, blank=True)
    repoid = models.IntegerField(null=True, blank=True)
    commitid = models.CharField(max_length=64, null=True, blank=True)
    report_type = models.CharField(max_length=32, null=True, blank=True)

    objects = RedisQueueManager()

    class Meta:
        managed = False
        app_label = "redis_admin"
        verbose_name = "Redis queue"
        verbose_name_plural = "Redis queues"
        # Required by Django even for unmanaged models so makemigrations stays
        # quiet; these have no real DB table.
        default_permissions = ("view",)

    def __str__(self) -> str:
        return self.name

    def save(self, *args, **kwargs):  # pragma: no cover - safety net
        raise RuntimeError("RedisQueue is read-only; saves are not supported.")

    def delete(self, *args, **kwargs):  # pragma: no cover - milestone 5
        raise NotImplementedError("RedisQueue.delete arrives in milestone 5.")


class RedisLock(models.Model):
    """Read-only view of Redis locks/fences/gates.

    Same field shape as `RedisQueue` but lives in its own admin so an
    operator can't accidentally clear them from the queues page; the
    queueset is wired with `category="lock"` so `iter_keys` only yields
    families flagged with `is_deletable=False`. M5's delete service
    refuses every key whose family has `is_deletable=False`, regardless
    of which model the row is rendered through.
    """

    name = models.CharField(primary_key=True, max_length=512)
    family = models.CharField(max_length=64)
    redis_type = models.CharField(max_length=16)
    depth = models.PositiveIntegerField(default=0)
    ttl_seconds = models.IntegerField(null=True, blank=True)
    repoid = models.IntegerField(null=True, blank=True)
    commitid = models.CharField(max_length=64, null=True, blank=True)
    report_type = models.CharField(max_length=32, null=True, blank=True)

    objects = RedisLockManager()

    class Meta:
        managed = False
        app_label = "redis_admin"
        verbose_name = "Redis lock"
        verbose_name_plural = "Redis locks"
        default_permissions = ("view",)

    def __str__(self) -> str:
        return self.name

    def save(self, *args, **kwargs):  # pragma: no cover - safety net
        raise RuntimeError("RedisLock is read-only; saves are not supported.")

    def delete(self, *args, **kwargs):  # pragma: no cover - intentional
        raise RuntimeError(
            "RedisLock entries are immutable from the admin; coordination "
            "locks must be released by their owning task, not by an operator."
        )


class CeleryBrokerQueueManager(models.Manager):
    def get_queryset(self) -> CeleryBrokerQueueQuerySet:  # type: ignore[override]
        return CeleryBrokerQueueQuerySet(self.model)


class CeleryBrokerQueue(models.Model):
    """A single celery message inside a `celery_broker` Redis list.

    Mirrors `RedisQueue`'s "fake managed=False model that the admin
    renders directly off Redis" pattern, but specialised for the
    celery broker: each row is one task in flight, with the kombu
    envelope already parsed into structured columns
    (`task_name`, `repoid`, `commitid`, etc.) so an on-call operator
    can filter and selectively clear stuck messages without DEL-ing
    the whole queue.

    The model lives in its own admin (`CeleryBrokerQueueAdmin`)
    rather than re-using `RedisQueueItem`: the celery body is a
    base64-wrapped kombu envelope that's noisy in the generic items
    view, and per-item delete needs the LSET-tombstone path
    (`services.celery_broker_clear`) which is unsafe for other
    families.

    `pk_token` shape: `<queue_name>#<index>` — same convention as
    `RedisQueueItem` so the admin's `change_view` URL construction
    works without customisation. `default_permissions` includes
    `delete` because superusers can clear individual messages from
    the changelist; a non-superuser sees the read-only inspector
    only.
    """

    pk_token = models.CharField(primary_key=True, max_length=600)
    queue_name = models.CharField(max_length=512)
    # Two row shapes share this model:
    #
    # * **Queue summary** (no `queue_name__exact` filter) — one row per
    #   known celery queue, with `depth = LLEN(queue)` set and every
    #   message-specific field `None`. `pk_token = "<queue>#summary"`.
    # * **Message** (with `queue_name__exact=<queue>`) — one row per
    #   kombu envelope inside that queue, with `index_in_queue` /
    #   `task_name` / `repoid` / `commitid` / etc. set and `depth =
    #   None`. `pk_token = "<queue>#<index>"`.
    #
    # The admin switches `list_display` between the two via
    # `get_list_display(request)` so each mode shows the columns that
    # are actually populated.
    index_in_queue = models.IntegerField(null=True, blank=True)
    depth = models.IntegerField(null=True, blank=True)
    task_name = models.CharField(max_length=256, null=True, blank=True)
    task_id = models.CharField(max_length=128, null=True, blank=True)
    repoid = models.IntegerField(null=True, blank=True)
    commitid = models.CharField(max_length=64, null=True, blank=True)
    ownerid = models.IntegerField(null=True, blank=True)
    pullid = models.IntegerField(null=True, blank=True)
    payload_preview = models.TextField(blank=True)

    objects = CeleryBrokerQueueManager()

    class Meta:
        managed = False
        app_label = "redis_admin"
        verbose_name = "Celery broker queue message"
        verbose_name_plural = "Celery broker queue messages"
        default_permissions = ("view", "delete")

    def __str__(self) -> str:
        if self.task_name:
            return f"{self.queue_name}[{self.index_in_queue}] {self.task_name}"
        return f"{self.queue_name}[{self.index_in_queue}]"

    def save(self, *args, **kwargs):  # pragma: no cover - safety net
        raise RuntimeError("CeleryBrokerQueue is read-only; saves are not supported.")

    def delete(self, *args, **kwargs):  # pragma: no cover - routed via service
        # Real deletion goes through `services.celery_broker_clear` so
        # the LSET-tombstone path runs and the audit log captures it;
        # bypassing that here would leave stale list entries.
        raise NotImplementedError(
            "CeleryBrokerQueue.delete is not supported on individual "
            "instances; use redis_admin.services.celery_broker_clear "
            "(which the admin's clear flow already invokes)."
        )


class UnackedQueueItemManager(models.Manager):
    def get_queryset(self) -> UnackedQueueQuerySet:  # type: ignore[override]
        return UnackedQueueQuerySet(self.model)


class UnackedQueueItem(models.Model):
    """A single Kombu unacked-message envelope.

    Surfaces Kombu's Redis transport unacked bookkeeping as one row
    per `delivery_tag`. The Grafana panel
    `redis_key_size{key="unacked"}` corresponds to this admin's
    `HLEN unacked` snapshot — the same hash whose drill-down this
    admin renders.

    Storage layout (verified against
    `kombu.transport.redis.Channel.unacked_key` /
    `unacked_index_key` in this venv):

    * `unacked` — Redis HASH. Field = `delivery_tag` (UUID-ish
      string). Value = JSON-serialised
      `[message_dict, exchange_str, routing_key_str]` triple.
      `message_dict["body"]` is base64-encoded JSON of the celery
      envelope, the same shape `parse_celery_envelope` understands.
    * `unacked_index` — Redis ZSET. Score = visibility-timeout
      deadline (unix ts). Member = `delivery_tag`. Used by Kombu's
      restore-loop to re-enqueue messages whose worker died.

    To clear an unacked entry, both `HDEL unacked <delivery_tag>`
    AND `ZREM unacked_index <delivery_tag>` are required. Skipping
    the ZREM leaves a phantom in `unacked_index` that points at a
    non-existent HASH field, which Kombu will then try to restore
    from the (now missing) hash entry.

    Mirrors `CeleryBrokerQueue`'s "two-mode" pattern: when no
    `routing_key__exact` filter is set, the queryset returns one
    summary row carrying `depth = HLEN(unacked)`; with the filter
    set, it returns per-message rows scoped to that routing_key
    (the original queue these messages were delivered from).

    `pk_token` shape: `unacked#<delivery_tag>` for a message row and
    `unacked#summary` for the singleton summary row, mirroring the
    `CeleryBrokerQueue` convention so admin URL plumbing
    (`change_view`, `pk__in=[...]` bulk actions) works without
    customisation. `default_permissions` includes `delete` because
    superusers can clear individual messages from the changelist.
    """

    pk_token = models.CharField(primary_key=True, max_length=600)
    delivery_tag = models.CharField(max_length=128, blank=True)
    routing_key = models.CharField(max_length=256, blank=True)
    exchange = models.CharField(max_length=128, blank=True)
    visibility_deadline = models.DateTimeField(null=True, blank=True)
    task_name = models.CharField(max_length=256, blank=True)
    task_id = models.CharField(max_length=128, blank=True)
    repoid = models.IntegerField(null=True, blank=True)
    commitid = models.CharField(max_length=64, blank=True)
    ownerid = models.IntegerField(null=True, blank=True)
    pullid = models.IntegerField(null=True, blank=True)
    payload_preview = models.TextField(blank=True)
    # Set ONLY on the summary row (no `routing_key__exact` filter):
    # `HLEN unacked` at the time of materialisation. None on
    # per-message rows.
    depth = models.IntegerField(null=True, blank=True)

    objects = UnackedQueueItemManager()

    class Meta:
        managed = False
        app_label = "redis_admin"
        verbose_name = "Unacked queue item"
        verbose_name_plural = "Unacked queue"
        default_permissions = ("view", "delete")

    def __str__(self) -> str:
        if self.depth is not None:
            return f"unacked (depth={self.depth})"
        if self.task_name:
            return f"unacked[{self.delivery_tag}] {self.task_name}"
        return f"unacked[{self.delivery_tag}]"

    def save(self, *args, **kwargs):  # pragma: no cover - safety net
        raise RuntimeError("UnackedQueueItem is read-only; saves are not supported.")

    def delete(self, *args, **kwargs):  # pragma: no cover - routed via service
        # Real deletion goes through `services.unacked_clear` so the
        # paired `HDEL unacked <tag>` + `ZREM unacked_index <tag>`
        # contract is honoured atomically; a bare HDEL leaves a
        # phantom in `unacked_index` for Kombu's restore loop to
        # trip over.
        raise NotImplementedError(
            "UnackedQueueItem.delete is not supported on individual "
            "instances; use redis_admin.services.unacked_clear "
            "(which the admin's clear flow already invokes)."
        )


class RedisQueueItem(models.Model):
    """A single item inside a Redis-backed container.

    `pk_token` is `<queue_name>#<index_or_field>`. For LIST items it's the
    LRANGE index; later milestones extend it to the field name (HASH) or the
    SHA1 of the value (SET) so each row in the admin still has a stable pk.
    """

    pk_token = models.CharField(primary_key=True, max_length=600)
    queue_name = models.CharField(max_length=512)
    index_or_field = models.CharField(max_length=128)
    raw_value = models.TextField(blank=True)

    objects = RedisQueueItemManager()

    class Meta:
        managed = False
        app_label = "redis_admin"
        verbose_name = "Redis queue item"
        verbose_name_plural = "Redis queue items"
        default_permissions = ("view",)

    def __str__(self) -> str:
        return f"{self.queue_name}[{self.index_or_field}]"

    def save(self, *args, **kwargs):  # pragma: no cover - safety net
        raise RuntimeError("RedisQueueItem is read-only; saves are not supported.")

    def delete(self, *args, **kwargs):  # pragma: no cover - milestone 5
        raise NotImplementedError("RedisQueueItem.delete arrives in milestone 5.")
