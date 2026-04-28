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
    index_in_queue = models.IntegerField(default=0)
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
