"""Unmanaged Django models that the admin renders from Redis.

These have `Meta.managed = False` so Django never tries to create or migrate
a database table for them. Field declarations exist purely to give the admin
something to introspect for `list_display`, ordering, and search machinery.
Instances are constructed by `redis_admin.queryset.RedisQueueQuerySet` from
Redis SCAN results and are never saved.
"""

from django.db import models

from .queryset import RedisItemQuerySet, RedisQueueQuerySet


class RedisQueueManager(models.Manager):
    def get_queryset(self) -> RedisQueueQuerySet:  # type: ignore[override]
        return RedisQueueQuerySet(self.model)


class RedisQueueItemManager(models.Manager):
    def get_queryset(self) -> RedisItemQuerySet:  # type: ignore[override]
        return RedisItemQuerySet(self.model)


class RedisQueue(models.Model):
    name = models.CharField(primary_key=True, max_length=512)
    family = models.CharField(max_length=64)
    redis_type = models.CharField(max_length=16)
    depth = models.PositiveIntegerField(default=0)
    ttl_seconds = models.IntegerField(null=True, blank=True)

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
