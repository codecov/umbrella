"""Django admin registration for the redis_admin app.

Milestone 1 ships a read-only changelist of every Redis key in the supported
families. Milestone 2 adds a per-queue items view linked from each row; you
can click "items" on a queue and land on `?queue_name__exact=<key>` filtered
to that queue's contents. Subsequent milestones add filters, search,
repo/commit grouping, and clear actions on top of these same `ModelAdmin`s.
"""

from urllib.parse import urlencode

from django.contrib import admin, messages
from django.http import HttpRequest
from django.urls import reverse
from django.utils.html import format_html

from . import settings as redis_admin_settings
from .models import RedisQueue, RedisQueueItem


@admin.register(RedisQueue)
class RedisQueueAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "family",
        "redis_type",
        "depth",
        "ttl_seconds",
        "items_link",
    )
    list_per_page = 50
    show_full_result_count = False
    ordering = ("-depth", "name")

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    @admin.display(description="items")
    def items_link(self, obj: RedisQueue) -> str:
        url = reverse("admin:redis_admin_redisqueueitem_changelist")
        query = urlencode({"queue_name__exact": obj.name})
        return format_html('<a href="{}?{}">view items</a>', url, query)


@admin.register(RedisQueueItem)
class RedisQueueItemAdmin(admin.ModelAdmin):
    list_display = ("index_or_field", "raw_value_truncated")
    list_per_page = redis_admin_settings.ITEM_PAGE_SIZE
    show_full_result_count = False

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        # No detail/edit page yet; M5 wires up per-item delete actions.
        return False

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_view_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def lookup_allowed(self, lookup, value) -> bool:
        if lookup == "queue_name__exact":
            return True
        return super().lookup_allowed(lookup, value)

    def get_queryset(self, request: HttpRequest):
        # Bypass ChangeList's automatic `.filter(**lookup_params)` plumbing so
        # the queue_name filter survives even when no list_filter is declared.
        queryset = self.model._default_manager.all()
        queue_name = request.GET.get("queue_name__exact")
        if queue_name:
            queryset = queryset.filter(queue_name__exact=queue_name)
        return queryset

    def changelist_view(self, request: HttpRequest, extra_context=None):
        if not request.GET.get("queue_name__exact"):
            messages.info(
                request,
                "Pick a queue from the Redis queues list (the 'view items' link) "
                "to see its contents.",
            )
        return super().changelist_view(request, extra_context)

    @admin.display(description="value")
    def raw_value_truncated(self, obj: RedisQueueItem) -> str:
        # Truncation already applied at queryset materialization time using
        # MAX_DECODE_BYTES, so this column just renders what's there.
        return obj.raw_value or ""
