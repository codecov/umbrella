"""Django admin registration for the redis_admin app.

Milestone 1 ships a read-only changelist of every Redis key in the supported
families. Subsequent milestones add filters, search, item drill-in, and
clear actions on top of this same `ModelAdmin`.
"""

from django.contrib import admin
from django.http import HttpRequest

from .models import RedisQueue


@admin.register(RedisQueue)
class RedisQueueAdmin(admin.ModelAdmin):
    list_display = ("name", "family", "redis_type", "depth", "ttl_seconds")
    list_per_page = 50
    # SCAN-based count is approximate and we do not want the admin to issue a
    # second pass just to render "X of Y" in the toolbar.
    show_full_result_count = False
    ordering = ("-depth", "name")

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False
