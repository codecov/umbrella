"""Django admin registration for the redis_admin app.

Milestone 1 ships the read-only changelist, M2 adds the per-queue items
view, M3 adds filtering / search / repo+commit links so an operator
investigating a backed-up queue can pivot to the relevant Repository or
Commit admin in one click.

Search syntax (M3): the changelist's search box accepts simple
prefix-style tokens in addition to plain substrings:

- `repoid:1234`         → narrows to that repo (pushed into SCAN MATCH)
- `commitid:abcdef0`    → commitid prefix
- `family:uploads`      → only that family (pushed into SCAN MATCH)
- `report_type:test_results`
- bare token            → substring match on the Redis key itself
"""

from __future__ import annotations

from urllib.parse import urlencode

from django.contrib import admin, messages
from django.http import HttpRequest
from django.urls import NoReverseMatch, reverse
from django.utils.html import format_html

from . import settings as redis_admin_settings
from .families import FAMILIES
from .models import RedisQueue, RedisQueueItem

# ---- list_filter classes ---------------------------------------------------


class FamilyFilter(admin.SimpleListFilter):
    title = "family"
    parameter_name = "family"

    def lookups(self, request, model_admin):
        return tuple((f.name, f.name) for f in FAMILIES)

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(family__exact=value)
        return queryset


class MinDepthFilter(admin.SimpleListFilter):
    title = "min depth"
    parameter_name = "min_depth"

    _PRESETS: tuple[tuple[str, str], ...] = (
        ("1", "1+"),
        ("10", "10+"),
        ("100", "100+"),
        ("1000", "1000+"),
    )

    def lookups(self, request, model_admin):
        return self._PRESETS

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(depth__gte=int(value))
        return queryset


class ReportTypeFilter(admin.SimpleListFilter):
    title = "report type"
    parameter_name = "report_type"

    _CHOICES: tuple[tuple[str, str], ...] = (
        ("coverage", "coverage"),
        ("test_results", "test_results"),
        ("bundle_analysis", "bundle_analysis"),
    )

    def lookups(self, request, model_admin):
        return self._CHOICES

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(report_type=value)
        return queryset


# ---- Helpers ---------------------------------------------------------------


def _parse_search_term(term: str) -> dict[str, str]:
    """Pull out `key:value` tokens from a search bar string.

    Recognised keys: `repoid`, `commitid`, `family`, `report_type`. Tokens
    that aren't in `key:value` form become a single `name_substring` filter.
    Multiple unrecognised tokens are joined back together so the substring
    match still works for keys that contain spaces/slashes.
    """

    bare: list[str] = []
    out: dict[str, str] = {}
    for tok in term.split():
        if ":" in tok:
            key, _, value = tok.partition(":")
            key = key.strip().lower()
            value = value.strip()
            if not value:
                continue
            if key == "repoid":
                out["repoid__exact"] = value
            elif key == "commitid":
                out["commitid__startswith"] = value
            elif key == "family":
                out["family__exact"] = value
            elif key == "report_type":
                out["report_type"] = value
            else:
                bare.append(tok)
        else:
            bare.append(tok)
    if bare:
        out["name__icontains"] = " ".join(bare)
    return out


# ---- Queue changelist ------------------------------------------------------


@admin.register(RedisQueue)
class RedisQueueAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "family",
        "redis_type",
        "depth",
        "ttl_seconds",
        "repoid_link",
        "commitid_link",
        "report_type",
        "items_link",
    )
    list_filter = (FamilyFilter, MinDepthFilter, ReportTypeFilter)
    list_per_page = 50
    show_full_result_count = False
    ordering = ("-depth", "name")
    # `search_fields` is set so the admin renders the search bar; the actual
    # query parsing happens in `get_search_results` so we can support both
    # bare substrings and `key:value` tokens.
    search_fields = ("name",)
    search_help_text = (
        "search by 'repoid:1234', 'commitid:abc', 'family:uploads', "
        "'report_type:test_results', or any substring of the Redis key"
    )

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def get_search_results(self, request, queryset, search_term):
        if not search_term:
            return queryset, False
        try:
            kwargs = _parse_search_term(search_term)
        except NotImplementedError:
            return queryset.none(), False
        if not kwargs:
            return queryset, False
        return queryset.filter(**kwargs), False

    @admin.display(description="repo")
    def repoid_link(self, obj: RedisQueue) -> str:
        if not obj.repoid:
            return "—"
        try:
            url = reverse("admin:core_repository_change", args=[obj.repoid])
        except NoReverseMatch:
            return str(obj.repoid)
        return format_html('<a href="{}">{}</a>', url, obj.repoid)

    @admin.display(description="commit")
    def commitid_link(self, obj: RedisQueue) -> str:
        if not obj.commitid:
            return "—"
        try:
            base = reverse("admin:core_commit_changelist")
        except NoReverseMatch:
            return obj.commitid[:7]
        url = f"{base}?{urlencode({'q': obj.commitid})}"
        return format_html(
            '<a href="{}" title="{}">{}</a>', url, obj.commitid, obj.commitid[:7]
        )

    @admin.display(description="items")
    def items_link(self, obj: RedisQueue) -> str:
        url = reverse("admin:redis_admin_redisqueueitem_changelist")
        query = urlencode({"queue_name__exact": obj.name})
        return format_html('<a href="{}?{}">view items</a>', url, query)


# ---- Item changelist (M2) --------------------------------------------------


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
