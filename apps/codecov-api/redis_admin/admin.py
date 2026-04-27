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
from django.contrib.admin.utils import quote, unquote
from django.http import HttpRequest
from django.urls import NoReverseMatch, reverse
from django.utils.html import format_html, format_html_join

from core.models import Repository

from . import settings as redis_admin_settings
from .families import FAMILIES
from .models import RedisLock, RedisQueue, RedisQueueItem
from .queryset import RedisItemQuerySet
from .services import redis_delete

# ---- Inline items preview (rendered on the queue change page) --------------
#
# Showing the items "below" the readonly field block on the change page
# saves a click on the overwhelmingly common "operator clicked a
# backed-up queue, wants to see what's stuck" investigation flow.
#
# We deliberately render this as Django admin's *tabular inline* DOM
# (`<div class="js-inline-admin-formset inline-group"><fieldset
# class="module">…</fieldset></div>`) rather than as a readonly field
# value, so it picks up the standard admin CSS for inlines and visually
# matches `TabularInline` blocks elsewhere in the admin. We can't use a
# real `TabularInline` because `RedisQueueItem` is unmanaged and has no
# FK back to the parent queue.
_ITEMS_PREVIEW_MAX_ROWS = 20


def _render_items_inline(obj, *, max_rows: int = _ITEMS_PREVIEW_MAX_ROWS):
    """Render the items in `obj` as Django admin tabular-inline HTML.

    Called from `RedisQueueAdmin.change_view` /
    `RedisLockAdmin.change_view` and injected into the change form via
    a custom template. Each row links to the per-item inspector for
    drill-in; a footer link points at the full items changelist so
    paging beyond the preview is one click away.
    """

    items_qs = RedisItemQuerySet(RedisQueueItem, queue_name=obj.name)
    snapshot = list(items_qs[:max_rows])

    full_url = reverse("admin:redis_admin_redisqueueitem_changelist")
    full_link = format_html(
        '<a href="{}?queue_name__exact={}">view all items \u2192</a>',
        full_url,
        obj.name,
    )

    heading = format_html("<h2>{}</h2>", "Items")

    if not snapshot:
        body = format_html(
            '<p class="paginator">{} <em>(empty / nothing to preview)</em></p>',
            full_link,
        )
        return format_html(
            '<div class="js-inline-admin-formset inline-group">'
            '<div class="tabular inline-related last-related">'
            '<fieldset class="module">{}{}</fieldset>'
            "</div></div>",
            heading,
            body,
        )

    item_change_url = "admin:redis_admin_redisqueueitem_change"
    rows = format_html_join(
        "",
        '<tr class="form-row has_original">'
        '<td class="original">'
        '<p><a href="{}" class="inlineviewlink">View</a></p>'
        "</td>"
        '<td class="field-index_or_field"><p>{}</p></td>'
        '<td class="field-raw_value"><pre style="margin:0;'
        'white-space:pre-wrap;word-break:break-all;">{}</pre></td>'
        "</tr>",
        (
            (
                reverse(item_change_url, args=[quote(item.pk_token)]),
                item.index_or_field,
                item.raw_value or "",
            )
            for item in snapshot
        ),
    )

    table = format_html(
        "<table>"
        "<thead><tr>"
        '<th class="original"></th>'
        '<th class="column-index_or_field">Index / field</th>'
        '<th class="column-raw_value">Value</th>'
        "</tr></thead>"
        "<tbody>{}</tbody>"
        "</table>",
        rows,
    )

    footer = format_html(
        '<p class="paginator">showing first {} item(s); {}</p>',
        len(snapshot),
        full_link,
    )

    return format_html(
        '<div class="js-inline-admin-formset inline-group">'
        '<div class="tabular inline-related last-related">'
        '<fieldset class="module">{}{}{}</fieldset>'
        "</div></div>",
        heading,
        table,
        footer,
    )


def _hydrate_repo_displays(rows) -> None:
    """Attach `_repo_display = "service:owner/name"` to each row in one query.

    Called from `get_changelist_instance` to avoid an N+1 lookup per row.
    """

    repoids = {row.repoid for row in rows if row.repoid}
    for row in rows:
        row._repo_display = None
    if not repoids:
        return

    mapping = {
        repo.repoid: (
            f"{repo.author.service}:{repo.author.username}/{repo.name}"
            if repo.author and repo.author.username
            else repo.name
        )
        for repo in Repository.objects.select_related("author").filter(
            repoid__in=repoids
        )
    }
    for row in rows:
        row._repo_display = mapping.get(row.repoid)


# ---- list_filter classes ---------------------------------------------------


class FamilyFilter(admin.SimpleListFilter):
    title = "family"
    parameter_name = "family"
    # `RedisQueueAdmin` and `RedisLockAdmin` override this on their
    # subclass so each admin only lists families it actually surfaces.
    category: str = "queue"

    def lookups(self, request, model_admin):
        return tuple((f.name, f.name) for f in FAMILIES if f.category == self.category)

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(family__exact=value)
        return queryset


class LockFamilyFilter(FamilyFilter):
    category = "lock"


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
        "repo_display",
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
    # The change page is a read-only inspector: every field is `readonly_fields`
    # so the form has no inputs, and `change_view` strips the Save buttons.
    # The standard "Delete" button still renders for superusers and routes
    # through our audited `redis_delete` service via `delete_model`.
    readonly_fields = (
        "name",
        "family",
        "redis_type",
        "depth",
        "ttl_seconds",
        "repoid",
        "commitid",
        "report_type",
    )

    # Override the change form so we can inject a tabular-inline-style
    # "Items" block below the readonly fields.
    change_form_template = "admin/redis_admin/items_inline_change_form.html"

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    # M5: delete is gated on `is_superuser`, *not* `is_staff`. Staff
    # users can still browse and dry-run; only superusers can commit.
    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_superuser)

    def get_fieldsets(self, request, obj=None):
        # Avoid the default ModelForm-based fieldset detection (which would
        # build a form against an unmanaged model); render every readonly
        # field in a single section instead.
        return ((None, {"fields": list(self.readonly_fields)}),)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        # Strip every save-related button so a stray POST can't reach
        # `save_model`. Delete still renders via the standard admin button.
        ctx = {
            "show_save": False,
            "show_save_and_continue": False,
            "show_save_and_add_another": False,
            "show_save_as_new": False,
        }
        if object_id is not None:
            try:
                # `object_id` arrives URL-escaped (admin uses `_2F` rather
                # than `%2F` for `/`); Django's `_changeform_view` runs the
                # same `unquote` before its own `get_object` call.
                obj = self.get_object(request, unquote(object_id))
            except self.model.DoesNotExist:
                obj = None
            if obj is not None:
                ctx["redis_admin_items_inline"] = _render_items_inline(obj)
        if extra_context:
            ctx.update(extra_context)
        return super().changeform_view(request, object_id, form_url, ctx)

    def save_model(self, request, obj, form, change):
        # Defensive: should be unreachable because the change form has no
        # editable fields, but Redis-backed rows must never round-trip
        # through Django's SQL save path.
        raise RuntimeError("RedisQueue rows are read-only.")

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

    # ---- Delete actions (M5.2) -----------------------------------------

    actions = ("clear_dry_run",)

    @admin.action(
        description="Dry-run: count what 'delete selected' would clear",
        permissions=("change",),
    )
    def clear_dry_run(self, request: HttpRequest, queryset) -> None:
        result = redis_delete(list(queryset), user=request.user, dry_run=True)
        self.message_user(
            request,
            (
                f"Dry-run: would delete {result.count} key(s) across "
                f"families={list(result.families) or '[]'}; "
                f"sample={list(result.sample[:5])}"
                + (f"; refused={list(result.refused[:5])}" if result.refused else "")
            ),
            level=messages.INFO,
        )

    def delete_queryset(self, request: HttpRequest, queryset) -> None:
        """Bulk 'Delete selected' on the queues changelist."""
        result = redis_delete(list(queryset), user=request.user, dry_run=False)
        self.message_user(
            request,
            (
                f"Cleared {result.count} Redis key(s) across "
                f"families={list(result.families) or '[]'}"
                + (f"; refused={list(result.refused[:5])}" if result.refused else "")
            ),
            level=messages.SUCCESS,
        )

    def delete_model(self, request: HttpRequest, obj: RedisQueue) -> None:
        """Single-object delete from the change page."""
        result = redis_delete([obj], user=request.user, dry_run=False)
        self.message_user(
            request,
            f"Cleared Redis key {obj.name!r} ({result.count} removed).",
            level=messages.SUCCESS,
        )

    @admin.display(description="repo")
    def repoid_link(self, obj: RedisQueue) -> str:
        if not obj.repoid:
            return "—"
        try:
            url = reverse("admin:core_repository_change", args=[obj.repoid])
        except NoReverseMatch:
            return str(obj.repoid)
        return format_html('<a href="{}">{}</a>', url, obj.repoid)

    @admin.display(description="repo (owner/name)")
    def repo_display(self, obj: RedisQueue) -> str:
        return getattr(obj, "_repo_display", None) or "—"

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

    def get_changelist_instance(self, request):
        # Hydrate `_repo_display` on the rows the template will actually
        # iterate. Doing this on the root queryset doesn't work because
        # `ChangeList` clones it (filter/order/paginate) and the clone
        # rebuilds fresh `RedisQueue` instances without our decorations.
        cl = super().get_changelist_instance(request)
        if cl.result_list:
            _hydrate_repo_displays(cl.result_list)
        return cl


# ---- Lock changelist (M4.3) ------------------------------------------------


@admin.register(RedisLock)
class RedisLockAdmin(admin.ModelAdmin):
    """Read-only admin for coordination locks, gates, and fences.

    Operators can browse to confirm a stuck task left a lock behind, but
    deletion is hard-disabled: the worker tasks that own these locks
    rely on them being released by `LockManager`, not by an operator
    clicking around in the admin. M5's delete service additionally
    refuses any key whose family has `is_deletable=False` so accidental
    URL-tampering can't bypass this.
    """

    list_display = (
        "name",
        "family",
        "redis_type",
        "ttl_seconds",
        "repoid_link",
        "repo_display",
        "commitid_link",
        "report_type",
    )
    list_filter = (LockFamilyFilter,)
    list_per_page = 50
    show_full_result_count = False
    ordering = ("family", "name")
    search_fields = ("name",)
    search_help_text = (
        "search by 'repoid:1234', 'commitid:abc', 'family:upload_finisher_gate', "
        "or any substring of the lock key"
    )
    readonly_fields = (
        "name",
        "family",
        "redis_type",
        "ttl_seconds",
        "repoid",
        "commitid",
        "report_type",
    )

    # Inject the same tabular-inline items block as `RedisQueueAdmin`,
    # so an operator can see what value the lock holds.
    change_form_template = "admin/redis_admin/items_inline_change_form.html"

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    # Locks are never deletable from the admin, regardless of role; the
    # worker's `LockManager` is the only legitimate releaser.
    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def get_actions(self, request):
        # Strip the default `delete_selected` even though
        # `has_delete_permission` already blocks it; this keeps the
        # action dropdown empty so there's nothing to click.
        actions = super().get_actions(request)
        actions.pop("delete_selected", None)
        return actions

    def get_fieldsets(self, request, obj=None):
        return ((None, {"fields": list(self.readonly_fields)}),)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        ctx = {
            "show_save": False,
            "show_save_and_continue": False,
            "show_save_and_add_another": False,
            "show_save_as_new": False,
        }
        if object_id is not None:
            try:
                obj = self.get_object(request, unquote(object_id))
            except self.model.DoesNotExist:
                obj = None
            if obj is not None:
                ctx["redis_admin_items_inline"] = _render_items_inline(obj)
        if extra_context:
            ctx.update(extra_context)
        return super().changeform_view(request, object_id, form_url, ctx)

    def save_model(self, request, obj, form, change):
        raise RuntimeError("RedisLock rows are read-only.")

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

    def get_changelist_instance(self, request):
        cl = super().get_changelist_instance(request)
        if cl.result_list:
            _hydrate_repo_displays(cl.result_list)
        return cl

    @admin.display(description="repo")
    def repoid_link(self, obj: RedisLock) -> str:
        if not obj.repoid:
            return "—"
        try:
            url = reverse("admin:core_repository_change", args=[obj.repoid])
        except NoReverseMatch:
            return str(obj.repoid)
        return format_html('<a href="{}">{}</a>', url, obj.repoid)

    @admin.display(description="repo (owner/name)")
    def repo_display(self, obj: RedisLock) -> str:
        return getattr(obj, "_repo_display", None) or "—"

    @admin.display(description="commit")
    def commitid_link(self, obj: RedisLock) -> str:
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


# ---- Item changelist (M2) --------------------------------------------------


@admin.register(RedisQueueItem)
class RedisQueueItemAdmin(admin.ModelAdmin):
    """Read-only items view (M2). M5 mutations operate at the *queue*
    level (DEL the whole key), not per-item: per-item LREM/SREM/HDEL
    requires reconstructing the original Redis value from the admin
    pk_token, which we deliberately don't expose to keep operator
    actions auditable and unambiguous. Operators who need to drop a
    bad message clear the entire queue from `RedisQueueAdmin`.
    """

    list_display = ("index_or_field", "raw_value_truncated")
    list_per_page = redis_admin_settings.ITEM_PAGE_SIZE
    show_full_result_count = False
    # Empty actions tuple = no bulk actions at all (not even
    # `delete_selected`), so the changelist's action dropdown is hidden.
    actions = ()
    # The "change" page is a strict read-only inspector for a single
    # item: every field is readonly, the form is short-circuited so we
    # never build a ModelForm against an unmanaged model, and any save
    # POST is refused.
    readonly_fields = ("pk_token", "queue_name", "index_or_field", "raw_value")

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_view_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def get_fieldsets(self, request, obj=None):
        return ((None, {"fields": list(self.readonly_fields)}),)

    def changeform_view(self, request, object_id=None, form_url="", extra_context=None):
        ctx = {
            "show_save": False,
            "show_save_and_continue": False,
            "show_save_and_add_another": False,
            "show_save_as_new": False,
        }
        if extra_context:
            ctx.update(extra_context)
        return super().changeform_view(request, object_id, form_url, ctx)

    def save_model(self, request, obj, form, change):
        raise RuntimeError("RedisQueueItem rows are read-only.")

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
