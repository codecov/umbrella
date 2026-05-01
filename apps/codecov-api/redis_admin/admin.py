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

import logging
from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import urlencode

from django.contrib import admin, messages
from django.contrib.admin.utils import quote, unquote
from django.core.exceptions import PermissionDenied
from django.http import (
    HttpRequest,
    HttpResponse,
    HttpResponseNotAllowed,
    HttpResponseRedirect,
    JsonResponse,
)
from django.shortcuts import render
from django.urls import NoReverseMatch, path, reverse
from django.utils.html import format_html, format_html_join

from core.models import Repository

from . import conn as _conn
from . import settings as redis_admin_settings
from .families import FAMILIES, iter_families, iter_keys
from .families import (
    _resolve_celery_queue_names as _celery_queue_names,  # noqa: PLC2701 - reused for filter lookups
)
from .models import CeleryBrokerQueue, RedisLock, RedisQueue, RedisQueueItem
from .queryset import (
    CeleryBrokerQueueQuerySet,
    RedisItemQuerySet,
    _build_redis_queue,
    _stream_frequency_aggregate,
    resolve_payload_preview,
)
from .services import (
    _CELERY_CLEAR_JOB_TERMINAL_STATES,
    _substitute_filter_any,
    celery_broker_clear,
    get_celery_broker_clear_job,
    redis_delete,
    request_cancel_celery_broker_clear_job,
    start_celery_broker_clear_job,
    streaming_celery_count,
)

log = logging.getLogger(__name__)

# Cap on the rows we render in the clear-by-filter preview
# table. Decoupled from `CELERY_BROKER_DISPLAY_LIMIT` because
# the preview page only needs a representative slice; the real
# `match_count` comes from the streaming counter.
_CLEAR_BY_FILTER_SAMPLE_SIZE: int = 20

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

    For `celery_broker` queues we route into `CeleryBrokerQueueAdmin`
    instead — its inline preview is structured (task / repoid /
    commit columns rather than raw kombu envelopes), and the bulk
    "view all items →" link goes to the celery-aware changelist.
    """

    if getattr(obj, "family", None) == "celery_broker":
        return _render_celery_items_inline(obj, max_rows=max_rows)

    items_qs = RedisItemQuerySet(RedisQueueItem, queue_name=obj.name)
    snapshot = list(items_qs[:max_rows])

    full_url = reverse("admin:redis_admin_redisqueueitem_changelist")
    # URL-encode the queue name so keys containing `&`, `#`, `?`, or other
    # query-meaningful characters round-trip cleanly into the items
    # changelist filter (`format_html` only HTML-escapes; it doesn't
    # percent-encode). Mirrors the `items_link` helper on
    # `RedisQueueAdmin`, which already uses `urlencode`.
    full_link_query = urlencode({"queue_name__exact": obj.name})
    full_link = format_html(
        '<a href="{}?{}">view all items \u2192</a>',
        full_url,
        full_link_query,
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


def _render_celery_items_inline(obj, *, max_rows: int = _ITEMS_PREVIEW_MAX_ROWS):
    """Tabular-inline preview specialised for `celery_broker` queues.

    Mirrors `_render_items_inline` for non-celery families but pulls
    rows from `CeleryBrokerQueueQuerySet` so the inline columns are
    `idx | task | repoid | commit` instead of the raw kombu envelope
    that `RedisQueueItem` would surface. The "view all items →"
    footer points at the celery-aware changelist
    (`CeleryBrokerQueueAdmin`) rather than the generic items view.
    """

    items_qs = CeleryBrokerQueueQuerySet(CeleryBrokerQueue, queue_name=obj.name)
    snapshot = list(items_qs[:max_rows])

    full_url = reverse("admin:redis_admin_celerybrokerqueue_changelist")
    full_link_query = urlencode({"queue_name__exact": obj.name})
    full_link = format_html(
        '<a href="{}?{}">view all items \u2192</a>',
        full_url,
        full_link_query,
    )

    heading = format_html("<h2>{}</h2>", "Celery messages")

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

    item_change_url = "admin:redis_admin_celerybrokerqueue_change"
    rows = format_html_join(
        "",
        '<tr class="form-row has_original">'
        '<td class="original">'
        '<p><a href="{}" class="inlineviewlink">View</a></p>'
        "</td>"
        '<td class="field-index_in_queue"><p>{}</p></td>'
        '<td class="field-task_name"><p>{}</p></td>'
        '<td class="field-repoid"><p>{}</p></td>'
        '<td class="field-commitid"><p>{}</p></td>'
        "</tr>",
        (
            (
                reverse(item_change_url, args=[quote(item.pk_token)]),
                item.index_in_queue,
                item.task_name or "—",
                item.repoid if item.repoid is not None else "—",
                (item.commitid[:7] if item.commitid else "—"),
            )
            for item in snapshot
        ),
    )

    table = format_html(
        "<table>"
        "<thead><tr>"
        '<th class="original"></th>'
        '<th class="column-index_in_queue">Idx</th>'
        '<th class="column-task_name">Task</th>'
        '<th class="column-repoid">Repo</th>'
        '<th class="column-commitid">Commit</th>'
        "</tr></thead>"
        "<tbody>{}</tbody>"
        "</table>",
        rows,
    )

    footer = format_html(
        '<p class="paginator">showing first {} message(s); {}</p>',
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


def _resolve_repo_displays(repoids: Iterable[int]) -> dict[int, str]:
    """Build `{repoid: "service:owner/name"}` for the given ids in one query.

    Single source of truth for the "service:owner/name" string the
    queue + lock changelists already render in the `repo_display`
    column; the celery_broker frequency chart reuses the same
    mapping so its repo column matches what operators see elsewhere
    (rather than rendering a bare numeric repoid).

    Falls back to just the repo `name` when the author / username
    is missing — same behaviour `_hydrate_repo_displays` had inline
    before this was extracted.
    """

    ids = {rid for rid in repoids if rid}
    if not ids:
        return {}
    return {
        repo.repoid: (
            f"{repo.author.service}:{repo.author.username}/{repo.name}"
            if repo.author and repo.author.username
            else repo.name
        )
        for repo in Repository.objects.select_related("author").filter(repoid__in=ids)
    }


def _hydrate_repo_displays(rows) -> None:
    """Attach `_repo_display = "service:owner/name"` to each row in one query.

    Called from `get_changelist_instance` to avoid an N+1 lookup per row.
    """

    mapping = _resolve_repo_displays(row.repoid for row in rows)
    for row in rows:
        row._repo_display = mapping.get(row.repoid) if row.repoid else None


# ---- list_filter classes ---------------------------------------------------


class FamilyFilter(admin.SimpleListFilter):
    title = "family"
    parameter_name = "family"
    # `RedisQueueAdmin` and `RedisLockAdmin` override this on their
    # subclass so each admin only lists families it actually surfaces.
    category: str = "queue"
    # Families that are categorically excluded from the dropdown even
    # though they match `category`. Subclasses use this to hide a
    # family that has its own dedicated admin (e.g. `celery_broker`
    # is surfaced by `CeleryBrokerQueueAdmin`; leaving it selectable
    # on the generic queue filter would be a dead option that
    # returns zero rows because `RedisQueueAdmin.get_queryset` runs
    # `.family_exclude("celery_broker")`).
    excluded_names: frozenset[str] = frozenset()

    def lookups(self, request, model_admin):
        return tuple(
            (f.name, f.name)
            for f in iter_families(category=self.category, exclude=self.excluded_names)
        )

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(family__exact=value)
        return queryset


class QueueFamilyFilter(FamilyFilter):
    """`FamilyFilter` for `RedisQueueAdmin` — omits `celery_broker`.

    The queue changelist hides `celery_broker` rows entirely
    (`get_queryset` calls `.family_exclude("celery_broker")`) so
    leaving the value in the dropdown surfaces a clickable option
    that filters the page to zero rows. Dropping it from the
    choice list keeps the sidebar in sync with what the admin can
    actually display.
    """

    excluded_names = frozenset({"celery_broker"})


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


class CeleryQueueFilter(admin.SimpleListFilter):
    """Narrow the changelist to a single Celery broker queue.

    The well-known Celery queues (`celery`, `healthcheck`, plus anything
    routed via `BaseCeleryConfig.task_routes[...]['queue']`) show up
    one-per-row in the Codecov health-overview Grafana dashboard, so an
    on-call engineer who sees `notify_celery` spike in Grafana would
    otherwise have to either remember the exact key name or hand-type
    `family:celery_broker name:notify_celery` into the search bar.

    This filter surfaces the same enumeration `families.celery_broker`
    uses for its `fixed_keys`, so the picker is always in sync with the
    queues the admin can actually inspect — adding a new queue to
    `task_routes` automatically adds it to the filter on next request.

    Selecting a queue narrows the changelist to a single row
    (`family__exact=celery_broker AND name__exact=<queue>`); the
    well-known queues are unique by name so this collapses cleanly to
    "the row for that queue".

    Dynamic `enterprise_*` queues aren't enumerable from configuration
    and are intentionally excluded from the dropdown — operators who
    need them can keep using the search bar (`family:celery_broker
    enterprise_acme`) which already handles substring match.
    """

    title = "celery queue"
    parameter_name = "celery_queue"

    def lookups(self, request, model_admin):
        # Re-resolved per request so a config change picks up without
        # requiring a worker restart; `_celery_queue_names` itself
        # falls back to `("celery", "healthcheck")` if the celery
        # config import fails, so the dropdown is never empty.
        return tuple((name, name) for name in _celery_queue_names())

    def queryset(self, request, queryset):
        value = self.value()
        if not value:
            return queryset
        # `name__exact` lets `RedisQueueQuerySet._fetch_all` take the
        # EXISTS-only shortcut (single GET / TYPE round-trip; no SCAN
        # sweep), the same path admin bulk actions use for `pk__in=`.
        # The `family__exact` guard is enforced post-resolve in
        # `_post_scan_predicate`: if `find_family(<queue>)` ever
        # routed the name to a non-celery family (theoretically
        # possible if some other family adopted a colliding fixed_key),
        # the row would be filtered out rather than silently surfacing
        # under the wrong family.
        return queryset.filter(family__exact="celery_broker", name__exact=value)


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
    # `celery_broker` is intentionally absent from the queue
    # changelist now: those queues are surfaced one-per-row (with a
    # frequency chart and per-`(repoid, commitid)` clear flow) by
    # `CeleryBrokerQueueAdmin`. `get_queryset` hides them with
    # `.family_exclude("celery_broker")`. The `CeleryQueueFilter`
    # sidebar — which existed only to let operators jump from the
    # queue list to a specific celery queue — is removed for the
    # same reason; the new admin's landing page is the discovery
    # surface for celery queues.
    list_filter = (QueueFamilyFilter, MinDepthFilter, ReportTypeFilter)
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
    # Override the changelist toolbar to surface the M6 "Clear by
    # scope…" link next to the search box.
    change_list_template = "admin/redis_admin/redisqueue/change_list.html"

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    # M5: delete is gated on `is_superuser`, *not* `is_staff`. Staff
    # users can still browse and dry-run; only superusers can commit.
    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_superuser)

    def get_queryset(self, request: HttpRequest):
        # Hide `celery_broker` queues: they have their own admin
        # (`CeleryBrokerQueueAdmin`) which understands the
        # one-row-per-message shape and surfaces a `(repoid,
        # commitid)` frequency chart on the drill-down page. Showing
        # them here would offer a redundant, less-informative view
        # (one row + DEL of the entire queue) and defeat the
        # discovery flow that points operators at the per-message
        # admin from Grafana.
        return super().get_queryset(request).family_exclude("celery_broker")

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

    # ---- Cross-family clear-by-scope (M6) -------------------------------
    #
    # `/admin/redis_admin/redisqueue/clear-by-scope/?repoid=…&commitid=…
    # &family=uploads&family=latest_upload`
    # aggregates every deletable Redis key tied to a given repo, commit,
    # and/or specific family list and clears them in a single audited
    # operation. This is the on-call escape hatch for "rerun completely
    # failed" — without it, an operator would have to navigate to every
    # family's filter and run `delete_selected` for each.
    #
    # Scope is the cross-product of three optional dimensions:
    #   - `repoid` (numeric)
    #   - `commitid` (full SHA or any prefix)
    #   - `family` (zero or more deletable family names; empty = all)
    # At least one of repoid / commitid / explicit family list must be
    # set. The empty form is refused.
    #
    # Safety stack:
    #   1. Superuser-only (matches `delete_selected` gating).
    #   2. GET = preview / form. POST = mutation. Empty scope refused.
    #   3. Typed-confirmation guard: the operator must re-type the
    #      "primary" scope value (repoid > commitid > joined family
    #      names, in that preference order) into a confirmation field
    #      to enable the destructive button. A simple "I really meant
    #      it" gate that costs an extra second but blocks fat-fingered
    #      double-clicks.
    #   4. `dry_run` button always available alongside `confirm`.
    #   5. The actual mutation funnels through the same `redis_delete`
    #      service used by `delete_selected`, so locks (`is_deletable
    #      =False`) are still refused even if a future family change
    #      starts surfacing them as queues.

    def get_urls(self):
        urls = super().get_urls()
        opts = self.model._meta
        clear_url = path(
            "clear-by-scope/",
            self.admin_site.admin_view(self.clear_by_scope_view),
            name=f"{opts.app_label}_{opts.model_name}_clear_by_scope",
        )
        # Insert before the catch-all `<path:object_id>/` patterns so
        # `clear-by-scope/` doesn't get swallowed as an object_id.
        return [clear_url, *urls]

    @staticmethod
    def _deletable_family_names() -> list[str]:
        """Names of every queue-category family that's safe to delete.

        Used by the form to render the family checkbox list and by the
        view to validate operator-supplied family names.
        """

        return sorted(
            f.name for f in FAMILIES if f.category == "queue" and f.is_deletable
        )

    def _resolve_scope_targets(
        self,
        *,
        repoid: int | None,
        commitid: str,
        families: Sequence[str] | None,
    ) -> list[RedisQueue]:
        """Materialise every deletable queue matching the active scope.

        Iterates per-requested-family when `families` is non-empty so
        the SCAN MATCH pattern is family-specific (e.g. `uploads/42/*`
        instead of a wildcard sweep). When `families` is None or
        empty, sweeps all deletable families. Skips lock families
        (`is_deletable=False`) regardless of how a key was surfaced.
        """

        family_iter: Iterable[str | None]
        if families:
            family_iter = list(families)
        else:
            family_iter = [None]

        # See `RedisQueueQuerySet._fetch_all` — same per-family
        # connection-kind cache so a clear-by-scope that spans the cache
        # and broker Redis instances opens at most one client per kind.
        clients: dict[str, Any] = {}

        def _client_for(family) -> Any:
            kind = family.connection_kind
            if kind not in clients:
                clients[kind] = _conn.get_connection(kind=kind)
            return clients[kind]

        targets: list[RedisQueue] = []
        seen: set[str] = set()
        for family_name in family_iter:
            for key, family in iter_keys(
                family=family_name,
                repoid=repoid,
                commitid_prefix=commitid or None,
                category="queue",
            ):
                if not family.is_deletable:
                    continue
                if key in seen:
                    continue
                obj = _build_redis_queue(self.model, key, family, _client_for(family))
                # `iter_keys` already pushed the filter into SCAN MATCH,
                # but glob `*` matches `/` so re-verify the parsed
                # values.
                if repoid is not None and obj.repoid != repoid:
                    continue
                if commitid and not (obj.commitid or "").startswith(commitid):
                    continue
                seen.add(key)
                targets.append(obj)
        return targets

    def clear_by_scope_view(self, request: HttpRequest) -> HttpResponse:
        if not request.user.is_superuser:
            raise PermissionDenied(
                "redis_admin clear-by-scope is restricted to superusers"
            )

        params = request.POST if request.method == "POST" else request.GET
        repoid_raw = (params.get("repoid") or "").strip()
        commitid = (params.get("commitid") or "").strip()
        # Multi-select: each checked family round-trips as one `family`
        # form value. `getlist` falls back to a query-string-friendly
        # comma-separated `family=a,b,c` as well so the URL can be
        # bookmarked.
        family_values = list(params.getlist("family"))
        if not family_values:
            csv = (params.get("family") or "").strip()
            if csv:
                family_values = [csv]
        families: list[str] = []
        for fv in family_values:
            for piece in fv.split(","):
                piece = piece.strip()
                if piece and piece not in families:
                    families.append(piece)

        deletable_families = self._deletable_family_names()
        invalid_families = [f for f in families if f not in deletable_families]
        family_error: str | None = None
        if invalid_families:
            family_error = "unknown or non-deletable family: " + ", ".join(
                invalid_families
            )

        repoid: int | None = None
        repoid_error: str | None = None
        if repoid_raw:
            try:
                repoid = int(repoid_raw)
            except ValueError:
                repoid_error = f"repoid must be an integer; got {repoid_raw!r}"

        targets: list[RedisQueue] = []
        scope_specified = bool(repoid_raw or commitid or families)
        if scope_specified and repoid_error is None and family_error is None:
            targets = self._resolve_scope_targets(
                repoid=repoid,
                commitid=commitid,
                families=families,
            )

        opts = self.model._meta
        changelist_url = reverse(f"admin:{opts.app_label}_{opts.model_name}_changelist")

        # Typed-confirmation: re-type whichever scope value drives the
        # action. Preference order is repoid > commitid > joined family
        # names because the more "operational" the identifier, the
        # easier it is to remember (a repoid is a number you just
        # typed; a list of family names is harder to fat-finger but
        # more memorable than a 40-char SHA).
        if repoid_raw:
            expected_confirm = repoid_raw
        elif commitid:
            expected_confirm = commitid
        else:
            expected_confirm = ",".join(sorted(families))
        action = (request.POST.get("action") or "").strip()
        typed_confirm = (request.POST.get("typed_confirm") or "").strip()
        result = None
        confirm_error: str | None = None

        if request.method == "POST":
            if repoid_error is not None:
                messages.error(request, repoid_error)
            elif family_error is not None:
                messages.error(request, family_error)
            elif not scope_specified:
                messages.error(
                    request,
                    "Refusing to clear: scope must include a repoid, "
                    "commitid, or at least one family",
                )
            elif action not in ("dry_run", "confirm"):
                messages.error(request, f"Unknown action: {action!r}")
            elif typed_confirm != expected_confirm:
                confirm_error = f"Typed confirmation must equal {expected_confirm!r}"
            else:
                dry_run = action == "dry_run"
                result = redis_delete(targets, user=request.user, dry_run=dry_run)
                if dry_run:
                    messages.info(
                        request,
                        f"Dry-run: would clear {result.count} key(s) across "
                        f"families={list(result.families) or '[]'}",
                    )
                else:
                    messages.success(
                        request,
                        f"Cleared {result.count} key(s) across "
                        f"families={list(result.families) or '[]'}",
                    )
                    return HttpResponseRedirect(changelist_url)

        sample_cap = 25
        sample_targets = targets[:sample_cap]
        scope_families = sorted({t.family for t in targets})

        scope_label_parts = []
        if repoid_raw:
            scope_label_parts.append(f"repoid={repoid_raw}")
        if commitid:
            scope_label_parts.append(f"commitid={commitid}")
        if families:
            scope_label_parts.append(f"family={','.join(sorted(families))}")
        scope_label = " ".join(scope_label_parts) if scope_label_parts else "(none)"

        family_choices = [
            {"name": name, "checked": name in families} for name in deletable_families
        ]

        ctx = {
            **self.admin_site.each_context(request),
            "title": "Clear Redis by scope",
            "opts": opts,
            "has_view_permission": self.has_view_permission(request),
            "repoid": repoid_raw,
            "commitid": commitid,
            "selected_families": families,
            "family_choices": family_choices,
            "scope_label": scope_label,
            "scope_specified": scope_specified,
            "repoid_error": repoid_error,
            "family_error": family_error,
            "confirm_error": confirm_error,
            "expected_confirm": expected_confirm,
            "typed_confirm": typed_confirm,
            "targets": targets,
            "target_count": len(targets),
            "sample_targets": sample_targets,
            "sample_cap": sample_cap,
            "scope_families": scope_families,
            "changelist_url": changelist_url,
            "result": result,
        }
        return render(request, "admin/redis_admin/clear_by_scope.html", ctx)

    # ---- Delete actions (M5.2 + dry-run-mandatory bulk) -----------------
    #
    # The standard `delete_selected` is replaced with `clear_selected`,
    # a 2-stage action that always runs a dry-run *before* offering the
    # destructive button. Django's stock flow is "select → confirm →
    # delete"; ours is "select → dry-run preview → confirm → delete".
    # The dry-run output (count, families, refused, sample) is rendered
    # inline on the confirmation page so the operator can sanity-check
    # the impact of the action before committing.
    #
    # `clear_dry_run` is kept as a separate action for the
    # "let me see what would happen if I cleared X, with no risk of
    # accidentally hitting the wrong button" workflow — it has no
    # destructive sibling on the same page, just `message_user`.

    actions = ("clear_dry_run", "clear_selected")

    def get_actions(self, request):
        # Strip Django's stock `delete_selected` so the only path that
        # actually mutates Redis from a bulk action goes through our
        # dry-run-mandatory `clear_selected`.
        actions = super().get_actions(request)
        actions.pop("delete_selected", None)
        return actions

    @admin.action(
        description="Dry-run: count what 'clear selected' would clear",
        # Staff with read-only access don't need a button that writes
        # `LogEntry` rows; the dry-run action is a sibling of the
        # destructive bulk-clear, and "anything next to a delete
        # button is superuser-only" is the simpler invariant to
        # reason about. Locks down the audit-log surface area too.
        permissions=("delete",),
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

    @admin.action(
        description="Clear selected (dry-run preview, then confirm)",
        permissions=("delete",),
    )
    def clear_selected(self, request: HttpRequest, queryset):
        """Two-stage clear: shows dry-run results, then waits for confirm.

        Stage 1 (no `confirm` flag): runs `redis_delete(dry_run=True)`
        and renders a confirmation page that displays the dry-run's
        count + families + refused + sample inline. The page's form
        re-posts the same action with `confirm=yes`.

        Stage 2 (`confirm=yes`): runs `redis_delete(dry_run=False)`,
        the actual mutation. Returning `None` falls through to the
        standard action redirect back to the changelist.
        """

        selected = list(queryset)
        # Pull the original `_selected_action` checkbox values so we can
        # round-trip them as hidden inputs on the confirmation form;
        # Django's action helper re-fetches the queryset from these on
        # the second POST.
        selected_pks = request.POST.getlist("_selected_action") or [
            obj.pk for obj in selected
        ]

        if request.POST.get("confirm") == "yes":
            result = redis_delete(selected, user=request.user, dry_run=False)
            self.message_user(
                request,
                (
                    f"Cleared {result.count} Redis key(s) across "
                    f"families={list(result.families) or '[]'}"
                    + (
                        f"; refused={list(result.refused[:5])}"
                        if result.refused
                        else ""
                    )
                ),
                level=messages.SUCCESS,
            )
            # Falls through to Django's "redirect back to changelist"
            # behaviour for actions that return None.
            return None

        dry_run_result = redis_delete(selected, user=request.user, dry_run=True)

        opts = self.model._meta
        ctx = {
            **self.admin_site.each_context(request),
            "title": "Confirm clear of selected Redis keys",
            "opts": opts,
            "action_name": "clear_selected",
            "selected": selected,
            "selected_pks": selected_pks,
            "dry_run_result": dry_run_result,
            "media": self.media,
        }
        return render(
            request, "admin/redis_admin/clear_selected_confirmation.html", ctx
        )

    def delete_queryset(self, request: HttpRequest, queryset) -> None:
        """Defensive: stock `delete_selected` is stripped in `get_actions`,
        but if a future code path reintroduces it, route through the
        same audited service rather than letting the ORM fall through
        on an unmanaged model.
        """

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
        """Single-object delete from the change page.

        The change page already runs through Django's standard
        `delete_view` confirmation interstitial, which displays the
        full key path before the operator commits, so we don't add a
        second dry-run gate here.
        """

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
        # Celery broker queues land on the celery-aware admin
        # (`CeleryBrokerQueueAdmin`) which decodes each kombu envelope
        # into structured columns and offers per-message clear with
        # the LSET-tombstone path. Other families keep using the
        # generic items view.
        if obj.family == "celery_broker":
            url = reverse("admin:redis_admin_celerybrokerqueue_changelist")
        else:
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
    # worker's `LockManager` is the only legitimate releaser. This is
    # stricter than the per-admin "superuser-only deletion" invariant
    # — locks aren't even superuser-deletable. `services.redis_delete`
    # additionally refuses any key whose family has `is_deletable=
    # False` so URL-tampering can't bypass this.
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

    # Item-level mutations are not exposed in the admin: per-item
    # `LREM` / `SREM` / `HDEL` would require reconstructing the
    # original Redis value from the admin pk_token (and SETs/LISTs
    # don't round-trip cleanly through SSCAN cursors), so operators
    # who need to drop a bad message clear the entire queue from
    # `RedisQueueAdmin`. If we ever lift this, the new gate must be
    # `is_superuser` to match every other deletion path in this app.
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


# ---- Celery broker per-message changelist (M6) -----------------------------


def _parse_celery_search_term(term: str) -> dict[str, str]:
    """Pull `key:value` tokens out of the celery changelist search bar.

    Recognised keys: `repoid`, `commit` / `commitid`, `task` /
    `task_name`, `task_id`, `ownerid`, `pullid`, `queue` /
    `queue_name`. Bare tokens become a `task_name__icontains` so a
    user can paste a task class name (`BundleAnalysisProcessor`)
    without remembering the prefix.
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
            elif key in ("commit", "commitid"):
                out["commitid__startswith"] = value
            elif key in ("task", "task_name"):
                out["task_name__icontains"] = value
            elif key == "task_id":
                out["task_id__exact"] = value
            elif key == "ownerid":
                out["ownerid__exact"] = value
            elif key == "pullid":
                out["pullid__exact"] = value
            elif key in ("queue", "queue_name"):
                out["queue_name__exact"] = value
            else:
                bare.append(tok)
        else:
            bare.append(tok)
    if bare:
        out["task_name__icontains"] = " ".join(bare)
    return out


class CeleryBrokerTaskFilter(admin.SimpleListFilter):
    """Sidebar dropdown of `task_name` values present in the queue.

    Built dynamically off the currently-filtered queue so the picker
    only shows tasks the operator can actually drill into.
    `lookups()` returns an empty tuple when no `queue_name__exact`
    is set — the admin renders the filter as "All" only.
    """

    title = "celery task"
    parameter_name = "celery_task"

    def lookups(self, request, model_admin):
        queue = request.GET.get("queue_name__exact")
        if not queue:
            return ()
        try:
            qs = CeleryBrokerQueueQuerySet(
                CeleryBrokerQueue, queue_name=queue
            )._fetch_all()
        except Exception:  # pragma: no cover - defensive
            return ()
        seen: list[tuple[str, str]] = []
        seen_set: set[str] = set()
        for row in qs:
            name = row.task_name
            if name and name not in seen_set:
                seen_set.add(name)
                seen.append((name, name))
        seen.sort()
        return tuple(seen)

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(task_name__exact=value)
        return queryset


class CeleryBrokerRepoidFilter(admin.SimpleListFilter):
    """Free-text `repoid` filter, rendered as a sidebar input.

    Django doesn't ship a stock text-input list filter, but
    `SimpleListFilter` with empty `lookups()` plus a `?repoid=`
    query string is enough to honour the URL parameter; the admin
    template surfaces it via `lookup_allowed`. Operators paste a
    repoid into the URL or the search bar (`repoid:1234`); this
    filter just makes the URL parameter participate in the
    queryset filter pipeline.
    """

    title = "repoid"
    parameter_name = "repoid"

    def lookups(self, request, model_admin):
        return ()

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(repoid__exact=value)
        return queryset

    def has_output(self) -> bool:  # pragma: no cover - admin convention
        return False


class CeleryBrokerCommitFilter(admin.SimpleListFilter):
    """Same shape as `CeleryBrokerRepoidFilter` but matches by commit prefix.

    `commitid__startswith` so pasting either the 7-char abbrev
    (rendered next to each row) or the full SHA both narrow
    correctly.
    """

    title = "commit"
    parameter_name = "commitid"

    def lookups(self, request, model_admin):
        return ()

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(commitid__startswith=value)
        return queryset

    def has_output(self) -> bool:  # pragma: no cover - admin convention
        return False


@admin.register(CeleryBrokerQueue)
class CeleryBrokerQueueAdmin(admin.ModelAdmin):
    """Two-mode admin for `celery_broker` queues.

    The same URL serves two different views, switched by whether the
    request carries a `?queue_name__exact=<queue>` filter:

    * **No queue filter (landing page).** One row per known celery
      queue with `depth = LLEN(queue)`. The "messages" column is a
      drill-in link that re-renders this admin with
      `?queue_name__exact=<queue>`. This is the default entry
      point so an on-call engineer who hits
      `/admin/redis_admin/celerybrokerqueue/` from Grafana sees
      every queue and its current depth at a glance.
    * **`?queue_name__exact=<queue>` (drill-down).** One row per
      kombu message inside that queue, with the envelope decoded
      into `task_name` / `repoid` / `commitid` / `task_id` /
      `ownerid` / `pullid` / `payload_preview` columns. Per-message
      delete uses the canonical LSET-tombstone path
      (`services.celery_broker_clear`) so it's race-safe under
      concurrent celery consumers.

    The model is the same `CeleryBrokerQueue` for both modes;
    `get_list_display` / `get_list_display_links` /
    `get_list_filter` / `get_actions` flip based on URL so each
    mode shows the columns that are actually populated and the
    actions that make sense for that scope.
    """

    summary_list_display = ("queue_name", "depth", "messages_link")
    message_list_display = (
        "index_in_queue",
        "task_name",
        "repoid_link",
        "commitid_link",
        "ownerid",
        "pullid",
        "task_id_short",
        "payload_preview_truncated",
    )
    list_display = message_list_display
    list_filter = (
        CeleryQueueFilter,
        CeleryBrokerTaskFilter,
        CeleryBrokerRepoidFilter,
        CeleryBrokerCommitFilter,
    )
    list_per_page = redis_admin_settings.ITEM_PAGE_SIZE
    show_full_result_count = False
    # Default ordering for the per-message drill-down (one row per
    # kombu envelope). `get_ordering` below flips this to
    # `(-depth, queue_name)` in summary mode so the landing page
    # surfaces the hottest queues first.
    ordering = ("index_in_queue",)
    # Summary mode ordering: depth DESC, then queue_name ASC. Kept
    # as a class attribute so tests / subclasses can pin the
    # expected sort without reaching into `get_ordering`.
    summary_ordering: tuple[str, ...] = ("-depth", "queue_name")
    search_fields = ("task_name",)
    search_help_text = (
        "search by 'repoid:1234', 'commit:abc', 'task:BundleAnalysisProcessor', "
        "'task_id:<uuid>', 'ownerid:N', 'pullid:N', or 'queue:notify'. "
        "Bare tokens are matched against the task name."
    )
    readonly_fields = (
        "pk_token",
        "queue_name",
        "index_in_queue",
        "task_name",
        "task_id",
        "repoid",
        "commitid",
        "ownerid",
        "pullid",
        "payload_preview",
    )
    actions = ("clear_dry_run", "clear_selected")

    # ---- Permissions / read-only safety -----------------------------------

    def has_add_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def has_view_permission(self, request: HttpRequest, obj=None) -> bool:
        return bool(request.user and request.user.is_staff)

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        # Mirrors `RedisQueueAdmin`: dry-run is staff-allowed via the
        # `permissions=("delete",)` action gate, but the destructive
        # commit is superuser-only.
        return bool(request.user and request.user.is_superuser)

    def lookup_allowed(self, lookup, value) -> bool:
        if lookup in (
            "queue_name__exact",
            "queue_name",
            "repoid",
            "repoid__exact",
            "commitid",
            "commitid__startswith",
            "commitid__exact",
            "task_name",
            "task_name__exact",
            "task_name__icontains",
            "task_id",
            "task_id__exact",
            "ownerid",
            "ownerid__exact",
            "pullid",
            "pullid__exact",
        ):
            return True
        return super().lookup_allowed(lookup, value)

    @staticmethod
    def _is_summary_request(request: HttpRequest) -> bool:
        """Summary mode = no `queue_name__exact` filter on the URL."""

        return not request.GET.get("queue_name__exact")

    def get_ordering(self, request: HttpRequest):
        """Default sort depends on mode.

        * Summary (landing page, no `queue_name__exact`): deepest
          queues first, ties broken alphabetically by queue name —
          an on-call engineer scanning the top of the page sees the
          hottest queues up top.
        * Drill-down (per-message view, `?queue_name__exact=<q>`):
          index-order is preserved so `matches[0]` is the next
          message a Celery consumer would pop. The clear-by-filter
          and keep-one-survivor paths rely on that invariant.

        Django's `ChangeList` still layers explicit `?o=` column-
        header clicks on top of this default, so operators can
        re-sort at will.
        """

        if self._is_summary_request(request):
            return self.summary_ordering
        return self.ordering

    def get_list_display(self, request: HttpRequest):
        if self._is_summary_request(request):
            return self.summary_list_display
        return self.message_list_display

    def get_list_display_links(self, request: HttpRequest, list_display):
        if self._is_summary_request(request):
            # Summary rows expose their drill-in via `messages_link`;
            # don't make Django wrap `queue_name` in a change-view link
            # (the change-view for a summary pk doesn't render anything
            # useful, since there's no real "queue object" to inspect).
            return (None,) if not list_display else None
        return super().get_list_display_links(request, list_display)

    def get_list_filter(self, request: HttpRequest):
        if self._is_summary_request(request):
            # The message-shaped sidebar filters (task / repoid /
            # commit) don't apply to queue summaries; hide them so
            # the landing page stays focused on "pick a queue".
            return ()
        return self.list_filter

    def get_actions(self, request):
        actions = super().get_actions(request)
        actions.pop("delete_selected", None)
        if self._is_summary_request(request):
            # Bulk-clear lives on the per-message view; selecting
            # whole queues and clearing them would amount to a `DEL`,
            # which the existing `RedisQueueAdmin` clear flow already
            # owns. Keep the surfaces non-overlapping.
            actions.pop("clear_dry_run", None)
            actions.pop("clear_selected", None)
        return actions

    def get_queryset(self, request: HttpRequest):
        # Bypass ChangeList's `.filter(**lookup_params)` plumbing so the
        # `queue_name__exact` flip between summary and per-message
        # mode survives untouched even when no list_filter is
        # declared on the URL.
        queryset = self.model._default_manager.all()
        queue_name = request.GET.get("queue_name__exact")
        if queue_name:
            queryset = queryset.filter(queue_name__exact=queue_name)
        # Plumb the request through so the changelist and the
        # frequency-chart context builder share a single LRANGE +
        # parse pass on the same queue (each calls `get_queryset`
        # separately during one render; without the cache, each
        # round-trips Redis on its own).
        queryset._request = request
        return queryset

    def changelist_view(self, request: HttpRequest, extra_context=None):
        # No info nudge — the empty-state used to ask the operator to
        # navigate back to `RedisQueue`, but `celery_broker` is now
        # hidden from that admin; the landing page itself is the
        # queue-discovery surface.
        ctx = dict(extra_context or {})
        if not self._is_summary_request(request):
            # Pass queue_name so the template can wire the lazy chart
            # fragment fetch. The chart itself is rendered by
            # `chart_fragment_view` via an AJAX call, keeping the
            # changelist fast even for 500k-deep queues.
            ctx["queue_name"] = request.GET.get("queue_name__exact", "")
            # Surface the actual `CELERY_BROKER_SCAN_LIMIT` so the
            # loader hint reflects whatever the deployment overrode it
            # to (50_000 / 200_000 / etc.), instead of the hardcoded
            # default. Keeps the user-facing message honest in
            # environments where the scan window has been tuned.
            # Pre-formatted with thousands separators since
            # `django.contrib.humanize` is not installed in this
            # service's INSTALLED_APPS, so `intcomma` is unavailable
            # in the template.
            ctx["scan_limit_label"] = (
                f"{redis_admin_settings.CELERY_BROKER_SCAN_LIMIT:,}"
            )
        return super().changelist_view(request, ctx)

    def _build_frequency_chart_context(
        self, request: HttpRequest, queue_name: str
    ) -> dict | None:
        """Compute the frequency chart payload for a given queue.

        Returns `None` when the chart should not render (no buckets
        or `LRANGE` couldn't reach the broker). Used by both the
        eager inline path (chart_fragment_view) and any future
        callers. Separated from `changelist_view` so the fragment
        endpoint can invoke it in isolation.
        """

        try:
            broker_conn = _conn.get_connection(kind="broker")
            buckets, total_sampled = _stream_frequency_aggregate(
                broker_conn, queue_name
            )
        except Exception:  # pragma: no cover - broker outage path
            return None
        if not buckets:
            return None
        try:
            client = _conn.get_connection(kind="broker")
            llen = client.llen(queue_name)
            total_depth = int(llen) if isinstance(llen, int) and llen >= 0 else 0
        except Exception:  # pragma: no cover - defensive
            total_depth = sum(b.count for b in buckets)
        total_visible = total_sampled
        try:
            clear_url = reverse("admin:redis_admin_celerybrokerqueue_clear_by_filter")
        except NoReverseMatch:  # pragma: no cover - URL is wired below
            clear_url = ""
        # Hydrate `service:owner/name` strings + per-repo change URLs
        # in one query, then attach to a flat view-model so the
        # template stays free of attribute lookups against
        # `FrequencyBucket` (which is frozen) or N+1 SQL.
        repo_displays = _resolve_repo_displays(b.repoid for b in buckets)
        rows: list[dict[str, Any]] = []
        for bucket in buckets:
            display = repo_displays.get(bucket.repoid) if bucket.repoid else None
            change_url: str | None = None
            if bucket.repoid is not None:
                try:
                    change_url = reverse(
                        "admin:core_repository_change", args=[bucket.repoid]
                    )
                except NoReverseMatch:  # pragma: no cover - admin always wires this
                    change_url = None
            rows.append(
                {
                    "task_name": bucket.task_name,
                    "repoid": bucket.repoid,
                    "repo_display": display,
                    "repo_change_url": change_url,
                    "commitid": bucket.commitid,
                    "count": bucket.count,
                    "pct": bucket.pct,
                }
            )
        return {
            "queue_name": queue_name,
            "buckets": rows,
            "total_visible": total_visible,
            "total_depth": total_depth,
            "can_clear": request.user.is_superuser,
            "clear_by_filter_url": clear_url,
        }

    def chart_fragment_view(
        self, request: HttpRequest, queue_name: str
    ) -> HttpResponse:
        """Render the frequency chart HTML fragment for `queue_name`.

        Fetched by the changelist's inline `<script>` on
        `DOMContentLoaded`. Returns `text/html` with the rendered
        `_frequency_chart.html` template (or an empty 204 when there
        are no buckets). Same permission gates as the changelist.
        """

        if not (request.user and request.user.is_staff):
            raise PermissionDenied(
                "redis_admin chart-fragment is restricted to staff users"
            )

        chart_ctx = self._build_frequency_chart_context(request, queue_name)
        if chart_ctx is None:
            return HttpResponse(status=204)
        ctx = {
            **self.admin_site.each_context(request),
            "chart": chart_ctx,
        }
        return render(
            request,
            "admin/redis_admin/celerybrokerqueue/_frequency_chart.html",
            ctx,
            content_type="text/html",
        )

    def get_urls(self):
        urls = super().get_urls()
        opts = self.model._meta
        clear_url = path(
            "clear-by-filter/",
            self.admin_site.admin_view(self.clear_by_filter_view),
            name=f"{opts.app_label}_{opts.model_name}_clear_by_filter",
        )
        # Three new URLs for the chunked-clear background job: a
        # progress page (HTML), a status JSON endpoint that the
        # progress page polls, and a cancel POST endpoint. All three
        # are routed under `clear-by-filter/job/<uuid:job_id>/...`
        # so they share the per-instance superuser gate and live
        # alongside the existing preview page in the URL tree.
        clear_progress_url = path(
            "clear-by-filter/job/<uuid:job_id>/",
            self.admin_site.admin_view(self.clear_by_filter_progress_view),
            name=(f"{opts.app_label}_{opts.model_name}_clear_by_filter_progress"),
        )
        clear_status_url = path(
            "clear-by-filter/job/<uuid:job_id>/status/",
            self.admin_site.admin_view(self.clear_by_filter_status_view),
            name=(f"{opts.app_label}_{opts.model_name}_clear_by_filter_status"),
        )
        clear_cancel_url = path(
            "clear-by-filter/job/<uuid:job_id>/cancel/",
            self.admin_site.admin_view(self.clear_by_filter_cancel_view),
            name=(f"{opts.app_label}_{opts.model_name}_clear_by_filter_cancel"),
        )
        chart_fragment_url = path(
            "<str:queue_name>/chart-fragment/",
            self.admin_site.admin_view(self.chart_fragment_view),
            name="celerybrokerqueue-chart-fragment",
        )
        # Inserted before the catch-all `<path:object_id>/` patterns so
        # custom routes don't get swallowed as object_ids.
        return [
            clear_url,
            clear_progress_url,
            clear_status_url,
            clear_cancel_url,
            chart_fragment_url,
            *urls,
        ]

    def clear_by_filter_view(self, request: HttpRequest) -> HttpResponse:
        """Targeted clear by `(queue_name, task_name?, repoid?, commitid?)`.

        Reached from the frequency chart's per-row "Clear queue"
        button (see `_frequency_chart.html`). The chart submits the
        bucket's `task_name` / `repoid` / `commitid` and lands on a
        preview page; the operator then picks one of three explicit
        actions:

        * `action=dry_run` — audited dry-run via
          `services.celery_broker_clear(dry_run=True)`. Leaves the
          queue untouched but writes a `LogEntry` so we have a paper
          trail for "what would have happened?". Re-renders the
          preview with an info banner.
        * `action=clear_keep_one` — clear every match EXCEPT the one
          with the lowest `index_in_queue`. The queryset materialises
          via `LRANGE 0 N-1` so `matches[0]` is the head-of-queue (the
          next message a Celery worker would pop); dropping it from
          the deletion set leaves a single representative in flight.
          Typical workflow: "drop the duplicate retries but leave one
          running so something still completes." Refused for
          single-match buckets where it would have to clear nothing.
        * `action=clear_all` — clear every matching message. The
          broadest of the three; equivalent to the old
          `mode=all` semantic.

        The two destructive actions both gate on a typed-confirmation
        check (operator must re-type the queue name). Filtering by
        `task_name` matters on shared queues like `celery` where
        multiple task classes coexist — without it, "clear all
        messages for repo X commit Y" would silently drop unrelated
        tasks routed through the same queue.

        Both POSTs ultimately funnel through
        `services.celery_broker_clear`, which runs the LSET-tombstone
        path and writes the audit log entry under
        `scope="celery_broker_clear"`.
        """

        if not request.user.is_superuser:
            raise PermissionDenied(
                "redis_admin clear-by-filter is restricted to superusers"
            )

        params = request.POST if request.method == "POST" else request.GET
        queue_name = (params.get("queue_name") or "").strip()
        task_name = (params.get("task_name") or "").strip()
        repoid_raw = (params.get("repoid") or "").strip()
        commitid = (params.get("commitid") or "").strip()
        action = (request.POST.get("action") or "").strip()
        # Backward-compat alias: the previous version of this view
        # used `action=confirm` paired with a `mode` form input. New
        # callers send `clear_all` / `clear_keep_one`; honour the old
        # shape for any stale tabs / scripts still in the wild.
        if action == "confirm":
            legacy_mode = (request.POST.get("mode") or "").strip()
            action = "clear_keep_one" if legacy_mode == "keep_one" else "clear_all"

        opts = self.model._meta
        changelist_url = reverse(f"admin:{opts.app_label}_{opts.model_name}_changelist")
        if queue_name:
            changelist_url = (
                f"{changelist_url}?{urlencode({'queue_name__exact': queue_name})}"
            )

        if not queue_name:
            messages.error(request, "queue_name is required for clear-by-filter")
            return HttpResponseRedirect(changelist_url)

        repoid: int | None = None
        if repoid_raw:
            try:
                repoid = int(repoid_raw)
            except ValueError:
                messages.error(
                    request,
                    f"repoid must be an integer; got {repoid_raw!r}",
                )
                return HttpResponseRedirect(changelist_url)

        # At least one narrowing filter beyond the queue is required —
        # otherwise this view collapses into "clear the whole queue",
        # which the M5 `clear_by_scope` flow already owns. Keeping
        # the surfaces non-overlapping prevents two ways to do the
        # same thing with subtly different audit-log shapes.
        if repoid is None and not commitid and not task_name:
            messages.error(
                request,
                "Refusing to clear: at least one of task_name, repoid, "
                "or commitid must be set",
            )
            return HttpResponseRedirect(changelist_url)

        queryset = self.model._default_manager.all().filter(
            queue_name__exact=queue_name
        )
        if task_name:
            queryset = queryset.filter(task_name__exact=task_name)
        if repoid is not None:
            queryset = queryset.filter(repoid=repoid)
        if commitid:
            queryset = queryset.filter(commitid__startswith=commitid)
        # Cap-bounded sample for the preview table (cheap, in-memory).
        # The queryset materialises the first `CELERY_BROKER_DISPLAY_LIMIT`
        # messages, then filters; we trim further to keep the rendered
        # table compact.
        sample_targets = sorted(queryset, key=lambda row: row.index_in_queue or 0)[
            :_CLEAR_BY_FILTER_SAMPLE_SIZE
        ]

        # `match_count` and `kept_index` come from a streaming pass
        # over the full queue so the preview agrees with what the
        # actual clear (also unbounded) would do. Going through the
        # queryset would underreport whenever `LLEN(queue) >
        # CELERY_BROKER_DISPLAY_LIMIT` (anything past the 2k materialise
        # window).
        try:
            match_count, kept_index = streaming_celery_count(
                queue_name,
                task_name=task_name or None,
                repoid=repoid,
                commitid=commitid or None,
            )
        except Exception:
            log.exception(
                "redis_admin.clear_by_filter: streaming count failed for %r",
                queue_name,
            )
            # Defensive fallback: degrade to the queryset count rather
            # than render a 500. The operator at least sees something
            # and can still hit "Clear all" — the streaming clear path
            # has its own error handling.
            match_count = len(sample_targets)
            kept_index = sample_targets[0].index_in_queue if sample_targets else None
        # Build a single synthetic target carrying the operator's
        # *exact* filter (queue + task_name? + repoid? + commitid?)
        # so `_celery_broker_clear` always derives the same
        # `frozenset({(task_name or None, repoid, commitid or None)})`
        # that `streaming_celery_count` walked the full queue with.
        #
        # Without this, the clear path inferred its filter set from
        # `sample_targets` — the materialised window capped by
        # `CELERY_BROKER_DISPLAY_LIMIT` (default 2_000). That had two
        # silent-no-op modes:
        #
        # 1. All matches sit beyond the display window (e.g. a recent
        #    burst on a queue with `LLEN > 2_000`): `sample_targets`
        #    is empty, `by_queue_filters` becomes `{}`, the streaming
        #    clear loop doesn't iterate, and the queue is never
        #    touched even though `match_count > 0` from streaming.
        # 2. Operator filtered by `repoid` only: each `sample_target`
        #    contributes a *specific* `(task_name, repoid, commitid)`
        #    triple, so the filter set excludes any task name not
        #    represented in the first 2_000 messages — those messages
        #    survive the clear.
        #
        # The synthetic target restores the streaming-clear semantic
        # of "match every envelope whose `(task, repoid, commitid)`
        # equals the operator's input." Per-message and bulk-select
        # paths (`delete_model`, `clear_selected`, `delete_queryset`,
        # `clear_dry_run`) still pass real materialised rows — those
        # are intentionally index-driven, not filter-driven, so they
        # don't go through this code path.
        # `_FILTER_ANY` (not `None`) for unset slots so the
        # streaming match treats them as wildcards. `None` would
        # require an exact-equality match against `meta.task is
        # None` / `meta.repoid is None` / `meta.commitid is None`,
        # which the per-message paths legitimately rely on for
        # tasks like `sync_repos` whose envelope leaves repoid /
        # commitid as `None`.
        task_any, repoid_any, commitid_any = _substitute_filter_any(
            task_name, repoid, commitid
        )
        filter_target = CeleryBrokerQueue(
            pk_token=f"{queue_name}#{kept_index if kept_index is not None else 'filter'}",
            queue_name=queue_name,
            index_in_queue=kept_index,
            task_name=task_any,
            repoid=repoid_any,
            commitid=commitid_any,
        )
        matches = [filter_target]

        expected_confirm = queue_name
        typed_confirm = (request.POST.get("typed_confirm") or "").strip()
        confirm_error: str | None = None

        destructive_actions = {"clear_keep_one", "clear_all"}
        valid_actions = destructive_actions | {"dry_run"}

        # The chart-driven submit lands here with no `action` set
        # (the chart's button just opens the preview page); fall
        # through to the render path without invoking the service or
        # writing a LogEntry.
        if request.method == "POST" and action in valid_actions:
            if match_count == 0:
                messages.info(
                    request,
                    "No messages match the filter — nothing to clear.",
                )
                return HttpResponseRedirect(changelist_url)

            if action == "clear_keep_one":
                if match_count <= 1:
                    # Single-message bucket: there's nothing to clear
                    # without removing the only in-flight message,
                    # which defeats the "keep first" semantic. The
                    # preview page only renders this button for
                    # buckets with match_count >= 2, but a hand-
                    # crafted POST or a count change between render
                    # and submit could still land here.
                    messages.info(
                        request,
                        f"Nothing to clear: only {match_count} message(s) "
                        f"match and 'clear all but first' would leave "
                        f"them all in place.",
                    )
                    return HttpResponseRedirect(changelist_url)

            targets = list(matches)

            if action in destructive_actions and typed_confirm != expected_confirm:
                confirm_error = f"Typed confirmation must equal {expected_confirm!r}"
            else:
                dry_run = action == "dry_run"
                keep_one = action == "clear_keep_one"
                # Dry-run keeps the synchronous shape: the request
                # already does the streaming-count walk above, so
                # the dry-run service call is fast (no LSET/LREM)
                # and the operator gets the audit-log entry +
                # re-rendered preview in one shot. The destructive
                # actions, by contrast, fan out to a background
                # thread so a 500k-deep queue clear never sits on
                # the gunicorn worker.
                if dry_run:
                    result = celery_broker_clear(
                        targets,
                        user=request.user,
                        dry_run=True,
                        keep_one=keep_one,
                    )
                    messages.info(
                        request,
                        f"Dry-run: would clear {result.count} of {match_count} "
                        f"matching message(s) from {queue_name} (audit log "
                        f"entry written; queue untouched).",
                    )
                else:
                    job_id = start_celery_broker_clear_job(
                        queue_name,
                        user=request.user,
                        task_name=task_name or None,
                        repoid=repoid,
                        commitid=commitid or None,
                        keep_one=keep_one,
                        dry_run=False,
                    )
                    progress_url = reverse(
                        f"admin:{opts.app_label}_{opts.model_name}"
                        "_clear_by_filter_progress",
                        kwargs={"job_id": job_id},
                    )
                    return HttpResponseRedirect(progress_url)

        ctx = {
            **self.admin_site.each_context(request),
            "title": f"Clear {queue_name} by filter",
            "opts": opts,
            "queue_name": queue_name,
            "task_name": task_name,
            "repoid": repoid,
            "commitid": commitid,
            "match_count": match_count,
            "kept_index": kept_index,
            "sample_targets": sample_targets,
            "expected_confirm": expected_confirm,
            "typed_confirm": typed_confirm,
            "confirm_error": confirm_error,
            "changelist_url": changelist_url,
        }
        return render(
            request, "admin/redis_admin/celerybrokerqueue/clear_by_filter.html", ctx
        )

    # ---- Chunked-clear background-job views ------------------------------
    #
    # Three superuser-only views back the chunked clear's progress page:
    #
    # * `clear_by_filter_progress_view` — renders the HTML page with an
    #   initial server-side snapshot, a `<progress>` bar, status pill,
    #   matched/total counters, and a Cancel button. Loads
    #   `celery_clear_progress.{js,css}` from `{% static %}` so the page
    #   stays CSP-compliant (the production admin only allows
    #   `'self'` + a single fixed sha256 hash for inline scripts).
    # * `clear_by_filter_status_view` — JSON endpoint polled every ~1s
    #   by the JS for progress updates. Pinned shape (see test
    #   `test_clear_job_status_view_returns_json_for_running_job`):
    #   `{job_id, status, processed, matched, drifted, ...}`.
    # * `clear_by_filter_cancel_view` — POST-only. Sets
    #   `cancel_requested=1` on the job hash; the worker thread polls
    #   it at the next chunk boundary, drains in-flight tombstones,
    #   and exits with `status=cancelled`.
    #
    # All three reuse the existing per-instance `is_superuser` gate
    # (`clear_by_filter_view` already requires it; the same staff /
    # superuser guard the rest of `CeleryBrokerQueueAdmin` uses).

    def _job_progress_context(
        self, *, job_id: str, job: dict[str, str]
    ) -> dict[str, Any]:
        """Shape the job hash + URL set for the progress template.

        Cast numeric fields to int so the template can do arithmetic
        without a `|add:0` dance. Empty strings remain empty so the
        template's `{% if commitid %}` checks behave as expected.
        Resolves both the status JSON URL and the cancel URL up
        front so the template (and the JS data attributes) doesn't
        have to know URL names.
        """

        def _int(field: str, default: int = 0) -> int:
            try:
                return int(job.get(field) or default)
            except (TypeError, ValueError):
                return default

        opts = self.model._meta
        status_url = reverse(
            f"admin:{opts.app_label}_{opts.model_name}_clear_by_filter_status",
            kwargs={"job_id": job_id},
        )
        cancel_url = reverse(
            f"admin:{opts.app_label}_{opts.model_name}_clear_by_filter_cancel",
            kwargs={"job_id": job_id},
        )
        changelist_url = reverse(f"admin:{opts.app_label}_{opts.model_name}_changelist")
        queue_name = job.get("queue", "")
        if queue_name:
            changelist_url = (
                f"{changelist_url}?{urlencode({'queue_name__exact': queue_name})}"
            )

        status = job.get("status", "pending")
        return {
            "job_id": job_id,
            "job": job,
            "status": status,
            "is_terminal": status in _CELERY_CLEAR_JOB_TERMINAL_STATES,
            "queue_name": queue_name,
            "filter_task": job.get("filter_task", ""),
            "filter_repoid": job.get("filter_repoid", ""),
            "filter_commitid": job.get("filter_commitid", ""),
            "dry_run": job.get("dry_run") == "1",
            "keep_one": job.get("keep_one") == "1",
            "started_at": job.get("started_at", ""),
            "updated_at": job.get("updated_at", ""),
            "completed_at": job.get("completed_at", ""),
            "total_estimated": _int("total_estimated"),
            "processed": _int("processed"),
            "matched": _int("matched"),
            "drifted": _int("drifted"),
            "passes_run": _int("passes_run"),
            "error": job.get("error", ""),
            "cancel_requested": job.get("cancel_requested") == "1",
            "status_url": status_url,
            "cancel_url": cancel_url,
            "changelist_url": changelist_url,
        }

    def clear_by_filter_progress_view(
        self, request: HttpRequest, job_id
    ) -> HttpResponse:
        """Render the progress page for a chunked clear job.

        The Django URL converter passes `job_id` as a `uuid.UUID`
        because the route uses `<uuid:job_id>`. We coerce to its
        hex form so it matches `start_celery_broker_clear_job`'s
        `uuid.uuid4().hex` storage key. Unknown / expired ids land
        on the changelist with an error message instead of a bare
        404 — the operator just clicked a stale link / refreshed
        past the 24h TTL, and the changelist is the most useful
        next step.
        """

        if not request.user.is_superuser:
            raise PermissionDenied(
                "redis_admin clear-by-filter is restricted to superusers"
            )

        opts = self.model._meta
        changelist_url = reverse(f"admin:{opts.app_label}_{opts.model_name}_changelist")
        # Django's `<uuid:>` converter hands us a `uuid.UUID`; cast
        # to canonical hyphenated string so the storage key matches
        # what `start_celery_broker_clear_job` wrote.
        job_id_str = str(job_id)
        job = get_celery_broker_clear_job(job_id_str)
        if job is None:
            messages.error(
                request,
                f"Clear job {job_id_str} not found (may have expired or "
                f"been submitted on a different deployment).",
            )
            return HttpResponseRedirect(changelist_url)

        progress_ctx = self._job_progress_context(job_id=job_id_str, job=job)
        ctx = {
            **self.admin_site.each_context(request),
            "title": f"Clearing {progress_ctx['queue_name']} (job {job_id_str[:8]})",
            "opts": opts,
            **progress_ctx,
        }
        return render(
            request,
            "admin/redis_admin/celerybrokerqueue/clear_by_filter_progress.html",
            ctx,
        )

    def clear_by_filter_status_view(self, request: HttpRequest, job_id) -> HttpResponse:
        """JSON endpoint for the progress page's polling loop.

        Returns a stable shape regardless of state. Pinned by
        `test_clear_job_status_view_returns_json_for_running_job` so
        the JS poll handler can rely on the field set even as the
        underlying job hash grows / shrinks across deploys.
        """

        if not request.user.is_superuser:
            raise PermissionDenied(
                "redis_admin clear-by-filter is restricted to superusers"
            )

        job_id_str = str(job_id)
        job = get_celery_broker_clear_job(job_id_str)
        if job is None:
            return JsonResponse(
                {"error": "not_found", "job_id": job_id_str}, status=404
            )

        progress_ctx = self._job_progress_context(job_id=job_id_str, job=job)
        # Trim the rendered context for the JSON wire shape: drop
        # the `job` raw blob (the polling client only needs decoded
        # fields) and the URLs (the page already has them).
        return JsonResponse(
            {
                "job_id": job_id_str,
                "status": progress_ctx["status"],
                "is_terminal": progress_ctx["is_terminal"],
                "queue_name": progress_ctx["queue_name"],
                "filter_task": progress_ctx["filter_task"],
                "filter_repoid": progress_ctx["filter_repoid"],
                "filter_commitid": progress_ctx["filter_commitid"],
                "dry_run": progress_ctx["dry_run"],
                "keep_one": progress_ctx["keep_one"],
                "started_at": progress_ctx["started_at"],
                "updated_at": progress_ctx["updated_at"],
                "completed_at": progress_ctx["completed_at"],
                "total_estimated": progress_ctx["total_estimated"],
                "processed": progress_ctx["processed"],
                "matched": progress_ctx["matched"],
                "drifted": progress_ctx["drifted"],
                "passes_run": progress_ctx["passes_run"],
                "error": progress_ctx["error"],
                "cancel_requested": progress_ctx["cancel_requested"],
            }
        )

    def clear_by_filter_cancel_view(self, request: HttpRequest, job_id) -> HttpResponse:
        """POST-only endpoint that flags the job for cancellation.

        Returns 202 (request accepted; effect lands at the next
        chunk boundary in the worker thread) with the latest job
        snapshot so the UI can update without waiting for the next
        poll cycle. Idempotent: re-cancelling a cancelled job is a
        no-op.
        """

        if not request.user.is_superuser:
            raise PermissionDenied(
                "redis_admin clear-by-filter is restricted to superusers"
            )
        if request.method != "POST":
            return HttpResponseNotAllowed(["POST"])

        job_id_str = str(job_id)
        ok = request_cancel_celery_broker_clear_job(job_id_str)
        if not ok:
            return JsonResponse(
                {"error": "not_found", "job_id": job_id_str}, status=404
            )
        # Re-fetch so the response carries `cancel_requested=1`
        # without the client needing a follow-up poll.
        job = get_celery_broker_clear_job(job_id_str) or {}
        return JsonResponse(
            {
                "job_id": job_id_str,
                "cancel_requested": job.get("cancel_requested") == "1",
                "status": job.get("status", ""),
            },
            status=202,
        )

    def get_search_results(self, request, queryset, search_term):
        if not search_term:
            return queryset, False
        kwargs = _parse_celery_search_term(search_term)
        if not kwargs:
            return queryset, False
        try:
            return queryset.filter(**kwargs), False
        except NotImplementedError:
            return queryset.none(), False

    # ---- Change form (per-message inspector) -----------------------------

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
        raise RuntimeError("CeleryBrokerQueue rows are read-only.")

    # ---- Bulk clear (dry-run + confirm) ----------------------------------

    @admin.action(
        description="Dry-run: count what 'clear selected' would clear",
        permissions=("delete",),
    )
    def clear_dry_run(self, request: HttpRequest, queryset) -> None:
        result = celery_broker_clear(list(queryset), user=request.user, dry_run=True)
        self.message_user(
            request,
            (
                f"Dry-run: would clear {result.count} celery message(s); "
                f"sample={list(result.sample[:5])}"
            ),
            level=messages.INFO,
        )

    @admin.action(
        description="Clear selected (dry-run preview, then confirm)",
        permissions=("delete",),
    )
    def clear_selected(self, request: HttpRequest, queryset):
        """Two-stage clear; LSET-tombstone path on commit."""

        selected = list(queryset)
        selected_pks = request.POST.getlist("_selected_action") or [
            obj.pk for obj in selected
        ]

        if request.POST.get("confirm") == "yes":
            result = celery_broker_clear(selected, user=request.user, dry_run=False)
            self.message_user(
                request,
                (
                    f"Cleared {result.count} celery message(s) "
                    f"across {len({obj.queue_name for obj in selected})} queue(s)."
                ),
                level=messages.SUCCESS,
            )
            return None

        dry_run_result = celery_broker_clear(selected, user=request.user, dry_run=True)
        opts = self.model._meta
        ctx = {
            **self.admin_site.each_context(request),
            "title": "Confirm clear of selected celery messages",
            "opts": opts,
            "action_name": "clear_selected",
            "selected": selected,
            "selected_pks": selected_pks,
            "dry_run_result": dry_run_result,
            "media": self.media,
        }
        return render(
            request, "admin/redis_admin/clear_selected_confirmation.html", ctx
        )

    def delete_queryset(self, request: HttpRequest, queryset) -> None:
        """Defensive: stripped from `get_actions` but route through the
        audited celery clear if anything reintroduces it.
        """

        result = celery_broker_clear(list(queryset), user=request.user, dry_run=False)
        self.message_user(
            request,
            f"Cleared {result.count} celery message(s).",
            level=messages.SUCCESS,
        )

    def delete_model(self, request: HttpRequest, obj: CeleryBrokerQueue) -> None:
        """Single-message delete from the change page."""

        result = celery_broker_clear([obj], user=request.user, dry_run=False)
        self.message_user(
            request,
            (
                f"Cleared celery message {obj.queue_name}[{obj.index_in_queue}] "
                f"({result.count} removed)."
            ),
            level=messages.SUCCESS,
        )

    # ---- Display helpers --------------------------------------------------

    @admin.display(description="repo")
    def repoid_link(self, obj: CeleryBrokerQueue) -> str:
        if obj.repoid is None:
            return "—"
        try:
            url = reverse("admin:core_repository_change", args=[obj.repoid])
        except NoReverseMatch:
            return str(obj.repoid)
        return format_html('<a href="{}">{}</a>', url, obj.repoid)

    @admin.display(description="commit")
    def commitid_link(self, obj: CeleryBrokerQueue) -> str:
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

    @admin.display(description="task id")
    def task_id_short(self, obj: CeleryBrokerQueue) -> str:
        if not obj.task_id:
            return "—"
        # Task IDs are kombu UUIDs; the first 8 chars uniquely identify
        # them in nearly every realistic queue size and keep the column
        # narrow.
        short = obj.task_id[:8]
        return format_html('<span title="{}">{}</span>', obj.task_id, short)

    @admin.display(description="payload")
    def payload_preview_truncated(self, obj: CeleryBrokerQueue) -> str:
        # Lazy-render on first display: the changelist queryset
        # constructs every row with an empty preview and a stashed
        # `_kwargs_for_preview`, so the per-row JSON-render cost only
        # gets paid for rows that actually paginate onto the screen.
        return resolve_payload_preview(obj)

    @admin.display(description="messages")
    def messages_link(self, obj: CeleryBrokerQueue) -> str:
        """Drill-in link rendered on each summary row.

        Re-renders the same admin URL with `?queue_name__exact=<queue>`,
        which flips `get_list_display` from `summary_list_display` to
        `message_list_display` and replaces this column set with the
        per-message decoder columns (`task_name`, `repoid_link`, …).
        """

        depth = obj.depth or 0
        try:
            base = reverse("admin:redis_admin_celerybrokerqueue_changelist")
        except NoReverseMatch:  # pragma: no cover - defensive
            return f"{depth} message(s)"
        query = urlencode({"queue_name__exact": obj.queue_name})
        # Match Django admin's standard "view link" copy and styling so
        # the column reads naturally next to `depth`.
        return format_html(
            '<a href="{}?{}">view {} message(s) \u2192</a>',
            base,
            query,
            depth,
        )
