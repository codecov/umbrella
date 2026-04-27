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

from collections.abc import Iterable, Sequence
from typing import Any
from urllib.parse import urlencode

from django.contrib import admin, messages
from django.contrib.admin.utils import quote, unquote
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest, HttpResponse, HttpResponseRedirect
from django.shortcuts import render
from django.urls import NoReverseMatch, path, reverse
from django.utils.html import format_html, format_html_join

from core.models import Repository

from . import conn as _conn
from . import settings as redis_admin_settings
from .families import FAMILIES, iter_keys
from .models import RedisLock, RedisQueue, RedisQueueItem
from .queryset import RedisItemQuerySet, _build_redis_queue
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
