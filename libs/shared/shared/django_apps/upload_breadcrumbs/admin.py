import json
from collections.abc import Iterator
from typing import Any

from django.conf import settings
from django.contrib import admin
from django.contrib.admin.views.main import ChangeList
from django.db.models import QuerySet
from django.http import HttpRequest
from django.utils.html import format_html
from django.utils.safestring import mark_safe

from shared.config import get_config
from shared.django_apps.core.models import Repository
from shared.django_apps.upload_breadcrumbs.models import (
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)


class PresentDataFilter(admin.SimpleListFilter):
    title = "Present Data"
    parameter_name = "present_data"

    def lookups(
        self, request: HttpRequest, model_admin: admin.ModelAdmin
    ) -> list[tuple[str, str]]:
        return [
            ("has_milestone", "Has Milestone"),
            ("has_endpoint", "Has Endpoint"),
            ("has_error", "Has Error"),
            ("has_error_text", "Has Error Text"),
            ("has_upload_ids", "Has Upload IDs"),
            ("has_sentry_trace", "Has Sentry Trace"),
        ]

    def choices(self, changelist: ChangeList) -> Iterator[Any]:
        """
        Override choices to add multiselect functionality.

        This method customizes the filter UI to allow multiple selections instead of
        the default single-select behavior. It creates checkboxes that users can
        toggle on/off to select multiple filter criteria simultaneously.
        """
        # Yield the "All" option that clears all filters
        # `query_string` is the URL-encoded query string for the filter that gets applied if selected
        yield {
            "selected": self.value() is None,
            "query_string": changelist.get_query_string(remove=[self.parameter_name]),
            "display": "All",
        }

        # Parse the current filter value into a list of selected options
        value = self.value()
        current_values = value.split(",") if isinstance(value, str) else []

        for lookup, title in self.lookup_choices:
            selected = lookup in current_values

            if selected:
                # Allow users to "uncheck" the option by clicking it again
                new_values = [v for v in current_values if v != lookup]
            else:
                # Allow users to "check" this option
                new_values = current_values + [lookup]

            new_value = ",".join(new_values) if new_values else None
            query_dict = {self.parameter_name: new_value} if new_value else {}

            yield {
                "selected": selected,
                "query_string": changelist.get_query_string(
                    query_dict, [self.parameter_name]
                ),
                "display": f"{'✓ ' if selected else ''}{title}",
            }

    def queryset(self, request: HttpRequest, queryset: QuerySet) -> QuerySet:
        if not self.value():
            return queryset

        selected_filters = (self.value() or "").split(",")

        for filter_type in selected_filters:
            if filter_type == "has_milestone":
                queryset = queryset.filter(breadcrumb_data__milestone__isnull=False)
            elif filter_type == "has_endpoint":
                queryset = queryset.filter(breadcrumb_data__endpoint__isnull=False)
            elif filter_type == "has_error":
                queryset = queryset.filter(breadcrumb_data__error__isnull=False)
            elif filter_type == "has_error_text":
                queryset = queryset.filter(breadcrumb_data__error_text__isnull=False)
            elif filter_type == "has_upload_ids":
                queryset = queryset.extra(where=["array_length(upload_ids, 1) >= 1"])
            elif filter_type == "has_sentry_trace":
                queryset = queryset.filter(sentry_trace_id__isnull=False)

        return queryset


class MilestoneFilter(admin.SimpleListFilter):
    title = "Milestone"
    parameter_name = "milestone"

    def lookups(
        self, request: HttpRequest, model_admin: admin.ModelAdmin
    ) -> list[tuple[str, str]]:
        return [(choice.value, choice.label) for choice in Milestones]

    def queryset(self, request: HttpRequest, queryset: QuerySet) -> QuerySet:
        if self.value():
            return queryset.filter(breadcrumb_data__milestone=self.value())
        return queryset


class EndpointFilter(admin.SimpleListFilter):
    title = "Endpoint"
    parameter_name = "endpoint"

    def lookups(
        self, request: HttpRequest, model_admin: admin.ModelAdmin
    ) -> list[tuple[str, str]]:
        return [(choice.value, choice.name) for choice in Endpoints]

    def queryset(self, request: HttpRequest, queryset: QuerySet) -> QuerySet:
        if self.value():
            return queryset.filter(breadcrumb_data__endpoint=self.value())
        return queryset


class ErrorFilter(admin.SimpleListFilter):
    title = "Error"
    parameter_name = "error"

    def lookups(
        self, request: HttpRequest, model_admin: admin.ModelAdmin
    ) -> list[tuple[str, str]]:
        return [(choice.value, choice.name) for choice in Errors]

    def queryset(self, request: HttpRequest, queryset: QuerySet) -> QuerySet:
        if self.value():
            return queryset.filter(breadcrumb_data__error=self.value())
        return queryset


@admin.register(UploadBreadcrumb)
class UploadBreadcrumbAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "repo_id",
        "formatted_commit_sha",
        "formatted_breadcrumb_data",
        "formatted_upload_ids",
        "formatted_sentry_trace_id",
    )
    show_full_result_count = False
    search_fields = (
        "repo_id__startswith",
        "commit_sha__startswith",
        "sentry_trace_id__startswith",
    )
    search_help_text = "Search by repository ID, commit SHA, and/or Sentry trace ID (all prefix match). Separate multiple values with spaces to AND search (E.g. '<repo_id> <commit_sha>')."
    list_filter = [
        PresentDataFilter,
        MilestoneFilter,
        EndpointFilter,
        ErrorFilter,
    ]
    fields = (
        "created_at",
        "formatted_repo_id",
        "commit_sha",
        "formatted_breadcrumb_data_detail",
        "formatted_upload_ids_detail",
        "formatted_sentry_trace_id_detail",
        "log_links",
    )
    date_hierarchy = "created_at"
    list_per_page = 50
    list_max_show_all = 200
    show_full_result_count = False  # Disable full result count for performance

    @admin.display(description="Repository", ordering="repo_id")
    def formatted_repo_id(self, obj: UploadBreadcrumb) -> str:
        """Display repository ID with link to repository admin page."""
        if not obj.repo_id:
            return "-"

        try:
            repo = Repository.objects.get(repoid=obj.repo_id)
            repo_url = (
                f"/{settings.DJANGO_ADMIN_URL}/core/repository/{repo.repoid}/change/"
            )
            return format_html(
                '<a href="{}" target="_blank">{}</a> ({})',
                repo_url,
                obj.repo_id,
                f"{repo.author.username}/{repo.name}",
            )
        except Exception:
            return str(obj.repo_id)

    @admin.display(description="Commit", ordering="commit_sha")
    def formatted_commit_sha(self, obj: UploadBreadcrumb) -> str:
        """Display commit SHA as short hash."""
        if not obj.commit_sha:
            return "-"

        return obj.commit_sha[:7]

    @admin.display(description="Breadcrumb Data")
    def formatted_breadcrumb_data(self, obj: UploadBreadcrumb) -> str:
        """Display breadcrumb data compactly for list view."""
        if not obj.breadcrumb_data:
            return "-"

        parts = []
        data = obj.breadcrumb_data

        if data.get("milestone"):
            milestone_label = Milestones(data["milestone"]).label
            parts.append(f"📍 {milestone_label}")

        if data.get("endpoint"):
            endpoint_label = Endpoints(data["endpoint"]).label
            parts.append(f"🔗 {endpoint_label}")

        if data.get("error"):
            error_label = Errors(data["error"]).label
            parts.append(f"❌ {error_label}")

        if data.get("error_text"):
            error_text = data["error_text"]
            if len(error_text) > 50:
                error_text = error_text[:47] + "..."
            parts.append(f"💬 {error_text}")

        return mark_safe("<br>".join(parts)) if parts else "-"

    @admin.display(description="Breadcrumb Data")
    def formatted_breadcrumb_data_detail(self, obj: UploadBreadcrumb) -> str:
        """Display detailed breadcrumb data for individual item view."""
        if not obj.breadcrumb_data:
            return "-"

        data = obj.breadcrumb_data
        html_parts = []

        html_parts.append(
            '<div style="font-family: monospace; padding: 15px; border-radius: 5px; border: 1px solid var(--hairline-color);">'
        )

        # Show each field with its label
        if data.get("milestone"):
            milestone_label = Milestones(data["milestone"]).label
            html_parts.append(
                f"<div><strong>📍 Milestone:</strong> {milestone_label} <span>({data['milestone']})</span></div>"
            )

        if data.get("endpoint"):
            endpoint_label = Endpoints(data["endpoint"]).label
            endpoint_name = Endpoints(data["endpoint"]).name
            html_parts.append(
                f"<div><strong>🔗 Endpoint:</strong> {endpoint_label} <span>({data['endpoint']} / {endpoint_name})</span></div>"
            )

        if data.get("error"):
            error_label = Errors(data["error"]).label
            html_parts.append(
                f"<div><strong>❌ Error:</strong> {error_label} <span>({data['error']})</span></div>"
            )

        if data.get("error_text"):
            html_parts.append(
                f"<div><strong>💬 Error Text:</strong> {data['error_text']}</div>"
            )

        html_parts.append("</div>")

        # Also show raw JSON
        html_parts.append(
            '<br><details><summary style="cursor: pointer; font-weight: bold;">Raw JSON Data</summary>'
        )
        html_parts.append(
            f'<pre style="padding: 10px; border-radius: 5px; border: 1px solid var(--hairline-color); overflow-x: auto;">{json.dumps(data, indent=2)}</pre>'
        )
        html_parts.append("</details>")

        return mark_safe("".join(html_parts))

    @admin.display(description="Upload IDs")
    def formatted_upload_ids(self, obj: UploadBreadcrumb) -> str:
        """Display upload IDs compactly for list view."""
        if not obj.upload_ids:
            return "-"

        MAX_DISPLAY_COUNT = 3

        if len(obj.upload_ids) <= MAX_DISPLAY_COUNT:
            return ", ".join(str(uid) for uid in obj.upload_ids)
        else:
            return f"{', '.join(str(uid) for uid in obj.upload_ids[:MAX_DISPLAY_COUNT])}, ... (+{len(obj.upload_ids) - MAX_DISPLAY_COUNT} more)"

    @admin.display(description="Upload IDs")
    def formatted_upload_ids_detail(self, obj: UploadBreadcrumb) -> str:
        """Display detailed upload IDs for individual item view."""
        if not obj.upload_ids:
            return "-"

        html_parts = []
        html_parts.append(
            '<div style="font-family: monospace; padding: 15px; border-radius: 5px; border: 1px solid var(--hairline-color);">'
        )
        html_parts.append(
            f"<div><strong>Upload IDs ({len(obj.upload_ids)} total):</strong></div>"
        )
        html_parts.append('<div style="margin-top: 10px;">')

        upload_items = [
            f"<div style='margin-left: 10px;'>• {upload_id}</div>"
            for upload_id in obj.upload_ids
        ]
        html_parts.extend(upload_items)

        html_parts.append("</div>")
        html_parts.append("</div>")

        return mark_safe("".join(html_parts))

    @admin.display(description="Sentry Trace")
    def formatted_sentry_trace_id(self, obj: UploadBreadcrumb) -> str:
        """Display Sentry trace ID as a hyperlink for list view."""
        if not obj.sentry_trace_id:
            return "-"

        short_trace = (
            obj.sentry_trace_id[:8] + "..."
            if len(obj.sentry_trace_id) > 8
            else obj.sentry_trace_id
        )

        sentry_url = (
            f"{settings.SENTRY_ORG_URL}/explore/traces/trace/{obj.sentry_trace_id}"
        )

        return format_html(
            '<a href="{}" target="_blank" title="{}">{}</a>',
            sentry_url,
            obj.sentry_trace_id,
            short_trace,
        )

    @admin.display(description="Sentry Trace")
    def formatted_sentry_trace_id_detail(self, obj: UploadBreadcrumb) -> str:
        """Display detailed Sentry trace ID with full link for individual item view."""
        if not obj.sentry_trace_id:
            return "-"

        sentry_url = (
            f"{settings.SENTRY_ORG_URL}/explore/traces/trace/{obj.sentry_trace_id}"
        )

        html_parts = []
        html_parts.append(
            '<div style="font-family: monospace; padding: 15px; border-radius: 5px; border: 1px solid var(--hairline-color, #e1e4e8);">'
        )
        html_parts.append(
            f'<div style="margin-bottom: 10px;"><strong>Trace ID:</strong> {obj.sentry_trace_id}</div>'
        )
        html_parts.append(
            f'<div><strong>Sentry Link:</strong> <a href="{sentry_url}" target="_blank">{sentry_url}</a></div>'
        )
        html_parts.append("</div>")

        return mark_safe("".join(html_parts))

    @admin.display(description="Log Links")
    def log_links(self, obj: UploadBreadcrumb) -> str:
        html_parts = []
        html_parts.append("<div>")

        if get_config(
            "setup", "upload_breadcrumbs", "gcp_log_links_enabled", default=False
        ):
            html_parts.extend(self._gcp_log_links(obj))

        html_parts.append("</div>")

        return mark_safe("".join(html_parts)) if len(html_parts) > 2 else "-"

    def _gcp_log_links(self, obj: UploadBreadcrumb) -> list[str]:
        logs_base_url = "https://console.cloud.google.com/logs/query"

        html_parts = []
        if obj.commit_sha:
            url = f"{logs_base_url};query=resource.type%3D%22k8s_container%22%0ASEARCH%2528%22%60{obj.commit_sha}%60%22%2529;duration=P2D"
            html_parts.append(
                f'<div>• <a href="{url}" target="_blank">GCP Commit SHA Logs</a></div>'
            )

        if obj.sentry_trace_id:
            url = f"{logs_base_url};query=resource.type%3D%22k8s_container%22%0ASEARCH%2528%22%60{obj.sentry_trace_id}%60%22%2529;duration=P2D"
            html_parts.append(
                f'<div>• <a href="{url}" target="_blank">GCP Sentry Trace Logs</a></div>'
            )

        return html_parts

    def changelist_view(
        self, request: HttpRequest, extra_context: dict | None = None
    ) -> Any:
        """Override to add info dialog explaining breadcrumbs."""
        extra_context = extra_context or {}

        milestone_list = "".join(
            [f"<li>{milestone.label}</li>" for milestone in Milestones]
        )

        breadcrumb_info = f"""
        <div style="padding-bottom: 20px;">
            <details>
                <summary style="cursor: pointer; font-weight: bold; margin-bottom: 10px;">
                    Upload Breadcrumbs Information (click to expand)
                </summary>
                <div style="margin-left: 20px;">
                    Upload breadcrumbs track the progress of coverage uploads throughout every stage of the upload process.
                    Each breadcrumb represents a step in the upload process and may include:
                    <ul style="margin: 10px 0 10px 20px;">
                        <li><strong>Milestone:</strong> Current stage of the upload. The possible milestones are as follows and should appear for a given upload in this order (although slight variations may occur due to breadcrumbs being saved asynchronously):
                            <ol style="margin: 5px 0 5px 20px;">
                                {milestone_list}
                            </ol>
                            Note that if "{Milestones.NOTIFICATIONS_TRIGGERED.label}" is not present, then "{Milestones.NOTIFICATIONS_SENT.label}" will also not be present. Outside of this, all coverage uploads should have every milestone.
                        </li>
                        <li><strong>Endpoint:</strong> API endpoint that triggered this breadcrumb. This is helpful to determine if there is an issue related to a specific endpoint.</li>
                        <li><strong>Error:</strong> Any errors encountered during processing. This will either be a pre-defined error or "Unknown" for anything else. Not every error is indicative of total failure (such as retries), but they give insight into potential issues.</li>
                        <li><strong>Error Text:</strong> If the error was not a known error, additional context will be provided here.</li>
                        <li><strong>Upload IDs:</strong> Associated upload identifiers generated from worker. These indicate how an upload gets batched and processed with other uploads.</li>
                        <li><strong>Sentry Trace:</strong> Trace ID for error tracking and debugging.</li>
                    </ul>
                    You can use the search bar and filters below to find specific breadcrumbs or uploads. Click on the ID of each to get more details and see relevant external links.
                </div>
            </details>
        </div>
        """

        extra_context["breadcrumb_info"] = mark_safe(breadcrumb_info)
        return super().changelist_view(request, extra_context)

    def has_delete_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_add_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj: Any = None) -> bool:
        return False
