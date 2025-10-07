import json
import logging
import re
from collections.abc import Iterator
from typing import Any

from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.views.main import ChangeList
from django.db.models import Q, QuerySet
from django.http import HttpRequest
from django.shortcuts import redirect
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html
from django.utils.safestring import mark_safe
from orjson import dumps

from shared.config import get_config
from shared.django_apps.core.models import Commit, Repository
from shared.django_apps.reports.models import ReportSession
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)
from shared.django_apps.utils.paginator import EstimatedCountPaginator
from shared.helpers.redis import get_redis_connection

# Import TaskService conditionally - it may not be available in all contexts
try:
    from services.task import TaskService
except ImportError:
    TaskService = None

# Regex pattern for hexadecimal string validation
HEX_PATTERN = re.compile(r"^[0-9a-fA-F]+$")

# Get logger for this module
log = logging.getLogger(__name__)


class PresentDataFilter(admin.SimpleListFilter):
    title = "Present Data"
    parameter_name = "present_data"

    def lookups(
        self, request: HttpRequest, model_admin: admin.ModelAdmin
    ) -> list[tuple[str, str]]:
        return [
            ("has_milestone", "Has Milestone"),
            ("has_endpoint", "Has Endpoint"),
            ("has_uploader", "Has Uploader"),
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
            elif filter_type == "has_uploader":
                queryset = queryset.filter(breadcrumb_data__uploader__isnull=False)
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
        "created_at",
        "id",
        "repo_id",
        "formatted_commit_sha",
        "formatted_breadcrumb_data",
        "formatted_upload_ids",
        "formatted_sentry_trace_id",
        "resend_upload_button",
    )
    list_display_links = ("created_at", "id")
    sortable_by = (
        "repo_id",
        "formatted_commit_sha",
    )  # Limit sorting options to indexed columns
    show_full_result_count = False
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
        "resend_upload_action",
    )
    list_per_page = 50
    list_max_show_all = 200
    show_full_result_count = False  # Disable full result count for performance
    paginator = EstimatedCountPaginator
    search_fields = ("repo_id", "commit_sha", "sentry_trace_id")
    search_help_text = "Search by repository ID, commit SHA, and/or Sentry trace ID (all exact match). Separate multiple values with spaces to AND search (E.g. '<repo_id> <commit_sha>')."

    retriable_errors = [
        Errors.FILE_NOT_IN_STORAGE.value,
        Errors.REPORT_EXPIRED.value,
        Errors.REPORT_EMPTY.value,
        Errors.TASK_TIMED_OUT.value,
        Errors.UNSUPPORTED_FORMAT.value,
        Errors.UNKNOWN.value,
    ]

    def get_search_results(
        self, request: HttpRequest, queryset: QuerySet, search_term: str
    ) -> tuple[QuerySet, bool]:
        """
        Custom search logic to determine field types and use exact matches for
        query performance.

        This allows us to take advantage of the indexes defined on the model
        by only searching the appropriate field based on the search term type.
        """
        search_terms = search_term.strip().split()
        if not search_terms:
            return queryset, False

        q_objects = []

        for term in search_terms:
            term_q = Q()

            # Looks like a repo_id (integer)
            if term.isdigit():
                repo_id = int(term)
                term_q |= Q(repo_id=repo_id)

            if HEX_PATTERN.match(term):
                # Looks like a commit SHA (40-character hex string)
                if len(term) == 40:
                    term_q |= Q(commit_sha__exact=term)
                # Looks like a sentry_trace_id (32-character hex string)
                elif len(term) == 32:
                    term_q |= Q(sentry_trace_id__exact=term)

            if not term_q:
                # If it doesn't match expected patterns, return no results
                return queryset.none(), False

            q_objects.append(term_q)

        queryset = queryset.filter(*q_objects)
        return queryset, True

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

        if data.get("uploader"):
            uploader_label = data["uploader"]
            parts.append(f"⬆️ {uploader_label}")

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

        if data.get("uploader"):
            uploader_label = data["uploader"]
            html_parts.append(
                f"<div><strong>⬆️ Uploader:</strong> {uploader_label}</div>"
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
                        <li><strong>Uploader:</strong> Uploader tool (e.g. codecov-cli) that made the request.</li>
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

    def get_urls(self):
        """Add custom URLs for the resend upload functionality."""
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/resend-upload/",
                self.admin_site.admin_view(self.resend_upload_view),
                name="upload_breadcrumbs_uploadbreadcrumb_resend_upload",
            ),
        ]
        return custom_urls + urls

    @admin.display(description="Actions", ordering=None)
    def resend_upload_button(self, obj: UploadBreadcrumb) -> str:
        """Display resend button in the list view for failed uploads."""
        if not self._is_failed_upload(obj):
            return "-"

        resend_url = reverse(
            "admin:upload_breadcrumbs_uploadbreadcrumb_resend_upload", args=[obj.id]
        )
        return format_html(
            '<a class="button" href="{}" onclick="return confirm(\'Are you sure you want to resend this upload?\')">🔄 Resend</a>',
            resend_url,
        )

    @admin.display(description="Resend Upload")
    def resend_upload_action(self, obj: UploadBreadcrumb) -> str:
        """Display resend actions in the detail view."""
        if not obj.pk:  # New object
            return "-"

        html_parts = []

        if self._is_failed_upload(obj):
            resend_url = reverse(
                "admin:upload_breadcrumbs_uploadbreadcrumb_resend_upload", args=[obj.id]
            )
            html_parts.append(
                f'<a class="button default" href="{resend_url}" '
                f"onclick=\"return confirm('Are you sure you want to resend this upload for commit {obj.commit_sha[:7]}?')\">🔄 Resend Upload</a>"
            )
            html_parts.append("<br><br>")
            html_parts.append(
                "<div><strong>⚠️ Note:</strong> This will create a new upload task for the same commit and repository. "
                "The original upload data may no longer be available in storage.</div>"
            )
        else:
            html_parts.append(
                "<div>✅ This upload does not appear to have failed. Resend option is not available.</div>"
            )

        return format_html("".join(html_parts))

    def _is_failed_upload(self, obj: UploadBreadcrumb) -> bool:
        """Check if this breadcrumb represents a failed upload."""
        if not obj.breadcrumb_data:
            return False

        data = obj.breadcrumb_data
        log.info(
            f"Data: {data}, Upload IDs: {obj.upload_ids}, Milestone: {data.get('milestone')}"
        )
        # Check if there's an error
        if (
            (
                data.get("milestone") == Milestones.PROCESSING_UPLOAD.value
                or data.get("milestone") == Milestones.COMPILING_UPLOADS.value
            )
            and obj.upload_ids
            and data.get("error")
        ):
            return data["error"] in self.retriable_errors

        return False

    def resend_upload_view(self, request, object_id):
        """Handle the resend upload request."""
        try:
            breadcrumb = self.get_object(request, object_id)
            if not breadcrumb:
                messages.error(request, "Upload breadcrumb not found.")
                return redirect("admin:upload_breadcrumbs_uploadbreadcrumb_changelist")

            if not self._is_failed_upload(breadcrumb):
                messages.error(request, "This upload does not appear to have failed.")
                return redirect("admin:upload_breadcrumbs_uploadbreadcrumb_changelist")

            # Trigger the resend
            success = self._resend_upload(breadcrumb, request.user)

            if success:
                upload_count = (
                    len(breadcrumb.upload_ids) if breadcrumb.upload_ids else 0
                )
                messages.success(
                    request,
                    f"Upload resend triggered successfully for commit {breadcrumb.commit_sha[:7]} "
                    f"({upload_count} upload{'s' if upload_count != 1 else ''}). "
                    f"Check the upload breadcrumbs for progress updates.",
                )
            else:
                messages.error(
                    request,
                    f"Failed to resend upload for commit {breadcrumb.commit_sha[:7]}. "
                    f"Please check the logs for more details.",
                )

        except Exception as e:
            messages.error(request, f"Error resending upload: {str(e)}")

        return redirect("admin:upload_breadcrumbs_uploadbreadcrumb_changelist")

    def _resend_upload(self, breadcrumb: UploadBreadcrumb, user) -> bool:
        """Actually trigger the upload resend."""
        # Create a TaskService instance and trigger a new upload task
        if TaskService:
            task_service = TaskService()
        else:
            log.error("TaskService not available - cannot resend upload")
            return False

        log.info(
            "Starting resend upload process",
            extra={
                "breadcrumb_id": breadcrumb.id,
                "commit_sha": breadcrumb.commit_sha,
                "repo_id": breadcrumb.repo_id,
            },
        )

        # Check if commit exists in database - this is required for upload task to run
        try:
            repository = Repository.objects.get(repoid=breadcrumb.repo_id)
            log.info(f"Found repository: {repository.name}")
        except Repository.DoesNotExist:
            log.error(
                "Repository not found in database - cannot resend upload",
                extra={
                    "repo_id": breadcrumb.repo_id,
                    "breadcrumb_id": breadcrumb.id,
                },
            )
            return False

        try:
            commit = Commit.objects.get(
                repository=breadcrumb.repo_id, commitid=breadcrumb.commit_sha
            )
            log.info(f"Found existing commit: {commit.commitid}")
        except Commit.DoesNotExist:
            log.error(
                "Commit not found in database - cannot resend upload",
                extra={
                    "repo_id": breadcrumb.repo_id,
                    "breadcrumb_id": breadcrumb.id,
                },
            )
            return False

        # Test Redis connection first
        try:
            redis = get_redis_connection()
            redis.ping()  # Test the connection
            log.info("Redis connection successful")
        except Exception as redis_error:
            log.error(
                "Failed to connect to Redis",
                extra={
                    "error": str(redis_error),
                    "breadcrumb_id": breadcrumb.id,
                },
            )
            return False

        # Try to get original upload data for all upload_ids
        all_upload_arguments = []
        resend_metadata = {
            "resend_via_admin": True,  # Mark this as an admin resend
            "resend_by": str(user.username) if hasattr(user, "username") else str(user),
            "resend_timestamp": timezone.now().isoformat(),
        }

        log.info(
            f"Collecting upload data for {len(breadcrumb.upload_ids)} uploads",
            extra={"upload_ids": breadcrumb.upload_ids},
        )

        # Guarunteed to have an upload_id from _is_failed_upload function
        # Fetch all uploads at once to avoid N+1 query problem
        uploads = ReportSession.objects.filter(
            id__in=breadcrumb.upload_ids
        ).prefetch_related("flags")

        # Create a mapping for easy lookup
        uploads_by_id = {upload.id: upload for upload in uploads}

        for upload_id in breadcrumb.upload_ids:
            upload = uploads_by_id.get(upload_id)
            if upload:
                log.info(
                    f"Found original upload: {upload.id} with storage_path: {upload.storage_path}"
                )
                upload_arguments = {
                    "commit": breadcrumb.commit_sha,
                    "upload_id": upload.id,  # CRITICAL: Include the upload_id so it reuses the existing upload
                    "reportid": str(upload.external_id) if upload.external_id else None,
                    "url": upload.storage_path,
                    "build": upload.build_code,
                    "build_url": upload.build_url,
                    "job": upload.job_code,
                    "service": upload.provider,
                    "flags": [flag.flag_name for flag in upload.flags.all()]
                    if upload.flags
                    else [],
                    **resend_metadata,  # Include resend metadata in each upload
                }
                all_upload_arguments.append(upload_arguments)
                log.info(
                    f"Retrieved upload data for upload_id {upload_id}: {upload_arguments}"
                )
            else:
                log.warning(f"Upload with ID {upload_id} not found")

        if not all_upload_arguments:
            log.error("No valid upload data found to resend")
            return False

        log.info(f"Collected {len(all_upload_arguments)} uploads to resend")

        # Store all upload arguments in Redis
        cache_uploads_eta = get_config(("setup", "cache", "uploads"), default=86400)
        report_type = "coverage"  # Default to coverage for now

        repo_queue_key = f"uploads/{breadcrumb.repo_id}/{breadcrumb.commit_sha}"
        latest_upload_key = (
            f"latest_upload/{breadcrumb.repo_id}/{breadcrumb.commit_sha}"
        )

        log.info(
            "Storing all upload arguments in Redis",
            extra={
                "repo_queue_key": repo_queue_key,
                "latest_upload_key": latest_upload_key,
                "num_uploads": len(all_upload_arguments),
            },
        )

        try:
            with redis.pipeline() as pipeline:
                # Push each upload's arguments to Redis
                for upload_args in all_upload_arguments:
                    pipeline.rpush(repo_queue_key, dumps(upload_args))

                pipeline.expire(
                    repo_queue_key,
                    cache_uploads_eta if cache_uploads_eta is not True else 86400,
                )
                pipeline.setex(latest_upload_key, 3600, timezone.now().timestamp())
                result = pipeline.execute()
                log.info(
                    f"Redis pipeline executed successfully: pushed {len(all_upload_arguments)} uploads",
                    extra={"pipeline_result": result},
                )
        except Exception as redis_store_error:
            log.error(
                "Failed to store upload arguments in Redis",
                extra={
                    "error": str(redis_store_error),
                    "repo_queue_key": repo_queue_key,
                    "num_uploads": len(all_upload_arguments),
                },
            )
            return False

        # Now dispatch the upload task
        log.info(
            f"Dispatching single upload task for {len(all_upload_arguments)} uploads via TaskService"
        )

        try:
            # Use the same countdown logic as the original dispatch_upload_task
            countdown = max(
                4, int(get_config("setup", "upload_processing_delay", default=0))
            )

            log.info(
                f"Calling TaskService.upload with countdown={countdown}",
                extra={"num_uploads": len(all_upload_arguments)},
            )
            task_service.upload(
                repoid=breadcrumb.repo_id,
                commitid=breadcrumb.commit_sha,
                report_type=report_type,
                arguments={},  # Arguments are in Redis, this param is not actually used
                countdown=countdown,
            )
            log.info(
                f"TaskService.upload() called successfully for {len(all_upload_arguments)} uploads"
            )
        except Exception as task_error:
            log.error(
                "Failed to dispatch upload task - likely Celery broker connection issue",
                extra={
                    "error": str(task_error),
                    "error_type": type(task_error).__name__,
                    "breadcrumb_id": breadcrumb.id,
                },
                exc_info=True,
            )
            return False

        # Create a breadcrumb to track the resend initiation
        try:
            resend_breadcrumb_data = BreadcrumbData(
                endpoint=Endpoints.CREATE_COMMIT,  # Use a generic endpoint
            )

            UploadBreadcrumb.objects.create(
                commit_sha=breadcrumb.commit_sha,
                repo_id=breadcrumb.repo_id,
                upload_ids=None,  # Will be populated when the task runs
                sentry_trace_id=None,
                breadcrumb_data=resend_breadcrumb_data.model_dump(),
            )
            log.info("Created resend initiation breadcrumb")
        except Exception as breadcrumb_error:
            log.error(
                "Failed to create resend breadcrumb, but task was dispatched successfully",
                extra={
                    "error": str(breadcrumb_error),
                    "error_type": type(breadcrumb_error).__name__,
                },
                exc_info=True,
            )

        log.info(
            "Successfully dispatched resend upload task",
            extra={
                "breadcrumb_id": breadcrumb.id,
                "commit_sha": breadcrumb.commit_sha,
                "repo_id": breadcrumb.repo_id,
                "num_uploads": len(all_upload_arguments),
                "resend_by": str(user),
            },
        )

        return True
