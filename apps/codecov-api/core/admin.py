from collections.abc import Callable
from dataclasses import dataclass

from django import forms
from django.contrib import admin, messages
from django.db.models import Q, QuerySet
from django.http import HttpRequest, HttpResponseRedirect
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html

from codecov.admin import AdminMixin
from codecov_auth.models import RepositoryToken
from core.models import Commit, Pull, Repository
from reports.models import CommitReport, ReportSession
from services.task.task import TaskService
from shared.celery_config import manual_upload_completion_trigger_task_name
from shared.django_apps.reports.models import ReportType
from shared.django_apps.ta_timeseries.models import Testrun
from shared.django_apps.timeseries.models import Measurement, MeasurementName
from shared.django_apps.upload_breadcrumbs.models import UploadBreadcrumb
from shared.django_apps.utils.paginator import EstimatedCountPaginator
from shared.helpers.redis import get_redis_connection
from shared.reports.enums import UploadState
from shared.yaml import UserYaml
from shared.yaml.user_yaml import OwnerContext
from upload.helpers import dispatch_upload_task


@dataclass
class ReprocessConfig:
    """Configuration for reprocessing a specific report type."""

    display_name: str
    report_type_filter: Q
    dispatch_report_type: CommitReport.ReportType
    cleanup_fn: Callable[[Commit, list[int]], tuple[int, str]] | None = None


def _cleanup_test_analytics(commit: Commit, upload_ids: list[int]) -> tuple[int, str]:
    """Delete existing test run data for uploads to avoid duplicates."""
    deleted_count, _ = (
        Testrun.objects.using("ta_timeseries").filter(upload_id__in=upload_ids).delete()
    )
    return deleted_count, "test runs"


def _cleanup_bundle_analysis(commit: Commit, upload_ids: list[int]) -> tuple[int, str]:
    """Delete existing bundle analysis measurement data to avoid duplicates."""
    bundle_analysis_measurement_names = [
        MeasurementName.BUNDLE_ANALYSIS_REPORT_SIZE.value,
        MeasurementName.BUNDLE_ANALYSIS_JAVASCRIPT_SIZE.value,
        MeasurementName.BUNDLE_ANALYSIS_STYLESHEET_SIZE.value,
        MeasurementName.BUNDLE_ANALYSIS_FONT_SIZE.value,
        MeasurementName.BUNDLE_ANALYSIS_IMAGE_SIZE.value,
        MeasurementName.BUNDLE_ANALYSIS_ASSET_SIZE.value,
    ]
    deleted_count, _ = (
        Measurement.objects.using("timeseries")
        .filter(
            repo_id=commit.repository.repoid,
            commit_sha=commit.commitid,
            name__in=bundle_analysis_measurement_names,
        )
        .delete()
    )
    return deleted_count, "bundle analysis measurements"


REPROCESS_CONFIGS = {
    "coverage": ReprocessConfig(
        display_name="coverage",
        report_type_filter=Q(report_type__in=[None, ReportType.COVERAGE]),
        dispatch_report_type=CommitReport.ReportType.COVERAGE,
    ),
    "test_analytics": ReprocessConfig(
        display_name="test analytics",
        report_type_filter=Q(report_type=ReportType.TEST_RESULTS),
        dispatch_report_type=CommitReport.ReportType.TEST_RESULTS,
        cleanup_fn=_cleanup_test_analytics,
    ),
    "bundle_analysis": ReprocessConfig(
        display_name="bundle analysis",
        report_type_filter=Q(report_type=ReportType.BUNDLE_ANALYSIS),
        dispatch_report_type=CommitReport.ReportType.BUNDLE_ANALYSIS,
        cleanup_fn=_cleanup_bundle_analysis,
    ),
}


def get_repo_yaml(repository):
    """
    Get the final YAML configuration for a repository.
    This merges owner-level and repo-level YAML configurations.
    """
    context = OwnerContext(
        owner_onboarding_date=repository.author.createstamp,
        owner_plan=repository.author.plan,
        ownerid=repository.author.ownerid,
    )
    return UserYaml.get_final_yaml(
        owner_yaml=repository.author.yaml,
        repo_yaml=repository.yaml,
        owner_context=context,
    )


class RepositoryTokenInline(admin.TabularInline):
    model = RepositoryToken
    readonly_fields = ["key"]

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    class Meta:
        readonly_fields = ("key",)


class RepositoryAdminForm(forms.ModelForm):
    # the model field has null=True but not blank=True, so we have to add a workaround
    # to be able to clear out this field through the django admin
    webhook_secret = forms.CharField(required=False, empty_value=None)
    yaml = forms.JSONField(required=False)
    using_integration = forms.BooleanField(required=False)
    hookid = forms.CharField(required=False, empty_value=None)

    class Meta:
        model = Repository
        fields = "__all__"


@admin.register(Repository)
class RepositoryAdmin(AdminMixin, admin.ModelAdmin):
    inlines = [RepositoryTokenInline]
    list_display = ("name", "service_id", "author")
    search_fields = (
        "name",
        "author__username__exact",
        "service_id__exact",
    )
    show_full_result_count = False
    autocomplete_fields = ("bot",)
    form = RepositoryAdminForm

    paginator = EstimatedCountPaginator

    readonly_fields = (
        "name",
        "author",
        "service_id",
        "updatestamp",
        "active",
        "language",
        "fork",
        "upload_token",
        "yaml",
        "image_token",
        "hookid",
        "activated",
        "deleted",
    )
    fields = readonly_fields + (
        "bot",
        "using_integration",
        "branch",
        "private",
        "webhook_secret",
    )

    def get_search_results(
        self,
        request: HttpRequest,
        queryset: QuerySet[Repository],
        search_term: str,
    ) -> tuple[QuerySet[Repository], bool]:
        """
        Search for repositories by name, service_id, author username, or repoid.
        https://docs.djangoproject.com/en/5.2/ref/contrib/admin/#django.contrib.admin.ModelAdmin.get_search_results
        """
        # Default search is by name, author username, and service_id (defined in `search_fields`)
        queryset, may_have_duplicates = super().get_search_results(
            request,
            queryset,
            search_term,
        )
        # Also search by repoid if the search term is numeric
        try:
            search_term_as_int = int(search_term)
        except ValueError:
            pass
        else:
            queryset |= self.model.objects.filter(repoid=search_term_as_int)
        # avoid N+1 queries for with union
        queryset = queryset.select_related("author")
        return queryset, may_have_duplicates

    def has_add_permission(self, _, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return bool(request.user and request.user.is_superuser)

    def delete_queryset(self, request, queryset) -> None:
        for repo in queryset:
            TaskService().flush_repo(repository_id=repo.repoid)

    def delete_model(self, request, obj) -> None:
        TaskService().flush_repo(repository_id=obj.repoid)

    def get_deleted_objects(self, objs, request):
        return [], {}, set(), []


@admin.register(Pull)
class PullsAdmin(AdminMixin, admin.ModelAdmin):
    list_display = ("pullid", "repository", "author")
    show_full_result_count = False
    paginator = EstimatedCountPaginator
    readonly_fields = (
        "repository",
        "id",
        "pullid",
        "issueid",
        "title",
        "base",
        "head",
        "user_provided_base_sha",
        "compared_to",
        "commentid",
        "author",
        "updatestamp",
        "diff",
        "flare",
    )
    fields = readonly_fields + ("state",)

    @admin.display(description="flare")
    def flare(self, instance):
        return instance.flare

    def has_delete_permission(self, request, obj=None):
        return False

    def has_add_permission(self, _, obj=None):
        return False


@admin.register(Commit)
class CommitAdmin(AdminMixin, admin.ModelAdmin):
    list_display = ("short_commitid", "repository", "branch", "state", "timestamp")
    list_filter = ("state", "timestamp")
    search_fields = ("commitid__startswith", "repository__name", "message")
    readonly_fields = (
        "id",
        "commitid",
        "repository",
        "author",
        "timestamp",
        "updatestamp",
        "branch",
        "pullid",
        "message",
        "parent_commit_id",
        "state",
        "ci_passed",
        "totals",
        "merged",
        "deleted",
        "notified",
        "reprocess_actions",
        "upload_pipeline_timeline",
    )
    fields = readonly_fields
    paginator = EstimatedCountPaginator
    show_full_result_count = False

    # Show short commit SHA in list view
    @admin.display(description="Commit SHA")
    def short_commitid(self, obj):
        return obj.commitid[:7] if obj.commitid else ""

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "<path:object_id>/reprocess_coverage/",
                self.admin_site.admin_view(self.reprocess_coverage_view),
                name="core_commit_reprocess_coverage",
            ),
            path(
                "<path:object_id>/reprocess_test_analytics/",
                self.admin_site.admin_view(self.reprocess_test_analytics_view),
                name="core_commit_reprocess_test_analytics",
            ),
            path(
                "<path:object_id>/reprocess_bundle_analysis/",
                self.admin_site.admin_view(self.reprocess_bundle_analysis_view),
                name="core_commit_reprocess_bundle_analysis",
            ),
            path(
                "<path:object_id>/trigger_notifications/",
                self.admin_site.admin_view(self.trigger_notifications_view),
                name="core_commit_trigger_notifications",
            ),
        ]
        return custom_urls + urls

    @admin.display(description="Reprocess Actions")
    def reprocess_actions(self, obj):
        if obj.pk is None:
            return ""

        # Check what actions are available for this commit
        has_coverage = CommitReport.objects.filter(
            commit=obj, report_type__in=[None, ReportType.COVERAGE]
        ).exists()
        has_test_analytics = CommitReport.objects.filter(
            commit=obj, report_type=ReportType.TEST_RESULTS
        ).exists()
        has_bundle_analysis = CommitReport.objects.filter(
            commit=obj, report_type=ReportType.BUNDLE_ANALYSIS
        ).exists()

        buttons = []

        if has_coverage:
            url = reverse("admin:core_commit_reprocess_coverage", args=[obj.pk])
            buttons.append(
                f'<a class="button" href="{url}" style="margin: 5px;">Reprocess Coverage</a>'
            )

        if has_test_analytics:
            url = reverse("admin:core_commit_reprocess_test_analytics", args=[obj.pk])
            buttons.append(
                f'<a class="button" href="{url}" style="margin: 5px;">Reprocess Test Analytics</a>'
            )

        if has_bundle_analysis:
            url = reverse("admin:core_commit_reprocess_bundle_analysis", args=[obj.pk])
            buttons.append(
                f'<a class="button" href="{url}" style="margin: 5px;">Reprocess Bundle Analysis</a>'
            )

        # Always show notifications trigger
        url = reverse("admin:core_commit_trigger_notifications", args=[obj.pk])
        buttons.append(
            f'<a class="button" href="{url}" style="margin: 5px;">Trigger Notifications</a>'
        )

        if not buttons:
            return "No reprocessing actions available"

        return format_html("<div>{}</div>", format_html("".join(buttons)))

    @admin.display(description="Upload Pipeline Timeline")
    def upload_pipeline_timeline(self, obj):
        if obj.pk is None:
            return ""

        breadcrumbs = UploadBreadcrumb.objects.filter(
            commit_sha=obj.commitid,
            repo_id=obj.repository.repoid,
        ).order_by("created_at")[:200]

        if not breadcrumbs:
            return format_html("<em>No breadcrumbs recorded for this commit.</em>")

        rows = []
        for bc in breadcrumbs:
            data = bc.breadcrumb_data or {}
            milestone = data.get("milestone", "")
            error = data.get("error", "")
            error_text = data.get("error_text", "")
            task_name = data.get("task_name", "")
            parent_task_id = data.get("parent_task_id", "")
            endpoint = data.get("endpoint", "")

            if error:
                color = "#d32f2f"
            elif milestone in ("lac", "lr", "uc", "ns"):
                color = "#388e3c"
            elif milestone in ("la", "pu", "nt"):
                color = "#1565c0"
            else:
                color = "#555"

            upload_ids_str = (
                ", ".join(str(uid) for uid in bc.upload_ids) if bc.upload_ids else ""
            )

            detail_parts = []
            if milestone:
                detail_parts.append(f"<strong>{milestone}</strong>")
            if endpoint:
                detail_parts.append(f"endpoint={endpoint}")
            if error:
                detail_parts.append(f'<span style="color:#d32f2f">err={error}</span>')
            if error_text:
                detail_parts.append(
                    f'<span style="color:#d32f2f">{error_text[:120]}</span>'
                )
            detail = " &middot; ".join(detail_parts) if detail_parts else "—"

            rows.append(
                f"<tr>"
                f'<td style="white-space:nowrap;color:{color};padding:4px 8px">'
                f"{bc.created_at:%Y-%m-%d %H:%M:%S}</td>"
                f'<td style="padding:4px 8px;font-family:monospace;font-size:12px">'
                f"{task_name}</td>"
                f'<td style="padding:4px 8px;font-family:monospace;font-size:12px">'
                f"{parent_task_id}</td>"
                f'<td style="padding:4px 8px">{detail}</td>'
                f'<td style="padding:4px 8px;font-family:monospace;font-size:12px">'
                f"{upload_ids_str}</td>"
                f"</tr>"
            )

        table = (
            '<table style="border-collapse:collapse;width:100%">'
            "<thead><tr>"
            '<th style="text-align:left;padding:4px 8px;border-bottom:2px solid #ccc">Time</th>'
            '<th style="text-align:left;padding:4px 8px;border-bottom:2px solid #ccc">Task</th>'
            '<th style="text-align:left;padding:4px 8px;border-bottom:2px solid #ccc">Parent Task ID</th>'
            '<th style="text-align:left;padding:4px 8px;border-bottom:2px solid #ccc">Detail</th>'
            '<th style="text-align:left;padding:4px 8px;border-bottom:2px solid #ccc">Upload IDs</th>'
            "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
        )

        return format_html(
            '<div style="max-height:600px;overflow:auto">{}</div>', format_html(table)
        )

    def _reprocess_uploads(
        self, request, commit: Commit, config: ReprocessConfig
    ) -> HttpResponseRedirect:
        """
        Generic helper to reprocess uploads for a given report type.

        Args:
            request: The HTTP request
            commit: The commit to reprocess uploads for
            config: Configuration specifying report type, cleanup, and dispatch details
        """
        display_name = config.display_name

        # Query for reports of the specified type
        reports = CommitReport.objects.filter(config.report_type_filter, commit=commit)

        if not reports.exists():
            self.message_user(
                request,
                f"This commit has no {display_name} data to reprocess",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(
                reverse("admin:core_commit_change", args=[commit.pk])
            )

        # Get all uploads (ReportSession) for reports with their flags
        uploads = (
            ReportSession.objects.filter(report__in=reports)
            .prefetch_related("flags")
            .select_related("report")
        )

        if not uploads.exists():
            self.message_user(
                request,
                f"This commit has no {display_name} uploads to reprocess",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(
                reverse("admin:core_commit_change", args=[commit.pk])
            )

        # Only include uploads that have a storage_path (raw data location)
        # Without storage_path, we cannot re-download and reprocess the upload
        reprocessable_uploads = [u for u in uploads if u.storage_path]
        skipped_uploads = [u for u in uploads if not u.storage_path]

        if skipped_uploads:
            self.message_user(
                request,
                f"Skipping {len(skipped_uploads)} uploads without storage path",
                level=messages.WARNING,
            )

        if not reprocessable_uploads:
            self.message_user(
                request,
                "No uploads with storage paths found to reprocess",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(
                reverse("admin:core_commit_change", args=[commit.pk])
            )

        upload_ids = [u.id for u in reprocessable_uploads]

        # Run optional cleanup to delete existing data before reprocessing
        if config.cleanup_fn:
            deleted_count, deleted_type = config.cleanup_fn(commit, upload_ids)
            if deleted_count > 0:
                self.message_user(
                    request,
                    f"Deleted {deleted_count} existing {deleted_type} for reprocessing",
                    level=messages.INFO,
                )

        # Reset upload states to UPLOADED so the processor won't skip them
        ReportSession.objects.filter(id__in=upload_ids).update(
            state="started",
            state_id=UploadState.UPLOADED.db_id,
        )

        # Build upload arguments for each upload
        reprocess_metadata = {
            "reprocess_via_admin": True,
            "reprocess_by": str(request.user.username)
            if hasattr(request.user, "username")
            else str(request.user),
            "reprocess_timestamp": timezone.now().isoformat(),
        }

        redis = get_redis_connection()

        for upload in reprocessable_uploads:
            upload_arguments = {
                "commit": commit.commitid,
                "upload_id": upload.id,
                "reportid": str(upload.external_id) if upload.external_id else None,
                "url": upload.storage_path,
                "build": upload.build_code,
                "build_url": upload.build_url,
                "job": upload.job_code,
                "service": upload.provider,
                "flags": [flag.flag_name for flag in upload.flags.all()]
                if upload.flags
                else [],
                **reprocess_metadata,
            }
            dispatch_upload_task(
                redis=redis,
                repoid=commit.repository.repoid,
                task_arguments=upload_arguments,
                report_type=config.dispatch_report_type,
            )

        self.message_user(
            request,
            f"Successfully queued {display_name} reprocessing for commit {commit.commitid[:7]} ({len(reprocessable_uploads)} uploads)",
            level=messages.SUCCESS,
        )

        return HttpResponseRedirect(
            reverse("admin:core_commit_change", args=[commit.pk])
        )

    def reprocess_coverage_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))
        return self._reprocess_uploads(request, commit, REPROCESS_CONFIGS["coverage"])

    def reprocess_test_analytics_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))
        return self._reprocess_uploads(
            request, commit, REPROCESS_CONFIGS["test_analytics"]
        )

    def reprocess_bundle_analysis_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))
        return self._reprocess_uploads(
            request, commit, REPROCESS_CONFIGS["bundle_analysis"]
        )

    def trigger_notifications_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))

        commit_yaml = get_repo_yaml(commit.repository)
        commit_yaml_dict = commit_yaml.to_dict() if commit_yaml else None

        task_service = TaskService()
        task_service.schedule_task(
            manual_upload_completion_trigger_task_name,
            kwargs={
                "repoid": commit.repository.repoid,
                "commitid": commit.commitid,
                "current_yaml": commit_yaml_dict,
            },
            apply_async_kwargs={},
        )
        self.message_user(
            request,
            f"Successfully queued notification trigger for commit {commit.commitid[:7]}",
            level=messages.SUCCESS,
        )

        return HttpResponseRedirect(
            reverse("admin:core_commit_change", args=[commit.pk])
        )
