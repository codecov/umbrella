from collections.abc import Callable
from dataclasses import dataclass

from django import forms
from django.contrib import admin, messages
from django.db.models import Count, F, Q, QuerySet
from django.http import HttpRequest, HttpResponseRedirect
from django.urls import path, reverse
from django.utils import timezone
from django.utils.html import format_html, format_html_join

from codecov.admin import AdminMixin, deny_viewers, is_viewer
from codecov_auth.models import Owner, RepositoryToken
from core.models import Commit, CommitNotification, Pull, Repository
from reports.models import CommitReport, ReportResults, ReportSession
from services.task.task import TaskService
from shared.celery_config import manual_upload_completion_trigger_task_name
from shared.django_apps.reports.models import ReportType
from shared.django_apps.ta_timeseries.models import Testrun
from shared.django_apps.timeseries.models import Measurement, MeasurementName
from shared.django_apps.utils.paginator import EstimatedCountPaginator
from shared.helpers.redis import get_redis_connection
from shared.reports.enums import UploadState
from shared.yaml import UserYaml
from shared.yaml.user_yaml import OwnerContext
from upload.helpers import dispatch_upload_task


def _installation_change_url(installation):
    return reverse(
        "admin:codecov_auth_githubappinstallation_change", args=[installation.pk]
    )


def installation_summary(installations):
    """Comma-separated, one-line summary of GitHub App installations for lists.

    Each entry links to the GithubAppInstallation admin change page.
    """
    installations = list(installations)
    if not installations:
        return "-"
    return format_html_join(
        ", ",
        '<a href="{}">{} (installation {})</a>',
        (
            (
                _installation_change_url(installation),
                installation.name,
                installation.installation_id,
            )
            for installation in installations
        ),
    )


def installation_table(installations):
    """Read-only HTML table of GitHub App installations for detail views.

    The name in each row links to the GithubAppInstallation admin change page.
    """
    installations = list(installations)
    if not installations:
        return "-"
    rows = format_html_join(
        "",
        "<tr>"
        "<td style='padding:2px 16px 2px 0'><a href='{}'>{}</a></td>"
        "<td style='padding:2px 16px 2px 0'>{}</td>"
        "<td style='padding:2px 16px 2px 0'>{}</td>"
        "<td style='padding:2px 16px 2px 0'>{}</td>"
        "<td>{}</td>"
        "</tr>",
        (
            (
                _installation_change_url(installation),
                installation.name,
                installation.app_id,
                installation.installation_id,
                "all repos" if installation.covers_all_repos() else "scoped repos",
                "yes" if installation.is_suspended else "no",
            )
            for installation in installations
        ),
    )
    return format_html(
        "<table><thead><tr>"
        "<th style='text-align:left;padding-right:16px'>Name</th>"
        "<th style='text-align:left;padding-right:16px'>App ID</th>"
        "<th style='text-align:left;padding-right:16px'>Installation ID</th>"
        "<th style='text-align:left;padding-right:16px'>Scope</th>"
        "<th style='text-align:left'>Suspended</th>"
        "</tr></thead><tbody>{}</tbody></table>",
        rows,
    )


def notification_failure_reason(notification: CommitNotification) -> str | None:
    if notification.state != CommitNotification.States.ERROR:
        return None
    if (
        notification.decoration_type
        and notification.decoration_type != CommitNotification.DecorationTypes.STANDARD
    ):
        return notification.get_decoration_type_display()
    return "Delivery failed"


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


PENDING_UPLOAD_FILTER = (
    Q(reports__sessions__state__in=["uploaded", "started"])
    | Q(reports__sessions__state="")
    | Q(reports__sessions__state__isnull=True)
)


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
    list_display = (
        "repo_slug",
        "service_id",
        "author_link",
        "active",
        "private",
        "activated",
        "deleted",
        "integrations",
    )
    list_filter = ("active", "private", "activated", "deleted", "using_integration")
    list_select_related = ("author",)
    search_fields = (
        "name",
        "author__username__exact",
        "service_id__exact",
    )
    # pg_trgm builds 3-char trigrams, so substring name search can only use the
    # repos_name_trgm_idx GIN index for terms of at least this length.
    name_search_min_length = 3
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
        "integrations_table",
    )
    fields = readonly_fields + (
        "bot",
        "using_integration",
        "branch",
        "private",
        "webhook_secret",
    )

    @admin.display(description="name", ordering="name")
    def repo_slug(self, obj):
        author = obj.author
        if author is None:
            return obj.name
        return f"{author.service}:{author.username}/{obj.name}"

    def get_queryset(self, request):
        # Prefetch the owner's GitHub App installations so the `integrations`
        # column resolves in one extra query per page instead of N+1 per row.
        return (
            super()
            .get_queryset(request)
            .prefetch_related("author__github_app_installations")
        )

    @admin.display(description="author", ordering="author")
    def author_link(self, obj):
        author = obj.author
        if author is None:
            return "-"
        url = reverse("admin:codecov_auth_owner_change", args=[author.ownerid])
        return format_html(
            '<a href="{}">{}/{}</a>', url, author.service, author.username
        )

    def _covering_installations(self, obj):
        # GitHub App installations covering this repo. Relies on the
        # `author__github_app_installations` prefetch in `get_queryset`.
        author = obj.author
        if author is None:
            return []
        return [
            installation
            for installation in author.github_app_installations.all()
            if installation.is_repo_covered_by_integration(obj)
        ]

    @admin.display(description="integrations")
    def integrations(self, obj):
        # Comma-separated list of the GitHub App installations covering this
        # repo, replacing the legacy `using_integration` boolean column.
        return installation_summary(self._covering_installations(obj))

    @admin.display(description="GitHub App installations")
    def integrations_table(self, obj):
        return installation_table(self._covering_installations(obj))

    def get_search_results(
        self,
        request: HttpRequest,
        queryset: QuerySet[Repository],
        search_term: str,
    ) -> tuple[QuerySet[Repository], bool]:
        """
        Search for repositories by name, service_id, author username, or repoid.
        https://docs.djangoproject.com/en/5.2/ref/contrib/admin/#django.contrib.admin.ModelAdmin.get_search_results

        Django's default search ORs `search_fields` across the repos<->owners
        join. Because `author__username` lives on a different table, Postgres
        cannot use per-table indexes for that OR and falls back to a full join
        scan (~4.4s over 24.6M repos). Instead we resolve usernames to ownerids
        first and build a single-table OR on repos, so every branch uses its own
        index (name trigram, service_id, ownerid, pk) via a BitmapOr.
        """
        term = search_term.strip()
        if not term:
            return queryset, False

        filters = Q()

        # Substring name match -> repos_name_trgm_idx (GIN trigram). Only usable
        # for terms >= name_search_min_length; shorter terms skip name search.
        if len(term) >= self.name_search_min_length:
            filters |= Q(name__icontains=term)

        # Exact service_id -> repos_service_id_author / repos_service_ids.
        filters |= Q(service_id=term)

        # Resolve author username to ownerids with one indexed query on owners so
        # the repos filter stays single-table (no cross-table OR).
        owner_ids = list(
            Owner.objects.filter(username=term).values_list("ownerid", flat=True)
        )
        if owner_ids:
            filters |= Q(author_id__in=owner_ids)

        # Exact repoid when the term is numeric.
        if term.isdigit():
            filters |= Q(repoid=int(term))

        return queryset.filter(filters), False

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


class CommitReportInline(admin.TabularInline):
    model = CommitReport
    extra = 0
    can_delete = False
    verbose_name = "Commit report"
    verbose_name_plural = "Commit reports"
    readonly_fields = (
        "id",
        "report_type_display",
        "code",
        "external_id",
        "created_at",
        "updated_at",
    )
    fields = readonly_fields
    ordering = ("report_type", "id")

    @admin.display(description="Report type")
    def report_type_display(self, obj):
        return obj.report_type or ReportType.COVERAGE

    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class CommitNotificationInline(admin.TabularInline):
    model = CommitNotification
    extra = 0
    can_delete = False
    verbose_name = "Commit notification"
    verbose_name_plural = "Commit notifications"
    readonly_fields = (
        "notification_type",
        "decoration_type",
        "state",
        "failure_reason",
        "gh_app_link",
        "created_at",
        "updated_at",
    )
    fields = readonly_fields
    ordering = ("notification_type",)

    @admin.display(description="Failure reason")
    def failure_reason(self, obj):
        return notification_failure_reason(obj) or "-"

    @admin.display(description="GitHub App")
    def gh_app_link(self, obj):
        if obj.gh_app_id is None:
            return "-"
        return format_html(
            '<a href="{}">{}</a>',
            _installation_change_url(obj.gh_app),
            obj.gh_app.name,
        )

    def has_add_permission(self, request, obj=None):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("gh_app")


class CommitNotificationStatusFilter(admin.SimpleListFilter):
    title = "notification status"
    parameter_name = "notification_status"

    def lookups(self, request, model_admin):
        return (
            ("all_passed", "All notifications passed"),
            ("has_failure", "Has notification failure"),
        )

    def queryset(self, request, queryset):
        if self.value() == "all_passed":
            return queryset.filter(notification_count__gt=0).filter(
                notification_count=F("success_notification_count")
            )
        if self.value() == "has_failure":
            return queryset.filter(notification_count__gt=0).exclude(
                notification_count=F("success_notification_count")
            )
        return queryset


@admin.register(Commit)
class CommitAdmin(AdminMixin, admin.ModelAdmin):
    inlines = [CommitReportInline, CommitNotificationInline]
    list_display = (
        "short_commitid",
        "repository_link",
        "report_count_display",
        "reports_status",
        "notification_count_display",
        "notifications_successful",
        "branch",
        "state",
        "timestamp",
    )
    list_filter = ("state", CommitNotificationStatusFilter, "timestamp")
    list_select_related = ("repository", "repository__author")
    ordering = ("-timestamp",)
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
        "parent_commit_link",
        "state",
        "ci_passed",
        "totals",
        "merged",
        "deleted",
        "notified",
        "reprocess_actions",
    )
    fields = readonly_fields
    paginator = EstimatedCountPaginator
    show_full_result_count = False

    # Show short commit SHA in list view
    @admin.display(description="Commit SHA")
    def short_commitid(self, obj):
        return obj.commitid[:7] if obj.commitid else ""

    @admin.display(description="Repository", ordering="repository__name")
    def repository_link(self, obj):
        repo = obj.repository
        if repo is None:
            return "-"
        author = repo.author
        if author is None:
            label = repo.name
        else:
            label = f"{author.service}:{author.username}/{repo.name}"
        url = reverse("admin:core_repository_change", args=[repo.repoid])
        return format_html('<a href="{}">{}</a>', url, label)

    @admin.display(description="Parent commit id")
    def parent_commit_link(self, obj):
        if not obj.parent_commit_id:
            return "-"
        parent = obj.parent_commit
        if parent is None:
            return obj.parent_commit_id
        url = reverse("admin:core_commit_change", args=[parent.pk])
        return format_html('<a href="{}">{}</a>', url, obj.parent_commit_id)

    def get_queryset(self, request):
        return (
            super()
            .get_queryset(request)
            .select_related("repository", "repository__author")
            .annotate(
                report_count=Count("reports", distinct=True),
                notification_count=Count("notifications", distinct=True),
                success_notification_count=Count(
                    "notifications",
                    filter=Q(notifications__state=CommitNotification.States.SUCCESS),
                    distinct=True,
                ),
                upload_count=Count("reports__sessions"),
                error_upload_count=Count(
                    "reports__sessions",
                    filter=Q(reports__sessions__state="error"),
                ),
                pending_upload_count=Count(
                    "reports__sessions",
                    filter=PENDING_UPLOAD_FILTER,
                ),
                error_report_results_count=Count(
                    "reports__reportresults",
                    filter=Q(
                        reports__reportresults__state=ReportResults.ReportResultsStates.ERROR
                    ),
                ),
            )
        )

    @admin.display(description="Reports", ordering="report_count")
    def report_count_display(self, obj):
        return obj.report_count

    @admin.display(description="Report status", boolean=True)
    def reports_status(self, obj):
        if obj.report_count == 0:
            return None
        if obj.error_upload_count or obj.error_report_results_count:
            return False
        if obj.pending_upload_count or obj.upload_count == 0:
            return None
        return True

    @admin.display(description="Notifications", ordering="notification_count")
    def notification_count_display(self, obj):
        return obj.notification_count

    @admin.display(description="All notifications OK", boolean=True)
    def notifications_successful(self, obj):
        if obj.notification_count == 0:
            return None
        return obj.success_notification_count == obj.notification_count

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def changeform_view(self, request, *args, **kwargs):
        # Stash the request so `reprocess_actions` can hide buttons for Viewers.
        self._reprocess_actions_request = request
        return super().changeform_view(request, *args, **kwargs)

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

        request = getattr(self, "_reprocess_actions_request", None)
        if request is not None and is_viewer(request):
            return "No reprocessing actions available"

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
        deny_viewers(request)
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))
        return self._reprocess_uploads(request, commit, REPROCESS_CONFIGS["coverage"])

    def reprocess_test_analytics_view(self, request, object_id):
        deny_viewers(request)
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))
        return self._reprocess_uploads(
            request, commit, REPROCESS_CONFIGS["test_analytics"]
        )

    def reprocess_bundle_analysis_view(self, request, object_id):
        deny_viewers(request)
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))
        return self._reprocess_uploads(
            request, commit, REPROCESS_CONFIGS["bundle_analysis"]
        )

    def trigger_notifications_view(self, request, object_id):
        deny_viewers(request)
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
