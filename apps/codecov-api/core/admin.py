from django import forms
from django.contrib import admin, messages
from django.db.models import QuerySet
from django.http import HttpRequest, HttpResponseRedirect
from django.urls import path, reverse
from django.utils.html import format_html

from codecov.admin import AdminMixin
from codecov_auth.models import RepositoryToken
from core.models import Commit, Pull, Repository
from reports.models import CommitReport, ReportSession
from services.task.task import TaskService
from shared.celery_config import (
    bundle_analysis_notify_task_name,
    manual_upload_completion_trigger_task_name,
    test_results_finisher_task_name,
    upload_finisher_task_name,
)
from shared.django_apps.reports.models import ReportType
from shared.django_apps.utils.paginator import EstimatedCountPaginator
from shared.yaml import UserYaml
from shared.yaml.user_yaml import OwnerContext


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
    search_fields = ("author__username__exact",)
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
        Search for repositories by name or repoid.
        https://docs.djangoproject.com/en/5.2/ref/contrib/admin/#django.contrib.admin.ModelAdmin.get_search_results
        """
        # Default search is by author username (defined in `search_fields`)
        queryset, may_have_duplicates = super().get_search_results(
            request,
            queryset,
            search_term,
        )
        try:
            search_term_as_int = int(search_term)
        except ValueError:
            pass
        else:
            queryset |= self.model.objects.filter(repoid=search_term_as_int)
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

    def reprocess_coverage_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))

        # Query for coverage reports
        coverage_reports = CommitReport.objects.filter(
            commit=commit, report_type__in=[None, ReportType.COVERAGE]
        )

        if not coverage_reports.exists():
            self.message_user(
                request,
                "This commit has no coverage data to reprocess",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(
                reverse("admin:core_commit_change", args=[commit.pk])
            )

        # Get all uploads (ReportSession) for coverage reports
        uploads = ReportSession.objects.filter(report__in=coverage_reports)

        if not uploads.exists():
            self.message_user(
                request,
                "This commit has no coverage uploads to reprocess",
                level=messages.WARNING,
            )
            return HttpResponseRedirect(
                reverse("admin:core_commit_change", args=[commit.pk])
            )

        # Construct processing_results to pass directly to the task
        # This keeps all retrieval logic in admin.py and avoids modifying the task
        processing_results = [
            {
                "upload_id": upload.id,
                "arguments": {
                    "commit": commit.commitid,
                    "upload_id": upload.id,
                    "version": "v4",
                    "reportid": str(upload.report.external_id),
                },
                "successful": upload.order_number is not None,
            }
            for upload in uploads
        ]

        commit_yaml = get_repo_yaml(commit.repository)
        commit_yaml_dict = commit_yaml.to_dict() if commit_yaml else None

        task_service = TaskService()
        task_service.schedule_task(
            upload_finisher_task_name,
            kwargs={
                "repoid": commit.repository.repoid,
                "commitid": commit.commitid,
                "commit_yaml": commit_yaml_dict,
                "processing_results": processing_results,
                "manual_trigger": True,  # Skip idempotency check for admin reprocessing
            },
            apply_async_kwargs={},
        )
        self.message_user(
            request,
            f"Successfully queued coverage reprocessing for commit {commit.commitid[:7]} ({len(processing_results)} uploads)",
            level=messages.SUCCESS,
        )

        return HttpResponseRedirect(
            reverse("admin:core_commit_change", args=[commit.pk])
        )

    def reprocess_test_analytics_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))

        has_test_analytics = CommitReport.objects.filter(
            commit=commit, report_type=ReportType.TEST_RESULTS
        ).exists()

        if has_test_analytics:
            commit_yaml = get_repo_yaml(commit.repository)
            commit_yaml_dict = commit_yaml.to_dict() if commit_yaml else None

            task_service = TaskService()
            task_service.schedule_task(
                test_results_finisher_task_name,
                args=(True,),  # _chain_result - required positional arg for this task
                kwargs={
                    "repoid": commit.repository.repoid,
                    "commitid": commit.commitid,
                    "commit_yaml": commit_yaml_dict,
                },
                apply_async_kwargs={},
            )
            self.message_user(
                request,
                f"Successfully queued test analytics reprocessing for commit {commit.commitid[:7]}",
                level=messages.SUCCESS,
            )
        else:
            self.message_user(
                request,
                "This commit has no test analytics data to reprocess",
                level=messages.WARNING,
            )

        return HttpResponseRedirect(
            reverse("admin:core_commit_change", args=[commit.pk])
        )

    def reprocess_bundle_analysis_view(self, request, object_id):
        commit = self.get_object(request, object_id)
        if commit is None:
            self.message_user(request, "Commit not found", level=messages.ERROR)
            return HttpResponseRedirect(reverse("admin:core_commit_changelist"))

        has_bundle_analysis = CommitReport.objects.filter(
            commit=commit, report_type=ReportType.BUNDLE_ANALYSIS
        ).exists()

        if has_bundle_analysis:
            commit_yaml = get_repo_yaml(commit.repository)
            commit_yaml_dict = commit_yaml.to_dict() if commit_yaml else None

            task_service = TaskService()
            task_service.schedule_task(
                bundle_analysis_notify_task_name,
                kwargs={
                    "repoid": commit.repository.repoid,
                    "commitid": commit.commitid,
                    "commit_yaml": commit_yaml_dict,
                },
                apply_async_kwargs={},
            )
            self.message_user(
                request,
                f"Successfully queued bundle analysis reprocessing for commit {commit.commitid[:7]}",
                level=messages.SUCCESS,
            )
        else:
            self.message_user(
                request,
                "This commit has no bundle analysis data to reprocess",
                level=messages.WARNING,
            )

        return HttpResponseRedirect(
            reverse("admin:core_commit_change", args=[commit.pk])
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
