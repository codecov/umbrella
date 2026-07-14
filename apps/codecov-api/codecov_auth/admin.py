import logging
from collections.abc import Sequence
from datetime import timedelta

import django.forms as forms
from django.conf import settings
from django.contrib import admin, messages
from django.contrib.admin.helpers import ACTION_CHECKBOX_NAME
from django.contrib.admin.models import LogEntry
from django.db.models import Count, OuterRef, Q, Subquery
from django.db.models.fields import BLANK_CHOICE_DASH
from django.db.models.functions import Coalesce
from django.forms import CheckboxInput, Select, Textarea
from django.http import HttpRequest
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.utils.html import format_html, format_html_join

from codecov.admin import AdminMixin, get_staff_role
from codecov.commands.exceptions import ValidationError
from codecov_auth.helpers import History
from codecov_auth.models import OrganizationLevelToken, Owner, SentryUser, Session, User
from codecov_auth.services.org_level_token_service import OrgLevelTokenService
from codecov_auth.services.owner_reconnect import (
    build_reconnect_preview,
)
from codecov_auth.services.owner_reconnect import (
    reconnect_owner as reconnect_owner_merge,
)
from core.admin import installation_summary, installation_table
from core.models import Repository
from services.task import TaskService
from shared.django_apps.codecov_auth.models import (
    Account,
    AccountsUsers,
    GithubAppInstallation,
    InvoiceBilling,
    OwnerExport,
    OwnerToBeDeleted,
    Plan,
    Service,
    StripeBilling,
    Tier,
    _generate_support_pin,
)
from shared.django_apps.codecov_auth.models import User as CodecovUser
from shared.owner_data_export.config import EXPORT_DAYS_DEFAULT
from shared.plan.service import PlanService
from utils.services import get_short_service_name

log = logging.getLogger(__name__)


def _originator_user_id(request: HttpRequest) -> int | None:
    """Id of the logged-in admin user, or None if not a resolvable int."""
    user_id = getattr(getattr(request, "user", None), "id", None)
    return user_id if isinstance(user_id, int) else None


class ExtendTrialForm(forms.Form):
    end_date = forms.DateTimeField(
        label="Trial End Date (YYYY-MM-DD HH:MM:SS):", required=False
    )


def extend_trial(self, request, queryset):
    if "extend_trial" in request.POST:
        form = ExtendTrialForm(request.POST)
        if form.is_valid():
            for org in queryset:
                plan_service = PlanService(current_org=org)
                try:
                    plan_service.start_trial_manually(
                        current_owner=request.current_owner,
                        end_date=form.cleaned_data["end_date"],
                    )
                except ValidationError as e:
                    self.message_user(
                        request,
                        e.message + f" for {org.username}",
                        level=messages.ERROR,
                    )
                else:
                    self.message_user(
                        request, f"Successfully started trial for {org.username}"
                    )
            return
    else:
        form = ExtendTrialForm()

    return render(
        request,
        "admin/extend_trial_form.html",
        context={
            "form": form,
            "datasets": queryset,
        },
    )


extend_trial.short_description = "Start and extend trial up to a selected date"


def impersonate_owner(self, request, queryset):
    if queryset.count() != 1:
        self.message_user(
            request, "You must impersonate exactly one Owner.", level=messages.ERROR
        )
        return

    owner: Owner = queryset.first()
    response = redirect(
        f"{settings.CODECOV_URL}/{get_short_service_name(owner.service)}/{owner.username}"
    )

    # this cookie is read by the `ImpersonationMiddleware` and
    # will reset `request.current_owner` to the impersonated owner
    max_age = 900  # 15 minutes
    response.set_cookie(
        "staff_user",
        owner.ownerid,
        domain=settings.COOKIES_DOMAIN,
        samesite=settings.COOKIE_SAME_SITE,
        max_age=max_age,
    )
    History.log(
        Owner.objects.get(ownerid=owner.ownerid),
        "Impersonation successful",
        request.user,
    )
    return response


impersonate_owner.short_description = "Impersonate the selected owner"


def refresh_owner(self, request, queryset):
    if queryset.count() != 1:
        self.message_user(
            request, "You must refresh exactly one Owner.", level=messages.ERROR
        )
        return

    owner = queryset.first()

    task_service = TaskService()
    task_service.refresh(
        ownerid=owner.ownerid,
        username=owner.username,
        sync_teams=True,
        sync_repos=True,
        manual_trigger=True,
    )

    self.message_user(
        request,
        f"Refresh task triggered for {owner.username} (ownerid: {owner.ownerid})",
        level=messages.SUCCESS,
    )
    History.log(
        Owner.objects.get(ownerid=owner.ownerid),
        "Refresh task triggered",
        request.user,
    )
    return


refresh_owner.short_description = "Sync repos and teams for the selected owner"


def export_owner_data(self, request, queryset):
    """
    Trigger an export of all data for the selected owner.

    Creates an OwnerExport record and triggers the export task pipeline.
    """
    if queryset.count() != 1:
        self.message_user(
            request,
            "You must export exactly one Owner at a time.",
            level=messages.ERROR,
        )
        return

    owner = queryset.first()

    existing_export = OwnerExport.objects.filter(
        owner=owner,
        status__in=[OwnerExport.Status.PENDING, OwnerExport.Status.IN_PROGRESS],
    ).first()
    if existing_export:
        log.warning(
            "Export blocked: existing export in progress",
            extra={
                "export_id": existing_export.id,
            },
        )
        self.message_user(
            request,
            f"An export is already in progress for {owner.username} "
            f"(Export ID: {existing_export.id}, Status: {existing_export.status}). "
            "Please wait for it to complete or fail before starting a new one.",
            level=messages.ERROR,
        )
        return

    user = getattr(owner, "user", None)
    if hasattr(request, "user") and hasattr(request.user, "id"):
        try:
            user = CodecovUser.objects.filter(id=request.user.id).first()
        except Exception:
            pass

    since_date = timezone.now() - timedelta(days=EXPORT_DAYS_DEFAULT)

    export = OwnerExport.objects.create(
        owner=owner,
        since_date=since_date,
        status=OwnerExport.Status.PENDING,
        created_by=user,
    )

    log.info(
        "Owner data export initiated",
        extra={"export_id": export.id},
    )

    task_service = TaskService()
    task_service.export_owner_data(
        ownerid=owner.ownerid,
        export_id=export.id,
        user_id=user.id if user else None,
    )

    self.message_user(
        request,
        f"Export task triggered for {owner.username} (ownerid: {owner.ownerid}). "
        f"Export ID: {export.id}. Check OwnerExport admin for status and download link.",
        level=messages.SUCCESS,
    )
    History.log(
        Owner.objects.get(ownerid=owner.ownerid),
        f"Data export triggered (export_id: {export.id})",
        request.user,
    )
    return


export_owner_data.short_description = "Export owner data (SQL dumps + archives)"


def regenerate_support_pin(self, request, queryset):
    """Regenerate the 6-digit support PIN for the selected owner(s)."""
    count = 0
    for owner in queryset:
        owner.support_pin = _generate_support_pin()
        owner.save(update_fields=["support_pin", "updatestamp"])
        History.log(
            Owner.objects.get(ownerid=owner.ownerid),
            "Support PIN regenerated",
            request.user,
        )
        count += 1

    self.message_user(
        request,
        f"Regenerated support PIN for {count} owner(s).",
        level=messages.SUCCESS,
    )
    return


regenerate_support_pin.short_description = "Regenerate support PIN"


class AccountsUsersInline(admin.TabularInline):
    model = AccountsUsers
    max_num = 10
    extra = 0
    verbose_name_plural = "Accounts Users (click save to commit changes)"
    verbose_name = "Account User"
    can_delete = False
    can_edit = False
    autocomplete_fields = ("account",)

    def get_queryset(self, request):
        return super().get_queryset(request).select_related("account", "user")


class OwnerUserInline(admin.TabularInline):
    model = Owner
    max_num = 5
    extra = 0
    verbose_name_plural = "Owners (read only)"
    verbose_name = "Owner"
    exclude = ("oauth_token",)
    can_delete = False

    readonly_fields = [
        "name",
        "username",
        "email",
        "service",
        "student",
    ]

    fields = [] + readonly_fields


class StaffRoleListFilter(admin.SimpleListFilter):
    """Filter users by `staff_role` (kept in sync with the flags by `User.save`)."""

    title = "role"
    parameter_name = "role"

    def lookups(self, request, model_admin):
        return User.StaffRole.choices

    def queryset(self, request, queryset):
        value = self.value()
        if value:
            return queryset.filter(staff_role=value)
        return queryset


@admin.register(User)
class UserAdmin(AdminMixin, admin.ModelAdmin):
    list_display = (
        "name",
        "email",
        "is_staff",
        "is_superuser",
        "staff_role",
    )
    list_filter = (StaffRoleListFilter,)
    inlines = [AccountsUsersInline, OwnerUserInline]
    search_fields = (
        "name__iregex",
        "email__iregex",
    )

    readonly_fields = (
        "id",
        "external_id",
    )

    fields = readonly_fields + (
        "name",
        "email",
        "is_staff",
        "is_superuser",
        "staff_role",
        "terms_agreement",
        "terms_agreement_at",
    )

    def get_form(self, request, obj=None, change=False, **kwargs):
        form = super().get_form(request, obj, change, **kwargs)

        # Only superusers may change admin access levels.
        if not request.user.is_superuser:
            for field_name in ("is_staff", "is_superuser", "staff_role"):
                if field_name in form.base_fields:
                    form.base_fields[field_name].disabled = True

        # Admin/None are flag-derived and read-only; only staff toggle Viewer/Member.
        staff_role_field = form.base_fields.get("staff_role")
        if staff_role_field is not None:
            if obj is not None and obj.is_superuser:
                staff_role_field.choices = [
                    (User.StaffRole.ADMIN.value, User.StaffRole.ADMIN.label),
                ]
                staff_role_field.disabled = True
            elif obj is not None and not obj.is_staff:
                staff_role_field.choices = [
                    (User.StaffRole.NONE.value, User.StaffRole.NONE.label),
                ]
                staff_role_field.disabled = True
            else:
                staff_role_field.choices = [
                    (User.StaffRole.VIEWER.value, User.StaffRole.VIEWER.label),
                    (User.StaffRole.MEMBER.value, User.StaffRole.MEMBER.label),
                ]

        return form

    def has_add_permission(self, _, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(SentryUser)
class SentryUserAdmin(AdminMixin, admin.ModelAdmin):
    list_display = (
        "name",
        "email",
    )
    search_fields = (
        "name__iregex",
        "email__iregex",
    )
    readonly_fields = (
        "id",
        "external_id",
        "sentry_id",
        "user",
    )
    fields = readonly_fields + (
        "name",
        "email",
    )

    def has_add_permission(self, _, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class OrgUploadTokenInline(admin.TabularInline):
    model = OrganizationLevelToken
    readonly_fields = ["token", "refresh"]
    fields = ["token", "valid_until", "token_type", "refresh"]
    extra = 0
    max_num = 1
    verbose_name = "Organization Level Token"

    def refresh(self, obj: OrganizationLevelToken):
        # 0 in this case refers to the 0th index of the inline
        # But there can only ever be 1 token per org, so it's fine to use that.
        return format_html(
            '<input type="checkbox" name="organization_tokens-0-REFRESH" id="id_organization_tokens-0-REFRESH">'
        )

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return request.user.is_staff

    def has_add_permission(self, request: HttpRequest, obj: Owner | None) -> bool:
        has_token = OrganizationLevelToken.objects.filter(owner=obj).count() > 0
        return (not has_token) and request.user.is_staff


class InvoiceBillingInline(admin.StackedInline):
    model = InvoiceBilling
    extra = 0
    can_delete = False
    verbose_name_plural = "Invoice Billing"
    verbose_name = "Invoice Billing (click save to commit changes)"


@admin.register(InvoiceBilling)
class InvoiceBillingAdmin(AdminMixin, admin.ModelAdmin):
    list_display = ("id", "account", "is_active")
    search_fields = (
        "account__name",
        "account__id__iexact",
        "id__iexact",
        "account_manager",
    )
    search_help_text = (
        "Search by account name, account id (exact), id (exact), or account_manager"
    )
    autocomplete_fields = ("account",)

    readonly_fields = [
        "id",
        "created_at",
        "updated_at",
    ]

    fields = readonly_fields + [
        "account",
        "account_manager",
        "invoice_notes",
        "is_active",
    ]

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        field = form.base_fields["account"]
        field.widget.can_add_related = False
        field.widget.can_change_related = False
        field.widget.can_delete_related = False
        return form


class StripeBillingInline(admin.StackedInline):
    can_delete = False
    extra = 0
    model = StripeBilling
    verbose_name_plural = "Stripe Billing"
    verbose_name = "Stripe Billing (click save to commit changes)"


@admin.register(StripeBilling)
class StripeBillingAdmin(AdminMixin, admin.ModelAdmin):
    list_display = ("id", "account", "is_active")
    search_fields = (
        "account__name",
        "account__id__iexact",
        "id__iexact",
        "customer_id__iexact",
        "subscription_id__iexact",
    )
    search_help_text = "Search by account name, account id (exact), id (exact), customer_id (exact), or subscription_id (exact)"
    autocomplete_fields = ("account",)

    readonly_fields = [
        "id",
        "created_at",
        "updated_at",
    ]

    fields = readonly_fields + [
        "account",
        "customer_id",
        "subscription_id",
        "is_active",
    ]

    def get_form(self, request, obj=None, **kwargs):
        form = super().get_form(request, obj, **kwargs)
        field = form.base_fields["account"]
        field.widget.can_add_related = False
        field.widget.can_change_related = False
        field.widget.can_delete_related = False
        return form


class OwnerOrgInline(admin.TabularInline):
    model = Owner
    max_num = 100
    extra = 0
    verbose_name_plural = "Organizations (read only)"
    verbose_name = "Organization"
    exclude = ("oauth_token",)
    can_delete = False

    readonly_fields = [
        "name",
        "username",
        "plan",
        "plan_activated_users",
        "service",
    ]

    fields = [] + readonly_fields


def find_and_remove_stale_users(
    orgs: Sequence[Owner], date_threshold: timedelta | None = None
) -> tuple[set[int], set[int]]:
    """
    This functions finds all the stale `plan_activated_users` in any of the given `orgs`.

    It then removes all those stale users from the given `orgs`,
    returning the set of stale users (`ownerid`), and the set of `orgs` that were updated (`ownerid`).

    A user is considered stale if it had no API or login `Session` or any opened PR within `date_threshold`.
    If no `date_threshold` is given, it defaults to *90 days*.
    """

    active_users = set()
    for org in orgs:
        active_users.update(set(org.plan_activated_users))

    if not active_users:
        return (set(), set())

    # NOTE: the `annotate_last_pull_timestamp` manager/queryset method does the same `annotate` with `Subquery`.
    sessions = Session.objects.filter(owner=OuterRef("pk")).order_by("-lastseen")
    resolved_users = list(
        Owner.objects.filter(ownerid__in=active_users)
        .annotate(latest_session=Subquery(sessions.values("lastseen")[:1]))
        .annotate_last_pull_timestamp()
        .values_list("ownerid", "latest_session", "last_pull_timestamp", named=True)
    )

    threshold = timezone.now() - (date_threshold or timedelta(days=90))

    def is_stale(user: dict) -> bool:
        return (user.latest_session is None or user.latest_session < threshold) and (
            # NOTE: `last_pull_timestamp` is not timezone-aware, so we explicitly compare without timezones here
            user.last_pull_timestamp is None
            or user.last_pull_timestamp.replace(tzinfo=None)
            < threshold.replace(tzinfo=None)
        )

    stale_users = {user.ownerid for user in resolved_users if is_stale(user)}
    affected_orgs = {
        org for org in orgs if stale_users.intersection(set(org.plan_activated_users))
    }

    if not affected_orgs:
        return (set(), set())

    # TODO: it might make sense to run all this within a transaction and locking the `affected_orgs` for update,
    # as we have a slight chance of races between querying the `orgs` at the very beginning and updating them here:
    for org in affected_orgs:
        org.plan_activated_users = list(
            set(org.plan_activated_users).difference(stale_users)
        )
    Owner.objects.bulk_update(affected_orgs, ["plan_activated_users"])

    return (stale_users, {org.ownerid for org in affected_orgs})


@admin.register(Account)
class AccountAdmin(AdminMixin, admin.ModelAdmin):
    list_display = ("name", "is_active", "organizations_count", "all_user_count")
    search_fields = ("name__iregex", "id")
    search_help_text = "Search by name (can use regex), or id (exact)"
    inlines = [OwnerOrgInline, StripeBillingInline, InvoiceBillingInline]
    actions = ["seat_check", "link_users_to_account", "deactivate_stale_users"]

    readonly_fields = ["id", "created_at", "updated_at", "users"]

    fields = readonly_fields + [
        "name",
        "is_active",
        "plan",
        "plan_seat_count",
        "free_seat_count",
        "plan_auto_activate",
        "is_delinquent",
    ]

    @admin.action(description="Deactivate all stale `plan_activated_users`")
    def deactivate_stale_users(self, request, queryset):
        orgs = [org for account in queryset for org in account.organizations.all()]
        stale_users, updated_orgs = find_and_remove_stale_users(orgs)

        if not stale_users or not updated_orgs:
            self.message_user(
                request,
                "No stale users found in selected accounts / organizations.",
                messages.INFO,
            )
        else:
            self.message_user(
                request,
                f"Removed {len(stale_users)} stale users from {len(updated_orgs)} affected organizations.",
                messages.SUCCESS,
            )

    @admin.action(
        description="Count current plan_activated_users across all Organizations"
    )
    def seat_check(self, request, queryset):
        self.link_users_to_account(request, queryset, dry_run=True)

    @admin.action(description="Link Users to Account")
    def link_users_to_account(self, request, queryset, dry_run=False):
        for account in queryset:
            account_plan_activated_user_ownerids = set()
            for org in account.organizations.all():
                account_plan_activated_user_ownerids.update(
                    set(org.plan_activated_users)
                )

            account_plan_activated_user_owners = Owner.objects.filter(
                ownerid__in=account_plan_activated_user_ownerids
            ).prefetch_related("user")

            non_student_count = account_plan_activated_user_owners.exclude(
                student=True
            ).count()
            total_seats_for_account = account.plan_seat_count + account.free_seat_count
            if non_student_count > total_seats_for_account:
                self.message_user(
                    request,
                    f"Request failed: Account plan does not have enough seats; "
                    f"current plan activated users (non-students): {non_student_count}, total seats for account: {total_seats_for_account}",
                    messages.ERROR,
                )
                return
            if dry_run:
                self.message_user(
                    request,
                    f"Request succeeded: Account plan has enough seats! "
                    f"current plan activated users (non-students): {non_student_count}, total seats for account: {total_seats_for_account}",
                    messages.SUCCESS,
                )
                return

            owners_without_user_objects = account_plan_activated_user_owners.filter(
                user__isnull=True
            )
            owners_with_new_user_objects = []
            for userless_owner in owners_without_user_objects:
                new_user = User.objects.create(
                    name=userless_owner.name, email=userless_owner.email
                )
                userless_owner.user = new_user
                owners_with_new_user_objects.append(userless_owner)
            total = Owner.objects.bulk_update(owners_with_new_user_objects, ["user"])
            self.message_user(
                request,
                f"Created a User for {total} Owners",
                messages.INFO,
            )
            if total > 0:
                log.info(
                    f"Admin operation for {account} - Created a User for {total} Owners",
                    extra={
                        "owners_with_new_user_objects": [
                            str(owner) for owner in owners_with_new_user_objects
                        ],
                        "account_id": account.id,
                    },
                )

            # redo this query to get all Owners and Users
            account_plan_activated_user_owners = Owner.objects.filter(
                ownerid__in=account_plan_activated_user_ownerids
            ).prefetch_related("user")

            already_linked_account_users = AccountsUsers.objects.filter(account=account)

            not_yet_linked_owners = account_plan_activated_user_owners.exclude(
                user_id__in=already_linked_account_users.values_list(
                    "user_id", flat=True
                )
            )

            account_users_that_should_be_unlinked = (
                already_linked_account_users.exclude(
                    user_id__in=account_plan_activated_user_owners.values_list(
                        "user_id", flat=True
                    )
                )
            )
            deleted_ids_for_log = list(
                account_users_that_should_be_unlinked.values_list("id", flat=True)
            )
            deleted_count, _ = account_users_that_should_be_unlinked.delete()

            new_accounts_users = []
            for owner in not_yet_linked_owners:
                new_account_user = AccountsUsers(
                    user_id=owner.user_id, account_id=account.id
                )
                new_accounts_users.append(new_account_user)
            total = AccountsUsers.objects.bulk_create(new_accounts_users)
            self.message_user(
                request,
                f"Created {len(total)} AccountsUsers, removed {deleted_count} AccountsUsers",
                messages.SUCCESS,
            )
            if len(total) > 0 or deleted_count > 0:
                log.info(
                    f"Admin operation for {account} - Created {len(total)} AccountsUsers, removed {deleted_count} AccountsUsers",
                    extra={
                        "new_accounts_users": total,
                        "removed_accounts_users_ids": deleted_ids_for_log,
                        "account_id": account.id,
                    },
                )


@admin.register(Owner)
class OwnerAdmin(AdminMixin, admin.ModelAdmin):
    exclude = ("oauth_token",)
    list_display = (
        "username",
        "name",
        "external_id",
        "repository_count",
        "github_app_installations_summary",
        "email",
        "service",
        "plan_display",
        "seats_display",
        "support_pin",
    )
    list_select_related = ("account",)
    readonly_fields = []
    search_fields = (
        "email__iregex",
        "external_id",
        "name__iregex",
        "ownerid",
        "username__iregex",
    )
    actions = [
        impersonate_owner,
        extend_trial,
        refresh_owner,
        export_owner_data,
        regenerate_support_pin,
    ]
    autocomplete_fields = ("bot", "account")
    inlines = [OrgUploadTokenInline]

    readonly_fields = (
        "ownerid",
        "external_id",
        "username",
        "service",
        "email",
        "name",
        "service_id",
        "createstamp",
        "parent_service_id",
        "root_parent_service_id",
        "private_access",
        "cache",
        "invoice_details",
        "yaml",
        "updatestamp",
        "permission_list",
        "student",
        "student_created_at",
        "student_updated_at",
        "user_link",
        "trial_fired_by_link",
        "stripe_customer_link",
        "stripe_subscription_link",
        "sentry_user_id",
        "support_pin",
        "plan_activated_users_list",
        "admins_list",
        "github_app_installations_table",
    )

    fieldsets = [
        (
            None,
            {
                "fields": [
                    "ownerid",
                    "external_id",
                    "username",
                    "service",
                    "name",
                    "service_id",
                    "student",
                    "user_link",
                    "sentry_user_id",
                ]
            },
        ),
        (
            "Trial fields",
            {
                "fields": [
                    "trial_status",
                    "trial_fired_by_link",
                    "trial_start_date",
                    "trial_end_date",
                ]
            },
        ),
        (
            "Plan fields - if the Owner has an Account, none of these fields are used, refer to the ones on the Account",
            {
                "fields": [
                    "account",
                    "plan_auto_activate",
                    "plan",
                    "plan_user_count",
                    "free",
                    "plan_activated_users",
                    "plan_activated_users_list",
                ]
            },
        ),
        (
            "Billing fields",
            {
                "fields": [
                    "uses_invoice",
                    "delinquent",
                    "stripe_customer_id",
                    "stripe_customer_link",
                    "stripe_subscription_id",
                    "stripe_subscription_link",
                    "plan_provider",
                ]
            },
        ),
        (
            "GitHub App installations",
            {
                "fields": [
                    "github_app_installations_table",
                ]
            },
        ),
        (
            "Reference fields",
            {
                "fields": [
                    "admins_list",
                    "staff",
                    "upload_token_required_for_public_repos",
                    "email",
                    "parent_service_id",
                    "root_parent_service_id",
                    "private_access",
                    "cache",
                    "yaml",
                    "bot",
                    "max_upload_limit",
                    "organizations",
                    "permission_list",
                    "student_created_at",
                    "student_updated_at",
                    "onboarding_completed",
                    "did_trial",
                    "createstamp",
                    "updatestamp",
                    "support_pin",
                ]
            },
        ),
    ]

    show_full_result_count = False

    def get_queryset(self, request):
        repository_count_subquery = (
            Repository.objects.filter(author=OuterRef("pk"))
            .order_by()
            .values("author")
            .annotate(count=Count("repoid"))
            .values("count")
        )
        return (
            super()
            .get_queryset(request)
            .annotate(repository_count=Coalesce(Subquery(repository_count_subquery), 0))
            .prefetch_related("github_app_installations")
        )

    @admin.display(description="repositories", ordering="repository_count")
    def repository_count(self, obj):
        return obj.repository_count

    @admin.display(description="GitHub App installations")
    def github_app_installations_summary(self, obj):
        # Comma-separated summary for the changelist. Relies on the
        # `github_app_installations` prefetch in `get_queryset`.
        return installation_summary(obj.github_app_installations.all())

    @admin.display(description="GitHub App installations")
    def github_app_installations_table(self, obj):
        return installation_table(obj.github_app_installations.all())

    @admin.display(description="Plan")
    def plan_display(self, obj):
        # An owner's plan lives on its Account when one is attached.
        if obj.account_id:
            return obj.account.plan
        return obj.plan

    @admin.display(description="Seats (taken / total)")
    def seats_display(self, obj):
        if obj.account_id:
            taken = obj.account.activated_user_count
            total = obj.account.total_seat_count
        else:
            taken = len(obj.plan_activated_users or [])
            total = (obj.plan_user_count or 0) + (obj.free or 0)
        return f"{taken} / {total}"

    @admin.display(description="User")
    def user_link(self, obj):
        user = obj.user
        if user is None:
            return "-"
        if user.name and user.email:
            label = f"{user.name} ({user.email})"
        else:
            label = user.name or user.email or user.pk
        url = reverse("admin:codecov_auth_user_change", args=[user.pk])
        return format_html('<a href="{}">{}</a>', url, label)

    @admin.display(description="Trial fired by")
    def trial_fired_by_link(self, obj):
        if not obj.trial_fired_by:
            return "-"
        owner = Owner.objects.filter(ownerid=obj.trial_fired_by).first()
        if owner is None:
            return obj.trial_fired_by
        url = reverse("admin:codecov_auth_owner_change", args=[owner.ownerid])
        return format_html('<a href="{}">{}</a>', url, owner.username or owner.ownerid)

    @admin.display(description="Stripe customer")
    def stripe_customer_link(self, obj):
        # We don't store Stripe invoices/charges locally; the Stripe dashboard
        # is the source of truth for this customer's billing records.
        if not obj.stripe_customer_id:
            return "-"
        url = f"https://dashboard.stripe.com/customers/{obj.stripe_customer_id}"
        return format_html(
            '<a href="{}" target="_blank" rel="noopener noreferrer">{}</a>',
            url,
            obj.stripe_customer_id,
        )

    @admin.display(description="Stripe subscription")
    def stripe_subscription_link(self, obj):
        if not obj.stripe_subscription_id:
            return "-"
        url = f"https://dashboard.stripe.com/subscriptions/{obj.stripe_subscription_id}"
        return format_html(
            '<a href="{}" target="_blank" rel="noopener noreferrer">{}</a>',
            url,
            obj.stripe_subscription_id,
        )

    def _links_table(self, ids, objects, url_name, label_fn, headers):
        """Render `ids` as a two-column table linking each to its admin page.

        `objects` maps id -> instance for the ids that still resolve; unresolved
        ids are shown inline as missing.
        """
        if not ids:
            return "-"
        rows = format_html_join(
            "",
            "<tr><td style='padding:2px 16px 2px 0'>{}</td><td>{}</td></tr>",
            (
                (
                    obj_id,
                    (
                        format_html(
                            '<a href="{}">{}</a>',
                            reverse(url_name, args=[obj_id]),
                            label_fn(objects[obj_id]),
                        )
                        if obj_id in objects
                        else format_html("<em>missing ({})</em>", obj_id)
                    ),
                )
                for obj_id in ids
            ),
        )
        return format_html(
            "<table><thead><tr>"
            "<th style='text-align:left;padding-right:16px'>{}</th>"
            "<th style='text-align:left'>{}</th>"
            "</tr></thead><tbody>{}</tbody></table>",
            headers[0],
            headers[1],
            rows,
        )

    def _owners_table(self, ownerids):
        owners = {
            owner.ownerid: owner
            for owner in Owner.objects.filter(ownerid__in=ownerids or [])
        }
        return self._links_table(
            ownerids,
            owners,
            "admin:codecov_auth_owner_change",
            lambda owner: owner.username or owner.ownerid,
            ("Owner ID", "Username"),
        )

    def _repo_slug(self, repo):
        author = repo.author
        if author is not None:
            return f"{author.service}:{author.username}/{repo.name}"
        return repo.name or repo.repoid

    def _repos_table(self, repoids):
        repos = {
            repo.repoid: repo
            for repo in Repository.objects.filter(
                repoid__in=repoids or []
            ).select_related("author")
        }
        return self._links_table(
            repoids,
            repos,
            "admin:core_repository_change",
            self._repo_slug,
            ("Repo ID", "Repository"),
        )

    @admin.display(description="Plan activated users (linked)")
    def plan_activated_users_list(self, obj):
        return self._owners_table(obj.plan_activated_users)

    @admin.display(description="Admins")
    def admins_list(self, obj):
        ownerids = obj.admins or []
        return format_html(
            "<div style='margin-bottom:6px'>Total: {}</div>{}",
            len(ownerids),
            self._owners_table(ownerids),
        )

    @admin.display(description="Permission")
    def permission_list(self, obj):
        repoids = obj.permission or []
        return format_html(
            "<div style='margin-bottom:6px'>Total: {}</div>{}",
            len(repoids),
            self._repos_table(repoids),
        )

    def get_form(self, request, obj=None, change=False, **kwargs):
        form = super().get_form(request, obj, change, **kwargs)
        PLANS_CHOICES = [
            (x, x)
            for x in Plan.objects.filter(is_active=True).values_list("name", flat=True)
        ]
        form.base_fields["plan"].widget = Select(
            choices=BLANK_CHOICE_DASH + PLANS_CHOICES
        )
        form.base_fields["uses_invoice"].widget = CheckboxInput()

        is_superuser = request.user.is_superuser
        if not is_superuser:
            form.base_fields["staff"].disabled = True

        field = form.base_fields["account"]
        field.widget.can_add_related = False
        field.widget.can_change_related = False
        field.widget.can_delete_related = False

        # workaround for when a model field has null=True without blank=True
        for field_name in [
            "trial_start_date",
            "trial_end_date",
            "trial_status",
            "free",
        ]:
            if form.base_fields.get(field_name):
                form.base_fields[field_name].required = False

        return form

    def get_actions(self, request):
        actions = super().get_actions(request)
        # The "Regenerate support PIN" action is only available to staff users.
        if not (request.user and request.user.is_staff):
            actions.pop("regenerate_support_pin", None)
        return actions

    def has_add_permission(self, _, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return bool(request.user and request.user.is_staff)

    def delete_queryset(self, request, queryset) -> None:
        originator_user_id = _originator_user_id(request)
        for owner in queryset:
            TaskService().delete_owner(
                ownerid=owner.ownerid, originator_user_id=originator_user_id
            )

    def delete_model(self, request, obj) -> None:
        TaskService().delete_owner(
            ownerid=obj.ownerid, originator_user_id=_originator_user_id(request)
        )

    def get_deleted_objects(self, objs, request):
        return [], {}, set(), []

    def save_related(self, request: HttpRequest, form, formsets, change: bool) -> None:
        if formsets:
            formset = formsets[0]
            token_id = formset.data.get("organization_tokens-0-id")
            token_refresh = formset.data.get("organization_tokens-0-REFRESH")
            # token_id only exists if the token already exists (edit operation)
            if formset.is_valid() and token_id and token_refresh:
                OrgLevelTokenService.refresh_token(token_id)
        return super().save_related(request, form, formsets, change)


@admin.register(GithubAppInstallation)
class GithubAppInstallationAdmin(AdminMixin, admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "owner_link",
        "app_id",
        "installation_id",
        "coverage",
        "is_suspended",
    )
    list_filter = ("is_suspended", "name")
    list_select_related = ("owner",)
    search_fields = (
        "owner__username",
        "name",
        "=installation_id",
        "=app_id",
    )
    search_help_text = (
        "Search by owner username, name, installation_id (exact), or app_id (exact)"
    )
    autocomplete_fields = ("owner",)

    # Cap the covered-repositories table so an "all repos" installation for a
    # large owner doesn't try to render thousands of rows.
    COVERED_REPOS_DISPLAY_LIMIT = 200

    readonly_fields = (
        "id",
        "external_id",
        "created_at",
        "updated_at",
        "coverage",
        "covered_repositories",
    )
    fields = (
        "id",
        "external_id",
        "created_at",
        "updated_at",
        "owner",
        "name",
        "app_id",
        "installation_id",
        "coverage",
        "repository_service_ids",
        "covered_repositories",
        "pem_path",
        "is_suspended",
    )

    @admin.display(description="Owner")
    def owner_link(self, obj):
        owner = obj.owner
        if owner is None:
            return "-"
        url = reverse("admin:codecov_auth_owner_change", args=[owner.ownerid])
        return format_html('<a href="{}">{}/{}</a>', url, owner.service, owner.username)

    @admin.display(description="Coverage")
    def coverage(self, obj):
        if obj.covers_all_repos():
            return "all repos"
        return f"{len(obj.repository_service_ids or [])} repo(s)"

    @admin.display(description="Covered repositories")
    def covered_repositories(self, obj):
        """Read-only tabular list of the repos this installation covers.

        A true Django ``TabularInline`` isn't possible here: coverage is
        derived from the ``repository_service_ids`` array rather than a
        ForeignKey back to the installation. This renders the same
        information and links each row to the Repository admin page.
        """
        if obj.pk is None:
            return "-"
        if obj.covers_all_repos():
            # Every repo for the owner is covered; linking to the filtered
            # Repository changelist is friendlier than rendering thousands of rows.
            url = reverse("admin:core_repository_changelist")
            url += f"?author__ownerid__exact={obj.owner.ownerid}"
            count = obj.repository_queryset().count()
            return format_html(
                '<a href="{}">View all {} repositories for this owner</a>',
                url,
                count,
            )
        queryset = obj.repository_queryset().order_by("name")
        total = queryset.count()
        if total == 0:
            return "-"
        repos = list(queryset[: self.COVERED_REPOS_DISPLAY_LIMIT])
        rows = format_html_join(
            "",
            "<tr>"
            "<td style='padding:2px 16px 2px 0'><a href='{}'>{}</a></td>"
            "<td style='padding:2px 16px 2px 0'>{}</td>"
            "<td style='padding:2px 16px 2px 0'>{}</td>"
            "<td>{}</td>"
            "</tr>",
            (
                (
                    reverse("admin:core_repository_change", args=[repo.repoid]),
                    repo.name,
                    repo.service_id,
                    "yes" if repo.private else "no",
                    "yes" if repo.active else "no",
                )
                for repo in repos
            ),
        )
        table = format_html(
            "<table><thead><tr>"
            "<th style='text-align:left;padding-right:16px'>Name</th>"
            "<th style='text-align:left;padding-right:16px'>Service ID</th>"
            "<th style='text-align:left;padding-right:16px'>Private</th>"
            "<th style='text-align:left'>Active</th>"
            "</tr></thead><tbody>{}</tbody></table>",
            rows,
        )
        if total > len(repos):
            return format_html(
                "{}<p>Showing {} of {} covered repositories.</p>",
                table,
                len(repos),
                total,
            )
        return table

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(LogEntry)
class LogEntryAdmin(admin.ModelAdmin):
    readonly_fields = (
        "action_time",
        "user",
        "content_type",
        "object_id",
        "object_repr",
        "action_flag",
        "change_message",
    )
    list_display = ["__str__", "action_time", "user", "change_message"]
    search_fields = ("object_repr", "change_message")

    # keep only view permission
    def has_add_permission(self, request):
        return False

    def has_change_permission(self, request, obj=None):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


@admin.register(AccountsUsers)
class AccountsUsersAdmin(AdminMixin, admin.ModelAdmin):
    list_display = ("id", "user", "account")
    search_fields = (
        "account__name",
        "account__id__iexact",
        "id__iexact",
        "user__id__iexact",
        "user__name",
        "user__email",
    )
    search_help_text = "Search by account name, account id (exact), id (exact), user id (exact), user's name or email"
    autocomplete_fields = ("account", "user")

    readonly_fields = [
        "id",
        "created_at",
        "updated_at",
    ]

    fields = readonly_fields + ["account", "user"]


class PlansInline(admin.TabularInline):
    model = Plan
    extra = 1
    verbose_name_plural = "Plans (click save to commit changes)"
    verbose_name = "Plan"
    fields = [
        "name",
        "marketing_name",
        "base_unit_price",
        "billing_rate",
        "max_seats",
        "monthly_uploads_limit",
        "paid_plan",
        "is_active",
        "stripe_id",
    ]
    formfield_overrides = {
        Plan._meta.get_field("benefits"): {"widget": Textarea(attrs={"rows": 3})},
    }


@admin.register(Tier)
class TierAdmin(admin.ModelAdmin):
    list_display = (
        "tier_name",
        "bundle_analysis",
        "test_analytics",
        "flaky_test_detection",
        "project_coverage",
        "private_repo_support",
    )
    list_editable = (
        "bundle_analysis",
        "test_analytics",
        "flaky_test_detection",
        "project_coverage",
        "private_repo_support",
    )
    search_fields = ("tier_name__iregex",)
    inlines = [PlansInline]
    fields = [
        "tier_name",
        "bundle_analysis",
        "test_analytics",
        "flaky_test_detection",
        "project_coverage",
        "private_repo_support",
    ]


class PlanAdminForm(forms.ModelForm):
    class Meta:
        model = Plan
        fields = "__all__"

    def clean_base_unit_price(self) -> int | None:
        base_unit_price = self.cleaned_data.get("base_unit_price")
        if base_unit_price is not None and base_unit_price < 0:
            raise forms.ValidationError("Base unit price cannot be negative.")
        return base_unit_price

    def clean_max_seats(self) -> int | None:
        max_seats = self.cleaned_data.get("max_seats")
        if max_seats is not None and max_seats < 0:
            raise forms.ValidationError("Max seats cannot be negative.")
        return max_seats

    def clean_monthly_uploads_limit(self) -> int | None:
        monthly_uploads_limit = self.cleaned_data.get("monthly_uploads_limit")
        if monthly_uploads_limit is not None and monthly_uploads_limit < 0:
            raise forms.ValidationError("Monthly uploads limit cannot be negative.")
        return monthly_uploads_limit


@admin.register(Plan)
class PlanAdmin(admin.ModelAdmin):
    form = PlanAdminForm
    list_display = (
        "name",
        "marketing_name",
        "is_active",
        "tier",
        "paid_plan",
        "billing_rate",
        "base_unit_price",
        "max_seats",
        "monthly_uploads_limit",
    )
    list_filter = ("is_active", "paid_plan", "billing_rate", "tier")
    search_fields = ("name__iregex", "marketing_name__iregex")
    fields = [
        "tier",
        "name",
        "marketing_name",
        "base_unit_price",
        "benefits",
        "billing_rate",
        "is_active",
        "max_seats",
        "monthly_uploads_limit",
        "paid_plan",
        "stripe_id",
    ]
    formfield_overrides = {
        Plan._meta.get_field("benefits"): {"widget": Textarea(attrs={"rows": 3})},
    }
    autocomplete_fields = ["tier"]  # a dropdown for selecting related Tiers


# TBD: Presigned URL might be manually created by infra so still adding this but may be removed later.
@admin.register(OwnerExport)
class OwnerExportAdmin(AdminMixin, admin.ModelAdmin):
    """Admin for viewing and managing owner data exports."""

    list_display = (
        "id",
        "owner_link",
        "status",
        "error_message",
        "created_at",
        "download_expires_at",
        "download_link",
    )
    list_filter = ("status",)
    search_fields = ("owner__username__iregex", "owner__ownerid")
    readonly_fields = (
        "id",
        "owner",
        "since_date",
        "status",
        "error_message",
        "download_url",
        "download_expires_at",
        "created_by",
        "created_at",
        "updated_at",
        "task_ids",
        "stats",
    )

    def owner_link(self, obj):
        if obj.owner:
            return format_html(
                '<a href="/admin/codecov_auth/owner/{}/change/">{}</a>',
                obj.owner.ownerid,
                obj.owner.username,
            )
        return "-"

    owner_link.short_description = "Owner"

    def download_link(self, obj):
        if obj.download_url and obj.status == OwnerExport.Status.COMPLETED:
            expires = obj.download_expires_at
            if expires and expires > timezone.now():
                return format_html(
                    '<a href="{}" target="_blank">Download</a> (expires {})',
                    obj.download_url,
                    expires.strftime("%Y-%m-%d %H:%M"),
                )
            return "Expired"
        return "-"

    download_link.short_description = "Download"

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return True


@admin.register(OwnerToBeDeleted)
class OwnerToBeDeletedAdmin(admin.ModelAdmin):
    """Owners queued for deletion. Members/Admins view; only Admins act."""

    list_display = (
        "id",
        "owner_link",
        "requested_by_link",
        "on_hold",
        "created_at",
        "updated_at",
    )
    list_filter = ("on_hold",)
    search_fields = ("owner_id",)
    search_help_text = "Search by owner id (exact)"
    ordering = ("-created_at",)
    list_select_related = ("requested_by",)
    actions = ["place_on_hold", "release_from_hold", "reconnect_owner"]

    readonly_fields = (
        "id",
        "owner_id",
        "owner_link",
        "requested_by_link",
        "on_hold",
        "created_at",
        "updated_at",
    )
    fields = readonly_fields

    def _can_view(self, request: HttpRequest) -> bool:
        return get_staff_role(request) in (
            User.StaffRole.MEMBER,
            User.StaffRole.ADMIN,
        )

    def _can_manage(self, request: HttpRequest) -> bool:
        return get_staff_role(request) == User.StaffRole.ADMIN

    def has_module_permission(self, request: HttpRequest) -> bool:
        return self._can_view(request)

    def has_view_permission(self, request: HttpRequest, obj=None) -> bool:
        return self._can_view(request)

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def get_actions(self, request: HttpRequest):
        if not self._can_manage(request):
            return {}
        return super().get_actions(request)

    @admin.display(description="Owner")
    def owner_link(self, obj):
        owner = Owner.objects.filter(ownerid=obj.owner_id).first()
        if owner is None:
            return format_html("<em>missing ({})</em>", obj.owner_id)
        url = reverse("admin:codecov_auth_owner_change", args=[owner.ownerid])
        return format_html('<a href="{}">{}</a>', url, owner.username or owner.ownerid)

    @admin.display(description="Requested by")
    def requested_by_link(self, obj):
        user = obj.requested_by
        if user is None:
            return "-"
        if user.name and user.email:
            label = f"{user.name} ({user.email})"
        else:
            label = user.name or user.email or user.pk
        url = reverse("admin:codecov_auth_user_change", args=[user.pk])
        return format_html('<a href="{}">{}</a>', url, label)

    @admin.action(description="Place on hold (prevent deletion)")
    def place_on_hold(self, request, queryset):
        updated = queryset.update(on_hold=True)
        log.info(
            "Owners-to-be-deleted placed on hold via admin",
            extra={
                "owner_ids": list(queryset.values_list("owner_id", flat=True)),
                "actor_user_id": _originator_user_id(request),
            },
        )
        self.message_user(
            request,
            f"Placed {updated} row(s) on hold. These owners will be skipped by "
            "the deletion cron until the hold is released.",
            level=messages.SUCCESS,
        )

    @admin.action(description="Release hold (allow deletion in a future cycle)")
    def release_from_hold(self, request, queryset):
        updated = queryset.update(on_hold=False)
        log.info(
            "Owners-to-be-deleted released from hold via admin",
            extra={
                "owner_ids": list(queryset.values_list("owner_id", flat=True)),
                "actor_user_id": _originator_user_id(request),
            },
        )
        self.message_user(
            request,
            f"Released hold on {updated} row(s). These owners are now eligible "
            "for deletion in a future cron cycle.",
            level=messages.SUCCESS,
        )

    def _render_reconnect(self, request, template, record, context):
        return render(
            request,
            template,
            context={
                **self.admin_site.each_context(request),
                "opts": self.model._meta,
                "record": record,
                "action_checkbox_name": ACTION_CHECKBOX_NAME,
                **context,
            },
        )

    @admin.action(description="Reconnect / undelete the original owner")
    def reconnect_owner(self, request, queryset):
        if queryset.count() != 1:
            self.message_user(
                request,
                "Select exactly one row to reconnect.",
                level=messages.ERROR,
            )
            return

        record = queryset.first()
        if not record.on_hold:
            self.message_user(
                request,
                "Place the row on hold before reconnecting an owner.",
                level=messages.ERROR,
            )
            return

        original = Owner.objects.filter(ownerid=record.owner_id).first()
        if original is None:
            self.message_user(
                request,
                f"Original owner {record.owner_id} no longer exists.",
                level=messages.ERROR,
            )
            return

        step = request.POST.get("reconnect_step")

        if step == "confirm" and "do_confirm" in request.POST:
            source = Owner.objects.filter(
                ownerid=request.POST.get("source_ownerid")
            ).first()
            if source is None or source.ownerid == original.ownerid:
                self.message_user(
                    request,
                    "Select a valid owner to reconnect (not the original).",
                    level=messages.ERROR,
                )
                return
            reconnect_owner_merge(
                original_ownerid=original.ownerid,
                source_ownerid=source.ownerid,
                actor_user_id=_originator_user_id(request),
            )
            self.message_user(
                request,
                f"Reconnected owner {original.ownerid}: merged data from owner "
                f"{source.ownerid} and removed the deletion request.",
                level=messages.SUCCESS,
            )
            return

        if step == "select" and "do_continue" in request.POST:
            source = Owner.objects.filter(
                ownerid=request.POST.get("source_ownerid")
            ).first()
            if source is not None and source.ownerid != original.ownerid:
                preview = build_reconnect_preview(original, source)
                return self._render_reconnect(
                    request,
                    "admin/codecov_auth/ownertobedeleted/reconnect_confirm.html",
                    record,
                    {
                        "title": "Confirm reconnect",
                        "original": original,
                        "source": source,
                        "identity_changes": preview.identity_changes,
                        "related_counts": preview.related_counts,
                    },
                )
            self.message_user(
                request,
                "Select a valid owner to reconnect (not the original).",
                level=messages.ERROR,
            )

        query = (request.POST.get("owner_search") or "").strip()
        candidates = []
        if query:
            search = (
                Q(username__icontains=query)
                | Q(name__icontains=query)
                | Q(email__icontains=query)
                | Q(service_id=query)
            )
            if query.isdigit():
                search = search | Q(ownerid=int(query))
            candidates = list(
                Owner.objects.exclude(ownerid=original.ownerid)
                .exclude(service=Service.TO_BE_DELETED.value)
                .filter(search)
                .order_by("-ownerid")[:25]
            )

        return self._render_reconnect(
            request,
            "admin/codecov_auth/ownertobedeleted/reconnect_select.html",
            record,
            {
                "title": "Reconnect owner",
                "original": original,
                "query": query,
                "candidates": candidates,
            },
        )
