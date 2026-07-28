from django.contrib import admin
from django.db.models import Count, Q
from django.http import HttpRequest
from django.urls import reverse
from django.utils.html import format_html, format_html_join

from billing.models import StripeBilling
from shared.django_apps.codecov_auth.models import Owner

STRIPE_DASHBOARD = "https://dashboard.stripe.com"


@admin.register(StripeBilling)
class StripeBillingAdmin(admin.ModelAdmin):
    """Read-only lookup of owner-backed Stripe customers."""

    list_display = (
        "stripe_customer_id",
        "owner_link",
        "account_link",
        "plan_display",
        "seats_display",
        "stripe_subscription_id",
        "owner_count",
    )
    list_filter = ("service",)
    ordering = ("ownerid",)
    # Non-empty search_fields is what makes Django render the search bar;
    # get_search_results below owns the actual matching.
    search_fields = ("stripe_customer_id",)
    search_help_text = (
        "Search by Stripe customer id (primary), subscription id, owner "
        "username, owner id, account name, or account id"
    )

    fields = (
        "customer_id_link",
        "subscription_id_link",
        "owner_link",
        "account_link",
        "plan_display",
        "seats_display",
        "associated_owners",
        "ownerid",
        "service",
    )
    readonly_fields = fields

    def get_queryset(self, request: HttpRequest):
        return (
            super()
            .get_queryset(request)
            .filter(stripe_customer_id__isnull=False)
            .exclude(stripe_customer_id="")
            .select_related("account")
            .annotate(owners_total=Count("account__organizations", distinct=True))
        )

    def get_search_results(self, request: HttpRequest, queryset, search_term: str):
        term = (search_term or "").strip()
        if not term:
            return queryset, False

        query = (
            Q(stripe_customer_id__icontains=term)
            | Q(stripe_subscription_id__icontains=term)
            | Q(username__icontains=term)
            | Q(account__name__icontains=term)
        )
        if term.isdigit():
            query |= Q(ownerid=int(term)) | Q(account_id=int(term))

        return queryset.filter(query), False

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        return False

    @admin.display(description="Owner", ordering="username")
    def owner_link(self, obj: StripeBilling):
        return format_html(
            '<a href="{}">{}</a>',
            reverse("admin:codecov_auth_owner_change", args=[obj.ownerid]),
            obj.username or obj.ownerid,
        )

    @admin.display(description="Account", ordering="account__name")
    def account_link(self, obj: StripeBilling):
        account = obj.account
        if account is None:
            return "-"
        return format_html(
            '<a href="{}">{} (id={})</a>',
            reverse("admin:codecov_auth_account_change", args=[account.id]),
            account.name or "(unnamed)",
            account.id,
        )

    @admin.display(description="Owners", ordering="owners_total")
    def owner_count(self, obj: StripeBilling):
        return getattr(obj, "owners_total", 0) or 1

    @admin.display(description="Plan")
    def plan_display(self, obj: StripeBilling):
        if obj.account_id:
            return obj.account.plan
        return obj.plan or "-"

    @admin.display(description="Seats (used / total)")
    def seats_display(self, obj: StripeBilling):
        if obj.account_id:
            taken = obj.account.activated_user_count
            total = obj.account.total_seat_count
        else:
            taken = len(obj.plan_activated_users or [])
            total = (obj.plan_user_count or 0) + (obj.free or 0)
        return f"{taken} / {total}"

    @admin.display(description="Associated owners")
    def associated_owners(self, obj: StripeBilling):
        owners = (
            Owner.objects.filter(account_id=obj.account_id).order_by("ownerid")
            if obj.account_id is not None
            else Owner.objects.filter(ownerid=obj.ownerid)
        )

        rows = format_html_join(
            "",
            "<tr><td style='padding:2px 16px 2px 0'>{}</td>"
            "<td style='padding:2px 16px 2px 0'>{}</td>"
            "<td>{}</td></tr>",
            (
                (
                    owner.ownerid,
                    owner.service or "-",
                    format_html(
                        '<a href="{}">{}</a>',
                        reverse(
                            "admin:codecov_auth_owner_change", args=[owner.ownerid]
                        ),
                        owner.username or owner.ownerid,
                    ),
                )
                for owner in owners
            ),
        )
        return format_html(
            "<div style='margin-bottom:6px'>Total: {}</div>"
            "<table><thead><tr>"
            "<th style='text-align:left;padding-right:16px'>Owner ID</th>"
            "<th style='text-align:left;padding-right:16px'>Service</th>"
            "<th style='text-align:left'>Username</th>"
            "</tr></thead><tbody>{}</tbody></table>",
            len(owners),
            rows,
        )

    @admin.display(description="Stripe customer id")
    def customer_id_link(self, obj: StripeBilling):
        if not obj.stripe_customer_id:
            return "-"
        return format_html(
            '{} &nbsp;<a href="{}/customers/{}" target="_blank" '
            'rel="noopener noreferrer">view in Stripe &rarr;</a>',
            obj.stripe_customer_id,
            STRIPE_DASHBOARD,
            obj.stripe_customer_id,
        )

    @admin.display(description="Stripe subscription id")
    def subscription_id_link(self, obj: StripeBilling):
        if not obj.stripe_subscription_id:
            return "-"
        return format_html(
            '{} &nbsp;<a href="{}/subscriptions/{}" target="_blank" '
            'rel="noopener noreferrer">view in Stripe &rarr;</a>',
            obj.stripe_subscription_id,
            STRIPE_DASHBOARD,
            obj.stripe_subscription_id,
        )
