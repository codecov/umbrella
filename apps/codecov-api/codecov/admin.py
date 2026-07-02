from django.contrib import admin
from django.core.exceptions import PermissionDenied
from django.http import HttpRequest
from django.urls import reverse
from django.utils.html import format_html
from django_better_admin_arrayfield.admin.mixins import DynamicArrayMixin

from codecov.forms import AutocompleteSearchForm
from codecov_auth.models import User
from shared.django_apps.rollouts.models import FeatureFlag, FeatureFlagVariant

# App labels whose models are hidden entirely from Viewer-level staff. These are
# the Redis/Celery queue admin surfaces, which Viewers must not be able to read.
VIEWER_RESTRICTED_APP_LABELS = {"redis_admin"}


def get_staff_role(request: HttpRequest) -> "User.StaffRole | None":
    """Resolve the RBAC role for the request's user (``None`` if not staff)."""
    user = getattr(request, "user", None)
    return getattr(user, "effective_staff_role", None)


def is_viewer(request: HttpRequest) -> bool:
    return get_staff_role(request) == User.StaffRole.VIEWER


def deny_viewers(request: HttpRequest) -> None:
    """Raise ``PermissionDenied`` for Viewer-level staff.

    Custom admin views registered via ``get_urls`` are wrapped in
    ``admin_site.admin_view`` which only checks ``is_staff``. Call this at the
    top of any such view that performs (or triggers) a write so Viewers, who are
    read-only, cannot invoke it via a GET link.
    """
    if is_viewer(request):
        raise PermissionDenied


class RBACAdminMixin:
    """Enforces the Viewer RBAC level on every registered ``ModelAdmin``.

    Members and Admins fall through to the wrapped admin's own logic (which
    still gates superuser-only fields), preserving today's behaviour. Viewers
    get read-only access, no actions, and no visibility into Redis queue models.
    """

    def _viewer_restricted_model(self) -> bool:
        return self.model._meta.app_label in VIEWER_RESTRICTED_APP_LABELS

    def has_view_permission(self, request: HttpRequest, obj=None) -> bool:
        if is_viewer(request) and self._viewer_restricted_model():
            return False
        return super().has_view_permission(request, obj)

    def has_module_permission(self, request: HttpRequest) -> bool:
        if is_viewer(request) and self._viewer_restricted_model():
            return False
        return super().has_module_permission(request)

    def has_add_permission(self, request: HttpRequest, *args, **kwargs) -> bool:
        if is_viewer(request):
            return False
        return super().has_add_permission(request, *args, **kwargs)

    def has_change_permission(self, request: HttpRequest, obj=None) -> bool:
        if is_viewer(request):
            return False
        return super().has_change_permission(request, obj)

    def has_delete_permission(self, request: HttpRequest, obj=None) -> bool:
        if is_viewer(request):
            return False
        return super().has_delete_permission(request, obj)

    def get_actions(self, request: HttpRequest):
        if is_viewer(request):
            return {}
        return super().get_actions(request)

    def get_readonly_fields(self, request: HttpRequest, obj=None):
        readonly = super().get_readonly_fields(request, obj)
        if not is_viewer(request):
            return readonly
        # Belt-and-suspenders: Django already renders a view-only change form,
        # but we also force every concrete field read-only.
        field_names = {field.name for field in self.model._meta.fields}
        field_names.update(readonly)
        return tuple(field_names)

    def get_inline_instances(self, request: HttpRequest, obj=None):
        inline_instances = super().get_inline_instances(request, obj)
        if not is_viewer(request):
            return inline_instances
        # Render inlines read-only so Viewers cannot add/edit/delete related rows.
        for inline in inline_instances:
            inline.can_delete = False
            inline.max_num = 0
            inline.extra = 0
            inline.has_add_permission = lambda request, obj=None: False
            inline.has_change_permission = lambda request, obj=None: False
            inline.has_delete_permission = lambda request, obj=None: False
        return inline_instances


class CodecovAdminSite(admin.AdminSite):
    """Default admin site that transparently applies :class:`RBACAdminMixin`.

    Every model registered via ``@admin.register`` / ``admin.site.register`` is
    wrapped so RBAC is enforced centrally without editing each app's admin.
    """

    def register(self, model_or_iterable, admin_class=None, **options) -> None:
        base = admin_class or admin.ModelAdmin
        if not issubclass(base, RBACAdminMixin):
            base = type(base.__name__, (RBACAdminMixin, base), {})
        super().register(model_or_iterable, base, **options)


class AdminMixin:
    def save_model(self, request, new_obj, form, change) -> None:
        if change:
            old_obj = self.model.objects.get(pk=new_obj.pk)
            new_obj.changed_fields = {}

            for changed_field in form.changed_data:
                prev_value = getattr(old_obj, changed_field)
                new_value = getattr(new_obj, changed_field)
                new_obj.changed_fields[changed_field] = (
                    f"prev value: {prev_value}, new value: {new_value}"
                )

        return super().save_model(request, new_obj, form, change)

    def log_change(self, request, object, message):
        message.append(object.changed_fields)
        return super().log_change(request, object, message)


class FeatureFlagVariantInline(admin.StackedInline):
    model = FeatureFlagVariant
    exclude = ["override_repo_ids", "override_owner_ids"]
    fields = ["name", "proportion", "value", "view_link"]
    readonly_fields = [
        "view_link",
    ]
    extra = 0

    def view_link(self, obj):
        link = reverse(
            "admin:rollouts_featureflagvariant_change", args=[obj.variant_id]
        )
        return format_html('<a href="{}">View</a>', link)

    view_link.short_description = "More Details"


class FeatureFlagAdmin(admin.ModelAdmin):
    list_display = ["name", "is_active", "number_of_variants", "proportion_percentage"]
    search_fields = ["name"]
    inlines = [FeatureFlagVariantInline]

    def number_of_variants(self, obj):
        return obj.variants.count()

    number_of_variants.short_description = "# of Variants"

    def proportion_percentage(self, obj):
        return str(round(obj.proportion * 100)) + "%"

    proportion_percentage.short_description = "Experiment Proportion"


class FeatureFlagVariantAdmin(admin.ModelAdmin, DynamicArrayMixin):
    list_display = ["variant_id", "name", "feature_flag"]
    search_fields = ["variant_id", "name", "feature_flag__name"]
    form = AutocompleteSearchForm


admin.site.register(FeatureFlag, FeatureFlagAdmin)
admin.site.register(FeatureFlagVariant, FeatureFlagVariantAdmin)
