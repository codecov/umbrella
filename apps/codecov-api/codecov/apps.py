from django.contrib.admin.apps import AdminConfig


class CodecovAdminConfig(AdminConfig):
    """Use our RBAC-aware admin site as the project's default admin site."""

    default_site = "codecov.admin.CodecovAdminSite"
