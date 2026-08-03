from django.apps import AppConfig
from django.contrib import admin


class CodecovAuthConfig(AppConfig):
    name = "codecov_auth"

    def ready(self):
        import codecov_auth.signals  # noqa: F401, PLC0415

        admin.site.login_template = "admin/login_with_okta.html"
