from django.apps import AppConfig


class CodecovAuthConfig(AppConfig):
    name = "codecov_auth"

    def ready(self):
        import codecov_auth.signals  # noqa: F401, PLC0415

        from django.contrib import admin

        admin.site.login_template = "admin/login_with_okta.html"
