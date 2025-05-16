from django.urls import re_path

from .views import ariadne_view, sentry_ariadne_view

ALLOWED_SERVICES = [
    "gh",
    "github",
    "gl",
    "gitlab",
    "bb",
    "bitbucket",
    "ghe",
    "github_enterprise",
    "gle",
    "gitlab_enterprise",
    "bbs",
    "bitbucket_server",
    "",
]

service_regex = "|".join(ALLOWED_SERVICES)

urlpatterns = [
    re_path(
        rf"^sentry/(?P<service>({service_regex}))$",
        sentry_ariadne_view,
        name="sentry_graphql",
    ),
    re_path(rf"^(?P<service>({service_regex}))$", ariadne_view, name="graphql"),
]
