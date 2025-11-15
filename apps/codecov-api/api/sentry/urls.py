from django.urls import path

from .views import (
    account_link,
    account_unlink,
    create_ta_export,
    get_ta_export,
)

urlpatterns = [
    path("internal/account/link/", account_link, name="account-link"),
    path("internal/account/unlink/", account_unlink, name="account-unlink"),
    path("internal/test-analytics/exports/", create_ta_export, name="create-ta-export"),
    path(
        "internal/test-analytics/exports/<str:task_id>/",
        get_ta_export,
        name="get-ta-export",
    ),
]
