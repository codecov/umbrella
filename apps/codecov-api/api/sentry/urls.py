from django.urls import path

from .views import account_link, account_unlink

urlpatterns = [
    path("internal/account/link/", account_link, name="account-link"),
    path("internal/account/unlink", account_unlink, name="account-unlink"),
]
