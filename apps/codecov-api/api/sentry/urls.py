from django.urls import path

from .views import account_link

urlpatterns = [
    path("internal/account/link/", account_link, name="account-link"),
]
