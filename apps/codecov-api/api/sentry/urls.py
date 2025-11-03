from django.urls import path

from .views import account_link, account_unlink, test_analytics_eu

urlpatterns = [
    path("internal/account/link/", account_link, name="account-link"),
    path("internal/account/unlink/", account_unlink, name="account-unlink"),
    path("internal/test-analytics/eu/", test_analytics_eu, name="test-analytics-eu"),
]
