"""Transitional helpers for the Sentry GitHub App deprecation.

TEMPORARY — delete this entire module at the deprecation cleanup (~2026-07-20).
It exists only to detect owners whose sole GitHub app installation is the legacy
Sentry app, so we can show them a one-time migration notice. Once the deprecation
window closes and the messaging is removed, nothing should import from here.
"""

from django.conf import settings

from shared.django_apps.codecov_auth.models import GithubAppInstallation

SENTRY_APP_DEPRECATION_DATE = "July 8, 2026"


def is_owner_only_using_sentry_app(owner_id: int) -> bool:
    # The Sentry GitHub app installation is recorded with app_id equal to the
    # configured GITHUB_SENTRY_APP_ID (github.sentry_merge_app_id), the same value
    # the webhook uses when it creates/updates the installation. Identifying it by
    # any other value silently fails to match real installations.
    sentry_app_id = getattr(settings, "GITHUB_SENTRY_APP_ID", None)
    if sentry_app_id is None:
        return False
    installations = GithubAppInstallation.objects.filter(owner_id=owner_id)
    count = installations.count()
    if count != 1:
        return False
    return installations.filter(app_id=sentry_app_id).exists()
