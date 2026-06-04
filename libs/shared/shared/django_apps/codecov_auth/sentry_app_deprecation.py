"""Transitional helpers for the Sentry GitHub App deprecation.

TEMPORARY — delete this entire module at the deprecation cleanup (~2026-07-20).
It exists only to detect owners whose sole GitHub app installation is the legacy
Sentry app, so we can show them a one-time migration notice. Once the deprecation
window closes and the messaging is removed, nothing should import from here.
"""

from shared.django_apps.codecov_auth.models import GithubAppInstallation

# Sentry's GitHub app, which Codecov historically used for PR notifications
SENTRY_APP_ID = 12637

# TODO(before merge): 30 days after the ~June 12 trigger/announcement date.
# Both the trigger date and this value are still moving ("currently" June 12),
# so confirm the final date with the comms/timeline owner before merge.
SENTRY_APP_DEPRECATION_DATE = "July 12, 2026"


def is_owner_only_using_sentry_app(owner_id: int) -> bool:
    installations = GithubAppInstallation.objects.filter(owner_id=owner_id)
    count = installations.count()
    if count != 1:
        return False
    return installations.filter(app_id=SENTRY_APP_ID).exists()
