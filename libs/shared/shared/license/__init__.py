from dataclasses import dataclass
from datetime import datetime


@dataclass
class LicenseInformation:
    """
    License information for self-hosted deployments.

    Note: License enforcement has been removed. Self-hosted deployments
    no longer require a license. This dataclass is kept for backward
    compatibility with existing API endpoints and UI components.
    """

    is_valid: bool = True
    is_trial: bool = False
    message: str = None
    url: str = None
    number_allowed_users: int = None
    number_allowed_repos: int = None
    expires: datetime = None
    is_pr_billing: bool = False


def get_current_license() -> LicenseInformation:
    """
    Returns license information for self-hosted deployments.

    License enforcement has been removed - self-hosted deployments
    are always considered valid with no user or repo limits.
    """
    return LicenseInformation(
        is_valid=True,
        message=None,
        number_allowed_users=None,  # No limit
        number_allowed_repos=None,  # No limit
        is_pr_billing=True,  # Default to PR billing for enterprise
    )
