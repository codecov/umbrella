from typing import Protocol


# apps/worker/database/models/core.py: Account
# libs/shared/shared/django_apps/codecov_auth/models.py: Account
class AccountProtocol(Protocol):
    sentry_org_id: int | None
    is_active: bool


# apps/worker/database/models/core.py: Owner
# libs/shared/shared/django_apps/codecov_auth/models.py: Owner
class OwnerProtocol(Protocol):
    account: AccountProtocol | None


def owner_uses_sentry(owner: OwnerProtocol | None) -> bool:
    return (
        owner
        and owner.account
        and owner.account.sentry_org_id
        and owner.account.is_active
    )
