import logging
from enum import Enum

from codecov_auth.models import Owner
from shared.helpers.sentry import owner_uses_sentry
from webhook_handlers.constants import GitHubWebhookEvents

log = logging.getLogger(__name__)


def resolve_owner_from_webhook(data, service_name: str) -> Owner | None:
    repo_data = data.get("repository", {})
    if repo_data:
        owner_service_id = repo_data.get("owner", {}).get("id")
        if owner_service_id:
            try:
                return Owner.objects.get(
                    service=service_name, service_id=owner_service_id
                )
            except Owner.DoesNotExist:
                pass

    installation_data = data.get("installation", {})
    if installation_data:
        account_data = installation_data.get("account", {})
        owner_service_id = account_data.get("id")
        if owner_service_id:
            try:
                return Owner.objects.get(
                    service=service_name, service_id=owner_service_id
                )
            except Owner.DoesNotExist:
                pass

    org_data = data.get("organization", {})
    if org_data:
        owner_service_id = org_data.get("id")
        if owner_service_id:
            try:
                return Owner.objects.get(
                    service=service_name, service_id=owner_service_id
                )
            except Owner.DoesNotExist:
                pass

    return None


def is_installation_event(event: str) -> bool:
    installation_events = [
        GitHubWebhookEvents.INSTALLATION,
        GitHubWebhookEvents.INSTALLATION_REPOSITORIES,
    ]
    return event in installation_events


class HANDLER(Enum):
    GITHUB = "github"
    SENTRY = "sentry"


def should_process(data, event: str, service_name: str) -> set[HANDLER]:
    if is_installation_event(event):
        return {HANDLER.GITHUB, HANDLER.SENTRY}

    owner = resolve_owner_from_webhook(data, service_name)

    if owner_uses_sentry(owner):
        return {HANDLER.SENTRY}

    return {HANDLER.GITHUB}
