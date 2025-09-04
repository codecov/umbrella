import logging
from enum import Enum

from codecov_auth.models import GITHUB_APP_INSTALLATION_DEFAULT_NAME, Owner
from core.models import Repository
from rest_framework.exceptions import NotFound
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

    if owner and owner.account and owner.account.sentry_org_id:
        return {HANDLER.SENTRY}

    return {HANDLER.GITHUB}


def get_repo_from_webhook(request_data, service_name: str):
    repo_data = request_data.get("repository", {})
    repo_service_id = repo_data.get("id")
    owner_service_id = repo_data.get("owner", {}).get("id")
    repo_slug = repo_data.get("full_name")

    try:
        owner = Owner.objects.get(
            service=service_name, service_id=owner_service_id
        )
    except Owner.DoesNotExist:
        try:
            return Repository.objects.get(
                author__service=service_name, service_id=repo_service_id
            )
        except Repository.DoesNotExist:
            return None
    else:
        try:
            return Repository.objects.get(
                author__ownerid=owner.ownerid, service_id=repo_service_id
            )
        except Repository.DoesNotExist:
            default_ghapp_installation = owner.github_app_installations.filter(
                name=GITHUB_APP_INSTALLATION_DEFAULT_NAME
            ).first()
            if default_ghapp_installation or owner.integration_id:
                return Repository.objects.get_or_create_from_git_repo(
                    repo_data, owner
                )[0]
            return None
