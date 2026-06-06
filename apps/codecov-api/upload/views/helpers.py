import logging

from codecov_auth.models import Owner, Service
from core.models import Repository

log = logging.getLogger(__name__)


def get_repository_and_owner_from_string(
    service: Service, repo_identifier: str
) -> tuple[Repository | None, Owner | None]:
    if not isinstance(service, Service):
        # if we pass this value to the db, it just raises DataError
        # No need for that
        return None, None

    if "::::" not in repo_identifier:
        return None, None

    owner_identifier, repo_name_identifier = repo_identifier.rsplit("::::", 1)
    owner = _get_owner_from_string(service, owner_identifier)
    if not owner:
        return None, None
    try:
        repository = Repository.objects.get(author=owner, name=repo_name_identifier)
    except Repository.DoesNotExist:
        return None, None

    return repository, owner


def get_or_create_repository_from_string(
    service: Service, repo_identifier: str
) -> tuple[Repository | None, Owner | None]:
    """
    Like get_repository_and_owner_from_string, but auto-creates the Repository
    record if it does not yet exist in the database.  Used by upload endpoints
    that authenticate via an org-level token so that a first upload for a
    previously-unseen repository does not fail with NotFound.
    """
    if not isinstance(service, Service):
        return None, None

    if "::::" not in repo_identifier:
        return None, None

    owner_identifier, repo_name_identifier = repo_identifier.rsplit("::::", 1)
    owner = _get_owner_from_string(service, owner_identifier)
    if not owner:
        return None, None

    repository, created = Repository.objects.get_or_create(
        author=owner,
        name=repo_name_identifier,
        defaults={
            "private": False,
            "active": False,
            "activated": False,
        },
    )

    if created:
        log.info(
            "Auto-created repository during bundle analysis upload",
            extra={
                "owner": owner.username,
                "service": service.value,
                "repo": repo_name_identifier,
            },
        )

    return repository, owner


def _get_owner_from_string(service: Service, owner_identifier: str) -> Owner | None:
    if ":::" in owner_identifier:
        owner_identifier = owner_identifier.replace(":::", ":")
    try:
        return Owner.objects.get(service=service, username=owner_identifier)
    except Owner.DoesNotExist:
        return None


def get_repository_from_string(
    service: Service, repo_identifier: str
) -> Repository | None:
    repository, _ = get_repository_and_owner_from_string(
        service=service, repo_identifier=repo_identifier
    )
    return repository
