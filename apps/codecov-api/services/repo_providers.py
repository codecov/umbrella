import logging
from collections.abc import Callable
from os import getenv

from asgiref.sync import sync_to_async
from django.conf import settings

from codecov_auth.models import (
    GITHUB_APP_INSTALLATION_DEFAULT_NAME,
    GithubAppInstallation,
    Owner,
    Service,
)
from core.models import Repository
from shared.bots.github_apps import (
    GithubInstallationInfo,
    get_github_app_token,
)
from shared.encryption.token import encode_token
from shared.torngit import get
from shared.typings.oauth_token_types import OauthConsumerToken
from utils.config import get_config
from utils.encryption import encryptor

log = logging.getLogger(__name__)


class TorngitInitializationFailed(Exception):
    """
    Exception when initializing the torngit provider object.
    """

    pass


def get_token_refresh_callback(
    owner: Owner | None, service: Service
) -> Callable[[dict], None]:
    """
    Produces a callback function that will encode and update the oauth token of an owner.
    This callback is passed to the TorngitAdapter for the service.
    """
    if owner is None:
        return None
    if service == Service.BITBUCKET or service == Service.BITBUCKET_SERVER:
        return None

    @sync_to_async
    def callback(new_token: OauthConsumerToken) -> None:
        log.info(
            "Saving new token after refresh",
            extra={"owner": owner.username, "ownerid": owner.ownerid},
        )
        string_to_save = encode_token(new_token)
        owner.oauth_token = encryptor.encode(string_to_save).decode()
        owner.save()

    return callback


def verify_ssl_config(use_ssl: bool, service: Service) -> str | None:
    if use_ssl:
        return (
            get_config(service, "ssl_pem")
            if get_config(service, "verify_ssl") is not False
            else getenv("REQUESTS_CA_BUNDLE")
        )
    else:
        return None


def get_generic_adapter_params(owner: Owner | None, service, use_ssl=False, token=None):
    verify_ssl = verify_ssl_config(use_ssl, service)

    if token is None:
        if owner is not None and owner.oauth_token is not None:
            token = encryptor.decrypt_token(owner.oauth_token)
            token["username"] = owner.username
        else:
            token = {"key": getattr(settings, f"{service.upper()}_BOT_KEY")}
    return {
        "verify_ssl": verify_ssl,
        "token": token,
        "timeouts": (5, 15),
        "oauth_consumer_token": {
            "key": getattr(settings, f"{service.upper()}_CLIENT_ID", "unknown"),
            "secret": getattr(settings, f"{service.upper()}_CLIENT_SECRET", "unknown"),
        },
        # By checking the "username" in token we can know if the token belongs to an Owner
        # We only try to refresh user-to-server tokens (e.g. belongs to owner)
        "on_token_refresh": (
            get_token_refresh_callback(owner, service) if "username" in token else None
        ),
    }


def get_sentry_adapter_params(
    owner: Owner, service: Service, ghapp: GithubAppInstallation, use_ssl=False
) -> dict:
    verify_ssl = verify_ssl_config(use_ssl, service)

    ghapp_info = GithubInstallationInfo(
        id=ghapp.id,
        installation_id=ghapp.installation_id,
        app_id=ghapp.app_id,
        pem_path=ghapp.pem_path,
    )

    token = get_github_app_token(
        service=owner.service,
        installation_info=ghapp_info,
    )
    return {
        "verify_ssl": verify_ssl,
        "token": token,
        "timeouts": (5, 15),
    }


def get_provider(service, adapter_params):
    provider = get(service, **adapter_params)
    if provider:
        return provider
    else:
        raise TorngitInitializationFailed()


def get_default_ghapp_installation(
    owner: Owner | None,
) -> GithubAppInstallation | None:
    return _get_ghapp_installation(
        owner, {"name": GITHUB_APP_INSTALLATION_DEFAULT_NAME}
    )


async def async_get_default_ghapp_installation(
    owner: Owner | None,
) -> GithubAppInstallation | None:
    return await _async_get_ghapp_installation(
        owner, {"name": GITHUB_APP_INSTALLATION_DEFAULT_NAME}
    )


def get_sentry_ghapp_installation(
    owner: Owner | None,
) -> GithubAppInstallation | None:
    return _get_ghapp_installation(owner, {"app_id": settings.SENTRY_APP_ID})


async def async_get_sentry_ghapp_installation(
    owner: Owner | None,
) -> GithubAppInstallation | None:
    return await _async_get_ghapp_installation(
        owner, {"app_id": settings.SENTRY_APP_ID}
    )


def _get_ghapp_installation(
    owner: Owner | None, filter_by: dict | None
) -> GithubAppInstallation | None:
    if owner is None or owner.service not in [
        Service.GITHUB.value,
        Service.GITHUB_ENTERPRISE.value,
    ]:
        return None
    return owner.github_app_installations.filter(**filter_by).first()


async def _async_get_ghapp_installation(
    owner: Owner | None, filter_by: dict | None
) -> GithubAppInstallation | None:
    if owner is None or owner.service not in [
        Service.GITHUB.value,
        Service.GITHUB_ENTERPRISE.value,
    ]:
        return None
    return await owner.github_app_installations.filter(**filter_by).afirst()


class RepoProviderService:
    def _is_using_integration(
        self, ghapp_installation: GithubAppInstallation | None, repo: Repository
    ) -> bool:
        if ghapp_installation:
            return ghapp_installation.is_repo_covered_by_integration(repo)
        return repo.using_integration

    async def async_get_adapter(
        self,
        owner: Owner | None,
        repo: Repository,
        use_ssl=False,
        token=None,
        should_use_sentry_app=False,
    ):
        if should_use_sentry_app:
            ghapp = await async_get_sentry_ghapp_installation(owner)
        else:
            ghapp = await async_get_default_ghapp_installation(owner)
        return self._get_adapter(
            owner,
            repo,
            use_ssl=use_ssl,
            ghapp=ghapp,
            token=token,
            should_use_sentry_app=should_use_sentry_app,
        )

    def get_adapter(
        self,
        owner: Owner | None,
        repo: Repository,
        use_ssl=False,
        token=None,
        should_use_sentry_app=False,
    ):
        if should_use_sentry_app:
            ghapp = get_sentry_ghapp_installation(owner)
        else:
            ghapp = get_default_ghapp_installation(owner)
        return self._get_adapter(
            owner,
            repo,
            use_ssl=use_ssl,
            ghapp=ghapp,
            token=token,
            should_use_sentry_app=should_use_sentry_app,
        )

    def _get_adapter(
        self,
        owner: Owner | None,
        repo: Repository,
        use_ssl: bool = False,
        token=None,
        ghapp: GithubAppInstallation | None = None,
        should_use_sentry_app: bool = False,
    ):
        """
        Return the corresponding implementation for calling the repository provider

        :param owner: :class:`codecov_auth.models.Owner`
        :param repo: :class:`core.models.Repository`
        :param use_ssl: bool, whether to use SSL for the connection
        :param token: optional OAuth token to use for the connection
        :param ghapp: :class:`codecov_auth.models.GithubAppInstallation`, optional
            Github App installation to use for the connection
        :param should_use_sentry_app: bool, whether to use the Sentry GitHub App
            for authentication
        :return:
        :raises: TorngitInitializationFailed
        """
        if should_use_sentry_app:
            if ghapp is None or owner is None:
                raise TorngitInitializationFailed(
                    "Using the Sentry Github App for authentication requires that it is installed."
                )

            generic_adapter_params = get_sentry_adapter_params(
                owner, repo.author.service, ghapp, use_ssl
            )

        else:
            generic_adapter_params = get_generic_adapter_params(
                owner, repo.author.service, use_ssl, token
            )

        owner_and_repo_params = {
            "repo": {
                "name": repo.name,
                "using_integration": self._is_using_integration(ghapp, repo),
                "service_id": repo.service_id,
                "private": repo.private,
                "repoid": repo.repoid,
            },
            "owner": {
                "username": repo.author.username,
                "service_id": repo.author.service_id,
            },
        }

        return get_provider(
            repo.author.service, {**generic_adapter_params, **owner_and_repo_params}
        )

    def get_by_name(self, owner, repo_name, repo_owner_username, repo_owner_service):
        """
        Return the corresponding implementation for calling the repository provider

        :param owner: Owner object of the user
        :param repo_name: string, name of the repo
        :param owner: Owner, owner of the repo in question
        :repo_owner_service: 'github', 'gitlab' etc
        :return:
        :raises: TorngitInitializationFailed
        """
        generic_adapter_params = get_generic_adapter_params(owner, repo_owner_service)
        owner_and_repo_params = {
            "repo": {"name": repo_name},
            "owner": {"username": repo_owner_username},
        }
        return get_provider(
            repo_owner_service, {**generic_adapter_params, **owner_and_repo_params}
        )
