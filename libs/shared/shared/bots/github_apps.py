import logging
import random
from datetime import UTC, datetime

from shared.bots.exceptions import NoConfiguredAppsAvailable, RequestedGithubAppNotFound
from shared.bots.types import TokenWithOwner
from shared.django_apps.codecov_auth.models import (
    GITHUB_APP_INSTALLATION_DEFAULT_NAME,
    GithubAppInstallation,
    Owner,
    Service,
)
from shared.django_apps.core.models import Repository
from shared.github import InvalidInstallationError, get_github_integration_token
from shared.helpers.redis import get_redis_connection
from shared.orms.owner_helper import DjangoSQLAlchemyOwnerWrapper
from shared.rate_limits import determine_if_entity_is_rate_limited, gh_app_key_name
from shared.typings.oauth_token_types import Token
from shared.typings.torngit import GithubInstallationInfo

log = logging.getLogger(__name__)


MAX_GITHUB_APP_SELECTION_WEIGHT = 1200


def _get_installation_weight(installation: GithubAppInstallation) -> int:
    """The weight for a given app installation.
    Establishes an exponential ramp-up period for installations after being updated.
    """
    age = datetime.now(UTC) - installation.created_at
    if age.days >= 10:
        return MAX_GITHUB_APP_SELECTION_WEIGHT
    seconds_in_hour = 3600
    age_hours = (age.seconds // seconds_in_hour) + age.days * 24
    # Prevent clock differences from making the weight negative
    return max(1, age_hours + 2**age.days)


def _can_use_this_app(
    app: GithubAppInstallation, installation_name: str, repository: Repository | None
) -> bool:
    return (
        app.name == installation_name
        # We ignore apps that are not configured because those can't be used
        and app.is_configured()
        # If repository is provided, the installation needs to cover it
        and ((not repository) or app.is_repo_covered_by_integration(repository))
    )


def _get_apps_from_weighted_selection(
    owner: Owner, installation_name: str, repository: Repository | None
) -> list[GithubAppInstallation]:
    """This function returns an ordered list of GithubAppInstallations that can be used to communicate with GitHub
    in behalf of the owner. The list is ordered in such a way that the 1st element is the app to be used in Torngit,
    and the subsequent apps are selected as fallbacks.

    IF the repository is provided, the selected apps also cover the repo.
    IF installation_name is not the default one, than the default codecov installation
      is also selected as a possible fallback app.

    Apps are selected randomly but assigned weights based on how recently they were created.
    This means that older apps are selected more frequently as the main app than newer ones.
    (up to 10 days, when the probability of being chosen is the same)
    The random selection is done so we can distribute request load more evenly among apps.
    """
    # Map GithubAppInstallation.id --> GithubAppInstallation
    ghapp_installations_filter: dict[int, GithubAppInstallation] = {
        obj.id: obj
        for obj in filter(
            lambda obj: _can_use_this_app(obj, installation_name, repository),
            DjangoSQLAlchemyOwnerWrapper.get_github_app_installations(owner) or [],
        )
    }
    # We assign weights to the apps based on how long ago they were created.
    # The idea is that there's a greater chance that a change misconfigured the app,
    # So apps recently created are selected less frequently than older apps
    keys = list(ghapp_installations_filter.keys())
    weights = [
        min(
            MAX_GITHUB_APP_SELECTION_WEIGHT,
            _get_installation_weight(ghapp_installations_filter[key]),
        )
        for key in keys
    ]
    # We pick apps one by one until all apps have been selected
    # Obviously apps with a higher weight have a higher change of being selected as the main app (1st selection)
    # But it's important that others are also selected so we can use them as fallbacks
    apps_to_consider = []
    apps_to_select = len(keys)
    selections = 0
    while selections < apps_to_select:
        selected_app_id = random.choices(keys, weights, k=1)[0]
        apps_to_consider.append(ghapp_installations_filter[selected_app_id])
        # random.choices chooses with replacement
        # which we are trying to avoid here. So we remove the key selected and its weight from the population.
        key_idx = keys.index(selected_app_id)
        keys.pop(key_idx)
        weights.pop(key_idx)
        selections += 1
    if installation_name != GITHUB_APP_INSTALLATION_DEFAULT_NAME:
        # Add the default app as the last fallback if the owner is using a different app for the task
        default_apps = filter(
            lambda obj: _can_use_this_app(
                obj, GITHUB_APP_INSTALLATION_DEFAULT_NAME, repository
            ),
            DjangoSQLAlchemyOwnerWrapper.get_github_app_installations(owner),
        )
        if default_apps:
            apps_to_consider.extend(default_apps)
    return apps_to_consider


def handle_invalid_installation(
    installation_info: GithubInstallationInfo, error: InvalidInstallationError
) -> None:
    """Handles the InvalidInstallationError, syncing our info with GitHub's , so that we don't have the same error again in the future.

    possible side effects:
        * marking GithubAppInstallation as suspended;
        * deleting GithubAppInstallations;
        * clearing out Owner.integration_id
    """
    if "id" in installation_info:
        match error.error_cause:
            case "installation_suspended":
                # Mark the installation as suspended so we don't keep trying to get the token for it
                GithubAppInstallation.objects.filter(id=installation_info["id"]).update(
                    is_suspended=True
                )
            case "installation_not_found":
                GithubAppInstallation.objects.filter(
                    id=installation_info["id"]
                ).delete()
    else:
        # This comes from the legacy Owner.integration_id. Clear it.
        # installation_id should be unique among Owners
        Owner.objects.filter(
            integration_id=installation_info["installation_id"]
        ).update(integration_id=None)


def get_github_app_token(
    service: Service, installation_info: GithubInstallationInfo
) -> TokenWithOwner:
    """Get an access_token from GitHub that we can use to authenticate as the installation
    See https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/authenticating-as-a-github-app-installation#generating-an-installation-access-token

    ⚠️ side effect: handle_invalid_installation has potential side effects to GithubAppInstallation and Owner models

    Raises:
      InvalidInstallationError: if we can't get the installation's access_token
    """
    try:
        app_id = installation_info.get("app_id", None)
        installation_id = installation_info["installation_id"]
        github_token = get_github_integration_token(
            service.value,
            installation_id,
            app_id=str(app_id) if app_id else None,
            pem_path=installation_info.get("pem_path", None),
        )
        installation_token = Token(
            key=github_token,
            username=f"installation_{installation_id}",
            entity_name=gh_app_key_name(
                installation_id=installation_id,
                app_id=app_id,
            ),
        )
        # The token is not owned by an Owner object, so 2nd arg is None
        return installation_token, None
    except InvalidInstallationError as err:
        handle_invalid_installation(installation_info, err)
        raise err


def get_specific_github_app_details(
    owner: Owner, github_app_id: int, commitid: str
) -> GithubInstallationInfo:
    """Gets the GithubInstallationInfo for GithubAppInstallation with id github_app_id.

    Args:
        owner (Owner): the owner of the app. We look only in the apps for this owner.
        github_app_id (int): the ID of the GithubAppInstallation we're looking for
        commitid (str): Commit.commitid, used for logging purposes

    Raises:
        RequestedGithubAppNotFound - if the app is not found in the given owner raise exception.
            The assumption is that we need this specific app for a reason, and if we can't find the app
            it's better to just fail
    """
    app: GithubAppInstallation | None = next(
        (
            obj
            for obj in DjangoSQLAlchemyOwnerWrapper.get_github_app_installations(owner)
            if obj.id == github_app_id
        ),
        None,
    )
    if app is None:
        log.exception(
            "Can't find requested app",
            extra={"ghapp_id": github_app_id, "commitid": commitid},
        )
        raise RequestedGithubAppNotFound()
    if not app.is_configured():
        log.warning(
            "Request for specific app that is not configured",
            extra={"ghapp_id": id, "commitid": commitid},
        )
    return GithubInstallationInfo(
        id=app.id,
        installation_id=app.installation_id,
        app_id=app.app_id,
        pem_path=app.pem_path,
    )


def _filter_rate_limited_apps(
    apps_to_consider: list[GithubAppInstallation],
) -> list[GithubAppInstallation]:
    redis_connection = get_redis_connection()
    return list(
        filter(
            lambda obj: not determine_if_entity_is_rate_limited(
                redis_connection,
                gh_app_key_name(app_id=obj.app_id, installation_id=obj.installation_id),
            ),
            apps_to_consider,
        )
    )


def _filter_suspended_apps(
    apps_to_consider: list[GithubAppInstallation],
) -> list[GithubAppInstallation]:
    return list(filter(lambda obj: not obj.is_suspended, apps_to_consider))


def get_github_app_info_for_owner(
    owner: Owner,
    *,
    repository: Repository | None = None,
    installation_name: str = GITHUB_APP_INSTALLATION_DEFAULT_NAME,
) -> list[GithubInstallationInfo]:
    """Gets the GitHub app info needed to communicate with GitHub using an app for this owner.
    If multiple apps are available for this owner a selection is done to have 1 main app, and the others
    are listed as fallback options.

    ⚠️ The return of this function is NOT enough to actually send requests to GitHub using the app. For that you need to call 'get_github_integration_token'
    with an installation info, to get a token. This token is used to send requests to GitHub _as the app_
    (i.e. authenticating as an app installation, see https://docs.github.com/en/apps/creating-github-apps/authenticating-with-a-github-app/about-authentication-with-a-github-app#authentication-as-an-app-installation)

    GitHub App information can be:
        1. A GithubAppInstallation that belongs to the owner
        2. (deprecated) Owner.integration_id

    Args:
        owner (Owner): The owner to get GitHub App info for.
        repository (Repository | None): The repo that we will interact with.
            Any GitHub App info returned needs to cover this repo
        installation_name (str): The installation name to search for in the available apps.
            GitHubAppInstallation.name must be equal to installation_name for it to be returned.

    Returns:
        (ordered) List[GithubInstallationInfo]: where index 0 is the main app and the others are fallback options

    Raises:
        NoConfiguredAppsAvailable: Owner has app installations available, but they are all currently rate limited.
    """
    extra_info_to_log = {
        "ownerid": owner.ownerid,
        "repoid": getattr(repository, "repoid", None),
    }
    log.info(
        "Getting owner's GitHub Apps info",
        extra=dict(
            installation_name=installation_name,
            **extra_info_to_log,
        ),
    )
    owner_service = Service(owner.service)
    if owner_service not in [Service.GITHUB, Service.GITHUB_ENTERPRISE]:
        log.info(
            "Owner's service not GitHub",
            extra=dict(service=owner_service, **extra_info_to_log),
        )
        return []

    # Get the apps available for the owner with the given 'installation_name'
    # AND that cover 'repository' (if provided)
    apps_to_consider = _get_apps_from_weighted_selection(
        owner, installation_name, repository
    )
    apps_matching_criteria_count = len(apps_to_consider)
    # We can't use apps that are rate limited
    apps_to_consider = _filter_rate_limited_apps(apps_to_consider)
    rate_limited_apps_count = apps_matching_criteria_count - len(apps_to_consider)
    # We can't use apps that are suspended (by the user)
    apps_to_consider = _filter_suspended_apps(apps_to_consider)
    suspended_apps_count = (
        apps_matching_criteria_count - rate_limited_apps_count - len(apps_to_consider)
    )

    if apps_to_consider:
        # There's at least 1 app that matches all the criteria and can be used to communicate with GitHub
        main_name = apps_to_consider[0].name
        info_to_get_tokens = [
            GithubInstallationInfo(
                id=obj.id,
                installation_id=obj.installation_id,
                app_id=obj.app_id,
                pem_path=obj.pem_path,
            )
            for obj in apps_to_consider
        ]
        log.info(
            "Selected installation to communicate with github",
            extra={
                "installation_id": info_to_get_tokens[0]["installation_id"],
                "installation_name": main_name,
                "fallback_installations": [
                    obj["installation_id"] for obj in info_to_get_tokens
                ],
            },
        )
        return info_to_get_tokens
    elif apps_matching_criteria_count > 0:
        # There are apps that match the criteria, but we can't use them.
        # Either they are currently rate limited or they have been suspended.
        raise NoConfiguredAppsAvailable(
            apps_count=apps_matching_criteria_count,
            rate_limited_count=rate_limited_apps_count,
            suspended_count=suspended_apps_count,
        )
    # DEPRECATED FLOW - begin
    if owner.integration_id and (
        (repository and repository.using_integration) or (repository is None)
    ):
        log.info(
            "Selected deprecated owner.integration_id to communicate with github",
            extra=extra_info_to_log,
        )
        return [GithubInstallationInfo(installation_id=owner.integration_id)]
    # DEPRECATED FLOW - end
    return []
