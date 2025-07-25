import enum
import logging
from functools import lru_cache

import sentry_sdk
from asgiref.sync import async_to_sync
from yaml import safe_load

from codecov_auth.models import Owner, get_config
from core.models import Commit
from services.repo_providers import RepoProviderService
from shared.helpers.cache import cache
from shared.yaml import UserYaml, fetch_current_yaml_from_provider_via_reference
from shared.yaml.validation import validate_yaml


class YamlStates(enum.Enum):
    DEFAULT = "default"


log = logging.getLogger(__name__)


@cache.cache_function(ttl=60 * 60)
def fetch_commit_yaml(
    commit: Commit, owner: Owner | None, should_use_sentry_app: bool = False
) -> dict | None:
    """
    Fetches the codecov.yaml file for a particular commit from the service provider.
    Service provider API request is made on behalf of the given `owner`.
    """

    # There's been a lot of instances where this function is called where the owner arg is not Owner type
    # Previously this issue is masked by the catch all exception handling. Most badly typed calls have
    # been addressed, but there might still be some entrypoints unaccounted for, will fix new discoveries
    # from this assertion being raised.
    if owner is not None and not isinstance(owner, Owner):
        raise TypeError(
            f"fetch_commit_yaml owner arg must be Owner or None. Provided: {type(owner)}"
        )

    try:
        repository_service = RepoProviderService().get_adapter(
            owner=owner,
            repo=commit.repository,
            should_use_sentry_app=should_use_sentry_app,
        )
        yaml_str = async_to_sync(fetch_current_yaml_from_provider_via_reference)(
            commit.commitid, repository_service
        )
        if yaml_str is None:
            raise ValueError("Yaml not found for commit")
        yaml_dict = safe_load(yaml_str)
        return validate_yaml(yaml_dict, show_secrets_for=None)
    except Exception as e:
        # fetching, parsing, validating the yaml inside the commit can
        # have various exceptions, which we do not care about to get the final
        # yaml used for a commit, as any error here, the codecov.yaml would not
        # be used, so we return None here

        log.warning(
            "Was not able to fetch yaml file for commit. Ignoring error and returning None.",
            extra={
                "commit_id": commit.commitid,
                "owner_arg": type(owner),
                "exception": str(e),
            },
        )
        return None


@lru_cache
@sentry_sdk.trace
def final_commit_yaml(
    commit: Commit, owner: Owner | None, should_use_sentry_app: bool = False
) -> UserYaml:
    return UserYaml.get_final_yaml(
        owner_yaml=commit.repository.author.yaml,
        repo_yaml=commit.repository.yaml,
        commit_yaml=fetch_commit_yaml(
            commit, owner, should_use_sentry_app=should_use_sentry_app
        ),
    )


def get_yaml_state(yaml: UserYaml) -> YamlStates | None:
    if yaml == get_config("site", default={}):
        return YamlStates.DEFAULT
    return None
