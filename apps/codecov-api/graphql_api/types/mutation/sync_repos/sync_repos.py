import logging

from ariadne import UnionType
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from graphql_api.helpers.mutation import (
    resolve_union_error_type,
    wrap_error_handling_mutation,
)
from services.refresh import RefreshService

log = logging.getLogger(__name__)


@wrap_error_handling_mutation
async def resolve_sync_repos(_, info):
    """
    Mutation to trigger a sync of all repos for an organization.

    This mutation will trigger a sync of all repos for an organization.
    The sync will be triggered using the integration flag,
    as opposed to sync_with_git_provider, which uses the User's token.
    """
    command = info.context["executor"].get_command("owner")
    await command.trigger_sync(using_integration=True)

    current_owner = info.context["request"].current_owner
    if not current_owner:
        return {"is_syncing": False}

    try:
        refresh_service = RefreshService()
        is_syncing = refresh_service.is_refreshing(current_owner.ownerid)
        return {"is_syncing": is_syncing}
    except (RedisConnectionError, RedisError):
        log.warning(
            "Redis error while checking sync status",
            extra={"ownerid": current_owner.ownerid},
        )
        return {"is_syncing": False}


error_sync_repos = UnionType("SyncReposError")
error_sync_repos.type_resolver(resolve_union_error_type)
