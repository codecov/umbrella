from ariadne import UnionType

from graphql_api.helpers.mutation import (
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


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
    return {"is_syncing": True}


error_sync_repos = UnionType("SyncReposError")
error_sync_repos.type_resolver(resolve_union_error_type)
