from graphql_api.helpers.mutation import (
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
async def resolve_sync_with_git_provider(_, info):
    command = info.context["executor"].get_command("owner")
    await command.trigger_sync()
    return {"me": info.context["request"].current_owner}
