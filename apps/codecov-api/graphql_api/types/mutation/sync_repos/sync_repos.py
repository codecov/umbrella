from graphql_api.helpers.mutation import wrap_error_handling_mutation


@wrap_error_handling_mutation
async def resolve_sync_repos(_, info):
    command = info.context["executor"].get_command("owner")
    await command.trigger_sync(using_integration=True)
    return {"isSyncing": True}
