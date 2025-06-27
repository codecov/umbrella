def resolve_sync_repos(_, info):
    command = info.context["executor"].get_command("owner")
    command.trigger_sync(using_integration=True)
    return {"is_syncing": True}
