from codecov_auth.commands.owner import OwnerCommands
from graphql_api.helpers.mutation import (
    require_authenticated,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
@require_authenticated
async def resolve_save_terms_agreement(_, info, input):
    command: OwnerCommands = info.context["executor"].get_command("owner")
    return await command.save_terms_agreement(input)
