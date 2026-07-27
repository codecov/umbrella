from ariadne import UnionType
from asgiref.sync import sync_to_async

from codecov_auth.commands.owner import OwnerCommands
from graphql_api.helpers.mutation import (
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
@sync_to_async
def resolve_delete_owner(_, info, input):
    command: OwnerCommands = info.context["executor"].get_command("owner")
    command.delete_owner(username=input["username"])


error_delete_owner = UnionType("DeleteOwnerError")
error_delete_owner.type_resolver(resolve_union_error_type)