from ariadne import UnionType

from graphql_api.helpers.mutation import (
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
async def resolve_regenerate_support_pin(_, info):
    command = info.context["executor"].get_command("owner")
    me = await command.regenerate_support_pin()
    return {"me": me}


error_regenerate_support_pin = UnionType("RegenerateSupportPinError")
error_regenerate_support_pin.type_resolver(resolve_union_error_type)
