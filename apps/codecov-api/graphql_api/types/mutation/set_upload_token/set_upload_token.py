from typing import Any

from ariadne import UnionType
from graphql import GraphQLResolveInfo

from core.commands.repository.repository import RepositoryCommands
from graphql_api.helpers.mutation import (
    require_authenticated,
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
@require_authenticated
async def resolve_set_upload_token(
    _: Any, info: GraphQLResolveInfo, owner: str, repo_name: str, token: str
) -> dict:
    command: RepositoryCommands = info.context["executor"].get_command("repository")
    await command.set_upload_token(
        owner_username=owner,
        repo_name=repo_name,
        token=token,
    )
    return {}


error_set_upload_token = UnionType("SetUploadTokenError")
error_set_upload_token.type_resolver(resolve_union_error_type)