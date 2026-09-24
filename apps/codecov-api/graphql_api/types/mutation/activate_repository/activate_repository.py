from typing import Any

from ariadne import UnionType
from asgiref.sync import sync_to_async
from graphql import GraphQLResolveInfo

from codecov.commands.exceptions import ValidationError
from codecov_auth.models import Owner
from graphql_api.helpers.mutation import (
    require_authenticated,
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
@require_authenticated
async def resolve_activate_repository(
    _: Any, info: GraphQLResolveInfo, owner: str, repo_name: str
) -> None:
    command = info.context["executor"].get_command("repository")
    service = info.context["service"]

    owner_obj = await sync_to_async(
        Owner.objects.filter(username=owner, service=service).first
    )()

    if owner_obj is None:
        raise ValidationError("Owner not found")

    await command.update_repository(
        repo_name,
        owner_obj,
        default_branch=None,
        activated=True,
    )
    return None


error_activate_repository = UnionType("ActivateRepositoryError")
error_activate_repository.type_resolver(resolve_union_error_type)