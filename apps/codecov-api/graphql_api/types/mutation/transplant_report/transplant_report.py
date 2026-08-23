from typing import Any

from ariadne import UnionType
from asgiref.sync import sync_to_async
from graphql import GraphQLResolveInfo

from core.commands.flag import FlagCommands
from graphql_api.helpers.mutation import (
    require_authenticated,
    resolve_union_error_type,
    wrap_error_handling_mutation,
)
from services.task import TaskService
from shared.django_apps.core.models import Repository


@wrap_error_handling_mutation
@require_authenticated
@sync_to_async
def resolve_transplant_report(
    _: Any, info: GraphQLResolveInfo, input: dict[str, Any]
) -> None:
    current_owner = info.context["request"].current_owner

    owner_username = input.get("owner") or current_owner.username
    repo_name = input.get("repo_name")
    from_sha = input.get("from_sha")
    to_sha = input.get("to_sha")

    repository = Repository.objects.filter(
        author__username=owner_username,
        name=repo_name,
    ).first()

    TaskService().transplant_report(
        repo_id=repository.repoid,
        from_sha=from_sha,
        to_sha=to_sha,
    )
    return None


error_transplant_report = UnionType("TransplantReportError")
error_transplant_report.type_resolver(resolve_union_error_type)
