from ariadne import UnionType

from graphql_api.helpers.mutation import (
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


@wrap_error_handling_mutation
async def resolve_create_upload(_, info, input):
    command = info.context["executor"].get_command("repository")
    upload = await command.create_upload(
        repo_name=input.get("repo_name"),
        owner_username=input.get("owner_username"),
        commit_sha=input.get("commit_sha"),
        branch=input.get("branch"),
        pull_id=input.get("pull_id"),
        flags=input.get("flags"),
        report_type=input.get("report_type"),
    )
    return {"upload": upload}


error_create_upload = UnionType("CreateUploadError")
error_create_upload.type_resolver(resolve_union_error_type)