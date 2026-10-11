from ariadne import ObjectType
from asgiref.sync import sync_to_async
from django.urls import reverse
from graphql import GraphQLResolveInfo

from graphql_api.helpers.connection import queryset_to_connection_sync
from graphql_api.types.enums import (
    UploadErrorEnum,
    UploadState,
    UploadType,
)
from reports.models import ReportSession
from shared.django_apps.utils.services import get_short_service_name

upload_bindable = ObjectType("Upload")
upload_bindable.set_alias("flags", "flag_names")

upload_error_bindable = ObjectType("UploadError")

"""
    Note Uploads are called ReportSession in the model, so I'm keeping the argument
    in line with the code vs product name.
"""


@upload_bindable.field("state")
def resolve_state(upload: ReportSession, info: GraphQLResolveInfo) -> UploadState:
    if not upload.state:
        return UploadState.ERROR
    return UploadState(upload.state)


@upload_bindable.field("id")
def resolve_id(upload: ReportSession, info: GraphQLResolveInfo) -> int | None:
    return upload.order_number


@upload_bindable.field("uploadType")
def resolve_upload_type(upload: ReportSession, info: GraphQLResolveInfo) -> UploadType:
    return UploadType(upload.upload_type)


@upload_bindable.field("errors")
@sync_to_async
def resolve_errors(report_session: ReportSession, info: GraphQLResolveInfo, **kwargs):
    return queryset_to_connection_sync(list(report_session.errors.all()))


_ERROR_CODE_MESSAGES = {
    "file_not_in_storage": "The upload file could not be found in storage.",
    "report_expired": "The upload has expired and is no longer available.",
    "report_empty": "The uploaded report is empty.",
    "processing_timeout": "The upload timed out during processing.",
    "unsupported_file_format": "The uploaded file format is not supported.",
    "unknown_processing": "An unknown error occurred during processing.",
    "unknown_storage": "An unknown storage error occurred.",
    "unknown_error_code": "An unknown error occurred.",
}


@upload_error_bindable.field("errorCode")
def resolve_error_code(error, info: GraphQLResolveInfo) -> UploadErrorEnum:
    try:
        return UploadErrorEnum(error.error_code)
    except ValueError:
        return UploadErrorEnum.UNKNOWN_ERROR_CODE


@upload_error_bindable.field("errorMessage")
def resolve_error_message(error, info: GraphQLResolveInfo) -> str:
    return _ERROR_CODE_MESSAGES.get(error.error_code, error.error_code)


@upload_bindable.field("ciUrl")
@sync_to_async
def resolve_ci_url(upload: ReportSession, info: GraphQLResolveInfo):
    return upload.ci_url


@upload_bindable.field("downloadUrl")
@sync_to_async
def resolve_download_url(upload: ReportSession, info) -> str:
    request = info.context["request"]
    repository = upload.report.commit.repository
    download_url = (
        reverse(
            "upload-download",
            kwargs={
                "service": get_short_service_name(repository.author.service),
                "owner_username": repository.author.username,
                "repo_name": repository.name,
            },
        )
        + f"?path={upload.storage_path}"
    )
    download_absolute_uri = request.build_absolute_uri(download_url)
    return download_absolute_uri.replace("http", "https", 1)


@upload_bindable.field("name")
def resolve_name(upload, info):
    return upload.name
