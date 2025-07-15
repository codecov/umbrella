from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from django.core.exceptions import ValidationError
from django.db import models
from django.utils.translation import gettext_lazy as _
from psqlextra.models import PostgresPartitionedModel
from psqlextra.types import PostgresPartitioningMethod
from pydantic import BaseModel as PydanticBaseModel
from pydantic import ValidationError as PydanticValidationError
from pydantic import field_validator, model_validator

from shared.django_apps.codecov.models import BaseModel


class Milestones(models.TextChoices):
    """
    Possible milestones for an upload breadcrumb.

    These milestones represent the various stages of the upload process.

    * FETCHING_COMMIT_DETAILS: Creating a commit database entry and fetching commit details.
    * COMMIT_PROCESSED: Commit has been processed and is ready for report preparation.
    * PREPARING_FOR_REPORT: Creating a report database entry.
    * READY_FOR_REPORT: Carry-forwarding flags from previous uploads is complete.
    * WAITING_FOR_COVERAGE_UPLOAD: Create a pre-signed URL for the upload and wait for the coverage upload.
    * COMPILING_UPLOADS: Scheduling upload processing task(s) and initializing any missing database entries.
    * PROCESSING_UPLOAD: Processing the uploaded file(s).
    * UPLOAD_COMPLETE: Processing and compilation of the upload is complete.
    * NOTIFICATIONS_SENT: Notifications (e.g. pull request comments) have been sent.
    """

    FETCHING_COMMIT_DETAILS = "fcd", _("Fetching commit details")
    COMMIT_PROCESSED = "cp", _("Commit processed")
    PREPARING_FOR_REPORT = "pfr", _("Preparing for report")
    READY_FOR_REPORT = "rfr", _("Ready for report")
    WAITING_FOR_COVERAGE_UPLOAD = "wfcu", _("Waiting for coverage upload")
    COMPILING_UPLOADS = "cu", _("Compiling uploads")
    PROCESSING_UPLOAD = "pu", _("Processing upload")
    UPLOAD_COMPLETE = "uc", _("Upload complete")
    NOTIFICATIONS_SENT = "ns", _("Notifications sent")


class Endpoints(models.TextChoices):
    """
    Possible source endpoints for an upload breadcrumb.

    These endpoints are all part of the upload API.
    """

    # Labels are URL names from apps/codecov-api/upload/urls.py
    CREATE_COMMIT = "cc", _("new_upload.commits")
    CREATE_REPORT = "cr", _("new_upload.reports")
    DO_UPLOAD = "du", _("new_upload.uploads")
    EMPTY_UPLOAD = "eu", _("new_upload.empty_upload")
    UPLOAD_COMPLETION = "ucomp", _("new_upload.upload-complete")
    UPLOAD_COVERAGE = "ucov", _("new_upload.upload_coverage")
    LEGACY_UPLOAD_COVERAGE = "luc", _("upload-handler")


class Errors(models.TextChoices):
    """
    Possible errors for an upload breadcrumb.

    TODO: add more and give better descriptions
    """

    MISSING_TOKEN = "mt", _("Missing authorization token")
    MALFORMED_INPUT = "mi", _("Malformed coverage report input")
    UNRECOGNIZED_FORMAT = "uf", _("Unrecognized coverage report format")
    TASK_TIMED_OUT = "tto", _("Task timed out")
    UNKNOWN = "u", _("Unknown error")


class BreadcrumbData(
    PydanticBaseModel,
    frozen=True,
    extra="forbid",
    use_enum_values=True,
):
    """
    Represents the data structure for the `breadcrumb_data` field which contains
    information about the milestone, endpoint, error, and error text.

    Each field is optional and cannot be set to an empty string. Note that any
    field not set or set to `None` will be excluded from the model dump.
    Additionally, if `error_text` is provided, `error` must also be provided.

    :param milestone: The milestone of the upload process.
    :type milestone: Milestones, optional
    :param endpoint: The endpoint of the upload process.
    :type endpoint: Endpoints, optional
    :param error: The error encountered during the upload process.
    :type error: Errors, optional
    :param error_text: Additional text describing the error.
    :type error_text: str, optional

    :raises ValidationError: If no non-empty fields are provided.
    :raises ValidationError: If any field is explicitly set to an empty string.
    :raises ValidationError: If `error_text` is provided without an `error`.
    :raises ValidationError: If `error` is set to UNKNOWN without an `error_text`.
    """

    milestone: Milestones | None = None
    endpoint: Endpoints | None = None
    error: Errors | None = None
    error_text: str | None = None

    @field_validator("*", mode="after")
    @classmethod
    def validate_initialized(cls, value):
        if value == "":
            raise ValueError("field must not be empty.")
        return value

    @model_validator(mode="after")
    def require_at_least_one_field(self):
        if not any(
            [
                self.milestone,
                self.endpoint,
                self.error,
                self.error_text,
            ]
        ):
            raise ValueError("at least one field must be provided.")
        return self

    @model_validator(mode="after")
    def check_error_dependency(self):
        if self.error_text and not self.error:
            raise ValueError("'error_text' is provided, but 'error' is missing.")
        return self

    @model_validator(mode="after")
    def check_unknown_error(self):
        if self.error == Errors.UNKNOWN and not self.error_text:
            raise ValueError("'error_text' must be provided when 'error' is UNKNOWN.")
        return self

    def model_dump(self, *args, **kwargs):
        kwargs["exclude_none"] = True
        return super().model_dump(*args, **kwargs)

    @classmethod
    def django_validate(cls, *args, **kwargs) -> None:
        """
        Performs validation in a way that conforms to the expectations of
        Django's model validation system.

        :raises ValidationError: If the model does not conform to the expected
            structure.
        """
        try:
            cls.model_validate(*args, **kwargs)
        except PydanticValidationError as e:
            # Map Pydantic validation errors to Django validation errors
            raise ValidationError(str(e))


class UploadBreadcrumb(
    PostgresPartitionedModel,
    BaseModel,
):
    """
    This model is used to track the progress of uploads through various milestones
    and endpoints, as well as any errors encountered during the upload process.

    :param commit_sha: The SHA of the commit associated with the upload.
    :type commit_sha: str
    :param repo_id: The ID of the repository associated with the commit.
    :type repo_id: int
    :param upload_ids: List of upload IDs associated with the commit.
    :type upload_ids: list[int] | None
    :param sentry_trace_id: The Sentry trace ID for tracking errors.
    :type sentry_trace_id: str | None
    :param breadcrumb_data: Variable data for the upload breadcrumb, including milestone,
        endpoint, error, and error text.
    :type breadcrumb_data: BreadcrumbData

    :raises ValidationError: If the `breadcrumb_data` does not conform to the
        expected structure defined by `BreadcrumbData`.
    """

    commit_sha = models.TextField()
    repo_id = models.BigIntegerField()
    upload_ids = ArrayField(models.BigIntegerField(), null=True, blank=True)
    sentry_trace_id = models.TextField(null=True, blank=True)
    breadcrumb_data = models.JSONField(
        help_text="Variable breadcrumb data for a given upload",
        validators=[BreadcrumbData.django_validate],
    )

    class PartitioningMeta:
        method = PostgresPartitioningMethod.RANGE
        key = ["created_at"]

    class Meta:
        ordering = ["-created_at"]
        indexes = [
            models.Index(fields=["commit_sha"], name="%(app_label)s_commit_sha"),
            models.Index(
                fields=["commit_sha", "repo_id"], name="%(app_label)s_sha_repo"
            ),
            GinIndex(fields=["upload_ids"], name="%(app_label)s_upload_ids"),
        ]
