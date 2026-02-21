from typing import Any

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

    * FETCHING_COMMIT_DETAILS: Creating a commit database entry and fetching
      commit details.
    * COMMIT_PROCESSED: Commit has been processed and is ready for report
      preparation.
    * PREPARING_FOR_REPORT: Creating a report database entry.
    * READY_FOR_REPORT: Carry-forwarding flags from previous uploads is
      complete.
    * WAITING_FOR_COVERAGE_UPLOAD: Create a pre-signed URL for the upload and
      wait for the coverage upload.
    * COMPILING_UPLOADS: Scheduling upload processing task(s) and initializing
      any missing database entries.
    * PROCESSING_UPLOAD: Processing the uploaded file(s).
    * NOTIFICATIONS_TRIGGERED: Notifications (e.g. pull request comments) have
      been triggered either manually or automatically.
    * UPLOAD_COMPLETE: Processing and compilation of the upload is complete.
    * NOTIFICATIONS_SENT: Notifications (e.g. pull request comments) have been
      sent.
    """

    FETCHING_COMMIT_DETAILS = "fcd", _("Fetching commit details")
    COMMIT_PROCESSED = "cp", _("Commit processed")
    PREPARING_FOR_REPORT = "pfr", _("Preparing for report")
    READY_FOR_REPORT = "rfr", _("Ready for report")
    WAITING_FOR_COVERAGE_UPLOAD = "wfcu", _("Waiting for coverage upload")
    COMPILING_UPLOADS = "cu", _("Compiling uploads")
    PROCESSING_UPLOAD = "pu", _("Processing upload")
    NOTIFICATIONS_TRIGGERED = "nt", _("Notifications triggered")
    UPLOAD_COMPLETE = "uc", _("Upload complete")
    NOTIFICATIONS_SENT = "ns", _("Notifications sent")
    LOCK_ACQUIRING = "la", _("Acquiring lock")
    LOCK_ACQUIRED = "lac", _("Lock acquired")
    LOCK_RELEASED = "lr", _("Lock released")


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
    """

    BAD_REQUEST = "br", _("Bad request, see API response for details")
    REPO_DEACTIVATED = "rd", _("Repository is deactivated within Codecov")
    REPO_MISSING_VALID_BOT = "rmb", _("Repository is missing a valid bot")
    REPO_NOT_FOUND = "rnf", _("Repository not found by the configured bot")
    REPO_BLACKLISTED = "rb", _("Repository is blacklisted by Codecov")
    COMMIT_NOT_FOUND = "cnf", _("Commit not found in the repository")
    COMMIT_UPLOAD_LIMIT = "cul", _("Upload limit exceeded for this commit")
    OWNER_UPLOAD_LIMIT = "oul", _("Owner (user or team) upload limit exceeded")
    GIT_CLIENT_ERROR = "gce", _("Git client returned a 4xx error")
    REPORT_NOT_FOUND = "rptnf", _("Report not found for the commit")
    REPORT_EXPIRED = "rpe", _("Report expired and cannot be processed")
    REPORT_EMPTY = "rem", _("Report is empty and cannot be processed")
    FILE_NOT_IN_STORAGE = "fnis", _("File not found in storage")
    UPLOAD_NOT_FOUND = "unf", _("Upload not found for the commit")
    UNSUPPORTED_FORMAT = (
        "uf",
        _(
            "Unsupported coverage report format. Please check that the file "
            "is a valid coverage report and that the report format is "
            "supported by Codecov."
        ),
    )
    TASK_TIMED_OUT = "tto", _("Task timed out")
    SKIPPED_NOTIFICATIONS = "sn", _("Notifications were skipped due to configuration")

    # Errors that should not be shown to the user
    INTERNAL_LOCK_ERROR = "int_le", _("Unable to acquire or release lock")
    INTERNAL_RETRYING = "int_re", _("Retrying the upload task")
    INTERNAL_OUT_OF_RETRIES = "int_or", _("Out of retries for the upload task")
    INTERNAL_NO_PENDING_JOBS = "int_np", _("No pending jobs found for the commit")
    INTERNAL_NO_ARGUMENTS = (
        "int_na",
        _("No arguments found in Redis for the upload task"),
    )
    INTERNAL_APP_RATE_LIMITED = (
        "int_rl",
        _("Codecov app is rate limited by the git client"),
    )
    INTERNAL_OTHER_JOB = (
        "int_oj",
        _("Another identical job is already running or will run for this commit"),
    )

    # Catch-all for other errors
    UNKNOWN = "u", _("Unknown error")


class BreadcrumbData(
    PydanticBaseModel,
    frozen=True,
    extra="forbid",
    use_enum_values=True,
):
    """
    Represents the data structure for the `breadcrumb_data` field which
    contains information about the milestone, endpoint, error, and error text.

    Each field is optional and cannot be set to an empty string. Note that any
    field not set or set to `None` will be excluded from the model dump.
    Additionally, if `error_text` is provided, `error` must also be provided.

    :param milestone: The milestone of the upload process.
    :type milestone: Milestones, optional
    :param endpoint: The endpoint of the upload process.
    :type endpoint: Endpoints, optional
    :param uploader: The uploader and version used for the upload process.
    :type uploader: str, optional
    :param error: The error encountered during the upload process.
    :type error: Errors, optional
    :param error_text: Additional text describing the error.
    :type error_text: str, optional

    :raises ValidationError: If no non-empty fields are provided.
    :raises ValidationError: If any field is explicitly set to an empty string.
    :raises ValidationError: If `error_text` is provided without an `error`.
    :raises ValidationError: If `error` is set to `UNKNOWN` without any
        `error_text`.
    """

    milestone: Milestones | None = None
    endpoint: Endpoints | None = None
    uploader: str | None = None
    error: Errors | None = None
    error_text: str | None = None
    task_name: str | None = None
    parent_task_id: str | None = None

    @field_validator("*", mode="after")
    @classmethod
    def validate_initialized(cls, value: Any) -> Any:
        if value == "":
            raise ValueError("field must not be empty.")
        return value

    @model_validator(mode="after")
    def require_at_least_one_field(self) -> "BreadcrumbData":
        if not any(
            [
                self.milestone,
                self.endpoint,
                self.uploader,
                self.error,
                self.error_text,
            ]
        ):
            raise ValueError("at least one field must be provided.")
        return self

    @model_validator(mode="after")
    def check_error_dependency(self) -> "BreadcrumbData":
        if self.error_text and not self.error:
            raise ValueError("'error_text' is provided, but 'error' is missing.")
        return self

    @model_validator(mode="after")
    def check_unknown_error(self) -> "BreadcrumbData":
        if self.error == Errors.UNKNOWN and not self.error_text:
            raise ValueError("'error_text' must be provided when 'error' is UNKNOWN.")
        return self

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, str]:
        kwargs["exclude_none"] = True
        return super().model_dump(*args, **kwargs)

    @classmethod
    def django_validate(cls, *args: Any, **kwargs: Any) -> None:
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
            models.Index(
                fields=["-created_at", "-id"], name="%(app_label)s_created_id"
            ),
            models.Index(fields=["commit_sha"], name="%(app_label)s_commit_sha"),
            models.Index(fields=["sentry_trace_id"], name="%(app_label)s_sentry_trc"),
            models.Index(
                fields=["repo_id", "commit_sha"], name="%(app_label)s_repo_sha"
            ),
            GinIndex(fields=["upload_ids"], name="%(app_label)s_upload_ids"),
        ]
