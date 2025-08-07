import pytest
from django.core.exceptions import ValidationError
from django.db.utils import IntegrityError
from pydantic import ValidationError as PydanticValidationError

from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)
from shared.django_apps.upload_breadcrumbs.tests.factories import (
    UploadBreadcrumbFactory,
)


@pytest.mark.parametrize(
    "enum_class, enum_entries, enum_values, enum_labels",
    [
        (
            Milestones,
            [
                Milestones.FETCHING_COMMIT_DETAILS,
                Milestones.COMMIT_PROCESSED,
                Milestones.PREPARING_FOR_REPORT,
                Milestones.READY_FOR_REPORT,
                Milestones.WAITING_FOR_COVERAGE_UPLOAD,
                Milestones.COMPILING_UPLOADS,
                Milestones.PROCESSING_UPLOAD,
                Milestones.NOTIFICATIONS_TRIGGERED,
                Milestones.UPLOAD_COMPLETE,
                Milestones.NOTIFICATIONS_SENT,
            ],
            ["fcd", "cp", "pfr", "rfr", "wfcu", "cu", "pu", "nt", "uc", "ns"],
            [
                "Fetching commit details",
                "Commit processed",
                "Preparing for report",
                "Ready for report",
                "Waiting for coverage upload",
                "Compiling uploads",
                "Processing upload",
                "Notifications triggered",
                "Upload complete",
                "Notifications sent",
            ],
        ),
        (
            Endpoints,
            [
                Endpoints.CREATE_COMMIT,
                Endpoints.CREATE_REPORT,
                Endpoints.DO_UPLOAD,
                Endpoints.EMPTY_UPLOAD,
                Endpoints.UPLOAD_COMPLETION,
                Endpoints.UPLOAD_COVERAGE,
                Endpoints.LEGACY_UPLOAD_COVERAGE,
            ],
            ["cc", "cr", "du", "eu", "ucomp", "ucov", "luc"],
            [
                "new_upload.commits",
                "new_upload.reports",
                "new_upload.uploads",
                "new_upload.empty_upload",
                "new_upload.upload-complete",
                "new_upload.upload_coverage",
                "upload-handler",
            ],
        ),
        (
            Errors,
            [
                Errors.BAD_REQUEST,
                Errors.REPO_DEACTIVATED,
                Errors.REPO_MISSING_VALID_BOT,
                Errors.REPO_NOT_FOUND,
                Errors.REPO_BLACKLISTED,
                Errors.COMMIT_NOT_FOUND,
                Errors.COMMIT_UPLOAD_LIMIT,
                Errors.OWNER_UPLOAD_LIMIT,
                Errors.GIT_CLIENT_ERROR,
                Errors.REPORT_NOT_FOUND,
                Errors.REPORT_EXPIRED,
                Errors.REPORT_EMPTY,
                Errors.FILE_NOT_IN_STORAGE,
                Errors.UPLOAD_NOT_FOUND,
                Errors.UNSUPPORTED_FORMAT,
                Errors.TASK_TIMED_OUT,
                Errors.SKIPPED_NOTIFICATIONS,
                Errors.INTERNAL_LOCK_ERROR,
                Errors.INTERNAL_RETRYING,
                Errors.INTERNAL_OUT_OF_RETRIES,
                Errors.INTERNAL_NO_PENDING_JOBS,
                Errors.INTERNAL_NO_ARGUMENTS,
                Errors.INTERNAL_APP_RATE_LIMITED,
                Errors.INTERNAL_OTHER_JOB,
                Errors.UNKNOWN,
            ],
            [
                "br",
                "rd",
                "rmb",
                "rnf",
                "rb",
                "cnf",
                "cul",
                "oul",
                "gce",
                "rptnf",
                "rpe",
                "rem",
                "fnis",
                "unf",
                "uf",
                "tto",
                "sn",
                "int_le",
                "int_re",
                "int_or",
                "int_np",
                "int_na",
                "int_rl",
                "int_oj",
                "u",
            ],
            [
                "Bad request, see API response for details",
                "Repository is deactivated within Codecov",
                "Repository is missing a valid bot",
                "Repository not found by the configured bot",
                "Repository is blacklisted by Codecov",
                "Commit not found in the repository",
                "Upload limit exceeded for this commit",
                "Owner (user or team) upload limit exceeded",
                "Git client returned a 4xx error",
                "Report not found for the commit",
                "Report expired and cannot be processed",
                "Report is empty and cannot be processed",
                "File not found in storage",
                "Upload not found for the commit",
                (
                    "Unsupported coverage report format. Please check that the"
                    " file is a valid coverage report and that the report"
                    " format is supported by Codecov."
                ),
                "Task timed out",
                "Notifications were skipped due to configuration",
                "Unable to acquire or release lock",
                "Retrying the upload task",
                "Out of retries for the upload task",
                "No pending jobs found for the commit",
                "No arguments found in Redis for the upload task",
                "Codecov app is rate limited by the git client",
                "Another identical job is already running or will run for this commit",
                "Unknown error",
            ],
        ),
    ],
)
def test_enums(
    enum_class: Milestones | Errors | Endpoints,
    enum_entries: list[Milestones | Errors | Endpoints],
    enum_values: list[str],
    enum_labels: list[str],
):
    """
    This test ensures that when any of the upload breadcrumb enums are updated,
    a developer is reminded to:

    * Consider the implications of removing or renaming an enum value
      * The values and labels are used in various places, including being shown
        to users, Django admins, and in Prometheus metrics
      * Old values may still be present in the database so reading them may
        fail and cause unexpected behavior
    * Update this test with the added/changed/removed enum value(s) and/or
      label(s)
    """

    message = (
        "You are modifying an important enum. Please read the comments"
        " associated with this test for details."
    )

    assert len(enum_class) == len(enum_entries), message
    for entry, value, label in zip(enum_entries, enum_values, enum_labels):
        assert entry.value == value, message
        assert entry.label == label, message


class TestBreadcrumbData:
    def test_valid_all_fields(self):
        data = BreadcrumbData(
            milestone=Milestones.FETCHING_COMMIT_DETAILS,
            endpoint=Endpoints.CREATE_COMMIT,
            error=Errors.UNKNOWN,
            error_text="An unknown error occurred.",
        )
        assert data.milestone == Milestones.FETCHING_COMMIT_DETAILS
        assert data.endpoint == Endpoints.CREATE_COMMIT
        assert data.error == Errors.UNKNOWN
        assert data.error_text == "An unknown error occurred."

    def test_valid_some_fields(self):
        data = BreadcrumbData(
            milestone=Milestones.FETCHING_COMMIT_DETAILS,
            error=None,
        )
        assert data.milestone == Milestones.FETCHING_COMMIT_DETAILS
        assert data.endpoint is None
        assert data.error is None
        assert data.error_text is None

    def test_invalid_empty(self):
        with pytest.raises(PydanticValidationError) as excinfo:
            BreadcrumbData()
        assert "at least one field must be provided." in str(excinfo.value)

    @pytest.mark.parametrize(
        "data, expected_error",
        [
            (
                {
                    "milestone": None,
                },
                "at least one field must be provided.",
            ),
            (
                {
                    "milestone": Milestones.NOTIFICATIONS_SENT,
                    "random_extra_field": "value",
                },
                "Extra inputs are not permitted",
            ),
            (
                {
                    "error": Errors.REPO_NOT_FOUND,
                    "error_text": "",
                },
                "field must not be empty.",
            ),
            (
                {
                    "milestone": Milestones.FETCHING_COMMIT_DETAILS,
                    "endpoint": Endpoints.CREATE_COMMIT,
                    "error": None,
                    "error_text": "Error text without an error",
                },
                "'error_text' is provided, but 'error' is missing.",
            ),
            (
                {
                    "milestone": Milestones.FETCHING_COMMIT_DETAILS,
                    "endpoint": Endpoints.CREATE_COMMIT,
                    "error": Errors.UNKNOWN,
                },
                "'error_text' must be provided when 'error' is UNKNOWN.",
            ),
        ],
    )
    def test_invalid_fields(self, data, expected_error):
        with pytest.raises(ValueError) as excinfo:
            BreadcrumbData(**data)
        assert expected_error in str(excinfo.value)

    def test_frozen_fields(self):
        data = BreadcrumbData(
            milestone=Milestones.PREPARING_FOR_REPORT,
            endpoint=Endpoints.CREATE_REPORT,
        )
        with pytest.raises(PydanticValidationError) as excinfo:
            data.milestone = Milestones.UPLOAD_COMPLETE
        assert "Instance is frozen" in str(excinfo.value)

    def test_all_fields_dump(self):
        data = BreadcrumbData(
            milestone=Milestones.FETCHING_COMMIT_DETAILS,
            endpoint=Endpoints.CREATE_COMMIT,
            error=Errors.UNKNOWN,
            error_text="An unknown error occurred.",
        )
        dumped_data = data.model_dump()
        expected_data = {
            "milestone": Milestones.FETCHING_COMMIT_DETAILS.value,
            "endpoint": Endpoints.CREATE_COMMIT.value,
            "error": Errors.UNKNOWN.value,
            "error_text": "An unknown error occurred.",
        }
        assert dumped_data == expected_data

    def test_some_fields_dump(self):
        data = BreadcrumbData(
            milestone=Milestones.FETCHING_COMMIT_DETAILS,
            endpoint=None,
        )
        dumped_data = data.model_dump()
        expected_data = {
            "milestone": Milestones.FETCHING_COMMIT_DETAILS.value,
        }
        assert dumped_data == expected_data
        # Make sure that None values are excluded no matter what
        assert dumped_data == data.model_dump(exclude_none=False)

    def test_pass_validate_function(self):
        data = BreadcrumbData(
            milestone=Milestones.FETCHING_COMMIT_DETAILS,
            endpoint=Endpoints.CREATE_COMMIT,
            error=Errors.UNKNOWN,
            error_text="An unknown error occurred.",
        )
        assert BreadcrumbData.model_validate(data) == data
        assert BreadcrumbData.django_validate(data) is None

    def test_fail_validate_function(self):
        data = {
            "milestone": Milestones.FETCHING_COMMIT_DETAILS,
            "endpoint": Endpoints.CREATE_COMMIT,
            "error_text": "An unknown error occurred.",
        }
        with pytest.raises(PydanticValidationError) as excinfo:
            BreadcrumbData.model_validate(data)
        assert "'error_text' is provided, but 'error' is missing." in str(excinfo.value)
        with pytest.raises(ValidationError) as excinfo:
            BreadcrumbData.django_validate(data)
        assert "'error_text' is provided, but 'error' is missing." in str(excinfo.value)


@pytest.mark.django_db
class TestUploadBreadcrumb:
    def test_valid_create(self):
        breadcrumb = UploadBreadcrumbFactory.create()
        assert isinstance(breadcrumb, UploadBreadcrumb)
        assert (
            breadcrumb.breadcrumb_data
            == BreadcrumbData(**breadcrumb.breadcrumb_data).model_dump()
        )

    def test_empty_commit_sha(self):
        with pytest.raises(IntegrityError) as excinfo:
            UploadBreadcrumb.objects.create(
                commit_sha=None,
                repo_id=1,
                upload_ids=[1, 2, 3],
                sentry_trace_id="trace123",
                breadcrumb_data={
                    "milestone": Milestones.FETCHING_COMMIT_DETAILS,
                    "endpoint": Endpoints.CREATE_COMMIT,
                    "error": Errors.UNKNOWN,
                    "error_text": "An unknown error occurred.",
                },
            )
        assert 'null value in column "commit_sha"' in str(excinfo.value)

    @pytest.mark.parametrize(
        "breadcrumb_data, exception, exception_text",
        [
            (None, IntegrityError, 'null value in column "breadcrumb_data"'),
            ({}, ValidationError, "This field cannot be blank."),
            (
                {
                    "milestone": Milestones.UPLOAD_COMPLETE,
                    "endpoint": "random_endpoint",
                    "error": Errors.UNSUPPORTED_FORMAT,
                },
                ValidationError,
                "1 validation error for BreadcrumbData\\nendpoint",
            ),
        ],
    )
    def test_invalid_breadcrumb_data(self, breadcrumb_data, exception, exception_text):
        with pytest.raises(exception) as excinfo:
            upf = UploadBreadcrumbFactory.create(breadcrumb_data=breadcrumb_data)
            upf.full_clean()
        assert exception_text in str(excinfo.value)
