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
                "field must not be None or empty.",
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
                    "milestone": Milestones.UPLOAD_COMPLETE,
                    "endpoint": None,
                },
                "field must not be None or empty.",
            ),
            (
                {
                    "milestone": Milestones.PREPARING_FOR_REPORT,
                    "endpoint": Endpoints.CREATE_REPORT,
                    "error": None,
                },
                "field must not be None or empty.",
            ),
            (
                {
                    "error": Errors.UNRECOGNIZED_FORMAT,
                    "error_text": None,
                },
                "field must not be None or empty.",
            ),
            (
                {
                    "error": Errors.MISSING_TOKEN,
                    "error_text": "",
                },
                "field must not be None or empty.",
            ),
            (
                {
                    "milestone": Milestones.FETCHING_COMMIT_DETAILS,
                    "endpoint": Endpoints.CREATE_COMMIT,
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
                    "error": Errors.UNRECOGNIZED_FORMAT,
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
