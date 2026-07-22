import pytest

from helpers.tests.unit.test_checkpoint_logger import (
    CounterAssertion,
    CounterAssertionSet,
)
from shared.django_apps.upload_breadcrumbs.models import (
    BreadcrumbData,
    Endpoints,
    Errors,
    Milestones,
    UploadBreadcrumb,
)
from tasks.upload_breadcrumb import UploadBreadcrumbTask


class TestUploadBreadcrumbTask:
    @pytest.mark.django_db
    def test_standard_breadcrumb(self, dbsession):
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_endpoint_total",
                {"endpoint": Endpoints.CREATE_COMMIT.label},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_uploader_total",
                {"uploader": "codecov-cli"},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.FETCHING_COMMIT_DETAILS.label},
                0,
            ),
        ]

        with CounterAssertionSet(counter_assertions):
            result = UploadBreadcrumbTask().run_impl(
                _db_session=dbsession,
                commit_sha="abc123",
                repo_id=1,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.FETCHING_COMMIT_DETAILS,
                    endpoint=Endpoints.CREATE_COMMIT,
                    uploader="codecov-cli/5.5.0",
                ),
                upload_ids=[1, 2],
                sentry_trace_id="trace123",
            )

        assert result == {"successful": True}
        assert UploadBreadcrumb.objects.count() == 0

    @pytest.mark.django_db
    def test_error_breadcrumb(self, dbsession):
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_error_total",
                {"error": Errors.TASK_TIMED_OUT.label},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.COMPILING_UPLOADS.label},
                0,
            ),
        ]

        with CounterAssertionSet(counter_assertions):
            result = UploadBreadcrumbTask().run_impl(
                _db_session=dbsession,
                commit_sha="def456",
                repo_id=2,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.COMPILING_UPLOADS,
                    error=Errors.TASK_TIMED_OUT,
                ),
                upload_ids=[3],
            )

        assert result == {"successful": True}
        assert UploadBreadcrumb.objects.count() == 0

    @pytest.mark.django_db
    def test_counter_with_all_fields(self, dbsession):
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_endpoint_total",
                {"endpoint": Endpoints.DO_UPLOAD.label},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_uploader_total",
                {"uploader": "github-action-uploader"},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_error_total",
                {"error": Errors.UNSUPPORTED_FORMAT.label},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.PROCESSING_UPLOAD.label},
                0,
            ),
        ]

        with CounterAssertionSet(counter_assertions):
            UploadBreadcrumbTask().run_impl(
                _db_session=dbsession,
                commit_sha="test123",
                repo_id=1,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.PROCESSING_UPLOAD,
                    endpoint=Endpoints.DO_UPLOAD,
                    uploader="github-action-2.1.0-uploader-0.8.0",
                    error=Errors.UNSUPPORTED_FORMAT,
                ),
            )

    @pytest.mark.django_db
    def test_counter_milestone_only(self, dbsession):
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.READY_FOR_REPORT.label},
                0,
            ),
            CounterAssertion(
                "upload_breadcrumbs_endpoint_total",
                {"endpoint": Endpoints.CREATE_COMMIT.label},
                0,
            ),
        ]

        with CounterAssertionSet(counter_assertions):
            UploadBreadcrumbTask().run_impl(
                _db_session=dbsession,
                commit_sha="test123",
                repo_id=1,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.READY_FOR_REPORT,
                ),
            )
