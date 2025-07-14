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
                1,
            ),
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.FETCHING_COMMIT_DETAILS.label},
                1,
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
                ),
                upload_ids=[1, 2],
                sentry_trace_id="trace123",
            )

        assert result == {"successful": True}

        rows = UploadBreadcrumb.objects.all()
        assert len(rows) == 1
        row = rows[0]
        assert row.commit_sha == "abc123"
        assert row.repo_id == 1
        assert row.breadcrumb_data["milestone"] == Milestones.FETCHING_COMMIT_DETAILS
        assert row.breadcrumb_data["endpoint"] == Endpoints.CREATE_COMMIT
        assert row.upload_ids == [1, 2]
        assert row.sentry_trace_id == "trace123"

    @pytest.mark.django_db
    def test_error_breadcrumb(self, dbsession):
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_error_total",
                {"error": Errors.TASK_TIMED_OUT.label},
                1,
            ),
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.COMPILING_UPLOADS.label},
                1,
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

        rows = UploadBreadcrumb.objects.all()
        assert len(rows) == 1
        row = rows[0]
        assert row.commit_sha == "def456"
        assert row.repo_id == 2
        assert row.breadcrumb_data["milestone"] == Milestones.COMPILING_UPLOADS
        assert row.breadcrumb_data["error"] == Errors.TASK_TIMED_OUT
        assert row.upload_ids == [3]
        assert row.sentry_trace_id is None  # No sentry trace ID provided

    @pytest.mark.django_db
    def test_counter_with_all_fields(self, dbsession):
        """Test that all counters are incremented correctly when all fields are provided."""
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_endpoint_total",
                {"endpoint": Endpoints.DO_UPLOAD.label},
                1,
            ),
            CounterAssertion(
                "upload_breadcrumbs_error_total",
                {"error": Errors.MALFORMED_INPUT.label},
                1,
            ),
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.PROCESSING_UPLOAD.label},
                1,
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
                    error=Errors.MALFORMED_INPUT,
                ),
            )

    @pytest.mark.django_db
    def test_counter_milestone_only(self, dbsession):
        """Test that only milestone counter is incremented when no error is present."""
        counter_assertions = [
            CounterAssertion(
                "upload_breadcrumbs_milestone_total",
                {"milestone": Milestones.READY_FOR_REPORT.label},
                1,
            ),
            CounterAssertion(
                "upload_breadcrumbs_endpoint_total",
                {"endpoint": Endpoints.CREATE_COMMIT.label},
                0,  # No endpoint provided
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
