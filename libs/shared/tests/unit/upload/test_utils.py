from datetime import timedelta
from unittest.mock import PropertyMock, patch

from django.test import TestCase
from django.utils import timezone
from freezegun import freeze_time

from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
from shared.django_apps.reports.tests.factories import (
    CommitReportFactory,
    UploadFactory,
)
from shared.django_apps.user_measurements.models import UserMeasurement
from shared.plan.service import PlanService
from shared.upload.utils import (
    bulk_insert_coverage_measurements,
    insert_coverage_measurement,
    query_monthly_coverage_measurements,
)
from tests.helper import mock_all_plans_and_tiers


class CoverageMeasurement(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        mock_all_plans_and_tiers()

    def add_upload_measurements_records(
        self,
        owner: Owner,
        quantity: int,
        report_type="coverage",
        private=True,
    ):
        for _ in range(quantity):
            repo = RepositoryFactory.create(author=owner, private=private)
            commit = CommitFactory.create(repository=repo)
            report = CommitReportFactory.create(commit=commit, report_type=report_type)
            upload = UploadFactory.create(report=report)
            insert_coverage_measurement(
                owner_id=owner.ownerid,
                repo_id=repo.repoid,
                commit_id=commit.id,
                upload_id=upload.id,
                uploader_used="CLI",
                private_repo=repo.private,
                report_type=report.report_type,
            )

    def test_query_monthly_coverage_measurements(self):
        owner = OwnerFactory()
        self.add_upload_measurements_records(owner=owner, quantity=5)
        plan_service = PlanService(current_org=owner)

        monthly_measurements = query_monthly_coverage_measurements(
            plan_service=plan_service
        )
        assert monthly_measurements == 5

    def test_query_monthly_coverage_measurements_with_a_public_repo(self):
        owner = OwnerFactory()
        self.add_upload_measurements_records(owner=owner, quantity=3)
        self.add_upload_measurements_records(owner=owner, quantity=1, private=False)

        plan_service = PlanService(current_org=owner)
        monthly_measurements = query_monthly_coverage_measurements(
            plan_service=plan_service
        )
        # Doesn't query the last 3
        assert monthly_measurements == 3

    def test_query_monthly_coverage_measurements_with_non_coverage_report(self):
        owner = OwnerFactory()
        self.add_upload_measurements_records(owner=owner, quantity=3)
        self.add_upload_measurements_records(
            owner=owner, quantity=1, report_type="bundle_analysis"
        )

        plan_service = PlanService(current_org=owner)
        monthly_measurements = query_monthly_coverage_measurements(
            plan_service=plan_service
        )
        # Doesn't query the last 3
        assert monthly_measurements == 3

    def test_query_monthly_coverage_measurements_after_30_days(self):
        owner = OwnerFactory()

        # Uploads before 30 days
        freezer = freeze_time("2023-10-15T00:00:00")
        freezer.start()
        self.add_upload_measurements_records(owner=owner, quantity=3)
        freezer.stop()

        # Now
        freezer = freeze_time("2024-02-10T00:00:00")
        self.add_upload_measurements_records(owner=owner, quantity=5)

        all_measurements = UserMeasurement.objects.all()
        assert len(all_measurements) == 8

        plan_service = PlanService(current_org=owner)
        monthly_measurements = query_monthly_coverage_measurements(
            plan_service=plan_service
        )
        assert monthly_measurements == 5

    def test_query_monthly_coverage_measurements_excluding_uploads_during_trial(self):
        freezer = freeze_time("2024-02-01T00:00:00")
        freezer.start()
        owner = OwnerFactory(
            trial_status="expired",
            trial_start_date=timezone.now(),
            trial_end_date=timezone.now() + timedelta(days=14),
        )
        freezer.stop()

        freezer = freeze_time("2024-02-05T00:00:00")
        freezer.start()
        self.add_upload_measurements_records(owner=owner, quantity=3)
        freezer.stop()

        # Now
        freezer = freeze_time("2024-02-20T00:00:00")
        self.add_upload_measurements_records(owner=owner, quantity=6)

        all_measurements = UserMeasurement.objects.all()
        assert len(all_measurements) == 9

        plan_service = PlanService(current_org=owner)
        monthly_measurements = query_monthly_coverage_measurements(
            plan_service=plan_service
        )
        assert monthly_measurements == 6

    @patch(
        "shared.plan.service.PlanService.monthly_uploads_limit",
        new_callable=PropertyMock,
    )
    def test_query_monthly_coverage_measurements_beyond_monthly_limit(
        self, monthly_uploads_mock
    ):
        owner = OwnerFactory()
        self.add_upload_measurements_records(owner=owner, quantity=10)

        plan_service = PlanService(current_org=owner)
        monthly_uploads_mock.return_value = 3
        monthly_measurements = query_monthly_coverage_measurements(
            plan_service=plan_service
        )
        # 10 uploads total, max 3 returned
        assert monthly_measurements == 3

    def test_bulk_insert_user_measurements(self):
        owner = OwnerFactory()
        measurements = []
        for _ in range(5):
            repo = RepositoryFactory.create(author=owner)
            commit = CommitFactory.create(repository=repo)
            report = CommitReportFactory.create(commit=commit)
            upload = UploadFactory.create(report=report)
            measurements.append(
                UserMeasurement(
                    owner_id=owner.ownerid,
                    repo_id=repo.repoid,
                    commit_id=commit.id,
                    upload_id=upload.id,
                    uploader_used="CLI",
                    private_repo=repo.private,
                    report_type=report.report_type,
                )
            )

        inserted_measurements = bulk_insert_coverage_measurements(
            measurements=measurements
        )
        assert len(inserted_measurements) == 5
