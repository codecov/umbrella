from django.core.management.base import BaseCommand
from django.urls import reverse

from compare.models import CommitComparison
from core.models import Commit, CommitNotification
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory, UserFactory
from shared.django_apps.core.tests.factories import (
    CommitFactory,
    CommitNotificationFactory,
    RepositoryFactory,
)
from shared.django_apps.reports.models import ReportResults, ReportType
from shared.django_apps.reports.tests.factories import (
    CommitReportFactory,
    ReportResultsFactory,
    UploadFactory,
)


class Command(BaseCommand):
    help = "Seed local demo data for Commit admin changelist and detail views."

    def handle(self, *args, **options):
        user, _ = UserFactory._meta.model.objects.get_or_create(
            email="commit-admin-demo@codecov.local",
            defaults={
                "name": "Commit Admin Demo",
                "is_staff": True,
                "is_superuser": True,
            },
        )
        user.is_staff = True
        user.is_superuser = True
        user.save(update_fields=["is_staff", "is_superuser"])

        owner = OwnerFactory(username="commit-admin-demo", service="github")
        owner.user = user
        owner.save(update_fields=["user"])
        repo = RepositoryFactory(
            author=owner,
            name="commit-admin-demo",
            service_id="commit-admin-demo",
        )

        parent_commit = CommitFactory(
            repository=repo,
            author=owner,
            commitid="7fe69c8a88a695add50508bf3a3b87e127be06",
            message="Release v0.20.0",
            parent_commit_id="6fdc0abcac42c365488b9c57a0868217e7dad20b",
            state=Commit.CommitStates.COMPLETE,
            totals={"c": "100.00000", "h": 10, "m": 0, "n": 10},
        )
        parent_report = CommitReportFactory(
            commit=parent_commit,
            report_type=ReportType.COVERAGE,
        )
        UploadFactory(report=parent_report, state="processed", name="parent-ci")

        inconsistent_commit = CommitFactory(
            repository=repo,
            author=owner,
            commitid="fa59b5c4c1ddf670e633ec56f7618810ae09fea9",
            message="Release v0.20.1",
            parent_commit_id=parent_commit.commitid,
            state=Commit.CommitStates.COMPLETE,
            totals={"c": "100.00000", "h": 10, "m": 0, "n": 10},
        )

        coverage_report = CommitReportFactory(
            commit=inconsistent_commit,
            report_type=ReportType.COVERAGE,
        )
        UploadFactory(
            report=coverage_report,
            state="started",
            name="stuck-upload-1",
        )
        UploadFactory(
            report=coverage_report,
            state="started",
            name="stuck-upload-2",
        )
        UploadFactory(
            report=coverage_report,
            state="started",
            name="stuck-upload-3",
        )
        UploadFactory(
            report=coverage_report,
            state="processed",
            name="merged-upload",
        )

        test_report = CommitReportFactory(
            commit=inconsistent_commit,
            report_type=ReportType.TEST_RESULTS,
        )
        UploadFactory(report=test_report, state="processed", name="test-analytics")
        ReportResultsFactory(
            report=test_report,
            state=ReportResults.ReportResultsStates.ERROR,
        )

        CommitComparison.objects.update_or_create(
            base_commit=parent_commit,
            compare_commit=inconsistent_commit,
            defaults={
                "state": CommitComparison.CommitComparisonStates.ERROR,
                "error": CommitComparison.CommitComparisonErrors.MISSING_HEAD_REPORT,
                "patch_totals": {"hits": 0, "misses": 0, "coverage": None},
            },
        )

        CommitNotificationFactory(
            commit=inconsistent_commit,
            state=CommitNotification.States.SUCCESS,
        )
        CommitNotificationFactory(
            commit=inconsistent_commit,
            state=CommitNotification.States.ERROR,
        )

        healthy_commit = CommitFactory(
            repository=repo,
            author=owner,
            commitid="a" * 40,
            message="Healthy commit with processed uploads",
            parent_commit_id=parent_commit.commitid,
            state=Commit.CommitStates.COMPLETE,
            totals={"c": "95.00000", "h": 19, "m": 1, "n": 20},
        )
        healthy_report = CommitReportFactory(
            commit=healthy_commit,
            report_type=ReportType.COVERAGE,
        )
        UploadFactory(report=healthy_report, state="processed", name="healthy-ci")
        CommitComparison.objects.update_or_create(
            base_commit=parent_commit,
            compare_commit=healthy_commit,
            defaults={
                "state": CommitComparison.CommitComparisonStates.PROCESSED,
                "error": None,
                "patch_totals": {"hits": 5, "misses": 0, "coverage": "100.00000"},
            },
        )

        changelist_url = reverse("admin:core_commit_changelist")
        detail_url = reverse("admin:core_commit_change", args=[inconsistent_commit.pk])
        healthy_url = reverse("admin:core_commit_change", args=[healthy_commit.pk])

        self.stdout.write(self.style.SUCCESS("Commit admin demo data created."))
        self.stdout.write(f"Staff user: {user.email} (is_staff={user.is_staff})")
        self.stdout.write(
            "Admin login: use your normal dev OAuth login, or mark an existing "
            "user is_staff=True in the DB."
        )
        self.stdout.write(f"Repository repoid: {repo.repoid}")
        self.stdout.write(f"Changelist: {changelist_url}")
        self.stdout.write(f"Inconsistent commit detail: {detail_url}")
        self.stdout.write(f"Healthy commit detail: {healthy_url}")
        self.stdout.write(
            f"Search tips: repoid {repo.repoid} or SHA {inconsistent_commit.commitid}"
        )
