from django.core.management.base import BaseCommand
from django.urls import reverse

from compare.models import CommitComparison
from core.models import Commit, CommitNotification, Repository
from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.reports.models import (
    CommitReport,
    ReportResults,
    ReportSession,
    ReportType,
)

DEMO_USERNAME = "commit-admin-demo"
DEMO_REPO_NAME = "commit-admin-demo"
PARENT_SHA = "7fe69c8a88a695add50508bf3a3b87e127be06"
INCONSISTENT_SHA = "fa59b5c4c1ddf670e633ec56f7618810ae09fea9"
HEALTHY_SHA = "a" * 40


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

        owner, _ = Owner.objects.get_or_create(
            service="github",
            username=DEMO_USERNAME,
            defaults={
                "service_id": DEMO_USERNAME,
                "name": "Commit Admin Demo",
            },
        )
        owner.user = user
        owner.save(update_fields=["user"])

        repo, _ = Repository.objects.get_or_create(
            author=owner,
            name=DEMO_REPO_NAME,
            defaults={"service_id": DEMO_USERNAME, "private": True},
        )

        parent_commit, _ = Commit.objects.update_or_create(
            repository=repo,
            commitid=PARENT_SHA,
            defaults={
                "author": owner,
                "message": "Release v0.20.0",
                "parent_commit_id": "6fdc0abcac42c365488b9c57a0868217e7dad20b",
                "state": Commit.CommitStates.COMPLETE,
                "totals": {"c": "100.00000", "h": 10, "m": 0, "n": 10},
            },
        )
        parent_report, _ = CommitReport.objects.get_or_create(
            commit=parent_commit,
            report_type=ReportType.COVERAGE,
        )
        ReportSession.objects.update_or_create(
            report=parent_report,
            name="parent-ci",
            defaults={"state": "processed"},
        )

        inconsistent_commit, _ = Commit.objects.update_or_create(
            repository=repo,
            commitid=INCONSISTENT_SHA,
            defaults={
                "author": owner,
                "message": "Release v0.20.1",
                "parent_commit_id": parent_commit.commitid,
                "state": Commit.CommitStates.COMPLETE,
                "totals": {"c": "100.00000", "h": 10, "m": 0, "n": 10},
            },
        )

        coverage_report, _ = CommitReport.objects.get_or_create(
            commit=inconsistent_commit,
            report_type=ReportType.COVERAGE,
        )
        for name, state in (
            ("stuck-upload-1", "started"),
            ("stuck-upload-2", "started"),
            ("stuck-upload-3", "started"),
            ("merged-upload", "processed"),
        ):
            ReportSession.objects.update_or_create(
                report=coverage_report,
                name=name,
                defaults={"state": state},
            )

        test_report, _ = CommitReport.objects.get_or_create(
            commit=inconsistent_commit,
            report_type=ReportType.TEST_RESULTS,
        )
        ReportSession.objects.update_or_create(
            report=test_report,
            name="test-analytics",
            defaults={"state": "processed"},
        )
        ReportResults.objects.update_or_create(
            report=test_report,
            defaults={"state": ReportResults.ReportResultsStates.ERROR},
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

        CommitNotification.objects.update_or_create(
            commit=inconsistent_commit,
            notification_type=CommitNotification.NotificationTypes.COMMENT,
            defaults={"state": CommitNotification.States.SUCCESS},
        )
        CommitNotification.objects.update_or_create(
            commit=inconsistent_commit,
            notification_type=CommitNotification.NotificationTypes.STATUS_PATCH,
            defaults={"state": CommitNotification.States.ERROR},
        )

        healthy_commit, _ = Commit.objects.update_or_create(
            repository=repo,
            commitid=HEALTHY_SHA,
            defaults={
                "author": owner,
                "message": "Healthy commit with processed uploads",
                "parent_commit_id": parent_commit.commitid,
                "state": Commit.CommitStates.COMPLETE,
                "totals": {"c": "95.00000", "h": 19, "m": 1, "n": 20},
            },
        )
        healthy_report, _ = CommitReport.objects.get_or_create(
            commit=healthy_commit,
            report_type=ReportType.COVERAGE,
        )
        ReportSession.objects.update_or_create(
            report=healthy_report,
            name="healthy-ci",
            defaults={"state": "processed"},
        )
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
