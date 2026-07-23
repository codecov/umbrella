"""Seed local data for exercising Commit admin inlines (reports + notifications).

Run from the umbrella repo root (not apps/codecov-api):

    make devenv.seed-commit-admin-data

Or directly:

    docker compose run --entrypoint python --rm api manage.py seed_commit_admin_data
    docker compose run --entrypoint python --rm api manage.py seed_commit_admin_data --with-uploads
"""

from hashlib import sha1

from django.core.management.base import BaseCommand
from django.urls import reverse
from django.utils import timezone

from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.core.models import Commit, CommitNotification, Repository
from shared.django_apps.reports.models import CommitReport, ReportSession, ReportType

SEED_OWNER_SERVICE_ID = "seed-commit-admin"
SEED_REPO_NAME = "seed-commit-admin-demo"
SEED_COMMIT_MESSAGE = "Seed commit for Commit admin inline demo"
SEED_COMMIT_SHA = sha1(SEED_COMMIT_MESSAGE.encode()).hexdigest()

REPORT_SPECS = (
    (ReportType.COVERAGE, "default"),
    (ReportType.TEST_RESULTS, "test-results"),
    (ReportType.BUNDLE_ANALYSIS, "bundle-analysis"),
)

NOTIFICATION_SPECS = (
    (
        CommitNotification.NotificationTypes.COMMENT,
        CommitNotification.DecorationTypes.STANDARD,
        CommitNotification.States.SUCCESS,
    ),
    (
        CommitNotification.NotificationTypes.STATUS_PROJECT,
        CommitNotification.DecorationTypes.STANDARD,
        CommitNotification.States.SUCCESS,
    ),
    (
        CommitNotification.NotificationTypes.STATUS_PATCH,
        CommitNotification.DecorationTypes.UPLOAD_LIMIT,
        CommitNotification.States.PENDING,
    ),
    (
        CommitNotification.NotificationTypes.CHECKS_PROJECT,
        CommitNotification.DecorationTypes.PROCESSING_UPLOAD,
        CommitNotification.States.ERROR,
    ),
)


class Command(BaseCommand):
    help = (
        "Create a demo commit with reports and notifications for local Commit "
        "admin testing. Safe to re-run: uses fixed owner/repo/commit identifiers."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "--commit-pk",
            type=int,
            help="Attach reports/notifications to an existing commit primary key.",
        )
        parser.add_argument(
            "--with-uploads",
            action="store_true",
            help="Also create a processed upload on each report.",
        )

    def handle(self, *args, **options):
        commit = self._resolve_commit(options["commit_pk"])
        reports = self._seed_reports(commit)
        notifications = self._seed_notifications(commit)

        if options["with_uploads"]:
            uploads = self._seed_uploads(reports)
        else:
            uploads = []

        admin_url = reverse("admin:core_commit_change", args=[commit.pk])
        self.stdout.write(self.style.SUCCESS("Seed data ready."))
        self.stdout.write(f"  Commit pk:        {commit.pk}")
        self.stdout.write(f"  Commit SHA:       {commit.commitid}")
        self.stdout.write(
            f"  Repository:       {commit.repository.name} ({commit.repository.repoid})"
        )
        self.stdout.write(f"  Reports:          {len(reports)}")
        self.stdout.write(f"  Notifications:    {len(notifications)}")
        if uploads:
            self.stdout.write(f"  Uploads:          {len(uploads)}")
        self.stdout.write(f"  Admin change URL: {admin_url}")

    def _resolve_commit(self, commit_pk: int | None) -> Commit:
        if commit_pk is not None:
            return Commit.objects.select_related("repository").get(pk=commit_pk)

        owner, _ = Owner.objects.get_or_create(
            service="github",
            service_id=SEED_OWNER_SERVICE_ID,
            defaults={
                "username": "seed-commit-admin",
                "name": "Seed Commit Admin User",
                "email": "seed-commit-admin@codecov.local",
            },
        )

        repo, _ = Repository.objects.get_or_create(
            author=owner,
            name=SEED_REPO_NAME,
            service_id=f"{SEED_OWNER_SERVICE_ID}-repo",
            defaults={
                "private": True,
                "branch": "main",
                "language": "python",
                "using_integration": False,
            },
        )

        commit, _ = Commit.objects.get_or_create(
            repository=repo,
            commitid=SEED_COMMIT_SHA,
            defaults={
                "message": SEED_COMMIT_MESSAGE,
                "author": owner,
                "branch": "main",
                "state": Commit.CommitStates.COMPLETE,
                "ci_passed": True,
                "notified": True,
                "parent_commit_id": sha1(
                    f"{SEED_COMMIT_MESSAGE}-parent".encode()
                ).hexdigest(),
                "totals": {
                    "c": "85.00000",
                    "h": 17,
                    "m": 3,
                    "n": 20,
                    "f": 3,
                },
            },
        )
        return commit

    def _seed_reports(self, commit: Commit) -> list[CommitReport]:
        reports = []
        for report_type, code in REPORT_SPECS:
            report, _ = CommitReport.objects.get_or_create(
                commit=commit,
                report_type=report_type,
                code=code,
            )
            reports.append(report)
        return reports

    def _seed_notifications(self, commit: Commit) -> list[CommitNotification]:
        notifications = []
        for notification_type, decoration_type, state in NOTIFICATION_SPECS:
            notification, _ = CommitNotification.objects.update_or_create(
                commit=commit,
                notification_type=notification_type,
                defaults={
                    "decoration_type": decoration_type,
                    "state": state,
                    "updated_at": timezone.now(),
                },
            )
            notifications.append(notification)
        return notifications

    def _seed_uploads(self, reports: list[CommitReport]) -> list[ReportSession]:
        uploads = []
        for index, report in enumerate(reports, start=1):
            upload, _ = ReportSession.objects.get_or_create(
                report=report,
                build_code=f"seed-build-{index}",
                defaults={
                    "state": "processed",
                    "upload_type": "uploaded",
                    "name": f"seed-upload-{report.report_type or 'coverage'}.xml",
                },
            )
            uploads.append(upload)
        return uploads
