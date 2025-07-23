import logging
from typing import Protocol, Required, TypedDict

from django.conf import settings
from rest_framework.exceptions import ValidationError
from rest_framework.request import Request

from codecov_auth.models import Service
from core.models import Commit, Repository
from reports.models import CommitReport
from upload.views.helpers import get_repository_from_string

log = logging.getLogger(__name__)


class UploadViewKwargs(TypedDict, total=False):
    """
    Type hints for kwargs of views that use `GetterMixin`.
    """

    service: Required[str]
    repo: Required[str]
    # Optional fields that may be present
    commit_sha: str
    report_code: str


class RequestProtocol(Protocol):
    @property
    def request(self) -> Request: ...


class UploadKwargsProtocol(Protocol):
    @property
    def kwargs(self) -> UploadViewKwargs: ...


class ShelterMixin:
    def is_shelter_request(self: RequestProtocol) -> bool:
        """
        Returns true when the incoming request originated from a Shelter.
        Shelter adds an `X-Shelter-Token` header which contains a shared secret.
        Use of that shared secret allows certain privileged functionality that normal
        uploads cannot access.
        """
        shelter_token = self.request.META.get("HTTP_X_SHELTER_TOKEN")
        return bool(shelter_token and shelter_token == settings.SHELTER_SHARED_SECRET)


class GetterMixin(ShelterMixin):
    def get_repo(self: UploadKwargsProtocol) -> Repository:
        service = self.kwargs["service"]
        repo_slug = self.kwargs["repo"]
        try:
            service_enum = Service(service)
        except ValueError:
            log.warning(f"Service not found: {service}", extra={"repo_slug": repo_slug})
            raise ValidationError(f"Service not found: {service}")

        repository = get_repository_from_string(service_enum, repo_slug)

        if not repository:
            log.warning(
                "Repository not found",
                extra={"repo_slug": repo_slug},
            )
            raise ValidationError("Repository not found")
        return repository

    def get_commit(self: UploadKwargsProtocol, repo: Repository) -> Commit:
        commit_sha = self.kwargs.get("commit_sha")
        try:
            commit = Commit.objects.get(
                commitid=commit_sha, repository__repoid=repo.repoid
            )
            return commit
        except Commit.DoesNotExist:
            log.warning(
                "Commit SHA not found",
                extra={"repo": repo.name, "commit_sha": commit_sha},
            )
            raise ValidationError("Commit SHA not found")

    def get_report(
        self: UploadKwargsProtocol,
        commit: Commit,
        report_type: CommitReport.ReportType | None = CommitReport.ReportType.COVERAGE,
    ) -> CommitReport:
        report_code = self.kwargs.get("report_code")
        if report_code not in (None, "default"):
            raise ValidationError("Non-default `report_code` has been deprecated")

        queryset = CommitReport.objects.filter(code=None, commit=commit)
        if report_type == CommitReport.ReportType.COVERAGE:
            queryset = queryset.coverage_reports()
        else:
            queryset = queryset.filter(report_type=report_type)
        report = queryset.first()
        if report is None:
            log.warning(
                "Report not found",
                extra={"commit_sha": commit.commitid},
            )
            raise ValidationError("Report not found")
        if report.report_type is None:
            report.report_type = CommitReport.ReportType.COVERAGE
            report.save()
        return report
