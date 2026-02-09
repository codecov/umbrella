import logging

from django.db import models
from django_prometheus.models import ExportModelOperationsMixin

from shared.django_apps.codecov.models import BaseCodecovModel
from shared.django_apps.reports.managers import CommitReportManager
from shared.django_apps.utils.services import get_short_service_name
from shared.reports.enums import UploadState, UploadType
from shared.upload.constants import ci

log = logging.getLogger(__name__)

# Added to avoid 'doesn't declare an explicit app_label and isn't in an application in INSTALLED_APPS' error\
# Needs to be called the same as the API app
REPORTS_APP_LABEL = "reports"


class ReportType(models.TextChoices):
    COVERAGE = "coverage"
    TEST_RESULTS = "test_results"
    BUNDLE_ANALYSIS = "bundle_analysis"


class AbstractTotals(
    ExportModelOperationsMixin("reports.abstract_totals"), BaseCodecovModel
):
    branches = models.IntegerField()
    coverage = models.DecimalField(max_digits=8, decimal_places=5)
    hits = models.IntegerField()
    lines = models.IntegerField()
    methods = models.IntegerField()
    misses = models.IntegerField()
    partials = models.IntegerField()
    files = models.IntegerField()

    class Meta:
        abstract = True


class CommitReport(
    ExportModelOperationsMixin("reports.commit_report"), BaseCodecovModel
):
    class ReportType(models.TextChoices):
        COVERAGE = "coverage"
        TEST_RESULTS = "test_results"
        BUNDLE_ANALYSIS = "bundle_analysis"

    commit = models.ForeignKey(
        "core.Commit", related_name="reports", on_delete=models.CASCADE
    )
    code = models.CharField(null=True, max_length=100)
    report_type = models.CharField(
        null=True, max_length=100, choices=ReportType.choices
    )

    class Meta:
        app_label = REPORTS_APP_LABEL
        indexes = [
            models.Index(fields=["commit_id", "id"], name="commitreport_commit_id_idx"),
        ]

    objects = CommitReportManager()


class ReportResults(
    ExportModelOperationsMixin("reports.report_results"), BaseCodecovModel
):
    class ReportResultsStates(models.TextChoices):
        PENDING = "pending"
        COMPLETED = "completed"
        ERROR = "error"

    report = models.OneToOneField(CommitReport, on_delete=models.CASCADE)
    state = models.TextField(null=True, choices=ReportResultsStates.choices)
    completed_at = models.DateTimeField(null=True)
    result = models.JSONField(default=dict)

    class Meta:
        app_label = REPORTS_APP_LABEL


class ReportLevelTotals(AbstractTotals):
    report = models.OneToOneField(CommitReport, on_delete=models.CASCADE)

    class Meta:
        app_label = REPORTS_APP_LABEL


class UploadError(ExportModelOperationsMixin("reports.upload_error"), BaseCodecovModel):
    report_session = models.ForeignKey(
        "ReportSession",
        db_column="upload_id",
        related_name="errors",
        on_delete=models.CASCADE,
    )
    error_code = models.CharField(max_length=100)
    error_params = models.JSONField(default=dict)

    class Meta:
        app_label = REPORTS_APP_LABEL
        db_table = "reports_uploaderror"
        indexes = [
            models.Index(
                fields=["report_session_id", "id"], name="uploaderror_session_id_idx"
            ),
        ]


class UploadFlagMembership(
    ExportModelOperationsMixin("reports.upload_flag_membership"), models.Model
):
    report_session = models.ForeignKey(
        "ReportSession", db_column="upload_id", on_delete=models.CASCADE
    )
    flag = models.ForeignKey("RepositoryFlag", on_delete=models.CASCADE)
    id = models.BigAutoField(primary_key=True)

    class Meta:
        app_label = REPORTS_APP_LABEL
        db_table = "reports_uploadflagmembership"
        indexes = [
            models.Index(
                fields=["report_session_id", "id"], name="uploadflag_session_id_idx"
            ),
        ]


class RepositoryFlag(
    ExportModelOperationsMixin("reports.repository_flag"), BaseCodecovModel
):
    repository = models.ForeignKey(
        "core.Repository", related_name="flags", on_delete=models.CASCADE
    )
    flag_name = models.CharField(max_length=1024)
    deleted = models.BooleanField(null=True)

    class Meta:
        app_label = REPORTS_APP_LABEL
        indexes = [
            models.Index(fields=["repository_id", "id"], name="repoflag_repo_id_idx"),
        ]


class ReportSession(
    ExportModelOperationsMixin("reports.report_session"), BaseCodecovModel
):
    # should be called Upload, but to do it we have to make the
    # constraints be manually named, which take a bit
    build_code = models.TextField(null=True)
    build_url = models.TextField(null=True)
    env = models.JSONField(null=True)
    flags = models.ManyToManyField(RepositoryFlag, through=UploadFlagMembership)
    job_code = models.TextField(null=True)
    name = models.CharField(null=True, max_length=100)
    provider = models.CharField(max_length=50, null=True)
    report = models.ForeignKey(
        "CommitReport", related_name="sessions", on_delete=models.CASCADE
    )
    state = models.CharField(max_length=100)
    storage_path = models.TextField(null=True)
    order_number = models.IntegerField(null=True)
    upload_type = models.CharField(max_length=100, default="uploaded")
    upload_extras = models.JSONField(default=dict)
    state_id = models.IntegerField(null=True, choices=UploadState.choices())
    upload_type_id = models.IntegerField(null=True, choices=UploadType.choices())

    class Meta:
        app_label = REPORTS_APP_LABEL
        db_table = "reports_upload"
        indexes = [
            models.Index(
                name="upload_report_type_idx",
                fields=["report_id", "upload_type"],
            ),
            models.Index(
                name="upload_storage_path_idx",
                fields=["storage_path"],
            ),
            models.Index(
                name="upload_report_id_idx",
                fields=["report_id", "id"],
            ),
        ]

    @property
    def ci_url(self):
        if self.build_url:
            # build_url was saved in the database
            return self.build_url

        # otherwise we need to construct it ourself (if possible)
        build_url = ci.get(self.provider, {}).get("build_url")
        if not build_url:
            return
        repository = self.report.commit.repository
        data = {
            "service_short": get_short_service_name(repository.author.service),
            "owner": repository.author,
            "upload": self,
            "repo": repository,
            "commit": self.report.commit,
        }
        return build_url.format(**data)

    @property
    def flag_names(self):
        return [flag.flag_name for flag in self.flags.all()]


class UploadLevelTotals(AbstractTotals):
    report_session = models.OneToOneField(
        ReportSession, db_column="upload_id", on_delete=models.CASCADE
    )

    class Meta:
        app_label = REPORTS_APP_LABEL
        db_table = "reports_uploadleveltotals"
        indexes = [
            models.Index(
                fields=["report_session_id", "id"],
                name="ult_session_id_idx",
            ),
        ]
