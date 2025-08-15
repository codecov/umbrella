from django.db import models
from django_prometheus.models import ExportModelOperationsMixin

from shared.django_apps.codecov.models import BaseModel
from shared.django_apps.db import sane_repr

# Added to avoid 'doesn't declare an explicit app_label and isn't in an application in INSTALLED_APPS' error\
# Needs to be called the same as the API app
TEST_ANALYTICS_APP_LABEL = "test_analytics"


class Flake(models.Model):
    id = models.BigAutoField(primary_key=True)

    repoid = models.IntegerField()
    test_id = models.BinaryField()
    flags_id = models.BinaryField(null=True)

    recent_passes_count = models.IntegerField()
    count = models.IntegerField()
    fail_count = models.IntegerField()
    start_date = models.DateTimeField()
    end_date = models.DateTimeField(null=True)

    class Meta:
        app_label = TEST_ANALYTICS_APP_LABEL
        db_table = "test_analytics_flake"
        indexes = [
            models.Index(fields=["repoid"]),
            models.Index(fields=["test_id"]),
            models.Index(fields=["repoid", "test_id"]),
            models.Index(fields=["repoid", "end_date"]),
        ]


class TAPullComment(BaseModel):
    """
    Stores pull request comment references without referencing the full commit
    or pull objects which are subject to Sentry retention policies.
    """

    repo_id = models.BigIntegerField()
    pull_id = models.IntegerField()
    comment_id = models.TextField(null=True)

    class Meta:
        app_label = TEST_ANALYTICS_APP_LABEL

        constraints = [
            models.UniqueConstraint(
                fields=["repo_id", "pull_id"],
                name="ta_pr_comment_repo_pull",
            ),
        ]
        indexes = [
            models.Index(
                name="ta_pr_comment_repo_pull_idx",
                fields=["repo_id", "pull_id"],
            ),
        ]


class TAUpload(ExportModelOperationsMixin("test_analytics.taupload"), BaseModel):
    repo_id = models.BigIntegerField(null=False)

    __repr__ = sane_repr("id", "repo_id", "created_at", "updated_at")  # type: ignore

    class Meta:
        app_label = TEST_ANALYTICS_APP_LABEL
        db_table = "test_analytics_upload"
        indexes = [
            models.Index(
                name="ta_upload_repo_idx",
                fields=["repo_id"],
            ),
            models.Index(
                name="ta_upload_time_idx",
                fields=["created_at"],
            ),
            models.Index(
                name="ta_upload_repo_time_idx",
                fields=["repo_id", "created_at"],
            ),
        ]
