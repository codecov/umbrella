import mmh3
from django.contrib.postgres.fields import ArrayField
from django.db import models
from django_prometheus.models import ExportModelOperationsMixin

from shared.django_apps.db import sane_repr

TA_TIMESERIES_APP_LABEL = "ta_timeseries"


def calc_test_id(name: str, classname: str, testsuite: str) -> bytes:
    """
    Calculate a test ID hash from test name, classname, and testsuite.

    Args:
        name: Test name
        classname: Test class name
        testsuite: Test suite name

    Returns:
        bytes: Hash digest as bytes
    """
    h = mmh3.mmh3_x64_128()  # assumes we're running on x64 machines
    h.update(testsuite.encode("utf-8"))
    h.update(classname.encode("utf-8"))
    h.update(name.encode("utf-8"))
    test_id_hash = h.digest()

    return test_id_hash


class Testrun(ExportModelOperationsMixin("ta_timeseries.testrun"), models.Model):
    __test__ = False
    timestamp = models.DateTimeField(null=False, primary_key=True)

    test_id = models.BinaryField(null=False)

    name = models.TextField(null=True)
    classname = models.TextField(null=True)
    testsuite = models.TextField(null=True)
    computed_name = models.TextField(null=True)

    outcome = models.TextField(null=False)

    duration_seconds = models.FloatField(null=True)
    failure_message = models.TextField(null=True)
    framework = models.TextField(null=True)
    filename = models.TextField(null=True)

    repo_id = models.BigIntegerField(null=False)
    commit_sha = models.TextField(null=True)
    branch = models.TextField(null=True)

    flags = ArrayField(models.TextField(), null=True)
    upload_id = models.BigIntegerField(null=True)

    properties = models.JSONField(null=True)

    __repr__ = sane_repr("timestamp", "test_id", "name", "outcome", "flags")  # type: ignore

    class Meta:
        app_label = TA_TIMESERIES_APP_LABEL
        indexes = [
            models.Index(
                name="ta_ts__branch_test_i",
                fields=["repo_id", "branch", "test_id", "timestamp"],
            ),
            models.Index(
                name="ta_ts__commit_i",
                fields=["repo_id", "commit_sha", "timestamp"],
            ),
            models.Index(
                name="ta_ts__upload_i",
                fields=["upload_id", "outcome"],
            ),
            models.Index(
                name="ta_ts_upload_outcome_test_idx",
                fields=["upload_id", "outcome", "test_id"],
            ),
        ]


class TestrunBranchSummary(
    ExportModelOperationsMixin("ta_timeseries.testrun_branch_summary"),
    models.Model,
):
    __test__ = False
    timestamp_bin = models.DateTimeField(primary_key=True)
    repo_id = models.IntegerField()
    branch = models.TextField()
    name = models.TextField()
    classname = models.TextField()
    testsuite = models.TextField()
    computed_name = models.TextField()
    failing_commits = models.IntegerField()
    avg_duration_seconds = models.FloatField()
    last_duration_seconds = models.FloatField()
    pass_count = models.IntegerField()
    fail_count = models.IntegerField()
    skip_count = models.IntegerField()
    flaky_fail_count = models.IntegerField()
    updated_at = models.DateTimeField()
    flags = ArrayField(models.TextField(), null=True)

    __repr__ = sane_repr(
        "timestamp_bin",
        "branch",
        "computed_name",
        "pass_count",
        "fail_count",
        "skip_count",
        "flaky_fail_count",
        "flags",
    )  # type: ignore

    class Meta:
        app_label = TA_TIMESERIES_APP_LABEL
        db_table = "ta_timeseries_testrun_branch_summary_1day"
        managed = False


class TestrunSummary(
    ExportModelOperationsMixin("ta_timeseries.testrun_summary"),
    models.Model,
):
    __test__ = False
    timestamp_bin = models.DateTimeField(primary_key=True)
    repo_id = models.IntegerField()
    name = models.TextField()
    classname = models.TextField()
    testsuite = models.TextField()
    computed_name = models.TextField()
    failing_commits = models.IntegerField()
    avg_duration_seconds = models.FloatField()
    last_duration_seconds = models.FloatField()
    pass_count = models.IntegerField()
    fail_count = models.IntegerField()
    skip_count = models.IntegerField()
    flaky_fail_count = models.IntegerField()
    updated_at = models.DateTimeField()
    flags = ArrayField(models.TextField(), null=True)

    __repr__ = sane_repr(
        "timestamp_bin",
        "computed_name",
        "pass_count",
        "fail_count",
        "skip_count",
        "flaky_fail_count",
        "flags",
    )  # type: ignore

    class Meta:
        app_label = TA_TIMESERIES_APP_LABEL
        db_table = "ta_timeseries_testrun_summary_1day"
        managed = False
