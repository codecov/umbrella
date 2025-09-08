from rest_framework import serializers

from reports.models import TestInstance
from shared.django_apps.ta_timeseries.models import Testrun


class TestInstanceSerializer(serializers.ModelSerializer):
    id = serializers.IntegerField(label="id")
    name = serializers.CharField(source="test.name", read_only=True, label="test name")
    test_id = serializers.CharField(label="test id")
    failure_message = serializers.CharField(label="test name")
    duration_seconds = serializers.FloatField(label="duration in seconds")
    commitid = serializers.CharField(label="commit SHA")
    outcome = serializers.CharField(label="outcome")
    branch = serializers.CharField(label="branch name")
    repoid = serializers.IntegerField(label="repo id")
    failure_rate = serializers.FloatField(
        source="test.failure_rate", read_only=True, label="failure rate"
    )
    commits_where_fail = serializers.ListField(
        source="test.commits_where_fail",
        read_only=True,
        label="commits where test failed",
    )

    class Meta:
        model = TestInstance
        read_only_fields = (
            "id",
            "test_id",
            "failure_message",
            "duration_seconds",
            "commitid",
            "outcome",
            "branch",
            "repoid",
            "failure_rate",
            "name",
            "commits_where_fail",
        )
        fields = read_only_fields


class TestrunSerializer(serializers.ModelSerializer):
    test_id = serializers.SerializerMethodField(label="test id")
    name = serializers.CharField(read_only=True, label="test name")
    classname = serializers.CharField(read_only=True, label="class name")
    testsuite = serializers.CharField(read_only=True, label="test suite")
    computed_name = serializers.CharField(read_only=True, label="computed name")
    outcome = serializers.CharField(read_only=True, label="outcome")
    duration_seconds = serializers.FloatField(
        read_only=True, label="duration in seconds"
    )
    failure_message = serializers.CharField(read_only=True, label="failure message")
    framework = serializers.CharField(read_only=True, label="framework")
    filename = serializers.CharField(read_only=True, label="filename")
    repo_id = serializers.IntegerField(read_only=True, label="repo id")
    commit_sha = serializers.CharField(read_only=True, label="commit SHA")
    branch = serializers.CharField(read_only=True, label="branch name")
    flags = serializers.ListField(read_only=True, label="flags")
    upload_id = serializers.IntegerField(read_only=True, label="upload id")
    properties = serializers.JSONField(read_only=True, label="properties")
    timestamp = serializers.DateTimeField(read_only=True, label="timestamp")

    def get_test_id(self, obj):
        """Convert binary test_id to string representation"""
        return obj.test_id.hex() if obj.test_id else None

    class Meta:
        model = Testrun
        read_only_fields = (
            "test_id",
            "name",
            "classname",
            "testsuite",
            "computed_name",
            "outcome",
            "duration_seconds",
            "failure_message",
            "framework",
            "filename",
            "repo_id",
            "commit_sha",
            "branch",
            "flags",
            "upload_id",
            "properties",
            "timestamp",
        )
        fields = read_only_fields
