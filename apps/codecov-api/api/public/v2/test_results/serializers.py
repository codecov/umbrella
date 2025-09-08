from rest_framework import serializers

from shared.django_apps.ta_timeseries.models import Testrun


class TestrunSerializer(serializers.ModelSerializer):
    test_id = serializers.SerializerMethodField(label="test id")
    name = serializers.CharField(read_only=True, allow_null=True, label="test name")
    classname = serializers.CharField(
        read_only=True, allow_null=True, label="class name"
    )
    testsuite = serializers.CharField(
        read_only=True, allow_null=True, label="test suite"
    )
    computed_name = serializers.CharField(
        read_only=True, allow_null=True, label="computed name"
    )
    outcome = serializers.CharField(read_only=True, label="outcome")
    duration_seconds = serializers.FloatField(
        read_only=True, allow_null=True, label="duration in seconds"
    )
    failure_message = serializers.CharField(
        read_only=True, allow_null=True, label="failure message"
    )
    framework = serializers.CharField(
        read_only=True, allow_null=True, label="framework"
    )
    filename = serializers.CharField(read_only=True, allow_null=True, label="filename")
    repo_id = serializers.IntegerField(read_only=True, label="repo id")
    commit_sha = serializers.CharField(
        read_only=True, allow_null=True, label="commit SHA"
    )
    branch = serializers.CharField(read_only=True, allow_null=True, label="branch name")
    flags = serializers.ListField(
        child=serializers.CharField(), read_only=True, allow_null=True, label="flags"
    )
    upload_id = serializers.IntegerField(
        read_only=True, allow_null=True, label="upload id"
    )
    properties = serializers.JSONField(
        read_only=True, allow_null=True, label="properties"
    )
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
