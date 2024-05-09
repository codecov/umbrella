from rest_framework import serializers

from api.shared.commit.serializers import ReportFileSerializer, ReportSerializer


class CoverageReportSerializer(ReportSerializer):
    commit_file_url = serializers.CharField(
        label="Codecov url to see file coverage on commit. Can be unreliable with partial path names."
    )


class FileReportSerializer(ReportFileSerializer):
    commit_sha = serializers.SerializerMethodField(
        label="commit SHA of the commit for which coverage info was found"
    )
    commit_file_url = serializers.SerializerMethodField(
        label="Codecov URL to see file coverage on commit."
    )

    def get_commit_sha(self, obj):
        return self.context["commit_sha"]

    def get_commit_file_url(self, obj):
        return self.context["commit_file_url"]
