from typing import Any

from django.conf import settings
from django.db.models import QuerySet
from rest_framework import serializers

from codecov_auth.models import Owner
from core.models import Commit, Repository
from reports.models import CommitReport, ReportSession, RepositoryFlag
from services.task import TaskService
from shared.api_archive.archive import ArchiveService
from shared.django_apps.upload_breadcrumbs.models import BreadcrumbData, Milestones


class FlagListField(serializers.ListField):
    child = serializers.CharField()

    def to_representation(self, data: QuerySet) -> list[str | None]:
        return [item.flag_name if item is not None else None for item in data.all()]


class UploadSerializer(serializers.ModelSerializer):
    flags = FlagListField(required=False)
    ci_url = serializers.CharField(source="build_url", required=False, allow_null=True)
    version = serializers.CharField(write_only=True, required=False)
    url = serializers.SerializerMethodField()
    storage_path = serializers.CharField(write_only=True, required=False)
    ci_service = serializers.CharField(write_only=True, required=False)

    class Meta:
        read_only_fields = (
            "external_id",
            "created_at",
            "raw_upload_location",
            "state",
            "provider",
            "upload_type",
            "url",
        )
        fields = read_only_fields + (
            "ci_url",
            "flags",
            "env",
            "name",
            "job_code",
            "version",
            "storage_path",
            "ci_service",
        )
        model = ReportSession

    raw_upload_location = serializers.SerializerMethodField()

    def get_raw_upload_location(self, obj: ReportSession) -> str:
        archive_service = ArchiveService(repository=None)
        return archive_service.create_presigned_put(obj.storage_path)

    def get_url(self, obj: ReportSession) -> str:
        repository = obj.report.commit.repository
        commit = obj.report.commit
        return f"{settings.CODECOV_DASHBOARD_URL}/{repository.author.service}/{repository.author.username}/{repository.name}/commit/{commit.commitid}"

    def _create_existing_flags_map(self, repoid: int) -> dict:
        existing_flags = RepositoryFlag.objects.filter(repository_id=repoid).all()
        existing_flags_map = {}
        for flag_obj in existing_flags:
            existing_flags_map[flag_obj.flag_name] = flag_obj
        return existing_flags_map

    def create(self, validated_data: dict[str, Any]) -> ReportSession | None:
        flag_names = (
            validated_data.pop("flags") if "flags" in validated_data.keys() else []
        )
        repoid = validated_data.pop("repo_id", None)

        # default is necessary here, or else if the key is not in the dict
        # the below will throw a KeyError
        validated_data.pop("version", None)
        # ReportSession uses provider but the input from CLI is ci_service, so
        # we rename that field before creating.
        validated_data["provider"] = validated_data.pop("ci_service", None)

        upload = ReportSession.objects.create(**validated_data)
        flags = []

        if upload:
            existing_flags_map = self._create_existing_flags_map(repoid)
            for individual_flag in flag_names:
                flag_obj = existing_flags_map.get(individual_flag, None)
                if flag_obj is None:
                    flag_obj = RepositoryFlag.objects.create(
                        repository_id=repoid, flag_name=individual_flag
                    )
                flags.append(flag_obj)
            upload.flags.set(flags)
            return upload


class OwnerSerializer(serializers.ModelSerializer):
    class Meta:
        model = Owner
        fields = (
            "avatar_url",
            "service",
            "username",
            "name",
            "ownerid",
        )
        read_only_fields = fields


class RepositorySerializer(serializers.ModelSerializer):
    is_private = serializers.BooleanField(source="private")

    class Meta:
        model = Repository
        fields = ("name", "is_private", "active", "language", "yaml")
        read_only_fields = fields


class CommitSerializer(serializers.ModelSerializer):
    author = OwnerSerializer(read_only=True)
    repository = RepositorySerializer(read_only=True)

    class Meta:
        model = Commit
        read_only_fields = (
            "message",
            "timestamp",
            "ci_passed",
            "state",
            "repository",
            "author",
        )
        fields = read_only_fields + (
            "commitid",
            "parent_commit_id",
            "pullid",
            "branch",
        )

    def create(self, validated_data: dict[str, Any]) -> Commit:
        repo = validated_data.pop("repository", None)
        commitid = validated_data.pop("commitid", None)
        commit, created = Commit.objects.get_or_create(
            repository=repo, commitid=commitid, defaults=validated_data
        )

        if created:
            TaskService().update_commit(
                commitid=commit.commitid, repoid=commit.repository.repoid
            )
        else:
            TaskService().upload_breadcrumb(
                commit_sha=commit.commitid,
                repo_id=repo.repoid,
                breadcrumb_data=BreadcrumbData(
                    milestone=Milestones.COMMIT_PROCESSED,
                ),
            )

        return commit


class CommitReportSerializer(serializers.ModelSerializer):
    commit_sha = serializers.CharField(source="commit.commitid", read_only=True)

    class Meta:
        model = CommitReport
        read_only_fields = (
            "external_id",
            "created_at",
            "commit_sha",
        )
        fields = read_only_fields + ("code",)

    def validate_code(self, value):
        if value not in (None, "default"):
            raise serializers.ValidationError(
                "Using a non-default `code` has been deprecated"
            )
        return None

    def create(self, validated_data: dict[str, Any]) -> tuple[CommitReport, bool]:
        report = (
            CommitReport.objects.coverage_reports()
            .filter(
                code=validated_data.get("code"),
                commit_id=validated_data.get("commit_id"),
            )
            .first()
        )
        if report:
            if report.report_type is None:
                report.report_type = CommitReport.ReportType.COVERAGE
                report.save()
            return report, False
        return super().create(validated_data), True
