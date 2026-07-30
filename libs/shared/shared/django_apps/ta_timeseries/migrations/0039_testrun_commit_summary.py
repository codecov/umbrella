import django.contrib.postgres.fields
import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0038_add_compression"),
    ]

    operations = [
        migrations.CreateModel(
            name="TestrunCommitSummary",
            fields=[
                ("id", models.BigAutoField(primary_key=True, serialize=False)),
                ("repo_id", models.BigIntegerField()),
                ("commit_sha", models.TextField()),
                ("test_id", models.BinaryField()),
                ("last_timestamp", models.DateTimeField()),
                ("outcome", models.TextField()),
                ("computed_name", models.TextField(null=True)),
                ("failure_message", models.TextField(null=True)),
                ("upload_id", models.BigIntegerField(null=True)),
                ("duration_seconds", models.FloatField(null=True)),
                (
                    "flags",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.TextField(), null=True, size=None
                    ),
                ),
            ],
            options={
                "db_table": "ta_testrun_commit_summary",
                "app_label": "ta_timeseries",
            },
        ),
        migrations.AddConstraint(
            model_name="testruncommitsummary",
            constraint=models.UniqueConstraint(
                fields=["repo_id", "commit_sha", "test_id"],
                name="ta_commit_summary__repo_commit_test_uniq",
            ),
        ),
        migrations.AddIndex(
            model_name="testruncommitsummary",
            index=models.Index(
                fields=["repo_id", "commit_sha"],
                name="ta_commit_summary__repo_commit_i",
            ),
        ),
    ]