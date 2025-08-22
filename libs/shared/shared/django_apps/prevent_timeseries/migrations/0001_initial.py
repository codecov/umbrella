import django.contrib.postgres.fields
import django_prometheus.models
from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="Testrun",
            fields=[
                ("timestamp", models.DateTimeField(primary_key=True, serialize=False)),
                ("repo_id", models.BigIntegerField()),
                ("test_id", models.BinaryField()),
                ("testsuite", models.TextField(null=True)),
                ("classname", models.TextField(null=True)),
                ("name", models.TextField(null=True)),
                ("computed_name", models.TextField(null=True)),
                ("outcome", models.TextField()),
                ("duration_seconds", models.FloatField(null=True)),
                ("failure_message", models.TextField(null=True)),
                ("framework", models.TextField(null=True)),
                ("filename", models.TextField(null=True)),
                ("commit_sha", models.TextField(null=True)),
                ("branch", models.TextField(null=True)),
                (
                    "flags",
                    django.contrib.postgres.fields.ArrayField(
                        base_field=models.TextField(), null=True, size=None
                    ),
                ),
                ("upload_id", models.BigIntegerField(null=True)),
                ("properties", models.JSONField(null=True)),
            ],
            options={"db_table": "prevent_timeseries_testrun"},
            bases=(
                django_prometheus.models.ExportModelOperationsMixin(
                    "prevent_timeseries.testrun"
                ),
                models.Model,
            ),
        ),
        migrations.RunSQL(
            "ALTER TABLE prevent_timeseries_testrun DROP CONSTRAINT prevent_timeseries_testrun_pkey;",
            reverse_sql="",
        ),
        migrations.RunSQL(
            "SELECT create_hypertable('prevent_timeseries_testrun', 'timestamp');",
            reverse_sql="",
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "branch", "timestamp"],
                name="prevent_ts__branch_i",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "branch", "test_id", "timestamp"],
                name="prevent_ts__branch_test_i",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "test_id", "timestamp"],
                name="prevent_ts__test_i",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "commit_sha", "timestamp"],
                name="prevent_ts__commit_i",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["upload_id", "outcome"],
                name="prevent_ts__upload_i",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["upload_id", "outcome", "test_id"],
                name="prevent_ts_upload_outcome_test_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "timestamp"],
                name="prevent_ts__repo_timestamp_idx",
            ),
        ),
        migrations.AddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "branch", "timestamp"],
                name="prevent_ts__repo_branch_time_idx",
            ),
        ),
        migrations.RunSQL(
            """
            CREATE OR REPLACE FUNCTION array_merge_dedup(anyarray, anyarray)
            RETURNS anyarray LANGUAGE sql IMMUTABLE AS $$
                SELECT array_agg(DISTINCT x)
                FROM (
                    SELECT unnest($1) as x
                    UNION
                    SELECT unnest($2)
                ) s;
            $$;
            CREATE OR REPLACE AGGREGATE array_merge_dedup_agg(anyarray) (
                SFUNC = array_merge_dedup,
                STYPE = anyarray,
                INITCOND = '{}'
            );
            """,
            reverse_sql="""
            DROP AGGREGATE IF EXISTS array_merge_dedup_agg(anyarray);
            DROP FUNCTION IF EXISTS array_merge_dedup(anyarray, anyarray);
            """,
        ),
    ]
