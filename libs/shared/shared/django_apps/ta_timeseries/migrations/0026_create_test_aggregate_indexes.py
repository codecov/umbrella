from django.db import migrations


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0025_add_test_aggregate_policies"),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_test_aggregate_hourly_repo_id_bucket_idx
            ON ta_timeseries_test_aggregate_hourly (repo_id, bucket_hourly DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_test_aggregate_hourly_repo_id_bucket_idx;",
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_test_aggregate_daily_repo_id_bucket_idx
            ON ta_timeseries_test_aggregate_daily (repo_id, bucket_daily DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_test_aggregate_daily_repo_id_bucket_idx;",
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_branch_test_aggregate_hourly_repo_branch_bucket_idx
            ON ta_timeseries_branch_test_aggregate_hourly (repo_id, branch, bucket_hourly DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_branch_test_aggregate_hourly_repo_branch_bucket_idx;",
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_branch_test_aggregate_daily_repo_branch_bucket_idx
            ON ta_timeseries_branch_test_aggregate_daily (repo_id, branch, bucket_daily DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_branch_test_aggregate_daily_repo_branch_bucket_idx;",
        ),
    ]
