from django.db import migrations


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0025_add_test_aggregate_policies"),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_test_aggregate_daily_repo_id_ts_bin_idx
            ON ta_timeseries_test_aggregate_daily (repo_id, timestamp_bin DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_test_aggregate_daily_repo_id_ts_bin_idx;",
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_branch_test_aggregate_daily_repo_branch_ts_bin_idx
            ON ta_timeseries_branch_test_aggregate_daily (repo_id, branch, timestamp_bin DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_branch_test_aggregate_daily_repo_branch_ts_bin_idx;",
        ),
    ]
