from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0022_remove_default_cagg_indexes"),
    ]

    operations = [
        RiskyRunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_agg_hourly_repo_bucket_idx
            ON ta_timeseries_aggregate_hourly (repo_id, bucket_hourly DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_agg_hourly_repo_bucket_idx;",
        ),
        RiskyRunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_agg_daily_repo_bucket_idx
            ON ta_timeseries_aggregate_daily (repo_id, bucket_daily DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_agg_daily_repo_bucket_idx;",
        ),
        RiskyRunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_branch_agg_hourly_repo_branch_bucket_idx
            ON ta_timeseries_branch_aggregate_hourly (repo_id, branch, bucket_hourly DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_branch_agg_hourly_repo_branch_bucket_idx;",
        ),
        RiskyRunSQL(
            """
            CREATE INDEX IF NOT EXISTS ta_ts_branch_agg_daily_repo_branch_bucket_idx
            ON ta_timeseries_branch_aggregate_daily (repo_id, branch, bucket_daily DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS ta_ts_branch_agg_daily_repo_branch_bucket_idx;",
        ),
    ]
