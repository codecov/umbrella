from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0017_enable_real_time_aggregates"),
    ]

    operations = [
        RiskyRunSQL(
            """
            CREATE MATERIALIZED VIEW ta_timeseries_aggregate_hourly
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT
                repo_id,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,
                
                SUM(CASE WHEN duration_seconds IS NOT NULL THEN duration_seconds ELSE 0 END) as total_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count
            FROM ta_timeseries_testrun
            GROUP BY repo_id, bucket_hourly;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_aggregate_hourly;",
        ),
        RiskyRunSQL(
            """
            CREATE MATERIALIZED VIEW ta_timeseries_aggregate_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT
                repo_id,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,
                SUM(total_duration_seconds) as total_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count
            FROM ta_timeseries_aggregate_hourly
            GROUP BY repo_id, bucket_daily;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_aggregate_daily;",
        ),
        # Create hourly branch repo summary continuous aggregate
        RiskyRunSQL(
            """
            CREATE MATERIALIZED VIEW ta_timeseries_branch_aggregate_hourly
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT
                repo_id,
                branch,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,
                
                SUM(CASE WHEN duration_seconds IS NOT NULL THEN duration_seconds ELSE 0 END) as total_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count
            FROM ta_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, bucket_hourly;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_branch_aggregate_hourly;",
        ),
        RiskyRunSQL(
            """
            CREATE MATERIALIZED VIEW ta_timeseries_branch_aggregate_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
            SELECT
                repo_id,
                branch,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,
                
                SUM(total_duration_seconds) as total_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count
            FROM ta_timeseries_branch_aggregate_hourly
            GROUP BY repo_id, branch, bucket_daily;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_branch_aggregate_daily;",
        ),
        RiskyRunSQL(
            """
            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_aggregate_hourly',
                start_offset => '2 hours',
                end_offset => NULL,
                schedule_interval => INTERVAL '1 hour'
            );
            """,
            reverse_sql="SELECT remove_continuous_aggregate_policy('ta_timeseries_aggregate_hourly');",
        ),
        RiskyRunSQL(
            """
            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_aggregate_daily',
                start_offset => '2 days',
                end_offset => NULL,
                schedule_interval => INTERVAL '1 day'
            );
            """,
            reverse_sql="SELECT remove_continuous_aggregate_policy('ta_timeseries_aggregate_daily');",
        ),
        RiskyRunSQL(
            """
            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_branch_aggregate_hourly',
                start_offset => '2 hours',
                end_offset => NULL,
                schedule_interval => INTERVAL '1 hour'
            );
            """,
            reverse_sql="SELECT remove_continuous_aggregate_policy('ta_timeseries_branch_aggregate_hourly');",
        ),
        RiskyRunSQL(
            """
            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_branch_aggregate_daily',
                start_offset => '2 days',
                end_offset => NULL,
                schedule_interval => INTERVAL '1 day'
            );
            """,
            reverse_sql="SELECT remove_continuous_aggregate_policy('ta_timeseries_branch_aggregate_daily');",
        ),
    ]
