from django.db import migrations

from shared.django_apps.migration_utils import (
    ts_add_continuous_aggregate_policy,
    ts_create_continuous_aggregate,
)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("prevent_timeseries", "0001_initial"),
    ]

    operations = [
        ts_create_continuous_aggregate(
            "prevent_timeseries_aggregate_hourly",
            select_sql="""
            SELECT
                repo_id,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,
                SUM(CASE WHEN duration_seconds IS NOT NULL THEN duration_seconds ELSE 0 END) as total_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count
            FROM prevent_timeseries_testrun
            GROUP BY repo_id, bucket_hourly
            """,
            materialized_only=False,
        ),
        ts_create_continuous_aggregate(
            "prevent_timeseries_aggregate_daily",
            select_sql="""
            SELECT
                repo_id,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,
                SUM(total_duration_seconds) as total_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count
            FROM prevent_timeseries_aggregate_hourly
            GROUP BY repo_id, bucket_daily
            """,
            materialized_only=False,
        ),
        ts_create_continuous_aggregate(
            "prevent_timeseries_branch_aggregate_hourly",
            select_sql="""
            SELECT
                repo_id,
                branch,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,
                SUM(CASE WHEN duration_seconds IS NOT NULL THEN duration_seconds ELSE 0 END) as total_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count
            FROM prevent_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, bucket_hourly
            """,
            materialized_only=False,
        ),
        ts_create_continuous_aggregate(
            "prevent_timeseries_branch_aggregate_daily",
            select_sql="""
            SELECT
                repo_id,
                branch,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,
                SUM(total_duration_seconds) as total_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count
            FROM prevent_timeseries_branch_aggregate_hourly
            GROUP BY repo_id, branch, bucket_daily
            """,
            materialized_only=False,
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_aggregate_hourly",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_branch_aggregate_hourly",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_branch_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
    ]
