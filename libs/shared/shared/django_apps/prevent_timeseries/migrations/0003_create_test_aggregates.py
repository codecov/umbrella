from django.db import migrations

from shared.django_apps.migration_utils import (
    ts_add_continuous_aggregate_policy,
    ts_create_continuous_aggregate,
    ts_create_index,
)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("prevent_timeseries", "0002_create_repo_aggregates"),
    ]

    operations = [
        ts_create_continuous_aggregate(
            "prevent_timeseries_test_aggregate_hourly",
            select_sql="""
            SELECT
                repo_id,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,

                min(computed_name) as computed_name,
                array_agg(DISTINCT commit_sha) FILTER (WHERE outcome = 'failure' OR outcome = 'flaky_fail') as failing_commits,
                last(duration_seconds, timestamp) as last_duration_seconds,
                avg(duration_seconds) as avg_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                last(outcome, timestamp) as last_outcome,
                MAX(timestamp) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM prevent_timeseries_testrun
            GROUP BY repo_id, testsuite, classname, name, bucket_hourly
            """,
            materialized_only=False,
        ),
        ts_create_continuous_aggregate(
            "prevent_timeseries_test_aggregate_daily",
            select_sql="""
            SELECT
                repo_id,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,

                min(computed_name) as computed_name,
                array_merge_dedup_agg(failing_commits) as failing_commits,
                last(last_duration_seconds, updated_at) as last_duration_seconds,
                avg(avg_duration_seconds) as avg_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count,
                last(last_outcome, updated_at) as last_outcome,
                MAX(updated_at) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM prevent_timeseries_test_aggregate_hourly
            GROUP BY repo_id, testsuite, classname, name, bucket_daily
            """,
            materialized_only=False,
        ),
        ts_create_continuous_aggregate(
            "prevent_timeseries_branch_test_aggregate_hourly",
            select_sql="""
            SELECT
                repo_id,
                branch,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,

                min(computed_name) as computed_name,
                array_agg(DISTINCT commit_sha) FILTER (WHERE outcome = 'failure' OR outcome = 'flaky_fail') as failing_commits,
                last(duration_seconds, timestamp) as last_duration_seconds,
                avg(duration_seconds) as avg_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                last(outcome, timestamp) as last_outcome,
                MAX(timestamp) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM prevent_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, testsuite, classname, name, bucket_hourly
            """,
            materialized_only=False,
        ),
        ts_create_continuous_aggregate(
            "prevent_timeseries_branch_test_aggregate_daily",
            select_sql="""
            SELECT
                repo_id,
                branch,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,

                min(computed_name) as computed_name,
                array_merge_dedup_agg(failing_commits) as failing_commits,
                last(last_duration_seconds, updated_at) as last_duration_seconds,
                avg(avg_duration_seconds) as avg_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count,
                last(last_outcome, updated_at) as last_outcome,
                MAX(updated_at) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM prevent_timeseries_branch_test_aggregate_hourly
            GROUP BY repo_id, branch, testsuite, classname, name, bucket_daily
            """,
            materialized_only=False,
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_test_aggregate_hourly",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_test_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_branch_test_aggregate_hourly",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "prevent_timeseries_branch_test_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        ts_create_index(
            "prevent_ts_test_aggregate_hourly_repo_id_bucket_idx",
            "prevent_timeseries_test_aggregate_hourly",
            ["repo_id", "bucket_hourly DESC"],
        ),
        ts_create_index(
            "prevent_ts_test_aggregate_daily_repo_id_bucket_idx",
            "prevent_timeseries_test_aggregate_daily",
            ["repo_id", "bucket_daily DESC"],
        ),
        ts_create_index(
            "prevent_ts_branch_test_aggregate_hourly_repo_branch_bucket_idx",
            "prevent_timeseries_branch_test_aggregate_hourly",
            ["repo_id", "branch", "bucket_hourly DESC"],
        ),
        ts_create_index(
            "prevent_ts_branch_test_aggregate_daily_repo_branch_bucket_idx",
            "prevent_timeseries_branch_test_aggregate_daily",
            ["repo_id", "branch", "bucket_daily DESC"],
        ),
    ]
