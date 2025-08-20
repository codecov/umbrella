from django.db import migrations

from shared.django_apps.migration_utils import (
    add_continuous_aggregate_policy,
    create_continuous_aggregate,
    create_index,
    drop_materialized_view,
    refresh_continuous_aggregate,
)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0030_add_testrun_repo_test_time_idx"),
    ]

    operations = [
        drop_materialized_view("ta_timeseries_test_aggregate_daily"),
        drop_materialized_view("ta_timeseries_branch_test_aggregate_daily"),
        drop_materialized_view("ta_timeseries_test_aggregate_hourly"),
        drop_materialized_view("ta_timeseries_branch_test_aggregate_hourly"),
        create_continuous_aggregate(
            "ta_timeseries_test_aggregate_hourly",
            select_sql="""
                SELECT
                    repo_id,
                    time_bucket(interval '1 hour', timestamp) AS bucket_hourly,
                    test_id,
                    min(computed_name) AS computed_name,
                    min(testsuite) AS testsuite,
                    array_agg(DISTINCT commit_sha) FILTER (WHERE outcome IN ('failure', 'flaky_fail')) AS failing_commits,
                    LAST(duration_seconds, timestamp) AS last_duration_seconds,
                    AVG(duration_seconds) AS avg_duration_seconds,
                    COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                    COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                    COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                    COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                    LAST(outcome, timestamp) AS last_outcome,
                    MAX(timestamp) AS updated_at,
                    array_merge_dedup_agg(flags) AS flags
                FROM ta_timeseries_testrun
                GROUP BY repo_id, test_id, bucket_hourly
            """,
        ),
        create_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_hourly",
            select_sql="""
                SELECT
                    repo_id,
                    branch,
                    time_bucket(interval '1 hour', timestamp) AS bucket_hourly,
                    test_id,
                    min(computed_name) AS computed_name,
                    min(testsuite) AS testsuite,
                    array_agg(DISTINCT commit_sha) FILTER (WHERE outcome IN ('failure', 'flaky_fail')) AS failing_commits,
                    LAST(duration_seconds, timestamp) AS last_duration_seconds,
                    AVG(duration_seconds) AS avg_duration_seconds,
                    COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                    COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                    COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                    COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                    LAST(outcome, timestamp) AS last_outcome,
                    MAX(timestamp) AS updated_at,
                    array_merge_dedup_agg(flags) AS flags
                FROM ta_timeseries_testrun
                WHERE branch IN ('main', 'master', 'develop')
                GROUP BY repo_id, branch, test_id, bucket_hourly
            """,
        ),
        create_index(
            "ta_ts_test_aggregate_hourly_repo_test_bucket_idx",
            "ta_timeseries_test_aggregate_hourly",
            ["repo_id", "test_id", "bucket_hourly DESC"],
        ),
        create_index(
            "ta_ts_branch_test_aggregate_hourly_repo_branch_test_bucket_idx",
            "ta_timeseries_branch_test_aggregate_hourly",
            ["repo_id", "branch", "test_id", "bucket_hourly DESC"],
        ),
        create_continuous_aggregate(
            "ta_timeseries_test_aggregate_daily",
            select_sql="""
                SELECT
                    repo_id,
                    time_bucket(interval '1 day', bucket_hourly) AS bucket_daily,
                    test_id,
                    min(computed_name) AS computed_name,
                    min(testsuite) AS testsuite,
                    array_merge_dedup_agg(failing_commits) AS failing_commits,
                    LAST(last_duration_seconds, updated_at) AS last_duration_seconds,
                    AVG(avg_duration_seconds) AS avg_duration_seconds,
                    SUM(pass_count) AS pass_count,
                    SUM(fail_count) AS fail_count,
                    SUM(skip_count) AS skip_count,
                    SUM(flaky_fail_count) AS flaky_fail_count,
                    LAST(last_outcome, updated_at) AS last_outcome,
                    MAX(updated_at) AS updated_at,
                    array_merge_dedup_agg(flags) AS flags
                FROM ta_timeseries_test_aggregate_hourly
                GROUP BY repo_id, test_id, bucket_daily
            """,
        ),
        create_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_daily",
            select_sql="""
                SELECT
                    repo_id,
                    branch,
                    time_bucket(interval '1 day', bucket_hourly) AS bucket_daily,
                    test_id,
                    min(computed_name) AS computed_name,
                    min(testsuite) AS testsuite,
                    array_merge_dedup_agg(failing_commits) AS failing_commits,
                    LAST(last_duration_seconds, updated_at) AS last_duration_seconds,
                    AVG(avg_duration_seconds) AS avg_duration_seconds,
                    SUM(pass_count) AS pass_count,
                    SUM(fail_count) AS fail_count,
                    SUM(skip_count) AS skip_count,
                    SUM(flaky_fail_count) AS flaky_fail_count,
                    LAST(last_outcome, updated_at) AS last_outcome,
                    MAX(updated_at) AS updated_at,
                    array_merge_dedup_agg(flags) AS flags
                FROM ta_timeseries_branch_test_aggregate_hourly
                GROUP BY repo_id, branch, test_id, bucket_daily
            """,
        ),
        create_index(
            "ta_ts_test_aggregate_daily_repo_id_bucket_idx",
            "ta_timeseries_test_aggregate_daily",
            ["repo_id", "bucket_daily DESC"],
        ),
        create_index(
            "ta_ts_branch_test_aggregate_daily_repo_branch_bucket_idx",
            "ta_timeseries_branch_test_aggregate_daily",
            ["repo_id", "branch", "bucket_daily DESC"],
        ),
        refresh_continuous_aggregate(
            "ta_timeseries_test_aggregate_hourly",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
        ),
        refresh_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_hourly",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
        ),
        refresh_continuous_aggregate(
            "ta_timeseries_test_aggregate_daily",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
        ),
        refresh_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_daily",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
        ),
        add_continuous_aggregate_policy(
            "ta_timeseries_test_aggregate_hourly",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        add_continuous_aggregate_policy(
            "ta_timeseries_branch_test_aggregate_hourly",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        add_continuous_aggregate_policy(
            "ta_timeseries_test_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        add_continuous_aggregate_policy(
            "ta_timeseries_branch_test_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
    ]
