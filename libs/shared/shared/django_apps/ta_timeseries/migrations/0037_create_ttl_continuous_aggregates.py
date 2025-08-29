# Create new TTL-enabled continuous aggregates with _ttl suffix

from django.db import migrations

from shared.django_apps.migration_utils import ts_create_continuous_aggregate


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0036_backfill_testrun_ttl"),
    ]

    operations = [
        # Create TTL-enabled repo summary continuous aggregates
        ts_create_continuous_aggregate(
            "ta_timeseries_aggregate_hourly_ttl",
            """
            SELECT
                repo_id,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,

                SUM(CASE WHEN duration_seconds IS NOT NULL THEN duration_seconds ELSE 0 END) as total_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                MAX(ttl) as ttl
            FROM ta_timeseries_testrun
            GROUP BY repo_id, bucket_hourly
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),
        ts_create_continuous_aggregate(
            "ta_timeseries_aggregate_daily_ttl",
            """
            SELECT
                repo_id,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,
                SUM(total_duration_seconds) as total_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count,
                MAX(ttl) as ttl
            FROM ta_timeseries_aggregate_hourly_ttl
            GROUP BY repo_id, bucket_daily
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),

        # Create TTL-enabled branch repo summary continuous aggregates
        ts_create_continuous_aggregate(
            "ta_timeseries_branch_aggregate_hourly_ttl",
            """
            SELECT
                repo_id,
                branch,
                time_bucket(interval '1 hour', timestamp) as bucket_hourly,

                SUM(CASE WHEN duration_seconds IS NOT NULL THEN duration_seconds ELSE 0 END) as total_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                MAX(ttl) as ttl
            FROM ta_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, bucket_hourly
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),
        ts_create_continuous_aggregate(
            "ta_timeseries_branch_aggregate_daily_ttl",
            """
            SELECT
                repo_id,
                branch,
                time_bucket(interval '1 day', bucket_hourly) as bucket_daily,

                SUM(total_duration_seconds) as total_duration_seconds,
                SUM(pass_count) AS pass_count,
                SUM(fail_count) AS fail_count,
                SUM(skip_count) AS skip_count,
                SUM(flaky_fail_count) AS flaky_fail_count,
                MAX(ttl) as ttl
            FROM ta_timeseries_branch_aggregate_hourly_ttl
            GROUP BY repo_id, branch, bucket_daily
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),

        # Create TTL-enabled test-specific continuous aggregates
        ts_create_continuous_aggregate(
            "ta_timeseries_test_aggregate_hourly_ttl",
            """
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
                array_merge_dedup_agg(flags) as flags,
                MAX(ttl) as ttl
            FROM ta_timeseries_testrun
            GROUP BY repo_id, testsuite, classname, name, bucket_hourly
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),
        ts_create_continuous_aggregate(
            "ta_timeseries_test_aggregate_daily_ttl",
            """
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
                array_merge_dedup_agg(flags) as flags,
                MAX(ttl) as ttl
            FROM ta_timeseries_test_aggregate_hourly_ttl
            GROUP BY repo_id, testsuite, classname, name, bucket_daily
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),

        # Create TTL-enabled branch test-specific continuous aggregates
        ts_create_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_hourly_ttl",
            """
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
                array_merge_dedup_agg(flags) as flags,
                MAX(ttl) as ttl
            FROM ta_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, testsuite, classname, name, bucket_hourly
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),
        ts_create_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_daily_ttl",
            """
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
                array_merge_dedup_agg(flags) as flags,
                MAX(ttl) as ttl
            FROM ta_timeseries_branch_test_aggregate_hourly_ttl
            GROUP BY repo_id, branch, testsuite, classname, name, bucket_daily
            """,
            materialized_only=False,
            create_group_indexes=False,
            with_no_data=True,
        ),
    ]
