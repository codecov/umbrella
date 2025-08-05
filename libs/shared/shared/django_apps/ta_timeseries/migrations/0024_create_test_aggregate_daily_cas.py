from django.db import migrations


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0023_create_cagg_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS
            ta_timeseries_test_aggregate_hourly
            WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.create_group_indexes = false) AS
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
            FROM ta_timeseries_testrun
            GROUP BY repo_id, testsuite, classname, name, bucket_hourly
            WITH NO DATA;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_test_aggregate_hourly;",
        ),
        migrations.RunSQL(
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS
            ta_timeseries_test_aggregate_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.create_group_indexes = false) AS
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
            FROM ta_timeseries_test_aggregate_hourly
            GROUP BY repo_id, testsuite, classname, name, bucket_daily
            WITH NO DATA;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_test_aggregate_daily;",
        ),
        migrations.RunSQL(
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS
            ta_timeseries_branch_test_aggregate_hourly
            WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.create_group_indexes = false) AS
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
            FROM ta_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, testsuite, classname, name, bucket_hourly
            WITH NO DATA;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_branch_test_aggregate_hourly;",
        ),
        migrations.RunSQL(
            """
            CREATE MATERIALIZED VIEW IF NOT EXISTS
            ta_timeseries_branch_test_aggregate_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.create_group_indexes = false) AS
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
            FROM ta_timeseries_branch_test_aggregate_hourly
            GROUP BY repo_id, branch, testsuite, classname, name, bucket_daily
            WITH NO DATA;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_branch_test_aggregate_daily;",
        ),
    ]
