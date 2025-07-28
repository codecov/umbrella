from django.db import migrations


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0023_create_cagg_indexes"),
    ]

    operations = [
        migrations.RunSQL(
            """
            CREATE MATERIALIZED VIEW ta_timeseries_test_aggregate_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.create_group_indexes = false) AS
            SELECT
                repo_id,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 days', timestamp) as timestamp_bin,

                min(computed_name) as computed_name,
                array_agg(DISTINCT commit_sha) FILTER (WHERE outcome = 'failure' OR outcome = 'flaky_fail') as failing_commits,
                last(duration_seconds, timestamp) as last_duration_seconds,
                avg(duration_seconds) as avg_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                MAX(timestamp) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM ta_timeseries_testrun
            GROUP BY repo_id, testsuite, classname, name, timestamp_bin;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_test_aggregate_daily;",
        ),
        migrations.RunSQL(
            """
            CREATE MATERIALIZED VIEW ta_timeseries_branch_test_aggregate_daily
            WITH (timescaledb.continuous, timescaledb.materialized_only = false, timescaledb.create_group_indexes = false) AS
            SELECT
                repo_id,
                branch,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 days', timestamp) as timestamp_bin,

                min(computed_name) as computed_name,
                array_agg(DISTINCT commit_sha) FILTER (WHERE outcome = 'failure' OR outcome = 'flaky_fail') as failing_commits,
                last(duration_seconds, timestamp) as last_duration_seconds,
                avg(duration_seconds) as avg_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                MAX(timestamp) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM ta_timeseries_testrun
            WHERE branch IN ('main', 'master', 'develop')
            GROUP BY repo_id, branch, testsuite, classname, name, timestamp_bin;
            """,
            reverse_sql="DROP MATERIALIZED VIEW ta_timeseries_branch_test_aggregate_daily;",
        ),
    ]
