from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0029_auto_20241231_0000"),
    ]

    operations = [
        migrations.DeleteModel(
            name="TestrunSummary",
        ),
        migrations.DeleteModel(
            name="TestrunBranchSummary",
        ),
        RiskyRunSQL(
            """
            DROP MATERIALIZED VIEW ta_timeseries_testrun_summary_1day;
            """,
            reverse_sql="""
            CREATE MATERIALIZED VIEW ta_timeseries_testrun_summary_1day
            WITH (timescaledb.continuous) AS
            SELECT
                repo_id,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 days', timestamp) as timestamp_bin,

                min(computed_name) as computed_name,
                COUNT(DISTINCT CASE WHEN outcome = 'failure' OR outcome = 'flaky_fail' THEN commit_sha ELSE NULL END) AS failing_commits,
                last(duration_seconds, timestamp) as last_duration_seconds,
                avg(duration_seconds) as avg_duration_seconds,
                COUNT(*) FILTER (WHERE outcome = 'pass') AS pass_count,
                COUNT(*) FILTER (WHERE outcome = 'failure') AS fail_count,
                COUNT(*) FILTER (WHERE outcome = 'skip') AS skip_count,
                COUNT(*) FILTER (WHERE outcome = 'flaky_fail') AS flaky_fail_count,
                MAX(timestamp) AS updated_at,
                array_merge_dedup_agg(flags) as flags
            FROM ta_timeseries_testrun
            GROUP BY
                repo_id, testsuite, classname, name, timestamp_bin;

            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_testrun_summary_1day',
                start_offset => '7 days',
                end_offset => '1 second',
                schedule_interval => INTERVAL '1 days'
            );

            SELECT add_retention_policy('ta_timeseries_testrun_summary_1day', INTERVAL '60 days');
            """,
        ),
        RiskyRunSQL(
            """
            DROP MATERIALIZED VIEW ta_timeseries_testrun_branch_summary_1day;
            """,
            reverse_sql="""
            CREATE MATERIALIZED VIEW ta_timeseries_testrun_branch_summary_1day
            WITH (timescaledb.continuous) AS
            SELECT
                repo_id,
                branch,
                testsuite,
                classname,
                name,
                time_bucket(interval '1 days', timestamp) as timestamp_bin,

                min(computed_name) as computed_name,
                COUNT(DISTINCT CASE WHEN outcome = 'failure' OR outcome = 'flaky_fail' THEN commit_sha ELSE NULL END) AS failing_commits,
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
            GROUP BY
                repo_id, branch, testsuite, classname, name, timestamp_bin;

            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_testrun_branch_summary_1day',
                start_offset => '7 days',
                end_offset => '1 second',
                schedule_interval => INTERVAL '1 days'
            );

            SELECT add_retention_policy('ta_timeseries_testrun_branch_summary_1day', INTERVAL '60 days');
            """,
        ),
    ]
