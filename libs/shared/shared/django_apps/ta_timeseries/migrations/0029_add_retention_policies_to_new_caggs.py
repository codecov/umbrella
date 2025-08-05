from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0028_branchtestaggregatedaily_testaggregatedaily"),
    ]

    operations = [
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_aggregate_hourly', INTERVAL '3 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_aggregate_hourly');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_branch_aggregate_hourly', INTERVAL '3 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_branch_aggregate_hourly');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_test_aggregate_hourly', INTERVAL '3 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_test_aggregate_hourly');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_branch_test_aggregate_hourly', INTERVAL '3 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_branch_test_aggregate_hourly');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_aggregate_daily', INTERVAL '60 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_aggregate_daily');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_branch_aggregate_daily', INTERVAL '60 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_branch_aggregate_daily');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_test_aggregate_daily', INTERVAL '60 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_test_aggregate_daily');
            """,
        ),
        migrations.RunSQL(
            """
            SELECT add_retention_policy('ta_timeseries_branch_test_aggregate_daily', INTERVAL '60 days');
            """,
            reverse_sql="""
            SELECT remove_retention_policy('ta_timeseries_branch_test_aggregate_daily');
            """,
        ),
    ]
