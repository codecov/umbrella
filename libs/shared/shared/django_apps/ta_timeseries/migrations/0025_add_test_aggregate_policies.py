from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0024_create_test_aggregate_daily_cas"),
    ]

    operations = [
        migrations.RunSQL(
            """
            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_test_aggregate_daily',
                start_offset => '2 days',
                end_offset => NULL,
                schedule_interval => INTERVAL '1 days'
            );
            """,
            reverse_sql="SELECT remove_continuous_aggregate_policy('ta_timeseries_test_aggregate_daily');",
        ),
        migrations.RunSQL(
            """
            SELECT add_continuous_aggregate_policy(
                'ta_timeseries_branch_test_aggregate_daily',
                start_offset => '2 days',
                end_offset => NULL,
                schedule_interval => INTERVAL '1 days'
            );
            """,
            reverse_sql="SELECT remove_continuous_aggregate_policy('ta_timeseries_branch_test_aggregate_daily');",
        ),
    ]
