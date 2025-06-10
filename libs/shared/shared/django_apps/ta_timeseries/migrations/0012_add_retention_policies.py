from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0011_auto_20250610_2135"),
    ]

    operations = [
        migrations.RunSQL(
            "SELECT add_retention_policy('ta_timeseries_testrun', INTERVAL '60 days');"
        ),
        migrations.RunSQL(
            "SELECT add_retention_policy('ta_timeseries_testrun_summary_1day', INTERVAL '60 days');"
        ),
        migrations.RunSQL(
            "SELECT add_retention_policy('ta_timeseries_testrun_branch_summary_1day', INTERVAL '60 days');"
        ),
    ]
