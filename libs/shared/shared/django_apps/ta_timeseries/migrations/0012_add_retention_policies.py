from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0011_auto_20250610_2135"),
    ]

    operations = [
        RiskyRunSQL(
            "SELECT add_retention_policy('ta_timeseries_testrun', INTERVAL '60 days');"
        ),
        RiskyRunSQL(
            "SELECT add_retention_policy('ta_timeseries_testrun_summary_1day', INTERVAL '60 days');"
        ),
        RiskyRunSQL(
            "SELECT add_retention_policy('ta_timeseries_testrun_branch_summary_1day', INTERVAL '60 days');"
        ),
    ]
