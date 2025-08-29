# Backfill TTL values for existing testruns

from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0035_add_testrun_ttl"),
    ]

    operations = [
        RiskyRunSQL(
            """
            UPDATE ta_timeseries_testrun
            SET ttl = timestamp + INTERVAL '60 days'
            WHERE ttl IS NULL;
            """,
            reverse_sql="""
            UPDATE ta_timeseries_testrun
            SET ttl = NULL
            WHERE ttl IS NOT NULL;
            """,
        ),
    ]
