from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0012_add_retention_policies"),
    ]

    operations = [
        RiskyRunSQL(
            "SELECT set_chunk_time_interval('ta_timeseries_testrun', INTERVAL '6 hours');"
        ),
    ]
