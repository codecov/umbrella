from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("prevent_timeseries", "0004_add_ttl_to_testrun"),
    ]

    atomic = False

    operations = [
        RiskyRunSQL(
            """
            UPDATE prevent_timeseries_testrun
            SET ttl = timestamp + INTERVAL '60 days'
            WHERE ttl IS NULL;
            """
        )
    ]
