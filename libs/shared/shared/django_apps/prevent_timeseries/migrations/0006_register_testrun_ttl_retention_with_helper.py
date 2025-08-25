from django.db import migrations

from shared.django_apps.migration_utils import (
    create_table_ttl_retention,
    ts_remove_retention_policy,
)


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("prevent_timeseries", "0005_backfill_ttl_60_days"),
    ]

    operations = [
        ts_remove_retention_policy(
            "prevent_timeseries_testrun", reverse_drop_after="60 days"
        ),
        create_table_ttl_retention(table_name="prevent_timeseries_testrun"),
    ]
