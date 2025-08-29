# Create TTL retention policies for testrun hypertable and daily CAs

from django.db import migrations

from shared.django_apps.migration_utils import (
    create_ca_ttl_retention,
    create_table_ttl_retention,
)


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0041_drop_old_continuous_aggregates"),
    ]

    operations = [
        create_table_ttl_retention(
            table_name="ta_timeseries_testrun",
            schedule_interval="1 hour",
        ),
        create_ca_ttl_retention(
            ca_name="ta_timeseries_aggregate_daily",
            schedule_interval="1 hour",
        ),
        create_ca_ttl_retention(
            ca_name="ta_timeseries_branch_aggregate_daily",
            schedule_interval="1 hour",
        ),
        create_ca_ttl_retention(
            ca_name="ta_timeseries_test_aggregate_daily",
            schedule_interval="1 hour",
        ),
        create_ca_ttl_retention(
            ca_name="ta_timeseries_branch_test_aggregate_daily",
            schedule_interval="1 hour",
        ),
    ]
