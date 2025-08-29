# Add 3-day retention policies for hourly continuous aggregates

from django.db import migrations

from shared.django_apps.migration_utils import ts_add_retention_policy


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0042_create_ttl_retention_policies"),
    ]

    operations = [
        ts_add_retention_policy(
            "ta_timeseries_aggregate_hourly",
            "3 days",
        ),
        ts_add_retention_policy(
            "ta_timeseries_branch_aggregate_hourly",
            "3 days",
        ),
        ts_add_retention_policy(
            "ta_timeseries_test_aggregate_hourly",
            "3 days",
        ),
        ts_add_retention_policy(
            "ta_timeseries_branch_test_aggregate_hourly",
            "3 days",
        ),
    ]
