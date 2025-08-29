# Add continuous aggregate policies to TTL CAs before swapping

from django.db import migrations

from shared.django_apps.migration_utils import ts_add_continuous_aggregate_policy


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0038_backfill_ttl_continuous_aggregates"),
    ]

    operations = [
        # Add continuous aggregate policies to TTL CAs for initial refresh
        # These will be removed when we swap to the final names
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_aggregate_hourly_ttl",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_aggregate_daily_ttl",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_branch_aggregate_hourly_ttl",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_branch_aggregate_daily_ttl",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_test_aggregate_hourly_ttl",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_test_aggregate_daily_ttl",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_branch_test_aggregate_hourly_ttl",
            start_offset="2 hours",
            end_offset=None,
            schedule_interval="1 hour",
        ),
        ts_add_continuous_aggregate_policy(
            "ta_timeseries_branch_test_aggregate_daily_ttl",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="1 day",
        ),
    ]
