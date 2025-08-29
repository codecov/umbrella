# Backfill TTL continuous aggregates with data from existing CAs

from django.db import migrations

from shared.django_apps.migration_utils import ts_refresh_continuous_aggregate


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0037_create_ttl_continuous_aggregates"),
    ]

    operations = [
        # Refresh all the new TTL CAs to populate them with data
        ts_refresh_continuous_aggregate(
            "ta_timeseries_aggregate_hourly_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_aggregate_daily_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_branch_aggregate_hourly_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_branch_aggregate_daily_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_test_aggregate_hourly_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_test_aggregate_daily_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_hourly_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
        ts_refresh_continuous_aggregate(
            "ta_timeseries_branch_test_aggregate_daily_ttl",
            "NOW() - INTERVAL '60 days'",
            None,
            risky=True,
            force=True,
        ),
    ]
