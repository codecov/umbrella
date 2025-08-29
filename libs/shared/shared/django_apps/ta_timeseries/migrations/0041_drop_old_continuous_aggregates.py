# Drop old continuous aggregates (they're now renamed to _old)

from django.db import migrations

from shared.django_apps.migration_utils import ts_drop_materialized_view


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0040_rename_ttl_continuous_aggregates"),
    ]

    operations = [
        # Drop daily aggregates first
        ts_drop_materialized_view("ta_timeseries_aggregate_daily_old"),
        ts_drop_materialized_view("ta_timeseries_branch_aggregate_daily_old"),
        ts_drop_materialized_view("ta_timeseries_test_aggregate_daily_old"),
        ts_drop_materialized_view("ta_timeseries_branch_test_aggregate_daily_old"),
        # Then drop hourly aggregates
        ts_drop_materialized_view("ta_timeseries_aggregate_hourly_old"),
        ts_drop_materialized_view("ta_timeseries_branch_aggregate_hourly_old"),
        ts_drop_materialized_view("ta_timeseries_test_aggregate_hourly_old"),
        ts_drop_materialized_view("ta_timeseries_branch_test_aggregate_hourly_old"),
    ]
