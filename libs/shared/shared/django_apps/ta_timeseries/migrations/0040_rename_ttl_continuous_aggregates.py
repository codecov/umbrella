# Rename TTL continuous aggregates to original names (atomic swap)

from django.db import migrations

from shared.django_apps.migration_utils import ts_rename_materialized_view


class Migration(migrations.Migration):
    atomic = True
    dependencies = [
        ("ta_timeseries", "0039_add_ttl_continuous_aggregate_policies"),
    ]

    operations = [
        # Old -> _old
        ts_rename_materialized_view(
            "ta_timeseries_aggregate_hourly",
            "ta_timeseries_aggregate_hourly_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_aggregate_daily",
            "ta_timeseries_aggregate_daily_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_aggregate_hourly",
            "ta_timeseries_branch_aggregate_hourly_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_aggregate_daily",
            "ta_timeseries_branch_aggregate_daily_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_test_aggregate_hourly",
            "ta_timeseries_test_aggregate_hourly_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_test_aggregate_daily",
            "ta_timeseries_test_aggregate_daily_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_test_aggregate_hourly",
            "ta_timeseries_branch_test_aggregate_hourly_old"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_test_aggregate_daily",
            "ta_timeseries_branch_test_aggregate_daily_old"
        ),

        # TTL -> final
        ts_rename_materialized_view(
            "ta_timeseries_aggregate_hourly_ttl",
            "ta_timeseries_aggregate_hourly"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_aggregate_daily_ttl",
            "ta_timeseries_aggregate_daily"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_aggregate_hourly_ttl",
            "ta_timeseries_branch_aggregate_hourly"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_aggregate_daily_ttl",
            "ta_timeseries_branch_aggregate_daily"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_test_aggregate_hourly_ttl",
            "ta_timeseries_test_aggregate_hourly"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_test_aggregate_daily_ttl",
            "ta_timeseries_test_aggregate_daily"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_test_aggregate_hourly_ttl",
            "ta_timeseries_branch_test_aggregate_hourly"
        ),
        ts_rename_materialized_view(
            "ta_timeseries_branch_test_aggregate_daily_ttl",
            "ta_timeseries_branch_test_aggregate_daily"
        ),
    ]


