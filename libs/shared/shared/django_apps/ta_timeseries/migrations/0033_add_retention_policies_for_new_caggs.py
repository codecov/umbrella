from django.db import migrations

from shared.django_apps.migration_utils import ts_add_retention_policy


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0032_testrun_ta_ts__repo_test_time_idx"),
    ]

    operations = [
        ts_add_retention_policy("ta_timeseries_test_aggregate_hourly", "3 days"),
        ts_add_retention_policy("ta_timeseries_branch_test_aggregate_hourly", "3 days"),
        ts_add_retention_policy("ta_timeseries_test_aggregate_daily", "60 days"),
        ts_add_retention_policy("ta_timeseries_branch_test_aggregate_daily", "60 days"),
    ]
