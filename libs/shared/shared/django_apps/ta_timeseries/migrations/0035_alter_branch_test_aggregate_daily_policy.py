from django.db import migrations

from shared.django_apps.migration_utils import ts_alter_continuous_aggregate_policy


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0034_remove_old_summary_caggs"),
    ]

    operations = [
        ts_alter_continuous_aggregate_policy(
            "ta_timeseries_branch_test_aggregate_daily",
            start_offset="2 days",
            end_offset=None,
            schedule_interval="2 hours",
            reverse_schedule_interval="1 day",
            risky=True,
        ),
    ]
