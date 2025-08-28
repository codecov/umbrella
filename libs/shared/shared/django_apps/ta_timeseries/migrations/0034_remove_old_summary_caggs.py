from django.db import migrations

from shared.django_apps.migration_utils import ts_drop_materialized_view


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0033_testrun_ta_ts__repo_test_time_idx"),
    ]

    operations = [
        migrations.DeleteModel(
            name="TestrunSummary",
        ),
        migrations.DeleteModel(
            name="TestrunBranchSummary",
        ),
        ts_drop_materialized_view("ta_timeseries_testrun_summary_1day"),
        ts_drop_materialized_view("ta_timeseries_testrun_branch_summary_1day"),
    ]
