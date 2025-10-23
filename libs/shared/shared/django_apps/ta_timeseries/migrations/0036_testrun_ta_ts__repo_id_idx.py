from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0035_alter_branch_test_aggregate_daily_policy"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX IF NOT EXISTS ta_ts__repo_id_idx "
                "ON ta_timeseries_testrun (repo_id) "
                "WITH (timescaledb.transaction_per_chunk);"
            ),
            reverse_sql=("DROP INDEX IF EXISTS ta_ts__repo_id_idx;"),
        ),
    ]
