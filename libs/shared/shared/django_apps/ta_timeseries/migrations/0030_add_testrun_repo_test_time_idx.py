from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0029_add_retention_policies_to_new_caggs"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX IF NOT EXISTS ta_ts__repo_test_time_idx "
                "ON ta_timeseries_testrun (repo_id, test_id, timestamp) "
                "WITH (timescaledb.transaction_per_chunk);"
            ),
            reverse_sql=("DROP INDEX IF EXISTS ta_ts__repo_test_time_idx;"),
        ),
    ]
