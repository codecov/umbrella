from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0036_testrun_ta_ts__repo_id_idx"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX IF NOT EXISTS ta_ts__repo_id_outcome_time_idx "
                "ON ta_timeseries_testrun (repo_id, outcome, timestamp) "
                "WITH (timescaledb.transaction_per_chunk);"
            ),
            reverse_sql=("DROP INDEX IF EXISTS ta_ts__repo_id_outcome_time_idx;"),
        ),
    ]
