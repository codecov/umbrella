from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0038_add_compression"),
    ]

    operations = [
        migrations.RunSQL(
            sql="DROP INDEX IF EXISTS ta_ts__repo_id_idx;",
            reverse_sql=(
                "CREATE INDEX IF NOT EXISTS ta_ts__repo_id_idx "
                "ON ta_timeseries_testrun (repo_id) "
                "WITH (timescaledb.transaction_per_chunk);"
            ),
        ),
    ]