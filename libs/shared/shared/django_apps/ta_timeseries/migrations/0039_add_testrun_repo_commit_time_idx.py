from django.db import migrations


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0038_add_compression"),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX IF NOT EXISTS ta_ts__repo_commit_time_idx "
                "ON ta_timeseries_testrun (repo_id, commit_sha, timestamp) "
                "WITH (timescaledb.transaction_per_chunk);"
            ),
            reverse_sql="DROP INDEX IF EXISTS ta_ts__repo_commit_time_idx;",
        ),
    ]