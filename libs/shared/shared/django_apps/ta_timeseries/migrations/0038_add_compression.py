from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0037_testrun_ta_ts_repo_id_outcome_time_idx"),
    ]

    operations = [
        RiskyRunSQL(
            """
            ALTER TABLE ta_timeseries_testrun SET (
                timescaledb.compress,
                timescaledb.compress_segmentby = 'repo_id',
                timescaledb.compress_orderby = 'timestamp DESC'
            );
            """,
            reverse_sql="ALTER TABLE ta_timeseries_testrun SET (timescaledb.compress = false);",
        ),
        RiskyRunSQL(
            "SELECT add_compression_policy('ta_timeseries_testrun', INTERVAL '7 days');",
            reverse_sql="SELECT remove_compression_policy('ta_timeseries_testrun');",
        ),
    ]
