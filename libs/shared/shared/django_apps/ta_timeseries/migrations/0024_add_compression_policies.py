from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0023_create_cagg_indexes"),
    ]

    operations = [
        RiskyRunSQL(
            """
            ALTER TABLE ta_timeseries_testrun SET (
                timescaledb.enable_columnstore,
                timescaledb.segmentby = 'repo_id, branch',
                timescaledb.orderby = 'testsuite, classname, name, timestamp DESC'
            );
            """,
            reverse_sql="ALTER TABLE ta_timeseries_testrun SET (timescaledb.enable_columnstore = false);",
        ),
        RiskyRunSQL(
            """
            CALL add_columnstore_policy('ta_timeseries_testrun', INTERVAL '7 days');
            """,
            reverse_sql="CALL remove_columnstore_policy('ta_timeseries_testrun');",
        ),
    ]
