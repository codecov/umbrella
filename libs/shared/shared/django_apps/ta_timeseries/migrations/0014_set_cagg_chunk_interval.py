from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0013_set_chunk_interval"),
    ]

    operations = [
        RiskyRunSQL(
            """
            DO $$
            DECLARE
                mat_hypertable_schema text;
                mat_hypertable_name text;
            BEGIN
                SELECT ca.materialization_hypertable_schema, ca.materialization_hypertable_name
                INTO mat_hypertable_schema, mat_hypertable_name
                FROM timescaledb_information.continuous_aggregates ca
                WHERE ca.view_name = 'ta_timeseries_testrun_summary_1day';

                IF mat_hypertable_name IS NOT NULL THEN
                    PERFORM set_chunk_time_interval(
                        format('%I.%I', mat_hypertable_schema, mat_hypertable_name),
                        INTERVAL '7 days'
                    );
                END IF;
            END $$;
            """,
            migrations.RunSQL.noop,
        ),
        RiskyRunSQL(
            """
            DO $$
            DECLARE
                mat_hypertable_schema text;
                mat_hypertable_name text;
            BEGIN
                SELECT ca.materialization_hypertable_schema, ca.materialization_hypertable_name
                INTO mat_hypertable_schema, mat_hypertable_name
                FROM timescaledb_information.continuous_aggregates ca
                WHERE ca.view_name = 'ta_timeseries_testrun_branch_summary_1day';

                IF mat_hypertable_name IS NOT NULL THEN
                    PERFORM set_chunk_time_interval(
                        format('%I.%I', mat_hypertable_schema, mat_hypertable_name),
                        INTERVAL '7 days'
                    );
                END IF;
            END $$;
            """,
            migrations.RunSQL.noop,
        ),
    ]
