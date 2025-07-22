from django.db import migrations

from shared.django_apps.migration_utils import RiskyRunSQL


class Migration(migrations.Migration):
    atomic = False
    dependencies = [
        ("ta_timeseries", "0021_testrun_ta_ts__repo_timestamp_idx_and_more"),
    ]

    operations = [
        RiskyRunSQL(
            """
            DO $$
            DECLARE
                mat_hypertable_name text;
            BEGIN
                SELECT materialization_hypertable_name
                INTO mat_hypertable_name
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name = 'ta_timeseries_branch_aggregate_hourly';

                IF mat_hypertable_name IS NOT NULL THEN
                    EXECUTE 'DROP INDEX IF EXISTS _timescaledb_internal.' || quote_ident(mat_hypertable_name || '_repo_id_bucket_hourly_idx');
                    EXECUTE 'DROP INDEX IF EXISTS _timescaledb_internal.' || quote_ident(mat_hypertable_name || '_branch_bucket_hourly_idx');
                END IF;
            END $$;
            """,
            reverse_sql="""
            DO $$
            DECLARE
                mat_hypertable_name text;
            BEGIN
                SELECT materialization_hypertable_name
                INTO mat_hypertable_name
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name = 'ta_timeseries_branch_aggregate_hourly';

                IF mat_hypertable_name IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(mat_hypertable_name || '_repo_id_bucket_hourly_idx') || 
                            ' ON _timescaledb_internal.' || quote_ident(mat_hypertable_name) || ' (repo_id, bucket_hourly)';
                    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(mat_hypertable_name || '_branch_bucket_hourly_idx') || 
                            ' ON _timescaledb_internal.' || quote_ident(mat_hypertable_name) || ' (branch, bucket_hourly)';
                END IF;
            END $$;
            """,
        ),
        RiskyRunSQL(
            """
            DO $$
            DECLARE
                mat_hypertable_name text;
            BEGIN
                SELECT materialization_hypertable_name
                INTO mat_hypertable_name
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name = 'ta_timeseries_branch_aggregate_daily';

                IF mat_hypertable_name IS NOT NULL THEN
                    EXECUTE 'DROP INDEX IF EXISTS _timescaledb_internal.' || quote_ident(mat_hypertable_name || '_repo_id_bucket_daily_idx');
                    EXECUTE 'DROP INDEX IF EXISTS _timescaledb_internal.' || quote_ident(mat_hypertable_name || '_branch_bucket_daily_idx');
                END IF;
            END $$;
            """,
            reverse_sql="""
            DO $$
            DECLARE
                mat_hypertable_name text;
            BEGIN
                SELECT materialization_hypertable_name
                INTO mat_hypertable_name
                FROM timescaledb_information.continuous_aggregates
                WHERE view_name = 'ta_timeseries_branch_aggregate_daily';

                IF mat_hypertable_name IS NOT NULL THEN
                    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(mat_hypertable_name || '_repo_id_bucket_daily_idx') || 
                            ' ON _timescaledb_internal.' || quote_ident(mat_hypertable_name) || ' (repo_id, bucket_daily)';
                    EXECUTE 'CREATE INDEX IF NOT EXISTS ' || quote_ident(mat_hypertable_name || '_branch_bucket_daily_idx') || 
                            ' ON _timescaledb_internal.' || quote_ident(mat_hypertable_name) || ' (branch, bucket_daily)';
                END IF;
            END $$;
            """,
        ),
    ]
