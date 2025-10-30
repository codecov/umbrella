from django.db import migrations, models


class Migration(migrations.Migration):
    atomic = False

    dependencies = [
        ("ta_timeseries", "0036_testrun_ta_ts__repo_id_idx"),
    ]

    operations = [
        # Sync state for repo_id index created in 0036
        migrations.SeparateDatabaseAndState(
            database_operations=[],  # Index already created in 0036
            state_operations=[
                migrations.AddIndex(
                    model_name="testrun",
                    index=models.Index(
                        fields=["repo_id"],
                        name="ta_ts__repo_id_idx",
                    ),
                ),
            ],
        ),
        # Create repo_id + outcome + timestamp index
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "CREATE INDEX IF NOT EXISTS ta_ts_repo_id_outcome_time_idx "
                        "ON ta_timeseries_testrun (repo_id, outcome, timestamp) "
                        "WITH (timescaledb.transaction_per_chunk);"
                    ),
                    reverse_sql=(
                        "DROP INDEX IF EXISTS ta_ts_repo_id_outcome_time_idx;"
                    ),
                ),
            ],
            state_operations=[
                migrations.AddIndex(
                    model_name="testrun",
                    index=models.Index(
                        fields=["repo_id", "outcome", "timestamp"],
                        name="ta_ts_repo_id_outcome_time_idx",
                    ),
                ),
            ],
        ),
    ]
