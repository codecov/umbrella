from django.conf import settings
from django.db import migrations, models

"""
These classes can be used to skip altering DB state while maintaining the state of migrations.
To use them you should manually replace the migration step in the migration file with its
corresponding "Risky" migration step.
Not all migration steps (such as AddField) are represented here because they cannot safely
exist in code while not being applied in the DB.
"""


class RiskyAddField(migrations.AddField):
    """
    Consult https://www.notion.so/sentry/Database-Tips-and-Tricks-76df725ded264b2a8154c960d7ef3869?pvs=4
    for how to risky add field.
    """

    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyAlterField(migrations.AlterField):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyRemoveField(migrations.RemoveField):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyAlterUniqueTogether(migrations.AlterUniqueTogether):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyAlterIndexTogether(migrations.AlterIndexTogether):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyAddIndex(migrations.AddIndex):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyRemoveIndex(migrations.RemoveIndex):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyAddConstraint(migrations.AddConstraint):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyRemoveConstraint(migrations.RemoveConstraint):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyRunSQL(migrations.RunSQL):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


class RiskyRunPython(migrations.RunPython):
    def database_forwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_forwards(app_label, schema_editor, from_state, to_state)

    def database_backwards(self, app_label, schema_editor, from_state, to_state):
        if settings.SKIP_RISKY_MIGRATION_STEPS:
            return

        super().database_backwards(app_label, schema_editor, from_state, to_state)


def ts_drop_materialized_view(
    view_name: str, if_exists: bool = True
) -> migrations.RunSQL:
    exists_clause = " IF EXISTS" if if_exists else ""
    forward_sql = f"DROP MATERIALIZED VIEW{exists_clause} {view_name};"
    return migrations.RunSQL(forward_sql)


def ts_create_continuous_aggregate(
    view_name: str,
    select_sql: str,
    *,
    materialized_only: bool = False,
    create_group_indexes: bool = False,
    with_no_data: bool = True,
) -> migrations.RunSQL:
    options = [
        "timescaledb.continuous",
        f"timescaledb.materialized_only = {'true' if materialized_only else 'false'}",
        f"timescaledb.create_group_indexes = {'true' if create_group_indexes else 'false'}",
    ]

    select_sql_clean = select_sql.strip()
    if select_sql_clean.endswith(";"):
        select_sql_clean = select_sql_clean[:-1]

    with_no_data_sql = "\nWITH NO DATA" if with_no_data else ""

    forward_sql = f"""
        CREATE MATERIALIZED VIEW {view_name}
        WITH (
            {",\n    ".join(options)}
        ) AS
        {select_sql_clean}{with_no_data_sql};
    """.strip()

    reverse_sql = f"DROP MATERIALIZED VIEW IF EXISTS {view_name};"
    return migrations.RunSQL(forward_sql, reverse_sql=reverse_sql)


def ts_create_index(
    index_name: str,
    table_name: str,
    columns: list[str],
) -> migrations.RunSQL:
    """
    Create an index on a table. Not suitable for hypertables managed by Django, because
    the state will diverge: the index will be created in the DB, but Django will not
    know about it. You should use ts_create_index_managed_hypertable instead.
    """
    column_exprs: list[str] = [str(col) for col in columns]

    cols_sql = ", ".join(column_exprs)
    forward_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({cols_sql}) WITH (timescaledb.transaction_per_chunk);"
    reverse_sql = f"DROP INDEX IF EXISTS {index_name};"
    return migrations.RunSQL(forward_sql, reverse_sql=reverse_sql)


def ts_create_index_managed_hypertable(
    index_name: str,
    table_name: str,
    columns: list[str],
    *,
    model_name: str,
) -> migrations.SeparateDatabaseAndState:
    """
    Create an index on a table that is managed by Django. This will ensure that the
    index is created in the DB and Django will know about it.
    """
    column_exprs: list[str] = [str(col) for col in columns]

    cols_sql = ", ".join(column_exprs)
    forward_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name} ({cols_sql}) WITH (timescaledb.transaction_per_chunk);"
    reverse_sql = f"DROP INDEX IF EXISTS {index_name};"

    return migrations.SeparateDatabaseAndState(
        database_operations=[
            migrations.RunSQL(forward_sql, reverse_sql=reverse_sql),
        ],
        state_operations=[
            migrations.AddIndex(
                model_name=model_name,
                index=models.Index(fields=columns, name=index_name),
            ),
        ],
    )


def ts_add_continuous_aggregate_policy(
    view_name: str,
    *,
    start_offset: str | None,
    end_offset: str | None,
    schedule_interval: str,
) -> migrations.RunSQL:
    start_sql = "NULL" if start_offset is None else f"'{start_offset}'"
    end_sql = "NULL" if end_offset is None else f"'{end_offset}'"
    schedule_sql = f"INTERVAL '{schedule_interval}'"

    forward_sql = f"""
        SELECT add_continuous_aggregate_policy(
            '{view_name}',
            start_offset => {start_sql},
            end_offset => {end_sql},
            schedule_interval => {schedule_sql}
        );
    """.strip()

    reverse_sql = f"SELECT remove_continuous_aggregate_policy('{view_name}');"
    return migrations.RunSQL(forward_sql, reverse_sql=reverse_sql)


def ts_add_retention_policy(relation_name: str, drop_after: str) -> migrations.RunSQL:
    forward_sql = (
        f"SELECT add_retention_policy('{relation_name}', INTERVAL '{drop_after}');"
    )
    reverse_sql = f"SELECT remove_retention_policy('{relation_name}');"
    return migrations.RunSQL(forward_sql, reverse_sql=reverse_sql)


def ts_remove_retention_policy(
    relation_name: str, *, reverse_drop_after: str
) -> migrations.RunSQL:
    forward_sql = f"SELECT remove_retention_policy('{relation_name}');"
    reverse_sql = f"SELECT add_retention_policy('{relation_name}', INTERVAL '{reverse_drop_after}');"
    return migrations.RunSQL(forward_sql, reverse_sql=reverse_sql)


def ts_refresh_continuous_aggregate(
    view_name: str,
    start_expr_sql: str,
    end_expr_sql: str | None,
    *,
    risky: bool,
    force: bool = False,
) -> RiskyRunSQL | migrations.RunSQL:
    end_sql = "NULL" if end_expr_sql is None else end_expr_sql
    force_sql = "true" if force else "false"
    forward_sql = f"""
        CALL refresh_continuous_aggregate(
            '{view_name}',
            {start_expr_sql},
            {end_sql},
            {force_sql}
        );
    """.strip()

    if risky:
        return RiskyRunSQL(forward_sql)
    else:
        return migrations.RunSQL(forward_sql)


def create_table_ttl_retention(
    *,
    table_name: str,
    schedule_interval: str = "1 hour",
) -> list[migrations.RunSQL]:
    identifier = table_name.replace(".", "_")
    proc_name = f"ttl_cleanup_{identifier}"

    proc_sql_body = f"""
DECLARE
    target_table text;
BEGIN
    target_table := '{table_name}';
    EXECUTE format('DELETE FROM %s WHERE ttl IS NOT NULL AND ttl < NOW()', target_table);
END""".strip()

    create_proc_sql = f"""
        CREATE OR REPLACE PROCEDURE {proc_name}(job_id int, config jsonb)
        LANGUAGE plpgsql
        AS $$
        {proc_sql_body}
        $$;
    """.strip()

    drop_proc_sql = f"DROP PROCEDURE IF EXISTS {proc_name}(int, jsonb);"
    add_job_sql = f"SELECT add_job('{proc_name}', '{schedule_interval}');"

    return [
        migrations.RunSQL(create_proc_sql, reverse_sql=drop_proc_sql),
        migrations.RunSQL(add_job_sql, reverse_sql=""),
    ]


def create_ca_ttl_retention(
    *,
    ca_name: str,
    schedule_interval: str = "1 hour",
) -> list[migrations.RunSQL]:
    identifier = ca_name.replace(".", "_")
    proc_name = f"ttl_cleanup_{identifier}"

    proc_sql_body = f"""
DECLARE
    target_table text;
BEGIN
    SELECT
        quote_ident(materialized_hypertable_schema) || '.' || quote_ident(materialized_hypertable_name)
    INTO target_table
    FROM timescaledb_information.continuous_aggregates
    WHERE (view_schema || '.' || view_name) = '{ca_name}' OR view_name = '{ca_name}'
    LIMIT 1;
    IF target_table IS NULL THEN
        RAISE EXCEPTION 'Could not resolve materialized hypertable for CA %', '{ca_name}';
    END IF;
    EXECUTE format('DELETE FROM %s WHERE ttl IS NOT NULL AND ttl < NOW()', target_table);
END""".strip()

    create_proc_sql = f"""
        CREATE OR REPLACE PROCEDURE {proc_name}(job_id int, config jsonb)
        LANGUAGE plpgsql
        AS $$
        {proc_sql_body}
        $$;
    """.strip()

    drop_proc_sql = f"DROP PROCEDURE IF EXISTS {proc_name}(int, jsonb);"
    add_job_sql = f"SELECT add_job('{proc_name}', '{schedule_interval}');"

    return [
        migrations.RunSQL(create_proc_sql, reverse_sql=drop_proc_sql),
        migrations.RunSQL(add_job_sql, reverse_sql=""),
    ]
