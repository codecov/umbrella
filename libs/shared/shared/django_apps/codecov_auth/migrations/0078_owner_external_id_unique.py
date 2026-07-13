from django.db import migrations, models


class Migration(migrations.Migration):
    """Enforce uniqueness on `Owner.external_id`.

    IMPORTANT: run this only AFTER `backfill_owner_external_ids.py` has
    populated every pre-existing row. Postgres treats NULLs as distinct so the
    unique index is satisfiable even with un-backfilled rows, but the intent is
    for the backfill to complete first.

    The backing index is built with CREATE UNIQUE INDEX CONCURRENTLY so the
    owners table is never locked against writes. CONCURRENTLY cannot run inside
    a transaction, hence `atomic = False`. Django's migration state records a
    matching UniqueConstraint via SeparateDatabaseAndState; the concurrently
    created unique index satisfies it (Postgres introspection surfaces unique
    indexes as constraints), so no additional locking ALTER TABLE is issued.

    The reverse uses a plain DROP INDEX (not CONCURRENTLY): dropping is fast and,
    unlike the concurrent create, must be runnable inside a transaction so the
    migration-test harness can unapply it.

    This also sets a server-side DEFAULT of gen_random_uuid() on external_id. The
    model already assigns uuid.uuid4() at the Django layer, so ORM writes were
    always covered; the DB default is belt-and-suspenders for any non-ORM insert
    (raw SQL, COPY/ETL, cross-service writes) so those rows never land NULL. It is
    a metadata-only change (no table rewrite) and does not affect Django's model
    state, so it runs as a plain RunSQL with a DROP DEFAULT reverse.

    BEGIN;
    --
    -- (run outside a transaction)
    CREATE UNIQUE INDEX CONCURRENTLY "owner_external_id_uniq" ON "owners" ("external_id");
    COMMIT;
    """

    atomic = False

    dependencies = [
        ("codecov_auth", "0077_owner_external_id"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[
                migrations.RunSQL(
                    sql=(
                        "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "
                        '"owner_external_id_uniq" ON "owners" ("external_id");'
                    ),
                    reverse_sql='DROP INDEX IF EXISTS "owner_external_id_uniq";',
                ),
            ],
            state_operations=[
                migrations.AddConstraint(
                    model_name="owner",
                    constraint=models.UniqueConstraint(
                        fields=["external_id"], name="owner_external_id_uniq"
                    ),
                ),
            ],
        ),
        migrations.RunSQL(
            sql=(
                'ALTER TABLE "owners" '
                'ALTER COLUMN "external_id" SET DEFAULT gen_random_uuid();'
            ),
            reverse_sql=(
                'ALTER TABLE "owners" ALTER COLUMN "external_id" DROP DEFAULT;'
            ),
        ),
    ]
