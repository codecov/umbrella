# Generated to fix missing owners.support_pin column in production.
#
# Migration 0075_owner_support_pin used RiskyAddField, which is skipped when
# SKIP_RISKY_MIGRATION_STEPS=True.  If that flag was set during the 0075 deploy,
# Django recorded the migration as applied but never ran the ALTER TABLE, leaving
# the column absent and causing ProgrammingError in any ORM query that touches Owner.
#
# This migration uses plain RunSQL with an idempotent DO-block so it always ensures
# the column exists regardless of the 0075 outcome.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("codecov_auth", "0075_owner_support_pin"),
    ]

    operations = [
        # Idempotently add the column — safe to run even if it already exists.
        migrations.RunSQL(
            sql="""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM information_schema.columns
                        WHERE table_name = 'owners'
                          AND column_name = 'support_pin'
                    ) THEN
                        ALTER TABLE "owners" ADD COLUMN "support_pin" varchar(6) NULL;
                    END IF;
                END
                $$;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
        # Backfill any rows that still have NULL (pre-existing rows skipped by 0075).
        migrations.RunSQL(
            sql="""
                UPDATE owners
                SET support_pin = lpad((floor(random() * 1000000))::int::text, 6, '0')
                WHERE support_pin IS NULL;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]