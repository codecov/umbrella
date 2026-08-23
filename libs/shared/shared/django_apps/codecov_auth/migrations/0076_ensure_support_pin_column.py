# Generated to fix: ProgrammingError: column owners.support_pin does not exist
#
# Migration 0075_owner_support_pin used RiskyAddField, which skips the actual
# DDL when SKIP_RISKY_MIGRATION_STEPS=True. This migration unconditionally
# ensures the column exists using IF NOT EXISTS, and backfills any rows that
# were missed.

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("codecov_auth", "0075_owner_support_pin"),
    ]

    operations = [
        # Add the column if it was skipped by the risky migration pattern.
        # IF NOT EXISTS is a no-op when the column already exists.
        migrations.RunSQL(
            sql="""
                ALTER TABLE "owners"
                ADD COLUMN IF NOT EXISTS "support_pin" varchar(6) NULL;

                UPDATE owners
                SET support_pin = lpad((floor(random() * 1000000))::int::text, 6, '0')
                WHERE support_pin IS NULL;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]