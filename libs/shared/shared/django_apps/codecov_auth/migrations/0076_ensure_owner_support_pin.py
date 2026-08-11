# Generated manually to fix missing support_pin column in production.
#
# Migration 0075 used RiskyAddField, which skips the DDL when
# SKIP_RISKY_MIGRATION_STEPS=True.  If that flag was set when 0075 ran, the
# column was recorded as applied in Django's migration table but was never
# actually added to the database, causing:
#
#   ProgrammingError: column owners.support_pin does not exist
#
# This migration unconditionally adds the column (using IF NOT EXISTS so it is
# idempotent) and backfills any rows that are still NULL.

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("codecov_auth", "0075_owner_support_pin"),
    ]

    operations = [
        # Use plain RunSQL (not RiskyRunSQL) so this always runs regardless of
        # the SKIP_RISKY_MIGRATION_STEPS setting.
        migrations.RunSQL(
            sql='ALTER TABLE "owners" ADD COLUMN IF NOT EXISTS "support_pin" varchar(6) NULL;',
            reverse_sql=migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            sql="UPDATE owners SET support_pin = lpad((floor(random() * 1000000))::int::text, 6, '0') WHERE support_pin IS NULL;",
            reverse_sql=migrations.RunSQL.noop,
        ),
    ]