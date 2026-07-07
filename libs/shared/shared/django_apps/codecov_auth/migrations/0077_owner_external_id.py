import uuid

from django.db import migrations, models

from shared.django_apps.migration_utils import RiskyAddField


class Migration(migrations.Migration):
    """
    BEGIN;
    --
    -- Add field external_id to owner
    --
    ALTER TABLE "owners" ADD COLUMN "external_id" uuid NULL;
    COMMIT;
    """

    dependencies = [
        ("codecov_auth", "0076_user_staff_role"),
    ]

    operations = [
        # Adding a nullable column with no database default is a metadata-only
        # change in Postgres, so existing rows are NOT rewritten and simply
        # default to NULL. Real UUIDs are assigned out of band by the
        # `backfill_owner_external_ids.py` script.
        #
        # The database side deliberately omits the `uuid.uuid4` default: a
        # callable default would be evaluated once at migration time and
        # stamped onto every existing row, giving them all the *same* UUID.
        # Instead we keep the column NULL in the DB while recording the real
        # `default=uuid.uuid4` in Django's state so that *new* owners created
        # through the ORM get a fresh UUID automatically.
        migrations.SeparateDatabaseAndState(
            database_operations=[
                RiskyAddField(
                    model_name="owner",
                    name="external_id",
                    field=models.UUIDField(blank=True, default=None, null=True),
                ),
            ],
            state_operations=[
                migrations.AddField(
                    model_name="owner",
                    name="external_id",
                    field=models.UUIDField(
                        blank=True,
                        default=uuid.uuid4,
                        editable=False,
                        null=True,
                    ),
                ),
            ],
        ),
    ]
