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
