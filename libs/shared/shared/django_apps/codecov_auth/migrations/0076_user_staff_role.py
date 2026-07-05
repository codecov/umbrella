# Generated for Django admin RBAC (None / Viewer / Member / Admin)

from django.db import migrations, models


def set_initial_staff_roles(apps, schema_editor):
    """Map existing users onto the RBAC levels.

    Superusers become ``admin`` and other staff become ``member`` (their
    pre-RBAC access); everyone else has no role (``none``).
    """
    User = apps.get_model("codecov_auth", "User")
    User.objects.filter(is_staff=True, is_superuser=False).update(staff_role="member")
    User.objects.filter(is_superuser=True).update(staff_role="admin")


def reverse_initial_staff_roles(apps, schema_editor):
    # No-op: dropping the column on reverse removes the data anyway.
    pass


class Migration(migrations.Migration):
    """
    BEGIN;
    --
    -- Add field staff_role to user
    --
    ALTER TABLE "users" ADD COLUMN "staff_role" text DEFAULT 'none' NULL;
    ALTER TABLE "users" ALTER COLUMN "staff_role" DROP DEFAULT;
    COMMIT;
    """

    dependencies = [
        ("codecov_auth", "0075_owner_support_pin"),
    ]

    operations = [
        migrations.AddField(
            model_name="user",
            name="staff_role",
            field=models.TextField(
                blank=True,
                choices=[
                    ("none", "None"),
                    ("viewer", "Viewer"),
                    ("member", "Member"),
                    ("admin", "Admin"),
                ],
                default="none",
                null=True,
            ),
        ),
        migrations.RunPython(
            set_initial_staff_roles,
            reverse_initial_staff_roles,
        ),
    ]
