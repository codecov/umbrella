# Generated for Django admin RBAC (Viewer / Member / Admin)

from django.db import migrations, models


def set_initial_staff_roles(apps, schema_editor):
    """Map existing admin users onto the new RBAC levels.

    Existing staff (``is_staff=True``) keep today's access as ``member`` while
    superusers become ``admin``. Everyone else keeps the ``viewer`` default,
    which only matters once they are granted ``is_staff``.
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
    ALTER TABLE "users" ADD COLUMN "staff_role" text DEFAULT 'viewer' NULL;
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
                    ("viewer", "Viewer"),
                    ("member", "Member"),
                    ("admin", "Admin"),
                ],
                default="viewer",
                null=True,
            ),
        ),
        migrations.RunPython(
            set_initial_staff_roles,
            reverse_initial_staff_roles,
        ),
    ]
