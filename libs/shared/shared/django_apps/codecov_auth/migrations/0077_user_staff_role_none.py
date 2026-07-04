# Generated for Django admin RBAC: add the `none` role for non-staff users

from django.db import migrations, models


def set_non_staff_to_none(apps, schema_editor):
    """Non-staff, non-superuser users have no RBAC role.

    Existing rows were defaulted to ``viewer`` by migration 0076; move any that
    are not staff onto the new ``none`` level. Staff and superusers keep their
    ``member`` / ``admin`` roles.
    """
    User = apps.get_model("codecov_auth", "User")
    User.objects.filter(is_superuser=False).exclude(is_staff=True).update(
        staff_role="none"
    )


def reverse_non_staff_to_none(apps, schema_editor):
    User = apps.get_model("codecov_auth", "User")
    User.objects.filter(staff_role="none").update(staff_role="viewer")


class Migration(migrations.Migration):
    """
    BEGIN;
    --
    -- Alter field staff_role on user
    --
    -- (choices/default are enforced in Python only; no DDL change.)
    COMMIT;
    """

    dependencies = [
        ("codecov_auth", "0076_user_staff_role"),
    ]

    operations = [
        migrations.AlterField(
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
            set_non_staff_to_none,
            reverse_non_staff_to_none,
        ),
    ]
