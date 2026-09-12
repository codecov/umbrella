from django.db import migrations

import shared.django_apps.db_fields


class Migration(migrations.Migration):
    dependencies = [
        ("codecov_auth", "0080_alter_owner_plan"),
    ]

    operations = [
        migrations.SeparateDatabaseAndState(
            database_operations=[],
            state_operations=[
                migrations.AlterField(
                    model_name="user",
                    name="email",
                    field=shared.django_apps.db_fields.CaseInsensitiveTextField(
                        null=True
                    ),
                ),
                migrations.AlterField(
                    model_name="owner",
                    name="username",
                    field=shared.django_apps.db_fields.CaseInsensitiveTextField(
                        null=True, unique=True
                    ),
                ),
            ],
        ),
    ]
