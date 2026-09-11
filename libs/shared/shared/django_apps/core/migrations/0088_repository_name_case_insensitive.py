from django.db import migrations

import shared.django_apps.db_fields


class Migration(migrations.Migration):
    dependencies = [
        ("core", "0087_increment_version"),
        ("codecov_auth", "0081_case_insensitive_text_fields"),
    ]

    operations = [
        migrations.AlterField(
            model_name="repository",
            name="name",
            field=shared.django_apps.db_fields.CaseInsensitiveTextField(),
        ),
    ]
