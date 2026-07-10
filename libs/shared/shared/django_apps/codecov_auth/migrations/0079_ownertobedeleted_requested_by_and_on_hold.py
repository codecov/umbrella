import django.db.models.deletion
from django.conf import settings
from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("codecov_auth", "0078_owner_external_id_unique"),
    ]

    operations = [
        migrations.AddField(
            model_name="ownertobedeleted",
            name="requested_by",
            field=models.ForeignKey(
                blank=True,
                null=True,
                on_delete=django.db.models.deletion.SET_NULL,
                related_name="requested_owner_deletions",
                to=settings.AUTH_USER_MODEL,
            ),
        ),
        migrations.AddField(
            model_name="ownertobedeleted",
            name="on_hold",
            field=models.BooleanField(default=False),
        ),
    ]
