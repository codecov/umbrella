from django.db import migrations, models


class Migration(migrations.Migration):
    """Django-only: Owner.plan can no longer be blank in forms/admin.

    Null remains allowed at the database layer. No SQL is issued.
    """

    dependencies = [
        ("codecov_auth", "0079_ownertobedeleted_requested_by_and_on_hold"),
    ]

    operations = [
        migrations.AlterField(
            model_name="owner",
            name="plan",
            field=models.TextField(default="users-developer", null=True),
        ),
    ]
