from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("codecov_auth", "0079_ownertobedeleted_requested_by_and_on_hold"),
        ("billing", "0003_delete_account"),
    ]

    operations = [
        migrations.CreateModel(
            name="StripeBilling",
            fields=[],
            options={
                "verbose_name": "Stripe billing",
                "verbose_name_plural": "Stripe billing",
                "proxy": True,
                "indexes": [],
                "constraints": [],
            },
            bases=("codecov_auth.owner",),
        ),
    ]
