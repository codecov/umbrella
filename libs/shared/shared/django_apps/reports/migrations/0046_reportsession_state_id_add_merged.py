from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("reports", "0045_uploadleveltotals_session_id_idx"),
    ]

    operations = [
        migrations.AlterField(
            model_name="reportsession",
            name="state_id",
            field=models.IntegerField(
                choices=[
                    (1, "UPLOADED"),
                    (2, "PROCESSED"),
                    (3, "ERROR"),
                    (4, "FULLY_OVERWRITTEN"),
                    (5, "PARTIALLY_OVERWRITTEN"),
                    (6, "MERGED"),
                ],
                null=True,
            ),
        ),
    ]
