from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_analytics", "0007_remove_old_indexes"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="flake",
            index=models.Index(
                fields=["repoid", "end_date", "test_id"],
                name="test_analyt_repoid_end_date_test_id_idx",
            ),
        ),
    ]
