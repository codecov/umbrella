from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_analytics", "0007_remove_old_indexes"),
    ]

    operations = [
        migrations.AddConstraint(
            model_name="flake",
            constraint=models.UniqueConstraint(
                fields=["repoid", "test_id"],
                name="test_analytics_flake_repoid_test_id_unique",
            ),
        ),
    ]