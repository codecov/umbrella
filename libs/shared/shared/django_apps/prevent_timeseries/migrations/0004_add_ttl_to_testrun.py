from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("prevent_timeseries", "0003_create_test_aggregates"),
    ]

    operations = [
        migrations.AddField(
            model_name="testrun",
            name="ttl",
            field=models.DateTimeField(null=True),
        ),
    ]
