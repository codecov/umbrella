from django.db import migrations, models

from shared.django_apps.migration_utils import RiskyAddIndex


class Migration(migrations.Migration):
    dependencies = [
        ("ta_timeseries", "0029_add_retention_policies_to_new_caggs"),
    ]

    operations = [
        RiskyAddIndex(
            model_name="testrun",
            index=models.Index(
                fields=["repo_id", "test_id", "timestamp"],
                name="ta_ts__repo_test_time_idx",
            ),
        ),
    ]
