from django.db import migrations, models

from shared.django_apps.migration_utils import RiskyAddIndex


class Migration(migrations.Migration):
    """
    BEGIN;
    CREATE INDEX "dataset_repo_id_idx" ON "timeseries_dataset" ("repository_id", "id");
    COMMIT;
    """

    dependencies = [
        ("timeseries", "0020_delete_testrun_delete_testrunbranchsummary_and_more"),
    ]

    operations = [
        RiskyAddIndex(
            model_name="dataset",
            index=models.Index(
                fields=["repository_id", "id"], name="dataset_repo_id_idx"
            ),
        ),
    ]
