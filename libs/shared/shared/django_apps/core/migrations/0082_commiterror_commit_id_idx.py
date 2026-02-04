from django.db import migrations, models


class Migration(migrations.Migration):
    """
    BEGIN;
    CREATE INDEX "commiterror_commit_id_idx" ON "core_commiterror" ("commit_id", "id");
    COMMIT;
    """

    dependencies = [
        ("core", "0081_increment_version"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="commiterror",
            index=models.Index(
                fields=["commit_id", "id"], name="commiterror_commit_id_idx"
            ),
        ),
    ]
