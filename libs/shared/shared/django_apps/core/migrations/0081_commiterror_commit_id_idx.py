from django.db import migrations, models


class Migration(migrations.Migration):
    """
    BEGIN;
    CREATE INDEX "commiterror_commit_id_idx" ON "core_commiterror" ("commit_id", "id");
    COMMIT;
    """

    dependencies = [
        ("core", "0080_repository_repos_name_trgm_idx"),
    ]

    operations = [
        migrations.AddIndex(
            model_name="commiterror",
            index=models.Index(
                fields=["commit_id", "id"], name="commiterror_commit_id_idx"
            ),
        ),
    ]
