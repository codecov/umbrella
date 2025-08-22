from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("test_analytics", "0006_taupload_state"),
    ]

    operations = [
        migrations.RemoveIndex(
            model_name="taupload",
            name="ta_upload_repo_idx",
        ),
        migrations.RemoveIndex(
            model_name="taupload",
            name="ta_upload_time_idx",
        ),
        migrations.RemoveIndex(
            model_name="taupload",
            name="ta_upload_repo_time_idx",
        ),
        migrations.AddIndex(
            model_name="taupload",
            index=models.Index(
                fields=["repo_id", "state", "created_at"],
                name="ta_upload_repo_state_time_idx",
            ),
        ),
    ]
