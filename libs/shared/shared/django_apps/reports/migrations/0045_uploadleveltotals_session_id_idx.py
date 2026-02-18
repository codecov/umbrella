from django.contrib.postgres.operations import AddIndexConcurrently
from django.db import migrations, models


class Migration(migrations.Migration):
    """
    CREATE INDEX CONCURRENTLY "ult_session_id_idx" ON "reports_uploadleveltotals" ("upload_id", "id");
    """

    atomic = False

    dependencies = [
        ("reports", "0044_add_fk_indexes"),
    ]

    operations = [
        AddIndexConcurrently(
            model_name="uploadleveltotals",
            index=models.Index(
                fields=["report_session_id", "id"],
                name="ult_session_id_idx",
            ),
        ),
    ]
