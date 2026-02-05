from django.db import migrations, models

from shared.django_apps.migration_utils import RiskyAddIndex


class Migration(migrations.Migration):
    """
    BEGIN;
    CREATE INDEX "commitreport_commit_id_idx" ON "reports_commitreport" ("commit_id", "id");
    CREATE INDEX "uploaderror_session_id_idx" ON "reports_uploaderror" ("upload_id", "id");
    CREATE INDEX "uploadflag_session_id_idx" ON "reports_uploadflagmembership" ("upload_id", "id");
    CREATE INDEX "repoflag_repo_id_idx" ON "reports_repositoryflag" ("repository_id", "id");
    CREATE INDEX "upload_report_id_idx" ON "reports_upload" ("report_id", "id");
    COMMIT;
    """

    dependencies = [
        ("reports", "0043_remove_flake_reduced_error_remove_flake_repository_and_more"),
    ]

    operations = [
        RiskyAddIndex(
            model_name="commitreport",
            index=models.Index(
                fields=["commit_id", "id"], name="commitreport_commit_id_idx"
            ),
        ),
        RiskyAddIndex(
            model_name="uploaderror",
            index=models.Index(
                fields=["report_session_id", "id"], name="uploaderror_session_id_idx"
            ),
        ),
        RiskyAddIndex(
            model_name="uploadflagmembership",
            index=models.Index(
                fields=["report_session_id", "id"], name="uploadflag_session_id_idx"
            ),
        ),
        RiskyAddIndex(
            model_name="repositoryflag",
            index=models.Index(
                fields=["repository_id", "id"], name="repoflag_repo_id_idx"
            ),
        ),
        RiskyAddIndex(
            model_name="reportsession",
            index=models.Index(fields=["report_id", "id"], name="upload_report_id_idx"),
        ),
    ]
