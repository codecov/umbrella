from unittest.mock import patch

from shared.django_apps.codecov_auth.migrations.migration_helpers.migration_0067 import (
    backfill_app_id,
)
from shared.utils.test_utils.test_migrations_helper import TestMigrations


class Migration0067Test(TestMigrations):
    app = "codecov_auth"
    migrate_from = "0066_add_pro_plan"
    migrate_to = "0066_add_pro_plan"

    def setUpBeforeMigration(self, apps):
        # have to get models this way when using this test class
        self.GithubAppInstallation = apps.get_model(
            "codecov_auth", "GithubAppInstallation"
        )
        self.Owner = apps.get_model("codecov_auth", "Owner")

        self.owner = self.Owner.objects.create(
            username="test_owner", service="github", service_id="12345"
        )

        self.null_app_id1 = self.GithubAppInstallation.objects.create(
            owner=self.owner, installation_id=201, app_id=None, name="test-null-app"
        )

        self.null_app_id2 = self.GithubAppInstallation.objects.create(
            owner=self.owner, installation_id=202, app_id=None, name="test-null-app"
        )

        self.existing_app_id = self.GithubAppInstallation.objects.create(
            owner=self.owner,
            installation_id=203,
            app_id=54321,
            name="test-existing-app",
        )

    @patch(
        "shared.django_apps.codecov_auth.migrations.migration_helpers.migration_0067.get_config"
    )
    def test_backfill_app_id_with_config_value(self, mock_get_config):
        DEFAULT_APP_ID = 12345
        mock_get_config.return_value = DEFAULT_APP_ID

        backfill_app_id(self.GithubAppInstallation)

        self.null_app_id1.refresh_from_db()
        self.null_app_id2.refresh_from_db()
        self.assertEqual(self.null_app_id1.app_id, DEFAULT_APP_ID)
        self.assertEqual(self.null_app_id2.app_id, DEFAULT_APP_ID)

        existing_app = self.GithubAppInstallation.objects.get(
            id=self.existing_app_id.id
        )
        self.assertEqual(existing_app.app_id, 54321)
