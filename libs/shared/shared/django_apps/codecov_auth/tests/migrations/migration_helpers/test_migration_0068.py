from shared.django_apps.codecov_auth.migrations.migration_helpers import (
    eliminate_dupes,
)
from shared.utils.test_utils.test_migrations_helper import TestMigrations


class Migration0068Test(TestMigrations):
    app = "codecov_auth"
    migrate_from = "0067_alter_githubappinstallation_app_id"
    migrate_to = "0067_alter_githubappinstallation_app_id"

    def setUpBeforeMigration(self, apps):
        # have to get models this way when using this test class
        self.GithubAppInstallation = apps.get_model(
            "codecov_auth", "GithubAppInstallation"
        )
        self.Owner = apps.get_model("codecov_auth", "Owner")

        self.owner = self.Owner.objects.create(
            username="test_owner", service="github", service_id="12345"
        )

        self.copy_1 = self.GithubAppInstallation.objects.create(
            owner=self.owner, installation_id=201, app_id=12345, name="test-default-app"
        )
        self.copy_2 = self.GithubAppInstallation.objects.create(
            owner=self.owner, installation_id=201, app_id=12345, name="test-default-app"
        )
        self.copy_3 = self.GithubAppInstallation.objects.create(
            owner=self.owner, installation_id=201, app_id=12345, name="test-default-app"
        )

        self.unique_install = self.GithubAppInstallation.objects.create(
            owner=self.owner,
            installation_id=203,
            app_id=54321,
            name="test-default-app",
        )

    def test_eliminate_dupes(self):
        self.assertEqual(
            self.GithubAppInstallation.objects.filter(
                installation_id=201, app_id=12345
            ).count(),
            3,
        )

        eliminate_dupes(self.GithubAppInstallation)

        # removed duplicates, keeping the first one
        self.assertEqual(
            self.GithubAppInstallation.objects.filter(
                installation_id=201, app_id=12345
            ).count(),
            1,
        )
        self.assertTrue(
            self.GithubAppInstallation.objects.filter(id=self.copy_1.id).exists()
        )
        self.assertFalse(
            self.GithubAppInstallation.objects.filter(id=self.copy_2.id).exists()
        )
        self.assertFalse(
            self.GithubAppInstallation.objects.filter(id=self.copy_3.id).exists()
        )

        # should be unchanged
        self.assertEqual(
            self.GithubAppInstallation.objects.filter(
                installation_id=203, app_id=54321, name="test-default-app"
            ).count(),
            1,
        )
