from unittest.mock import patch

from django.utils import timezone
from rest_framework.reverse import reverse

from api.internal.commit.serializers import (
    CommitTotalsSerializer,
    CommitWithFileLevelReportSerializer,
)
from api.internal.tests.test_utils import GetAdminProviderAdapter
from codecov.tests.base_test import InternalAPITest
from core.models import Repository
from shared.django_apps.core.tests.factories import (
    CommitFactory,
    OwnerFactory,
    RepositoryFactory,
)
from shared.torngit.exceptions import TorngitClientGeneralError
from utils.test_utils import Client


class RepositoryViewSetTestSuite(InternalAPITest):
    def _list(self, kwargs={}, query_params={}):
        if kwargs == {}:
            kwargs = {"service": self.org.service, "owner_username": self.org.username}

        return self.client.get(reverse("repos-list", kwargs=kwargs), data=query_params)

    def _retrieve(self, kwargs={}, data={}):
        if kwargs == {}:
            kwargs = {
                "service": self.org.service,
                "owner_username": self.org.username,
                "repo_name": self.repo.name,
            }
        return self.client.get(reverse("repos-detail", kwargs=kwargs), data=data)

    def _update(self, kwargs={}, data={}):
        if kwargs == {}:
            kwargs = {
                "service": self.org.service,
                "owner_username": self.org.username,
                "repo_name": self.repo.name,
            }
        return self.client.patch(
            reverse("repos-detail", kwargs=kwargs),
            data=data,
            content_type="application/json",
        )

    def _destroy(self, kwargs={}):
        if kwargs == {}:
            kwargs = {
                "service": self.org.service,
                "owner_username": self.org.username,
                "repo_name": self.repo.name,
            }
        return self.client.delete(reverse("repos-detail", kwargs=kwargs))


class TestRepositoryViewSetList(RepositoryViewSetTestSuite):
    def setUp(self):
        self.org = OwnerFactory(username="codecov", service="github")

        self.repo1 = RepositoryFactory(
            author=self.org, active=True, private=True, name="A"
        )
        self.repo2 = RepositoryFactory(
            author=self.org, active=True, private=True, name="B"
        )

        repos_with_permission = [self.repo1.repoid, self.repo2.repoid]

        self.current_owner = OwnerFactory(
            username="codecov-user",
            service="github",
            organizations=[self.org.ownerid],
            permission=repos_with_permission,
        )

        self.client = Client()
        self.client.force_login_owner(self.current_owner)

    def test_can_retrieve_repo_list_if_not_authenticated(self):
        self.client.logout()
        response = self._list()
        assert response.status_code == 200

    def test_order_by_updatestamp(self):
        response = self._list(query_params={"ordering": "updatestamp"})

        assert response.data["results"][0]["repoid"] == self.repo1.repoid
        assert response.data["results"][1]["repoid"] == self.repo2.repoid

        reverse_response = self._list(query_params={"ordering": "-updatestamp"})

        assert reverse_response.data["results"][0]["repoid"] == self.repo2.repoid
        assert reverse_response.data["results"][1]["repoid"] == self.repo1.repoid

    def test_order_by_name(self):
        response = self._list(query_params={"ordering": "name"})

        assert response.data["results"][0]["repoid"] == self.repo1.repoid
        assert response.data["results"][1]["repoid"] == self.repo2.repoid

        reverse_response = self._list(query_params={"ordering": "-name"})

        assert reverse_response.data["results"][0]["repoid"] == self.repo2.repoid
        assert reverse_response.data["results"][1]["repoid"] == self.repo1.repoid

    def test_order_by_coverage(self):
        default_totals = {
            "f": 1,
            "n": 4,
            "h": 4,
            "m": 0,
            "p": 0,
            "c": 100.0,
            "b": 0,
            "d": 0,
            "s": 1,
            "C": 0.0,
            "N": 0.0,
            "diff": "",
        }

        CommitFactory(repository=self.repo1, totals={**default_totals, "c": 25})
        CommitFactory(repository=self.repo1, totals={**default_totals, "c": 41})
        CommitFactory(repository=self.repo2, totals={**default_totals, "c": 32})

        response = self._list(query_params={"ordering": "coverage"})

        assert response.data["results"][0]["repoid"] == self.repo2.repoid
        assert response.data["results"][1]["repoid"] == self.repo1.repoid

        reverse_response = self._list(query_params={"ordering": "-coverage"})

        assert reverse_response.data["results"][0]["repoid"] == self.repo1.repoid
        assert reverse_response.data["results"][1]["repoid"] == self.repo2.repoid

    def test_order_by_lines(self):
        default_totals = {
            "f": 1,
            "n": 4,
            "h": 4,
            "m": 0,
            "p": 0,
            "c": 100.0,
            "b": 0,
            "d": 0,
            "s": 1,
            "C": 0.0,
            "N": 0.0,
            "diff": "",
        }

        CommitFactory(repository=self.repo1, totals={**default_totals, "n": 25})
        CommitFactory(repository=self.repo2, totals={**default_totals, "n": 32})

        response = self._list(query_params={"ordering": "lines"})

        assert response.data["results"][0]["repoid"] == self.repo1.repoid
        assert response.data["results"][1]["repoid"] == self.repo2.repoid

        reverse_response = self._list(query_params={"ordering": "-lines"})

        assert reverse_response.data["results"][0]["repoid"] == self.repo2.repoid
        assert reverse_response.data["results"][1]["repoid"] == self.repo1.repoid

    def test_totals_serializer(self):
        default_totals = {
            "f": 1,
            "n": 4,
            "h": 4,
            "m": 0,
            "p": 0,
            "c": 100.0,
            "b": 0,
            "d": 0,
            "s": 1,
            "C": 0.0,
            "N": 0.0,
            "diff": "",
        }

        CommitFactory(repository=self.repo1, totals=default_totals)
        # Make sure we only get the commit from the default branch
        CommitFactory(
            repository=self.repo1, totals={**default_totals, "c": 90.0}, branch="other"
        )

        response = self._list(query_params={"names": "A"})

        assert (
            response.data["results"][0]["latest_commit_totals"]["files"]
            == default_totals["f"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["lines"]
            == default_totals["n"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["hits"]
            == default_totals["h"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["misses"]
            == default_totals["m"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["partials"]
            == default_totals["p"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["coverage"]
            == default_totals["c"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["branches"]
            == default_totals["b"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["methods"]
            == default_totals["d"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["sessions"]
            == default_totals["s"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["complexity"]
            == default_totals["C"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["complexity_total"]
            == default_totals["N"]
        )
        assert (
            response.data["results"][0]["latest_commit_totals"]["complexity_ratio"] == 0
        )

    def test_get_totals_with_timestamp(self):
        default_totals = {
            "f": 1,
            "n": 4,
            "h": 4,
            "m": 0,
            "p": 0,
            "c": 100.0,
            "b": 0,
            "d": 0,
            "s": 1,
            "C": 0.0,
            "N": 0.0,
            "diff": "",
        }
        older_coverage = 90.0

        CommitFactory(
            repository=self.repo1, totals={**default_totals, "c": older_coverage}
        )
        # We're testing that the lte works as expected, so we're not sending the exact same timestamp
        fetching_time = timezone.now().isoformat()

        CommitFactory(repository=self.repo1, totals=default_totals)

        response = self._list(query_params={"names": "A", "before_date": fetching_time})

        # The fetching truncates the time, so it will not take into account the time part of the date time
        assert response.data["results"][0]["latest_commit_totals"]["coverage"] == 100.0

    def test_get_repos_with_totals(self):
        default_totals = {
            "f": 1,
            "n": 4,
            "h": 4,
            "m": 0,
            "p": 0,
            "c": 100.0,
            "b": 0,
            "d": 0,
            "s": 1,
            "C": 0.0,
            "N": 0.0,
            "diff": "",
        }

        CommitFactory(repository=self.repo1, totals=default_totals)

        response = self._list(query_params={"exclude_uncovered": True})

        assert response.data["count"] == 1

    def test_get_active_repos(self):
        RepositoryFactory(author=self.org, name="C")
        response = self._list(query_params={"active": True})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            len(response.data["results"]),
            2,
            "got the wrong number of repos: {}".format(len(response.data["results"])),
        )

    def test_get_inactive_repos(self):
        RepositoryFactory(author=self.org, name="C", private=False)

        response = self._list(query_params={"active": False})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            len(response.data["results"]),
            1,
            "got the wrong number of repos: {}".format(len(response.data["results"])),
        )

    def test_get_all_repos(self):
        RepositoryFactory(author=self.org, name="C", private=False)

        response = self._list()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            len(response.data["results"]),
            3,
            "got the wrong number of repos: {}".format(len(response.data["results"])),
        )

    def test_get_all_repos_by_name(self):
        RepositoryFactory(author=self.org, name="C", private=False)

        response = self._list(query_params={"names": ["A", "B"]})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            len(response.data["results"]),
            2,
            "got the wrong number of repos: {}".format(len(response.data["results"])),
        )

    def test_returns_private_repos_if_user_has_permission(self):
        new_repo = RepositoryFactory(author=self.org, name="C")
        self.current_owner.permission.append(new_repo.repoid)
        self.current_owner.save()

        response = self._list()

        assert response.status_code == 200
        assert len(response.data["results"]) == 3

    def test_returns_private_repos_if_user_owns_repo(self):
        new_repo = RepositoryFactory(author=self.current_owner, name="C")

        response = self._list(
            {
                "service": self.current_owner.service,
                "owner_username": self.current_owner.username,
            }
        )

        assert response.status_code == 200
        assert new_repo.name in [repo["name"] for repo in response.data["results"]]

    def test_returns_public_repos_if_not_owned_by_user_and_not_in_permissions_array(
        self,
    ):
        new_repo = RepositoryFactory(author=self.org, name="C", private=False)

        response = self._list()

        assert response.status_code == 200
        assert new_repo.name in [repo["name"] for repo in response.data["results"]]

    def test_doesnt_return_private_repos_if_above_conditions_not_met(self):
        # Private repo, not owned by user, not in users permissions array
        private_repo = RepositoryFactory(author=self.org, name="C", private=True)

        response = self._list()

        assert response.status_code == 200
        assert private_repo.name not in [
            repo["name"] for repo in response.data["results"]
        ]

    def test_returns_latest_coverage_change(self):
        CommitFactory(
            totals={
                "f": 1,
                "n": 4,
                "h": 4,
                "m": 0,
                "p": 0,
                "c": 100.0,
                "b": 0,
                "d": 0,
                "s": 1,
                "C": 0.0,
                "N": 0.0,
                "diff": "",
            },
            repository=self.repo1,
        )
        CommitFactory(
            totals={
                "f": 1,
                "n": 4,
                "h": 4,
                "m": 0,
                "p": 0,
                "c": 70.0,
                "b": 0,
                "d": 0,
                "s": 1,
                "C": 0.0,
                "N": 0.0,
                "diff": "",
            },
            repository=self.repo1,
        )

        response = self._list()
        repo1 = [repo for repo in response.data["results"] if repo["name"] == "A"][0]
        assert repo1["latest_coverage_change"] == -30

    def test_latest_commit_null(self):
        response = self._list()
        repo1 = [repo for repo in response.data["results"] if repo["name"] == "A"][0]

        # When the commit is missing, its set to None or empty string.
        assert repo1["latest_commit_totals"] is None

    def test_returns_latest_commit_totals(self):
        commit = CommitFactory(repository=self.repo1)
        response = self._list()
        repo1 = [repo for repo in response.data["results"] if repo["name"] == "A"][0]

        assert (
            repo1["latest_commit_totals"] == CommitTotalsSerializer(commit.totals).data
        )


class TestRepositoryViewSetExtraActions(RepositoryViewSetTestSuite):
    def setUp(self):
        self.org = OwnerFactory(username="codecov", service="github")

        self.repo1 = RepositoryFactory(
            author=self.org, active=True, private=True, name="A"
        )
        self.repo2 = RepositoryFactory(
            author=self.org, active=True, private=True, name="B"
        )
        self.repo1Commit1 = CommitFactory(
            totals={
                "f": 1,
                "n": 4,
                "h": 4,
                "m": 0,
                "p": 0,
                "c": 100.0,
                "b": 0,
                "d": 0,
                "s": 1,
                "C": 0.0,
                "N": 0.0,
                "diff": "",
            },
            repository=self.repo1,
        )
        self.repo1Commit2 = CommitFactory(
            totals={
                "f": 1,
                "n": 4,
                "h": 0,
                "m": 0,
                "p": 4,
                "c": 70.0,
                "b": 0,
                "d": 0,
                "s": 1,
                "C": 0.0,
                "N": 0.0,
                "diff": "",
            },
            repository=self.repo1,
        )
        self.repo2Commit1 = CommitFactory(
            totals={
                "f": 1,
                "n": 8,
                "h": 4,
                "m": 4,
                "p": 0,
                "c": 100.0,
                "b": 0,
                "d": 0,
                "s": 1,
                "C": 0.0,
                "N": 0.0,
                "diff": "",
            },
            repository=self.repo2,
        )
        self.repo2Commit2 = CommitFactory(
            totals={
                "f": 1,
                "n": 8,
                "h": 3,
                "m": 5,
                "p": 0,
                "c": 60.0,
                "b": 0,
                "d": 0,
                "s": 1,
                "C": 0.0,
                "N": 0.0,
                "diff": "",
            },
            repository=self.repo2,
        )

        repos_with_permission = [self.repo1.repoid, self.repo2.repoid]

        self.current_owner = OwnerFactory(
            username="codecov-user",
            service="github",
            organizations=[self.org.ownerid],
            permission=repos_with_permission,
        )

        self.client = Client()
        self.client.force_login_owner(self.current_owner)


@patch("api.shared.repo.repository_accessors.RepoAccessors.get_repo_permissions")
class TestRepositoryViewSetDetailActions(RepositoryViewSetTestSuite):
    def setUp(self):
        self.org = OwnerFactory(
            username="codecov", service="github", service_id="5767537"
        )
        self.repo = RepositoryFactory(
            author=self.org,
            active=True,
            private=True,
            name="repo1",
            service_id="201298242",
        )

        self.current_owner = OwnerFactory(
            username="codecov-user", service="github", organizations=[self.org.ownerid]
        )

        self.client = Client()
        self.client.force_login_owner(self.current_owner)

    def test_can_retrieve_repo_if_not_authenticated(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True
        self.client.logout()
        author = OwnerFactory()
        public_repo = RepositoryFactory(author=author, private=False)
        response = self._retrieve(
            kwargs={
                "service": public_repo.author.service,
                "owner_username": public_repo.author.username,
                "repo_name": public_repo.name,
            }
        )
        assert response.status_code == 200

    def test_cant_access_private_repo_if_not_authenticated(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = False, False
        self.client.logout()
        author = OwnerFactory()
        public_repo = RepositoryFactory(author=author, private=True)
        response = self._retrieve(
            kwargs={
                "service": public_repo.author.service,
                "owner_username": public_repo.author.username,
                "repo_name": public_repo.name,
            }
        )
        assert response.status_code == 404

    def test_retrieve_with_view_and_edit_permissions_succeeds(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True
        response = self._retrieve()
        self.assertEqual(response.status_code, 200)
        assert "upload_token" in response.data

    def test_retrieve_without_read_permissions_returns_404(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = False, False
        response = self._retrieve()
        assert response.status_code == 404

    def test_retrieve_for_inactive_user_returns_403(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True
        self.org.plan = "users-inappy"
        self.org.plan_auto_activate = False
        self.org.save()

        response = self._retrieve()
        assert response.status_code == 403
        assert response.data["detail"] == "User not activated"

    def test_retrieve_without_edit_permissions_returns_detail_view_without_upload_token(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, False
        response = self._retrieve()
        assert response.status_code == 200
        assert "upload_token" not in response.data

    def test_destroy_repo_with_admin_rights_succeeds(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True
        self.org.admins = [self.current_owner.ownerid]
        self.org.save()
        response = self._destroy()
        assert response.status_code == 204
        assert not Repository.objects.filter(name="repo1").exists()

    @patch("api.shared.permissions.get_provider")
    def test_destroy_repo_with_provider_admin_rights_succeedes(
        self, mocked_get_provider, mocked_get_permissions
    ):
        mocked_get_provider.return_value = GetAdminProviderAdapter(result=True)
        mocked_get_permissions.return_value = True, True
        response = self._destroy()
        assert response.status_code == 204
        assert not Repository.objects.filter(name="repo1").exists()

    @patch("api.shared.permissions.get_provider")
    def test_destroy_repo_without_admin_rights_returns_403(
        self, mocked_get_provider, mocked_get_permissions
    ):
        mocked_get_provider.return_value = GetAdminProviderAdapter()
        mocked_get_permissions.return_value = True, True

        assert self.current_owner.ownerid not in self.org.admins

        response = self._destroy()
        assert response.status_code == 403
        assert Repository.objects.filter(name="repo1").exists()

    def test_destroy_repo_as_inactive_user_returns_403(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True
        self.org.admins = [self.current_owner.ownerid]
        self.org.plan = "users-inappy"
        self.org.plan_auto_activate = False
        self.org.save()

        response = self._destroy()
        assert response.status_code == 403
        assert response.data["detail"] == "User not activated"
        assert Repository.objects.filter(name="repo1").exists()

    def test_update_default_branch_with_permissions_succeeds(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True
        new_default_branch = "dev"

        response = self._update(data={"branch": new_default_branch})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.data["branch"],
            "dev",
            "got unexpected response: {}".format(response.data["branch"]),
        )
        self.repo.refresh_from_db()
        assert self.repo.branch == new_default_branch

    def test_update_default_branch_without_write_permissions_returns_403(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, False

        response = self._update(data={"branch": "dev"})
        self.assertEqual(response.status_code, 403)

    def test_retrieve_returns_yaml(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, False

        yaml = {"yaml": "val"}
        self.repo.yaml = yaml
        self.repo.save()

        response = self._retrieve()
        assert response.status_code == 200
        assert response.data["yaml"] == yaml

    def test_activation_checks_if_credits_available_for_legacy_users(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True

        self.org.plan = "v4-10m"
        self.org.save()

        for i in range(9):  # including the one used by other tests, should be 10 total
            RepositoryFactory(
                name=str(i) + "random", author=self.org, private=True, active=True
            )

        RepositoryFactory(author=self.org, private=True, active=False)

        activation_data = {"active": True}
        response = self._update(data=activation_data)

        assert response.status_code == 403

    def test_repo_bot_returns_username_if_bot_not_null(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True
        username = "huecoTanks"
        self.repo.bot = OwnerFactory(username=username)
        self.repo.save()

        response = self._retrieve()

        assert "bot" in response.data
        assert response.data["bot"] == username

    def test_retrieve_with_no_commits_doesnt_crash(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True

        self.repo.commits.all().delete()

        response = self._retrieve()
        assert response.status_code == 200

    @patch("shared.api_archive.archive.ArchiveService.read_chunks", lambda obj, _: "")
    def test_retrieve_returns_latest_commit_data(self, mocked_get_permissions):
        self.maxDiff = None
        mocked_get_permissions.return_value = True, True
        commit = CommitFactory(
            repository=self.repo,
            _report={
                "files": {
                    "test_file_1.py": [
                        2,
                        [1, 10, 8, 2, 5, "80.00000", 6, 7, 9, 8, 20, 40, 13],
                        [[0, 10, 8, 2, 0, "80.00000", 0, 0, 0, 0, 0, 0, 0]],
                        [0, 2, 1, 1, 0, "50.00000", 0, 0, 0, 0, 0, 0, 0],
                    ],
                    "test_file_2.py": [
                        0,
                        [1, 3, 2, 1, 0, "66.66667", 0, 0, 0, 0, 0, 0, 0],
                        [[0, 3, 2, 1, 0, "66.66667", 0, 0, 0, 0, 0, 0, 0]],
                        None,
                    ],
                },
                "sessions": {},
            },
        )

        expected_commit_payload = CommitWithFileLevelReportSerializer(commit).data

        response = self._retrieve()
        assert response.status_code == 200
        assert (
            response.data["latest_commit"]["report"]["totals"]
            == expected_commit_payload["report"]["totals"]
        )
        self.assertEqual(
            response.data["latest_commit"]["report"]["files"],
            [
                {
                    "name": "test_file_1.py",
                    "totals": {
                        "files": 1,
                        "lines": 10,
                        "hits": 8,
                        "misses": 2,
                        "partials": 5,
                        "coverage": 80.0,
                        "branches": 6,
                        "methods": 7,
                        "sessions": 8,
                        "complexity": 20.0,
                        "complexity_total": 40.0,
                        "complexity_ratio": 50.0,
                        "diff": 0,
                    },
                },
                {
                    "name": "test_file_2.py",
                    "totals": {
                        "files": 1,
                        "lines": 3,
                        "hits": 2,
                        "misses": 1,
                        "partials": 0,
                        "coverage": 66.66,
                        "branches": 0,
                        "methods": 0,
                        "sessions": 0,
                        "complexity": 0,
                        "complexity_total": 0,
                        "complexity_ratio": 0,
                        "diff": 0,
                    },
                },
            ],
        )

    @patch("shared.api_archive.archive.ArchiveService.read_chunks", lambda obj, _: "")
    def test_retrieve_returns_latest_commit_of_default_branch_if_branch_not_specified(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True

        commit = CommitFactory(repository=self.repo)
        more_recent_commit = CommitFactory(repository=self.repo, branch="other-branch")

        response = self._retrieve()

        assert response.data["latest_commit"]["commitid"] == commit.commitid
        assert response.data["latest_commit"]["commitid"] != more_recent_commit.commitid

    @patch("shared.api_archive.archive.ArchiveService.read_chunks", lambda obj, _: "")
    def test_retrieve_accepts_branch_query_param_to_specify_latest_commit(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True

        commit = CommitFactory(repository=self.repo, branch="other-branch")
        more_recent_commit = CommitFactory(repository=self.repo)

        response = self._retrieve(data={"branch": "other-branch"})

        assert response.data["latest_commit"]["commitid"] == commit.commitid
        assert response.data["latest_commit"]["commitid"] != more_recent_commit.commitid

    @patch("shared.api_archive.archive.ArchiveService.read_chunks", lambda obj, _: "")
    def test_latest_commit_is_none_if_dne(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True

        response = self._retrieve()

        assert response.data["latest_commit"] is None

    def test_can_retrieve_repo_name_containing_dot(self, mocked_get_permissions):
        mocked_get_permissions.return_value = True, True

        self.repo.name = "codecov.io"
        self.repo.save()

        response = self._retrieve()
        self.assertEqual(response.status_code, 200)

    # Note (Matt): the only special char that github isn't
    # filtering is .
    def test_can_retrieve_repo_name_containing_special_char(
        self, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True
        self.repo.name = "codec@v.i"
        self.repo.save()

        response = self._retrieve()
        self.assertEqual(response.status_code, 200)

    def test_permissions_check_handles_torngit_error(self, mocked_get_permissions):
        err_code, err_message = 403, "yo, no."
        mocked_get_permissions.side_effect = TorngitClientGeneralError(
            err_code, message=err_message, response_data=None
        )
        response = self._retrieve()
        assert response.status_code == err_code
        assert response.data == {"detail": err_message}

    @patch("api.shared.repo.repository_accessors.RepoAccessors.get_repo_details")
    def test_get_object_handles_torngit_error(
        self, mocked_get_details, mocked_get_perms
    ):
        mocked_get_perms.return_value = True, True
        err_code, err_message = 403, "yo, no."
        mocked_get_details.side_effect = TorngitClientGeneralError(
            status_code=err_code, message=err_message, response_data=None
        )
        response = self._retrieve()
        assert response.status_code == err_code
        assert response.data == {"detail": err_message}

    @patch("api.shared.repo.repository_accessors.RepoAccessors.get_repo_details")
    @patch(
        "api.shared.repo.repository_accessors.RepoAccessors.fetch_from_git_and_create_repo"
    )
    def test_create_repo_on_fetch_if_dne(
        self, mocked_fetch_and_create, mocked_get_repo_details, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True
        mocked_get_repo_details.return_value = None
        mocked_fetch_and_create.return_value = self.repo

        response = self._retrieve(
            kwargs={
                "service": self.org.service,
                "owner_username": self.org.username,
                "repo_name": self.repo.name,
            }
        )
        mocked_fetch_and_create.assert_called()
        mocked_fetch_and_create.assert_called()
        self.assertEqual(response.status_code, 200)

    @patch("api.shared.repo.repository_accessors.RepoAccessors.get_repo_details")
    @patch(
        "api.shared.repo.repository_accessors.RepoAccessors.fetch_from_git_and_create_repo"
    )
    def test_unable_to_fetch_git_repo(
        self, mocked_fetch_and_create, mocked_get_repo_details, mocked_get_permissions
    ):
        mocked_get_permissions.return_value = True, True
        mocked_get_repo_details.return_value = None
        mocked_fetch_and_create.side_effect = TorngitClientGeneralError(
            403, response_data=None, message="Forbidden"
        )

        response = self._retrieve(
            kwargs={
                "service": self.org.service,
                "owner_username": self.org.username,
                "repo_name": "new-repo",
            }
        )
        mocked_fetch_and_create.assert_called()
        mocked_fetch_and_create.assert_called()
        self.assertEqual(response.status_code, 403)

    def test_fetch_repo_with_fork_doesnt_crash(self, mocked_get_perms):
        mocked_get_perms.return_value = True, True
        author = OwnerFactory()
        repo = RepositoryFactory(author=author, fork=RepositoryFactory())
        self._retrieve(
            kwargs={
                "service": repo.author.service,
                "owner_username": author.username,
                "repo_name": repo.name,
            }
        )
