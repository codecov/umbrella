from unittest.mock import patch

from django.test import TestCase, override_settings

from .helper import GraphQLTestHelper


class TestConfigType(GraphQLTestHelper, TestCase):
    @override_settings(
        GITHUB_CLIENT_ID="Github",
        GITHUB_ENTERPRISE_CLIENT_ID="Github Enterprise",
        GITLAB_CLIENT_ID="Gitlab",
        GITLAB_ENTERPRISE_CLIENT_ID="Gitlab Enterprise",
        BITBUCKET_CLIENT_ID="Bitbucket",
        BITBUCKET_SERVER_CLIENT_ID="Bitbucket Server",
        OKTA_OAUTH_CLIENT_ID="Okta",
    )
    def test_login_providers(self):
        data = self.gql_request("query { config { loginProviders }}")
        assert data == {
            "config": {
                "loginProviders": [
                    "GITHUB",
                    "GITHUB_ENTERPRISE",
                    "GITLAB",
                    "GITLAB_ENTERPRISE",
                    "BITBUCKET",
                    "BITBUCKET_SERVER",
                    "OKTA",
                ],
            },
        }

    @override_settings(
        GITHUB_CLIENT_ID="Github",
        GITHUB_ENTERPRISE_CLIENT_ID="Github Enterprise",
        OKTA_OAUTH_CLIENT_ID="Okta",
        DISABLE_GIT_BASED_LOGIN=True,
    )
    def test_login_providers_no_git(self):
        data = self.gql_request("query { config { loginProviders }}")
        assert data == {
            "config": {
                "loginProviders": [
                    "OKTA",
                ],
            },
        }

    def test_seats_used(self):
        data = self.gql_request("query { config { seatsUsed }}")
        assert data == {
            "config": {
                "seatsUsed": None,
            },
        }

    @override_settings(IS_ENTERPRISE=True)
    @patch("services.self_hosted.activated_owners")
    def test_seats_used_self_hosted(self, activated_owners):
        activated_owners.count.return_value = 1
        data = self.gql_request("query { config { seatsUsed }}")
        assert data == {
            "config": {
                "seatsUsed": 1,
            },
        }

    def test_plan_auto_activate(self):
        data = self.gql_request("query { config { planAutoActivate }}")
        assert data == {
            "config": {
                "planAutoActivate": None,
            },
        }

    @override_settings(IS_ENTERPRISE=True)
    @patch("services.self_hosted.is_autoactivation_enabled")
    def test_plan_auto_activate_self_hosted(self, is_autoactivation_enabled):
        is_autoactivation_enabled.return_value = True

        data = self.gql_request("query { config { planAutoActivate }}")
        assert data == {
            "config": {
                "planAutoActivate": True,
            },
        }

    def test_seats_limit(self):
        data = self.gql_request("query { config { seatsLimit }}")
        assert data == {
            "config": {
                "seatsLimit": None,
            },
        }

    @override_settings(IS_ENTERPRISE=True)
    def test_seats_limit_self_hosted(self):
        """Enterprise deployments have no seat limits."""
        data = self.gql_request("query { config { seatsLimit }}")
        assert data == {
            "config": {
                "seatsLimit": None,
            },
        }

    @override_settings(TIMESERIES_ENABLED=True)
    def test_timeseries_enabled(self):
        data = self.gql_request("query { config { isTimescaleEnabled }}")
        assert data == {
            "config": {
                "isTimescaleEnabled": True,
            },
        }

    @override_settings(TIMESERIES_ENABLED=False)
    def test_timeseries_enabled_is_false(self):
        data = self.gql_request("query { config { isTimescaleEnabled }}")
        assert data == {
            "config": {
                "isTimescaleEnabled": False,
            },
        }

    @override_settings(TIMESERIES_ENABLED="true")
    def test_timeseries_enabled_is_true_string(self):
        data = self.gql_request("query { config { isTimescaleEnabled }}")
        assert data == {
            "config": {
                "isTimescaleEnabled": True,
            },
        }

    @override_settings(TIMESERIES_ENABLED="false")
    def test_timeseries_enabled_is_false_string(self):
        data = self.gql_request("query { config { isTimescaleEnabled }}")
        assert data == {
            "config": {
                "isTimescaleEnabled": False,
            },
        }

    @override_settings(IS_ENTERPRISE=True, ADMINS_LIST=[])
    def test_has_admins_empty_admins_list(self):
        data = self.gql_request("query { config { hasAdmins }}")
        assert data == {
            "config": {
                "hasAdmins": False,
            },
        }

    @override_settings(IS_ENTERPRISE=False)
    def test_has_admins_enterprise_is_false(self):
        data = self.gql_request("query { config { hasAdmins }}")
        assert data == {
            "config": {
                "hasAdmins": None,
            },
        }

    @override_settings(
        IS_ENTERPRISE=True, ADMINS_LIST=[{"service": "github", "username": "Imogen"}]
    )
    def test_has_admins_with_enterprise_and_admins(self):
        data = self.gql_request("query { config { hasAdmins }}")
        assert data == {
            "config": {
                "hasAdmins": True,
            },
        }

    @override_settings(
        IS_ENTERPRISE=True,
        GITHUB_ENTERPRISE_CLIENT_ID="Github",
        GITHUB_ENTERPRISE_URL="https://github.example.com",
    )
    def test_resolve_github_enterprise_url(self):
        data = self.gql_request("query { config { githubEnterpriseURL }}")
        assert data == {
            "config": {
                "githubEnterpriseURL": "https://github.example.com",
            },
        }

    @override_settings(
        IS_ENTERPRISE=False,
    )
    def test_resolve_null_github_enterprise_url(self):
        data = self.gql_request("query { config { githubEnterpriseURL }}")
        assert data == {
            "config": {
                "githubEnterpriseURL": None,
            },
        }

    @override_settings(
        IS_ENTERPRISE=True,
        GITLAB_ENTERPRISE_CLIENT_ID="Gitlab",
        GITLAB_ENTERPRISE_URL="https://gitlab.example.com",
    )
    def test_resolve_gitlab_enterprise_url(self):
        data = self.gql_request("query { config { gitlabEnterpriseURL }}")
        assert data == {
            "config": {
                "gitlabEnterpriseURL": "https://gitlab.example.com",
            },
        }

    @override_settings(
        IS_ENTERPRISE=False,
    )
    def test_resolve_null_gitlab_enterprise_url(self):
        data = self.gql_request("query { config { gitlabEnterpriseURL }}")
        assert data == {
            "config": {
                "gitlabEnterpriseURL": None,
            },
        }

    @override_settings(
        IS_ENTERPRISE=True,
        BITBUCKET_SERVER_CLIENT_ID="Bitbucket",
        BITBUCKET_SERVER_URL="https://bitbucket.example.com",
    )
    def test_resolve_bitbucket_server_url(self):
        data = self.gql_request("query { config { bitbucketServerURL }}")
        assert data == {
            "config": {
                "bitbucketServerURL": "https://bitbucket.example.com",
            },
        }

    @override_settings(
        IS_ENTERPRISE=False,
    )
    def test_resolve_null_bitbucket_sever_url(self):
        data = self.gql_request("query { config { bitbucketServerURL }}")
        assert data == {
            "config": {
                "bitbucketServerURL": None,
            },
        }

    @override_settings(IS_ENTERPRISE=False)
    def test_self_hosted_license_returns_null_if_not_enterprise(self):
        data = self.gql_request(
            "query { config { selfHostedLicense { expirationDate } } }"
        )
        assert data == {
            "config": {
                "selfHostedLicense": None,
            },
        }

    @override_settings(IS_ENTERPRISE=True)
    def test_self_hosted_license_returns_valid_for_enterprise(self):
        """Enterprise deployments always have a valid license (no enforcement)."""
        data = self.gql_request(
            "query { config { selfHostedLicense { expirationDate } } }"
        )
        # License is always valid, but expires is None (no expiration)
        assert data == {
            "config": {
                "selfHostedLicense": {"expirationDate": None},
            },
        }

    @override_settings(
        GITHUB_CLIENT_ID="Github",
        GITHUB_ENTERPRISE_CLIENT_ID="Github Enterprise",
        GITLAB_CLIENT_ID="Gitlab",
        GITLAB_ENTERPRISE_CLIENT_ID="Gitlab Enterprise",
        BITBUCKET_CLIENT_ID="Bitbucket",
        BITBUCKET_SERVER_CLIENT_ID="Bitbucket Server",
        OKTA_OAUTH_CLIENT_ID="Okta",
        DISABLE_GIT_BASED_LOGIN=True,
    )
    def test_sync_providers(self):
        data = self.gql_request("query { config { syncProviders } }")
        assert data == {
            "config": {
                "syncProviders": [
                    "GITHUB",
                    "GITHUB_ENTERPRISE",
                    "GITLAB",
                    "GITLAB_ENTERPRISE",
                    "BITBUCKET",
                    "BITBUCKET_SERVER",
                ],
            },
        }
