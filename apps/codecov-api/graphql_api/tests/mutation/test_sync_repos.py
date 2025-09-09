from unittest.mock import AsyncMock, patch

from django.test import TestCase

from codecov.commands.exceptions import Unauthenticated
from graphql_api.tests.helper import GraphQLTestHelper
from shared.django_apps.core.tests.factories import OwnerFactory

query = """
mutation {
  syncRepos {
    isSyncing
    error {
      __typename
      ... on ResolverError {
        message
      }
    }
  }
}
"""


class SyncReposTestCase(GraphQLTestHelper, TestCase):
    def setUp(self):
        self.organization_username = "sample-default-org-username"
        self.organization = OwnerFactory(
            username=self.organization_username, service="github"
        )

    def test_sync_repos_success(self):
        """Test successful sync repos mutation"""
        with patch(
            "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
            new_callable=AsyncMock,
        ) as mock_trigger_sync:
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
        assert response == {"syncRepos": {"isSyncing": True, "error": None}}

    def test_sync_repos_unauthenticated_no_user(self):
        """Test sync repos mutation when no user is provided"""
        response = self.gql_request(query)
        assert response == {
            "syncRepos": {
                "isSyncing": None,
                "error": {
                    "__typename": "UnauthenticatedError",
                    "message": "You are not authenticated",
                },
            }
        }

    def test_sync_repos_trigger_sync_unauthenticated_exception(self):
        """Test sync repos when trigger_sync raises Unauthenticated exception"""
        with patch(
            "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
            new_callable=AsyncMock,
            side_effect=Unauthenticated(),
        ) as mock_trigger_sync:
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
        assert response == {
            "syncRepos": {
                "isSyncing": None,
                "error": {
                    "__typename": "UnauthenticatedError",
                    "message": "You are not authenticated",
                },
            }
        }
