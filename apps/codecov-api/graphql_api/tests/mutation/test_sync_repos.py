import asyncio
from unittest.mock import AsyncMock, Mock, patch

from django.test import TestCase
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError

from codecov.commands.exceptions import Unauthenticated
from graphql_api.tests.helper import GraphQLTestHelper
from graphql_api.types.mutation.sync_repos.sync_repos import resolve_sync_repos
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
        with (
            patch(
                "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
                new_callable=AsyncMock,
            ) as mock_trigger_sync,
            patch(
                "graphql_api.types.mutation.sync_repos.sync_repos.RefreshService.is_refreshing",
                return_value=True,
            ) as mock_is_refreshing,
        ):
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
            mock_is_refreshing.assert_called_once_with(self.organization.ownerid)
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

    def test_sync_repos_null_current_owner(self):
        """Test sync repos mutation when current_owner is None"""
        mock_info = Mock()
        mock_info.context = {"request": Mock(current_owner=None), "executor": Mock()}

        mock_executor = Mock()
        mock_command = Mock()
        mock_command.trigger_sync = AsyncMock()
        mock_executor.get_command.return_value = mock_command
        mock_info.context["executor"] = mock_executor

        result = asyncio.run(resolve_sync_repos(None, mock_info))

        mock_command.trigger_sync.assert_called_once_with(using_integration=True)
        assert result == {"is_syncing": False}

    def test_sync_repos_redis_connection_error(self):
        """Test sync repos mutation when Redis connection fails"""
        with (
            patch(
                "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
                new_callable=AsyncMock,
            ) as mock_trigger_sync,
            patch(
                "graphql_api.types.mutation.sync_repos.sync_repos.RefreshService",
                side_effect=RedisConnectionError("Connection failed"),
            ) as mock_refresh_service,
        ):
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
            mock_refresh_service.assert_called_once()

        assert response == {"syncRepos": {"isSyncing": False, "error": None}}

    def test_sync_repos_redis_error(self):
        """Test sync repos mutation when Redis raises generic error"""
        with (
            patch(
                "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
                new_callable=AsyncMock,
            ) as mock_trigger_sync,
            patch(
                "graphql_api.types.mutation.sync_repos.sync_repos.RefreshService",
                side_effect=RedisError("Redis operation failed"),
            ) as mock_refresh_service,
        ):
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
            mock_refresh_service.assert_called_once()

        assert response == {"syncRepos": {"isSyncing": False, "error": None}}

    def test_sync_repos_is_refreshing_false(self):
        """Test sync repos mutation when is_refreshing returns False"""
        with (
            patch(
                "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
                new_callable=AsyncMock,
            ) as mock_trigger_sync,
            patch(
                "graphql_api.types.mutation.sync_repos.sync_repos.RefreshService.is_refreshing",
                return_value=False,
            ) as mock_is_refreshing,
        ):
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
            mock_is_refreshing.assert_called_once_with(self.organization.ownerid)

        assert response == {"syncRepos": {"isSyncing": False, "error": None}}

    @patch("graphql_api.types.mutation.sync_repos.sync_repos.log")
    def test_sync_repos_redis_connection_error_with_logging(self, mock_log):
        """Test sync repos mutation logs properly when Redis connection fails"""
        with (
            patch(
                "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
                new_callable=AsyncMock,
            ) as mock_trigger_sync,
            patch(
                "graphql_api.types.mutation.sync_repos.sync_repos.RefreshService",
                side_effect=RedisConnectionError("Connection failed"),
            ) as mock_refresh_service,
        ):
            response = self.gql_request(
                query,
                owner=self.organization,
            )
            mock_trigger_sync.assert_called_once_with(using_integration=True)
            mock_refresh_service.assert_called_once()

            mock_log.warning.assert_called_once_with(
                "Redis error while checking sync status",
                extra={"ownerid": self.organization.ownerid},
            )

        assert response == {"syncRepos": {"isSyncing": False, "error": None}}
