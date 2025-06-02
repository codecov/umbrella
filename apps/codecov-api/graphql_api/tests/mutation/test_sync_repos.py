from unittest.mock import AsyncMock, patch

from django.test import TestCase

from graphql_api.tests.helper import GraphQLTestHelper
from shared.django_apps.core.tests.factories import OwnerFactory

query = """
mutation {
  syncRepos {
    isSyncing
  }
}
"""


class SyncReposTestCase(GraphQLTestHelper, TestCase):
    def setUp(self):
        self.organization_username = "sample-default-org-username"
        self.organization = OwnerFactory(
            username=self.organization_username, service="github"
        )

    def test_sync_repos(self):
        with patch(
            "codecov_auth.commands.owner.owner.OwnerCommands.trigger_sync",
            new_callable=AsyncMock,
        ):
            response = self.gql_request(
                query,
                owner=self.organization,
            )
        assert response == {"syncRepos": {"isSyncing": True}}
