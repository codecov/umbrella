from unittest.mock import patch

from django.test import TestCase

from graphql_api.tests.helper import GraphQLTestHelper
from shared.django_apps.core.tests.factories import OwnerFactory, RepositoryFactory

query = """
mutation($input: ActivateMeasurementsInput!) {
  activateMeasurements(input: $input) {
    error {
      __typename
    }
  }
}
"""


query = """
mutation UpdateBundleCacheConfig(
    $owner: String!
    $repoName: String!
    $bundles: [BundleCacheConfigInput!]!
) {
    updateBundleCacheConfig(input: {
        owner: $owner,
        repoName: $repoName,
        bundles: $bundles
    }) {
        results {
            bundleName
            isCached
            cacheConfig
        }
        error {
            __typename
            ... on UnauthenticatedError {
                message
            }
            ... on UnauthorizedError {
                message
            }
            ... on ValidationError {
                message
            }
        }
    }
}
"""


class UpdateBundleCacheConfigTestCase(GraphQLTestHelper, TestCase):
    def setUp(self):
        self.org = OwnerFactory(username="codecov")
        self.repo = RepositoryFactory(author=self.org, name="test-repo", private=False)
        self.owner = OwnerFactory(organizations=[self.org.ownerid])

    def test_when_unauthenticated(self):
        data = self.gql_request(
            query,
            variables={
                "owner": "codecov",
                "repoName": "test-repo",
                "bundles": [{"bundleName": "pr_bundle1", "toggleCaching": True}],
            },
        )
        assert (
            data["updateBundleCacheConfig"]["error"]["__typename"]
            == "UnauthenticatedError"
        )

    def test_when_unauthorized_user_not_part_of_org(self):
        random_user = OwnerFactory()
        data = self.gql_request(
            query,
            owner=random_user,
            variables={
                "owner": "codecov",
                "repoName": "test-repo",
                "bundles": [{"bundleName": "pr_bundle1", "toggleCaching": False}],
            },
        )
        assert (
            data["updateBundleCacheConfig"]["error"]["__typename"]
            == "UnauthorizedError"
        )

    @patch(
        "core.commands.repository.interactors.update_bundle_cache_config.UpdateBundleCacheConfigInteractor.execute"
    )
    def test_when_authenticated(self, execute):
        data = self.gql_request(
            query,
            owner=self.owner,
            variables={
                "owner": "codecov",
                "repoName": "test-repo",
                "bundles": [{"bundleName": "pr_bundle1", "toggleCaching": True}],
            },
        )
        assert data == {"updateBundleCacheConfig": {"results": [], "error": None}}
