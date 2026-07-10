from django.test import TestCase

from graphql_api.tests.helper import GraphQLTestHelper
from shared.django_apps.core.tests.factories import OwnerFactory

query = """
mutation {
  regenerateSupportPin {
    me {
      supportPin
    }
    error {
      __typename
    }
  }
}
"""


class RegenerateSupportPinTests(GraphQLTestHelper, TestCase):
    def setUp(self):
        self.owner = OwnerFactory(username="codecov", support_pin="000000")

    def test_when_unauthenticated(self):
        data = self.gql_request(query)
        assert (
            data["regenerateSupportPin"]["error"]["__typename"]
            == "UnauthenticatedError"
        )

    def test_when_authenticated_regenerates_pin(self):
        data = self.gql_request(query, owner=self.owner)

        new_pin = data["regenerateSupportPin"]["me"]["supportPin"]
        assert data["regenerateSupportPin"]["error"] is None
        assert new_pin != "000000"
        assert len(new_pin) == 6
        assert new_pin.isdigit()

        self.owner.refresh_from_db()
        assert self.owner.support_pin == new_pin
