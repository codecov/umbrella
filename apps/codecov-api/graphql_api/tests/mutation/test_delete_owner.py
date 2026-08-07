import asyncio
from unittest.mock import patch

from django.test import TestCase

from graphql_api.tests.helper import GraphQLTestHelper
from shared.django_apps.core.tests.factories import OwnerFactory

query = """
mutation($input: DeleteOwnerInput!) {
  deleteOwner(input: $input) {
    error {
      __typename
    }
  }
}
"""


class DeleteOwnerMutationTest(GraphQLTestHelper, TestCase):
    def setUp(self):
        self.owner = OwnerFactory(username="codecov-user", service="github")
        asyncio.set_event_loop(asyncio.new_event_loop())

    @patch("codecov_auth.commands.owner.owner.OwnerCommands.delete_owner")
    def test_mutation_dispatch_to_command(self, command_mock):
        f = asyncio.Future()
        f.set_result(None)
        command_mock.return_value = f

        input = {"username": self.owner.username}
        data = self.gql_request(query, owner=self.owner, variables={"input": input})

        command_mock.assert_called_once_with(username=self.owner.username)
        assert data["deleteOwner"] is None or data["deleteOwner"]["error"] is None

    def test_mutation_when_unauthenticated(self):
        input = {"username": self.owner.username}
        data = self.gql_request(query, variables={"input": input})
        assert data["deleteOwner"]["error"]["__typename"] == "UnauthenticatedError"

    @patch(
        "codecov_auth.commands.owner.interactors.delete_owner.TaskService.delete_owner"
    )
    def test_mutation_deletes_personal_account(self, delete_owner_mock):
        input = {"username": self.owner.username}
        data = self.gql_request(query, owner=self.owner, variables={"input": input})

        assert data["deleteOwner"]["error"] is None
        delete_owner_mock.assert_called_once_with(ownerid=self.owner.ownerid)

    @patch(
        "codecov_auth.commands.owner.interactors.delete_owner.TaskService.delete_owner"
    )
    def test_mutation_unauthorized_for_other_owner(self, delete_owner_mock):
        other_owner = OwnerFactory(username="someone-else", service="github")
        input = {"username": other_owner.username}
        data = self.gql_request(query, owner=self.owner, variables={"input": input})

        assert data["deleteOwner"]["error"]["__typename"] == "UnauthorizedError"
        delete_owner_mock.assert_not_called()
