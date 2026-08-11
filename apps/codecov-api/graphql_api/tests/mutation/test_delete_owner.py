import asyncio
from unittest.mock import patch

from django.test import TestCase

from graphql_api.tests.helper import GraphQLTestHelper
from shared.django_apps.core.tests.factories import OwnerFactory
from shared.plan.constants import PlanName

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

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    def test_mutation_deletes_personal_account(self, task_service_mock):
        input = {"username": self.owner.username}
        data = self.gql_request(query, owner=self.owner, variables={"input": input})

        assert data["deleteOwner"] is None
        task_service_mock.return_value.delete_owner.assert_called_once_with(
            ownerid=self.owner.ownerid
        )

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    def test_mutation_unauthorized_for_other_owner(self, task_service_mock):
        other_owner = OwnerFactory(username="someone-else", service="github")
        input = {"username": other_owner.username}
        data = self.gql_request(query, owner=self.owner, variables={"input": input})

        assert data["deleteOwner"]["error"]["__typename"] == "UnauthorizedError"
        task_service_mock.return_value.delete_owner.assert_not_called()

    @patch("codecov_auth.commands.owner.interactors.delete_owner.TaskService")
    def test_mutation_validation_error_for_active_subscription(self, task_service_mock):
        self.owner.plan = PlanName.CODECOV_PRO_MONTHLY.value
        self.owner.stripe_subscription_id = "sub_123"
        self.owner.save()
        input = {"username": self.owner.username}
        data = self.gql_request(query, owner=self.owner, variables={"input": input})

        assert data["deleteOwner"]["error"]["__typename"] == "ValidationError"
        task_service_mock.return_value.delete_owner.assert_not_called()
