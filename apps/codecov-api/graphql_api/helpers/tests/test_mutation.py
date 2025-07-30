from unittest.mock import Mock

from asgiref.sync import sync_to_async
from django.test import SimpleTestCase

from codecov.commands.exceptions import (
    NotFound,
    Unauthenticated,
    Unauthorized,
    ValidationError,
)
from codecov_auth.constants import USE_SENTRY_APP_INDICATOR

from ..mutation import (
    require_authenticated,
    resolve_union_error_type,
    wrap_error_handling_mutation,
)


class HelperMutationTest(SimpleTestCase):
    async def test_mutation_when_everything_is_good(self):
        @wrap_error_handling_mutation
        @sync_to_async
        def resolver():
            return "5"

        assert await resolver() == "5"

    async def test_mutation_when_unauthenticated_is_raised(self):
        @wrap_error_handling_mutation
        @sync_to_async
        def resolver():
            raise Unauthenticated()

        resolved_value = await resolver()
        assert resolved_value["error"].message == "You are not authenticated"
        graphql_type_error = resolve_union_error_type(resolved_value["error"])
        assert graphql_type_error == "UnauthenticatedError"

    async def test_mutation_when_unauthorized_is_raised(self):
        @wrap_error_handling_mutation
        @sync_to_async
        def resolver():
            raise Unauthorized()

        resolved_value = await resolver()
        assert resolved_value["error"].message == "You are not authorized"
        graphql_type_error = resolve_union_error_type(resolved_value["error"])
        assert graphql_type_error == "UnauthorizedError"

    async def test_mutation_when_validation_is_raised(self):
        @wrap_error_handling_mutation
        @sync_to_async
        def resolver():
            raise ValidationError("wrong data")

        resolved_value = await resolver()
        assert resolved_value["error"].message == "wrong data"
        graphql_type_error = resolve_union_error_type(resolved_value["error"])
        assert graphql_type_error == "ValidationError"

    async def test_mutation_when_not_found_is_raised(self):
        @wrap_error_handling_mutation
        @sync_to_async
        def resolver():
            raise NotFound()

        resolved_value = await resolver()
        assert resolved_value["error"].message == "Cant find the requested resource"
        graphql_type_error = resolve_union_error_type(resolved_value["error"])
        assert graphql_type_error == "NotFoundError"

    async def test_mutation_when_random_exception_is_raised_it_reraise(self):
        @wrap_error_handling_mutation
        @sync_to_async
        def resolver():
            raise AttributeError()

        with self.assertRaises(AttributeError):
            await resolver()


class RequireAuthenticatedTest(SimpleTestCase):
    def setUp(self):
        self.mock_info = Mock()
        self.mock_request = Mock()
        self.mock_info.context = {"request": self.mock_request}

    def test_require_authenticated_with_authenticated_user_passes(self):
        """Test that authenticated users pass (original behavior)"""
        # No Sentry app indicator
        self.mock_request.user.is_authenticated = True

        @require_authenticated
        def test_resolver(instance, info, *args, **kwargs):
            return "success"

        result = test_resolver(None, self.mock_info)
        assert result == "success"

    def test_require_authenticated_unauthenticated_raises_error(self):
        """Test that unauthenticated users are blocked"""
        setattr(self.mock_request, USE_SENTRY_APP_INDICATOR, False)
        self.mock_request.user.is_authenticated = False

        @require_authenticated
        def test_resolver(instance, info, *args, **kwargs):
            return "success"

        with self.assertRaises(Unauthenticated):
            test_resolver(None, self.mock_info)

    def test_require_authenticated_with_sentry_app_and_unauthenticated_user_passes(
        self,
    ):
        """Test that unauthenticated requests with Sentry app indicator pass"""
        setattr(self.mock_request, USE_SENTRY_APP_INDICATOR, True)
        self.mock_request.user.is_authenticated = False

        @require_authenticated
        def test_resolver(instance, info, *args, **kwargs):
            return "success"

        result = test_resolver(None, self.mock_info)
        assert result == "success"
