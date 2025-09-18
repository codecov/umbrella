import time
from unittest.mock import patch

import jwt
import pytest
from django.conf import settings
from django.http import HttpResponseForbidden, HttpResponseNotFound
from django.test import RequestFactory

from codecov_auth.middleware import (
    jwt_middleware,
)
from codecov_auth.models import Owner
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory, UserFactory


@pytest.fixture
def request_factory():
    return RequestFactory()


@pytest.fixture
def sentry_jwt_middleware_instance():
    async def async_func(x):
        return x

    return jwt_middleware(async_func)


@pytest.fixture
def valid_jwt_token():
    return jwt.encode(
        {
            "g_o": "sentry_middleware_check",
            "g_p": "github",
            "exp": int(time.time()) + 3600,  # Expires in 1 hour
            "iat": int(time.time()),  # Issued at current time
            "iss": "https://sentry.io",  # Issuer
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )


@pytest.fixture
def mock_owner():
    user = UserFactory(name="Sentry Test User", email="sentry@example.com")

    return OwnerFactory(
        username="sentry_middleware_check",
        service="github",
        service_id="123",
        user=user,
    )


# Sentry JWT Middleware tests
@pytest.mark.asyncio
async def test_sentry_jwt_no_auth_header(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior when no Authorization header is present"""
    request = request_factory.get("/")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Missing or Invalid Authorization header"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_invalid_auth_format(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with invalid Authorization header format"""
    request = request_factory.get("/", HTTP_AUTHORIZATION="InvalidFormat")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Missing or Invalid Authorization header"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_invalid_token(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with invalid JWT token"""
    request = request_factory.get("/", HTTP_AUTHORIZATION="Bearer invalid.token.here")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Invalid JWT token"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_expired_token(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with expired JWT token"""
    # Create a token with an expired timestamp
    payload = {
        "g_o": "sentry_middleware_check",
        "g_p": "github",
        "exp": int(time.time()) - 3600,  # Expired 1 hour ago
        "iat": int(time.time()) - 7200,  # Issued 2 hours ago
        "iss": "https://sentry.io",  # Issuer
    }
    token = jwt.encode(payload, settings.SENTRY_JWT_SHARED_SECRET, algorithm="HS256")
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "JWT token has expired"
    assert request.current_owner is None


@pytest.mark.parametrize(
    "key, value",
    [
        ("g_o", "sentry_middleware_check"),
        ("g_p", "github"),
    ],
    ids=["organization", "provider"],
)
@pytest.mark.asyncio
async def test_sentry_jwt_missing_params(
    request_factory, sentry_jwt_middleware_instance, key, value
):
    """Test middleware behavior with missing"""
    token = jwt.encode(
        {
            key: value,
            "exp": int(time.time()) + 3600,  # Expires in 1 hour
            "iat": int(time.time()),  # Issued at current time
            "iss": "https://sentry.io",  # Issuer
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Missing or Invalid Authorization header"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_invalid_issuer(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with invalid issuer"""
    token = jwt.encode(
        {
            "g_o": "sentry_middleware_check",
            "g_p": "github",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "iss": "invalid_issuer",
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Missing or Invalid Authorization header"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_decode_error(request_factory, sentry_jwt_middleware_instance):
    """Test middleware behavior when JWT decode fails"""
    request = request_factory.get("/", HTTP_AUTHORIZATION="Bearer invalid.token.here")

    with patch("codecov_auth.middleware.jwt.decode") as mock_decode:
        mock_decode.side_effect = jwt.InvalidTokenError("Invalid token")

        response = await sentry_jwt_middleware_instance(request)

        assert isinstance(response, HttpResponseForbidden)
        assert response.content.decode() == "Invalid JWT token"
        assert request.current_owner is None


@pytest.mark.asyncio
@pytest.mark.django_db
async def test_sentry_jwt_valid_token_existing_owner(
    request_factory, sentry_jwt_middleware_instance, valid_jwt_token, mock_owner
):
    """Test middleware behavior with valid JWT token and existing owner"""
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {valid_jwt_token}")

    with patch(
        "codecov_auth.middleware.Owner.objects.select_related"
    ) as mock_select_related:
        mock_queryset = mock_select_related.return_value
        mock_queryset.get.return_value = mock_owner

        response = await sentry_jwt_middleware_instance(request)

        assert not isinstance(response, HttpResponseForbidden)
        assert not isinstance(response, HttpResponseNotFound)
        assert request.current_owner == mock_owner

        assert request.current_owner.user is not None
        assert request.current_owner.user.name == "Sentry Test User"
        assert request.current_owner.user.email == "sentry@example.com"

        mock_select_related.assert_called_once_with("user")
        mock_queryset.get.assert_called_once_with(
            username="sentry_middleware_check", service="github"
        )


@pytest.mark.asyncio
async def test_sentry_jwt_valid_token_missing_owner(
    request_factory, sentry_jwt_middleware_instance, valid_jwt_token
):
    """Test middleware behavior with valid JWT token and missing owner"""
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {valid_jwt_token}")

    with patch(
        "codecov_auth.middleware.Owner.objects.select_related"
    ) as mock_select_related:
        mock_queryset = mock_select_related.return_value
        mock_queryset.get.side_effect = Owner.DoesNotExist

        response = await sentry_jwt_middleware_instance(request)

        assert isinstance(response, HttpResponseNotFound)
        assert response.content.decode() == "Account not found"
        assert request.current_owner is None
        mock_select_related.assert_called_once_with("user")
        mock_queryset.get.assert_called_once_with(
            username="sentry_middleware_check", service="github"
        )
