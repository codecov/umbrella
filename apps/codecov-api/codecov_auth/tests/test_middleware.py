import time
from unittest.mock import MagicMock, patch

import jwt
import pytest
from django.conf import settings
from django.http import HttpResponseForbidden
from django.test import RequestFactory

from codecov_auth.middleware import (
    jwt_middleware,
)
from codecov_auth.models import Owner


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
            "g_u": "123",
            "g_p": "github",
            "exp": int(time.time()) + 3600,  # Expires in 1 hour
            "iat": int(time.time()),  # Issued at current time
            "iss": "sentry",  # Issuer
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )


@pytest.fixture
def mock_owner():
    owner = MagicMock(spec=Owner)
    owner.service = "github"
    owner.service_id = "123"
    return owner


# Sentry JWT Middleware tests
@pytest.mark.asyncio
async def test_sentry_jwt_no_auth_header(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior when no Authorization header is present"""
    request = request_factory.get("/")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Missing or invalid Authorization header"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_invalid_auth_format(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with invalid Authorization header format"""
    request = request_factory.get("/", HTTP_AUTHORIZATION="InvalidFormat")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Missing or invalid Authorization header"
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
        "g_u": "123",
        "g_p": "github",
        "exp": int(time.time()) - 3600,  # Expired 1 hour ago
        "iat": int(time.time()) - 7200,  # Issued 2 hours ago
        "iss": "sentry",  # Issuer
    }
    token = jwt.encode(payload, settings.SENTRY_JWT_SHARED_SECRET, algorithm="HS256")
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "JWT token has expired"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_missing_provider_user_id(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with JWT token missing provider_user_id"""
    token = jwt.encode(
        {
            "g_p": "github",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "iss": "sentry",  # Issuer
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Invalid JWT payload"
    assert request.current_owner is None


@pytest.mark.asyncio
async def test_sentry_jwt_missing_provider(
    request_factory, sentry_jwt_middleware_instance
):
    """Test middleware behavior with JWT token missing provider"""
    token = jwt.encode(
        {
            "g_u": "123",
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "iss": "sentry",  # Issuer
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")

    response = await sentry_jwt_middleware_instance(request)

    assert isinstance(response, HttpResponseForbidden)
    assert response.content.decode() == "Invalid JWT payload"
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
async def test_sentry_jwt_valid_token_existing_owner(
    request_factory, sentry_jwt_middleware_instance, valid_jwt_token, mock_owner
):
    """Test middleware behavior with valid JWT token and existing owner"""
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {valid_jwt_token}")

    with patch(
        "codecov_auth.middleware.Owner.objects.get_or_create"
    ) as mock_get_or_create:
        mock_get_or_create.return_value = (mock_owner, False)

        response = await sentry_jwt_middleware_instance(request)

        assert not isinstance(response, HttpResponseForbidden)
        assert request.current_owner == mock_owner
        mock_get_or_create.assert_called_once_with(service_id="123", service="github")


@pytest.mark.asyncio
async def test_sentry_jwt_valid_token_new_owner(
    request_factory, sentry_jwt_middleware_instance, valid_jwt_token, mock_owner
):
    """Test middleware behavior with valid JWT token and new owner creation"""
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {valid_jwt_token}")

    with patch(
        "codecov_auth.middleware.Owner.objects.get_or_create"
    ) as mock_get_or_create:
        mock_get_or_create.return_value = (mock_owner, True)

        response = await sentry_jwt_middleware_instance(request)

        assert not isinstance(response, HttpResponseForbidden)
        assert request.current_owner == mock_owner
        mock_get_or_create.assert_called_once_with(service_id="123", service="github")


@pytest.mark.asyncio
async def test_sentry_jwt_owner_creation_error(
    request_factory, sentry_jwt_middleware_instance, valid_jwt_token
):
    """Test middleware behavior when owner creation fails"""
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {valid_jwt_token}")

    with patch(
        "codecov_auth.middleware.Owner.objects.get_or_create"
    ) as mock_get_or_create:
        mock_get_or_create.side_effect = Exception("Database error")

        response = await sentry_jwt_middleware_instance(request)

        assert isinstance(response, HttpResponseForbidden)
        assert response.content.decode() == "Invalid JWT token"
        assert request.current_owner is None
