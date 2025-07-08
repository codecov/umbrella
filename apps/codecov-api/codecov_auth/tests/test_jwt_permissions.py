import time

import jwt
import pytest
from django.conf import settings
from rest_framework.test import APIRequestFactory

from codecov_auth.permissions import JWTAuthenticationPermission


@pytest.fixture
def request_factory():
    return APIRequestFactory()


@pytest.fixture
def valid_jwt_token():
    return jwt.encode(
        {
            "exp": int(time.time()) + 3600,  # Expires in 1 hour
            "iat": int(time.time()),  # Issued at current time
            "iss": "https://sentry.io",  # Issuer
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )


def test_valid_token(request_factory, valid_jwt_token):
    """Test that a valid JWT token is accepted"""
    request = request_factory.get(
        "/",
        HTTP_AUTHORIZATION=f"Bearer {valid_jwt_token}",
    )
    permission = JWTAuthenticationPermission()
    assert permission.has_permission(request, None) is True
    assert request.jwt_payload is not None


def test_missing_auth_header(request_factory):
    """Test that missing Authorization header is rejected"""
    request = request_factory.get("/")
    permission = JWTAuthenticationPermission()
    assert permission.has_permission(request, None) is False


def test_invalid_auth_format(request_factory):
    """Test that invalid Authorization header format is rejected"""
    request = request_factory.get("/", HTTP_AUTHORIZATION="InvalidFormat")
    permission = JWTAuthenticationPermission()
    assert permission.has_permission(request, None) is False


def test_expired_token(request_factory):
    """Test that expired token is rejected"""
    token = jwt.encode(
        {
            "exp": int(time.time()) - 3600,  # Expired 1 hour ago
            "iat": int(time.time()) - 7200,  # Issued 2 hours ago
            "iss": "https://sentry.io",
        },
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")
    permission = JWTAuthenticationPermission()
    assert permission.has_permission(request, None) is False


def test_missing_required_claims(request_factory):
    """Test that token missing required claims is rejected"""
    token = jwt.encode(
        {},
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithm="HS256",
    )
    request = request_factory.get("/", HTTP_AUTHORIZATION=f"Bearer {token}")
    permission = JWTAuthenticationPermission()
    assert permission.has_permission(request, None) is False
