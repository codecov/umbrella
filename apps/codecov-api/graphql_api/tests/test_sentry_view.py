import json
import time
from unittest.mock import patch

import jwt
import pytest
from django.conf import settings
from django.test import TestCase

from codecov_auth.models import Owner
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory


@pytest.mark.django_db
class TestSentryAriadneView(TestCase):
    def setUp(self):
        self.mock_owner = self._create_mock_owner()
        self.valid_jwt_token = self._create_valid_jwt_token()
        self.query = """
            query CurrentUser { me  {owner {username}} }
        """

    def _create_valid_jwt_token(self):
        payload = {
            "g_o": "sentry_ariadne_check",
            "g_p": "github",
            "exp": int(time.time()) + 3600,  # Expires in 1 hour
            "iat": int(time.time()),  # Issued at current time
            "iss": "https://sentry.io",  # Issuer
        }
        return jwt.encode(payload, settings.SENTRY_JWT_SHARED_SECRET, algorithm="HS256")

    def _create_mock_owner(self):
        owner = OwnerFactory(
            username="sentry_ariadne_check", service="github", service_id="1234567890"
        )
        owner.save()
        return owner

    def do_query(self, query="{ failing }", token=""):
        data = {"query": query}
        headers = {}
        if token:
            headers["HTTP_AUTHORIZATION"] = f"Bearer {token}"

        response = self.client.post(
            "/graphql/sentry/gh",
            data=json.dumps(data),
            content_type="application/json",
            **headers,
        )
        return response

    def test_sentry_ariadne_view_valid_token(self):
        """Test sentry_ariadne_view with valid JWT token"""
        with patch("codecov_auth.middleware.Owner.objects.get") as mock_get:
            mock_get.return_value = self.mock_owner
            response = self.do_query(query=self.query, token=self.valid_jwt_token)

            assert response.status_code == 200
            assert response.json() == {
                "data": {"me": {"owner": {"username": str(self.mock_owner.username)}}}
            }
            mock_get.assert_called_once_with(
                username="sentry_ariadne_check", service="github"
            )

    def test_sentry_ariadne_view_missing_token(self):
        """Test sentry_ariadne_view with missing JWT token"""
        response = self.do_query(query=self.query)

        assert response.status_code == 403
        assert response.content.decode() == "Missing or Invalid Authorization header"

    def test_sentry_ariadne_view_invalid_token(self):
        """Test sentry_ariadne_view with invalid JWT token"""
        response = self.do_query(query=self.query, token="invalid_token")

        assert response.status_code == 403
        assert response.content.decode() == "Invalid JWT token"

    def test_sentry_ariadne_view_expired_token(self):
        """Test sentry_ariadne_view with expired JWT token"""
        payload = {
            "g_o": "sentry_ariadne_check",
            "g_p": "github",
            "exp": int(time.time()) - 3600,  # Expired 1 hour ago
            "iat": int(time.time()),  # Issued at current time
            "iss": "https://sentry.io",  # Issuer
        }
        expired_token = jwt.encode(
            payload, settings.SENTRY_JWT_SHARED_SECRET, algorithm="HS256"
        )

        response = self.do_query(query=self.query, token=expired_token)
        assert response.status_code == 403
        assert response.content.decode() == "JWT token has expired"

    def test_sentry_ariadne_view_owner_creation_error(self):
        """Test sentry_ariadne_view when owner does not exist"""
        with patch("codecov_auth.middleware.Owner.objects.get") as mock_get_or_create:
            mock_get_or_create.side_effect = Owner.DoesNotExist

            response = self.do_query(query=self.query, token=self.valid_jwt_token)

            assert response.status_code == 404
            assert response.content.decode() == "Account not found"

    def test_sentry_ariadne_view_missing_exp(self):
        """Test sentry_ariadne_view with JWT token missing expiration time"""
        payload = {
            "g_o": "sentry_ariadne_check",
            "g_p": "github",
            "iat": int(time.time()),
            "iss": "https://sentry.io",
        }
        token = jwt.encode(
            payload, settings.SENTRY_JWT_SHARED_SECRET, algorithm="HS256"
        )

        response = self.do_query(query=self.query, token=token)
        assert response.status_code == 403
        assert response.content.decode() == "Invalid JWT token"
