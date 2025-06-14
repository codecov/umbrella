import hmac
from hashlib import sha256
from unittest.mock import patch

from django.urls import reverse
from rest_framework import status
from rest_framework.test import APITestCase

from shared.django_apps.codecov_auth.tests.factories import GithubAppInstallationFactory
from shared.django_apps.core.tests.factories import OwnerFactory

PAYLOAD_SECRET = b"testixik8qdauiab1yiffydimvi72ekq"
VIEW_URL = reverse("auth")


def sign_payload(data: bytes, secret=PAYLOAD_SECRET):
    signature = "sha256=" + hmac.new(secret, data, digestmod=sha256).hexdigest()
    return signature, data


class GenAIAuthViewTests(APITestCase):
    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_missing_parameters(self, mock_config):
        payload = b"{}"
        sig, data = sign_payload(payload)
        response = self.client.post(
            VIEW_URL,
            data=data,
            content_type="application/json",
            HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=sig,
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Missing required parameters", response.data)

    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_invalid_signature(self, mock_config):
        # Correct payload
        payload = b'{"external_owner_id":"owner1","repo_service_id":"101"}'
        # Wrong signature based on a different payload
        wrong_sig = "sha256=" + hmac.new(PAYLOAD_SECRET, b"{}", sha256).hexdigest()
        response = self.client.post(
            VIEW_URL,
            data=payload,
            content_type="application/json",
            HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=wrong_sig,
        )
        self.assertEqual(response.status_code, 403)

    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_owner_not_found(self, mock_config):
        payload = b'{"external_owner_id":"nonexistent_owner","repo_service_id":"101"}'
        sig, data = sign_payload(payload)
        response = self.client.post(
            VIEW_URL,
            data=data,
            content_type="application/json",
            HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=sig,
        )
        self.assertEqual(response.status_code, 404)

    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_no_installation(self, mock_config):
        # Create a valid owner but no installation
        OwnerFactory(service="github", service_id="owner1", username="test1")
        payload = b'{"external_owner_id":"owner1","repo_service_id":"101"}'
        sig, data = sign_payload(payload)
        response = self.client.post(
            VIEW_URL,
            data=data,
            content_type="application/json",
            HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=sig,
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, {"is_valid": False})

    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_authorized(self, mock_config):
        with patch("api.gen_ai.views.AI_FEATURES_GH_APP_ID", 12345):
            owner = OwnerFactory(
                service="github", service_id="owner2", username="test2"
            )
            GithubAppInstallationFactory(
                installation_id=12345,
                owner=owner,
                name="ai-features",
                repository_service_ids=["101", "202"],
                app_id=12345,
            )
            payload = b'{"external_owner_id":"owner2","repo_service_id":"101"}'
            sig, data = sign_payload(payload)
            response = self.client.post(
                VIEW_URL,
                data=data,
                content_type="application/json",
                HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=sig,
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK)
            self.assertEqual(response.data, {"is_valid": True})

    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_unauthorized(self, mock_config):
        with patch("api.gen_ai.views.AI_FEATURES_GH_APP_ID", 12345):
            owner = OwnerFactory(
                service="github", service_id="owner3", username="test3"
            )
            GithubAppInstallationFactory(
                installation_id=2,
                owner=owner,
                name="ai-features",
                repository_service_ids=["303", "404"],
                app_id=12345,
            )
            payload = b'{"external_owner_id":"owner3","repo_service_id":"101"}'
            sig, data = sign_payload(payload)
            response = self.client.post(
                VIEW_URL,
                data=data,
                content_type="application/json",
                HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=sig,
            )
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.data, {"is_valid": False})

    @patch("api.gen_ai.views.get_config", return_value=PAYLOAD_SECRET)
    def test_non_github_service(self, mock_config):
        with patch("api.gen_ai.views.AI_FEATURES_GH_APP_ID", 12345):
            owner = OwnerFactory(
                service="gitlab", service_id="owner4", username="test4"
            )
            GithubAppInstallationFactory(
                installation_id=12345,
                owner=owner,
                name="ai-features",
                repository_service_ids=["101", "202"],
                app_id=12345,
            )
            payload = b'{"external_owner_id":"owner4","repo_service_id":"101"}'
            sig, data = sign_payload(payload)
            response = self.client.post(
                VIEW_URL,
                data=data,
                content_type="application/json",
                HTTP_HTTP_X_GEN_AI_AUTH_SIGNATURE=sig,
            )
            self.assertEqual(response.status_code, 404)
