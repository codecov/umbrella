import json
import time
from unittest.mock import patch
from urllib.parse import urljoin

import jwt
import pytest
import requests
from django.conf import settings
from django.test import RequestFactory, override_settings
from rest_framework import status
from rest_framework.reverse import reverse
from rest_framework.test import APIClient

from billing.tests.mocks import mock_all_plans_and_tiers
from codecov_auth.models import GithubAppInstallation
from codecov_auth.permissions import JWTAuthenticationPermission
from shared.django_apps.codecov_auth.tests.factories import AccountFactory, OwnerFactory
from shared.django_apps.core.tests.factories import RepositoryFactory
from webhook_handlers.constants import GitHubHTTPHeaders


@pytest.fixture(autouse=True)
def sentry_settings():
    with override_settings(SENTRY_JWT_SHARED_SECRET="test-shared-secret"):
        yield


@pytest.fixture
def owner():
    return OwnerFactory(service="github", service_id="12345")


@pytest.fixture
def repo(owner):
    return RepositoryFactory(author=owner, service_id="67890")


@pytest.fixture
def url():
    return reverse("sentry-webhook")


@pytest.fixture
def client():
    return APIClient()


@pytest.fixture
def create_valid_jwt_token():
    payload = {
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
        "iss": "https://sentry.io",
        "g_p": "github",
    }
    return jwt.encode(payload, "test-shared-secret", algorithm="HS256")


@pytest.fixture
def create_expired_jwt_token():
    payload = {
        "exp": int(time.time()) - 3600,
        "iat": int(time.time()) - 7200,
        "iss": "https://sentry.io",
        "g_p": "github",
    }
    return jwt.encode(payload, "test-shared-secret", algorithm="HS256")


@pytest.fixture
def mock_task_service():
    with patch("webhook_handlers.views.github.TaskService") as mock:
        yield mock


@pytest.fixture
def create_invalid_jwt_token():
    payload = {
        "exp": int(time.time()) + 3600,
        "iat": int(time.time()),
        "iss": "https://sentry.io",
        "g_p": "github",
    }
    return jwt.encode(payload, "wrong-secret", algorithm="HS256")


@pytest.fixture
def installation_webhook_payload(owner, repo):
    return {
        "action": "created",
        "installation": {
            "id": 12345,
            "account": {
                "id": owner.service_id,
                "login": owner.username,
            },
            "app_id": 67890,
            "repository_selection": "selected",
        },
        "repositories": [{"id": repo.service_id, "node_id": "some-node-id"}],
        "sender": {"login": "test-user"},
    }


@pytest.fixture
def installation_repositories_webhook_payload(owner, repo):
    return {
        "action": "added",
        "installation": {
            "id": 12345,
            "account": {
                "id": owner.service_id,
                "login": owner.username,
            },
            "app_id": 67890,
        },
        "repository_selection": "selected",
        "repositories_added": [{"id": repo.service_id, "node_id": "some-node-id"}],
        "sender": {"login": "test-user"},
    }


@pytest.fixture
def push_webhook_payload(owner, repo):
    return {
        "ref": "refs/heads/main",
        "repository": {
            "id": repo.service_id,
            "full_name": f"{owner.username}/{repo.name}",
            "owner": {"id": owner.service_id},
        },
        "commits": [{"id": "abc123", "message": "Test commit"}],
    }


def test_permission_class_directly(create_valid_jwt_token, create_invalid_jwt_token):
    factory = RequestFactory()
    token = create_valid_jwt_token

    request = factory.post("/", HTTP_AUTHORIZATION=f"Bearer {token}")
    permission = JWTAuthenticationPermission()
    assert permission.has_permission(request, None)

    invalid_token = create_invalid_jwt_token
    request = factory.post("/", HTTP_AUTHORIZATION=f"Bearer {invalid_token}")
    permission = JWTAuthenticationPermission()
    assert not permission.has_permission(request, None)


@pytest.mark.django_db(databases=["default"], transaction=True)
class TestSentryWebhook:
    @pytest.fixture(autouse=True, scope="function")
    def mock_all_plans_and_tiers_fixture(self):
        mock_all_plans_and_tiers()

    def test_valid_jwt_authentication(
        self,
        client,
        url,
        installation_webhook_payload,
        create_valid_jwt_token,
        mock_task_service,
    ):
        data = installation_webhook_payload

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.data == {"status": "ok"}

    def test_missing_jwt_token(self, client, url, installation_webhook_payload):
        data = installation_webhook_payload

        response = client.post(
            url,
            data=data,
            format="json",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_invalid_authorization_header_format(
        self, client, url, installation_webhook_payload
    ):
        data = installation_webhook_payload

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION="InvalidFormat token",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_invalid_jwt_token(
        self, client, url, installation_webhook_payload, create_invalid_jwt_token
    ):
        data = installation_webhook_payload
        token = create_invalid_jwt_token

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_expired_jwt_token(
        self, client, url, installation_webhook_payload, create_expired_jwt_token
    ):
        data = installation_webhook_payload
        token = create_expired_jwt_token

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_jwt_token_missing_required_claims(
        self, client, url, installation_webhook_payload
    ):
        payload = {
            "exp": int(time.time()) + 3600,
        }
        token = jwt.encode(payload, "test-shared-secret", algorithm="HS256")

        data = installation_webhook_payload
        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_jwt_token_wrong_issuer(self, client, url, installation_webhook_payload):
        payload = {
            "exp": int(time.time()) + 3600,
            "iat": int(time.time()),
            "iss": "https://wrong-issuer.com",
        }
        token = jwt.encode(payload, "test-shared-secret", algorithm="HS256")

        data = installation_webhook_payload
        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_403_FORBIDDEN

    def test_unknown_event_type(self, client, url, create_valid_jwt_token):
        data = {"some": "data"}

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "unknown_event_type"},
        )
        assert response.status_code == status.HTTP_200_OK

    def test_missing_event_field(self, client, url, create_valid_jwt_token):
        data = {"some": "data"}

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_missing_payload_field(self, client, url, create_valid_jwt_token):
        data = {}

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_empty_request_data(self, client, url, create_valid_jwt_token):
        data = {}

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )
        assert response.status_code == status.HTTP_400_BAD_REQUEST

    def test_successful_installation_response_format(
        self,
        client,
        url,
        installation_webhook_payload,
        create_valid_jwt_token,
        mock_task_service,
    ):
        data = installation_webhook_payload
        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_200_OK

    def test_successful_installation_repositories_response_format(
        self,
        client,
        url,
        installation_repositories_webhook_payload,
        create_valid_jwt_token,
        mock_task_service,
    ):
        data = installation_repositories_webhook_payload
        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "installation_repositories"},
        )

        assert response.status_code == status.HTTP_200_OK

    def test_successful_push_response_format(
        self,
        client,
        url,
        push_webhook_payload,
        create_valid_jwt_token,
        mock_task_service,
    ):
        data = push_webhook_payload
        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "push"},
        )

        assert response.status_code == status.HTTP_200_OK

    def test_successful_pull_request_response_format(
        self, client, url, owner, repo, create_valid_jwt_token, mock_task_service
    ):
        data = {
            "action": "opened",
            "number": 123,
            "repository": {
                "id": repo.service_id,
                "full_name": f"{owner.username}/{repo.name}",
                "owner": {"id": owner.service_id},
            },
            "pull_request": {"title": "Test PR"},
        }
        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "pull_request"},
        )

        assert response.status_code == status.HTTP_200_OK

    def test_repository_event_processed_when_owner_has_sentry_account(
        self, client, url
    ):
        account = AccountFactory(sentry_org_id=424242)
        owner = OwnerFactory(service="github", account=account)
        repo = RepositoryFactory(author=owner, private=True, activated=True)
        token = jwt.encode(
            {
                "exp": int(time.time()) + 3600,
                "iat": int(time.time()),
                "iss": "https://sentry.io",
                "g_p": "github",
            },
            "test-shared-secret",
            algorithm="HS256",
        )

        response = client.post(
            url,
            data={
                "action": "publicized",
                "repository": {
                    "id": repo.service_id,
                    "owner": {"id": owner.service_id},
                },
            },
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {token}",
            **{GitHubHTTPHeaders.EVENT: "repository"},
        )

        assert response.status_code == status.HTTP_200_OK
        repo.refresh_from_db()
        assert repo.private is False
        assert repo.activated is False

    def test_repository_event_ignored_when_owner_has_no_sentry_account(
        self, client, url
    ):
        owner = OwnerFactory(service="github")
        repo = RepositoryFactory(author=owner, private=True, activated=True)
        token = jwt.encode(
            {
                "exp": int(time.time()) + 3600,
                "iat": int(time.time()),
                "iss": "https://sentry.io",
                "g_p": "github",
            },
            "test-shared-secret",
            algorithm="HS256",
        )

        response = client.post(
            url,
            data={
                "action": "publicized",
                "repository": {
                    "id": repo.service_id,
                    "owner": {"id": owner.service_id},
                },
            },
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {token}",
            **{GitHubHTTPHeaders.EVENT: "repository"},
        )

        assert response.status_code == status.HTTP_200_OK
        repo.refresh_from_db()
        assert repo.private is True
        assert repo.activated is True

    def test_e2e_valid_jwt_authentication_live_server(
        self,
        live_server,
        installation_webhook_payload,
        create_valid_jwt_token,
        mock_task_service,
    ):
        e2e_url = urljoin(live_server.url, reverse("sentry-webhook"))
        response = requests.post(
            e2e_url,
            data=json.dumps(installation_webhook_payload),
            headers={
                "Authorization": f"Bearer {create_valid_jwt_token}",
                "X-GitHub-Event": "installation",
                "Content-Type": "application/json",
            },
            timeout=5,
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.json() == {"status": "ok"}

    @pytest.mark.django_db(databases=["default"], transaction=True)
    @override_settings(
        GITHUB_SENTRY_APP_ID=98765,
        GITHUB_SENTRY_APP_NAME="sentry-test-app",
        GITHUB_SENTRY_APP_PEM="/path/to/sentry/pem",
    )
    def test_sentry_app_installation_settings_applied(
        self,
        client,
        url,
        owner,
        repo,
        create_valid_jwt_token,
        mock_task_service,
        installation_webhook_payload,
    ):
        installation_webhook_payload["installation"]["app_id"] = (
            settings.GITHUB_SENTRY_APP_ID
        )
        data = installation_webhook_payload

        response = client.post(
            url,
            data=data,
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "installation"},
        )

        assert response.status_code == status.HTTP_200_OK
        assert response.data == {"status": "ok"}

        ghapp_installation = GithubAppInstallation.objects.get(
            installation_id=12345, app_id=settings.GITHUB_SENTRY_APP_ID
        )
        assert ghapp_installation.app_id == settings.GITHUB_SENTRY_APP_ID
        assert ghapp_installation.name == settings.GITHUB_SENTRY_APP_NAME
        assert ghapp_installation.pem_path is None

    def test_github_webhook_handler_has_request_attribute_when_handler_fails(
        self,
        client,
        url,
        create_valid_jwt_token,
        mocker,
    ):
        """
        Regression test for AttributeError: 'GithubWebhookHandler' has no attribute 'request'.

        When SentryWebhookHandler delegates to GithubWebhookHandler methods and those
        methods fail (e.g., repo not found), _inc_err() is called which accesses
        self.request. This test ensures the request is properly set on the handler.

        Also verifies that NotFound exceptions (expected for repos we don't track)
        are NOT sent to Sentry - they return 200 OK silently.
        """
        mock_capture = mocker.patch(
            "webhook_handlers.views.sentry.sentry_sdk.capture_exception"
        )

        account = AccountFactory(sentry_org_id=999999)
        owner = OwnerFactory(service="github", account=account)

        response = client.post(
            url,
            data={
                "action": "deleted",
                "repository": {
                    "id": "nonexistent-repo-id",
                    "full_name": f"{owner.username}/nonexistent",
                    "owner": {"id": owner.service_id},
                },
            },
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "repository"},
        )

        # NotFound is expected for repos we don't track - should return 200 OK
        assert response.status_code == status.HTTP_200_OK
        assert response.data == {"status": "ok"}
        # Verify sentry_sdk.capture_exception was NOT called
        mock_capture.assert_not_called()

    def test_unexpected_exception_is_sent_to_sentry(
        self,
        client,
        url,
        create_valid_jwt_token,
        mocker,
    ):
        """
        Test that unexpected exceptions ARE sent to Sentry and return 400.
        """
        mock_capture = mocker.patch(
            "webhook_handlers.views.sentry.sentry_sdk.capture_exception"
        )
        # Mock the handler to raise an unexpected exception
        mocker.patch(
            "webhook_handlers.views.sentry.GithubWebhookHandler.repository",
            side_effect=RuntimeError("Unexpected error"),
        )

        account = AccountFactory(sentry_org_id=999999)
        owner = OwnerFactory(service="github", account=account)

        response = client.post(
            url,
            data={
                "action": "deleted",
                "repository": {
                    "id": "some-repo-id",
                    "full_name": f"{owner.username}/some-repo",
                    "owner": {"id": owner.service_id},
                },
            },
            format="json",
            HTTP_AUTHORIZATION=f"Bearer {create_valid_jwt_token}",
            **{GitHubHTTPHeaders.EVENT: "repository"},
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Error handling webhook" in response.data.get("detail", "")
        # Verify sentry_sdk.capture_exception WAS called
        mock_capture.assert_called_once()