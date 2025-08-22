from unittest.mock import patch

import pytest
from django.urls import reverse
from rest_framework.test import APIClient

from services.task import TaskService
from shared.django_apps.codecov_auth.tests.factories import (
    OrganizationLevelTokenFactory,
)
from shared.django_apps.core.tests.factories import (
    OwnerFactory,
    RepositoryFactory,
)

TEST_COMMIT_SHA = "6fd5b89357fc8cdf34d6197549ac7c6d7e5977ef"
TEST_STORAGE_PATH = "custom/storage/path/test.txt"
TEST_PRESIGNED_URL = "test-presigned-put"
TEST_FAKE_TOKEN = "12345678-1234-1234-1234-123456789012"


@pytest.fixture
def api_client():
    return APIClient()


@pytest.fixture
def owner():
    return OwnerFactory(service="github", username="codecov")


@pytest.fixture
def repository(owner):
    return RepositoryFactory.create(
        author=owner, active=False, activated=False, test_analytics_enabled=False
    )


@pytest.fixture
def authenticated_client(api_client, repository):
    api_client.credentials(HTTP_AUTHORIZATION=f"token {repository.upload_token}")
    return api_client


@pytest.fixture
def org_token_client(api_client, repository):
    org_token = OrganizationLevelTokenFactory.create(owner=repository.author)
    api_client.credentials(HTTP_AUTHORIZATION=f"token {org_token.token}")
    return api_client


@pytest.fixture
def mock_task_service(mocker):
    return mocker.patch.object(TaskService, "queue_ta_upload")


@pytest.fixture
def mock_presigned_put(mocker):
    return mocker.patch(
        "shared.api_archive.archive.ArchiveService.create_presigned_put",
        return_value=TEST_PRESIGNED_URL,
    )


@pytest.fixture
def mock_shelter_request(mocker):
    return mocker.patch(
        "upload.views.upload_test_analytics.TAUploadView.is_shelter_request",
        return_value=False,
    )


@pytest.fixture
def mock_shelter_request_true(mocker):
    return mocker.patch(
        "upload.views.upload_test_analytics.TAUploadView.is_shelter_request",
        return_value=True,
    )


def test_ta_upload_missing_required_fields(db, authenticated_client, mock_task_service):
    res = authenticated_client.post(
        reverse("upload-test-analytics"),
        {"slug": "test:::repo"},
        format="json",
    )
    assert res.status_code == 400
    assert res.json() == {"commit": ["This field is required."]}
    assert not mock_task_service.called

    res = authenticated_client.post(
        reverse("upload-test-analytics"),
        {"commit": TEST_COMMIT_SHA},
        format="json",
    )
    assert res.status_code == 400
    assert res.json() == {"slug": ["This field is required."]}
    assert not mock_task_service.called


def test_ta_upload_invalid_user_type(
    db, org_token_client, repository, mock_task_service
):
    res = org_token_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
        },
        format="json",
    )
    assert res.status_code == 401
    assert not mock_task_service.called


def test_ta_upload_repository_token_ignores_slug(
    db,
    authenticated_client,
    repository,
    mock_task_service,
    mock_presigned_put,
    mock_shelter_request,
):
    res = authenticated_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": "FakeUser::::NonExistentRepo",
        },
        format="json",
    )
    assert res.status_code == 201
    assert res.json() == {"raw_upload_location": TEST_PRESIGNED_URL}

    mock_task_service.assert_called_once_with(
        repo_id=repository.repoid,
        upload_context={
            "branch": None,
            "commit_sha": TEST_COMMIT_SHA,
            "flags": None,
            "merged": False,
            "pull_id": None,
            "storage_path": mock_presigned_put.call_args[0][0],
        },
    )


def test_ta_upload_happy_path_shelter_request(
    db,
    repository,
    mock_task_service,
    mock_presigned_put,
    mock_shelter_request_true,
    mock_prometheus_metrics,
):
    client = APIClient()
    client.credentials(HTTP_AUTHORIZATION=f"token {repository.upload_token}")

    res = client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
            "branch": "main",
            "flags": ["unit", "integration"],
            "pr": "123",
            "storage_path": TEST_STORAGE_PATH,
        },
        format="json",
        headers={"User-Agent": "codecov-cli/0.4.7"},
    )

    assert res.status_code == 201
    assert res.data is None

    repository.refresh_from_db()
    assert repository.active is True
    assert repository.activated is True
    assert repository.test_analytics_enabled is True

    mock_task_service.assert_called_once_with(
        repo_id=repository.repoid,
        upload_context={
            "branch": "main",
            "commit_sha": TEST_COMMIT_SHA,
            "flags": ["unit", "integration"],
            "merged": False,
            "pull_id": "123",
            "storage_path": TEST_STORAGE_PATH,
        },
    )

    mock_presigned_put.assert_not_called()
    mock_prometheus_metrics.assert_called()


def test_ta_upload_happy_path_non_shelter_request(
    db,
    owner,
    mock_task_service,
    mock_presigned_put,
    mock_shelter_request,
    mock_prometheus_metrics,
):
    repository = RepositoryFactory.create(
        author=owner, active=True, activated=True, test_analytics_enabled=False
    )

    client = APIClient()
    client.credentials(HTTP_AUTHORIZATION=f"token {repository.upload_token}")

    res = client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
            "branch": repository.branch,
            "build": "test-build",
            "buildUrl": "test-build-url",
            "code": "test-job",
            "service": "github-actions",
        },
        format="json",
        headers={"User-Agent": "codecov-cli/0.4.7"},
    )

    assert res.status_code == 201
    assert res.json() == {"raw_upload_location": TEST_PRESIGNED_URL}

    repository.refresh_from_db()
    assert repository.active is True
    assert repository.activated is True
    assert repository.test_analytics_enabled is True

    expected_storage_path = mock_presigned_put.call_args[0][0]
    mock_task_service.assert_called_once_with(
        repo_id=repository.repoid,
        upload_context={
            "branch": repository.branch,
            "commit_sha": TEST_COMMIT_SHA,
            "flags": None,
            "merged": True,
            "pull_id": None,
            "storage_path": expected_storage_path,
        },
    )

    mock_presigned_put.assert_called_once()
    storage_path_arg = mock_presigned_put.call_args[0][0]
    assert storage_path_arg.startswith("test_results/v1/raw/")


def test_ta_upload_no_auth(db, api_client, repository, mock_task_service):
    api_client.credentials(HTTP_AUTHORIZATION="token BAD_TOKEN")

    res = api_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
        },
        format="json",
    )
    assert res.status_code == 401
    assert not mock_task_service.called


def test_ta_upload_invalid_repository_token(db, api_client, mock_task_service):
    api_client.credentials(HTTP_AUTHORIZATION=f"token {TEST_FAKE_TOKEN}")

    res = api_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": "some:::repo",
        },
        format="json",
    )
    assert res.status_code == 401
    assert not mock_task_service.called


@patch("upload.helpers.jwt.decode")
@patch("upload.helpers.PyJWKClient")
def test_ta_upload_github_oidc_token(
    mock_jwks_client,
    mock_jwt_decode,
    db,
    api_client,
    repository,
    mock_task_service,
    mock_presigned_put,
    mock_shelter_request,
):
    mock_jwt_decode.return_value = {
        "repository": f"url/{repository.name}",
        "repository_owner": repository.author.username,
        "iss": "https://token.actions.githubusercontent.com",
    }
    token = "ThisValueDoesNotMatterBecauseOf_mock_jwt_decode"

    api_client.credentials(HTTP_AUTHORIZATION=f"token {token}")

    res = api_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
            "branch": "main",
        },
        format="json",
    )
    assert res.status_code == 201
    assert res.json() == {"raw_upload_location": TEST_PRESIGNED_URL}

    mock_task_service.assert_called_once()


def test_ta_upload_shelter_request_without_storage_path(
    db,
    authenticated_client,
    repository,
    mock_task_service,
    mock_presigned_put,
    mock_shelter_request_true,
):
    res = authenticated_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
            "branch": "main",
        },
        format="json",
    )

    assert res.status_code == 201
    assert res.json() == {"raw_upload_location": TEST_PRESIGNED_URL}

    mock_presigned_put.assert_called_once()
    mock_task_service.assert_called_once()


def test_ta_upload_serializer_all_optional_fields(
    db,
    authenticated_client,
    repository,
    mock_task_service,
    mock_presigned_put,
    mock_shelter_request,
):
    res = authenticated_client.post(
        reverse("upload-test-analytics"),
        {
            "commit": TEST_COMMIT_SHA,
            "slug": f"{repository.author.username}::::{repository.name}",
            "service": "github-actions",
            "build": "test-build",
            "buildUrl": "test-build-url",
            "code": "test-job",
            "flags": ["unit", "integration", "backend"],
            "pr": "456",
            "branch": "feature-branch",
            "storage_path": "custom/path.txt",
            "file_not_found": True,
        },
        format="json",
    )

    assert res.status_code == 201

    call_args = mock_task_service.call_args
    upload_context = call_args.kwargs["upload_context"]

    assert upload_context["commit_sha"] == TEST_COMMIT_SHA
    assert upload_context["branch"] == "feature-branch"
    assert upload_context["flags"] == ["unit", "integration", "backend"]
    assert upload_context["pull_id"] == "456"
    assert upload_context["merged"] == False
