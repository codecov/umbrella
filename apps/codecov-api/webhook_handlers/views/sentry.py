import logging
from typing import Literal

from pydantic import BaseModel
from rest_framework import exceptions
from rest_framework.authentication import BaseAuthentication
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework.views import APIView
from shared.metrics import Counter


class RepoInfo(BaseModel):
    repo_service_id: int
    owner_service_id: int
    repo_slug: str


class Installation(BaseModel):
    owner_service_id: int
    owner_username: str

    action: Literal["created", "deleted"]

    installation_id: int

    sender_login: str | None
    app_id: int | None

    repo_selection: Literal["all", "selected"] | None
    repos: list[int] | None
    repos_added: list[int] | None
    repos_removed: list[int] | None


class InstallationRepositories(BaseModel):
    owner_service_id: int
    owner_username: str

    action: Literal["created", "deleted"]

    installation_id: int

    sender_login: str | None
    app_id: int | None

    repo_selection: Literal["all", "selected"] | None
    repos: list[int] | None
    repos_added: list[int] | None
    repos_removed: list[int] | None


class Commit(BaseModel):
    id: str
    message: str


class Push(BaseModel):
    ref: str | None
    commits: list[Commit] | None


class PullRequest(BaseModel):
    action: (
        Literal["opened", "closed", "reopened", "synchronize", "labeled", "edited"]
        | None
    )
    number: int | None
    title: str | None


# temporary stand in
class NoAuth(BaseAuthentication):
    def authenticate(self, request):
        raise exceptions.AuthenticationFailed(
            "This endpoint is not ready for production use"
        )


sentry_webhook = Counter(
    "sentry_webhook", "Number of sentry webhooks received", ["event"]
)

log = logging.getLogger(__name__)


class SentryWebhookHandler(APIView):
    authentication_classes = [NoAuth]
    permission_classes = [IsAuthenticated]

    def post(self, request):
        action = request.data.get("event")
        match action:
            case "installation":
                _ = Installation.model_validate(request.data["payload"])
                sentry_webhook.labels(event="installation").inc()
            case "installation_repositories":
                _ = InstallationRepositories.model_validate(request.data["payload"])
                sentry_webhook.labels(event="installation_repositories").inc()
            case "push":
                _ = Push.model_validate(request.data["payload"])
                sentry_webhook.labels(event="push").inc()
            case "pull_request":
                _ = PullRequest.model_validate(request.data["payload"])
                sentry_webhook.labels(event="pull_request").inc()
            case _:
                raise ValueError(f"Unknown action: {action}")
        return Response({"status": "ok"})
