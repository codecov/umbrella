import logging

from django.db import transaction
from rest_framework.response import Response
from rest_framework.views import APIView

from shared.metrics import Counter
from webhook_handlers.constants import GitHubWebhookEvents
from webhook_handlers.permissions import JWTAuthenticationPermission
from webhook_handlers.views.github import GithubWebhookHandler

sentry_webhook = Counter(
    "sentry_webhook", "Number of sentry webhooks received", ["event"]
)

log = logging.getLogger(__name__)


class SentryWebhookHandler(APIView):
    permission_classes = [JWTAuthenticationPermission]

    def post(self, request):
        with transaction.atomic():
            # this looks weird but we're basically doing "dry runs"
            # of the webhook handling

            # this will eventually be removed once we validate it's working
            # correctly in prod
            transaction.set_rollback(True)
            self.handle_installation(request)

    def handle_installation(self, request):
        """
        Handle installation webhook.
        """
        github_webhook_handler = GithubWebhookHandler()
        action = request.data.get("event")
        match action:
            case "installation":
                sentry_webhook.labels(event="installation").inc()
                log.info(
                    "Received installation webhook",
                    extra={"payload": request.data["payload"]},
                )

                github_webhook_handler.event = GitHubWebhookEvents.INSTALLATION
                github_webhook_handler.installation(request.data["payload"])
            case "installation_repositories":
                sentry_webhook.labels(event="installation_repositories").inc()

                log.info(
                    "Received installation repositories webhook",
                    extra={"payload": request.data["payload"]},
                )

                github_webhook_handler.event = (
                    GitHubWebhookEvents.INSTALLATION_REPOSITORIES
                )
                github_webhook_handler.installation_repositories(
                    request.data["payload"]
                )
            case "push":
                sentry_webhook.labels(event="push").inc()

                log.info(
                    "Received push webhook",
                    extra={"payload": request.data["payload"]},
                )

                github_webhook_handler.event = GitHubWebhookEvents.PUSH
                github_webhook_handler.push(request.data["payload"])
            case "pull_request":
                sentry_webhook.labels(event="pull_request").inc()

                log.info(
                    "Received pull request webhook",
                    extra={"payload": request.data["payload"]},
                )

                github_webhook_handler.event = GitHubWebhookEvents.PULL_REQUEST
                github_webhook_handler.pull_request(request.data["payload"])
            case _:
                raise ValueError(f"Unknown action: {action}")
        return Response({"status": "ok"})
