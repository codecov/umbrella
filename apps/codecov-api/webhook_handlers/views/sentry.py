import logging

import sentry_sdk
from django.db import transaction
from rest_framework.exceptions import ParseError
from rest_framework.response import Response
from rest_framework.views import APIView

from codecov_auth.permissions import JWTAuthenticationPermission
from rollouts import ROLLBACK_SENTRY_WEBHOOK
from shared.metrics import Counter
from webhook_handlers.constants import GitHubHTTPHeaders
from webhook_handlers.views.github import GithubWebhookHandler

sentry_webhook = Counter(
    "sentry_webhook", "Number of sentry webhooks received", ["event"]
)

log = logging.getLogger(__name__)

github_webhook_handler = GithubWebhookHandler()


def handle_pull_request(request):
    return None


SENTRY_EVENT_HANDLERS = {
    "repository": github_webhook_handler.repository,
    "delete": github_webhook_handler.delete,
    "member": github_webhook_handler.member,
    "organization": github_webhook_handler.organization,
    "installation": github_webhook_handler.installation,
    "installation_repositories": github_webhook_handler.installation_repositories,
    "public": github_webhook_handler.public,
    "pull_request": handle_pull_request,
}


class SentryWebhookHandler(APIView):
    authentication_classes = []
    permission_classes = [JWTAuthenticationPermission]

    def post(self, request):
        if ROLLBACK_SENTRY_WEBHOOK.check_value(0, True):
            with transaction.atomic():
                result = self.handle_installation(request)
                transaction.set_rollback(True)
                return result
        else:
            return self.handle_installation(request)

    def handle_installation(self, request):
        action = request.META.get(GitHubHTTPHeaders.EVENT)
        if not action:
            raise ParseError("Missing event header")

        sentry_webhook.labels(event=action).inc()
        log.info(
            f"Received {action} webhook",
            extra={"payload": request.data},
        )

        handler = SENTRY_EVENT_HANDLERS.get(action)
        if handler is None:
            return Response({"status": "ok"})

        try:
            github_webhook_handler.event = action
            handler(request)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            log.error(
                "sentry webhooks: Error handling webhook",
                extra={"error": str(e), "action": action, "payload": request.data},
            )
            raise ParseError(f"Error handling webhook: {action}")

        return Response({"status": "ok"})
