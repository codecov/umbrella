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
from webhook_handlers.helpers import HANDLER, should_process
from webhook_handlers.views.github import GithubWebhookHandler

from . import WEBHOOKS_RECEIVED

log = logging.getLogger(__name__)


UNEXPECTED_EVENT = Counter(
    "api_webhooks_unexpected_event",
    "Unexpected events received, broken down by service, event, and action",
    ["service", "event", "action"],
)


def handle_pull_request(request):
    return None


class SentryWebhookHandler(APIView):
    authentication_classes = []
    permission_classes = [JWTAuthenticationPermission]

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.github_webhook_handler = GithubWebhookHandler()
        self.event_handlers = {
            "repository": self.github_webhook_handler.repository,
            "delete": self.github_webhook_handler.delete,
            "member": self.github_webhook_handler.member,
            "organization": self.github_webhook_handler.organization,
            "installation": self.github_webhook_handler.installation,
            "installation_repositories": self.github_webhook_handler.installation_repositories,
            "public": self.github_webhook_handler.public,
            "pull_request": handle_pull_request,
        }

    def post(self, request):
        if ROLLBACK_SENTRY_WEBHOOK.check_value(0, True):
            with transaction.atomic():
                result = self.handle_installation(request)
                transaction.set_rollback(True)
                return result
        else:
            return self.handle_installation(request)

    def handle_installation(self, request):
        event = request.META.get(GitHubHTTPHeaders.EVENT)
        if not event:
            raise ParseError("Missing event header")

        action = request.data.get("action", "")

        handlers = should_process(
            request.data, event, self.github_webhook_handler.service_name
        )
        if HANDLER.SENTRY not in handlers:
            return Response()

        handler = self.event_handlers.get(event)
        if handler is None:
            UNEXPECTED_EVENT.labels(service="sentry", event=event, action=action).inc()
            return Response({"status": "ok"})

        log.info(
            "Received sentry github webhook",
            extra={"payload": request.data, "event": event, "action": action},
        )

        WEBHOOKS_RECEIVED.labels(service="sentry", event=event, action=action).inc()

        try:
            self.github_webhook_handler.event = event
            handler(request)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            log.exception(
                "sentry webhooks: Error handling webhook",
                extra={"event": event, "payload": request.data},
            )
            raise ParseError(f"Error handling webhook: {event}")

        return Response({"status": "ok"})
