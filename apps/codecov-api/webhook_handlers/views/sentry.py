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


class SentryWebhookHandler(APIView):
    authentication_classes = []
    permission_classes = [JWTAuthenticationPermission]

    def post(self, request):
        if ROLLBACK_SENTRY_WEBHOOK.check_value(0, True):
            with transaction.atomic():
                # this will eventually be removed once we validate it's working
                # correctly in prod
                result = self.handle_installation(request)
                transaction.set_rollback(True)
                return result
        else:
            return self.handle_installation(request)

    def handle_installation(self, request):
        github_webhook_handler = GithubWebhookHandler()
        github_webhook_handler.dry_run = True
        action = request.META.get(GitHubHTTPHeaders.EVENT)

        sentry_webhook.labels(event=action).inc()
        log.info(
            f"Received {action} webhook",
            extra={"payload": request.data},
        )

        try:
            handler_method = getattr(github_webhook_handler, action)
            if handler_method is None:
                raise ValueError(f"Unknown action: {action}")

            github_webhook_handler.event = action
            handler_method(request)
        except Exception as e:
            sentry_sdk.capture_exception(e)
            log.error(
                f"Error handling {action} webhook",
                extra={"error": str(e), "payload": request.data},
            )
            raise ParseError(f"Error handling {action} webhook")

        return Response({"status": "ok"})
