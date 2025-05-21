from typing import Any

import jwt
from rest_framework.permissions import BasePermission

from codecov_auth.utils import get_sentry_jwt_payload
from webhook_handlers.views import WEBHOOKS_ERRORED


class JWTAuthenticationPermission(BasePermission):
    """
    Permission class to validate JWT tokens in Sentry webhook requests.
    """

    def has_permission(self, request: Any, view: Any) -> bool:
        try:
            payload = get_sentry_jwt_payload(request)
        except PermissionError:
            self._inc_err("missing_or_invalid_auth_header")
            return False
        except jwt.ExpiredSignatureError:
            self._inc_err("token_expired")
            return False
        except jwt.InvalidTokenError:
            self._inc_err("invalid_token")
            return False

        # Set the validated payload on the request for use in the view
        request.jwt_payload = payload
        return True

    def _inc_err(self, reason: str) -> None:
        """Increment error counter for metrics tracking"""
        WEBHOOKS_ERRORED.labels(
            service="sentry",
            event="webhook",
            action="",
            error_reason=reason,
        ).inc()
