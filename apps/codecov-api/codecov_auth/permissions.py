from typing import Any

import jwt
from rest_framework.permissions import BasePermission

from codecov_auth.utils import get_sentry_jwt_payload


class SpecificScopePermission(BasePermission):
    def has_permission(self, request, view):
        return request.auth is not None and all(
            scope in request.auth.get_scopes() for scope in view.required_scopes
        )


class JWTAuthenticationPermission(BasePermission):
    """
    Permission class to validate JWT tokens in Sentry webhook requests.
    """

    def has_permission(self, request: Any, view: Any) -> bool:
        try:
            payload = get_sentry_jwt_payload(request)
        except PermissionError:
            return False
        except jwt.ExpiredSignatureError:
            return False
        except jwt.InvalidTokenError:
            return False

        # Set the validated payload on the request for use in the view
        request.jwt_payload = payload
        return True
