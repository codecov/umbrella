import jwt
from django.conf import settings
from django.http import HttpRequest


def get_sentry_jwt_payload(request: HttpRequest) -> dict:
    """
    Get the JWT Payload for requests that comes from Sentry.
    """
    # Extract JWT token from Authorization header
    auth_header = request.META.get("HTTP_AUTHORIZATION", "")
    if not auth_header.startswith("Bearer "):
        raise PermissionError("Missing or invalid Authorization header")

    token = auth_header.split(" ")[1]
    payload = jwt.decode(
        token,
        settings.SENTRY_JWT_SHARED_SECRET,
        algorithms=["HS256"],
        options={"verify_exp": True, "require": ["exp", "iat", "iss"]},
    )

    if payload.get("iss") != "https://sentry.io":
        raise PermissionError("Invalid issuer")
    return payload
