from traceback import format_stack

import requests
from django.conf import settings
from django.contrib.admin.models import CHANGE, LogEntry
from django.contrib.contenttypes.models import ContentType
from django.http import HttpRequest
from ipware import get_client_ip

from codecov_auth.constants import GITLAB_BASE_URL

GITLAB_PAYLOAD_AVATAR_URL_KEY = "avatar_url"


def get_gitlab_url(email, size):
    res = requests.get(f"{GITLAB_BASE_URL}/api/v4/avatar?email={email}&size={size}")
    url = ""
    if res.status_code == 200:
        data = res.json()
        try:
            url = data[GITLAB_PAYLOAD_AVATAR_URL_KEY]
        except KeyError:
            pass

    return url


def current_user_part_of_org(owner, org):
    if owner is None:
        return False
    if owner == org:
        return True
    # owner is a direct member of the org
    orgs_of_user = owner.organizations or []
    return org.ownerid in orgs_of_user


def get_client_ip_address(request: HttpRequest) -> str:
    """
    Get the client's IP address from the request, parsing the X-Forwarded-For
    header if it exists with a configured proxy depth or Remote-Addr otherwise.

    Uses django-ipware to safely extract the client IP from proxy headers,
    with protection against header spoofing. The library uses the "right-most"
    strategy, which means it trusts the rightmost non-trusted IP in the
    X-Forwarded-For chain (the most reliable position).

    :param request: The HTTP request object
    :return: The client's IP address as a string
    """
    # Get configuration for trusted proxies
    proxy_count = getattr(settings, "TRUSTED_PROXY_COUNT", 0)
    trusted_proxies = getattr(settings, "TRUSTED_PROXY_IPS", [])

    # Use ipware to get the client IP with security best practices
    client_ip, is_routable = get_client_ip(
        request,
        proxy_order="right-most",  # Use rightmost non-trusted IP (most secure)
        proxy_count=proxy_count,  # Number of proxies to traverse
        proxy_trusted_ips=trusted_proxies,  # List of trusted proxy IPs
        request_header_order=["X_FORWARDED_FOR", "REMOTE_ADDR"],
    )

    # Fallback to REMOTE_ADDR if ipware couldn't determine the IP
    # or return a safe default
    return client_ip or request.META.get("REMOTE_ADDR", "unknown")


# https://stackoverflow.com/questions/7905106/adding-a-log-entry-for-an-action-by-a-user-in-a-django-ap


class History:
    @staticmethod
    def log(objects, message, user, action_flag=None, add_traceback=False):
        """
        Log an action in the admin log
        :param objects: Objects being operated on
        :param message: Message to log
        :param user: User performing action
        :param action_flag: Type of action being performed
        :param add_traceback: Add the stack trace to the message
        """
        if action_flag is None:
            action_flag = CHANGE

        if not isinstance(objects, list):
            objects = [objects]

        if add_traceback:
            message = f"{message}: {format_stack()}"

        for obj in objects:
            if not obj:
                continue

            LogEntry.objects.log_action(
                user_id=user.pk,
                content_type_id=ContentType.objects.get_for_model(obj).pk,
                object_repr=str(obj),
                object_id=obj.ownerid,
                change_message=message,
                action_flag=action_flag,
            )
