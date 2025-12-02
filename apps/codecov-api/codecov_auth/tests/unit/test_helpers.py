from unittest.mock import Mock, patch

import pytest
from django.contrib.admin.models import LogEntry
from django.test import override_settings

from codecov_auth.helpers import (
    History,
    current_user_part_of_org,
    get_client_ip_address,
)
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory


@pytest.mark.django_db
def test_current_user_part_of_org_when_user_not_authenticated():
    org = OwnerFactory()
    assert current_user_part_of_org(None, org) is False


@pytest.mark.django_db
def test_current_user_part_of_org_when_user_is_owner():
    current_user = OwnerFactory()
    assert current_user_part_of_org(current_user, current_user) is True


@pytest.mark.django_db
def test_current_user_part_of_org_when_user_doesnt_have_org():
    org = OwnerFactory()
    current_user = OwnerFactory(organizations=None)
    current_user.save()
    assert current_user_part_of_org(current_user, org) is False


@pytest.mark.django_db
def test_current_user_part_of_org_when_user_has_org():
    org = OwnerFactory()
    current_user = OwnerFactory(organizations=[org.ownerid])
    current_user.save()
    assert current_user_part_of_org(current_user, current_user) is True


def test_client_ip_from_x_forwarded_for_default_count():
    """
    With default TRUSTED_PROXY_COUNT=0, should ignore XFF and use REMOTE_ADDR
    for security (prevents XFF spoofing).
    """
    request = Mock()
    request.META = {"HTTP_X_FORWARDED_FOR": "127.0.0.1,blah", "REMOTE_ADDR": "10.0.0.1"}

    result = get_client_ip_address(request)
    # Default count=0 means don't trust XFF, use REMOTE_ADDR
    assert result == "10.0.0.1"


@override_settings(TRUSTED_PROXY_COUNT=1)
def test_client_ip_from_x_forwarded_for_single_proxy():
    """
    With TRUSTED_PROXY_COUNT=1, trust one proxy and get the client IP
    from the rightmost non-proxy IP in the XFF chain.
    XFF format: client_ip, proxy_ip
    """
    request = Mock()
    request.META = {
        "HTTP_X_FORWARDED_FOR": "192.168.1.100, 10.0.0.1",
        "REMOTE_ADDR": "10.0.0.1",
    }

    result = get_client_ip_address(request)
    # With 1 proxy, ipware returns the client IP (leftmost)
    assert result == "192.168.1.100"


@override_settings(TRUSTED_PROXY_COUNT=2)
def test_client_ip_from_x_forwarded_for_multiple_proxies():
    """
    With TRUSTED_PROXY_COUNT=2, trust two proxies in the chain.
    XFF format: client_ip, proxy1_ip, proxy2_ip
    """
    request = Mock()
    request.META = {
        "HTTP_X_FORWARDED_FOR": "203.0.113.1,192.168.1.1,10.0.0.1",
        "REMOTE_ADDR": "10.0.0.1",
    }

    result = get_client_ip_address(request)
    # With 2 proxies, ipware returns the real client IP
    assert result == "203.0.113.1"


@override_settings(TRUSTED_PROXY_COUNT=1)
def test_client_ip_handles_whitespace_in_xff():
    """
    Should handle whitespace in X-Forwarded-For header correctly.
    """
    request = Mock()
    request.META = {
        "HTTP_X_FORWARDED_FOR": "  192.168.1.100  ,   10.0.0.1  ",
        "REMOTE_ADDR": "10.0.0.1",
    }

    result = get_client_ip_address(request)
    # ipware should handle whitespace and extract the client IP
    assert result == "192.168.1.100"


@override_settings(TRUSTED_PROXY_COUNT=2, TRUSTED_PROXY_IPS=["10.0.0.1", "10.0.0.2"])
def test_client_ip_with_trusted_proxy_ips():
    """
    When TRUSTED_PROXY_IPS is set, only requests from those IPs
    should have their XFF headers processed.
    """
    request = Mock()
    request.META = {
        "HTTP_X_FORWARDED_FOR": "203.0.113.1,192.168.1.1,10.0.0.1",
        "REMOTE_ADDR": "10.0.0.1",
    }

    result = get_client_ip_address(request)
    # Should trust the XFF because REMOTE_ADDR is in TRUSTED_PROXY_IPS
    assert result == "203.0.113.1"


def test_client_ip_from_remote_addr():
    """
    When no X-Forwarded-For header is present, should use REMOTE_ADDR.
    """
    request = Mock()
    request.META = {"HTTP_X_FORWARDED_FOR": None, "REMOTE_ADDR": "10.0.0.1"}

    result = get_client_ip_address(request)
    assert result == "10.0.0.1"


def test_client_ip_with_empty_xff():
    """
    When X-Forwarded-For is empty, should fall back to REMOTE_ADDR.
    """
    request = Mock()
    request.META = {"HTTP_X_FORWARDED_FOR": "", "REMOTE_ADDR": "10.0.0.1"}

    result = get_client_ip_address(request)
    assert result == "10.0.0.1"


def test_client_ip_prevents_xff_spoofing_with_default_config():
    """
    SECURITY TEST: With default config (TRUSTED_PROXY_COUNT=0),
    should NOT trust X-Forwarded-For headers, preventing IP spoofing.
    This is the safest default configuration.
    """
    request = Mock()
    # Attacker tries to spoof their IP using XFF header
    request.META = {
        "HTTP_X_FORWARDED_FOR": "1.2.3.4,5.6.7.8,9.10.11.12",
        "REMOTE_ADDR": "203.0.113.50",  # Real IP
    }

    result = get_client_ip_address(request)
    # Should use REMOTE_ADDR, not the spoofed IPs
    assert result == "203.0.113.50"


@override_settings(TRUSTED_PROXY_COUNT=1)
def test_client_ip_rightmost_strategy_prevents_leftmost_spoofing():
    """
    SECURITY TEST: Using right-most strategy prevents attackers from
    spoofing IPs by prepending fake IPs to the XFF chain.
    """
    request = Mock()
    # Attacker tries to spoof by prepending fake IPs
    request.META = {
        "HTTP_X_FORWARDED_FOR": "1.1.1.1,2.2.2.2,3.3.3.3,203.0.113.50",
        "REMOTE_ADDR": "10.0.0.1",
    }

    result = get_client_ip_address(request)
    # With 1 proxy, should get the IP just before the proxy (rightmost non-proxy)
    # ipware will extract 3.3.3.3, not the leftmost spoofed IPs
    assert result == "3.3.3.3"


@override_settings(TRUSTED_PROXY_COUNT=2)
def test_client_ip_with_insufficient_proxies_in_chain():
    """
    When TRUSTED_PROXY_COUNT is higher than the number of IPs in XFF,
    should handle gracefully.
    """
    request = Mock()
    request.META = {
        "HTTP_X_FORWARDED_FOR": "192.168.1.100",  # Only 1 IP
        "REMOTE_ADDR": "10.0.0.1",
    }

    result = get_client_ip_address(request)
    # Should still return a valid IP (fallback behavior)
    assert result in ["192.168.1.100", "10.0.0.1"]


def test_client_ip_with_no_meta():
    """
    When request.META has neither XFF nor REMOTE_ADDR, should return 'unknown'.
    """
    request = Mock()
    request.META = {}

    result = get_client_ip_address(request)
    assert result == "unknown"


@pytest.mark.django_db
@patch("codecov_auth.helpers.format_stack")
def test_log_entry(mocked_format_stack):
    mocked_format_stack.return_value = "test"
    orig_owner = OwnerFactory()
    impersonated_owner = OwnerFactory()
    History.log(
        impersonated_owner,
        "Impersonation successful",
        orig_owner.user,
        add_traceback=True,
    )
    log_entries = LogEntry.objects.all()
    assert (
        str(log_entries.first())
        == f"Changed “{str(impersonated_owner)}” — Impersonation successful: test"
    )


@pytest.mark.django_db
@patch("codecov_auth.helpers.format_stack")
def test_log_entry_no_object(mocked_format_stack):
    mocked_format_stack.return_value = "test"
    orig_owner = OwnerFactory()
    History.log(
        None,
        "Impersonation successful",
        orig_owner.user,
        add_traceback=True,
    )
    log_entries = LogEntry.objects.all()
    assert log_entries.first() is None
