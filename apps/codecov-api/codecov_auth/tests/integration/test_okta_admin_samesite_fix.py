"""
Integration test for Okta admin SameSite=None fix.

This test verifies that the complete fix (middleware + view) actually solves
the redirect loop issue caused by browsers blocking SameSite=Lax cookies
during OAuth callbacks.
"""

import pytest
from django.contrib.sessions.middleware import SessionMiddleware
from django.http import HttpResponse
from django.test import RequestFactory, TestCase, override_settings

from codecov_auth.middleware import okta_admin_samesite_middleware
from codecov_auth.views.okta_admin import OktaAdminLoginView


@override_settings(
    OKTA_ISS="https://example.okta.com",
    OKTA_ADMIN_CLIENT_ID="test-client",
    OKTA_ADMIN_CLIENT_SECRET="test-secret",
    OKTA_ADMIN_REDIRECT_URL="https://api-admin.codecov.io/login/okta-admin",
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_NAME="sessionid",
    SESSION_COOKIE_DOMAIN=".codecov.io",
)
class TestOktaAdminSameSiteIntegration(TestCase):
    """Integration tests for the complete SameSite=None fix."""

    @pytest.fixture
    def factory(self):
        return RequestFactory()

    def test_oauth_initiation_sets_samesite_none_cookie(self, factory, db):
        """
        Test the complete flow:
        1. User requests /login/okta-admin
        2. SessionMiddleware sets session cookie with default SameSite
        3. View redirects to Okta
        4. Our middleware overrides cookie with SameSite=None
        5. Browser can send cookie when Okta redirects back

        This is the fix for the redirect loop issue.
        """
        # Create request
        request = factory.get("/login/okta-admin")

        # Apply SessionMiddleware (as it would run in production)
        session_middleware = SessionMiddleware(lambda r: None)
        session_middleware.process_request(request)
        request.session.save()

        # Call the view
        view = OktaAdminLoginView.as_view()
        response = view(request)

        # SessionMiddleware would normally process response here
        response = session_middleware.process_response(request, response)

        # Now our custom middleware runs AFTER SessionMiddleware
        custom_middleware = okta_admin_samesite_middleware(lambda r: response)
        final_response = custom_middleware(request)

        # Verify the fix: Cookie should have SameSite=None
        assert "sessionid" in final_response.cookies
        cookie = final_response.cookies["sessionid"]
        assert cookie["samesite"] == "None", "SameSite must be None for OAuth to work"
        assert cookie["secure"] is True, "Secure must be True when SameSite=None"
        assert cookie["httponly"] is True, "HttpOnly protects against XSS"

        # Verify OAuth redirect happened
        assert final_response.status_code == 302
        assert "https://example.okta.com/oauth2/v1/authorize" in final_response.url

    def test_other_paths_not_affected(self, factory, db):
        """
        Verify that other login paths don't get SameSite=None.
        Only /login/okta-admin should be affected.
        """
        paths_to_test = [
            "/login/okta",
            "/admin/",
            "/api/v2/users/",
        ]

        for path in paths_to_test:
            request = factory.get(path)

            session_middleware = SessionMiddleware(lambda r: None)
            session_middleware.process_request(request)
            request.session.save()

            response = HttpResponse("OK")
            response = session_middleware.process_response(request, response)

            custom_middleware = okta_admin_samesite_middleware(lambda r: response)
            final_response = custom_middleware(request)

            if "sessionid" in final_response.cookies:
                cookie = final_response.cookies["sessionid"]
                assert cookie.get("samesite") != "None", (
                    f"SameSite=None should not be set for {path}"
                )

    @override_settings(SESSION_COOKIE_SECURE=False)
    def test_no_override_when_secure_cookies_disabled(self, factory, db):
        """
        Verify that the fix doesn't apply when SESSION_COOKIE_SECURE=False.
        """
        request = factory.get("/login/okta-admin")

        session_middleware = SessionMiddleware(lambda r: None)
        session_middleware.process_request(request)
        request.session.save()

        view = OktaAdminLoginView.as_view()
        response = view(request)
        response = session_middleware.process_response(request, response)

        custom_middleware = okta_admin_samesite_middleware(lambda r: response)
        final_response = custom_middleware(request)

        if "sessionid" in final_response.cookies:
            cookie = final_response.cookies["sessionid"]
            assert cookie.get("samesite") != "None"
