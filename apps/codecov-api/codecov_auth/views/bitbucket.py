import logging
import secrets

from asgiref.sync import async_to_sync
from django.conf import settings
from django.shortcuts import redirect
from django.urls import reverse
from django.views import View

from codecov_auth.views.base import LoginMixin
from shared.django_apps.codecov_metrics.service.codecov_metrics import (
    UserOnboardingMetricsService,
)
from shared.torngit import Bitbucket
from shared.torngit.exceptions import (
    TorngitClientGeneralError,
    TorngitServerFailureError,
)

log = logging.getLogger(__name__)


class BitbucketLoginView(View, LoginMixin):
    service = "bitbucket"

    @async_to_sync
    async def fetch_user_data(self, token):
        repo_service = Bitbucket(
            oauth_consumer_token={
                "key": settings.BITBUCKET_CLIENT_ID,
                "secret": settings.BITBUCKET_CLIENT_SECRET,
            },
            token=token,
        )
        user_data = await repo_service.get_authenticated_user()
        authenticated_user = {
            "key": token["key"],
            "secret": token["secret"],
            "id": user_data["uuid"][1:-1],
            "login": user_data.pop("username"),
        }
        user_orgs = await repo_service.list_teams()
        return {
            "user": authenticated_user,
            "orgs": user_orgs,
            "is_student": False,
            "has_private_access": True,
        }

    def redirect_to_bitbucket_step(self, request):
        repo_service = Bitbucket(
            oauth_consumer_token={
                "key": settings.BITBUCKET_CLIENT_ID,
                "secret": settings.BITBUCKET_CLIENT_SECRET,
            }
        )
        state = secrets.token_urlsafe(32)
        url_to_redirect = repo_service.generate_redirect_url(
            settings.BITBUCKET_REDIRECT_URI, state=state
        )
        response = redirect(url_to_redirect)
        response.set_signed_cookie(
            "_bb_oauth_state",
            state,
            domain=settings.COOKIES_DOMAIN,
            httponly=True,
            secure=settings.SESSION_COOKIE_SECURE,
            samesite=settings.COOKIE_SAME_SITE,
            max_age=300,
        )
        self.store_to_cookie_utm_tags(response)
        return response

    def actual_login_step(self, request):
        repo_service = Bitbucket(
            oauth_consumer_token={
                "key": settings.BITBUCKET_CLIENT_ID,
                "secret": settings.BITBUCKET_CLIENT_SECRET,
            }
        )
        expected_state = request.get_signed_cookie("_bb_oauth_state", default=None)
        if not expected_state or request.GET.get("state") != expected_state:
            log.warning("Bitbucket OAuth state mismatch — possible CSRF attempt")
            return redirect(reverse("bitbucket-login"))
        code = request.GET.get("code")
        token = repo_service.generate_access_token(
            code, settings.BITBUCKET_REDIRECT_URI
        )
        user_dict = self.fetch_user_data(token)
        user = self.get_and_modify_owner(user_dict, request)
        redirection_url = settings.CODECOV_DASHBOARD_URL + "/bb"
        redirection_url = self.modify_redirection_url_based_on_default_user_org(
            redirection_url, user
        )
        response = redirect(redirection_url)
        response.delete_cookie("_bb_oauth_state", domain=settings.COOKIES_DOMAIN)
        self.login_owner(user, request, response)
        log.info("User successfully logged in", extra={"ownerid": user.ownerid})
        UserOnboardingMetricsService.create_user_onboarding_metric(
            org_id=user.ownerid, event="INSTALLED_APP", payload={"login": "bitbucket"}
        )
        return response

    def get(self, request):
        if settings.DISABLE_GIT_BASED_LOGIN and request.user.is_anonymous:
            return redirect(f"{settings.CODECOV_DASHBOARD_URL}/login")

        try:
            if request.GET.get("code"):
                log.info("Logging into bitbucket after authorization")
                return self.actual_login_step(request)
            else:
                log.info("Redirecting user to bitbucket for authorization")
                return self.redirect_to_bitbucket_step(request)
        except TorngitServerFailureError:
            log.warning("Bitbucket not available for login")
            return redirect(reverse("bitbucket-login"))
        except TorngitClientGeneralError:
            log.warning("Bitbucket OAuth error during login")
            return redirect(reverse("bitbucket-login"))
