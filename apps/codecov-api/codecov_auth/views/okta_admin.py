import logging

from django.conf import settings
from django.contrib.auth import login
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect
from django.views import View
from requests.auth import HTTPBasicAuth

from codecov_auth.models import OktaUser, User
from codecov_auth.views.base import StateMixin
from codecov_auth.views.okta_mixin import (
    OktaLoginMixin,
    OktaTokenResponse,
    validate_id_token,
)

log = logging.getLogger(__name__)

_ADMIN_URL = getattr(settings, "DJANGO_ADMIN_URL", "admin")


class OktaAdminLoginView(OktaLoginMixin, StateMixin, View):
    """Handles Okta OAuth for the Django admin panel.

    Adds Okta as a *second* login method alongside username/password.
    On success, the resolved User has is_staff=True set and is redirected
    to the admin URL (or the `next` param supplied in the initial request).
    """

    def get(self, request: HttpRequest) -> HttpResponse:
        iss = settings.OKTA_ISS
        if not iss:
            log.warning("Okta admin login attempted but OKTA_ISS is not configured")
            return HttpResponse(
                "Okta SSO is not configured for this instance.", status=503
            )

        if request.GET.get("code"):
            return self._handle_callback(request, iss)

        # Stash the intended post-login destination before leaving for Okta.
        request.session["okta_admin_next"] = request.GET.get(
            "next", f"/{_ADMIN_URL}/"
        )
        return self._redirect_to_consent(
            iss=iss,
            client_id=settings.OKTA_ADMIN_CLIENT_ID,
            oauth_redirect_url=settings.OKTA_ADMIN_REDIRECT_URL,
        )

    def _handle_callback(self, request: HttpRequest, iss: str) -> HttpResponse:
        code = request.GET.get("code")
        state = request.GET.get("state")

        if not self.verify_state(state):
            log.warning("Invalid state during Okta admin login callback")
            return redirect(f"/{_ADMIN_URL}/login/")

        basic_auth = HTTPBasicAuth(
            settings.OKTA_ADMIN_CLIENT_ID, settings.OKTA_ADMIN_CLIENT_SECRET
        )
        user_data: OktaTokenResponse | None = self._fetch_user_data(
            iss, code, state, settings.OKTA_ADMIN_REDIRECT_URL, basic_auth
        )
        if user_data is None:
            log.warning("Okta admin login failed: token exchange returned no data")
            return redirect(f"/{_ADMIN_URL}/login/")

        current_user = self._resolve_user(request, iss, user_data)
        if current_user is None:
            return redirect(f"/{_ADMIN_URL}/login/")

        current_user.is_staff = True
        current_user.save(update_fields=["is_staff"])

        self.remove_state(state)
        login(request, current_user, backend="codecov_auth.backends.OktaAdminBackend")
        next_url = request.session.pop("okta_admin_next", f"/{_ADMIN_URL}/")
        return redirect(next_url)

    def _resolve_user(
        self, request: HttpRequest, iss: str, user_data: OktaTokenResponse
    ) -> User | None:
        """Get or create the Django User linked to this Okta identity."""
        try:
            id_payload = validate_id_token(
                iss, user_data.id_token, settings.OKTA_ADMIN_CLIENT_ID
            )
        except Exception:
            log.warning("Okta admin login failed: id_token validation error", exc_info=True)
            return None

        okta_id = id_payload.sub
        okta_user = OktaUser.objects.filter(okta_id=okta_id).first()

        if okta_user:
            log.info(
                "Existing Okta user logging in via admin",
                extra={"okta_user_id": okta_user.pk},
            )
            return okta_user.user

        # First-time login — create the User and link the OktaUser record.
        current_user = User.objects.create(
            name=id_payload.name,
            email=id_payload.email,
        )
        okta_user = OktaUser.objects.create(
            user=current_user,
            okta_id=okta_id,
            name=id_payload.name,
            email=id_payload.email,
            access_token=user_data.access_token,
        )
        log.info(
            "Created Okta user during admin login",
            extra={"okta_user_id": okta_user.pk},
        )
        return current_user
