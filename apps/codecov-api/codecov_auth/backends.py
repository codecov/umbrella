import logging

from django.contrib.auth.backends import ModelBackend

from codecov_auth.models import OktaUser

log = logging.getLogger(__name__)


class OktaAdminBackend(ModelBackend):
    """Authentication backend for Okta-based Django admin login.

    Allows `django.contrib.auth.login()` to accept a User resolved by
    okta_id without requiring a password. The standard ModelBackend is
    also kept in AUTHENTICATION_BACKENDS so username/password login
    continues to work unchanged.
    """

    def authenticate(self, request, okta_id=None, **kwargs):
        if not okta_id:
            return None
        try:
            okta_user = OktaUser.objects.select_related("user").get(okta_id=okta_id)
            return okta_user.user
        except OktaUser.DoesNotExist:
            log.debug(
                "OktaAdminBackend: no OktaUser found for okta_id=%s", okta_id
            )
            return None
