from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import Unauthenticated
from shared.django_apps.codecov_auth.models import _generate_support_pin


class RegenerateSupportPinInteractor(BaseInteractor):
    def validate(self):
        if not self.current_user.is_authenticated or not self.current_owner:
            raise Unauthenticated()

    @sync_to_async
    def execute(self):
        self.validate()
        self.current_owner.support_pin = _generate_support_pin()
        self.current_owner.save()
        return self.current_owner
