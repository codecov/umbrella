from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import Unauthenticated
from services.refresh import RefreshService


class TriggerSyncInteractor(BaseInteractor):
    def validate(self, using_integration: bool) -> None:
        if not self.current_owner:
            raise Unauthenticated()
        if not using_integration and not self.current_user.is_authenticated:
            raise Unauthenticated()

    @sync_to_async
    def execute(self, using_integration: bool = False) -> None:
        self.validate(using_integration)
        RefreshService().trigger_refresh(
            self.current_owner.ownerid,
            self.current_owner.username,
            using_integration=using_integration,
            manual_trigger=True,
        )
