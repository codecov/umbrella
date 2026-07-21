from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import NotFound, Unauthenticated
from codecov_auth.models import Owner
from services.task import TaskService


class DeleteOwnerInteractor(BaseInteractor):
    def validate(self) -> None:
        if not self.current_user.is_authenticated:
            raise Unauthenticated()

    @sync_to_async
    def execute(self, username: str) -> None:
        self.validate()

        owner = Owner.objects.filter(
            service=self.service, username=username
        ).first()
        if not owner:
            raise NotFound()

        # `ensure_is_admin` permits deleting a personal account (the current
        # owner deleting themselves) as well as an organization the current
        # owner administers.
        self.ensure_is_admin(owner)

        # Route through the same code path the Django admin uses so that
        # self-serve deletions are marked, obfuscated, and hard-deleted
        # identically to staff-initiated deletions.
        TaskService().delete_owner(ownerid=owner.ownerid)
