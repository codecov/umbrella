from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import NotFound, Unauthenticated
from codecov_auth.models import Owner
from shared.django_apps.codecov_auth.models import OwnerToBeDeleted


class DeleteOwnerInteractor(BaseInteractor):
    def execute(self, username: str) -> None:
        if not self.current_user.is_authenticated:
            raise Unauthenticated()

        owner = Owner.objects.filter(
            service=self.service, username=username
        ).first()
        if not owner:
            raise NotFound()

        self.ensure_is_admin(owner)

        if not OwnerToBeDeleted.objects.filter(owner_id=owner.ownerid).exists():
            OwnerToBeDeleted.objects.create(owner_id=owner.ownerid)