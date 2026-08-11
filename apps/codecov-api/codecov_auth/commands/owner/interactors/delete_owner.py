from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import NotFound, Unauthenticated, ValidationError
from codecov_auth.models import Owner, PlanProviders, StripeBilling
from services.task import TaskService
from shared.plan.service import PlanService


def _validate_owner_can_be_deleted(owner: Owner) -> None:
    if owner.uses_invoice:
        raise ValidationError("Contact support to delete invoice-billed accounts.")

    if owner.root_organization is not None:
        return

    plan_service = PlanService(current_org=owner)
    if plan_service.is_free_plan:
        return

    if owner.plan_provider == PlanProviders.GITHUB:
        raise ValidationError(
            "Cancel or downgrade your GitHub Marketplace subscription before deleting this account."
        )

    has_stripe = owner.stripe_subscription_id or (
        owner.has_billing_account
        and StripeBilling.objects.filter(account=owner.account, is_active=True).exists()
    )
    if has_stripe:
        raise ValidationError(
            "Cancel or downgrade your subscription before deleting this account."
        )


class DeleteOwnerInteractor(BaseInteractor):
    def validate(self) -> None:
        if not self.current_user.is_authenticated:
            raise Unauthenticated()

    @sync_to_async
    def execute(self, username: str) -> None:
        self.validate()

        owner = Owner.objects.filter(service=self.service, username=username).first()
        if not owner:
            raise NotFound()

        # `ensure_is_admin` permits deleting a personal account (the current
        # owner deleting themselves) as well as an organization the current
        # owner administers.
        self.ensure_is_admin(owner)
        _validate_owner_can_be_deleted(owner)

        # Route through the same code path the Django admin uses so that
        # self-serve deletions are marked, obfuscated, and hard-deleted
        # identically to staff-initiated deletions.
        TaskService().delete_owner(ownerid=owner.ownerid)
