from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import NotFound, Unauthenticated, ValidationError
from codecov_auth.models import Owner, Plan, PlanProviders, StripeBilling
from services.task import TaskService
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName
from shared.plan.service import PlanService

_FREE_PLAN_NAMES = frozenset(
    {
        DEFAULT_FREE_PLAN,
        PlanName.USERS_DEVELOPER.value,
        PlanName.FREE_PLAN_NAME.value,
        PlanName.BASIC_PLAN_NAME.value,
    }
)


def _is_on_free_plan(owner: Owner) -> bool:
    try:
        return PlanService(current_org=owner).is_free_plan
    except ValueError:
        plan = Plan.objects.filter(name=owner.plan).first()
        if plan is not None:
            return not plan.paid_plan
        return owner.plan in _FREE_PLAN_NAMES


def _validate_owner_can_be_deleted(owner: Owner) -> None:
    if owner.uses_invoice:
        raise ValidationError("Contact support to delete invoice-billed accounts.")

    if owner.root_organization is not None:
        return

    if _is_on_free_plan(owner):
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
