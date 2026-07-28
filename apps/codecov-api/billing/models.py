from shared.django_apps.codecov_auth.models import Owner


class StripeBilling(Owner):
    """Owner-backed Stripe customer lookup for the Billing admin."""

    class Meta:
        proxy = True
        app_label = "billing"
        verbose_name = "Stripe billing"
        verbose_name_plural = "Stripe billing"

    def __str__(self) -> str:
        return self.stripe_customer_id or str(self.ownerid)
