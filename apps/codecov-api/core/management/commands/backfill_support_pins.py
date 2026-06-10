from django.core.management.base import BaseCommand

from shared.django_apps.codecov_auth.models import Owner, _generate_support_pin

PLACEHOLDER_PIN = "000000"
BATCH_SIZE = 1000


class Command(BaseCommand):
    help = (
        "Assign a random support PIN to every owner still on the "
        f"'{PLACEHOLDER_PIN}' placeholder. Idempotent: owners that already have "
        "a real PIN are skipped, so it is safe to re-run."
    )

    def handle(self, *args, **options) -> None:
        last_ownerid = 0
        total = 0

        while True:
            # Keyset pagination on the primary key: updated rows drop out of the
            # filter and `ownerid__gt` guarantees forward progress.
            batch = list(
                Owner.objects.filter(
                    support_pin=PLACEHOLDER_PIN, ownerid__gt=last_ownerid
                )
                .order_by("ownerid")
                .only("ownerid")[:BATCH_SIZE]
            )
            if not batch:
                break

            for owner in batch:
                owner.support_pin = _generate_support_pin()

            Owner.objects.bulk_update(batch, ["support_pin"])
            last_ownerid = batch[-1].ownerid
            total += len(batch)

        self.stdout.write(self.style.SUCCESS(f"Backfilled {total} support PINs."))
