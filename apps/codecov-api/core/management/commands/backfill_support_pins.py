import time

from django.core.management.base import BaseCommand

from shared.django_apps.codecov_auth.models import Owner, _generate_support_pin

PLACEHOLDER_PIN = "000000"
BATCH_SIZE = 1000


def _format_duration(seconds: float) -> str:
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


class Command(BaseCommand):
    help = (
        "Assign a random support PIN to every owner still on the "
        f"'{PLACEHOLDER_PIN}' placeholder. Idempotent: owners that already have "
        "a real PIN are skipped, so it is safe to re-run."
    )

    def handle(self, *args, **options) -> None:
        total = Owner.objects.filter(support_pin=PLACEHOLDER_PIN).count()
        if total == 0:
            self.stdout.write(
                self.style.SUCCESS("No owners need a support PIN backfill.")
            )
            return

        self.stdout.write(f"Backfilling support PINs for {total} owners...")

        last_ownerid = 0
        processed = 0
        start = time.monotonic()

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
            processed += len(batch)

            elapsed = time.monotonic() - start
            rate = processed / elapsed if elapsed > 0 else 0
            eta = (total - processed) / rate if rate > 0 else 0
            pct = min(processed / total * 100, 100)
            self.stdout.write(
                f"  {processed}/{total} ({pct:.1f}%) | "
                f"{rate:.0f}/s | elapsed {_format_duration(elapsed)} | "
                f"ETA {_format_duration(eta)}"
            )

        self.stdout.write(self.style.SUCCESS(f"Backfilled {processed} support PINs."))
