import time

from django.core.management.base import BaseCommand, CommandParser
from django.db.models import Q

from shared.django_apps.codecov_auth.models import Owner, _generate_support_pin

PLACEHOLDER_PIN = "000000"


class Command(BaseCommand):
    help = (
        "Assign a real random support PIN to every owner still holding the "
        f"'{PLACEHOLDER_PIN}' placeholder (or NULL). Runs in keyset-paginated "
        "batches with an optional pause between them so it doesn't overload the "
        "database."
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--batch-size",
            type=int,
            default=1000,
            help="Number of owners to update per batch (default: 1000).",
        )
        parser.add_argument(
            "--sleep",
            type=float,
            default=0.0,
            help="Seconds to sleep between batches to ease DB load (default: 0).",
        )
        parser.add_argument(
            "--starting-ownerid",
            type=int,
            default=0,
            help="Resume from this ownerid (exclusive). Useful after an interrupt.",
        )
        parser.add_argument(
            "--placeholder",
            type=str,
            default=PLACEHOLDER_PIN,
            help=f"Placeholder value to replace (default: '{PLACEHOLDER_PIN}').",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report what would be updated without writing anything.",
        )

    def handle(self, *args, **options) -> None:
        batch_size: int = options["batch_size"]
        sleep_seconds: float = options["sleep"]
        placeholder: str = options["placeholder"]
        dry_run: bool = options["dry_run"]
        last_ownerid: int = options["starting_ownerid"]

        if batch_size < 1:
            self.stderr.write("--batch-size must be a positive integer")
            return

        pending = Owner.objects.filter(
            Q(support_pin=placeholder) | Q(support_pin__isnull=True)
        ).order_by("ownerid")

        total = 0
        while True:
            # Keyset pagination on the primary key: updated rows fall out of the
            # filter, and `ownerid__gt` guarantees forward progress.
            batch = list(
                pending.filter(ownerid__gt=last_ownerid).only("ownerid")[:batch_size]
            )
            if not batch:
                break

            for owner in batch:
                owner.support_pin = _generate_support_pin()

            last_ownerid = batch[-1].ownerid

            if dry_run:
                self.stdout.write(
                    f"[dry-run] would update {len(batch)} owners "
                    f"(through ownerid {last_ownerid})"
                )
            else:
                Owner.objects.bulk_update(batch, ["support_pin"])
                total += len(batch)
                self.stdout.write(
                    f"Updated {len(batch)} owners "
                    f"(through ownerid {last_ownerid}); {total} total"
                )

            if sleep_seconds > 0:
                time.sleep(sleep_seconds)

        if dry_run:
            self.stdout.write(self.style.SUCCESS("Dry run complete."))
        else:
            self.stdout.write(self.style.SUCCESS(f"Done. Updated {total} owners."))
