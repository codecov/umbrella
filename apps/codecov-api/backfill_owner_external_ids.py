"""Backfill `Owner.external_id` for every owner still missing one.

New owners get an `external_id` automatically via the model's `uuid.uuid4`
default, but rows that pre-date the column are NULL until this script runs.

Run it as a standalone shell script from `apps/codecov-api/`:

    python backfill_owner_external_ids.py

It is idempotent and safe to re-run: owners that already have an
`external_id` are skipped, so an interrupted run can simply be started again.
"""

import logging
import os
import time
import uuid

import django

from shared.django_apps.utils.config import get_settings_module

# Setup Django environment before importing any models.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", get_settings_module("codecov"))
django.setup()

from shared.django_apps.codecov_auth.models import Owner  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

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


def backfill_owner_external_ids(batch_size: int = BATCH_SIZE) -> int:
    total = Owner.objects.filter(external_id__isnull=True).count()
    if total == 0:
        logger.info("No owners need an external_id backfill.")
        return 0

    logger.info(f"Backfilling external_id for {total} owners...")

    last_ownerid = 0
    processed = 0
    start = time.monotonic()

    while True:
        # Keyset pagination on the primary key: updated rows drop out of the
        # filter and `ownerid__gt` guarantees forward progress even as rows are
        # backfilled underneath us.
        batch = list(
            Owner.objects.filter(external_id__isnull=True, ownerid__gt=last_ownerid)
            .order_by("ownerid")
            .only("ownerid")[:batch_size]
        )
        if not batch:
            break

        for owner in batch:
            owner.external_id = uuid.uuid4()

        Owner.objects.bulk_update(batch, ["external_id"])
        last_ownerid = batch[-1].ownerid
        processed += len(batch)

        elapsed = time.monotonic() - start
        rate = processed / elapsed if elapsed > 0 else 0
        eta = (total - processed) / rate if rate > 0 else 0
        pct = min(processed / total * 100, 100)
        logger.info(
            f"  {processed}/{total} ({pct:.1f}%) | "
            f"{rate:.0f}/s | elapsed {_format_duration(elapsed)} | "
            f"ETA {_format_duration(eta)}"
        )

    logger.info(f"Backfilled {processed} owner external_ids.")
    return processed


if __name__ == "__main__":
    backfill_owner_external_ids()
