import pytest

from backfill_owner_external_ids import backfill_owner_external_ids
from shared.django_apps.codecov_auth.models import Owner
from shared.django_apps.core.tests.factories import OwnerFactory


def _clear_external_id(owner: Owner) -> None:
    # New owners get an external_id from the model default, so bypass it with a
    # queryset update to simulate a pre-migration row.
    Owner.objects.filter(pk=owner.pk).update(external_id=None)


@pytest.mark.django_db
def test_backfill_owner_external_ids_populates_missing():
    missing_one = OwnerFactory()
    missing_two = OwnerFactory()
    already_set = OwnerFactory()
    _clear_external_id(missing_one)
    _clear_external_id(missing_two)
    existing_external_id = already_set.external_id
    assert existing_external_id is not None

    processed = backfill_owner_external_ids()

    missing_one.refresh_from_db()
    missing_two.refresh_from_db()
    already_set.refresh_from_db()

    # Owners without an external_id get a fresh, unique UUID.
    assert missing_one.external_id is not None
    assert missing_two.external_id is not None
    assert missing_one.external_id != missing_two.external_id

    # Owners that already had one are left untouched.
    assert already_set.external_id == existing_external_id

    # OwnerFactory may create extra owners (e.g. bots) that already have an id,
    # so only assert our two cleared rows were among those processed.
    assert processed >= 2


@pytest.mark.django_db
def test_backfill_owner_external_ids_is_idempotent():
    owner = OwnerFactory()
    _clear_external_id(owner)

    backfill_owner_external_ids()
    owner.refresh_from_db()
    first_external_id = owner.external_id
    assert first_external_id is not None

    # A second run finds nothing to backfill and leaves the value untouched.
    processed = backfill_owner_external_ids()
    owner.refresh_from_db()
    assert owner.external_id == first_external_id
    assert processed == 0


@pytest.mark.django_db
def test_backfill_owner_external_ids_updates_in_bulk(mocker):
    bulk_update = mocker.spy(Owner.objects, "bulk_update")
    for _ in range(3):
        owner = OwnerFactory()
        _clear_external_id(owner)

    backfill_owner_external_ids()

    # All cleared owners fit in a single batch -> one bulk_update call.
    assert bulk_update.call_count == 1
