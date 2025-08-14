import pytest

from shared.django_apps.codecov_auth.tests.factories import OwnerFactory


class TestAdminHelpers:
    @pytest.mark.django_db
    def test_update_single_owner_admins_no_admins(self):
        owner = OwnerFactory.create(admins=None)
        original_admins = owner.admins

        owner.update_admins()

        owner.refresh_from_db()
        assert owner.admins == original_admins

    @pytest.mark.django_db
    def test_update_single_owner_admins_valid_admins(self):
        owner = OwnerFactory.create()

        admin1 = OwnerFactory.create(organizations=[owner.ownerid, 4])
        admin2 = OwnerFactory.create(organizations=[owner.ownerid, 5])

        owner.admins = [admin1.ownerid, admin2.ownerid]
        owner.save()

        owner.update_admins()

        owner.refresh_from_db()
        assert set(owner.admins) == {admin1.ownerid, admin2.ownerid}

    @pytest.mark.django_db
    def test_update_single_owner_admins_remove_invalid_admin(self):
        owner = OwnerFactory.create()

        admin1 = OwnerFactory.create(organizations=[999, 1000])
        admin2 = OwnerFactory.create(organizations=[owner.ownerid, 555])

        owner.admins = [admin1.ownerid, admin2.ownerid]
        owner.save()

        owner.update_admins()

        owner.refresh_from_db()
        assert len(owner.admins) == 1
        assert owner.admins[0] == admin2.ownerid

    @pytest.mark.django_db
    def test_update_org_admins_calls_single_for_each_owner(self):
        owner1 = OwnerFactory.create(admins=None)

        owner2 = OwnerFactory.create()
        valid_admin = OwnerFactory.create(organizations=[owner2.ownerid])
        owner2.admins = [valid_admin.ownerid]
        owner2.save()

        owner3 = OwnerFactory.create()
        invalid_admin = OwnerFactory.create(organizations=[999])
        owner3.admins = [invalid_admin.ownerid]
        owner3.save()

        owners = [owner1, owner2, owner3]
        for owner in owners:
            owner.update_admins()

        owner1.refresh_from_db()
        owner2.refresh_from_db()
        owner3.refresh_from_db()

        assert owner1.admins is None
        assert owner2.admins == [valid_admin.ownerid]
        assert owner3.admins == []
