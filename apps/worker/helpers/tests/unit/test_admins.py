import pytest

from database.tests.factories import OwnerFactory
from helpers.admins import update_single_owner_admins


class TestAdminHelpers:
    @pytest.mark.parametrize("admins", [None, []])
    def test_update_single_owner_admins_no_admins(self, dbsession, admins):
        owner = OwnerFactory()
        owner.admins = admins
        dbsession.add(owner)
        dbsession.commit()

        original_admins = owner.admins

        update_single_owner_admins(dbsession, owner)

        dbsession.refresh(owner)
        assert owner.admins == original_admins

    def test_update_single_owner_admins_valid_admins(self, dbsession):
        owner = OwnerFactory()
        dbsession.add(owner)
        dbsession.flush()

        admin1 = OwnerFactory()
        admin1.organizations = [owner.ownerid]
        admin2 = OwnerFactory()
        admin2.organizations = [owner.ownerid]

        dbsession.add_all([admin1, admin2])
        dbsession.flush()

        owner.admins = [admin1.ownerid, admin2.ownerid]
        dbsession.commit()

        update_single_owner_admins(dbsession, owner)

        dbsession.refresh(owner)
        assert set(owner.admins) == {admin1.ownerid, admin2.ownerid}

    def test_update_single_owner_admins_remove_invalid_admin(self, dbsession):
        owner = OwnerFactory()
        dbsession.add(owner)
        dbsession.flush()

        admin1 = OwnerFactory()
        admin1.organizations = [4, 5]
        admin2 = OwnerFactory()
        admin2.organizations = [owner.ownerid, 5]

        dbsession.add_all([admin1, admin2])
        dbsession.flush()

        owner.admins = [admin1.ownerid, admin2.ownerid]
        dbsession.commit()

        update_single_owner_admins(dbsession, owner)

        dbsession.refresh(owner)
        assert owner.admins == [admin2.ownerid]

    def test_update_single_owner_admins_remove_all_invalid_admins(self, dbsession):
        owner = OwnerFactory()
        dbsession.add(owner)
        dbsession.flush()

        admin1 = OwnerFactory()
        admin1.organizations = [999]
        admin2 = OwnerFactory()
        admin2.organizations = [888]

        dbsession.add_all([admin1, admin2])
        dbsession.flush()

        owner.admins = [admin1.ownerid, admin2.ownerid]
        dbsession.commit()

        update_single_owner_admins(dbsession, owner)

        dbsession.refresh(owner)
        assert owner.admins == []
