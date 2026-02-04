from database.tests.factories import OwnerFactory
from services.activation import activate_user


class TestActivationServiceTestCase:
    def test_activate_user_no_seats(
        self, request, dbsession, mocker, with_sql_functions
    ):
        org = OwnerFactory.create(
            plan_user_count=0, plan_activated_users=[], plan_auto_activate=True
        )
        user = OwnerFactory.create_from_test_request(request)
        dbsession.add(org)
        dbsession.add(user)
        dbsession.flush()

        was_activated = activate_user(dbsession, org.ownerid, user.ownerid)
        assert was_activated is False
        dbsession.commit()
        assert user.ownerid not in org.plan_activated_users

    def test_activate_user_success(
        self, request, dbsession, mocker, with_sql_functions
    ):
        org = OwnerFactory.create(
            plan_user_count=1, plan_activated_users=[], plan_auto_activate=True
        )
        user = OwnerFactory.create_from_test_request(request)
        dbsession.add(org)
        dbsession.add(user)
        dbsession.flush()

        was_activated = activate_user(dbsession, org.ownerid, user.ownerid)
        assert was_activated is True
        dbsession.commit()
        assert user.ownerid in org.plan_activated_users

    def test_activate_user_success_for_users_free(
        self, request, dbsession, mocker, with_sql_functions
    ):
        org = OwnerFactory.create(
            plan="users-free",
            plan_user_count=1,
            plan_activated_users=None,
            plan_auto_activate=True,
        )
        user = OwnerFactory.create_from_test_request(request)
        dbsession.add(org)
        dbsession.add(user)
        dbsession.flush()

        was_activated = activate_user(dbsession, org.ownerid, user.ownerid)
        assert was_activated is True
        dbsession.commit()
        assert user.ownerid in org.plan_activated_users

    def test_activate_user_success_for_enterprise(
        self, request, dbsession, mocker, with_sql_functions
    ):
        """Enterprise/self-hosted activation succeeds."""
        mocker.patch("services.activation.is_enterprise", return_value=True)

        org = OwnerFactory.create(
            service="github",
            oauth_token=None,
            plan_activated_users=list(range(15, 20)),
            plan_auto_activate=True,
        )
        dbsession.add(org)
        dbsession.flush()

        user = OwnerFactory.create_from_test_request(request)
        dbsession.add(org)
        dbsession.add(user)
        dbsession.flush()

        was_activated = activate_user(dbsession, org.ownerid, user.ownerid)
        assert was_activated is True
        dbsession.commit()
        assert user.ownerid in org.plan_activated_users

    def test_activate_user_enterprise_multiple_orgs(
        self, request, dbsession, mocker, with_sql_functions
    ):
        """Enterprise activation works across multiple orgs."""
        mocker.patch("services.activation.is_enterprise", return_value=True)

        org = OwnerFactory.create(
            service="github",
            oauth_token=None,
            plan_activated_users=list(range(1, 6)),
            plan_auto_activate=True,
        )
        dbsession.add(org)
        dbsession.flush()

        org_second = OwnerFactory.create(
            service="github",
            oauth_token=None,
            plan_activated_users=list(range(2, 8)),
            plan_auto_activate=True,
        )
        dbsession.add(org_second)
        dbsession.flush()

        user = OwnerFactory.create_from_test_request(request)
        dbsession.add(user)
        dbsession.flush()

        was_activated = activate_user(dbsession, org_second.ownerid, user.ownerid)
        assert was_activated is True
        dbsession.commit()

        was_activated = activate_user(dbsession, org.ownerid, user.ownerid)
        assert was_activated is True
        dbsession.commit()

    def test_activate_user_enterprise_nonexistent_org(
        self, request, dbsession, mocker, with_sql_functions
    ):
        """Enterprise activation fails when org doesn't exist."""
        mocker.patch("services.activation.is_enterprise", return_value=True)

        user = OwnerFactory.create_from_test_request(request)
        dbsession.add(user)
        dbsession.flush()

        # Use a non-existent org_ownerid
        nonexistent_org_id = 999999999
        was_activated = activate_user(dbsession, nonexistent_org_id, user.ownerid)
        assert was_activated is False
