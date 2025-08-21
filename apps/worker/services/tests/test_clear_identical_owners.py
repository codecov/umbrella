import uuid

from database.models import LoginSession, Owner
from database.tests.factories import OwnerFactory
from services.owner import clear_identical_owners


def create_owner(
    dbsession, *, service: str = "github", username: str = "username", service_id=None
) -> Owner:
    kwargs = {"service": service, "username": username}
    if service_id is not None:
        kwargs["service_id"] = service_id
    owner: Owner = OwnerFactory.create(**kwargs)
    dbsession.add(owner)
    dbsession.flush()
    return owner


def add_session(dbsession, owner: Owner, name: str) -> LoginSession:
    session = LoginSession(
        ownerid=owner.ownerid,
        name=name,
        token=uuid.uuid4(),
        session_type="login",
    )
    dbsession.add(session)
    dbsession.flush()
    return session


def count_sessions(dbsession, owner: Owner) -> int:
    return (
        dbsession.query(LoginSession)
        .filter(LoginSession.ownerid == owner.ownerid)
        .count()
    )


class TestClearIdenticalOwners:
    def test_clears_other_owner_and_sessions(self, dbsession):
        curr_owner = create_owner(dbsession, service_id="1001")
        other_owner = create_owner(dbsession, service_id="2002")

        add_session(dbsession, other_owner, "s1")
        add_session(dbsession, other_owner, "s2")
        add_session(dbsession, curr_owner, "keep")

        clear_identical_owners(
            dbsession, curr_owner, username="username", service="github"
        )
        dbsession.flush()

        refreshed_other = (
            dbsession.query(Owner).filter_by(ownerid=other_owner.ownerid).one()
        )
        assert refreshed_other.username is None
        assert count_sessions(dbsession, other_owner) == 0
        assert count_sessions(dbsession, curr_owner) == 1

    def test_noop_when_no_other_owner_with_username(self, dbsession):
        curr_owner = create_owner(dbsession, service_id="1001")
        clear_identical_owners(dbsession, curr_owner, username="bob", service="github")
        dbsession.flush()

        refreshed_curr = (
            dbsession.query(Owner).filter_by(ownerid=curr_owner.ownerid).one()
        )
        assert refreshed_curr.username == "username"

    def test_noop_when_duplicate_is_same_owner(self, dbsession):
        curr_owner = create_owner(dbsession, service_id="1001")
        add_session(dbsession, curr_owner, "keep")

        clear_identical_owners(
            dbsession, curr_owner, username="username", service="github"
        )
        dbsession.flush()

        assert count_sessions(dbsession, curr_owner) == 1
