import logging

from sqlalchemy.orm import Session

import shared.torngit as torngit
from database.models import LoginSession, Owner
from helpers.token_refresh import get_token_refresh_callback
from shared.bots import get_adapter_auth_information
from shared.config import get_config, get_verify_ssl
from shared.django_apps.codecov_auth.models import Service
from shared.typings.torngit import (
    OwnerInfo,
    TorngitInstanceData,
)

log = logging.getLogger(__name__)


def get_owner_provider_service(
    owner, *, ignore_installation=False, additional_data=None
):
    _timeouts = [
        get_config("setup", "http", "timeouts", "connect", default=15),
        get_config("setup", "http", "timeouts", "receive", default=30),
    ]
    service = Service(owner.service)
    adapter_auth_info = get_adapter_auth_information(
        owner, ignore_installations=ignore_installation
    )
    if additional_data is None:
        additional_data = {}
    data = TorngitInstanceData(
        owner=OwnerInfo(
            service_id=owner.service_id, ownerid=owner.ownerid, username=owner.username
        ),
        installation=adapter_auth_info["selected_installation_info"],
        fallback_installations=adapter_auth_info["fallback_installations"],
        additional_data=additional_data,
    )

    adapter_params = dict(
        token=adapter_auth_info["token"],
        verify_ssl=get_verify_ssl(service.value),
        timeouts=_timeouts,
        oauth_consumer_token={
            "key": get_config(service, "client_id"),
            "secret": get_config(service, "client_secret"),
        },
        # if using integration we will use the integration token
        # not the owner's token
        on_token_refresh=get_token_refresh_callback(adapter_auth_info["token_owner"]),
        **data,
    )
    return _get_owner_provider_service_instance(service, **adapter_params)


def _get_owner_provider_service_instance(service_name, **adapter_params):
    return torngit.get(service_name, **adapter_params)


def clear_identical_owners(
    db_session: Session, curr_owner: Owner, username: str, service: str
):
    other_owner = (
        db_session.query(Owner)
        .filter(
            Owner.service == service,
            Owner.username == username,
            Owner.ownerid != curr_owner.ownerid,
        )
        .first()
    )

    if not other_owner:
        return

    # Lock both owner rows in a deterministic order (by ownerid) before
    # mutating them. Without this, concurrent workers touching the same
    # owners can acquire row locks in opposite orders and deadlock.
    (
        db_session.query(Owner)
        .filter(Owner.ownerid.in_([curr_owner.ownerid, other_owner.ownerid]))
        .order_by(Owner.ownerid)
        .with_for_update()
        .all()
    )
    # Another worker may have changed the username while we waited on the lock
    db_session.refresh(other_owner, attribute_names=["username"])
    if other_owner.username != username:
        return

    log.warning(
        "clear_identical_owners: Username already exists for another owner",
        extra={
            "service": service,
            "service_id": curr_owner.service_id,
            "username": username,
            "other_ownerid": other_owner.ownerid,
            "current_ownerid": curr_owner.ownerid,
        },
    )
    other_owner.username = None
    # Flush explicitly so the UPDATE happens here, under the locks we hold,
    # instead of as a hidden autoflush side effect of the delete below.
    db_session.flush()
    db_session.query(LoginSession).filter(
        LoginSession.ownerid == other_owner.ownerid
    ).delete(synchronize_session=False)
