import logging

from sqlalchemy.orm import Session

from database.models import Owner

log = logging.getLogger(__name__)


def update_single_owner_admins(db_session: Session, owner: Owner) -> None:
    if not owner.admins:
        return

    admin_ids = owner.admins if owner.admins is not None else []
    admins = db_session.query(Owner).filter(Owner.ownerid.in_(admin_ids)).all()
    valid_admin_ids: set[int] = set()

    for admin in admins:
        if admin.organizations and owner.ownerid in admin.organizations:
            valid_admin_ids.add(admin.ownerid)
        else:
            log.warning(
                "Suppressing billing email to admin not in organization",
                extra={
                    "org_owner_id": owner.ownerid,
                    "admin_owner_id": admin.ownerid,
                },
            )

    if valid_admin_ids != set(owner.admins):
        owner.admins = list(valid_admin_ids)
        db_session.flush()
