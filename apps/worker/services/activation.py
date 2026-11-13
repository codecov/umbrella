import logging

from sqlalchemy import func
from sqlalchemy.exc import OperationalError
from sqlalchemy.sql import text

from app import celery_app
from services.license import (
    calculate_reason_for_not_being_valid,
    get_current_license,
    get_installation_plan_activated_users,
    requires_license,
)
from shared.celery_config import (
    activate_account_user_task_name,
    new_user_activated_task_name,
)

log = logging.getLogger(__name__)

# Maximum time to wait for the org's owners row lock during auto-activation.
# Without this, the UPDATE in try_to_auto_activate can queue behind another
# long-running transaction on the same row for minutes, pinning a DB connection
# and contributing to database-wide stalls.
AUTO_ACTIVATE_LOCK_TIMEOUT = "2s"

# Postgres SQLSTATE for lock_not_available (raised when lock_timeout expires)
LOCK_NOT_AVAILABLE_PGCODE = "55P03"


def _is_user_already_activated(db_session, org_ownerid: int, user_ownerid: int) -> bool:
    """
    Plain (non-locking) read to check whether the user is already activated,
    so we can skip the row-locking UPDATE entirely in the common case.
    """
    row = db_session.execute(
        text(
            """
            SELECT coalesce(plan_activated_users @> array[:user_ownerid]::int[], false)
            FROM owners
            WHERE ownerid = :org_ownerid
            """
        ),
        {"user_ownerid": user_ownerid, "org_ownerid": org_ownerid},
    ).first()
    return bool(row and row[0])


def _try_to_auto_activate_with_lock_timeout(
    db_session, org_ownerid: int, user_ownerid: int
) -> bool:
    """
    Calls try_to_auto_activate inside a savepoint with a bounded lock_timeout,
    restoring the previous lock_timeout afterwards so the rest of the
    transaction is unaffected.
    """
    previous_lock_timeout = db_session.execute(text("SHOW lock_timeout")).scalar()
    try:
        with db_session.begin_nested():
            db_session.execute(
                text("SELECT set_config('lock_timeout', :timeout, true)"),
                {"timeout": AUTO_ACTIVATE_LOCK_TIMEOUT},
            )
            result = db_session.query(
                func.public.try_to_auto_activate(org_ownerid, user_ownerid)
            ).first()
        return bool(result and result[0])
    except OperationalError as exc:
        pgcode = getattr(getattr(exc, "orig", None), "pgcode", None)
        if pgcode != LOCK_NOT_AVAILABLE_PGCODE:
            raise
        log.warning(
            "Auto activation skipped: timed out waiting for owners row lock",
            extra={
                "org_ownerid": org_ownerid,
                "author_ownerid": user_ownerid,
                "lock_timeout": AUTO_ACTIVATE_LOCK_TIMEOUT,
            },
        )
        return False
    finally:
        db_session.execute(
            text("SELECT set_config('lock_timeout', :timeout, true)"),
            {"timeout": previous_lock_timeout},
        )


def activate_user(db_session, org_ownerid: int, user_ownerid: int) -> bool:
    """
    Attempt to activate the user for the given org

    Returns:
        bool: was the user successfully activated
    """
    if requires_license():
        # we will not activate if the license is invalid for any reason.
        license_status = calculate_reason_for_not_being_valid(db_session)
        if license_status is None:
            # check if you have an available seat with which to activate.
            seat_query = get_installation_plan_activated_users(db_session)

            this_license = get_current_license()
            can_activate = all(
                result[0] < this_license.number_allowed_users for result in seat_query
            )
            # add user_ownerid to orgs, plan activated users.
            if can_activate:
                query_string = text(
                    """
                            UPDATE owners
                                            set plan_activated_users = array_append_unique(plan_activated_users, :user_ownerid)
                                            where ownerid=:org_ownerid
                                            returning ownerid, 
                                            plan_activated_users, 
                                            username, 
                                            plan_activated_users @> array[:user_ownerid]::int[] as has_access;"""
                )
                (activation_success,) = db_session.execute(
                    query_string,
                    {"user_ownerid": user_ownerid, "org_ownerid": org_ownerid},
                ).fetchall()

                log.info(
                    "PR Auto activation attempted",
                    extra={
                        "org_ownerid": org_ownerid,
                        "author_ownerid": user_ownerid,
                        "activation_success": activation_success,
                    },
                )

                return True
            else:
                log.info(
                    "Auto activation failed due to no seats remaining",
                    extra={
                        "org_ownerid": org_ownerid,
                        "author_ownerid": user_ownerid,
                        "activation_success": False,
                        "license_status": license_status,
                    },
                )
                return False

        else:
            log.info(
                "Auto activation failed due to invalid license",
                extra={
                    "org_ownerid": org_ownerid,
                    "author_ownerid": user_ownerid,
                    "activation_success": False,
                    "license_status": license_status,
                },
            )
            return False

    # TODO: we need to decide the best way for this logic to be shared across
    # worker and codecov-api - ideally moving logic from database to application layer
    if _is_user_already_activated(db_session, org_ownerid, user_ownerid):
        activation_success = True
    else:
        activation_success = _try_to_auto_activate_with_lock_timeout(
            db_session, org_ownerid, user_ownerid
        )

    log.info(
        "Auto activation attempted",
        extra={
            "org_ownerid": org_ownerid,
            "author_ownerid": user_ownerid,
            "activation_success": activation_success,
        },
    )
    return activation_success


def schedule_new_user_activated_task(org_ownerid, user_ownerid):
    celery_app.send_task(
        new_user_activated_task_name,
        args=None,
        kwargs={"org_ownerid": org_ownerid, "user_ownerid": user_ownerid},
    )
    # Activate the account user if it exists.
    celery_app.send_task(
        activate_account_user_task_name,
        args=None,
        kwargs={
            "user_ownerid": user_ownerid,
            "org_ownerid": org_ownerid,
        },
    )
