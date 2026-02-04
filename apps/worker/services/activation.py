import logging

from sqlalchemy import func
from sqlalchemy.sql import text

from app import celery_app
from helpers.environment import is_enterprise
from shared.celery_config import (
    activate_account_user_task_name,
    new_user_activated_task_name,
)

log = logging.getLogger(__name__)


def activate_user(db_session, org_ownerid: int, user_ownerid: int) -> bool:
    """
    Attempt to activate the user for the given org

    Returns:
        bool: was the user successfully activated
    """
    if is_enterprise():
        # Enterprise/self-hosted: directly activate the user
        # Use coalesce to handle NULL plan_activated_users (new orgs)
        query_string = text(
            """
            UPDATE owners
            SET plan_activated_users = array_append_unique(coalesce(plan_activated_users, '{}'::int[]), :user_ownerid)
            WHERE ownerid = :org_ownerid
            RETURNING ownerid,
                      plan_activated_users,
                      username,
                      plan_activated_users @> array[:user_ownerid]::int[] as has_access;
            """
        )
        result = db_session.execute(
            query_string,
            {"user_ownerid": user_ownerid, "org_ownerid": org_ownerid},
        ).fetchone()

        # result is None if org_ownerid doesn't exist, or a row with has_access indicating success
        activation_success = result is not None and result.has_access

        log.info(
            "Enterprise auto activation attempted",
            extra={
                "org_ownerid": org_ownerid,
                "author_ownerid": user_ownerid,
                "activation_success": activation_success,
            },
        )

        return activation_success

    (activation_success,) = db_session.query(
        func.public.try_to_auto_activate(org_ownerid, user_ownerid)
    ).first()

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
    celery_app.send_task(
        activate_account_user_task_name,
        args=None,
        kwargs={
            "user_ownerid": user_ownerid,
            "org_ownerid": org_ownerid,
        },
    )
