import contextvars
import logging
from dataclasses import asdict, dataclass, field, replace

import sentry_sdk

from database.models.core import Owner, Repository
from shared.config import get_config
from shared.utils.sentry import current_sentry_trace_id

log = logging.getLogger("log_context")


@dataclass
class LogContext:
    """
    Class containing all the information we may want to add in logs and metrics.
    """

    task_name: str = "???"
    task_id: str = "???"

    _populated_from_db = False
    owner_username: str | None = None
    owner_service: str | None = None
    owner_plan: str | None = None
    owner_id: int | None = None
    repo_name: str | None = None
    repo_id: int | None = None
    commit_sha: str | None = None

    # TODO: begin populating this again or remove it
    # we can populate this again if we reduce query load by passing the Commit,
    # Owner, and Repository models we use to populate the log context into the
    # various task implementations so they don't fetch the same models again
    commit_id: int | None = None

    checkpoints_data: dict = field(default_factory=lambda: {})

    @property
    def sentry_trace_id(self):
        return current_sentry_trace_id()

    def as_dict(self):
        d = asdict(self)
        d.pop("_populated_from_db", None)
        d["sentry_trace_id"] = self.sentry_trace_id
        return d

    def populate_from_sqlalchemy(self, dbsession):
        """
        Attempt to use the information we have to fill in other context fields. For
        example, if we have `self.repo_id` but not `self.owner_id`, we can look up
        the latter in the database.

        Ignore exceptions; no need to fail a task for a missing context field.
        """
        if self._populated_from_db or not get_config(
            "setup", "log_context", "populate", default=True
        ):
            return

        try:
            # - commit_id (or commit_sha + repo_id) is enough to get everything else
            #   - however, this slams the commit table and we rarely really need the
            #     DB PK for a commit, so we don't do this.
            # - repo_id is enough to get repo and owner
            # - owner_id is just enough to get owner

            if self.repo_id:
                query = (
                    dbsession.query(
                        Repository.name,
                        Owner.ownerid,
                        Owner.username,
                        Owner.service,
                        Owner.plan,
                    )
                    .join(Repository.owner)
                    .filter(Repository.repoid == self.repo_id)
                )

                (
                    self.repo_name,
                    self.owner_id,
                    self.owner_username,
                    self.owner_service,
                    self.owner_plan,
                ) = query.first()

            elif self.owner_id:
                query = dbsession.query(
                    Owner.username, Owner.service, Owner.plan
                ).filter(Owner.ownerid == self.owner_id)

                (self.owner_username, self.owner_service, self.owner_plan) = (
                    query.first()
                )

        except Exception:
            log.exception("Failed to populate log context")

        self._populated_from_db = True

    def add_to_log_record(self, log_record: dict):
        d = self.as_dict()
        d.pop("checkpoints_data", None)
        log_record["context"] = d

    def add_to_sentry(self):
        d = self.as_dict()
        d.pop("sentry_trace_id", None)
        d.pop("checkpoints_data", None)
        sentry_sdk.set_tags(d)


_log_context = contextvars.ContextVar("log_context", default=LogContext())


def set_log_context(context: LogContext):
    """
    Overwrite whatever is currently in the log context. Also sets tags in the
    Sentry SDK appropriately.
    """
    _log_context.set(context)
    context.add_to_sentry()


def update_log_context(context: dict):
    """
    Add new fields to the log context without removing old ones.
    """
    current_context: LogContext = _log_context.get()
    new_context = replace(current_context, **context)
    set_log_context(new_context)


def get_log_context() -> LogContext:
    """
    Access the log context.
    """
    return _log_context.get()
