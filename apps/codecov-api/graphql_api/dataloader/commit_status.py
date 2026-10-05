from asgiref.sync import sync_to_async

from graphql_api.actions.commits import load_commit_statuses
from reports.models import CommitReport

from .loader import BaseLoader


class CommitStatusLoader(BaseLoader):
    """
    Batch loads the per-report-type statuses for commits, keyed by commit `id`.

    Each loaded value is a `dict[CommitReport.ReportType, str]`.
    """

    @sync_to_async
    def batch_load_fn(
        self, keys: list[int]
    ) -> list[dict[CommitReport.ReportType, str]]:
        statuses = load_commit_statuses(list(keys))
        # the returned list must be in the exact order of `keys`
        return [statuses.get(key, {}) for key in keys]
