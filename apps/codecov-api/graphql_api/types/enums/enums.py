import enum
from typing import Self

from shared.upload.constants import UploadErrorCode as SharedUploadErrorCode


class OrderingParameter(enum.Enum):
    NAME = "name"
    COVERAGE = "coverage"
    HITS = "hits"
    MISSES = "misses"
    PARTIALS = "partials"
    LINES = "lines"


class TestResultsFilterParameter(enum.Enum):
    __test__ = False

    FLAKY_TESTS = "flaky_tests"
    FAILED_TESTS = "failed_tests"
    SLOWEST_TESTS = "slowest_tests"
    SKIPPED_TESTS = "skipped_tests"


class TestResultsOrderingParameter(enum.Enum):
    __test__ = False

    TOTAL_DURATION = "total_duration"
    LAST_DURATION = "last_duration"
    AVG_DURATION = "avg_duration"
    FAILURE_RATE = "failure_rate"
    FLAKE_RATE = "flake_rate"
    COMMITS_WHERE_FAIL = "commits_where_fail"
    UPDATED_AT = "updated_at"


class PathContentDisplayType(enum.Enum):
    TREE = "tree"
    LIST = "list"


class RepositoryOrdering(enum.Enum):
    COMMIT_DATE = "latest_commit_at"
    COVERAGE = "coverage"
    ID = "repoid"
    NAME = "name"


class OrderingDirection(enum.Enum):
    ASC = "ascending"
    DESC = "descending"


class CoverageLine(enum.Enum):
    H = "hit"
    M = "miss"
    P = "partial"


class TypeProjectOnboarding(enum.Enum):
    PERSONAL = "PERSONAL"
    YOUR_ORG = "YOUR_ORG"
    OPEN_SOURCE = "OPEN_SOURCE"
    EDUCATIONAL = "EDUCATIONAL"


class GoalOnboarding(enum.Enum):
    STARTING_WITH_TESTS = "STARTING_WITH_TESTS"
    IMPROVE_COVERAGE = "IMPROVE_COVERAGE"
    MAINTAIN_COVERAGE = "MAINTAIN_COVERAGE"
    TEAM_REQUIREMENTS = "TEAM_REQUIREMENTS"
    OTHER = "OTHER"


class PullRequestState(enum.Enum):
    OPEN = "open"
    CLOSED = "closed"
    MERGED = "merged"


class UploadState(enum.Enum):
    STARTED = "started"
    UPLOADED = "uploaded"
    PROCESSED = "processed"
    ERROR = "error"
    COMPLETE = "complete"


class UploadType(enum.Enum):
    UPLOADED = "uploaded"
    CARRIEDFORWARD = "carriedforward"


UploadErrorEnum = SharedUploadErrorCode


class LoginProvider(enum.Enum):
    GITHUB = "github"
    GITHUB_ENTERPRISE = "github_enterprise"
    GITLAB = "gitlab"
    GITLAB_ENTERPRISE = "gitlab_enterprise"
    BITBUCKET = "bitbucket"
    BITBUCKET_SERVER = "bitbucket_server"
    OKTA = "okta"


class SyncProvider(enum.Enum):
    GITHUB = "github"
    GITHUB_ENTERPRISE = "github_enterprise"
    GITLAB = "gitlab"
    GITLAB_ENTERPRISE = "gitlab_enterprise"
    BITBUCKET = "bitbucket"
    BITBUCKET_SERVER = "bitbucket_server"


class CommitErrorGeneralType(enum.Enum):
    yaml_error = "YAML_ERROR"
    bot_error = "BOT_ERROR"


class CommitErrorCode(enum.Enum):
    invalid_yaml = ("invalid_yaml", CommitErrorGeneralType.yaml_error)
    yaml_client_error = ("yaml_client_error", CommitErrorGeneralType.yaml_error)
    yaml_unknown_error = ("yaml_unknown_error", CommitErrorGeneralType.yaml_error)
    repo_bot_invalid = ("repo_bot_invalid", CommitErrorGeneralType.bot_error)

    def __init__(self, db_string: str, error_type: CommitErrorGeneralType):
        self.db_string = db_string
        self.error_type = error_type

    @classmethod
    def get_codes_from_type(cls, error_type: CommitErrorGeneralType) -> list[Self]:
        return [item for item in cls if item.error_type == error_type]


class CommitStatus(enum.Enum):
    COMPLETED = "COMPLETED"
    ERROR = "ERROR"
    PENDING = "PENDING"


class BundleLoadTypes(enum.Enum):
    ENTRY = "ENTRY"
    INITIAL = "INITIAL"
    LAZY = "LAZY"


class AssetOrdering(enum.Enum):
    NAME = "name"
    SIZE = "size"
    TYPE = "asset_type"
