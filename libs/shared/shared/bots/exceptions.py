class RequestedGithubAppNotFound(Exception):
    pass


class OwnerWithoutValidBotError(Exception):
    pass


class NoConfiguredAppsAvailable(Exception):
    def __init__(
        self, apps_count: int, rate_limited_count: int, suspended_count: int
    ) -> None:
        self.apps_count = apps_count
        self.rate_limited_count = rate_limited_count
        self.suspended_count = suspended_count


class RepositoryWithoutValidBotError(Exception):
    pass


class UnsupportedRepoProviderError(RepositoryWithoutValidBotError):
    """Raised when no torngit adapter exists for a service (e.g. `to_be_deleted`).

    Subclasses `RepositoryWithoutValidBotError` so existing handlers degrade gracefully.
    """

    def __init__(self, service) -> None:
        super().__init__(f"Unsupported repository provider service: {service!r}")
        self.service = service
