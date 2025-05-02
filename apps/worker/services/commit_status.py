from services.yaml import read_yaml_field
from shared.config import get_config


def _ci_providers() -> list[str]:
    providers = get_config("services", "ci_providers")
    if not providers:
        return []
    elif isinstance(providers, list):
        return providers
    else:
        return [p.strip() for p in providers.split(",")]


ENTERPRISE_DEFAULTS = set(filter(None, _ci_providers()))

CI_CONTEXTS = {
    "ci",
    "Codefresh",
    "wercker",
    "semaphoreci",
    "pull request validator (cloudbees)",
    "Taskcluster (pull_request)",
    "continuous-integration",
    "buildkite",
}

CI_DOMAINS = {
    "jenkins",
    "codefresh",
    "bitbucket",
    "teamcity",
    "buildkite",
    "taskcluster",
}

CI_EXCLUDE = {"styleci"}


class RepositoryCIFilter(object):
    def __init__(self, commit_yaml) -> None:
        ci = read_yaml_field(commit_yaml, ("codecov", "ci")) or []
        ci = set(ci) | ENTERPRISE_DEFAULTS
        self.exclude = (
            set((c[1:] for c in ci if c[0] == "!") if ci else []) | CI_EXCLUDE
        )
        self.include = set((c for c in ci if c[0] != "!") if ci else []) | CI_DOMAINS

    def __call__(self, status) -> bool:
        return self._filter(status)

    def _filter(self, status) -> bool:
        domain = ((status["url"] or "").split("/") + ["", "", ""])[2]
        if domain:
            # ignore.com in ('ignore.com',) || skip.domain.com in ('skip',)
            if domain in self.exclude or set(domain.split(".")) & self.exclude:
                return False

            elif domain in self.include or set(domain.split(".")) & self.include:
                return True

        if status["context"]:
            contexts = set(status["context"].split("/"))
            if status["context"] in self.exclude or contexts & self.exclude:
                return False

            elif status["context"] in self.include or contexts & (
                self.include | CI_CONTEXTS
            ):
                return True

            elif (
                "jenkins" in status["context"].lower() and "jenkins" not in self.exclude
            ):
                # url="", context="Jenkins2 - Build"
                return True

        return False
