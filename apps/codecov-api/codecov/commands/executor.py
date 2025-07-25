from codecov.commands.base import BaseCommand
from codecov_auth.commands.owner import OwnerCommands
from codecov_auth.constants import USE_SENTRY_APP_INDICATOR
from codecov_auth.models import Owner, User
from compare.commands.compare import CompareCommands
from core.commands.branch import BranchCommands
from core.commands.commit import CommitCommands
from core.commands.component import ComponentCommands
from core.commands.flag import FlagCommands
from core.commands.pull import PullCommands
from core.commands.repository import RepositoryCommands
from utils.services import get_long_service_name

mapping = {
    "commit": CommitCommands,
    "owner": OwnerCommands,
    "repository": RepositoryCommands,
    "branch": BranchCommands,
    "compare": CompareCommands,
    "pull": PullCommands,
    "flag": FlagCommands,
    "component": ComponentCommands,
}


class Executor:
    def __init__(
        self,
        current_owner: Owner,
        service: str,
        current_user: User,
        should_use_sentry_app: bool = False,
    ):
        self.current_user = current_user
        self.current_owner = current_owner
        self.service = service
        self.should_use_sentry_app = should_use_sentry_app

    def get_command(self, namespace) -> BaseCommand:
        KlassCommand: type[BaseCommand] = mapping[namespace]
        return KlassCommand(
            self.current_owner,
            self.service,
            self.current_user,
            self.should_use_sentry_app,
        )


def get_executor_from_request(request):
    service_in_url = request.resolver_match.kwargs["service"]
    return Executor(
        current_owner=request.current_owner,
        service=get_long_service_name(service_in_url),
        current_user=request.user,
        should_use_sentry_app=getattr(request, USE_SENTRY_APP_INDICATOR, False),
    )


def get_executor_from_command(command: BaseCommand):
    return Executor(
        current_owner=command.current_owner,
        service=command.service,
        current_user=command.current_user,
        should_use_sentry_app=command.should_use_sentry_app,
    )
