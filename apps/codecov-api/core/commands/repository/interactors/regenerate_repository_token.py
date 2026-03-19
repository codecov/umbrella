from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import Unauthorized
from codecov_auth.helpers import current_user_part_of_org
from codecov_auth.models import RepositoryToken


class RegenerateRepositoryTokenInteractor(BaseInteractor):
    @sync_to_async
    def execute(self, repo_name: str, owner_username: str, token_type: str):
        owner, repo = self.resolve_owner_and_repo(
            owner_username, repo_name, only_viewable=True, only_active=True
        )

        if not current_user_part_of_org(self.current_owner, owner):
            raise Unauthorized()

        token, created = RepositoryToken.objects.get_or_create(
            repository_id=repo.repoid, token_type=token_type
        )
        if not created:
            token.key = token.generate_key()
            token.save()
        return token.key
