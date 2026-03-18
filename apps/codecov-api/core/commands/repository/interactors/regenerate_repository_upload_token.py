import uuid

from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import Unauthorized
from codecov_auth.helpers import current_user_part_of_org


class RegenerateRepositoryUploadTokenInteractor(BaseInteractor):
    @sync_to_async
    def execute(self, repo_name: str, owner_username: str) -> uuid.UUID:
        owner, repo = self.resolve_owner_and_repo(
            owner_username, repo_name, only_viewable=True
        )

        if not current_user_part_of_org(self.current_owner, owner):
            raise Unauthorized()

        repo.upload_token = uuid.uuid4()
        repo.save()
        return repo.upload_token
