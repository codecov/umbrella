import uuid

from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor
from codecov.commands.exceptions import Unauthorized, ValidationError
from codecov_auth.helpers import current_user_part_of_org


class SetUploadTokenInteractor(BaseInteractor):
    @sync_to_async
    def execute(self, owner_username: str, repo_name: str, token: str) -> None:
        try:
            token_uuid = uuid.UUID(token)
        except (ValueError, AttributeError):
            raise ValidationError("token must be a valid UUID")

        owner, repo = self.resolve_owner_and_repo(
            owner_username, repo_name, only_viewable=True
        )

        if not current_user_part_of_org(self.current_owner, owner):
            raise Unauthorized()

        repo.upload_token = token_uuid
        repo.save()