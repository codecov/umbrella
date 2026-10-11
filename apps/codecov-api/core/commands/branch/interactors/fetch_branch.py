from asgiref.sync import sync_to_async

from codecov.commands.base import BaseInteractor


class FetchBranchInteractor(BaseInteractor):
    @sync_to_async
    def execute(self, repository, branch_name):
        if "\x00" in branch_name:
            return None
        return repository.branches.filter(name=branch_name).first()
