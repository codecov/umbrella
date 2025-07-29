from abc import ABC, abstractmethod

from django.contrib.auth.models import Group, Permission
from django.db.models.manager import EmptyManager

from core.models import Repository
from shared.django_apps.codecov_auth.models import TokenTypeChoices


class RepositoryAsUser:
    def __init__(self, repository: Repository):
        self._repository = repository

    def is_authenticated(self) -> bool:
        return True


class RepositoryAuthInterface(ABC):
    @abstractmethod
    def get_scopes(self) -> list[str] | list[TokenTypeChoices]:
        pass

    @abstractmethod
    def get_repositories(self) -> list[Repository]:
        pass

    @abstractmethod
    def allows_repo(self, repository: Repository) -> bool:
        pass


class DjangoUser:
    id = None
    pk = None
    is_staff = False
    is_superuser = False
    is_active = False
    _groups = EmptyManager(Group)
    _user_permissions = EmptyManager(Permission)

    @property
    def is_anonymous(self) -> bool:
        return False

    @property
    def is_authenticated(self) -> bool:
        return False

    @property
    def groups(self) -> bool:
        return False

    @property
    def user_permissions(self) -> bool:
        return False

    def get_user_permissions(self, obj=None):
        return False

    def get_group_permissions(self, obj=None):
        return False

    def get_all_permissions(self, obj=None):
        return False

    def has_perm(self, perm, obj=None):
        return False

    def has_perms(self, perm_list, obj=None):
        return False


class SuperUser(DjangoUser):
    is_super_user = True

    pass


class InternalUser(DjangoUser):
    is_internal_user = True

    pass


class DjangoToken:
    def __init__(self, token=None):
        self.token = token


class SuperToken(DjangoToken):
    is_super_token = True

    pass


class InternalToken(DjangoToken):
    is_internal_token = True

    pass
