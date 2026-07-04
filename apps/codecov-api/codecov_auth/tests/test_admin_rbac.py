"""Tests for the Django admin RBAC levels (Viewer / Member / Admin).

Covers the `User.staff_role` sync rules and the central `RBACAdminMixin` /
`CodecovAdminSite` enforcement (read-only Viewers, no actions, Redis models
hidden, and custom action views denied).
"""

import pytest
from django.contrib import admin
from django.test import RequestFactory, TestCase

from codecov_auth.admin import StaffRoleListFilter
from codecov_auth.models import Owner, User
from redis_admin.models import RedisQueue
from shared.django_apps.codecov_auth.tests.factories import UserFactory
from shared.django_apps.core.tests.factories import CommitFactory, RepositoryFactory
from utils.test_utils import Client


def _request_for(user):
    request = RequestFactory().get("/admin/")
    request.user = user
    return request


# Model: staff_role sync with is_superuser (is_admin)


@pytest.mark.django_db
def test_superuser_forces_admin_role():
    user = UserFactory(is_staff=True, is_superuser=True, staff_role="member")
    user.refresh_from_db()

    assert user.staff_role == User.StaffRole.ADMIN
    assert user.effective_staff_role == User.StaffRole.ADMIN


@pytest.mark.django_db
def test_demoting_superuser_drops_to_viewer():
    user = UserFactory(is_staff=True, is_superuser=True)
    assert user.staff_role == User.StaffRole.ADMIN

    user.is_superuser = False
    user.save()
    user.refresh_from_db()

    assert user.staff_role == User.StaffRole.VIEWER
    assert user.effective_staff_role == User.StaffRole.VIEWER


@pytest.mark.django_db
def test_demotion_synced_even_with_update_fields():
    user = UserFactory(is_staff=True, is_superuser=True)

    user.is_superuser = False
    user.save(update_fields=["is_superuser"])
    user.refresh_from_db()

    assert user.staff_role == User.StaffRole.VIEWER


@pytest.mark.django_db
def test_effective_role_defaults_to_viewer_when_unset():
    user = UserFactory(is_staff=True, staff_role=None)

    assert user.effective_staff_role == User.StaffRole.VIEWER


@pytest.mark.django_db
def test_member_role_is_preserved():
    user = UserFactory(is_staff=True, staff_role="member")
    user.refresh_from_db()

    assert user.effective_staff_role == User.StaffRole.MEMBER


@pytest.mark.django_db
def test_non_staff_user_has_none_role():
    user = UserFactory(is_staff=False, staff_role="member")
    user.refresh_from_db()

    assert user.staff_role == User.StaffRole.NONE
    assert user.effective_staff_role == User.StaffRole.NONE


@pytest.mark.django_db
def test_losing_staff_drops_role_to_none():
    user = UserFactory(is_staff=True, staff_role="member")
    assert user.effective_staff_role == User.StaffRole.MEMBER

    user.is_staff = False
    user.save()
    user.refresh_from_db()

    assert user.staff_role == User.StaffRole.NONE
    assert user.effective_staff_role == User.StaffRole.NONE


# RBACAdminMixin: per-model permission enforcement


@pytest.mark.django_db
def test_viewer_has_no_write_permissions():
    viewer = UserFactory(is_staff=True, staff_role="viewer")
    request = _request_for(viewer)
    owner_admin = admin.site._registry[Owner]

    assert owner_admin.has_view_permission(request) is True
    assert owner_admin.has_add_permission(request) is False
    assert owner_admin.has_change_permission(request) is False
    assert owner_admin.has_delete_permission(request) is False


@pytest.mark.django_db
def test_viewer_has_no_actions():
    viewer = UserFactory(is_staff=True, staff_role="viewer")
    request = _request_for(viewer)
    owner_admin = admin.site._registry[Owner]

    assert owner_admin.get_actions(request) == {}


@pytest.mark.django_db
def test_member_keeps_actions_and_change_permission():
    member = UserFactory(is_staff=True, staff_role="member")
    request = _request_for(member)
    owner_admin = admin.site._registry[Owner]

    assert owner_admin.has_change_permission(request) is True
    assert owner_admin.get_actions(request) != {}


@pytest.mark.django_db
def test_viewer_cannot_see_redis_models_but_member_can():
    viewer = UserFactory(is_staff=True, staff_role="viewer")
    member = UserFactory(is_staff=True, staff_role="member")
    redis_admin_instance = admin.site._registry[RedisQueue]

    assert redis_admin_instance.has_view_permission(_request_for(viewer)) is False
    assert redis_admin_instance.has_module_permission(_request_for(viewer)) is False
    assert redis_admin_instance.has_view_permission(_request_for(member)) is True


@pytest.mark.django_db
def test_viewer_gets_all_fields_readonly():
    viewer = UserFactory(is_staff=True, staff_role="viewer")
    owner_admin = admin.site._registry[Owner]

    readonly = owner_admin.get_readonly_fields(_request_for(viewer))
    model_field_names = {field.name for field in Owner._meta.fields}

    assert model_field_names.issubset(set(readonly))


# HTTP-level enforcement


class ViewerHttpTest(TestCase):
    def setUp(self):
        self.client = Client()

    def test_viewer_blocked_from_redis_changelist(self):
        viewer = UserFactory(is_staff=True, staff_role="viewer")
        self.client.force_login(viewer)

        response = self.client.get("/admin/redis_admin/redisqueue/")

        assert response.status_code == 403

    def test_member_can_open_redis_changelist(self):
        member = UserFactory(is_staff=True, staff_role="member")
        self.client.force_login(member)

        response = self.client.get("/admin/redis_admin/redisqueue/")

        # Members retain the historical staff access to the Redis admin.
        assert response.status_code == 200

    def test_viewer_cannot_open_owner_add_page(self):
        viewer = UserFactory(is_staff=True, staff_role="viewer")
        self.client.force_login(viewer)

        response = self.client.get("/admin/codecov_auth/owner/add/")

        # Viewers are read-only: any add page is denied.
        assert response.status_code == 403

    def test_viewer_denied_reprocess_custom_view(self):
        viewer = UserFactory(is_staff=True, staff_role="viewer")
        self.client.force_login(viewer)
        repo = RepositoryFactory()
        commit = CommitFactory(repository=repo)

        response = self.client.get(
            f"/admin/core/commit/{commit.pk}/reprocess_coverage/"
        )

        assert response.status_code == 403

    def test_member_can_still_reach_reprocess_custom_view(self):
        member = UserFactory(is_staff=True, staff_role="member")
        self.client.force_login(member)
        repo = RepositoryFactory()
        commit = CommitFactory(repository=repo)

        response = self.client.get(
            f"/admin/core/commit/{commit.pk}/reprocess_coverage/"
        )

        # No coverage data to reprocess -> redirect back to the change page,
        # but crucially not a 403 (Members retain access).
        assert response.status_code == 302


# UserAdmin: role editing is restricted to superusers


@pytest.mark.django_db
def test_useradmin_role_fields_disabled_for_non_superuser():
    member = UserFactory(is_staff=True, staff_role="member")
    target = UserFactory(is_staff=True, staff_role="member")
    user_admin = admin.site._registry[User]

    form = user_admin.get_form(_request_for(member), obj=target)()

    assert form.fields["is_staff"].disabled
    assert form.fields["is_superuser"].disabled
    assert form.fields["staff_role"].disabled


@pytest.mark.django_db
def test_useradmin_role_choices_limited_for_non_superuser_target():
    superuser = UserFactory(is_staff=True, is_superuser=True)
    target = UserFactory(is_staff=True, staff_role="member")
    user_admin = admin.site._registry[User]

    form = user_admin.get_form(_request_for(superuser), obj=target)()
    choice_values = [value for value, _ in form.fields["staff_role"].choices]

    assert User.StaffRole.ADMIN.value not in choice_values
    assert User.StaffRole.VIEWER.value in choice_values
    assert User.StaffRole.MEMBER.value in choice_values


@pytest.mark.django_db
def test_useradmin_shows_admin_readonly_for_superuser_target():
    superuser = UserFactory(is_staff=True, is_superuser=True)
    target = UserFactory(is_staff=True, is_superuser=True)
    user_admin = admin.site._registry[User]

    form = user_admin.get_form(_request_for(superuser), obj=target)()
    choice_values = [value for value, _ in form.fields["staff_role"].choices]

    assert choice_values == [User.StaffRole.ADMIN.value]
    assert form.fields["staff_role"].disabled


@pytest.mark.django_db
def test_useradmin_shows_none_readonly_for_non_staff_target():
    superuser = UserFactory(is_staff=True, is_superuser=True)
    target = UserFactory(is_staff=False)
    user_admin = admin.site._registry[User]

    form = user_admin.get_form(_request_for(superuser), obj=target)()
    choice_values = [value for value, _ in form.fields["staff_role"].choices]

    assert choice_values == [User.StaffRole.NONE.value]
    assert form.fields["staff_role"].disabled


# StaffRoleListFilter: filter the users changelist by effective role


@pytest.mark.django_db
def test_staff_role_filter_buckets_users_by_effective_role():
    none_user = UserFactory(is_staff=False)
    viewer = UserFactory(is_staff=True, staff_role="viewer")
    member = UserFactory(is_staff=True, staff_role="member")
    admin_user = UserFactory(is_staff=True, is_superuser=True)
    user_admin = admin.site._registry[User]

    def ids_for(role):
        request = _request_for(admin_user)
        filt = StaffRoleListFilter(request, {"role": role}, User, user_admin)
        return set(
            filt.queryset(request, User.objects.all()).values_list("id", flat=True)
        )

    assert none_user.id in ids_for(User.StaffRole.NONE)
    assert viewer.id in ids_for(User.StaffRole.VIEWER)
    assert member.id in ids_for(User.StaffRole.MEMBER)
    assert admin_user.id in ids_for(User.StaffRole.ADMIN)

    # Buckets are mutually exclusive.
    assert none_user.id not in ids_for(User.StaffRole.VIEWER)
    assert viewer.id not in ids_for(User.StaffRole.NONE)
    assert member.id not in ids_for(User.StaffRole.ADMIN)
    assert admin_user.id not in ids_for(User.StaffRole.MEMBER)
