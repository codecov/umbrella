from unittest.mock import patch

import pytest

from shared.django_apps.codecov_auth.models import Owner, OwnerToBeDeleted, Service
from tasks.mark_owner_for_deletion import MarkOwnerForDeletionTask, obfuscate_owner_data

pytestmark = pytest.mark.django_db


@pytest.fixture
def owner():
    """Create a test owner"""
    return Owner.objects.create(
        ownerid=12345,
        service=Service.TO_BE_DELETED.value,
        username="testuser",
        email="test@example.com",
        business_email="business@example.com",
        name="Test User",
        oauth_token="secret_token",
    )


@pytest.fixture
def task():
    """Create a test task instance"""
    return MarkOwnerForDeletionTask()


def test_obfuscate_owner_data(owner):
    """Test that owner data is properly obfuscated"""
    obfuscate_owner_data(owner)

    # Refresh from database
    owner.refresh_from_db()

    # Check that data is obfuscated
    assert owner.name == "[DELETED_USER_12345]"
    assert owner.email == "deleted_12345@deleted.codecov.io"
    assert owner.business_email == "deleted_12345@deleted.codecov.io"
    assert owner.username == "deleted_user_12345"
    assert owner.service == Service.TO_BE_DELETED.value
    assert owner.oauth_token is None


def test_mark_owner_for_deletion_success(owner, task):
    """Test successful marking of owner for deletion"""
    with patch("tasks.mark_owner_for_deletion.log") as mock_log:
        result = task.run_impl(None, 12345)

    # Check that owner was added to OwnerToBeDeleted table
    assert OwnerToBeDeleted.objects.filter(owner_id=12345).exists()

    # Check that owner data was obfuscated
    owner.refresh_from_db()
    assert owner.name == "[DELETED_USER_12345]"
    assert owner.service == Service.TO_BE_DELETED.value

    # Check return value
    assert result["status"] == "success"
    assert result["ownerid"] == 12345


def test_mark_owner_for_deletion_already_marked(owner, task):
    """Test that already marked owners are handled correctly"""
    # Mark owner for deletion first
    OwnerToBeDeleted.objects.create(owner_id=12345)

    with patch("tasks.mark_owner_for_deletion.log") as mock_log:
        result = task.run_impl(None, 12345)

    # Should return already_marked status
    assert result["status"] == "already_marked"
    assert result["ownerid"] == 12345


def test_mark_owner_for_deletion_owner_not_found(task):
    """Test handling of non-existent owner"""
    with patch("tasks.mark_owner_for_deletion.log") as mock_log:
        with pytest.raises(ValueError, match="Owner 99999 not found"):
            task.run_impl(None, 99999)


def test_mark_owner_for_deletion_with_none_values(task):
    """Test obfuscation when owner has None values"""
    # Create owner with None values
    owner_with_none = Owner.objects.create(
        ownerid=54321,
        service=Service.GITLAB.value,
        username=None,
        email=None,
        business_email=None,
        name=None,
        oauth_token=None,
    )

    obfuscate_owner_data(owner_with_none)
    owner_with_none.refresh_from_db()

    # Should not crash and should set service to deleted
    assert owner_with_none.service == Service.TO_BE_DELETED.value
    assert owner_with_none.name is None
    assert owner_with_none.email is None
    assert owner_with_none.business_email is None
    assert owner_with_none.username is None
    assert owner_with_none.oauth_token is None
