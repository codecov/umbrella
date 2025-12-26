import datetime
from unittest.mock import MagicMock

import pytest
from django.test import override_settings
from redis import RedisError

from shared.bots.exceptions import NoConfiguredAppsAvailable, RequestedGithubAppNotFound
from shared.bots.github_apps import (
    _get_earliest_rate_limit_ttl,
    _get_rate_limited_apps,
    _partition_apps_by_rate_limit_status,
    get_github_app_info_for_owner,
    get_github_app_token,
    get_specific_github_app_details,
)
from shared.django_apps.codecov_auth.models import (
    GITHUB_APP_INSTALLATION_DEFAULT_NAME,
    Account,
    GithubAppInstallation,
    Owner,
    Service,
)
from shared.django_apps.codecov_auth.tests.factories import OwnerFactory
from shared.github import InvalidInstallationError
from shared.rate_limits import RATE_LIMIT_REDIS_KEY_PREFIX, gh_app_key_name
from shared.typings.torngit import GithubInstallationInfo


def _get_owner_with_apps() -> Owner:
    owner = OwnerFactory(service="github", integration_id=1111)
    # Just so there are other owners in the database
    _ = OwnerFactory(service="github", integration_id=1234)
    app_1 = GithubAppInstallation(
        owner=owner,
        installation_id=1200,
        app_id=12,
    )
    app_2 = GithubAppInstallation(
        owner=owner,
        installation_id=1500,
        app_id=15,
        pem_path="some_path",
    )
    GithubAppInstallation.objects.bulk_create([app_1, app_2])
    assert list(owner.github_app_installations.all()) == [app_1, app_2]
    return owner


def _to_installation_info(
    installation: GithubAppInstallation,
) -> GithubInstallationInfo:
    return GithubInstallationInfo(
        id=installation.id,
        installation_id=installation.installation_id,
        pem_path=installation.pem_path,
        app_id=installation.app_id,
    )


class TestGetSpecificGithubAppDetails:
    @pytest.mark.django_db
    def test_get_specific_github_app_details(self):
        owner = _get_owner_with_apps()
        assert get_specific_github_app_details(
            owner, owner.github_app_installations.all()[0].id, "commit_id_for_logs"
        ) == GithubInstallationInfo(
            id=owner.github_app_installations.all()[0].id,
            installation_id=1200,
            app_id=12,
            pem_path=None,
        )
        assert get_specific_github_app_details(
            owner, owner.github_app_installations.all()[1].id, "commit_id_for_logs"
        ) == GithubInstallationInfo(
            id=owner.github_app_installations.all()[1].id,
            installation_id=1500,
            app_id=15,
            pem_path="some_path",
        )

    @pytest.mark.django_db
    def test_get_specific_github_app_not_found(self):
        owner = _get_owner_with_apps()
        with pytest.raises(RequestedGithubAppNotFound):
            get_specific_github_app_details(owner, 123456, "commit_id_for_logs")

    @pytest.mark.parametrize(
        "app, is_rate_limited",
        [
            pytest.param(
                GithubAppInstallation(
                    repository_service_ids=None,
                    installation_id=1400,
                    name=GITHUB_APP_INSTALLATION_DEFAULT_NAME,
                    app_id=400,
                    pem_path="pem_path",
                    created_at=datetime.datetime.now(datetime.UTC),
                    is_suspended=True,
                ),
                False,
                id="suspended_app",
            ),
            pytest.param(
                GithubAppInstallation(
                    repository_service_ids=None,
                    installation_id=1400,
                    name=GITHUB_APP_INSTALLATION_DEFAULT_NAME,
                    app_id=400,
                    pem_path="pem_path",
                    created_at=datetime.datetime.now(datetime.UTC),
                    is_suspended=False,
                ),
                True,
                id="rate_limited_app",
            ),
        ],
    )
    @pytest.mark.django_db
    def test_raise_NoAppsConfiguredAvailable_if_suspended_or_rate_limited(
        self, app, is_rate_limited, mocker
    ):
        owner = OwnerFactory(
            service="github",
            bot=None,
            unencrypted_oauth_token="owner_token: :refresh_token",
        )
        owner.save()

        app.owner = owner
        app.save()

        mock_is_rate_limited = mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=is_rate_limited,
        )
        with pytest.raises(NoConfiguredAppsAvailable) as exp:
            get_github_app_info_for_owner(owner)
        mock_is_rate_limited.assert_called()
        assert exp.value.apps_count == 1
        assert exp.value.suspended_count == int(app.is_suspended)
        assert exp.value.rate_limited_count == int(is_rate_limited)

    @pytest.mark.django_db
    @override_settings(GITHUB_SENTRY_APP_NAME="test_sentry_app")
    def test_sentry_org_id_overrides_installation_name(self, mocker):
        owner = OwnerFactory(service="github")
        account = Account.objects.create(name="test_account", sentry_org_id=12345)
        owner.account = account
        owner.save()

        sentry_app = GithubAppInstallation(
            owner=owner,
            installation_id=2000,
            app_id=20,
            name="test_sentry_app",
            pem_path="sentry_pem",
        )
        sentry_app.save()

        result = get_github_app_info_for_owner(
            owner, installation_name=GITHUB_APP_INSTALLATION_DEFAULT_NAME
        )

        assert len(result) == 1
        assert result[0]["installation_id"] == 2000
        assert result[0]["app_id"] == 20


class TestGettingGitHubAppTokenSideEffect:
    @pytest.mark.django_db
    def test_mark_installation_suspended_side_effect(self, mocker):
        owner = _get_owner_with_apps()
        installations: list[GithubAppInstallation] = (
            owner.github_app_installations.all()
        )
        installation_info = _to_installation_info(installations[0])
        mocker.patch(
            "shared.bots.github_apps.get_github_integration_token",
            side_effect=InvalidInstallationError("installation_suspended"),
        )

        assert all(installation.is_suspended == False for installation in installations)

        with pytest.raises(InvalidInstallationError):
            get_github_app_token(Service(owner.service), installation_info)

        installations[0].refresh_from_db()
        assert installations[0].is_suspended is True
        installations[1].refresh_from_db()
        assert installations[1].is_suspended is False
        assert not Owner.objects.filter(integration_id=None).exists()

    @pytest.mark.django_db
    def test_mark_installation_not_found_side_effect(self, mocker):
        owner = _get_owner_with_apps()
        installations: list[GithubAppInstallation] = (
            owner.github_app_installations.all()
        )
        installation_info = _to_installation_info(installations[0])
        mocker.patch(
            "shared.bots.github_apps.get_github_integration_token",
            side_effect=InvalidInstallationError("installation_not_found"),
        )

        assert all(installation.is_suspended == False for installation in installations)

        with pytest.raises(InvalidInstallationError):
            get_github_app_token(Service(owner.service), installation_info)

        installations[1].refresh_from_db()
        assert installations[1].is_suspended is False
        assert not Owner.objects.filter(integration_id=None).exists()
        owner.refresh_from_db()
        assert list(owner.github_app_installations.all()) == [installations[1]]

    @pytest.mark.django_db
    def test_mark_installation_suspended_legacy_path_side_effect(self, mocker):
        owner = _get_owner_with_apps()
        installations: list[GithubAppInstallation] = (
            owner.github_app_installations.all()
        )
        installation_info = {"installation_id": 1111}
        mocker.patch(
            "shared.bots.github_apps.get_github_integration_token",
            side_effect=InvalidInstallationError("installation_not_found"),
        )

        assert all(installation.is_suspended == False for installation in installations)

        with pytest.raises(InvalidInstallationError):
            get_github_app_token(Service(owner.service), installation_info)

        for installation in installations:
            installation.refresh_from_db()
            assert installation.is_suspended == False
        owner.refresh_from_db()
        assert list(Owner.objects.filter(integration_id=None).all()) == [owner]

    @pytest.mark.django_db
    def test_mark_installation_suspended_side_effect_not_called(self, mocker):
        owner = _get_owner_with_apps()
        installations: list[GithubAppInstallation] = (
            owner.github_app_installations.all()
        )
        installation_info = _to_installation_info(installations[0])
        mocker.patch(
            "shared.bots.github_apps.get_github_integration_token",
            side_effect=InvalidInstallationError("permission_error"),
        )

        assert all(installation.is_suspended == False for installation in installations)

        with pytest.raises(InvalidInstallationError):
            get_github_app_token(Service(owner.service), installation_info)

        installations[0].refresh_from_db()
        installations[1].refresh_from_db()
        assert all(installation.is_suspended == False for installation in installations)


class TestGetRateLimitedApps:
    @pytest.mark.django_db
    def test_get_rate_limited_apps_empty_list(self):
        """Test that empty list returns empty list."""
        assert _get_rate_limited_apps([]) == []

    @pytest.mark.django_db
    def test_get_rate_limited_apps_all_rate_limited(self, mocker):
        """Test that all rate-limited apps are returned."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=True,
        )

        result = _get_rate_limited_apps([app1, app2])
        assert len(result) == 2
        assert app1 in result
        assert app2 in result

    @pytest.mark.django_db
    def test_get_rate_limited_apps_none_rate_limited(self, mocker):
        """Test that no rate-limited apps returns empty list."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=False,
        )

        result = _get_rate_limited_apps([app1, app2])
        assert result == []

    @pytest.mark.django_db
    def test_get_rate_limited_apps_mixed(self, mocker):
        """Test that only rate-limited apps are returned when mixed."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        def is_rate_limited_side_effect(redis_connection, key_name):
            # app1 is rate-limited, app2 is not
            if "10_1001" in key_name:
                return True
            return False

        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            side_effect=is_rate_limited_side_effect,
        )

        result = _get_rate_limited_apps([app1, app2])
        assert len(result) == 1
        assert app1 in result
        assert app2 not in result


class TestPartitionAppsByRateLimitStatus:
    @pytest.mark.django_db
    def test_partition_apps_by_rate_limit_status_empty_list(self):
        """Test that empty list returns two empty lists."""
        rate_limited, non_rate_limited = _partition_apps_by_rate_limit_status([])
        assert rate_limited == []
        assert non_rate_limited == []

    @pytest.mark.django_db
    def test_partition_apps_by_rate_limit_status_all_rate_limited(self, mocker):
        """Test that all rate-limited apps are partitioned correctly."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=True,
        )

        rate_limited, non_rate_limited = _partition_apps_by_rate_limit_status(
            [app1, app2]
        )
        assert len(rate_limited) == 2
        assert len(non_rate_limited) == 0
        assert app1 in rate_limited
        assert app2 in rate_limited

    @pytest.mark.django_db
    def test_partition_apps_by_rate_limit_status_none_rate_limited(self, mocker):
        """Test that no rate-limited apps returns empty rate-limited list."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=False,
        )

        rate_limited, non_rate_limited = _partition_apps_by_rate_limit_status(
            [app1, app2]
        )
        assert len(rate_limited) == 0
        assert len(non_rate_limited) == 2
        assert app1 in non_rate_limited
        assert app2 in non_rate_limited

    @pytest.mark.django_db
    def test_partition_apps_by_rate_limit_status_mixed(self, mocker):
        """Test that apps are correctly partitioned when mixed."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        def is_rate_limited_side_effect(redis_connection, key_name):
            # app1 is rate-limited, app2 is not
            if "10_1001" in key_name:
                return True
            return False

        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            side_effect=is_rate_limited_side_effect,
        )

        rate_limited, non_rate_limited = _partition_apps_by_rate_limit_status(
            [app1, app2]
        )
        assert len(rate_limited) == 1
        assert len(non_rate_limited) == 1
        assert app1 in rate_limited
        assert app2 in non_rate_limited


class TestGetEarliestRateLimitTtl:
    @pytest.mark.django_db
    def test_get_earliest_rate_limit_ttl_empty_list(self):
        """Test that empty list returns None."""
        assert _get_earliest_rate_limit_ttl([]) is None

    @pytest.mark.django_db
    def test_get_earliest_rate_limit_ttl_single_app(self, mocker):
        """Test that single app TTL is returned."""
        owner = OwnerFactory(service="github")
        app = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app.save()

        mock_redis = MagicMock()
        mock_redis.ttl.return_value = 300  # 5 minutes
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        result = _get_earliest_rate_limit_ttl([app])
        assert result == 300
        key_name = gh_app_key_name(
            app_id=app.app_id, installation_id=app.installation_id
        )
        expected_key = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name}"
        mock_redis.ttl.assert_called_once_with(expected_key)

    @pytest.mark.django_db
    def test_get_earliest_rate_limit_ttl_multiple_apps_returns_minimum(self, mocker):
        """Test that minimum TTL is returned when multiple apps have different TTLs."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mock_redis = MagicMock()
        key_name1 = gh_app_key_name(
            app_id=app1.app_id, installation_id=app1.installation_id
        )
        key_name2 = gh_app_key_name(
            app_id=app2.app_id, installation_id=app2.installation_id
        )
        expected_key1 = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name1}"
        expected_key2 = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name2}"

        def ttl_side_effect(key):
            if key == expected_key1:
                return 600  # 10 minutes
            elif key == expected_key2:
                return 180  # 3 minutes (minimum)
            return -1

        mock_redis.ttl.side_effect = ttl_side_effect
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        result = _get_earliest_rate_limit_ttl([app1, app2])
        assert result == 180  # Should return minimum

    @pytest.mark.django_db
    def test_get_earliest_rate_limit_ttl_ttl_zero_or_negative_ignored(self, mocker):
        """Test that TTL <= 0 is ignored."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mock_redis = MagicMock()
        key_name1 = gh_app_key_name(
            app_id=app1.app_id, installation_id=app1.installation_id
        )
        key_name2 = gh_app_key_name(
            app_id=app2.app_id, installation_id=app2.installation_id
        )
        expected_key1 = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name1}"
        expected_key2 = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name2}"

        def ttl_side_effect(key):
            if key == expected_key1:
                return 0  # Key doesn't exist or expired
            elif key == expected_key2:
                return -1  # Key doesn't exist
            return -1

        mock_redis.ttl.side_effect = ttl_side_effect
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        result = _get_earliest_rate_limit_ttl([app1, app2])
        assert result is None  # No valid TTLs

    @pytest.mark.django_db
    def test_get_earliest_rate_limit_ttl_redis_exception_continues(self, mocker):
        """Test that Redis exceptions are caught and processing continues."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app2 = GithubAppInstallation(
            owner=owner,
            installation_id=1002,
            app_id=11,
            pem_path="pem2",
        )
        GithubAppInstallation.objects.bulk_create([app1, app2])

        mock_redis = MagicMock()
        key_name1 = gh_app_key_name(
            app_id=app1.app_id, installation_id=app1.installation_id
        )
        key_name2 = gh_app_key_name(
            app_id=app2.app_id, installation_id=app2.installation_id
        )
        expected_key1 = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name1}"
        expected_key2 = f"{RATE_LIMIT_REDIS_KEY_PREFIX}{key_name2}"

        def ttl_side_effect(key):
            if key == expected_key1:
                raise RedisError("Redis connection failed")
            elif key == expected_key2:
                return 300  # Valid TTL
            return -1

        mock_redis.ttl.side_effect = ttl_side_effect
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        result = _get_earliest_rate_limit_ttl([app1, app2])
        assert result == 300  # Should return valid TTL from app2 despite app1 failing

    @pytest.mark.django_db
    def test_get_earliest_rate_limit_ttl_all_redis_exceptions_returns_none(
        self, mocker
    ):
        """Test that if all Redis operations fail, None is returned."""
        owner = OwnerFactory(service="github")
        app1 = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
        )
        app1.save()

        mock_redis = MagicMock()
        mock_redis.ttl.side_effect = RedisError("Redis connection failed")
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        result = _get_earliest_rate_limit_ttl([app1])
        assert result is None  # All operations failed


class TestGetGithubAppInfoForOwnerWithEarliestRetryTime:
    @pytest.mark.django_db
    def test_get_github_app_info_for_owner_sets_earliest_retry_when_rate_limited(
        self, mocker
    ):
        """Test that earliest_retry_after_seconds is set when rate-limited apps exist."""
        owner = OwnerFactory(service="github")
        app = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
            is_suspended=False,
        )
        app.save()

        # Mock rate-limited check to return True
        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=True,
        )

        # Mock Redis TTL to return a value
        mock_redis = MagicMock()
        mock_redis.ttl.return_value = 240  # 4 minutes
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        with pytest.raises(NoConfiguredAppsAvailable) as exp:
            get_github_app_info_for_owner(owner)

        assert exp.value.apps_count == 1
        assert exp.value.rate_limited_count == 1
        assert exp.value.suspended_count == 0
        assert exp.value.earliest_retry_after_seconds == 240

    @pytest.mark.django_db
    def test_get_github_app_info_for_owner_no_earliest_retry_when_not_rate_limited(
        self, mocker
    ):
        """Test that earliest_retry_after_seconds is None when no rate-limited apps."""
        owner = OwnerFactory(service="github")
        app = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
            is_suspended=True,  # Suspended but not rate-limited
        )
        app.save()

        # Mock rate-limited check to return False
        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=False,
        )

        with pytest.raises(NoConfiguredAppsAvailable) as exp:
            get_github_app_info_for_owner(owner)

        assert exp.value.apps_count == 1
        assert exp.value.rate_limited_count == 0
        assert exp.value.suspended_count == 1
        assert exp.value.earliest_retry_after_seconds is None

    @pytest.mark.django_db
    def test_get_github_app_info_for_owner_earliest_retry_none_when_ttl_fails(
        self, mocker
    ):
        """Test that earliest_retry_after_seconds is None when TTL lookup fails."""
        owner = OwnerFactory(service="github")
        app = GithubAppInstallation(
            owner=owner,
            installation_id=1001,
            app_id=10,
            pem_path="pem1",
            is_suspended=False,
        )
        app.save()

        # Mock rate-limited check to return True
        mocker.patch(
            "shared.bots.github_apps.determine_if_entity_is_rate_limited",
            return_value=True,
        )

        # Mock Redis TTL to raise exception
        mock_redis = MagicMock()
        mock_redis.ttl.side_effect = RedisError("Redis connection failed")
        mocker.patch(
            "shared.bots.github_apps.get_redis_connection", return_value=mock_redis
        )

        with pytest.raises(NoConfiguredAppsAvailable) as exp:
            get_github_app_info_for_owner(owner)

        assert exp.value.apps_count == 1
        assert exp.value.rate_limited_count == 1
        assert exp.value.suspended_count == 0
        assert exp.value.earliest_retry_after_seconds is None  # TTL lookup failed
