import pytest

from shared.django_apps.codecov_auth.tests.factories import AccountFactory, OwnerFactory
from webhook_handlers.helpers import (
    HANDLER,
    is_installation_event,
    resolve_owner_from_webhook,
    should_process,
)


@pytest.mark.django_db
class TestResolveOwnerFromWebhook:
    def test_resolve_owner_from_repository_owner_id(self):
        owner = OwnerFactory(service="github")
        data = {"repository": {"owner": {"id": owner.service_id}}}
        result = resolve_owner_from_webhook(data, "github")
        assert result is not None
        assert result.ownerid == owner.ownerid

    def test_resolve_owner_from_installation_account_id(self):
        owner = OwnerFactory(service="github")
        data = {"installation": {"account": {"id": owner.service_id}}}
        result = resolve_owner_from_webhook(data, "github")
        assert result is not None
        assert result.ownerid == owner.ownerid

    def test_resolve_owner_from_organization_id(self):
        owner = OwnerFactory(service="github")
        data = {"organization": {"id": owner.service_id}}
        result = resolve_owner_from_webhook(data, "github")
        assert result is not None
        assert result.ownerid == owner.ownerid

    def test_resolve_owner_none_when_no_match(self):
        data = {
            "repository": {"owner": {"id": 1}},
            "installation": {"account": {"id": 2}},
            "organization": {"id": 3},
        }
        result = resolve_owner_from_webhook(data, "github")
        assert result is None


class TestIsInstallationEvent:
    def test_true_for_installation(self):
        assert is_installation_event("installation") is True

    def test_true_for_installation_repositories(self):
        assert is_installation_event("installation_repositories") is True

    def test_false_for_other_events(self):
        assert is_installation_event("repository") is False


@pytest.mark.django_db
class TestShouldProcess:
    def test_installation_events_process_both_handlers(self):
        data = {"installation": {"account": {"id": 123}}}
        result = should_process(data, "installation", "github")
        assert result == {HANDLER.GITHUB, HANDLER.SENTRY}

    def test_non_installation_with_sentry_account_processes_sentry_only(self):
        account = AccountFactory(sentry_org_id=123456789)
        owner = OwnerFactory(service="github", account=account)
        data = {"repository": {"owner": {"id": owner.service_id}}}
        result = should_process(data, "repository", "github")
        assert result == {HANDLER.SENTRY}

    def test_non_installation_without_sentry_account_processes_github_only(self):
        owner = OwnerFactory(service="github")
        data = {"repository": {"owner": {"id": owner.service_id}}}
        result = should_process(data, "repository", "github")
        assert result == {HANDLER.GITHUB}
