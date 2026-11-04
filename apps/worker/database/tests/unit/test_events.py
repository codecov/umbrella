import logging
import os

import database.events  # noqa: F401
from database.tests.factories import OwnerFactory, RepositoryFactory
from shared.plan.constants import DEFAULT_FREE_PLAN, PlanName
from tasks.github_marketplace import SyncPlansTask


def _plan_change_records(caplog):
    return [r for r in caplog.records if r.message == "Owner plan changed"]


def test_shelter_repo_sync(dbsession, mock_configuration, mocker):
    # this prevents the pubsub SDK from trying to load credentials
    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost"
    publish = mocker.patch("google.cloud.pubsub_v1.PublisherClient.publish")

    mock_configuration.set_params(
        {
            "setup": {
                "shelter": {
                    "pubsub_project_id": "test-project-id",
                    "sync_repo_topic_id": "test-topic-id",
                    "enabled": True,
                }
            }
        }
    )

    # this triggers the publish via SQLAlchemy events (after_insert)
    repo = RepositoryFactory(
        repoid=91728376,
        name="test-123",
        author=OwnerFactory(ownerid=123),
        private=False,
    )
    dbsession.add(repo)
    dbsession.commit()

    publish.assert_called_once_with(
        "projects/test-project-id/topics/test-topic-id",
        b'{"type": "repo", "sync": "one", "id": 91728376}',
    )
    publish_calls = publish.call_args_list

    # Synchronize object flush for history.deleted to be perceived by sqlalchemy
    dbsession.refresh(repo)

    # this triggers the publish via SQLAlchemy events (after_update)
    repo.name = "test-456"
    dbsession.commit()
    dbsession.refresh(repo)
    assert len(publish_calls) == 2

    # Does not trigger another publish with untracked field
    repo.message = "foo"
    dbsession.commit()
    dbsession.refresh(repo)
    assert len(publish_calls) == 2

    # Triggers call when owner is changed
    repo.author = OwnerFactory(ownerid=456)
    dbsession.commit()
    dbsession.refresh(repo)
    assert len(publish_calls) == 3

    # Triggers call when private is changed
    repo.private = True
    dbsession.commit()
    dbsession.refresh(repo)
    assert len(publish_calls) == 4


def test_shelter_owner_username_sync(dbsession, mock_configuration, mocker):
    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost"
    publish = mocker.patch("google.cloud.pubsub_v1.PublisherClient.publish")

    mock_configuration.set_params(
        {
            "setup": {
                "shelter": {
                    "pubsub_project_id": "test-project-id",
                    "sync_repo_topic_id": "test-topic-id",
                    "enabled": True,
                }
            }
        }
    )

    owner = OwnerFactory(ownerid=123, username="old-org", service="github")
    repos = [
        RepositoryFactory(
            repoid=91728378,
            name="example-repo-a",
            author=owner,
            private=False,
        ),
        RepositoryFactory(
            repoid=91728379,
            name="example-repo-b",
            author=owner,
            private=False,
        ),
    ]
    dbsession.add(owner)
    for repo in repos:
        dbsession.add(repo)
    dbsession.commit()
    publish.reset_mock()

    dbsession.refresh(owner)
    owner.username = "new-org"
    dbsession.commit()

    publish.assert_any_call(
        "projects/test-project-id/topics/test-topic-id",
        b'{"type": "owner", "sync": "one", "id": 123}',
    )
    repo_messages = {
        call.args[1]
        for call in publish.call_args_list
        if call.args[1].startswith(b'{"type": "repo"')
    }
    assert repo_messages == {
        b'{"type": "repo", "sync": "one", "id": 91728378}',
        b'{"type": "repo", "sync": "one", "id": 91728379}',
    }


def test_repo_sync_when_shelter_disabled(dbsession, mock_configuration, mocker):
    # this prevents the pubsub SDK from trying to load credentials
    os.environ["PUBSUB_EMULATOR_HOST"] = "localhost"

    mock_configuration.set_params(
        {
            "setup": {
                "shelter": {
                    "pubsub_project_id": "test-project-id",
                    "sync_repo_topic_id": "test-topic-id",
                    "enabled": False,
                }
            }
        }
    )

    publish = mocker.patch("google.cloud.pubsub_v1.PublisherClient.publish")

    # Create new repo with shelter disabled
    repo = RepositoryFactory(repoid=91728377, name="test-789")
    dbsession.add(repo)
    dbsession.commit()

    # Verify no publish was called when shelter is disabled
    publish.assert_not_called()


def test_logs_github_marketplace_purchase(dbsession, mocker, caplog):
    owner = OwnerFactory(
        service="github",
        service_id="98765",
        plan=DEFAULT_FREE_PLAN,
        plan_user_count=1,
        stripe_customer_id="cus_123",
        stripe_subscription_id="sub_123",
    )
    dbsession.add(owner)
    dbsession.commit()
    dbsession.refresh(owner)
    mocker.patch("tasks.github_marketplace.stripe.Subscription.cancel")
    caplog.set_level(logging.INFO, logger="shared.plan.change_log")

    SyncPlansTask().create_or_update_plan(
        dbsession, mocker.MagicMock(), "98765", {"unit_count": 10}
    )
    dbsession.flush()

    (record,) = _plan_change_records(caplog)
    assert record.ownerid == owner.ownerid
    assert record.previous_plan == DEFAULT_FREE_PLAN
    assert record.new_plan == PlanName.GHM_PLAN_NAME.value
    assert record.previous_plan_user_count == 1
    assert record.new_plan_user_count == 10
    assert record.billing_provider == "github_marketplace"
    assert record.github_marketplace_account_id == "98765"
    assert record.stripe_customer_id == "cus_123"
    assert record.previous_stripe_subscription_id == "sub_123"
    assert record.new_stripe_subscription_id is None


def test_logs_github_marketplace_cancellation(dbsession, mocker, caplog):
    owner = OwnerFactory(
        service="github",
        service_id="98766",
        plan=PlanName.GHM_PLAN_NAME.value,
        plan_provider="github",
        plan_user_count=10,
    )
    dbsession.add(owner)
    dbsession.commit()
    dbsession.refresh(owner)
    caplog.set_level(logging.INFO, logger="shared.plan.change_log")

    SyncPlansTask().create_or_update_to_free_plan(
        dbsession, mocker.MagicMock(), "98766"
    )
    dbsession.flush()

    (record,) = _plan_change_records(caplog)
    assert record.previous_plan == PlanName.GHM_PLAN_NAME.value
    assert record.new_plan == DEFAULT_FREE_PLAN
    assert record.new_plan_user_count == 1
    assert record.billing_provider == "github_marketplace"
    assert record.github_marketplace_account_id == "98766"


def test_no_plan_log_for_unrelated_owner_update(dbsession, caplog):
    owner = OwnerFactory(plan=DEFAULT_FREE_PLAN)
    dbsession.add(owner)
    dbsession.commit()
    dbsession.refresh(owner)
    caplog.set_level(logging.INFO, logger="shared.plan.change_log")

    owner.name = "new name"
    owner.plan = DEFAULT_FREE_PLAN
    dbsession.flush()

    assert _plan_change_records(caplog) == []
