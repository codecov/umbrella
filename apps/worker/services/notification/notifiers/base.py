import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from database.models import Repository
from services.comparison import ComparisonProxy
from services.decoration import Decoration
from shared.torngit.base import TorngitBaseAdapter
from shared.yaml import UserYaml

log = logging.getLogger(__name__)


@dataclass
class NotificationResult:
    notification_attempted: bool = False
    notification_successful: bool = False
    explanation: str | None = None
    data_sent: Mapping[str, Any] | None = None
    data_received: Mapping[str, Any] | None = None
    github_app_used: int | None = None

    def merge(self, other: "NotificationResult") -> "NotificationResult":
        ans = NotificationResult()
        ans.notification_attempted = bool(
            self.notification_attempted or other.notification_attempted
        )
        ans.notification_successful = bool(
            self.notification_successful or other.notification_successful
        )
        ans.explanation = self.explanation or other.explanation
        ans.data_sent = self.data_sent or other.data_sent
        ans.data_received = self.data_received or other.data_received
        return ans

    def to_dict(self) -> dict[str, Any]:
        return {
            "notification_attempted": self.notification_attempted,
            "notification_successful": self.notification_successful,
            "explanation": self.explanation,
            "data_sent": self.data_sent,
            "data_received": self.data_received,
            "github_app_used": self.github_app_used,
        }


class AbstractBaseNotifier:
    """
    Base Notifier, abstract class that should not be used

    This class has the core ideas of a notifier that has the structure:

    notifications:
        <notifier_name:
            <notifier_title>:
                ... <notifier_fields>

    """

    def __init__(
        self,
        repository: Repository,
        title: str,
        notifier_yaml_settings: Mapping[str, Any],
        notifier_site_settings: Mapping[str, Any],
        current_yaml: UserYaml,
        repository_service: TorngitBaseAdapter,
        decoration_type: Decoration | None = None,
    ):
        """
        :param repository: The repository notifications are being sent to.
        :param title: The project name for this notification, if applicable. For more info see https://docs.codecov.io/docs/commit-status#splitting-up-projects-example
        :param notifier_yaml_settings: Contains the codecov yaml fields, if any, for this particular notification.
            example: status -> patch -> custom_project_name -> <whatever is here is in notifier_yaml_settings for custom_project_name's status patch notifier>
        :param notifier_site_settings: Contains the codecov yaml fields under the "notify" header
        :param current_yaml: The complete codecov yaml used for this notification.
        :param decoration_type: Indicates whether the user needs to upgrade their account before they can see the notification
        """
        self.repository = repository
        self.title = title
        self.notifier_yaml_settings = notifier_yaml_settings
        self.site_settings = notifier_site_settings
        self.current_yaml = current_yaml
        self.decoration_type = decoration_type
        self.repository_service = repository_service

    @property
    def name(self) -> str:
        raise NotImplementedError()

    def notify(
        self,
        comparison: ComparisonProxy,
        status_or_checks_helper_text: dict[str, str] | None = None,
    ) -> NotificationResult:
        raise NotImplementedError()

    def is_enabled(self) -> bool:
        raise NotImplementedError()

    def store_results(self, comparison: ComparisonProxy, result: NotificationResult):
        """
            This function stores the result in the notification wherever it needs to be saved
            This is the only function in this class allowed to have side-effects in the database

        Args:
            comparison (Comparison): The comparison with which this notify ran
            result (NotificationResult): The results of the notification
        """
        raise NotImplementedError()

    def should_use_upgrade_decoration(self) -> bool:
        return self.decoration_type == Decoration.upgrade

    def should_use_upload_limit_decoration(self) -> bool:
        return self.decoration_type == Decoration.upload_limit

    def is_passing_empty_upload(self) -> bool:
        return self.decoration_type == Decoration.passing_empty_upload

    def is_failing_empty_upload(self) -> bool:
        return self.decoration_type == Decoration.failing_empty_upload

    def is_empty_upload(self) -> bool:
        return self.is_passing_empty_upload() or self.is_failing_empty_upload()

    def is_processing_upload(self) -> bool:
        return self.decoration_type == Decoration.processing_upload
