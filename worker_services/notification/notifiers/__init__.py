from typing import Dict, List, Type

from worker_services.notification.notifiers.base import AbstractBaseNotifier
from worker_services.notification.notifiers.checks import (
    ChangesChecksNotifier,
    PatchChecksNotifier,
    ProjectChecksNotifier,
)
from worker_services.notification.notifiers.comment import CommentNotifier
from worker_services.notification.notifiers.gitter import GitterNotifier
from worker_services.notification.notifiers.hipchat import HipchatNotifier
from worker_services.notification.notifiers.irc import IRCNotifier
from worker_services.notification.notifiers.slack import SlackNotifier
from worker_services.notification.notifiers.status import (
    ChangesStatusNotifier,
    PatchStatusNotifier,
    ProjectStatusNotifier,
)
from worker_services.notification.notifiers.webhook import WebhookNotifier


def get_all_notifier_classes_mapping() -> Dict[str, Type[AbstractBaseNotifier]]:
    return {
        "gitter": GitterNotifier,
        "hipchat": HipchatNotifier,
        "irc": IRCNotifier,
        "slack": SlackNotifier,
        "webhook": WebhookNotifier,
    }


def get_status_notifier_class(
    status_type: str, class_type: str = "status"
) -> Type[AbstractBaseNotifier]:
    if status_type == "patch" and class_type == "checks":
        return PatchChecksNotifier
    if status_type == "project" and class_type == "checks":
        return ProjectChecksNotifier
    if status_type == "changes" and class_type == "checks":
        return ChangesChecksNotifier
    if status_type == "patch" and class_type == "status":
        return PatchStatusNotifier
    if status_type == "project" and class_type == "status":
        return ProjectStatusNotifier
    if status_type == "changes" and class_type == "status":
        return ChangesStatusNotifier


def get_pull_request_notifiers() -> List[Type[AbstractBaseNotifier]]:
    return [CommentNotifier]
