import hashlib
import os
from typing import Any

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.django import DjangoIntegration
from sentry_sdk.integrations.httpx import HttpxIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from helpers.version import get_current_version
from shared.config import get_config

# Sampling configuration for high-volume expected errors.
# Structure: {repo_name: {error_type: sample_rate}}
# Sample rate of 0.001 = 0.1% of events are kept (99.9% reduction)
SAMPLED_ERROR_PATTERNS: dict[str, dict[str, float]] = {
    "square-web": {
        "LockError": 0.001,
        "LockRetry": 0.001,
        "MaxRetriesExceededError": 0.001,
    },
}


def _should_sample_event(event_id: str, sample_rate: float) -> bool:
    """
    Deterministically decide whether to sample an event based on its ID.

    Uses MD5 hash of the event ID to ensure consistent sampling decisions
    across all processes - the same event_id will always produce the same result.

    Args:
        event_id: The unique Sentry event ID
        sample_rate: The fraction of events to keep (0.0 to 1.0)

    Returns:
        True if the event should be kept, False if it should be dropped
    """
    if sample_rate >= 1.0:
        return True
    if sample_rate <= 0.0:
        return False

    # Use MD5 for deterministic hashing across processes
    # (Python's hash() uses randomization and varies per process)
    hash_bytes = hashlib.md5(event_id.encode(), usedforsecurity=False).digest()
    # Use first 4 bytes as an integer for uniform distribution
    hash_value = int.from_bytes(hash_bytes[:4], byteorder="big") % 10000
    threshold = int(sample_rate * 10000)
    return hash_value < threshold


def before_send(event: dict[str, Any], hint: dict[str, Any]) -> dict[str, Any] | None:
    """
    Filter and sample high-volume expected errors before sending to Sentry.

    This hook samples (not completely suppresses) known high-volume errors from
    specific repositories to reduce noise while maintaining visibility.

    Args:
        event: The Sentry event dictionary
        hint: Additional context about the event

    Returns:
        The event to send, or None to drop it
    """
    # Extract error type from the exception
    error_type: str | None = None
    try:
        error_type = event["exception"]["values"][0]["type"]
    except (KeyError, IndexError, TypeError):
        pass

    # Early return if we couldn't determine the error type (per Tom's feedback)
    if error_type is None:
        return event

    # Extract repo name from tags
    tags = event.get("tags")
    repo_name: str | None = None
    if isinstance(tags, dict):
        repo_name = tags.get("repo_name")
    elif isinstance(tags, list):
        # Tags can be a list of [key, value] pairs
        for tag in tags:
            if (
                isinstance(tag, list | tuple)
                and len(tag) == 2
                and tag[0] == "repo_name"
            ):
                repo_name = tag[1]
                break

    if repo_name is None:
        return event

    # Check if this error pattern should be sampled
    repo_patterns = SAMPLED_ERROR_PATTERNS.get(repo_name)
    if repo_patterns is None:
        return event

    sample_rate = repo_patterns.get(error_type)
    if sample_rate is None:
        return event

    # Deterministically sample based on event ID
    event_id = event.get("event_id", "")
    if not _should_sample_event(event_id, sample_rate):
        return None  # Drop the event

    return event


def is_sentry_enabled() -> bool:
    return bool(get_config("services", "sentry", "server_dsn"))


def before_send_transaction(transaction, hint):
    """
    Filter out noisy transactions from Sentry to reduce ingestion costs.

    Returns None to drop the transaction, or the transaction object to send it.
    """
    # Filter out UploadBreadcrumb tasks (can appear with or without task prefix)
    if transaction.name in ("UploadBreadcrumb", "app.tasks.upload.UploadBreadcrumb"):
        return None  # Drop the transaction

    # Can add more filters here as needed
    # if "health_check" in transaction.name.lower():
    #     return None

    return transaction


def initialize_sentry() -> None:
    version = get_current_version()
    version_str = f"worker-{version}"
    sentry_dsn = get_config("services", "sentry", "server_dsn")
    sentry_sdk.init(
        sentry_dsn,
        sample_rate=float(os.getenv("SENTRY_PERCENTAGE", "1.0")),
        environment=os.getenv("DD_ENV", "production"),
        traces_sample_rate=float(os.environ.get("SERVICES__SENTRY__SAMPLE_RATE", "1")),
        profiles_sample_rate=float(
            os.environ.get("SERVICES__SENTRY__PROFILES_SAMPLE_RATE", "1")
        ),
        _experiments={"enable_logs": True},
        enable_backpressure_handling=False,
        integrations=[
            CeleryIntegration(monitor_beat_tasks=True),
            DjangoIntegration(signals_spans=False),
            SqlalchemyIntegration(),
            RedisIntegration(cache_prefixes=["cache:"]),
            HttpxIntegration(),
        ],
        release=os.getenv("SENTRY_RELEASE", version_str),
        before_send=before_send,
        before_send_transaction=before_send_transaction,
    )
    if os.getenv("CLUSTER_ENV"):
        sentry_sdk.set_tag("cluster", os.getenv("CLUSTER_ENV"))
