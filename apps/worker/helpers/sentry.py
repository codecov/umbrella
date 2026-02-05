import os

import sentry_sdk
from sentry_sdk.integrations.celery import CeleryIntegration
from sentry_sdk.integrations.django import DjangoIntegration
from sentry_sdk.integrations.httpx import HttpxIntegration
from sentry_sdk.integrations.redis import RedisIntegration
from sentry_sdk.integrations.sqlalchemy import SqlalchemyIntegration

from helpers.version import get_current_version
from shared.config import get_config


def is_sentry_enabled() -> bool:
    return bool(get_config("services", "sentry", "server_dsn"))


def before_send_transaction(transaction, hint):
    """
    Filter out noisy transactions from Sentry to reduce ingestion costs.

    Returns None to drop the transaction, or the transaction object to send it.
    """
    transaction_name = transaction.get("transaction", "")
    if transaction_name in ("UploadBreadcrumb", "app.tasks.upload.UploadBreadcrumb"):
        return None

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
        before_send_transaction=before_send_transaction,
    )
    if os.getenv("CLUSTER_ENV"):
        sentry_sdk.set_tag("cluster", os.getenv("CLUSTER_ENV"))
