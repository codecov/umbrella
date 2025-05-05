import dataclasses
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager

from django.db.models import Model

import shared.storage
from shared.config import get_config
from shared.storage.base import BaseStorageService

log = logging.getLogger(__name__)


@dataclasses.dataclass
class CleanupResult:
    cleaned_models: int
    cleaned_files: int = 0


@dataclasses.dataclass
class CleanupSummary:
    totals: CleanupResult
    summary: dict[str, CleanupResult]


class CleanupContext:
    threadpool: ThreadPoolExecutor
    storage: BaseStorageService
    default_bucket: str
    bundleanalysis_bucket: str
    summary: CleanupSummary

    _current_model: str | None = None
    _last_progress_report: float

    def __init__(self):
        self.threadpool = ThreadPoolExecutor()
        self.storage = shared.storage.get_appropriate_storage_service()
        self.default_bucket = get_config(
            "services", "minio", "bucket", default="archive"
        )
        self.bundleanalysis_bucket = get_config(
            "bundle_analysis", "bucket_name", default="bundle-analysis"
        )
        self.summary = CleanupSummary(CleanupResult(0), {})
        self._last_progress_report = time.monotonic()

    def set_current_model(self, model: type[Model] | None):
        self._current_model = model.__name__ if model else None

    def add_progress(
        self,
        cleaned_models: int = 0,
        cleaned_files: int = 0,
        model: type[Model] | None = None,
    ):
        self.summary.totals.cleaned_models += cleaned_models
        self.summary.totals.cleaned_files += cleaned_files

        model_name = model.__name__ if model else self._current_model
        if model_name and (cleaned_models or cleaned_files):
            result = self.summary.summary.setdefault(model_name, CleanupResult(0))
            result.cleaned_models += cleaned_models
            result.cleaned_files += cleaned_files

        now = time.monotonic()
        if (now - self._last_progress_report) > 120.0:
            self._last_progress_report = now
            log.info("Cleanup is making progress…", extra={"summary": self.summary})


@contextmanager
def cleanup_context():
    context = CleanupContext()
    try:
        yield context
        context.set_current_model(None)
    finally:
        msg = (
            "Cleanup was interrupted" if context._current_model else "Cleanup completed"
        )
        log.info(msg, extra={"summary": context.summary})
        context.threadpool.shutdown()
