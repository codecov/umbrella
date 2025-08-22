from __future__ import annotations

import logging
from datetime import UTC, datetime

import test_results_parser
from sentry_sdk import capture_exception

from app import celery_app
from services.test_analytics.prevent_timeseries import (
    get_flaky_tests_set,
    insert_testrun,
)
from shared.api_archive.archive import ArchiveService
from shared.celery_config import ingest_testruns_task_name
from shared.config import get_config
from shared.django_apps.codecov_auth.models import Plan
from shared.django_apps.core.models import Repository
from shared.django_apps.test_analytics.models import TAUpload
from shared.plan.constants import TierName
from shared.storage.exceptions import FileNotInStorageError
from shared.upload.types import TAUploadContext
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


def _read_payload_from_storage(
    archive_service: ArchiveService,
    storage_path: str,
) -> bytes | None:
    try:
        return archive_service.read_file(storage_path)
    except FileNotInStorageError as exc:
        capture_exception(exc)
        log.warning(
            "Storage read failed: file not in storage",
            extra={"storage_path": storage_path, "err": str(exc)},
        )
        return None
    except Exception as exc:
        capture_exception(exc)
        log.warning(
            "Storage read failed: unexpected error",
            extra={"storage_path": storage_path, "err": str(exc)},
        )
        return None


def _parse_payload_bytes(
    payload_bytes: bytes,
    storage_path: str,
) -> tuple[list[test_results_parser.ParsingInfo], bytes] | None:
    try:
        parsing_infos, readable_file = test_results_parser.parse_raw_upload(
            payload_bytes
        )
        return parsing_infos, readable_file
    except RuntimeError as exc:
        capture_exception(exc)
        log.error(
            "Error parsing test results payload",
            extra={"storage_path": storage_path, "err": str(exc)},
        )
        return None
    except Exception as exc:
        capture_exception(exc)
        log.error(
            "Unexpected error parsing test results payload",
            extra={"storage_path": storage_path, "err": str(exc)},
        )
        return None


def _ingest_parsing_infos(
    repoid: int,
    commit_sha: str,
    branch: str | None,
    parsing_infos: list[test_results_parser.ParsingInfo],
    now_ts: datetime,
    flaky_set: set[bytes],
    upload_id: int | None,
    flags: list[str] | None = None,
) -> int:
    testruns_written = 0
    for parsing_info in parsing_infos:
        testruns = parsing_info.get("testruns", [])
        if not testruns:
            continue
        insert_testrun(
            timestamp=now_ts,
            repo_id=repoid,
            commit_sha=commit_sha,
            branch=branch,
            upload_id=upload_id,
            flags=flags,
            parsing_info=parsing_info,
            flaky_test_ids=flaky_set,
        )
        testruns_written += len(testruns)
    return testruns_written


def _finalize_storage(
    archive_service: ArchiveService,
    storage_path: str,
    readable_file: bytes,
) -> None:
    if get_config("services", "minio", "expire_raw_after_n_days"):
        try:
            archive_service.delete_file(storage_path)
        except Exception as exc:
            capture_exception(exc)
            log.warning(
                "Failed deleting raw test results file",
                extra={"storage_path": storage_path, "err": str(exc)},
            )
    else:
        try:
            archive_service.write_file(storage_path, bytes(readable_file))
        except Exception as exc:
            capture_exception(exc)
            log.warning(
                "Failed writing readable test results file",
                extra={"storage_path": storage_path, "err": str(exc)},
            )


def not_private_and_free_or_team(repo: Repository):
    plan = Plan.objects.select_related("tier").get(name=repo.author.plan)

    return not (
        repo.private
        and plan
        and plan.tier.tier_name in {TierName.BASIC.value, TierName.TEAM.value}
    )


class IngestTestruns(BaseCodecovTask, name=ingest_testruns_task_name):
    def run_impl(
        self,
        _db_session,
        *,
        repoid: int,
        upload_context: TAUploadContext,
        **kwargs,
    ):
        log.info(
            "ingest_testruns invoked",
            extra={
                "repoid": repoid,
                "upload_context": upload_context,
            },
        )

        repo = Repository.objects.get(repoid=repoid)
        storage_path = upload_context.get("storage_path")

        archive_service = ArchiveService(repository=repo)
        payload_bytes = _read_payload_from_storage(
            archive_service=archive_service,
            storage_path=storage_path,
        )
        if payload_bytes is None:
            return {"processed": 0, "reason": "file_not_in_storage"}

        parsed = _parse_payload_bytes(
            payload_bytes=payload_bytes, storage_path=storage_path
        )
        if parsed is None:
            return {"processed": 0, "reason": "unsupported_file_format"}
        parsing_infos, readable_file = parsed

        flaky_set = get_flaky_tests_set(repoid)
        now_ts = datetime.now(UTC)

        upload_id = None
        if not_private_and_free_or_team(repo):
            upload_row = TAUpload.objects.create(repo_id=repoid, state="pending")
            upload_id = upload_row.id

        testruns_written = _ingest_parsing_infos(
            repoid,
            upload_context["commit_sha"],
            upload_context["branch"],
            parsing_infos,
            now_ts,
            flaky_set,
            upload_id,
            upload_context.get("flags"),
        )

        _finalize_storage(
            archive_service,
            storage_path,
            readable_file,
        )

        return {"processed": testruns_written, "repoid": repoid}


RegisteredIngestTestruns = celery_app.register_task(IngestTestruns())
ingest_testruns_task = celery_app.tasks[RegisteredIngestTestruns.name]
