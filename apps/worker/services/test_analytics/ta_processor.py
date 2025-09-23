from __future__ import annotations

import base64
import logging
import zlib
from dataclasses import dataclass
from typing import Any

import orjson
import sentry_sdk
import test_results_parser
from test_results_parser import parse_raw_upload

from rollouts import ALLOW_VITEST_EVALS
from services.processing.types import UploadArguments
from services.test_analytics.ta_metrics import write_tests_summary
from services.test_analytics.ta_timeseries import get_flaky_tests_set, insert_testrun
from services.yaml import UserYaml, read_yaml_field
from shared.api_archive.archive import ArchiveService
from shared.config import get_config
from shared.django_apps.core.models import Commit, Repository
from shared.django_apps.reports.models import ReportSession, UploadError
from shared.helpers.sentry import owner_uses_sentry
from shared.metrics import Counter, inc_counter
from shared.storage.exceptions import FileNotInStorageError

log = logging.getLogger(__name__)


TA_PROCESSED_COUNTER = Counter(
    "ta_processed_counter",
    "Number of times TA was processed",
    ["product"],
)


TA_FAILED_COUNTER = Counter(
    "ta_failed_counter",
    "Number of times TA failed",
    ["product", "reason"],
)


def ta_processor(
    repoid: int,
    commitid: str,
    commit_yaml: dict[str, Any],
    argument: UploadArguments,
) -> bool:
    log.info("Processing single TA argument")
    commit = Commit.objects.get(repoid=repoid, commitid=commitid)

    product = "prevent" if owner_uses_sentry(commit.repository.author) else "codecov"
    upload_id = argument.get("upload_id")

    if upload_id is None:
        inc_counter(
            TA_FAILED_COUNTER,
            labels={"product": product, "reason": "upload_id_is_none"},
        )
        return False

    upload = ReportSession.objects.using("default").get(id=upload_id)
    if upload.state == "processed":
        inc_counter(
            TA_FAILED_COUNTER,
            labels={"product": product, "reason": "already_processed"},
        )
        # don't need to process again because the intermediate result should already be in redis
        return False

    if upload.storage_path is None:
        inc_counter(
            TA_FAILED_COUNTER,
            labels={"product": product, "reason": "storage_path_is_none"},
        )
        handle_file_not_found(upload)
        return False

    ta_proc_info = get_ta_processing_info(repoid, commitid, commit_yaml)

    archive_service = ArchiveService(ta_proc_info.repository)

    try:
        payload_bytes = archive_service.read_file(upload.storage_path)
    except FileNotInStorageError:
        inc_counter(
            TA_FAILED_COUNTER,
            labels={"product": product, "reason": "file_not_in_storage"},
        )
        handle_file_not_found(upload)
        return False

    try:
        # Consuming `vitest` JSON output is just a temporary solution until we have
        # a properly standardized and implemented junit xml extension for carrying
        # evals metadata (or any kind of metadata/properties really).
        # This code is just for internal testing, it should *not* be stabilized,
        # but rather removed completely once we have implemented this properly.
        # "famous last words" applies here, lol.
        parsing_infos = None
        try_vitest_evals = ALLOW_VITEST_EVALS.check_value(
            ta_proc_info.repository.repoid, default=False
        )
        if try_vitest_evals:
            try:
                parsing_infos, readable_file = try_parsing_vitest_evals(payload_bytes)
            except Exception:
                pass

        if not parsing_infos:
            parsing_infos, readable_file = parse_raw_upload(payload_bytes)
    except RuntimeError as exc:
        inc_counter(
            TA_FAILED_COUNTER,
            labels={"product": product, "reason": "runtime_error"},
        )
        handle_parsing_error(upload, exc)
        return False

    UploadError.objects.bulk_create(
        [
            UploadError(
                report_session=upload,
                error_code="warning",
                error_params={"warning_message": warning},
            )
            for info in parsing_infos
            for warning in info["warnings"]
        ]
    )

    with write_tests_summary.labels("new").time():
        insert_testruns_timeseries(
            repoid, commitid, ta_proc_info.branch, upload, parsing_infos
        )

    upload.state = "processed"
    upload.save()

    rewrite_or_delete_upload(
        archive_service, ta_proc_info.user_yaml, upload, readable_file
    )

    inc_counter(
        TA_PROCESSED_COUNTER,
        labels={"product": product},
    )

    return True


def try_parsing_vitest_evals(raw_upload: bytes):
    json = orjson.loads(raw_upload)

    parsing_infos = []
    readable_file = b""

    for file in json["test_results_files"]:
        data = zlib.decompress(base64.b64decode(file["data"]))
        file_json = orjson.loads(data)

        readable_file += f"# path={file['filename']}\n".encode()
        readable_file += data
        readable_file += b"\n<<<<<< EOF\n"

        testruns = []

        for test_file in file_json["testResults"]:
            testruns = [
                {
                    "filename": test_file["name"],
                    "classname": test_result["ancestorTitles"][-1],
                    "name": test_result["title"],
                    "computed_name": f"{test_result['ancestorTitles'][-1]} > {test_result['title']}",
                    "testsuite": "",
                    "duration": test_result["duration"] / 1000.0,
                    "outcome": "pass"
                    if test_result["status"] == "passed"
                    else "failure",
                    "properties": test_result.get("meta"),
                    "failure_message": "\n".join(test_result["failureMessages"]),
                }
                for test_result in test_file["assertionResults"]
            ]

        parsing_infos.append(
            {"framework": "Vitest", "testruns": testruns, "warnings": []}
        )

    return (parsing_infos, readable_file)


@dataclass
class TAProcInfo:
    repository: Repository
    branch: str | None
    user_yaml: UserYaml


def handle_file_not_found(upload: ReportSession):
    upload.state = "processed"
    upload.save()
    UploadError.objects.create(
        report_session=upload,
        error_code="file_not_in_storage",
        error_params={},
    )


def handle_parsing_error(upload: ReportSession, exc: Exception):
    sentry_sdk.capture_exception(exc, tags={"upload_state": upload.state})
    upload.state = "processed"
    upload.save()
    UploadError.objects.create(
        report_session=upload,
        error_code="unsupported_file_format",
        error_params={"error_message": str(exc)},
    )


def get_ta_processing_info(
    repoid: int,
    commitid: str,
    commit_yaml: dict[str, Any],
) -> TAProcInfo:
    repository = Repository.objects.using("default").get(repoid=repoid)

    commit = Commit.objects.using("default").get(
        repository=repository, commitid=commitid
    )
    branch = commit.branch

    user_yaml: UserYaml = UserYaml(commit_yaml)
    return TAProcInfo(
        repository,
        branch,
        user_yaml,
    )


def should_delete_archive_settings(user_yaml: UserYaml) -> bool:
    if get_config("services", "minio", "expire_raw_after_n_days"):
        return True
    return not read_yaml_field(user_yaml, ("codecov", "archive", "uploads"), _else=True)


def rewrite_or_delete_upload(
    archive_service: ArchiveService,
    user_yaml: UserYaml,
    upload: ReportSession,
    readable_file: bytes,
):
    if should_delete_archive_settings(user_yaml):
        archive_url = upload.storage_path
        if archive_url and not archive_url.startswith("http"):
            archive_service.delete_file(archive_url)
    else:
        archive_service.write_file(upload.storage_path, bytes(readable_file))


def insert_testruns_timeseries(
    repoid: int,
    commitid: str,
    branch: str | None,
    upload: ReportSession,
    parsing_infos: list[test_results_parser.ParsingInfo],
):
    flaky_test_set = get_flaky_tests_set(repoid)

    for parsing_info in parsing_infos:
        insert_testrun(
            timestamp=upload.created_at,
            repo_id=repoid,
            commit_sha=commitid,
            branch=branch,
            upload_id=upload.id,
            flags=upload.flag_names,
            parsing_info=parsing_info,
            flaky_test_ids=flaky_test_set,
        )
