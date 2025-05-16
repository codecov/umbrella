import base64
import logging
import zlib
from typing import Any

import orjson
from test_results_parser import parse_raw_upload

from rollouts import ALLOW_VITEST_EVALS
from services.processing.types import UploadArguments
from services.test_analytics.ta_metrics import write_tests_summary
from services.test_analytics.ta_processing import (
    get_ta_processing_info,
    handle_file_not_found,
    handle_parsing_error,
    insert_testruns_timeseries,
    rewrite_or_delete_upload,
)
from shared.api_archive.archive import ArchiveService
from shared.django_apps.reports.models import ReportSession, UploadError
from shared.storage.exceptions import FileNotInStorageError

log = logging.getLogger(__name__)


def ta_processor_impl(
    repoid: int,
    commitid: str,
    commit_yaml: dict[str, Any],
    argument: UploadArguments,
    update_state: bool = False,
) -> bool:
    log.info(
        "Processing single TA argument",
        extra={
            "upload_id": argument.get("upload_id"),
            "repoid": repoid,
            "commitid": commitid,
        },
    )

    upload_id = argument.get("upload_id")
    if upload_id is None:
        return False

    upload = ReportSession.objects.using("default").get(id=upload_id)
    if upload.state == "processed":
        # don't need to process again because the intermediate result should already be in redis
        return False

    if upload.storage_path is None:
        if update_state:
            handle_file_not_found(upload)
        return False

    ta_proc_info = get_ta_processing_info(repoid, commitid, commit_yaml)

    archive_service = ArchiveService(ta_proc_info.repository)

    try:
        payload_bytes = archive_service.read_file(upload.storage_path)
    except FileNotInStorageError:
        if update_state:
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
        if update_state:
            handle_parsing_error(upload, exc)
        return False

    if update_state:
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

    if update_state:
        upload.state = "processed"
        upload.save()

        rewrite_or_delete_upload(
            archive_service, ta_proc_info.user_yaml, upload, readable_file
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
