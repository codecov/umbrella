"""
Export Owner Data Tasks

Orchestrates the export of an owner's data to a downloadable archive containing
PostgreSQL/TimescaleDB SQL exports, archive files, and a manifest.
"""

import json
import logging
import os
import tarfile
import tempfile
from datetime import datetime, timedelta
from io import BytesIO

from celery import chord
from celery.exceptions import SoftTimeLimitExceeded
from celery.result import AsyncResult
from django.utils import timezone

from app import celery_app
from shared.celery_config import (
    export_owner_archives_task_name,
    export_owner_cleanup_task_name,
    export_owner_finalize_task_name,
    export_owner_sql_task_name,
    export_owner_task_name,
)
from shared.django_apps.codecov_auth.models import Owner, OwnerExport
from shared.owner_data_export.archive_collector import collect_archives_for_export
from shared.owner_data_export.config import (
    ARCHIVE_TASK_SOFT_TIME_LIMIT,
    ARCHIVE_TASK_TIME_LIMIT,
    DOWNLOAD_URL_EXPIRY_SECONDS,
    EXPORT_DAYS_DEFAULT,
    FINALIZE_TASK_SOFT_TIME_LIMIT,
    FINALIZE_TASK_TIME_LIMIT,
    SQL_TASK_SOFT_TIME_LIMIT,
    SQL_TASK_TIME_LIMIT,
    get_archive_bucket,
    get_archives_path,
    get_export_path,
    get_manifest_path,
    get_postgres_sql_path,
    get_tarball_path,
    get_timescale_sql_path,
)
from shared.owner_data_export.sql_generator import (
    ExportContext,
    generate_full_export,
    generate_timescale_export,
)
from shared.storage import get_appropriate_storage_service
from tasks.base import BaseCodecovTask

log = logging.getLogger(__name__)


def _mark_export_failed_by_id(export_id: int, error_message: str) -> bool:
    """Mark export as failed by ID. Returns True if updated, False otherwise."""
    try:
        export = OwnerExport.objects.get(id=export_id)
        if export.status in (OwnerExport.Status.COMPLETED, OwnerExport.Status.FAILED):
            return False
        export.status = OwnerExport.Status.FAILED
        export.error_message = error_message[:500]
        export.save(update_fields=["status", "error_message", "updated_at"])
        return True
    except Exception:
        return False


class ExportOwnerTask(BaseCodecovTask, name=export_owner_task_name):
    """Orchestrator task that coordinates the export pipeline."""

    acks_late = True
    max_retries = 3

    def run_impl(
        self,
        _db_session,
        *,
        owner_id: int,
        export_id: int,
        user_id: int | None = None,
    ) -> dict:
        log.info(
            "Export orchestrator started",
            extra={"export_id": export_id},
        )

        try:
            export = OwnerExport.objects.get(id=export_id)
        except OwnerExport.DoesNotExist:
            log.error(
                "OwnerExport record not found",
                extra={"export_id": export_id},
            )
            return {"error": "Export not found"}

        if export.owner_id != owner_id:
            log.error(
                "Owner ID mismatch in export record",
                extra={
                    "export_id": export_id,
                    "expected": owner_id,
                    "actual": export.owner_id,
                },
            )
            export.status = OwnerExport.Status.FAILED
            export.error_message = (
                f"Owner ID mismatch: export belongs to {export.owner_id}"
            )
            export.save(update_fields=["status", "error_message", "updated_at"])
            return {"error": "Owner ID mismatch"}

        export.status = OwnerExport.Status.IN_PROGRESS
        export.save(update_fields=["status", "updated_at"])

        since_date_iso = export.since_date.isoformat() if export.since_date else None

        workflow = chord(
            [
                export_owner_sql_task.s(
                    owner_id=owner_id,
                    export_id=export_id,
                    since_date_iso=since_date_iso,
                ),
                export_owner_archives_task.s(
                    owner_id=owner_id,
                    export_id=export_id,
                    since_date_iso=since_date_iso,
                ),
            ],
            export_owner_finalize_task.s(owner_id=owner_id, export_id=export_id),
        )

        result = workflow.apply_async(
            link_error=export_owner_cleanup_task.s(
                owner_id=owner_id, export_id=export_id
            ),
            allow_error_cb_on_chord_header=True,
        )

        export.task_ids = {"chord_id": result.id}
        export.save(update_fields=["task_ids", "updated_at"])

        log.info(
            "Export workflow dispatched",
            extra={"export_id": export_id, "chord_id": result.id},
        )

        return {
            "export_id": export_id,
            "owner_id": owner_id,
            "chord_id": result.id,
        }


class ExportOwnerSQLTask(BaseCodecovTask, name=export_owner_sql_task_name):
    """Generate PostgreSQL and TimescaleDB SQL export files."""

    acks_late = True
    max_retries = 3
    soft_time_limit = SQL_TASK_SOFT_TIME_LIMIT
    time_limit = SQL_TASK_TIME_LIMIT

    def run_impl(
        self,
        _db_session,
        *,
        owner_id: int,
        export_id: int,
        since_date_iso: str | None = None,
    ) -> dict:
        log.info(
            "SQL export task started",
            extra={"export_id": export_id, "owner_id": owner_id},
        )

        storage = get_appropriate_storage_service()
        bucket = get_archive_bucket()
        since_date = datetime.fromisoformat(since_date_iso) if since_date_iso else None

        stats = {"postgres": {}, "timescale": {}}

        try:
            # PostgreSQL export
            log.info(
                "Generating PostgreSQL export",
                extra={"export_id": export_id},
            )
            postgres_path = get_postgres_sql_path(owner_id, export_id)
            postgres_stats, postgres_size = self._generate_and_upload_sql(
                owner_id,
                postgres_path,
                bucket,
                storage,
                generate_full_export,
                since_date,
                export_id,
            )
            stats["postgres"] = {
                **postgres_stats,
                "bytes": postgres_size,
                "path": postgres_path,
            }
            log.info(
                "PostgreSQL export completed",
                extra={
                    "export_id": export_id,
                },
            )

            # TimescaleDB export
            log.info(
                "Generating TimescaleDB export",
                extra={"export_id": export_id},
            )
            timescale_path = get_timescale_sql_path(owner_id, export_id)
            timescale_stats, timescale_size = self._generate_and_upload_sql(
                owner_id,
                timescale_path,
                bucket,
                storage,
                generate_timescale_export,
                since_date,
                export_id,
            )
            stats["timescale"] = {
                **timescale_stats,
                "bytes": timescale_size,
                "path": timescale_path,
            }
        except SoftTimeLimitExceeded:
            log.error(
                "SQL export timed out",
                extra={"export_id": export_id},
            )
            _mark_export_failed_by_id(export_id, "SQL export timed out")
            raise
        except Exception as e:
            log.error(
                "SQL export failed",
                extra={"export_id": export_id, "error": str(e)},
            )
            _mark_export_failed_by_id(export_id, str(e))
            raise

        log.info(
            "SQL export task completed",
            extra={
                "export_id": export_id,
                "postgres_rows": stats["postgres"].get("total_rows", 0),
                "timescale_rows": stats["timescale"].get("total_rows", 0),
            },
        )
        return {"sql_stats": stats, "owner_id": owner_id, "export_id": export_id}

    def _generate_and_upload_sql(
        self,
        owner_id,
        gcs_path,
        bucket,
        storage,
        generator_func,
        since_date=None,
        export_id=None,
    ) -> tuple[dict, int]:
        """Generate SQL to temp file and upload to GCS. Returns (stats, file_size)."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".sql", delete=True, encoding="utf-8"
        ) as tmp:
            stats = generator_func(
                owner_id, tmp, since_date=since_date, export_id=export_id
            )
            tmp.flush()
            file_size = os.path.getsize(tmp.name)
            with open(tmp.name, "rb") as f:
                storage.write_file(bucket, gcs_path, f)
            return stats, file_size


class ExportOwnerArchivesTask(BaseCodecovTask, name=export_owner_archives_task_name):
    """Collect archive files and copy them to the export folder."""

    acks_late = True
    max_retries = 3
    soft_time_limit = ARCHIVE_TASK_SOFT_TIME_LIMIT
    time_limit = ARCHIVE_TASK_TIME_LIMIT

    def run_impl(
        self,
        _db_session,
        *,
        owner_id: int,
        export_id: int,
        since_date_iso: str | None = None,
    ) -> dict:
        log.info("Collecting archive files", extra={"export_id": export_id})

        since_date = datetime.fromisoformat(since_date_iso) if since_date_iso else None

        try:
            context = (
                ExportContext(owner_id=owner_id, since_date=since_date)
                if since_date
                else ExportContext(owner_id=owner_id)
            )
            archive_stats = collect_archives_for_export(
                owner_id=owner_id, export_id=export_id, context=context
            )
            log.info(
                "Archive collection task completed",
                extra={
                    "export_id": export_id,
                    "files_copied": archive_stats.get("files_copied", 0),
                    "files_failed": archive_stats.get("files_failed", 0),
                },
            )
        except SoftTimeLimitExceeded:
            log.error(
                "Archive collection timed out",
                extra={"export_id": export_id},
            )
            _mark_export_failed_by_id(export_id, "Archive collection timed out")
            raise
        except Exception as e:
            log.error(
                "Archive collection failed",
                extra={"export_id": export_id, "error": str(e)},
            )
            _mark_export_failed_by_id(export_id, str(e))
            raise

        return {
            "archive_stats": archive_stats,
            "owner_id": owner_id,
            "export_id": export_id,
        }


class ExportOwnerFinalizeTask(BaseCodecovTask, name=export_owner_finalize_task_name):
    """Finalize export: write manifest, create tarball, generate download URL."""

    acks_late = True
    max_retries = 3
    soft_time_limit = FINALIZE_TASK_SOFT_TIME_LIMIT
    time_limit = FINALIZE_TASK_TIME_LIMIT

    def run_impl(
        self,
        _db_session,
        parallel_results: list[dict] | None = None,
        *,
        owner_id: int,
        export_id: int,
    ) -> dict:
        log.info("Finalizing export", extra={"export_id": export_id})

        storage = get_appropriate_storage_service()
        bucket = get_archive_bucket()
        export_path = get_export_path(owner_id, export_id)

        try:
            export = OwnerExport.objects.get(id=export_id)
        except OwnerExport.DoesNotExist:
            log.error(
                "Export record not found in finalize",
                extra={"export_id": export_id},
            )
            raise ValueError(f"Export {export_id} not found")

        if export.owner_id != owner_id:
            log.error(
                "Owner ID mismatch in finalize",
                extra={
                    "export_id": export_id,
                    "expected": owner_id,
                    "actual": export.owner_id,
                },
            )
            export.status = OwnerExport.Status.FAILED
            export.error_message = (
                f"Owner ID mismatch: expected {owner_id}, found {export.owner_id}"
            )
            export.save(update_fields=["status", "error_message", "updated_at"])
            raise ValueError(f"Owner ID mismatch for export {export_id}")

        # Merge results from parallel tasks
        sql_stats, archive_stats = {}, {}
        for result in parallel_results or []:
            if isinstance(result, dict):
                sql_stats = result.get("sql_stats", sql_stats)
                archive_stats = result.get("archive_stats", archive_stats)

        try:
            # Write manifest
            manifest = self._create_manifest(
                owner_id, export_id, export, sql_stats, archive_stats
            )
            manifest_path = get_manifest_path(owner_id, export_id)
            storage.write_file(
                bucket, manifest_path, BytesIO(json.dumps(manifest, indent=2).encode())
            )

            # Create tarball
            tarball_path = get_tarball_path(owner_id, export_id)
            tarball_result = self._create_and_upload_tarball(
                storage,
                bucket,
                export_path,
                tarball_path,
                sql_stats,
                archive_stats,
                manifest_path,
                export_id,
            )

            # Generate presigned URL
            expires_at = timezone.now() + timedelta(seconds=DOWNLOAD_URL_EXPIRY_SECONDS)
            download_url = storage.create_presigned_get(
                bucket, tarball_path, DOWNLOAD_URL_EXPIRY_SECONDS
            )

            # Update export record
            export.status = OwnerExport.Status.COMPLETED
            export.download_url = download_url
            export.download_expires_at = expires_at
            export.stats = {
                "sql_stats": sql_stats,
                "archive_stats": archive_stats,
                "tarball_bytes": tarball_result["tarball_size"],
                "tarball_files_added": tarball_result["files_added"],
                "tarball_files_failed": tarball_result["files_failed"],
                "tarball_failed_files": tarball_result["failed_files"],
            }
            export.save(
                update_fields=[
                    "status",
                    "download_url",
                    "download_expires_at",
                    "stats",
                    "updated_at",
                ]
            )

            log.info(
                "Export completed successfully",
                extra={
                    "export_id": export_id,
                    "tarball_bytes": tarball_result["tarball_size"],
                },
            )
            return {
                "export_id": export_id,
                "owner_id": owner_id,
                "download_url": download_url,
                "expires_at": expires_at.isoformat(),
                "tarball_bytes": tarball_result["tarball_size"],
            }

        except SoftTimeLimitExceeded:
            log.error(
                "Export finalization timed out",
                extra={"export_id": export_id},
            )
            export.status = OwnerExport.Status.FAILED
            export.error_message = "Finalization timed out"
            export.save(update_fields=["status", "error_message", "updated_at"])
            raise
        except Exception as e:
            log.error(
                "Export finalization failed",
                extra={"export_id": export_id, "error": str(e)},
            )
            export.status = OwnerExport.Status.FAILED
            export.error_message = str(e)[:500]
            export.save(update_fields=["status", "error_message", "updated_at"])
            raise

    def _create_manifest(
        self, owner_id, export_id, export, sql_stats, archive_stats
    ) -> dict:
        try:
            owner = Owner.objects.get(ownerid=owner_id)
            owner_info = {
                "ownerid": owner.ownerid,
                "username": owner.username,
                "service": owner.service,
                "service_id": owner.service_id,
            }
        except Owner.DoesNotExist:
            owner_info = {"ownerid": owner_id}

        return {
            "version": "1.0",
            "export_id": export.id,
            "owner": owner_info,
            "export_path": f"exports/{owner_id}/{export_id}/",
            "export_date": timezone.now().isoformat(),
            "since_date": export.since_date.isoformat() if export.since_date else None,
            "days_exported": EXPORT_DAYS_DEFAULT,
            "stats": {"sql": sql_stats, "archives": archive_stats},
            "files": {
                "postgres_sql": "postgres.sql",
                "timescale_sql": "timescale.sql",
                "archives_dir": "archives/",
            },
            "import_instructions": {
                "postgres": "psql -d your_database -f postgres.sql",
                "timescale": "psql -d your_timescale_database -f timescale.sql",
            },
        }

    def _create_and_upload_tarball(
        self,
        storage,
        bucket,
        export_path,
        tarball_path,
        sql_stats,
        archive_stats,
        manifest_path,
        export_id=None,
    ) -> dict:
        log.info("Creating and uploading tarball", extra={"export_id": export_id})
        files_added, files_failed = [], []

        sql_files = [
            ("postgres", sql_stats.get("postgres", {}).get("path")),
            ("timescale", sql_stats.get("timescale", {}).get("path")),
        ]
        if not any(path for _, path in sql_files):
            raise ValueError("No SQL files found in export stats")

        with tempfile.NamedTemporaryFile(suffix=".tar.gz", delete=True) as tmp:
            with tarfile.open(fileobj=tmp, mode="w:gz") as tar:
                # Add SQL files (critical - raise on failure)
                for name, path in sql_files:
                    if path:
                        if self._add_file_to_tarball(
                            tar, storage, bucket, path, export_path, export_id
                        ):
                            files_added.append(path)
                        else:
                            raise ValueError(f"Failed to add {name}.sql to tarball")

                # Add manifest (critical)
                if self._add_file_to_tarball(
                    tar, storage, bucket, manifest_path, export_path, export_id
                ):
                    files_added.append(manifest_path)
                else:
                    raise ValueError("Failed to add manifest.json to tarball")

                # Add archive files (non-critical)
                for path in archive_stats.get("copied_files") or []:
                    if self._add_file_to_tarball(
                        tar, storage, bucket, path, export_path, export_id
                    ):
                        files_added.append(path)
                    else:
                        files_failed.append(path)

            tmp.flush()
            tarball_size = tmp.tell()
            tmp.seek(0)
            storage.write_file(bucket, tarball_path, tmp)

        return {
            "tarball_size": tarball_size,
            "files_added": len(files_added),
            "files_failed": len(files_failed),
            "failed_files": files_failed,
        }

    def _add_file_to_tarball(
        self, tar, storage, bucket, file_path, export_path, export_id=None
    ) -> bool:
        try:
            file_content = BytesIO()
            storage.read_file(bucket, file_path, file_obj=file_content)
            file_content.seek(0)

            arcname = file_path.replace(export_path, "").lstrip(
                "/"
            ) or os.path.basename(file_path)
            tarinfo = tarfile.TarInfo(name=arcname)
            tarinfo.size = len(file_content.getvalue())
            tarinfo.mtime = int(timezone.now().timestamp())
            tar.addfile(tarinfo, file_content)
            return True
        except Exception as e:
            log.warning(
                "Failed to add file to tarball",
                extra={"export_id": export_id, "file_path": file_path, "error": str(e)},
            )
            return False


class ExportOwnerCleanupTask(BaseCodecovTask, name=export_owner_cleanup_task_name):
    """Cleanup task that runs when export fails. Removes partial files from GCS."""

    acks_late = True
    max_retries = 3

    def run_impl(
        self,
        _db_session,
        failed_task_id_or_request=None,
        exc=None,
        _traceback=None,
        *,
        owner_id: int,
        export_id: int,
    ) -> dict:
        log.info("Cleaning up failed export", extra={"export_id": export_id})

        # Extract error info (handles both old-style and new-style Celery errbacks)
        failed_task_id = self._extract_task_id(failed_task_id_or_request)
        error_message = self._get_error_message(failed_task_id, exc)
        export_marked = _mark_export_failed_by_id(export_id, error_message)

        storage = get_appropriate_storage_service()
        bucket = get_archive_bucket()

        deleted_files, failed_deletes = [], []

        # Delete known files
        for path in [
            get_postgres_sql_path(owner_id, export_id),
            get_timescale_sql_path(owner_id, export_id),
            get_manifest_path(owner_id, export_id),
            get_tarball_path(owner_id, export_id),
        ]:
            try:
                storage.delete_file(bucket, path)
                deleted_files.append(path)
            except Exception:
                failed_deletes.append(path)

        # Delete archive files by prefix
        deleted_files.extend(
            self._cleanup_archive_files(
                storage, bucket, get_archives_path(owner_id, export_id), export_id
            )
        )

        log.info(
            "Cleanup task completed",
            extra={
                "export_id": export_id,
                "files_deleted": len(deleted_files),
                "error_message": error_message,
            },
        )
        return {
            "export_id": export_id,
            "owner_id": owner_id,
            "deleted_files": deleted_files,
            "failed_deletes": failed_deletes,
            "export_marked_failed": export_marked,
        }

    def _cleanup_archive_files(
        self, storage, bucket: str, prefix: str, export_id: int | None = None
    ) -> list[str]:
        deleted = []
        try:
            if hasattr(storage, "minio_client"):
                for obj in storage.minio_client.list_objects(
                    bucket, prefix=prefix, recursive=True
                ):
                    try:
                        storage.delete_file(bucket, obj.object_name)
                        deleted.append(obj.object_name)
                    except Exception:
                        pass
        except Exception as e:
            log.error(
                "Failed to cleanup archive files",
                extra={"export_id": export_id, "error": str(e)},
            )
        return deleted

    def _extract_task_id(self, failed_task_id_or_request) -> str | None:
        if failed_task_id_or_request is None:
            return None
        if hasattr(failed_task_id_or_request, "id"):
            return failed_task_id_or_request.id
        if isinstance(failed_task_id_or_request, str):
            return failed_task_id_or_request
        return None

    def _get_error_message(
        self, failed_task_id: str | None, exc: Exception | None
    ) -> str:
        if exc is not None:
            return f"Export failed: {str(exc)[:450]}"
        if failed_task_id is None:
            return "Export failed (unknown error)"
        try:
            result = AsyncResult(failed_task_id)
            if result.failed() and result.result:
                return f"Export failed: {str(result.result)[:450]}"
        except Exception:
            pass
        return f"Export failed (task {failed_task_id})"


celery_app.register_task(ExportOwnerTask())
celery_app.register_task(ExportOwnerSQLTask())
celery_app.register_task(ExportOwnerArchivesTask())
celery_app.register_task(ExportOwnerFinalizeTask())
celery_app.register_task(ExportOwnerCleanupTask())

export_owner_task = celery_app.tasks[ExportOwnerTask.name]
export_owner_sql_task = celery_app.tasks[ExportOwnerSQLTask.name]
export_owner_archives_task = celery_app.tasks[ExportOwnerArchivesTask.name]
export_owner_finalize_task = celery_app.tasks[ExportOwnerFinalizeTask.name]
export_owner_cleanup_task = celery_app.tasks[ExportOwnerCleanupTask.name]
