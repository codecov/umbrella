"""
Export Owner Data Tasks

Orchestrates the export of an owner's data to a downloadable archive.
The export includes:
- PostgreSQL UPSERT statements for main database models
- TimescaleDB UPSERT statements for time-series models
- A manifest file with export metadata

Tasks:
- ExportOwnerTask: Orchestrator that coordinates the export pipeline
- ExportOwnerSQLTask: Generates SQL export files
- ExportOwnerArchivesTask: Collects archive files from GCS
- ExportOwnerFinalizeTask: Creates tarball and presigned download URL

Architecture:
- SQL and Archive tasks run in PARALLEL (chord)
- Finalize task runs after both complete, receiving combined results
- Uses streaming to avoid memory issues with large exports
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


class ExportOwnerTask(BaseCodecovTask, name=export_owner_task_name):
    """
    Orchestrator task that coordinates the export pipeline.
    """

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
        log.info("Starting export for owner %d, export %d", owner_id, export_id)

        try:
            export = OwnerExport.objects.get(id=export_id)
        except OwnerExport.DoesNotExist:
            log.error("OwnerExport %d not found", export_id)
            return {"error": "Export not found"}

        if export.owner_id != owner_id:
            error_msg = (
                f"Owner ID mismatch: export belongs to owner {export.owner_id}, "
                f"not {owner_id}"
            )
            log.error(
                "Owner ID mismatch: export %d belongs to owner %d, not %d",
                export_id,
                export.owner_id,
                owner_id,
            )
            self._mark_export_failed(export, error_msg)
            return {"error": "Owner ID mismatch"}

        export.status = OwnerExport.Status.IN_PROGRESS
        export.save(update_fields=["status", "updated_at"])

        since_date_iso = export.since_date.isoformat() if export.since_date else None

        # Run SQL and Archives in parallel, then finalize
        # Use link_error to trigger cleanup if any task fails
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

        # Cleanup task runs if any header task fails, preventing orphaned files
        result = workflow.apply_async(
            link_error=export_owner_cleanup_task.s(
                owner_id=owner_id, export_id=export_id
            ),
            allow_error_cb_on_chord_header=True,
        )

        export.task_ids = {"chord_id": result.id}
        export.save(update_fields=["task_ids", "updated_at"])

        return {
            "export_id": export_id,
            "owner_id": owner_id,
            "chord_id": result.id,
        }

    def _mark_export_failed(self, export: OwnerExport, error_message: str):
        """Mark the export as failed with an error message."""
        export.status = OwnerExport.Status.FAILED
        export.error_message = error_message[:500]
        export.save(update_fields=["status", "error_message", "updated_at"])


class ExportOwnerSQLTask(BaseCodecovTask, name=export_owner_sql_task_name):
    """
    Generate PostgreSQL and TimescaleDB SQL export files.

    Uses streaming via temp files to avoid memory issues with large exports.
    Uploads directly to GCS without holding entire file in memory.
    """

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
        log.info("Generating SQL export for owner %d, export %d", owner_id, export_id)

        storage = get_appropriate_storage_service()
        bucket = get_archive_bucket()

        since_date = None
        if since_date_iso:
            since_date = datetime.fromisoformat(since_date_iso)

        stats = {
            "postgres": {},
            "timescale": {},
        }

        try:
            # Generate PostgreSQL export using temp file for streaming
            postgres_path = get_postgres_sql_path(owner_id, export_id)
            postgres_stats, postgres_size = self._generate_and_upload_sql(
                owner_id,
                postgres_path,
                bucket,
                storage,
                generate_full_export,
                since_date,
            )
            stats["postgres"] = postgres_stats
            stats["postgres"]["bytes"] = postgres_size
            stats["postgres"]["path"] = postgres_path
            log.info(
                "PostgreSQL export complete: %d rows, %d bytes",
                stats["postgres"].get("total_rows", 0),
                postgres_size,
            )

            # Generate TimescaleDB export using temp file for streaming
            timescale_path = get_timescale_sql_path(owner_id, export_id)
            timescale_stats, timescale_size = self._generate_and_upload_sql(
                owner_id,
                timescale_path,
                bucket,
                storage,
                generate_timescale_export,
                since_date,
            )
            stats["timescale"] = timescale_stats
            stats["timescale"]["bytes"] = timescale_size
            stats["timescale"]["path"] = timescale_path
            log.info(
                "TimescaleDB export complete: %d rows, %d bytes",
                stats["timescale"].get("total_rows", 0),
                timescale_size,
            )

        except SoftTimeLimitExceeded:
            log.error("SQL export timed out for owner %d", owner_id)
            self._mark_export_failed(export_id, "SQL export timed out")
            raise

        except Exception as e:
            log.error("SQL export failed for owner %d: %s", owner_id, str(e))
            self._mark_export_failed(export_id, str(e))
            raise

        return {
            "sql_stats": stats,
            "owner_id": owner_id,
            "export_id": export_id,
        }

    def _generate_and_upload_sql(
        self,
        owner_id: int,
        gcs_path: str,
        bucket: str,
        storage,
        generator_func,
        since_date=None,
    ) -> tuple[dict, int]:
        """
        Generate SQL to a temp file and stream upload to GCS.

        This avoids holding the entire SQL content in memory.
        Returns (stats_dict, file_size_bytes).
        """
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".sql",
            delete=True,
            encoding="utf-8",
        ) as tmp:
            # Generate SQL directly to temp file
            stats = generator_func(owner_id, tmp, since_date=since_date)
            tmp.flush()

            file_size = os.path.getsize(tmp.name)

            with open(tmp.name, "rb") as f:
                storage.write_file(bucket, gcs_path, f)

            return stats, file_size

    def _mark_export_failed(self, export_id: int, error_message: str):
        try:
            export = OwnerExport.objects.get(id=export_id)
            export.status = OwnerExport.Status.FAILED
            export.error_message = error_message[:500]
            export.save(update_fields=["status", "error_message", "updated_at"])
        except OwnerExport.DoesNotExist:
            pass


class ExportOwnerArchivesTask(BaseCodecovTask, name=export_owner_archives_task_name):
    """
    Collect archive files and copy them to the exports/<owner_id>/<export_id>/ folder.

    Uses streaming copy and parallel workers for efficient handling
    of large numbers of files.
    """

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
        log.info(
            "Collecting archive files for owner %d, export %d", owner_id, export_id
        )

        since_date = None
        if since_date_iso:
            since_date = datetime.fromisoformat(since_date_iso)

        try:
            context = (
                ExportContext(owner_id=owner_id, since_date=since_date)
                if since_date
                else ExportContext(owner_id=owner_id)
            )
            archive_stats = collect_archives_for_export(
                owner_id=owner_id,
                export_id=export_id,
                context=context,
            )

            log.info(
                "Archive collection complete: %d copied, %d failed",
                archive_stats.get("files_copied", 0),
                archive_stats.get("files_failed", 0),
            )

        except SoftTimeLimitExceeded:
            log.error("Archive collection timed out for owner %d", owner_id)
            self._mark_export_failed(export_id, "Archive collection timed out")
            raise

        except Exception as e:
            log.error("Archive collection failed for owner %d: %s", owner_id, str(e))
            self._mark_export_failed(export_id, str(e))
            raise

        return {
            "archive_stats": archive_stats,
            "owner_id": owner_id,
            "export_id": export_id,
        }

    def _mark_export_failed(self, export_id: int, error_message: str):
        try:
            export = OwnerExport.objects.get(id=export_id)
            export.status = OwnerExport.Status.FAILED
            export.error_message = error_message[:500]
            export.save(update_fields=["status", "error_message", "updated_at"])
        except OwnerExport.DoesNotExist:
            pass


class ExportOwnerFinalizeTask(BaseCodecovTask, name=export_owner_finalize_task_name):
    """
    Finalize the export by:
    1. Writing a manifest.json with export metadata
    2. Creating a tarball of all exported files (streamed to temp file)
    3. Generating a presigned download URL
    4. Updating the OwnerExport record

    Receives results from both SQL and Archive tasks via chord callback.
    """

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
        log.info("Finalizing export for owner %d, export %d", owner_id, export_id)

        storage = get_appropriate_storage_service()
        bucket = get_archive_bucket()
        export_path = get_export_path(owner_id, export_id)

        try:
            export = OwnerExport.objects.get(id=export_id)
        except OwnerExport.DoesNotExist:
            log.error("OwnerExport %d not found during finalization", export_id)
            raise ValueError(
                f"Export {export_id} not found - export record may have been deleted during processing"
            )

        if export.owner_id != owner_id:
            log.error(
                "Owner ID mismatch: export %d belongs to owner %d, not %d",
                export_id,
                export.owner_id,
                owner_id,
            )
            self._mark_export_failed(
                export,
                f"Owner ID mismatch: expected {owner_id}, found {export.owner_id}",
            )
            raise ValueError(
                f"Owner ID mismatch for export {export_id}: expected {owner_id}, found {export.owner_id}"
            )

        # Merge results from parallel SQL and Archive tasks
        combined_result = self._merge_parallel_results(parallel_results)
        sql_stats = combined_result.get("sql_stats") or {}
        archive_stats = combined_result.get("archive_stats") or {}

        try:
            # 1. Write manifest
            manifest = self._create_manifest(
                owner_id, export_id, export, sql_stats, archive_stats
            )
            manifest_path = get_manifest_path(owner_id, export_id)
            manifest_bytes = json.dumps(manifest, indent=2).encode("utf-8")
            storage.write_file(bucket, manifest_path, BytesIO(manifest_bytes))
            log.info("Manifest written to %s", manifest_path)

            # 2. Create tarball using temp file to avoid memory issues
            tarball_path = get_tarball_path(owner_id, export_id)
            tarball_result = self._create_and_upload_tarball(
                storage,
                bucket,
                export_path,
                tarball_path,
                sql_stats,
                archive_stats,
                manifest_path,
            )
            tarball_size = tarball_result["tarball_size"]
            tarball_files_failed = tarball_result["files_failed"]

            if tarball_files_failed > 0:
                log.warning(
                    "Tarball created with %d files failed to add: %s",
                    tarball_files_failed,
                    tarball_result["failed_files"][:10],
                )

            log.info(
                "Tarball created and uploaded: %d bytes, %d files added, %d files failed",
                tarball_size,
                tarball_result["files_added"],
                tarball_files_failed,
            )

            # 3. Generate presigned download URL
            expires_at = timezone.now() + timedelta(seconds=DOWNLOAD_URL_EXPIRY_SECONDS)
            download_url = storage.create_presigned_get(
                bucket, tarball_path, DOWNLOAD_URL_EXPIRY_SECONDS
            )

            # 4. Update export record
            export.status = OwnerExport.Status.COMPLETED
            export.download_url = download_url
            export.download_expires_at = expires_at
            export.stats = {
                "sql_stats": sql_stats,
                "archive_stats": archive_stats,
                "tarball_bytes": tarball_size,
                "tarball_files_added": tarball_result["files_added"],
                "tarball_files_failed": tarball_files_failed,
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
                "Export %d completed. Download URL expires at %s",
                export_id,
                expires_at.isoformat(),
            )

            return {
                "export_id": export_id,
                "owner_id": owner_id,
                "download_url": download_url,
                "expires_at": expires_at.isoformat(),
                "tarball_bytes": tarball_size,
            }

        except SoftTimeLimitExceeded:
            log.error("Export finalization timed out for owner %d", owner_id)
            self._mark_export_failed(export, "Finalization timed out")
            raise

        except Exception as e:
            log.error("Export finalization failed for owner %d: %s", owner_id, str(e))
            self._mark_export_failed(export, str(e))
            raise

    def _merge_parallel_results(self, results: list[dict] | None) -> dict:
        """
        Merge results from parallel SQL and Archive tasks.

        The chord callback receives a list of results from the parallel tasks.
        We need to combine them into a single dict.
        Handles None or invalid results gracefully.
        """
        merged = {}
        if not results:
            return merged

        for result in results:
            if not isinstance(result, dict):
                log.warning(
                    "Unexpected result type in parallel results: %s", type(result)
                )
                continue
            if "sql_stats" in result:
                merged["sql_stats"] = result["sql_stats"]
            if "archive_stats" in result:
                merged["archive_stats"] = result["archive_stats"]
        return merged

    def _create_manifest(
        self,
        owner_id: int,
        export_id: int,
        export: OwnerExport,
        sql_stats: dict,
        archive_stats: dict,
    ) -> dict:
        """Create the export manifest with metadata."""
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

        since_date = export.since_date

        return {
            "version": "1.0",
            "export_id": export.id,
            "owner": owner_info,
            "export_path": f"exports/{owner_id}/{export_id}/",
            "export_date": timezone.now().isoformat(),
            "since_date": since_date.isoformat() if since_date else None,
            "days_exported": EXPORT_DAYS_DEFAULT,
            "stats": {
                "sql": sql_stats,
                "archives": archive_stats,
            },
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
        bucket: str,
        export_path: str,
        tarball_path: str,
        sql_stats: dict,
        archive_stats: dict,
        manifest_path: str,
    ) -> dict:
        """
        Create a tarball using a temp file and stream upload to GCS.

        This avoids holding the entire tarball in memory.
        Returns a dict with tarball_size, files_added, and files_failed.
        Raises ValueError if critical files (SQL, manifest) fail to be added.
        """
        files_added = []
        files_failed = []

        with tempfile.NamedTemporaryFile(
            suffix=".tar.gz",
            delete=True,
        ) as tmp:
            with tarfile.open(fileobj=tmp, mode="w:gz") as tar:
                sql_files = [
                    ("postgres", sql_stats.get("postgres", {}).get("path")),
                    ("timescale", sql_stats.get("timescale", {}).get("path")),
                ]

                for sql_name, file_path in sql_files:
                    if file_path:
                        if self._add_file_to_tarball(
                            tar, storage, bucket, file_path, export_path
                        ):
                            files_added.append(file_path)
                        else:
                            raise ValueError(
                                f"Failed to add critical file {sql_name}.sql to tarball"
                            )

                # Add manifest
                if self._add_file_to_tarball(
                    tar, storage, bucket, manifest_path, export_path
                ):
                    files_added.append(manifest_path)
                else:
                    raise ValueError("Failed to add manifest.json to tarball")

                # Add archive files
                archive_files = archive_stats.get("copied_files") or []
                for file_path in archive_files:
                    if self._add_file_to_tarball(
                        tar, storage, bucket, file_path, export_path
                    ):
                        files_added.append(file_path)
                    else:
                        files_failed.append(file_path)

            # Get file size and upload
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
        self,
        tar: tarfile.TarFile,
        storage,
        bucket: str,
        file_path: str,
        export_path: str,
    ) -> bool:
        """
        Add a single file from GCS to the tarball.

        Returns True if the file was added successfully, False otherwise.
        """
        try:
            file_content = BytesIO()
            storage.read_file(bucket, file_path, file_obj=file_content)
            file_content.seek(0)

            arcname = file_path.replace(export_path, "")
            if arcname.startswith("/"):
                arcname = arcname[1:]

            tarinfo = tarfile.TarInfo(name=arcname)
            tarinfo.size = len(file_content.getvalue())
            tarinfo.mtime = int(timezone.now().timestamp())
            tar.addfile(tarinfo, file_content)

            log.debug("Added to tarball: %s", arcname)
            return True

        except Exception as e:
            log.warning("Failed to add %s to tarball: %s", file_path, str(e))
            return False

    def _mark_export_failed(self, export: OwnerExport, error_message: str):
        export.status = OwnerExport.Status.FAILED
        export.error_message = error_message[:500]
        export.save(update_fields=["status", "error_message", "updated_at"])


class ExportOwnerCleanupTask(BaseCodecovTask, name=export_owner_cleanup_task_name):
    """
    Cleanup task that runs when the export chord fails.

    Removes any partial files that were uploaded before the failure to prevent
    orphaned resources in GCS. This task is triggered via link_error on the chord.

    Note: Archive files under exports/<owner_id>/archives/ are cleaned up by
    listing and deleting objects with that prefix using the minio client directly.
    """

    acks_late = True
    max_retries = 3

    def run_impl(
        self,
        _db_session,
        failed_task_id: str | None = None,
        *,
        owner_id: int,
        export_id: int,
    ) -> dict:
        log.info(
            "Cleaning up failed export for owner %d, export %d", owner_id, export_id
        )

        error_message = self._get_error_message_from_task(failed_task_id)
        export_marked = self._mark_export_failed(export_id, error_message)

        storage = get_appropriate_storage_service()
        bucket = get_archive_bucket()

        deleted_files = []
        failed_deletes = []

        # Known files that might have been created before failure
        files_to_delete = [
            get_postgres_sql_path(owner_id, export_id),
            get_timescale_sql_path(owner_id, export_id),
            get_manifest_path(owner_id, export_id),
            get_tarball_path(owner_id, export_id),
        ]

        for file_path in files_to_delete:
            try:
                storage.delete_file(bucket, file_path)
                deleted_files.append(file_path)
                log.debug("Deleted partial file: %s", file_path)
            except Exception as e:
                # File may not exist if the task failed before creating it
                log.debug("Could not delete %s: %s", file_path, str(e))
                failed_deletes.append(file_path)

        # Clean up archive files by listing and deleting objects with the archives prefix
        archives_prefix = get_archives_path(owner_id, export_id)
        archive_files_deleted = self._cleanup_archive_files(
            storage, bucket, archives_prefix
        )
        deleted_files.extend(archive_files_deleted)

        log.info(
            "Export cleanup complete for owner %d: %d files deleted, export marked failed: %s",
            owner_id,
            len(deleted_files),
            export_marked,
        )

        return {
            "export_id": export_id,
            "owner_id": owner_id,
            "deleted_files": deleted_files,
            "failed_deletes": failed_deletes,
            "export_marked_failed": export_marked,
        }

    def _cleanup_archive_files(self, storage, bucket: str, prefix: str) -> list[str]:
        """
        Delete all files under the given prefix.

        Uses the minio client directly to list objects since the storage service
        doesn't expose a list method.
        """
        deleted = []
        try:
            # Access minio client directly for listing objects
            if hasattr(storage, "minio_client"):
                objects = storage.minio_client.list_objects(
                    bucket, prefix=prefix, recursive=True
                )
                for obj in objects:
                    try:
                        storage.delete_file(bucket, obj.object_name)
                        deleted.append(obj.object_name)
                        log.debug("Deleted archive file: %s", obj.object_name)
                    except Exception as e:
                        log.warning(
                            "Failed to delete archive file %s: %s",
                            obj.object_name,
                            str(e),
                        )
            else:
                log.warning(
                    "Storage service does not support listing objects, "
                    "archive files under %s may be orphaned",
                    prefix,
                )
        except Exception as e:
            log.error(
                "Failed to list/delete archive files under %s: %s", prefix, str(e)
            )

        return deleted

    def _get_error_message_from_task(self, failed_task_id: str | None) -> str:
        """
        Extract error message from a failed task using AsyncResult.

        Celery's link_error callback receives the failed task's ID, not the exception.
        We use AsyncResult to retrieve the actual exception from the task result backend.
        """
        if failed_task_id is None:
            return "Export failed (unknown error)"

        try:
            result = AsyncResult(failed_task_id)
            if result.failed() and result.result is not None:
                exc = result.result
                return f"Export failed: {str(exc)[:450]}"
            return f"Export failed (task {failed_task_id} failed)"
        except Exception as e:
            log.warning(
                "Could not retrieve exception from failed task %s: %s",
                failed_task_id,
                str(e),
            )
            return f"Export failed (task {failed_task_id})"

    def _mark_export_failed(self, export_id: int, error_message: str) -> bool:
        """
        Mark the export as failed if it's not already in a terminal state.

        Returns True if the export was marked as failed, False otherwise.
        """
        try:
            export = OwnerExport.objects.get(id=export_id)

            if export.status in (
                OwnerExport.Status.COMPLETED,
                OwnerExport.Status.FAILED,
            ):
                log.info(
                    "Export %d already in terminal state %s, not updating",
                    export_id,
                    export.status,
                )
                return False

            export.status = OwnerExport.Status.FAILED
            export.error_message = error_message[:500]
            export.save(update_fields=["status", "error_message", "updated_at"])
            log.info("Marked export %d as FAILED: %s", export_id, error_message)
            return True

        except OwnerExport.DoesNotExist:
            log.warning(
                "OwnerExport %d not found when trying to mark as failed", export_id
            )
            return False
        except Exception as e:
            log.error("Failed to mark export %d as failed: %s", export_id, str(e))
            return False


# Register tasks
RegisteredExportOwnerTask = celery_app.register_task(ExportOwnerTask())
RegisteredExportOwnerSQLTask = celery_app.register_task(ExportOwnerSQLTask())
RegisteredExportOwnerArchivesTask = celery_app.register_task(ExportOwnerArchivesTask())
RegisteredExportOwnerFinalizeTask = celery_app.register_task(ExportOwnerFinalizeTask())
RegisteredExportOwnerCleanupTask = celery_app.register_task(ExportOwnerCleanupTask())

export_owner_task = celery_app.tasks[ExportOwnerTask.name]
export_owner_sql_task = celery_app.tasks[ExportOwnerSQLTask.name]
export_owner_archives_task = celery_app.tasks[ExportOwnerArchivesTask.name]
export_owner_finalize_task = celery_app.tasks[ExportOwnerFinalizeTask.name]
export_owner_cleanup_task = celery_app.tasks[ExportOwnerCleanupTask.name]
