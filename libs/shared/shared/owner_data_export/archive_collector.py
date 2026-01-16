"""
Archive Collector for owner data export.

Copies GCS archive files within the same bucket to the exports/<owner_id>/<export_id>/ folder.
Uses server-side copy for efficiency when source and destination are in the same bucket.
Uses parallel copying with a thread pool for improved performance.
"""

import logging
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from threading import Lock

from django.utils import timezone
from minio.commonconfig import CopySource
from minio.error import S3Error

import shared.storage
from shared.api_archive.archive import ArchiveService
from shared.bundle_analysis.storage import StoragePaths as BundleStoragePaths
from shared.bundle_analysis.storage import get_bucket_name as get_bundle_analysis_bucket
from shared.storage.base import BaseStorageService
from shared.storage.exceptions import FileNotInStorageError

from .config import (
    ARCHIVE_COPY_WORKERS,
    EXPORT_DAYS_DEFAULT,
    get_archive_bucket,
    get_archive_destination_path,
)
from .models_registry import get_model_class
from .sql_generator import ExportContext

log = logging.getLogger(__name__)


@dataclass
class ArchiveFile:
    """Represents a file to be copied from archive to export."""

    source_path: str
    dest_path: str
    source_model: str
    source_bucket: str | None = None


def discover_archive_files(
    context: ExportContext,
    owner_id: int,
    export_id: int,
) -> Generator[ArchiveFile, None, dict]:
    """
    Discover all archive files that need to be exported.
    Yields ArchiveFile objects for each file found.
    Returns stats dict when complete.
    """
    stats = {
        "commits_with_report": 0,
        "uploads_with_storage": 0,
        "chunks_files": 0,
        "bundle_analysis_reports": 0,
    }

    # 1. Commit report storage paths
    Commit = get_model_class("core.Commit")
    commits_qs = Commit.objects.filter(
        id__in=context.commit_ids,
        _report_storage_path__isnull=False,
    ).values_list("id", "_report_storage_path", "repository_id")

    for commit_id, storage_path, repo_id in commits_qs.iterator():
        if storage_path:
            stats["commits_with_report"] += 1
            yield ArchiveFile(
                source_path=storage_path,
                dest_path=get_archive_destination_path(
                    owner_id, export_id, storage_path
                ),
                source_model=f"core.Commit:{commit_id}",
            )

    # 2. ReportSession (Upload) storage paths
    ReportSession = get_model_class("reports.ReportSession")
    uploads_qs = ReportSession.objects.filter(
        report_id__in=context.commit_report_ids,
        storage_path__isnull=False,
    ).values_list("id", "storage_path")

    for session_id, storage_path in uploads_qs.iterator():
        if storage_path:
            stats["uploads_with_storage"] += 1
            yield ArchiveFile(
                source_path=storage_path,
                dest_path=get_archive_destination_path(
                    owner_id, export_id, storage_path
                ),
                source_model=f"reports.ReportSession:{session_id}",
            )

    # 3. Chunks files - constructed from commit + repository info
    Repository = get_model_class("core.Repository")
    repos = {
        r.repoid: r
        for r in Repository.objects.filter(repoid__in=context.repository_ids)
    }

    commits_for_chunks = Commit.objects.filter(
        id__in=context.commit_ids,
    ).values_list("commitid", "repository_id")

    for commitid, repo_id in commits_for_chunks.iterator():
        repo = repos.get(repo_id)
        if repo:
            repo_hash = ArchiveService.get_archive_hash(repo)
            chunks_path = f"v4/repos/{repo_hash}/commits/{commitid}/chunks.txt"
            stats["chunks_files"] += 1
            yield ArchiveFile(
                source_path=chunks_path,
                dest_path=get_archive_destination_path(
                    owner_id, export_id, chunks_path
                ),
                source_model=f"chunks:{commitid}",
            )

    # 4. Bundle Analysis reports (stored in separate bucket)
    CommitReport = get_model_class("reports.CommitReport")
    bundle_reports_qs = CommitReport.objects.filter(
        id__in=context.commit_report_ids,
        report_type="bundle_analysis",
    ).select_related("commit__repository")

    bundle_bucket = get_bundle_analysis_bucket()
    for bundle_report in bundle_reports_qs.iterator():
        commit = bundle_report.commit
        repo = commit.repository
        if repo:
            repo_hash = ArchiveService.get_archive_hash(repo)
            bundle_path = BundleStoragePaths.bundle_report.path(
                repo_key=repo_hash,
                report_key=commit.commitid,
            )
            stats["bundle_analysis_reports"] += 1
            yield ArchiveFile(
                source_path=bundle_path,
                dest_path=get_archive_destination_path(
                    owner_id, export_id, bundle_path
                ),
                source_model=f"reports.CommitReport:{bundle_report.id}",
                source_bucket=bundle_bucket,
            )

    return stats


def copy_archive_file(
    bucket: str,
    file: ArchiveFile,
    storage: BaseStorageService,
) -> tuple[bool, int]:
    """
    Copy a file to the export location using server-side copy.

    Args:
        bucket: The destination bucket
        file: The ArchiveFile to copy
        storage: The storage service to use

    Returns (success, bytes_copied).
    """
    source_bucket = file.source_bucket or bucket

    try:
        storage.minio_client.copy_object(
            bucket,
            file.dest_path,
            CopySource(source_bucket, file.source_path),
        )
        stat = storage.minio_client.stat_object(bucket, file.dest_path)
        return True, stat.size
    except FileNotInStorageError:
        log.warning("File not found: %s (from %s)", file.source_path, file.source_model)
        return False, 0
    except S3Error as e:
        if e.code == "NoSuchKey":
            log.warning(
                "File not found: %s (from %s)", file.source_path, file.source_model
            )
            return False, 0
        log.error("Failed to copy %s: %s", file.source_path, str(e), exc_info=True)
        return False, 0
    except Exception as e:
        log.error("Failed to copy %s: %s", file.source_path, str(e), exc_info=True)
        return False, 0


def collect_archives_for_export(
    owner_id: int,
    export_id: int,
    context: ExportContext | None = None,
) -> dict:
    """
    Discover and copy all archive files for an owner export.

    Files are copied within the same bucket to exports/<owner_id>/<export_id>/archives/.
    Uses streaming copy to handle large files efficiently.
    Uses parallel copying with a thread pool for improved performance.

    Args:
        owner_id: The owner ID
        export_id: The export ID (ensures unique paths for concurrent exports)
        context: ExportContext

    Returns:
        Dict with collection statistics including list of copied file paths
    """
    if context is None:
        context = ExportContext(owner_id=owner_id)

    bucket = get_archive_bucket()

    stats = {
        "files_found": 0,
        "files_copied": 0,
        "files_failed": 0,
        "total_bytes": 0,
        "discovery_stats": {},
        "copied_files": [],
    }
    stats_lock = Lock()

    def copy_single_file(file: ArchiveFile) -> tuple[bool, int, str]:
        """Copy a single file and return (success, bytes, dest_path)."""
        # Each thread gets its own storage client to avoid connection issues
        storage = shared.storage.get_appropriate_storage_service()
        success, bytes_copied = copy_archive_file(bucket, file, storage)
        return success, bytes_copied, file.dest_path

    files_to_copy: list[ArchiveFile] = []
    file_gen = discover_archive_files(context, owner_id, export_id)

    try:
        while True:
            file = next(file_gen)
            files_to_copy.append(file)
    except StopIteration as e:
        if e.value:
            stats["discovery_stats"] = e.value

    stats["files_found"] = len(files_to_copy)
    log.info(
        "Discovered %d archive files to copy for owner %d", len(files_to_copy), owner_id
    )

    # Copy files in parallel using thread pool
    with ThreadPoolExecutor(max_workers=ARCHIVE_COPY_WORKERS) as executor:
        futures = {
            executor.submit(copy_single_file, file): file for file in files_to_copy
        }

        for future in as_completed(futures):
            try:
                success, bytes_copied, dest_path = future.result()
                with stats_lock:
                    if success:
                        stats["files_copied"] += 1
                        stats["total_bytes"] += bytes_copied
                        stats["copied_files"].append(dest_path)
                    else:
                        stats["files_failed"] += 1
            except Exception as e:
                file = futures[future]
                log.error("Unexpected error copying %s: %s", file.source_path, str(e))
                with stats_lock:
                    stats["files_failed"] += 1

    log.info(
        "Archive collection complete for owner %d: %d found, %d copied, %d failed, %d bytes",
        owner_id,
        stats["files_found"],
        stats["files_copied"],
        stats["files_failed"],
        stats["total_bytes"],
    )

    return stats


def collect_archives_for_repository(
    owner_id: int,
    export_id: int,
    repository_id: int,
    since_date: datetime | None = None,
) -> dict:
    """
    Collect archive files for a SINGLE repository.

    This function is designed to be called as a separate Celery task per repository,
    enabling parallel processing and fault isolation.

    Args:
        owner_id: The owner ID
        export_id: The export ID (ensures unique paths for concurrent exports)
        repository_id: The specific repository to export
        since_date: Only export commits updated after this date (default: 60 days ago)

    Returns:
        Dict with collection statistics for this repository
    """
    if since_date is None:
        since_date = timezone.now() - timedelta(days=EXPORT_DAYS_DEFAULT)

    bucket = get_archive_bucket()
    bundle_bucket = get_bundle_analysis_bucket()
    storage = shared.storage.get_appropriate_storage_service()

    stats = {
        "repository_id": repository_id,
        "files_found": 0,
        "files_copied": 0,
        "files_failed": 0,
        "total_bytes": 0,
    }

    Repository = get_model_class("core.Repository")
    Commit = get_model_class("core.Commit")
    ReportSession = get_model_class("reports.ReportSession")
    CommitReport = get_model_class("reports.CommitReport")

    try:
        repo = Repository.objects.get(repoid=repository_id, author_id=owner_id)
    except Repository.DoesNotExist:
        log.error(
            "Repository %d not found or does not belong to owner %d",
            repository_id,
            owner_id,
        )
        return stats

    repo_hash = ArchiveService.get_archive_hash(repo)

    commits_qs = Commit.objects.filter(
        repository_id=repository_id,
        updatestamp__gte=since_date,
    )

    if not commits_qs.exists():
        log.info(
            "No commits found for repository %d since %s", repository_id, since_date
        )
        return stats

    # 1. Commit report storage paths
    for commit_id, storage_path in (
        commits_qs.filter(_report_storage_path__isnull=False)
        .values_list("id", "_report_storage_path")
        .iterator()
    ):
        if storage_path:
            file = ArchiveFile(
                source_path=storage_path,
                dest_path=get_archive_destination_path(
                    owner_id, export_id, storage_path
                ),
                source_model=f"core.Commit:{commit_id}",
            )
            stats["files_found"] += 1
            success, size = copy_archive_file(bucket, file, storage)
            if success:
                stats["files_copied"] += 1
                stats["total_bytes"] += size
            else:
                stats["files_failed"] += 1

    # 2. Chunks files
    for commitid in commits_qs.values_list("commitid", flat=True).iterator():
        chunks_path = f"v4/repos/{repo_hash}/commits/{commitid}/chunks.txt"
        file = ArchiveFile(
            source_path=chunks_path,
            dest_path=get_archive_destination_path(owner_id, export_id, chunks_path),
            source_model=f"chunks:{commitid}",
        )
        stats["files_found"] += 1
        success, size = copy_archive_file(bucket, file, storage)
        if success:
            stats["files_copied"] += 1
            stats["total_bytes"] += size
        else:
            stats["files_failed"] += 1

    # 3. ReportSession storage paths
    commit_reports_subquery = CommitReport.objects.filter(commit__in=commits_qs).values(
        "id"
    )

    for session_id, storage_path in (
        ReportSession.objects.filter(
            report_id__in=commit_reports_subquery,
            storage_path__isnull=False,
        )
        .values_list("id", "storage_path")
        .iterator()
    ):
        if storage_path:
            file = ArchiveFile(
                source_path=storage_path,
                dest_path=get_archive_destination_path(
                    owner_id, export_id, storage_path
                ),
                source_model=f"reports.ReportSession:{session_id}",
            )
            stats["files_found"] += 1
            success, size = copy_archive_file(bucket, file, storage)
            if success:
                stats["files_copied"] += 1
                stats["total_bytes"] += size
            else:
                stats["files_failed"] += 1

    # 4. Bundle Analysis reports (from separate bucket)
    for bundle_report in (
        CommitReport.objects.filter(
            commit__in=commits_qs,
            report_type="bundle_analysis",
        )
        .select_related("commit")
        .iterator()
    ):
        bundle_path = BundleStoragePaths.bundle_report.path(
            repo_key=repo_hash,
            report_key=bundle_report.commit.commitid,
        )
        file = ArchiveFile(
            source_path=bundle_path,
            dest_path=get_archive_destination_path(owner_id, export_id, bundle_path),
            source_model=f"reports.CommitReport:{bundle_report.id}",
            source_bucket=bundle_bucket,
        )
        stats["files_found"] += 1
        success, size = copy_archive_file(bucket, file, storage)
        if success:
            stats["files_copied"] += 1
            stats["total_bytes"] += size
        else:
            stats["files_failed"] += 1

    log.info(
        "Repository %d archive collection: %d found, %d copied, %d failed, %d bytes",
        repository_id,
        stats["files_found"],
        stats["files_copied"],
        stats["files_failed"],
        stats["total_bytes"],
    )

    return stats
