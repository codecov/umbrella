"""
Archive Collector for owner data export.

Discovers and copies GCS archive files from the archive bucket to the export bucket.
Uses streaming copy to handle large files without loading them entirely into memory.
"""

import logging
import tempfile
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta

from django.utils import timezone

import shared.storage
from shared.api_archive.archive import ArchiveService
from shared.bundle_analysis.storage import StoragePaths as BundleStoragePaths
from shared.bundle_analysis.storage import get_bucket_name as get_bundle_analysis_bucket
from shared.storage.base import BaseStorageService
from shared.storage.exceptions import FileNotInStorageError

from .config import (
    EXPORT_DAYS_DEFAULT,
    get_archive_bucket,
    get_archive_destination_path,
    get_export_bucket,
)
from .models_registry import get_model_class
from .sql_generator import ExportContext

log = logging.getLogger(__name__)

STREAMING_THRESHOLD_BYTES = 10 * 1024 * 1024  # 10MB


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


def copy_archive_file_streaming(
    source_bucket: str,
    dest_bucket: str,
    file: ArchiveFile,
    storage: BaseStorageService,
) -> tuple[bool, int]:
    """
    Copy a single file from source to destination bucket using streaming.

    Uses a temp file to avoid loading the entire file into memory,
    which is critical for large files (chunks.txt, bundle_report.sqlite).

    Returns (success, bytes_copied).
    """
    try:
        with tempfile.SpooledTemporaryFile(
            max_size=STREAMING_THRESHOLD_BYTES,
            mode="w+b",
        ) as tmp:
            # Stream read from source to temp file
            storage.read_file(source_bucket, file.source_path, file_obj=tmp)

            # Get size and reset for reading
            bytes_copied = tmp.tell()
            tmp.seek(0)

            # Stream write from temp file to destination
            # Pass compression_type=None to avoid setting Content-Encoding header,
            # since the source file's compression format is unknown (could be gzip,
            # zstd, or uncompressed).
            storage.write_file(
                dest_bucket,
                file.dest_path,
                tmp,
                is_compressed=True,  # Don't re-compress, preserve original
                compression_type=None,  # Don't set Content-Encoding header
            )
            return True, bytes_copied
    except FileNotInStorageError:
        log.warning("File not found: %s (from %s)", file.source_path, file.source_model)
        return False, 0
    except Exception as e:
        log.error(
            "Failed to copy %s: %s",
            file.source_path,
            str(e),
            exc_info=True,
        )
        return False, 0


def collect_archives_for_export(
    owner_id: int,
    export_id: int,
    context: ExportContext | None = None,
) -> dict:
    """
    Discover and copy all archive files for an owner export.

    Uses streaming copy to handle large files efficiently.

    Args:
        owner_id: The owner ID
        export_id: The export ID
        context: ExportContext

    Returns:
        Dict with collection statistics
    """
    if context is None:
        context = ExportContext(owner_id=owner_id)

    source_bucket = get_archive_bucket()
    dest_bucket = get_export_bucket()
    storage = shared.storage.get_appropriate_storage_service()

    stats = {
        "files_found": 0,
        "files_copied": 0,
        "files_failed": 0,
        "total_bytes": 0,
        "discovery_stats": {},
    }

    file_gen = discover_archive_files(context, owner_id, export_id)

    # Manually iterate using next() to capture the generator's return value.
    # A for loop would catch and discard the StopIteration exception,
    # losing the stats dict returned by discover_archive_files().
    try:
        while True:
            file = next(file_gen)
            stats["files_found"] += 1

            # Use file-specific bucket if specified, otherwise default archive bucket
            file_source_bucket = file.source_bucket or source_bucket
            success, bytes_copied = copy_archive_file_streaming(
                file_source_bucket, dest_bucket, file, storage
            )

            if success:
                stats["files_copied"] += 1
                stats["total_bytes"] += bytes_copied
            else:
                stats["files_failed"] += 1
    except StopIteration as e:
        if e.value:
            stats["discovery_stats"] = e.value

    log.info(
        "Archive collection complete: %d found, %d copied, %d failed, %d bytes",
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
        export_id: The export ID
        repository_id: The specific repository to export
        since_date: Only export commits updated after this date (default: 60 days ago)

    Returns:
        Dict with collection statistics for this repository
    """
    if since_date is None:
        since_date = timezone.now() - timedelta(days=EXPORT_DAYS_DEFAULT)

    source_bucket = get_archive_bucket()
    dest_bucket = get_export_bucket()
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

    # Get the repository
    try:
        repo = Repository.objects.get(repoid=repository_id)
    except Repository.DoesNotExist:
        log.error("Repository %d not found", repository_id)
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
            success, size = copy_archive_file_streaming(
                source_bucket, dest_bucket, file, storage
            )
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
        success, size = copy_archive_file_streaming(
            source_bucket, dest_bucket, file, storage
        )
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
            success, size = copy_archive_file_streaming(
                source_bucket, dest_bucket, file, storage
            )
            if success:
                stats["files_copied"] += 1
                stats["total_bytes"] += size
            else:
                stats["files_failed"] += 1

    # 4. Bundle Analysis reports
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
        success, size = copy_archive_file_streaming(
            bundle_bucket, dest_bucket, file, storage
        )
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
