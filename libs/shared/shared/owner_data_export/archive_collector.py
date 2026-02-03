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

from django.db.models import QuerySet
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
    BATCH_SIZE,
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


def _paginate_queryset_values(queryset: "QuerySet", batch_size: int):
    """
    Paginate a values queryset using primary key ordering.
    """
    pk_field = queryset.model._meta.pk.attname
    ordered_qs = queryset.order_by(pk_field)

    last_pk = None
    while True:
        if last_pk is not None:
            page_qs = ordered_qs.filter(**{f"{pk_field}__gt": last_pk})
        else:
            page_qs = ordered_qs

        batch = list(page_qs[:batch_size])

        if not batch:
            break

        yield from batch

        if hasattr(batch[-1], pk_field):
            last_pk = getattr(batch[-1], pk_field)
        elif isinstance(batch[-1], tuple):
            last_pk = batch[-1][0]
        elif isinstance(batch[-1], dict):
            last_pk = (
                batch[-1].get(pk_field) or batch[-1].get("id") or batch[-1].get("pk")
            )
        else:
            last_pk = batch[-1]


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

    Repository = get_model_class("core.Repository")
    Commit = get_model_class("core.Commit")
    ReportSession = get_model_class("reports.ReportSession")
    CommitReport = get_model_class("reports.CommitReport")

    repo_subquery = Repository.objects.filter(author_id=owner_id).values("repoid")
    commit_subquery = Commit.objects.filter(
        repository_id__in=repo_subquery,
        updatestamp__gte=context.since_date,
    ).values("id")

    repos = {r.repoid: r for r in Repository.objects.filter(author_id=owner_id)}

    commits_qs = Commit.objects.filter(
        id__in=commit_subquery,
        _report_storage_path__isnull=False,
    ).values_list("id", "_report_storage_path", "repository_id")

    # 1. Commit report storage paths
    for commit_id, storage_path, repo_id in _paginate_queryset_values(
        commits_qs, BATCH_SIZE
    ):
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
    commit_report_subquery = CommitReport.objects.filter(
        commit_id__in=commit_subquery
    ).values("id")
    uploads_qs = ReportSession.objects.filter(
        report_id__in=commit_report_subquery,
        storage_path__isnull=False,
    ).values_list("id", "storage_path")

    for session_id, storage_path in _paginate_queryset_values(uploads_qs, BATCH_SIZE):
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
    commits_for_chunks = Commit.objects.filter(
        id__in=commit_subquery,
    ).values_list("id", "commitid", "repository_id")

    for commit_pk, commitid, repo_id in _paginate_queryset_values(
        commits_for_chunks, BATCH_SIZE
    ):
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
    bundle_reports_qs = (
        CommitReport.objects.filter(
            id__in=commit_report_subquery,
            report_type="bundle_analysis",
        )
        .select_related("commit__repository")
        .order_by("id")
    )

    bundle_bucket = get_bundle_analysis_bucket()

    # Use keyset pagination for bundle reports
    last_pk = None
    while True:
        page_qs = bundle_reports_qs
        if last_pk is not None:
            page_qs = page_qs.filter(id__gt=last_pk)

        batch = list(page_qs[:BATCH_SIZE])
        if not batch:
            break

        for bundle_report in batch:
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

        last_pk = batch[-1].id

    return stats


def copy_archive_file(
    bucket: str,
    file: ArchiveFile,
    storage: BaseStorageService,
    export_id: int | None = None,
    owner_id: int | None = None,
) -> tuple[bool, int]:
    """
    Copy a file to the export location using server-side copy.

    Args:
        bucket: The destination bucket
        file: The ArchiveFile to copy
        storage: The storage service to use
        export_id: The export ID for logging
        owner_id: The owner ID for logging

    Returns (success, bytes_copied).
    """
    source_bucket = file.source_bucket or bucket

    try:
        storage.minio_client.copy_object(
            bucket,
            file.dest_path,
            CopySource(source_bucket, file.source_path),
        )
    except FileNotInStorageError:
        # File not found is expected for some files, don't log as it's noisy
        return False, 0
    except S3Error as e:
        if e.code == "NoSuchKey":
            return False, 0
        log.error(
            "Failed to copy archive file",
            extra={
                "export_id": export_id,
                "source_path": file.source_path,
                "error": str(e),
            },
        )
        return False, 0
    except Exception as e:
        log.error(
            "Failed to copy archive file",
            extra={
                "export_id": export_id,
                "source_path": file.source_path,
                "error": str(e),
            },
        )
        return False, 0

    try:
        stat = storage.minio_client.stat_object(bucket, file.dest_path)
        return True, stat.size
    except Exception:
        # Stat failure after successful copy is rare, return success anyway
        return True, 0


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
        success, bytes_copied = copy_archive_file(
            bucket, file, storage, export_id, owner_id
        )
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
                log.error(
                    "Unexpected error copying archive file",
                    extra={
                        "export_id": export_id,
                        "source_path": file.source_path,
                        "error": str(e),
                    },
                )
                with stats_lock:
                    stats["files_failed"] += 1

    log.info(
        "Archive collection finished",
        extra={
            "export_id": export_id,
            "files_copied": stats["files_copied"],
            "files_failed": stats["files_failed"],
        },
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
            "Repository not found or does not belong to owner",
            extra={"export_id": export_id, "repository_id": repository_id},
        )
        return stats

    repo_hash = ArchiveService.get_archive_hash(repo)

    commits_qs = Commit.objects.filter(
        repository_id=repository_id,
        updatestamp__gte=since_date,
    )

    if not commits_qs.exists():
        return stats

    # 1. Commit report storage paths
    for commit_id, storage_path in (
        commits_qs.filter(_report_storage_path__isnull=False)
        .values_list("id", "_report_storage_path")
        .iterator(chunk_size=BATCH_SIZE)
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
            success, size = copy_archive_file(
                bucket, file, storage, export_id, owner_id
            )
            if success:
                stats["files_copied"] += 1
                stats["total_bytes"] += size
            else:
                stats["files_failed"] += 1

    # 2. Chunks files
    for commitid in commits_qs.values_list("commitid", flat=True).iterator(
        chunk_size=BATCH_SIZE
    ):
        chunks_path = f"v4/repos/{repo_hash}/commits/{commitid}/chunks.txt"
        file = ArchiveFile(
            source_path=chunks_path,
            dest_path=get_archive_destination_path(owner_id, export_id, chunks_path),
            source_model=f"chunks:{commitid}",
        )
        stats["files_found"] += 1
        success, size = copy_archive_file(bucket, file, storage, export_id, owner_id)
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
        .iterator(chunk_size=BATCH_SIZE)
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
            success, size = copy_archive_file(
                bucket, file, storage, export_id, owner_id
            )
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
        .iterator(chunk_size=BATCH_SIZE)
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
        success, size = copy_archive_file(bucket, file, storage, export_id, owner_id)
        if success:
            stats["files_copied"] += 1
            stats["total_bytes"] += size
        else:
            stats["files_failed"] += 1

    log.info(
        "Repository archive collection completed",
        extra={
            "export_id": export_id,
            "repository_id": repository_id,
            "files_copied": stats["files_copied"],
            "files_failed": stats["files_failed"],
        },
    )

    return stats
