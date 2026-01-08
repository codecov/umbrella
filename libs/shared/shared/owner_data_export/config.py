"""
Configuration constants and helpers for owner data export.
"""

from shared.config import get_config

EXPORT_DAYS_DEFAULT = 60
BATCH_SIZE = 1000


def get_export_bucket() -> str:
    """
    Get the GCS bucket name for storing exports.
    Configurable via: services.owner_export.bucket
    """
    return get_config("services", "owner_export", "bucket", default="codecov-exports")


def get_archive_bucket() -> str:
    """
    Get the GCS bucket name where archive files are stored.
    This is the same bucket used by ArchiveService.
    """
    return get_config("services", "minio", "bucket", default="archive")


def get_export_path(owner_id: int, export_id: int) -> str:
    """
    Get the base GCS path for an export.
    """
    return f"exports/{owner_id}/{export_id}/"


def get_postgres_sql_path(owner_id: int, export_id: int) -> str:
    """
    Get the GCS path for the postgres SQL export file.
    """
    return f"{get_export_path(owner_id, export_id)}postgres.sql"


def get_timescale_sql_path(owner_id: int, export_id: int) -> str:
    """
    Get the GCS path for the timescale SQL export file.
    """
    return f"{get_export_path(owner_id, export_id)}timescale.sql"


def get_manifest_path(owner_id: int, export_id: int) -> str:
    """
    Get the GCS path for the export manifest file.
    """
    return f"{get_export_path(owner_id, export_id)}manifest.json"


def get_archives_path(owner_id: int, export_id: int) -> str:
    """
    Get the GCS path prefix for archived files.
    """
    return f"{get_export_path(owner_id, export_id)}archives/"


def get_archive_destination_path(
    owner_id: int, export_id: int, source_path: str
) -> str:
    """
    Get the destination path for an archive file in the export.

    The source path (from the archive bucket) is preserved under the archives/ prefix.
    Example:
        source: v4/repos/ABC123/commits/sha/chunks.txt
        dest:   exports/123/456/archives/v4/repos/ABC123/commits/sha/chunks.txt
    """
    return f"{get_archives_path(owner_id, export_id)}{source_path}"
