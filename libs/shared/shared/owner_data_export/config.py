"""
Configuration constants and helpers for owner data export.

All exports are stored in the same bucket as archive files (the minio/archive bucket),
under the `exports/<owner_id>/<export_id>/` prefix. Each export gets its own unique
directory to prevent concurrent exports from overwriting each other's files.
"""

from shared.config import get_config

# Export Parameters
EXPORT_DAYS_DEFAULT = 60
BATCH_SIZE = 5000

# Default 7 days
DOWNLOAD_URL_EXPIRY_SECONDS = int(
    get_config(
        "services",
        "owner_export",
        "download_url_expiry_seconds",
        default=7 * 24 * 60 * 60,
    )
)

SQL_TASK_SOFT_TIME_LIMIT = int(
    get_config(
        "services",
        "owner_export",
        "sql_task_soft_time_limit",
        default=3600,  # 1 hour
    )
)
SQL_TASK_TIME_LIMIT = int(
    get_config(
        "services",
        "owner_export",
        "sql_task_time_limit",
        default=3660,
    )
)
ARCHIVE_TASK_SOFT_TIME_LIMIT = int(
    get_config(
        "services",
        "owner_export",
        "archive_task_soft_time_limit",
        default=7200,  # 2 hours
    )
)
ARCHIVE_TASK_TIME_LIMIT = int(
    get_config(
        "services",
        "owner_export",
        "archive_task_time_limit",
        default=7260,
    )
)

FINALIZE_TASK_SOFT_TIME_LIMIT = int(
    get_config(
        "services",
        "owner_export",
        "finalize_task_soft_time_limit",
        default=3600,  # 1 hour
    )
)
FINALIZE_TASK_TIME_LIMIT = int(
    get_config(
        "services",
        "owner_export",
        "finalize_task_time_limit",
        default=3660,
    )
)

# Number of concurrent workers for archive file copying
ARCHIVE_COPY_WORKERS = int(
    get_config("services", "owner_export", "archive_copy_workers", default=4)
)


def get_archive_bucket() -> str:
    """
    Get the GCS bucket name where archive files are stored.
    This is the same bucket used by ArchiveService.
    """
    return get_config("services", "minio", "bucket", default="archive")


def get_export_path(owner_id: int, export_id: int) -> str:
    """
    Get the base GCS path for an owner's export.

    Each export gets its own unique directory to prevent concurrent exports
    from overwriting each other's files.
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

    The source path is preserved under the archives/ prefix.
    Example:
        source: v4/repos/ABC123/commits/sha/chunks.txt
        dest:   exports/12345/67890/archives/v4/repos/ABC123/commits/sha/chunks.txt
    """
    return f"{get_archives_path(owner_id, export_id)}{source_path}"


def get_tarball_path(owner_id: int, export_id: int) -> str:
    """
    Get the GCS path for the export tarball.
    """
    return f"{get_export_path(owner_id, export_id)}export.tar.gz"
