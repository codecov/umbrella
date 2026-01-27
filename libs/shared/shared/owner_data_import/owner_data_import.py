"""
Import archive files from an extracted export tarball to MinIO storage.
"""

import argparse
import logging
import sys
from pathlib import Path

from minio.error import S3Error

from shared.storage.minio import MinioStorageService

log = logging.getLogger(__name__)

DEFAULT_BUCKET = "archive"


def import_archives(
    input_dir: Path,
    storage: MinioStorageService,
    bucket: str,
) -> tuple[int, int]:
    """
    Import archive files from extracted export to MinIO storage.

    Returns (files_uploaded, files_failed).
    """
    archives_dir = input_dir / "archives"
    if not archives_dir.exists():
        raise ValueError(f"Missing archives/ directory in {input_dir}")

    uploaded = 0
    failed = 0

    for file_path in archives_dir.rglob("*"):
        if file_path.is_file():
            relative_path = file_path.relative_to(archives_dir).as_posix()
            try:
                with open(file_path, "rb") as f:
                    storage.write_file(bucket, relative_path, f, is_compressed=True)
                uploaded += 1
                log.info("Uploaded: %s", relative_path)
            except Exception as e:
                failed += 1
                log.error("Failed to upload %s: %s", relative_path, e)

    return uploaded, failed


def create_storage(
    host: str,
    access_key: str,
    secret_key: str,
    secure: bool = False,
) -> MinioStorageService:
    """Create MinIO storage service."""
    host_part, port_part = (host.rsplit(":", 1) + [None])[:2]
    return MinioStorageService(
        {
            "host": host_part,
            "port": port_part,
            "access_key_id": access_key,
            "secret_access_key": secret_key,
            "verify_ssl": secure,
            "iam_auth": False,
        }
    )


def run_import(
    input_dir: Path,
    minio_host: str,
    minio_access_key: str,
    minio_secret_key: str,
    bucket: str = DEFAULT_BUCKET,
    secure: bool = False,
) -> tuple[int, int]:
    """
    Run the import process.

    Returns (files_uploaded, files_failed).
    """
    log.info("Starting import from %s", input_dir)

    storage = create_storage(minio_host, minio_access_key, minio_secret_key, secure)

    if not storage.minio_client.bucket_exists(bucket):
        log.info("Creating bucket: %s", bucket)
        try:
            storage.minio_client.make_bucket(bucket)
        except S3Error as e:
            if e.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                raise

    uploaded, failed = import_archives(input_dir, storage, bucket)

    log.info("Import complete: %d uploaded, %d failed", uploaded, failed)
    return uploaded, failed


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Import owner data export to MinIO storage.",
    )
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--minio-host", default="localhost:9000")
    parser.add_argument("--minio-access-key", default="minioadmin")
    parser.add_argument("--minio-secret-key", default="minioadmin")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--secure", action="store_true")

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    try:
        _, failed = run_import(
            input_dir=args.input_dir,
            minio_host=args.minio_host,
            minio_access_key=args.minio_access_key,
            minio_secret_key=args.minio_secret_key,
            bucket=args.bucket,
            secure=args.secure,
        )
        sys.exit(1 if failed > 0 else 0)
    except Exception as e:
        log.error("Import failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
