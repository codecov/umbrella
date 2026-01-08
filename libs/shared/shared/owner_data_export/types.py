from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class ExportConfig:
    """Configuration for an owner data export."""

    owner_id: int
    export_id: int
    since_date: datetime
    # Temp directory for intermediate files (set by orchestrator)
    temp_dir: str | None = None


@dataclass
class ExportResult:
    """
    Result from a single export task.
    Stats is a metadata object that contains information about the export.
    """

    success: bool
    error: str | None = None
    # TODO: this might not be needed, tbd if we want to keep track of it
    files: list[str] = field(default_factory=list)
    stats: dict = field(default_factory=dict)


@dataclass
class FinalExportResult:
    """Final result after all tasks complete."""

    success: bool
    download_url: str | None = None
    download_expires_at: datetime | None = None
    error: str | None = None
    # Aggregated stats from all tasks
    stats: dict = field(default_factory=dict)
