"""
SQL Generator for owner data export.

Generates PostgreSQL UPSERT statements (INSERT ... ON CONFLICT DO UPDATE)
for additive imports that don't conflict with existing data.
"""

import json
import logging
import math
from collections.abc import Generator
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, TextIO
from uuid import UUID

from django.db.models import Model, QuerySet
from django.utils import timezone

from .config import BATCH_SIZE, EXPORT_DAYS_DEFAULT
from .models_registry import (
    EXPORTABLE_MODELS,
    TIMESCALE_MODELS,
    get_default_fields,
    get_model_class,
    get_nullified_fields,
)

log = logging.getLogger(__name__)

# Models that export all data regardless of date
FULL_EXPORT_MODELS = {
    "codecov_auth.User",
    "codecov_auth.Owner",
    "codecov_auth.OwnerProfile",
    "core.Repository",
}

# Models already date-filtered via commit_ids
DATE_FILTERED_VIA_HIERARCHY = {
    "core.Commit",
    "core.CommitError",
    "reports.CommitReport",
    "reports.ReportResults",
    "reports.ReportLevelTotals",
    "reports.ReportSession",
    "reports.UploadError",
    "reports.UploadLevelTotals",
    "reports.UploadFlagMembership",
    "timeseries.Measurement",
}

COMPOSITE_CONFLICT_COLUMNS: dict[str, list[str]] = {
    "core.Branch": ["branch", "repoid"],
    "timeseries.Measurement": [
        "name",
        "owner_id",
        "repo_id",
        "measurable_id",
        "commit_sha",
        "timestamp",
    ],
}


def get_since_date() -> datetime:
    """Get the cutoff date for time-filtered exports."""
    return timezone.now() - timedelta(days=EXPORT_DAYS_DEFAULT)


@dataclass
class ExportContext:
    """
    Pre-fetches IDs at each level of the ownership hierarchy to avoid expensive JOINs.

    Hierarchy:
        Owner
        ├── User, OwnerProfile
        └── Repository
            ├── Branch, Pull, RepositoryFlag
            └── Commit
                ├── CommitError, CommitReport
                └── CommitReport
                    ├── ReportResults, ReportLevelTotals, ReportSession
                    └── ReportSession
                        └── UploadError, UploadLevelTotals, UploadFlagMembership
    """

    owner_id: int
    since_date: datetime = field(default_factory=get_since_date)
    export_id: int | None = None

    _repository_ids: list[int] | None = field(default=None, repr=False)
    _commit_ids: list[int] | None = field(default=None, repr=False)
    _commit_report_ids: list[int] | None = field(default=None, repr=False)
    _report_session_ids: list[int] | None = field(default=None, repr=False)

    @property
    def repository_ids(self) -> list[int]:
        if self._repository_ids is None:
            Repository = get_model_class("core.Repository")
            self._repository_ids = list(
                Repository.objects.filter(author_id=self.owner_id).values_list(
                    "repoid", flat=True
                )
            )
        return self._repository_ids

    @property
    def commit_ids(self) -> list[int]:
        if self._commit_ids is None:
            Commit = get_model_class("core.Commit")
            self._commit_ids = list(
                Commit.objects.filter(
                    repository_id__in=self.repository_ids,
                    updatestamp__gte=self.since_date,
                ).values_list("id", flat=True)
            )
        return self._commit_ids

    @property
    def commit_report_ids(self) -> list[int]:
        if self._commit_report_ids is None:
            CommitReport = get_model_class("reports.CommitReport")
            self._commit_report_ids = list(
                CommitReport.objects.filter(commit_id__in=self.commit_ids).values_list(
                    "id", flat=True
                )
            )
        return self._commit_report_ids

    @property
    def report_session_ids(self) -> list[int]:
        if self._report_session_ids is None:
            ReportSession = get_model_class("reports.ReportSession")
            self._report_session_ids = list(
                ReportSession.objects.filter(
                    report_id__in=self.commit_report_ids
                ).values_list("id", flat=True)
            )
        return self._report_session_ids

    def get_queryset(self, model_path: str) -> QuerySet:
        """Get a queryset for the model, filtered by pre-fetched IDs."""
        model = get_model_class(model_path)
        qs = self._get_base_queryset(model_path, model)

        if model_path not in FULL_EXPORT_MODELS:
            qs = self._apply_date_filter(model_path, model, qs)

        return qs

    def _apply_date_filter(
        self, model_path: str, model: type[Model], qs: QuerySet
    ) -> QuerySet:
        """Apply date filtering based on available timestamp field."""
        if model_path in DATE_FILTERED_VIA_HIERARCHY:
            return qs

        if hasattr(model, "created_at"):
            return qs.filter(created_at__gte=self.since_date)
        if hasattr(model, "updatestamp"):
            return qs.filter(updatestamp__gte=self.since_date)
        if hasattr(model, "timestamp"):
            return qs.filter(timestamp__gte=self.since_date)

        return qs

    def _get_base_queryset(self, model_path: str, model: type[Model]) -> QuerySet:
        """Get the base queryset with ownership filtering."""
        if model_path == "codecov_auth.Owner":
            return model.objects.filter(ownerid=self.owner_id)

        if model_path == "codecov_auth.User":
            return model.objects.filter(owners__ownerid=self.owner_id)

        if model_path == "codecov_auth.OwnerProfile":
            return model.objects.filter(owner_id=self.owner_id)

        if model_path == "core.Repository":
            return model.objects.filter(author_id=self.owner_id)

        if model_path in ("core.Branch", "core.Pull", "reports.RepositoryFlag"):
            return model.objects.filter(repository_id__in=self.repository_ids)

        if model_path == "core.Commit":
            return model.objects.filter(id__in=self.commit_ids)

        if model_path in ("core.CommitError", "reports.CommitReport"):
            return model.objects.filter(commit_id__in=self.commit_ids)

        if model_path in (
            "reports.ReportResults",
            "reports.ReportLevelTotals",
            "reports.ReportSession",
        ):
            return model.objects.filter(report_id__in=self.commit_report_ids)

        if model_path in (
            "reports.UploadError",
            "reports.UploadLevelTotals",
            "reports.UploadFlagMembership",
        ):
            return model.objects.filter(report_session_id__in=self.report_session_ids)

        if model_path == "timeseries.Dataset":
            return model.objects.filter(repository_id__in=self.repository_ids)

        if model_path == "timeseries.Measurement":
            return model.objects.filter(
                owner_id=self.owner_id,
                timestamp__gte=self.since_date,
            )

        raise ValueError(f"Unknown model: {model_path}")


def serialize_value(value: Any, field: Any = None) -> str:
    """Convert a Python value to a SQL literal string."""
    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, int):
        return str(value)

    if isinstance(value, float):
        if math.isnan(value):
            return "'NaN'::float8"
        if math.isinf(value):
            return "'Infinity'::float8" if value > 0 else "'-Infinity'::float8"
        return str(value)

    if isinstance(value, Decimal):
        if value.is_nan():
            return "'NaN'::float8"
        if value.is_infinite():
            return "'Infinity'::float8" if value > 0 else "'-Infinity'::float8"
        return str(value)

    if isinstance(value, str):
        # Strip NULL bytes which are invalid in PostgreSQL text columns
        cleaned = value.replace("\x00", "")
        return f"'{cleaned.replace(chr(39), chr(39) + chr(39))}'"

    if isinstance(value, datetime | date | time):
        return f"'{value.isoformat()}'"

    if isinstance(value, timedelta):
        return f"'{value.total_seconds()} seconds'::interval"

    if isinstance(value, UUID):
        return f"'{value}'"

    if isinstance(value, list):
        if field is not None and field.get_internal_type() == "ArrayField":
            return _serialize_pg_array(value, field)
        cleaned = json.dumps(value).replace("\x00", "")
        return f"'{cleaned.replace(chr(39), chr(39) + chr(39))}'::jsonb"

    if isinstance(value, dict):
        # Strip NULL bytes which are invalid in PostgreSQL text columns
        cleaned = json.dumps(value).replace("\x00", "")
        return f"'{cleaned.replace(chr(39), chr(39) + chr(39))}'::jsonb"

    if isinstance(value, bytes):
        return f"'\\x{value.hex()}'::bytea"

    if hasattr(value, "value"):  # Enum
        return serialize_value(value.value, field)

    # Strip NULL bytes which are invalid in PostgreSQL text columns
    cleaned = str(value).replace("\x00", "")
    return f"'{cleaned.replace(chr(39), chr(39) + chr(39))}'"


def _serialize_pg_array(value: list, field: Any) -> str:
    """Serialize a Python list to PostgreSQL array literal syntax."""
    if not value:
        return "'{}'"

    base_field = field.base_field
    base_type = base_field.get_internal_type()
    pg_type_map = {
        "IntegerField": "integer",
        "BigIntegerField": "bigint",
        "SmallIntegerField": "smallint",
        "CharField": "text",
        "TextField": "text",
        "BooleanField": "boolean",
        "FloatField": "float8",
        "DecimalField": "numeric",
        "UUIDField": "uuid",
    }

    pg_type = pg_type_map.get(base_type, "text")

    elements = []
    for item in value:
        if item is None:
            elements.append("NULL")
        elif isinstance(item, bool):
            elements.append("TRUE" if item else "FALSE")
        elif isinstance(item, int | float | Decimal):
            elements.append(str(item))
        elif isinstance(item, str):
            escaped = item.replace("\\", "\\\\").replace('"', '\\"')
            elements.append(f'"{escaped}"')
        elif isinstance(item, UUID):
            elements.append(str(item))
        else:
            elements.append(str(item))

    array_literal = "{" + ",".join(elements) + "}"
    return f"'{array_literal}'::{pg_type}[]"


def get_row_values(instance: Model, model_path: str, fields: list) -> list[str]:
    """Extract field values from a model instance, applying nullification and defaults."""
    nullified = get_nullified_fields(model_path)
    defaults = get_default_fields(model_path)

    values = []
    for f in fields:
        if f.name in nullified or f.attname in nullified:
            value = None
        elif f.name in defaults:
            default_value = defaults[f.name]
            value = default_value() if callable(default_value) else default_value
        elif f.attname in defaults:
            default_value = defaults[f.attname]
            value = default_value() if callable(default_value) else default_value
        else:
            value = getattr(instance, f.attname)
        values.append(serialize_value(value, f))

    return values


def get_conflict_columns(model_path: str, model) -> list[str]:
    """Get the columns to use for ON CONFLICT clause."""
    return COMPOSITE_CONFLICT_COLUMNS.get(model_path, [model._meta.pk.column])


def generate_upsert_sql(
    model_path: str,
    context: ExportContext,
) -> Generator[str, None, dict]:
    """
    Generate batched UPSERT SQL statements for a model.
    Yields SQL statements in batches. Returns stats dict when complete.
    """
    model = get_model_class(model_path)
    queryset = context.get_queryset(model_path)

    fields = list(model._meta.fields)
    field_names = [f.column for f in fields]
    table_name = model._meta.db_table
    conflict_columns = get_conflict_columns(model_path, model)

    columns_str = ", ".join(field_names)
    update_fields = [f.column for f in fields if f.column not in conflict_columns]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_fields)

    row_count = 0
    batch = []

    for instance in queryset.iterator():
        values = get_row_values(instance, model_path, fields)
        batch.append(f"  ({', '.join(values)})")
        row_count += 1

        if len(batch) >= BATCH_SIZE:
            yield _build_upsert_statement(
                table_name, columns_str, batch, conflict_columns, set_clause
            )
            batch = []

    if batch:
        yield _build_upsert_statement(
            table_name, columns_str, batch, conflict_columns, set_clause
        )

    return {"model": model_path, "rows": row_count}


def _build_upsert_statement(
    table_name: str,
    columns_str: str,
    value_rows: list[str],
    conflict_columns: list[str],
    set_clause: str,
) -> str:
    """Build a single UPSERT statement from batched rows."""
    conflict_clause = ", ".join(conflict_columns)
    if set_clause:
        on_conflict = f"ON CONFLICT ({conflict_clause}) DO UPDATE SET\n  {set_clause}"
    else:
        # All fields are conflict columns, nothing to update
        on_conflict = f"ON CONFLICT ({conflict_clause}) DO NOTHING"

    return f"""INSERT INTO {table_name} ({columns_str})
VALUES
{",".join(value_rows)}
{on_conflict};

"""


def generate_sequence_resets(models: list[str]) -> str:
    """
    Generate SQL to reset auto-increment sequences after import.
    Required because explicit ID inserts don't update sequences.
    """
    statements = ["-- Reset sequences for proper auto-increment\n"]

    for model_path in models:
        model = get_model_class(model_path)
        pk_field = model._meta.pk

        if pk_field.get_internal_type() not in ("AutoField", "BigAutoField"):
            continue

        table = model._meta.db_table
        col = pk_field.column
        statements.append(
            f"SELECT setval('{table}_{col}_seq', "
            f"COALESCE((SELECT MAX({col}) FROM {table}), 0) + 1, false);\n"
        )

    return "".join(statements)


def generate_full_export(
    owner_id: int,
    output_file: TextIO,
    models: list[str] | None = None,
    since_date: datetime | None = None,
    export_id: int | None = None,
) -> dict:
    """Generate complete SQL export for an owner."""
    if models is None:
        models = EXPORTABLE_MODELS

    context = (
        ExportContext(owner_id=owner_id, since_date=since_date, export_id=export_id)
        if since_date
        else ExportContext(owner_id=owner_id, export_id=export_id)
    )
    stats = {
        "owner_id": owner_id,
        "since_date": context.since_date.isoformat(),
        "models": {},
        "total_rows": 0,
    }

    # Header
    output_file.write(
        f"-- Owner Data Export\n"
        f"-- Owner ID: {owner_id}\n"
        f"-- Since: {context.since_date.isoformat()}\n"
        f"-- Generated: {timezone.now().isoformat()}\n"
        f"-- Run with: psql -d your_database -f this_file.sql\n\n"
        f"BEGIN;\n\n"
    )

    for model_path in models:
        output_file.write(f"-- {model_path}\n")
        gen = generate_upsert_sql(model_path, context)

        model_rows = 0
        try:
            while True:
                sql = next(gen)
                output_file.write(sql)
        except StopIteration as e:
            if e.value:
                model_rows = e.value.get("rows", 0)

        stats["models"][model_path] = model_rows
        stats["total_rows"] += model_rows

        if model_rows == 0:
            output_file.write("-- (no rows)\n\n")

    output_file.write("\n")
    output_file.write(generate_sequence_resets(models))
    output_file.write("\nCOMMIT;\n")

    log.info(
        "SQL generation completed",
        extra={"export_id": export_id, "total_rows": stats["total_rows"]},
    )

    return stats


def generate_timescale_export(
    owner_id: int,
    output_file: TextIO,
    since_date: datetime | None = None,
    export_id: int | None = None,
) -> dict:
    """Generate SQL export for TimescaleDB models."""
    return generate_full_export(
        owner_id,
        output_file,
        TIMESCALE_MODELS,
        since_date=since_date,
        export_id=export_id,
    )
