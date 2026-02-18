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

from django.db.models import Model, Q, QuerySet
from django.utils import timezone

from .config import BATCH_SIZE, EXPORT_DAYS_DEFAULT
from .models_registry import (
    DEFAULT_FIELDS,
    EXPORTABLE_MODELS,
    TIMESCALE_MODELS,
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
    "reports.RepositoryFlag",
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

UPLOAD_LEVEL_MODELS = {
    "reports.UploadError",
    "reports.UploadLevelTotals",
    "reports.UploadFlagMembership",
}

COMPOSITE_CONFLICT_COLUMNS: dict[str, list[str]] = {
    "core.Branch": ["branch", "repoid"],
    "core.Pull": ["repoid", "pullid"],
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
    Provides efficient querysets for owner data export.
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

    _commit_report_ids: list[int] | None = field(default=None, init=False, repr=False)

    _COMMIT_REPORT_IDS_CONSUMERS: frozenset[str] = frozenset(
        {
            "reports.ReportSession",
            "reports.UploadError",
            "reports.UploadLevelTotals",
            "reports.UploadFlagMembership",
        }
    )

    def model_completed(self, model_path: str, remaining_models: set[str]) -> None:
        """Release caches that are no longer needed by remaining models."""
        if self._commit_report_ids is not None and not (
            remaining_models & self._COMMIT_REPORT_IDS_CONSUMERS
        ):
            count = len(self._commit_report_ids)
            self._commit_report_ids = None
            log.info(
                "Released commit_report IDs cache",
                extra={"export_id": self.export_id, "freed_ids": count},
            )

    def _repository_subquery(self) -> QuerySet:
        Repository = get_model_class("core.Repository")
        return Repository.objects.filter(author_id=self.owner_id).values("repoid")

    def _commit_subquery(self) -> QuerySet:
        Commit = get_model_class("core.Commit")
        return Commit.objects.filter(
            repository_id__in=self._repository_subquery(),
            timestamp__gte=self.since_date,
        ).values("id")

    def _commit_report_subquery(self) -> QuerySet:
        CommitReport = get_model_class("reports.CommitReport")
        return CommitReport.objects.filter(
            commit_id__in=self._commit_subquery()
        ).values("id")

    def _get_commit_report_ids(self) -> list[int]:
        if self._commit_report_ids is None:
            self._commit_report_ids = list(
                self._commit_report_subquery().values_list("id", flat=True)
            )
            log.info(
                "Materialized commit_report IDs",
                extra={
                    "export_id": self.export_id,
                    "count": len(self._commit_report_ids),
                },
            )
        return self._commit_report_ids

    def iter_upload_model_rows(
        self,
        model_path: str,
        field_attnames: list[str],
        chunk_size: int = 5000,
    ) -> Generator[tuple]:
        """
        Yield values_list tuples for upload-level models.
        """
        model = get_model_class(model_path)
        commit_report_ids = self._get_commit_report_ids()

        for i in range(0, len(commit_report_ids), chunk_size):
            cr_chunk = commit_report_ids[i : i + chunk_size]

            rows = model.objects.filter(
                report_session__report_id__in=cr_chunk
            ).values_list(*field_attnames)
            yield from rows

    def get_queryset(self, model_path: str) -> QuerySet:
        """Get a queryset for the model, filtered appropriately."""
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
        """
        Get the base queryset with ownership filtering.
        """
        if model_path == "codecov_auth.Owner":
            return model.objects.filter(ownerid=self.owner_id)

        if model_path == "codecov_auth.User":
            return model.objects.filter(owners__ownerid=self.owner_id)

        if model_path == "codecov_auth.OwnerProfile":
            return model.objects.filter(owner_id=self.owner_id)

        if model_path == "core.Repository":
            return model.objects.filter(author_id=self.owner_id)

        if model_path in ("core.Branch", "core.Pull"):
            return model.objects.filter(repository_id__in=self._repository_subquery())

        if model_path == "reports.RepositoryFlag":
            return model.objects.filter(
                repository_id__in=self._repository_subquery()
            ).order_by("repository_id", "id")

        if model_path == "core.Commit":
            return model.objects.filter(id__in=self._commit_subquery())

        if model_path == "core.CommitError":
            return model.objects.filter(commit_id__in=self._commit_subquery())

        if model_path == "reports.CommitReport":
            return model.objects.filter(commit_id__in=self._commit_subquery()).order_by(
                "commit_id", "id"
            )

        if model_path == "reports.ReportLevelTotals":
            return model.objects.filter(report_id__in=self._commit_report_subquery())

        if model_path == "reports.ReportResults":
            return model.objects.filter(report_id__in=self._commit_report_subquery())

        if model_path == "reports.ReportSession":
            return model.objects.filter(
                report_id__in=self._get_commit_report_ids()
            ).order_by("report_id", "id")

        if model_path in UPLOAD_LEVEL_MODELS:
            # Handled via iter_upload_model_rows()
            raise ValueError(f"{model_path} uses chunked iteration, not get_queryset()")

        if model_path == "timeseries.Dataset":
            Repository = get_model_class("core.Repository")
            repo_ids = list(
                Repository.objects.filter(author_id=self.owner_id).values_list(
                    "repoid", flat=True
                )
            )
            return model.objects.filter(repository_id__in=repo_ids)

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

    if not value:
        return f"'{{}}'::{pg_type}[]"

    elements = []
    for item in value:
        if item is None:
            elements.append("NULL")
        elif isinstance(item, bool):
            elements.append("TRUE" if item else "FALSE")
        elif isinstance(item, int | float | Decimal):
            elements.append(str(item))
        elif isinstance(item, str):
            cleaned = item.replace("\x00", "")
            escaped = (
                cleaned.replace("\\", "\\\\").replace('"', '\\"').replace("'", "''")
            )
            elements.append(f'"{escaped}"')
        elif isinstance(item, UUID):
            elements.append(str(item))
        else:
            cleaned = str(item).replace("\x00", "")
            escaped = (
                cleaned.replace("\\", "\\\\").replace('"', '\\"').replace("'", "''")
            )
            elements.append(f'"{escaped}"')

    array_literal = "{" + ",".join(elements) + "}"
    return f"'{array_literal}'::{pg_type}[]"


# Sentinel for _FieldHandler.kind
_KIND_NULL = 0
_KIND_DEFAULT_STATIC = 1
_KIND_DEFAULT_CALLABLE = 2
_KIND_VALUE = 3


@dataclass(slots=True)
class _FieldHandler:
    """
    Pre-computed instruction for serializing one field of a row.
    """

    kind: int
    row_index: int
    field: Any
    default_value: Any = None


def _build_field_handlers(
    fields: list,
    nullified: set[str],
    defaults: dict[str, Any],
) -> list[_FieldHandler]:
    """
    Create a dispatch table for all fields in a model.
    """
    handlers: list[_FieldHandler] = []
    for i, f in enumerate(fields):
        if f.name in nullified or f.attname in nullified:
            handlers.append(_FieldHandler(kind=_KIND_NULL, row_index=i, field=f))
        elif f.name in defaults:
            raw = defaults[f.name]
            if callable(raw):
                handlers.append(
                    _FieldHandler(
                        kind=_KIND_DEFAULT_CALLABLE,
                        row_index=i,
                        field=f,
                        default_value=raw,
                    )
                )
            else:
                handlers.append(
                    _FieldHandler(
                        kind=_KIND_DEFAULT_STATIC,
                        row_index=i,
                        field=f,
                        default_value=raw,
                    )
                )
        elif f.attname in defaults:
            raw = defaults[f.attname]
            if callable(raw):
                handlers.append(
                    _FieldHandler(
                        kind=_KIND_DEFAULT_CALLABLE,
                        row_index=i,
                        field=f,
                        default_value=raw,
                    )
                )
            else:
                handlers.append(
                    _FieldHandler(
                        kind=_KIND_DEFAULT_STATIC,
                        row_index=i,
                        field=f,
                        default_value=raw,
                    )
                )
        else:
            handlers.append(_FieldHandler(kind=_KIND_VALUE, row_index=i, field=f))
    return handlers


def _serialize_row_fast(
    row: tuple,
    handlers: list[_FieldHandler],
) -> list[str]:
    """
    Serialize a values_list tuple using a pre-computed dispatch table.
    """
    values: list[str] = []
    for h in handlers:
        kind = h.kind
        if kind == _KIND_NULL:
            values.append("NULL")
        elif kind == _KIND_DEFAULT_STATIC:
            values.append(serialize_value(h.default_value, h.field))
        elif kind == _KIND_DEFAULT_CALLABLE:
            values.append(serialize_value(h.default_value(), h.field))
        else:
            values.append(serialize_value(row[h.row_index], h.field))
    return values


def get_conflict_columns(model_path: str, model) -> list[str]:
    """Get the columns to use for ON CONFLICT clause."""
    return COMPOSITE_CONFLICT_COLUMNS.get(model_path, [model._meta.pk.column])


def _keyset_paginate_values(
    queryset: QuerySet,
    field_attnames: list[str],
    batch_size: int,
) -> Generator[list[tuple]]:
    """
    Keyset pagination returning raw ``values_list`` tuples.
    """
    order_fields = list(queryset.query.order_by)
    vl_qs = queryset.values_list(*field_attnames)
    attname_to_idx = {name: i for i, name in enumerate(field_attnames)}
    order_idxs = [attname_to_idx[f] for f in order_fields]

    last_values: tuple | None = None
    while True:
        page_qs = vl_qs
        if last_values is not None:
            if len(order_fields) == 1:
                page_qs = page_qs.filter(**{f"{order_fields[0]}__gt": last_values[0]})
            else:
                f1, f2 = order_fields
                v1, v2 = last_values
                page_qs = page_qs.filter(
                    Q(**{f"{f1}__gt": v1}) | Q(**{f1: v1, f"{f2}__gt": v2})
                )

        batch = list(page_qs[:batch_size])
        if not batch:
            break

        yield batch

        last_row = batch[-1]
        last_values = tuple(last_row[i] for i in order_idxs)


def _row_iter_for_model(
    model_path: str,
    context: ExportContext,
    queryset: QuerySet,
    field_attnames: list[str],
) -> Generator[tuple]:
    """
    Return the appropriate row iterator for a model.
    """
    if model_path in UPLOAD_LEVEL_MODELS:
        yield from context.iter_upload_model_rows(model_path, field_attnames)
    else:
        for batch in _keyset_paginate_values(queryset, field_attnames, BATCH_SIZE):
            yield from batch


def generate_upsert_sql(
    model_path: str,
    context: ExportContext,
) -> Generator[str, None, dict]:
    """
    Generate batched UPSERT SQL statements for a model.
    Yields SQL statements in batches. Returns stats dict when complete.
    """
    model = get_model_class(model_path)

    fields = list(model._meta.fields)
    field_names = [f.column for f in fields]
    field_attnames = [f.attname for f in fields]
    table_name = model._meta.db_table
    conflict_columns = get_conflict_columns(model_path, model)

    columns_str = ", ".join(field_names)
    update_fields = [f.column for f in fields if f.column not in conflict_columns]
    set_clause = ", ".join(f"{col} = EXCLUDED.{col}" for col in update_fields)

    nullified = set(get_nullified_fields(model_path))
    defaults = DEFAULT_FIELDS.get(model_path, {})
    field_handlers = _build_field_handlers(fields, nullified, defaults)

    queryset = None
    if model_path not in UPLOAD_LEVEL_MODELS:
        queryset = context.get_queryset(model_path)
        if not queryset.query.order_by:
            pk_field = model._meta.pk.attname
            queryset = queryset.order_by(pk_field)

    row_count = 0
    batch: list[str] = []
    batch_count = 0
    log_interval = 10

    for row in _row_iter_for_model(model_path, context, queryset, field_attnames):
        values = _serialize_row_fast(row, field_handlers)
        batch.append(f"  ({', '.join(values)})")
        row_count += 1

        if len(batch) >= BATCH_SIZE:
            yield _build_upsert_statement(
                table_name, columns_str, batch, conflict_columns, set_clause
            )
            batch = []
            batch_count += 1

            if batch_count % log_interval == 0:
                log.info(
                    "Model export progress",
                    extra={
                        "export_id": context.export_id,
                        "model": model_path,
                        "rows_processed": row_count,
                        "batches_completed": batch_count,
                    },
                )

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

    for model_index, model_path in enumerate(models):
        model_start = timezone.now()
        log.info(
            "Exporting model",
            extra={
                "export_id": export_id,
                "model": model_path,
                "model_index": model_index,
                "total_models": len(models),
            },
        )

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

        elapsed_seconds = (timezone.now() - model_start).total_seconds()
        log.info(
            "Model export completed",
            extra={
                "export_id": export_id,
                "model": model_path,
                "model_index": model_index,
                "rows": model_rows,
                "elapsed_seconds": round(elapsed_seconds, 2),
            },
        )

        remaining = set(models[model_index + 1 :])
        context.model_completed(model_path, remaining)

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
