"""
DataBridge — Multi-tenant data ingestion core.

feat: add export() method and improve ingest performance
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import uuid
import csv
import io
import json


# The canonical schema defines the shared fields we normalise across all tenants.
# Source keys that map to one of these end up in canonical_fields;
# everything else is preserved as-is in extra_fields.
CANONICAL_FIELDS = {
    "name", "email", "phone", "city",
    "zip_code", "country", "company_name", "company_id",
}


@dataclass
class Record:
    """A normalised record, regardless of its source tenant.

    Attributes:
        id:               UUID assigned at ingestion time.
        tenant_id:        Source company identifier.
        entity_type:      Logical type, e.g. ``"customer"``, ``"product"``.
        canonical_fields: Normalised fields shared across all tenants.
        extra_fields:     Tenant-specific fields with no canonical mapping.
        raw:              Original row as received, kept for audit purposes.
    """
    id: str
    tenant_id: str
    entity_type: str
    canonical_fields: dict[str, Any]
    extra_fields: dict[str, Any]
    raw: dict[str, Any]


class DataStore:
    """In-memory store for multi-tenant records.

    Records from all tenants are held in a single list; callers filter
    by tenant_id or entity_type at query time.
    """

    def __init__(self) -> None:
        self._records: list[Record] = []

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def ingest(
        self,
        tenant_id: str,
        entity_type: str,
        row: dict[str, Any],
        mapping: dict[str, str | None],
    ) -> Record:
        """Ingest a raw row and normalise it using *mapping*.

        Args:
            tenant_id:   Identifier for the source company.
            entity_type: e.g. ``"customer"``, ``"product"``, ``"address"``.
            row:         Raw data as received from the source system.
            mapping:     Maps source keys → canonical keys (``None`` = discard field).

        Returns:
            The created and stored :class:`Record`.
        """
        canonical: dict[str, Any] = {}
        extra: dict[str, Any] = {}

        # Walk the mapping to decide where each field lands.
        for source_key, canonical_key in mapping.items():
            value = row.get(source_key)
            if canonical_key and canonical_key in CANONICAL_FIELDS:
                canonical[canonical_key] = value
            else:
                extra[source_key] = value

        record = Record(
            id=str(uuid.uuid4()),
            tenant_id=tenant_id,
            entity_type=entity_type,
            canonical_fields=canonical,
            extra_fields=extra,
            raw=row,
        )
        self._records.append(record)
        return record

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def query(
        self,
        entity_type: str,
        filters: dict[str, Any] | None = None,
    ) -> list[Record]:
        """Return records matching *entity_type*, optionally filtered.

        Filters are matched against canonical_fields only.
        """
        results = [r for r in self._records if r.entity_type == entity_type]
        if filters:
            for key, val in filters.items():
                results = [
                    r for r in results
                    if r.canonical_fields.get(key) == val
                ]
        return results

    def all(self) -> list[Record]:
        """Return a snapshot of all records across all tenants."""
        return list(self._records)

    # ------------------------------------------------------------------
    # Export  (new in this PR)
    # ------------------------------------------------------------------

    def export(
        self,
        entity_type: str,
        fields: list[str] | None = None,
        include_extra: bool = False,
        fmt: str = "dict",
    ) -> Any:
        """Export records as a list of dicts, a JSON string, or a CSV string.

        Args:
            entity_type:   Filter by entity type.
            fields:        Canonical fields to include (``None`` = all).
            include_extra: Also include tenant-specific extra_fields.
            fmt:           ``"dict"`` | ``"json"`` | ``"csv"``.
        """
        rows = [
            self._build_row(record, fields, include_extra)
            for record in self.query(entity_type)
        ]

        if fmt == "json":
            return json.dumps(rows, ensure_ascii=False, indent=2)

        if fmt == "csv":
            if not rows:
                return ""
            buf = io.StringIO()
            writer = csv.DictWriter(
                buf,
                fieldnames=rows[0].keys(),
                extrasaction="ignore",
            )
            writer.writeheader()
            writer.writerows(rows)
            return buf.getvalue()

        return rows

    def _build_row(
        self,
        record: Record,
        fields: list[str] | None,
        include_extra: bool,
    ) -> dict[str, Any]:
        # Build the output row from canonical fields, then layer in extras if requested.
        row = record.canonical_fields
        if include_extra:
            row.update(record.extra_fields)
        if fields:
            row = {k: row.get(k) for k in fields}
        return row
