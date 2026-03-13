"""
DataBridge — Multi-tenant data ingestion core.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import uuid


CANONICAL_FIELDS = {
    "name", "email", "phone", "city",
    "zip_code", "country", "company_name", "company_id",
}


@dataclass
class Record:
    """A normalised record, regardless of its source tenant."""
    id: str
    tenant_id: str
    entity_type: str
    canonical_fields: dict[str, Any]
    extra_fields: dict[str, Any]
    raw: dict[str, Any]


class DataStore:
    """In-memory store for multi-tenant records."""

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
            entity_type: e.g. "customer", "product", "address".
            row:         Raw data as received.
            mapping:     Maps source keys to canonical keys (or None to skip).

        Returns:
            The created :class:`Record`.
        """
        canonical: dict[str, Any] = {}
        extra: dict[str, Any] = {}

        for source_key, value in row.items():
            canonical_key = mapping.get(source_key)
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
            raw=row.copy(),
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
        """Return records matching *entity_type*, optionally filtered."""
        results = [r for r in self._records if r.entity_type == entity_type]
        if filters:
            for key, val in filters.items():
                results = [
                    r for r in results
                    if r.canonical_fields.get(key) == val
                ]
        return results

    def all(self) -> list[Record]:
        return list(self._records)
