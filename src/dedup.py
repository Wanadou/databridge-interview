"""
Duplicate detection and merging across tenants.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any
import uuid

from .store import Record, DataStore


def find_duplicates(
    store: DataStore,
    key_field: str = "email",
) -> list[dict[str, Any]]:
    """Find records sharing the same *key_field* across different tenants.

    Returns:
        A list of duplicate groups::

            [
              {
                "key_field": "email",
                "value": "alice@example.com",
                "records": [...],
                "tenants": ["acme", "betacorp"],
              },
              ...
            ]
    """
    groups: dict[str, list[Record]] = defaultdict(list)

    for record in store.all():
        value = record.canonical_fields.get(key_field)
        if value is not None:
            groups[value].append(record)

    return [
        {
            "key_field": key_field,
            "value": value,
            "records": records,
            "tenants": list({r.tenant_id for r in records}),
        }
        for value, records in groups.items()
        if len({r.tenant_id for r in records}) > 1
    ]


def merge_records(records: list[Record]) -> Record:
    """Merge multiple records; non-None values from earlier records win."""
    merged_canonical: dict[str, Any] = {}
    merged_extra: dict[str, Any] = {}

    for record in records:
        for k, v in record.canonical_fields.items():
            if v is not None and k not in merged_canonical:
                merged_canonical[k] = v
        merged_extra.update(record.extra_fields)

    return Record(
        id=str(uuid.uuid4()),
        tenant_id="merged",
        entity_type=records[0].entity_type,
        canonical_fields=merged_canonical,
        extra_fields=merged_extra,
        raw={},
    )
