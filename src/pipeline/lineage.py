"""
Data Lineage Module

Tracks the origin, transformations, and destination of data
for compliance and debugging purposes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class LineageRecord(BaseModel):
    """Record of a data transformation with lineage tracking."""

    lineage_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Source information
    source_type: str  # "file", "database", "api", etc.
    source_location: str
    source_hash: str | None = None
    
    # Transformation information
    transformation: str
    transformation_version: str = "1.0.0"
    parameters: dict[str, Any] = Field(default_factory=dict)
    
    # Output information
    destination_type: str
    destination_location: str
    destination_hash: str | None = None
    
    # Metrics
    input_records: int
    output_records: int
    records_filtered: int = 0
    records_failed: int = 0
    
    # Parent lineage (for chained transformations)
    parent_lineage_id: str | None = None


class LineageTracker:
    """
    Tracks data lineage across pipeline stages.
    
    Maintains a graph of data transformations that can be:
    - Queried for compliance audits
    - Used for debugging data quality issues
    - Exported for regulatory reporting
    """

    def __init__(self):
        self._records: list[LineageRecord] = []

    def record(
        self,
        source_type: str,
        source_location: str,
        transformation: str,
        destination_type: str,
        destination_location: str,
        input_records: int,
        output_records: int,
        source_hash: str | None = None,
        destination_hash: str | None = None,
        parameters: dict[str, Any] | None = None,
        parent_lineage_id: str | None = None,
        records_filtered: int = 0,
        records_failed: int = 0,
    ) -> LineageRecord:
        """Record a data transformation."""
        record = LineageRecord(
            source_type=source_type,
            source_location=source_location,
            source_hash=source_hash,
            transformation=transformation,
            parameters=parameters or {},
            destination_type=destination_type,
            destination_location=destination_location,
            destination_hash=destination_hash,
            input_records=input_records,
            output_records=output_records,
            records_filtered=records_filtered,
            records_failed=records_failed,
            parent_lineage_id=parent_lineage_id,
        )
        self._records.append(record)
        return record

    def get_lineage(self, lineage_id: str) -> LineageRecord | None:
        """Get a specific lineage record."""
        for record in self._records:
            if record.lineage_id == lineage_id:
                return record
        return None

    def get_ancestors(self, lineage_id: str) -> list[LineageRecord]:
        """Get all ancestor records in the lineage chain."""
        ancestors = []
        current_id = lineage_id

        while current_id:
            record = self.get_lineage(current_id)
            if record:
                ancestors.append(record)
                current_id = record.parent_lineage_id
            else:
                break

        return ancestors

    def get_by_source(self, source_location: str) -> list[LineageRecord]:
        """Get all lineage records with a specific source."""
        return [r for r in self._records if r.source_location == source_location]

    def get_by_destination(self, destination_location: str) -> list[LineageRecord]:
        """Get all lineage records with a specific destination."""
        return [r for r in self._records if r.destination_location == destination_location]

    def export(self) -> list[dict[str, Any]]:
        """Export all lineage records as dictionaries."""
        return [r.model_dump() for r in self._records]

    def summary(self) -> dict[str, Any]:
        """Get a summary of lineage statistics."""
        total_input = sum(r.input_records for r in self._records)
        total_output = sum(r.output_records for r in self._records)
        total_filtered = sum(r.records_filtered for r in self._records)
        total_failed = sum(r.records_failed for r in self._records)

        return {
            "total_transformations": len(self._records),
            "total_input_records": total_input,
            "total_output_records": total_output,
            "total_filtered_records": total_filtered,
            "total_failed_records": total_failed,
            "unique_sources": len(set(r.source_location for r in self._records)),
            "unique_destinations": len(set(r.destination_location for r in self._records)),
        }
