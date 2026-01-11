"""
Data Lineage Module

Track the complete provenance of data through pipelines.
Supports graph traversal, impact analysis, and compliance reporting.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


# =============================================================================
# LINEAGE ENUMS
# =============================================================================


class NodeType(str, Enum):
    """Types of nodes in the lineage graph."""
    SOURCE = "source"           # Input data source
    TRANSFORM = "transform"     # Transformation operation
    DESTINATION = "destination" # Output destination
    STAGING = "staging"         # Intermediate storage


class SourceType(str, Enum):
    """Types of data sources."""
    FILE = "file"
    DATABASE = "database"
    API = "api"
    STREAM = "stream"
    S3 = "s3"
    GCS = "gcs"
    AZURE_BLOB = "azure_blob"


# =============================================================================
# LINEAGE MODELS
# =============================================================================


class LineageNode(BaseModel):
    """A node in the lineage graph."""
    
    node_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:12])
    node_type: NodeType
    name: str
    
    # Location/identification
    source_type: SourceType | None = None
    location: str = ""
    
    # Metadata
    schema_version: str = ""
    properties: dict[str, Any] = Field(default_factory=dict)
    
    # Timing
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class LineageEdge(BaseModel):
    """An edge connecting two nodes in the lineage graph."""
    
    edge_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:12])
    source_node_id: str
    target_node_id: str
    
    # Operation details
    operation: str = ""
    operation_version: str = "1.0.0"
    
    # Data metrics
    input_records: int = 0
    output_records: int = 0
    records_filtered: int = 0
    records_failed: int = 0
    
    # Hashes for verification
    input_hash: str = ""
    output_hash: str = ""
    
    # Timing
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    duration_ms: int = 0
    
    # Execution context
    pipeline_id: str = ""
    run_id: str = ""


class LineageRecord(BaseModel):
    """
    Complete lineage record for a data transformation.
    
    Captures source, transformation, and destination in one record.
    """
    
    lineage_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Source information
    source_type: SourceType
    source_location: str
    source_hash: str | None = None
    
    # Transformation information
    transformation: str
    transformation_version: str = "1.0.0"
    parameters: dict[str, Any] = Field(default_factory=dict)
    
    # Destination information
    destination_type: SourceType
    destination_location: str
    destination_hash: str | None = None
    
    # Metrics
    input_records: int
    output_records: int
    records_filtered: int = 0
    records_failed: int = 0
    
    # Chain
    parent_lineage_id: str | None = None
    
    # Execution context
    pipeline_id: str = ""
    run_id: str = ""
    duration_ms: int = 0


# =============================================================================
# LINEAGE TRACKER
# =============================================================================


class LineageTracker:
    """
    Track and query data lineage.
    
    Supports:
    - Recording transformations
    - Querying lineage history
    - Impact analysis
    - Graph traversal
    """
    
    def __init__(self, pipeline_id: str = "", run_id: str = ""):
        self.pipeline_id = pipeline_id or str(uuid.uuid4())[:8]
        self.run_id = run_id or str(uuid.uuid4())[:8]
        
        self._records: list[LineageRecord] = []
        self._nodes: dict[str, LineageNode] = {}
        self._edges: list[LineageEdge] = []
    
    def record(
        self,
        source_type: SourceType | str,
        source_location: str,
        transformation: str,
        destination_type: SourceType | str,
        destination_location: str,
        input_records: int,
        output_records: int,
        source_hash: str | None = None,
        destination_hash: str | None = None,
        parameters: dict[str, Any] | None = None,
        parent_lineage_id: str | None = None,
        records_filtered: int = 0,
        records_failed: int = 0,
        duration_ms: int = 0,
    ) -> LineageRecord:
        """
        Record a data transformation.
        
        Returns:
            The created LineageRecord
        """
        if isinstance(source_type, str):
            source_type = SourceType(source_type)
        if isinstance(destination_type, str):
            destination_type = SourceType(destination_type)
        
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
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
            duration_ms=duration_ms,
        )
        
        self._records.append(record)
        
        # Also create graph representation
        self._add_to_graph(record)
        
        return record
    
    def _add_to_graph(self, record: LineageRecord) -> None:
        """Add lineage record to the internal graph."""
        # Create/update source node
        source_id = f"src_{record.source_location}"
        if source_id not in self._nodes:
            self._nodes[source_id] = LineageNode(
                node_id=source_id,
                node_type=NodeType.SOURCE,
                name=record.source_location,
                source_type=record.source_type,
                location=record.source_location,
            )
        
        # Create transform node
        transform_id = f"txn_{record.lineage_id[:8]}"
        self._nodes[transform_id] = LineageNode(
            node_id=transform_id,
            node_type=NodeType.TRANSFORM,
            name=record.transformation,
            properties={"version": record.transformation_version},
        )
        
        # Create/update destination node
        dest_id = f"dst_{record.destination_location}"
        if dest_id not in self._nodes:
            self._nodes[dest_id] = LineageNode(
                node_id=dest_id,
                node_type=NodeType.DESTINATION,
                name=record.destination_location,
                source_type=record.destination_type,
                location=record.destination_location,
            )
        
        # Create edges
        self._edges.append(LineageEdge(
            source_node_id=source_id,
            target_node_id=transform_id,
            operation="input",
            input_records=record.input_records,
            input_hash=record.source_hash or "",
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
        ))
        
        self._edges.append(LineageEdge(
            source_node_id=transform_id,
            target_node_id=dest_id,
            operation=record.transformation,
            operation_version=record.transformation_version,
            input_records=record.input_records,
            output_records=record.output_records,
            records_filtered=record.records_filtered,
            records_failed=record.records_failed,
            output_hash=record.destination_hash or "",
            duration_ms=record.duration_ms,
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
        ))
    
    def get_lineage(self, lineage_id: str) -> LineageRecord | None:
        """Get a specific lineage record."""
        for record in self._records:
            if record.lineage_id == lineage_id:
                return record
        return None
    
    def get_by_source(self, source_location: str) -> list[LineageRecord]:
        """Get all records with a specific source."""
        return [r for r in self._records if r.source_location == source_location]
    
    def get_by_destination(self, destination_location: str) -> list[LineageRecord]:
        """Get all records with a specific destination."""
        return [r for r in self._records if r.destination_location == destination_location]
    
    def get_ancestors(self, lineage_id: str) -> list[LineageRecord]:
        """Get all ancestor records in the lineage chain."""
        ancestors = []
        current_id: str | None = lineage_id
        
        while current_id:
            record = self.get_lineage(current_id)
            if record:
                ancestors.append(record)
                current_id = record.parent_lineage_id
            else:
                break
        
        return ancestors
    
    def get_descendants(self, source_location: str) -> list[LineageRecord]:
        """Get all downstream records from a source."""
        descendants = []
        to_check = [source_location]
        seen = set()
        
        while to_check:
            current = to_check.pop(0)
            if current in seen:
                continue
            seen.add(current)
            
            for record in self._records:
                if record.source_location == current:
                    descendants.append(record)
                    to_check.append(record.destination_location)
        
        return descendants
    
    def impact_analysis(self, source_location: str) -> dict[str, Any]:
        """
        Analyze the downstream impact of a data source.
        
        Useful for change impact assessment.
        """
        descendants = self.get_descendants(source_location)
        
        affected_destinations = set()
        affected_transforms = set()
        total_records_impacted = 0
        
        for record in descendants:
            affected_destinations.add(record.destination_location)
            affected_transforms.add(record.transformation)
            total_records_impacted += record.output_records
        
        return {
            "source": source_location,
            "affected_destinations": list(affected_destinations),
            "affected_transforms": list(affected_transforms),
            "total_downstream_records": len(descendants),
            "total_records_impacted": total_records_impacted,
        }
    
    def summary(self) -> dict[str, Any]:
        """Get summary statistics for all lineage."""
        total_input = sum(r.input_records for r in self._records)
        total_output = sum(r.output_records for r in self._records)
        total_filtered = sum(r.records_filtered for r in self._records)
        total_failed = sum(r.records_failed for r in self._records)
        
        return {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "total_transformations": len(self._records),
            "total_input_records": total_input,
            "total_output_records": total_output,
            "total_filtered_records": total_filtered,
            "total_failed_records": total_failed,
            "unique_sources": len(set(r.source_location for r in self._records)),
            "unique_destinations": len(set(r.destination_location for r in self._records)),
            "unique_transforms": len(set(r.transformation for r in self._records)),
        }
    
    def export(self) -> list[dict[str, Any]]:
        """Export all lineage records as dictionaries."""
        return [r.model_dump() for r in self._records]
    
    def export_graph(self) -> dict[str, Any]:
        """Export the lineage graph."""
        return {
            "nodes": [n.model_dump() for n in self._nodes.values()],
            "edges": [e.model_dump() for e in self._edges],
        }
    
    def save(self, path: str | Path) -> None:
        """Save lineage to a JSON file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        data = {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "records": self.export(),
            "graph": self.export_graph(),
            "summary": self.summary(),
        }
        
        with open(path, "w") as f:
            json.dump(data, f, indent=2, default=str)
    
    @classmethod
    def load(cls, path: str | Path) -> "LineageTracker":
        """Load lineage from a JSON file."""
        with open(path, "r") as f:
            data = json.load(f)
        
        tracker = cls(
            pipeline_id=data.get("pipeline_id", ""),
            run_id=data.get("run_id", ""),
        )
        
        for record_data in data.get("records", []):
            record = LineageRecord.model_validate(record_data)
            tracker._records.append(record)
            tracker._add_to_graph(record)
        
        return tracker


# =============================================================================
# EXPORTS
# =============================================================================


__all__ = [
    "NodeType",
    "SourceType",
    "LineageNode",
    "LineageEdge",
    "LineageRecord",
    "LineageTracker",
]
