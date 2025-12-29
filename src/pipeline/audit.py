"""
Audit Logging Module

Provides append-only audit logging for compliance and traceability.
All pipeline operations are logged with immutable entries.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class AuditEntry(BaseModel):
    """Immutable audit log entry."""

    entry_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    pipeline_id: str
    stage: str
    action: str
    status: str  # "started", "completed", "failed"
    user: str = "system"
    details: dict[str, Any] = Field(default_factory=dict)
    input_hash: str | None = None
    output_hash: str | None = None
    record_count: int | None = None
    duration_ms: int | None = None


class AuditLogger:
    """
    Append-only audit logger for pipeline operations.
    
    All entries are written as JSON lines (JSONL) format for:
    - Immutability: Each line is a complete record
    - Streaming: Can be processed line-by-line
    - Compliance: Easy to verify no modifications
    """

    def __init__(self, audit_path: str | Path):
        self.audit_path = Path(audit_path)
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)

    def log(self, entry: AuditEntry) -> None:
        """Append an audit entry to the log file."""
        with open(self.audit_path, "a") as f:
            f.write(entry.model_dump_json() + "\n")

    def log_start(
        self,
        pipeline_id: str,
        stage: str,
        action: str,
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log the start of an operation."""
        entry = AuditEntry(
            pipeline_id=pipeline_id,
            stage=stage,
            action=action,
            status="started",
            details=details or {},
        )
        self.log(entry)
        return entry

    def log_complete(
        self,
        pipeline_id: str,
        stage: str,
        action: str,
        record_count: int | None = None,
        output_hash: str | None = None,
        duration_ms: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log the successful completion of an operation."""
        entry = AuditEntry(
            pipeline_id=pipeline_id,
            stage=stage,
            action=action,
            status="completed",
            record_count=record_count,
            output_hash=output_hash,
            duration_ms=duration_ms,
            details=details or {},
        )
        self.log(entry)
        return entry

    def log_failure(
        self,
        pipeline_id: str,
        stage: str,
        action: str,
        error: str,
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log a failed operation."""
        entry = AuditEntry(
            pipeline_id=pipeline_id,
            stage=stage,
            action=action,
            status="failed",
            details={"error": error, **(details or {})},
        )
        self.log(entry)
        return entry

    def read_all(self) -> list[AuditEntry]:
        """Read all audit entries from the log file."""
        if not self.audit_path.exists():
            return []

        entries = []
        with open(self.audit_path, "r") as f:
            for line in f:
                if line.strip():
                    entries.append(AuditEntry.model_validate_json(line))
        return entries

    def verify_integrity(self) -> bool:
        """
        Verify the integrity of the audit log.
        
        In production, this would verify cryptographic signatures
        or compare against a blockchain/ledger.
        """
        try:
            entries = self.read_all()
            # Basic verification: all entries parse correctly
            return all(isinstance(e, AuditEntry) for e in entries)
        except Exception:
            return False
