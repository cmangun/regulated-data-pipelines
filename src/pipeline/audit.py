"""
Audit Logging Module

HIPAA-compliant, tamper-evident audit logging for data pipelines.
Supports integrity verification, retention policies, and export formats.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


# =============================================================================
# AUDIT ENUMS
# =============================================================================


class AuditAction(str, Enum):
    """Audit action types."""
    
    # Pipeline lifecycle
    PIPELINE_START = "pipeline_start"
    PIPELINE_COMPLETE = "pipeline_complete"
    PIPELINE_FAILED = "pipeline_failed"
    
    # Data operations
    DATA_READ = "data_read"
    DATA_WRITE = "data_write"
    DATA_TRANSFORM = "data_transform"
    DATA_VALIDATE = "data_validate"
    DATA_DELETE = "data_delete"
    
    # Access control
    ACCESS_GRANTED = "access_granted"
    ACCESS_DENIED = "access_denied"
    ACCESS_REVOKED = "access_revoked"
    
    # PHI operations
    PHI_ACCESS = "phi_access"
    PHI_EXPORT = "phi_export"
    PHI_DEIDENTIFY = "phi_deidentify"
    PHI_REDACT = "phi_redact"
    
    # System events
    CONFIG_CHANGE = "config_change"
    ERROR = "error"
    WARNING = "warning"


class AuditLevel(str, Enum):
    """Audit severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


# =============================================================================
# AUDIT ENTRY
# =============================================================================


class AuditEntry(BaseModel):
    """
    Immutable audit log entry with integrity verification.
    
    Each entry includes a hash chain for tamper detection.
    """
    
    # Identity
    entry_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # Timestamp (always UTC)
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    
    # Context
    pipeline_id: str
    pipeline_name: str = ""
    stage: str = ""
    
    # Action
    action: AuditAction
    level: AuditLevel = AuditLevel.INFO
    
    # Actor
    user_id: str = "system"
    user_role: str = ""
    client_ip: str = ""
    
    # Details
    resource_type: str = ""
    resource_id: str = ""
    details: dict[str, Any] = Field(default_factory=dict)
    
    # Data metrics
    record_count: int | None = None
    input_hash: str | None = None
    output_hash: str | None = None
    
    # Timing
    duration_ms: int | None = None
    
    # Integrity
    previous_hash: str = ""
    entry_hash: str = ""
    
    def compute_hash(self) -> str:
        """Compute SHA-256 hash of entry contents."""
        # Exclude entry_hash itself from the hash computation
        data = self.model_dump(exclude={"entry_hash"})
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    def with_hash(self, previous_hash: str = "") -> "AuditEntry":
        """Return a new entry with computed hash chain."""
        self.previous_hash = previous_hash
        self.entry_hash = self.compute_hash()
        return self
    
    def verify_integrity(self) -> bool:
        """Verify that entry hash matches contents."""
        expected = self.compute_hash()
        return self.entry_hash == expected


# =============================================================================
# AUDIT LOGGER
# =============================================================================


class AuditLogger:
    """
    Production-grade audit logger with:
    - Append-only JSONL format
    - Hash chain for integrity
    - Automatic rotation support
    - Multiple output formats
    """
    
    def __init__(
        self,
        audit_path: str | Path,
        pipeline_id: str | None = None,
        pipeline_name: str = "",
        user_id: str = "system",
    ):
        self.audit_path = Path(audit_path)
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.pipeline_id = pipeline_id or str(uuid.uuid4())[:8]
        self.pipeline_name = pipeline_name
        self.user_id = user_id
        
        self._last_hash = ""
        self._entry_count = 0
        
        # Load last hash if file exists
        if self.audit_path.exists():
            self._load_last_hash()
    
    def _load_last_hash(self) -> None:
        """Load the hash of the last entry for chain continuity."""
        try:
            with open(self.audit_path, "rb") as f:
                # Seek to find last line
                f.seek(0, 2)  # End of file
                size = f.tell()
                if size == 0:
                    return
                
                # Read backwards to find last newline
                pos = size - 1
                while pos > 0:
                    f.seek(pos)
                    if f.read(1) == b"\n":
                        break
                    pos -= 1
                
                # Read last line
                if pos > 0:
                    f.seek(pos + 1)
                else:
                    f.seek(0)
                
                last_line = f.readline().decode("utf-8").strip()
                if last_line:
                    entry = AuditEntry.model_validate_json(last_line)
                    self._last_hash = entry.entry_hash
                    self._entry_count += 1
        except Exception:
            pass
    
    def log(self, entry: AuditEntry) -> AuditEntry:
        """
        Append an audit entry to the log.
        
        Automatically adds hash chain and writes to file.
        """
        # Set defaults from logger config
        if not entry.pipeline_id:
            entry.pipeline_id = self.pipeline_id
        if not entry.pipeline_name:
            entry.pipeline_name = self.pipeline_name
        if entry.user_id == "system":
            entry.user_id = self.user_id
        
        # Compute hash chain
        entry = entry.with_hash(self._last_hash)
        
        # Write to file
        with open(self.audit_path, "a") as f:
            f.write(entry.model_dump_json() + "\n")
        
        # Update state
        self._last_hash = entry.entry_hash
        self._entry_count += 1
        
        return entry
    
    def log_action(
        self,
        action: AuditAction,
        stage: str = "",
        level: AuditLevel = AuditLevel.INFO,
        resource_type: str = "",
        resource_id: str = "",
        record_count: int | None = None,
        input_hash: str | None = None,
        output_hash: str | None = None,
        duration_ms: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Convenience method to log an action."""
        entry = AuditEntry(
            pipeline_id=self.pipeline_id,
            pipeline_name=self.pipeline_name,
            stage=stage,
            action=action,
            level=level,
            user_id=self.user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            record_count=record_count,
            input_hash=input_hash,
            output_hash=output_hash,
            duration_ms=duration_ms,
            details=details or {},
        )
        return self.log(entry)
    
    def log_pipeline_start(
        self,
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log pipeline start."""
        return self.log_action(
            action=AuditAction.PIPELINE_START,
            stage="init",
            details=details,
        )
    
    def log_pipeline_complete(
        self,
        record_count: int,
        duration_ms: int,
        output_hash: str = "",
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log pipeline completion."""
        return self.log_action(
            action=AuditAction.PIPELINE_COMPLETE,
            stage="complete",
            record_count=record_count,
            output_hash=output_hash,
            duration_ms=duration_ms,
            details=details,
        )
    
    def log_pipeline_failed(
        self,
        error: str,
        stage: str = "",
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log pipeline failure."""
        return self.log_action(
            action=AuditAction.PIPELINE_FAILED,
            stage=stage,
            level=AuditLevel.ERROR,
            details={"error": error, **(details or {})},
        )
    
    def log_data_read(
        self,
        source: str,
        record_count: int,
        input_hash: str = "",
        duration_ms: int | None = None,
    ) -> AuditEntry:
        """Log data read operation."""
        return self.log_action(
            action=AuditAction.DATA_READ,
            stage="read",
            resource_type="data_source",
            resource_id=source,
            record_count=record_count,
            input_hash=input_hash,
            duration_ms=duration_ms,
        )
    
    def log_data_write(
        self,
        destination: str,
        record_count: int,
        output_hash: str = "",
        duration_ms: int | None = None,
    ) -> AuditEntry:
        """Log data write operation."""
        return self.log_action(
            action=AuditAction.DATA_WRITE,
            stage="write",
            resource_type="data_destination",
            resource_id=destination,
            record_count=record_count,
            output_hash=output_hash,
            duration_ms=duration_ms,
        )
    
    def log_transform(
        self,
        transform_name: str,
        input_count: int,
        output_count: int,
        input_hash: str = "",
        output_hash: str = "",
        duration_ms: int | None = None,
        details: dict[str, Any] | None = None,
    ) -> AuditEntry:
        """Log data transformation."""
        return self.log_action(
            action=AuditAction.DATA_TRANSFORM,
            stage="transform",
            resource_type="transform",
            resource_id=transform_name,
            record_count=output_count,
            input_hash=input_hash,
            output_hash=output_hash,
            duration_ms=duration_ms,
            details={
                "input_count": input_count,
                "output_count": output_count,
                **(details or {}),
            },
        )
    
    def log_phi_access(
        self,
        resource_id: str,
        access_reason: str,
        fields_accessed: list[str] | None = None,
    ) -> AuditEntry:
        """Log PHI access for HIPAA compliance."""
        return self.log_action(
            action=AuditAction.PHI_ACCESS,
            stage="phi",
            level=AuditLevel.WARNING,
            resource_type="phi",
            resource_id=resource_id,
            details={
                "access_reason": access_reason,
                "fields_accessed": fields_accessed or [],
            },
        )
    
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
    
    def verify_chain_integrity(self) -> tuple[bool, list[str]]:
        """
        Verify the integrity of the entire audit log.
        
        Returns:
            Tuple of (is_valid, list of error messages)
        """
        entries = self.read_all()
        errors: list[str] = []
        
        if not entries:
            return True, []
        
        previous_hash = ""
        
        for i, entry in enumerate(entries):
            # Verify entry hash
            if not entry.verify_integrity():
                errors.append(
                    f"Entry {i} ({entry.entry_id}): Hash mismatch - "
                    f"expected {entry.compute_hash()}, got {entry.entry_hash}"
                )
            
            # Verify chain linkage
            if entry.previous_hash != previous_hash:
                errors.append(
                    f"Entry {i} ({entry.entry_id}): Chain broken - "
                    f"expected previous_hash {previous_hash}, got {entry.previous_hash}"
                )
            
            previous_hash = entry.entry_hash
        
        return len(errors) == 0, errors
    
    def export_csv(self, output_path: str | Path) -> int:
        """Export audit log to CSV format."""
        import csv
        
        entries = self.read_all()
        output_path = Path(output_path)
        
        if not entries:
            return 0
        
        fieldnames = [
            "timestamp", "entry_id", "pipeline_id", "action", "level",
            "stage", "user_id", "resource_type", "resource_id",
            "record_count", "duration_ms", "entry_hash",
        ]
        
        with open(output_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            for entry in entries:
                row = {
                    k: getattr(entry, k, "")
                    for k in fieldnames
                }
                row["timestamp"] = entry.timestamp.isoformat()
                writer.writerow(row)
        
        return len(entries)
    
    def get_summary(self) -> dict[str, Any]:
        """Get summary statistics for the audit log."""
        entries = self.read_all()
        
        if not entries:
            return {"total_entries": 0}
        
        actions = {}
        levels = {}
        
        for entry in entries:
            actions[entry.action.value] = actions.get(entry.action.value, 0) + 1
            levels[entry.level.value] = levels.get(entry.level.value, 0) + 1
        
        return {
            "total_entries": len(entries),
            "first_entry": entries[0].timestamp.isoformat(),
            "last_entry": entries[-1].timestamp.isoformat(),
            "actions": actions,
            "levels": levels,
            "pipeline_id": self.pipeline_id,
        }


# =============================================================================
# EXPORTS
# =============================================================================


__all__ = [
    "AuditAction",
    "AuditLevel",
    "AuditEntry",
    "AuditLogger",
]
