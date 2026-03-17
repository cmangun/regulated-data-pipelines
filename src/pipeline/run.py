"""
Pipeline Runner Module

Main entry point for running compliant data pipelines.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
import uuid
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field

from .audit import AuditLogger, AuditAction
from .lineage import LineageTracker, SourceType


class PipelineConfig(BaseModel):
    """Configuration for a pipeline run."""

    pipeline_id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    input_path: Path
    output_path: Path
    audit_path: Path
    transformations: list[str] = Field(default_factory=lambda: ["validate", "transform"])


class PipelineResult(BaseModel):
    """Result of a pipeline run."""

    pipeline_id: str
    success: bool
    input_records: int
    output_records: int
    duration_ms: int
    output_hash: str
    errors: list[str] = Field(default_factory=list)


def compute_file_hash(path: Path) -> str:
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def validate_data(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Validate input data and return valid rows with errors."""
    errors = []

    required_cols = ["id", "value"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        errors.append(f"Missing required columns: {missing}")
        return df, errors

    null_ids = df["id"].isna().sum()
    if null_ids > 0:
        errors.append(f"Removed {null_ids} rows with null IDs")
        df = df.dropna(subset=["id"])

    return df, errors


def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Apply transformations to the data."""
    if "value" in df.columns:
        df = df.copy()
        df["value_squared"] = df["value"] ** 2
        df["processed_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    return df


def run_pipeline(config: PipelineConfig) -> PipelineResult:
    """Run a compliant data pipeline with full audit logging."""
    start_time = time.time()
    audit = AuditLogger(
        audit_path=config.audit_path,
        pipeline_id=config.pipeline_id,
    )
    lineage = LineageTracker(pipeline_id=config.pipeline_id)
    errors: list[str] = []

    audit.log_pipeline_start(details={
        "input": str(config.input_path),
        "output": str(config.output_path),
    })

    try:
        input_hash = compute_file_hash(config.input_path)
        df = pd.read_csv(config.input_path)
        input_records = len(df)
        audit.log_data_read(
            source=str(config.input_path),
            record_count=input_records,
            input_hash=input_hash,
        )

        if "validate" in config.transformations:
            df, validation_errors = validate_data(df)
            errors.extend(validation_errors)
            audit.log_transform(
                transform_name="validate_data",
                input_count=input_records,
                output_count=len(df),
            )

        if "transform" in config.transformations:
            pre_transform_count = len(df)
            df = transform_data(df)
            audit.log_transform(
                transform_name="transform_data",
                input_count=pre_transform_count,
                output_count=len(df),
            )

        config.output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(config.output_path, index=False)
        output_hash = compute_file_hash(config.output_path)
        output_records = len(df)
        audit.log_data_write(
            destination=str(config.output_path),
            record_count=output_records,
            output_hash=output_hash,
        )

        lineage.record(
            source_type=SourceType.FILE,
            source_location=str(config.input_path),
            source_hash=input_hash,
            transformation="etl_pipeline",
            destination_type=SourceType.FILE,
            destination_location=str(config.output_path),
            destination_hash=output_hash,
            input_records=input_records,
            output_records=output_records,
        )

        duration_ms = int((time.time() - start_time) * 1000)
        audit.log_pipeline_complete(
            record_count=output_records,
            duration_ms=duration_ms,
            output_hash=output_hash,
        )

        return PipelineResult(
            pipeline_id=config.pipeline_id,
            success=True,
            input_records=input_records,
            output_records=output_records,
            duration_ms=duration_ms,
            output_hash=output_hash,
            errors=errors,
        )

    except Exception as e:
        duration_ms = int((time.time() - start_time) * 1000)
        audit.log_pipeline_failed(error=str(e), stage="pipeline")
        return PipelineResult(
            pipeline_id=config.pipeline_id,
            success=False,
            input_records=0,
            output_records=0,
            duration_ms=duration_ms,
            output_hash="",
            errors=[str(e)],
        )


def main() -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Run a compliant data pipeline")
    parser.add_argument("--input", "-i", required=True, help="Input CSV file")
    parser.add_argument("--output", "-o", required=True, help="Output CSV file")
    parser.add_argument("--audit", "-a", required=True, help="Audit log file (JSONL)")

    args = parser.parse_args()

    config = PipelineConfig(
        input_path=Path(args.input),
        output_path=Path(args.output),
        audit_path=Path(args.audit),
    )

    print(f"Starting pipeline {config.pipeline_id}...")
    result = run_pipeline(config)

    if result.success:
        print(f"✅ Pipeline completed: {result.output_records} records")
        return 0
    else:
        print(f"❌ Pipeline failed: {result.errors}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
