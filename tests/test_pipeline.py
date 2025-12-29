"""Tests for regulated data pipelines."""

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from pipeline.audit import AuditLogger, AuditEntry
from pipeline.lineage import LineageTracker
from pipeline.run import (
    PipelineConfig,
    run_pipeline,
    validate_data,
    transform_data,
    compute_file_hash,
)


class TestAuditLogger:
    """Tests for audit logging functionality."""

    def test_log_and_read(self, tmp_path: Path):
        """Test basic logging and reading."""
        audit_path = tmp_path / "audit.jsonl"
        logger = AuditLogger(audit_path)
        
        entry = logger.log_start("test-123", "read", "read_csv")
        
        entries = logger.read_all()
        assert len(entries) == 1
        assert entries[0].pipeline_id == "test-123"
        assert entries[0].status == "started"

    def test_log_complete(self, tmp_path: Path):
        """Test logging completion."""
        audit_path = tmp_path / "audit.jsonl"
        logger = AuditLogger(audit_path)
        
        logger.log_complete(
            "test-123", "transform", "transform_data",
            record_count=100,
            duration_ms=50,
        )
        
        entries = logger.read_all()
        assert entries[0].status == "completed"
        assert entries[0].record_count == 100

    def test_log_failure(self, tmp_path: Path):
        """Test logging failure."""
        audit_path = tmp_path / "audit.jsonl"
        logger = AuditLogger(audit_path)
        
        logger.log_failure(
            "test-123", "pipeline", "error",
            error="Something went wrong",
        )
        
        entries = logger.read_all()
        assert entries[0].status == "failed"
        assert "Something went wrong" in entries[0].details["error"]


class TestLineageTracker:
    """Tests for lineage tracking functionality."""

    def test_record_lineage(self):
        """Test recording lineage."""
        tracker = LineageTracker()
        
        record = tracker.record(
            source_type="file",
            source_location="/data/input.csv",
            transformation="etl",
            destination_type="file",
            destination_location="/data/output.csv",
            input_records=100,
            output_records=95,
        )
        
        assert record.input_records == 100
        assert record.output_records == 95
        assert tracker.get_lineage(record.lineage_id) == record

    def test_lineage_summary(self):
        """Test lineage summary."""
        tracker = LineageTracker()
        
        tracker.record(
            source_type="file",
            source_location="/data/a.csv",
            transformation="etl",
            destination_type="file",
            destination_location="/data/b.csv",
            input_records=100,
            output_records=90,
            records_filtered=10,
        )
        
        summary = tracker.summary()
        assert summary["total_transformations"] == 1
        assert summary["total_input_records"] == 100
        assert summary["total_filtered_records"] == 10


class TestTransformations:
    """Tests for data transformations."""

    def test_validate_data(self):
        """Test data validation."""
        df = pd.DataFrame({
            "id": [1, 2, None, 4],
            "value": [10, 20, 30, 40],
        })
        
        result, errors = validate_data(df)
        
        assert len(result) == 3  # One row removed
        assert any("null IDs" in e for e in errors)

    def test_transform_data(self):
        """Test data transformation."""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "value": [2, 3, 4],
        })
        
        result = transform_data(df)
        
        assert "value_squared" in result.columns
        assert result["value_squared"].tolist() == [4, 9, 16]
        assert "processed_at" in result.columns


class TestPipeline:
    """Tests for full pipeline runs."""

    def test_run_pipeline(self, tmp_path: Path):
        """Test complete pipeline run."""
        # Create input file
        input_path = tmp_path / "input.csv"
        output_path = tmp_path / "output.csv"
        audit_path = tmp_path / "audit.jsonl"
        
        pd.DataFrame({
            "id": [1, 2, 3],
            "value": [10, 20, 30],
        }).to_csv(input_path, index=False)
        
        config = PipelineConfig(
            input_path=input_path,
            output_path=output_path,
            audit_path=audit_path,
        )
        
        result = run_pipeline(config)
        
        assert result.success
        assert result.input_records == 3
        assert result.output_records == 3
        assert output_path.exists()
        assert audit_path.exists()
        
        # Check output has new columns
        output_df = pd.read_csv(output_path)
        assert "value_squared" in output_df.columns

    def test_file_hash(self, tmp_path: Path):
        """Test file hashing."""
        file_path = tmp_path / "test.txt"
        file_path.write_text("hello world")
        
        hash1 = compute_file_hash(file_path)
        hash2 = compute_file_hash(file_path)
        
        assert hash1 == hash2
        assert len(hash1) == 64  # SHA-256 hex
