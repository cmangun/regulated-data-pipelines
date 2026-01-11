"""Tests for data transforms, audit logging, and lineage tracking."""

from datetime import datetime
from pathlib import Path
import tempfile

import pytest

from pipeline.transforms import (
    FilterTransform,
    MapTransform,
    SelectTransform,
    DeriveTransform,
    DeduplicateTransform,
    AgeCalculatorTransform,
    ICD10ValidatorTransform,
    NPIValidatorTransform,
    TransformPipeline,
)
from pipeline.audit import (
    AuditAction,
    AuditLevel,
    AuditEntry,
    AuditLogger,
)
from pipeline.lineage import (
    SourceType,
    LineageTracker,
)


# =============================================================================
# TRANSFORM TESTS
# =============================================================================


class TestFilterTransform:
    """Tests for FilterTransform."""

    def test_filter_records(self):
        """Test basic filtering."""
        transform = FilterTransform(
            name="filter_active",
            predicate=lambda r: r.get("is_active", False),
        )
        
        data = [
            {"id": 1, "is_active": True},
            {"id": 2, "is_active": False},
            {"id": 3, "is_active": True},
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert result.output_count == 2
        assert result.filtered_count == 1
        assert all(r["is_active"] for r in result.data)

    def test_filter_all(self):
        """Test filtering all records."""
        transform = FilterTransform(
            name="filter_none",
            predicate=lambda r: False,
        )
        
        data = [{"id": 1}, {"id": 2}]
        result = transform.transform(data)
        
        assert result.success
        assert result.output_count == 0
        assert result.filtered_count == 2


class TestMapTransform:
    """Tests for MapTransform."""

    def test_map_records(self):
        """Test basic mapping."""
        transform = MapTransform(
            name="uppercase_name",
            mapper=lambda r: {**r, "name": r["name"].upper()},
        )
        
        data = [
            {"id": 1, "name": "john"},
            {"id": 2, "name": "jane"},
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert result.data[0]["name"] == "JOHN"
        assert result.data[1]["name"] == "JANE"

    def test_map_with_error(self):
        """Test mapping with error handling."""
        transform = MapTransform(
            name="divide",
            mapper=lambda r: {**r, "result": 100 / r["value"]},
            skip_errors=True,
        )
        
        data = [
            {"id": 1, "value": 10},
            {"id": 2, "value": 0},  # Division by zero
            {"id": 3, "value": 5},
        ]
        
        result = transform.transform(data)
        
        assert result.success  # skip_errors=True
        assert result.failed_count == 1
        assert len(result.data) == 2


class TestSelectTransform:
    """Tests for SelectTransform."""

    def test_select_fields(self):
        """Test field selection."""
        transform = SelectTransform(
            name="select_basic",
            fields=["id", "name"],
        )
        
        data = [
            {"id": 1, "name": "John", "age": 30, "email": "john@test.com"},
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert set(result.data[0].keys()) == {"id", "name"}

    def test_select_with_rename(self):
        """Test field selection with renaming."""
        transform = SelectTransform(
            name="select_rename",
            fields=["id", "name"],
            rename={"id": "patient_id", "name": "patient_name"},
        )
        
        data = [{"id": 1, "name": "John"}]
        result = transform.transform(data)
        
        assert "patient_id" in result.data[0]
        assert "patient_name" in result.data[0]


class TestDeriveTransform:
    """Tests for DeriveTransform."""

    def test_derive_fields(self):
        """Test deriving new fields."""
        transform = DeriveTransform(
            name="derive_full_name",
            derivations={
                "full_name": lambda r: f"{r['first']} {r['last']}",
                "name_length": lambda r: len(r["first"]) + len(r["last"]),
            },
        )
        
        data = [{"first": "John", "last": "Doe"}]
        result = transform.transform(data)
        
        assert result.success
        assert result.data[0]["full_name"] == "John Doe"
        assert result.data[0]["name_length"] == 7


class TestDeduplicateTransform:
    """Tests for DeduplicateTransform."""

    def test_deduplicate(self):
        """Test basic deduplication."""
        transform = DeduplicateTransform(
            name="dedupe_id",
            key_fields=["id"],
        )
        
        data = [
            {"id": 1, "value": "a"},
            {"id": 2, "value": "b"},
            {"id": 1, "value": "c"},  # Duplicate
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert result.output_count == 2
        assert result.filtered_count == 1

    def test_deduplicate_keep_last(self):
        """Test keeping last duplicate."""
        transform = DeduplicateTransform(
            name="dedupe_last",
            key_fields=["id"],
            keep="last",
        )
        
        data = [
            {"id": 1, "value": "first"},
            {"id": 1, "value": "last"},
        ]
        
        result = transform.transform(data)
        
        assert result.data[0]["value"] == "last"


class TestAgeCalculatorTransform:
    """Tests for AgeCalculatorTransform."""

    def test_calculate_age(self):
        """Test age calculation."""
        transform = AgeCalculatorTransform(
            name="calc_age",
            reference_date=datetime(2024, 1, 15),
        )
        
        data = [
            {"id": 1, "date_of_birth": "1990-01-15"},
            {"id": 2, "date_of_birth": "1990-01-16"},  # Birthday not yet
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert result.data[0]["age"] == 34
        assert result.data[1]["age"] == 33


class TestICD10ValidatorTransform:
    """Tests for ICD10ValidatorTransform."""

    def test_validate_icd10(self):
        """Test ICD-10 code validation."""
        transform = ICD10ValidatorTransform(
            name="validate_icd10",
            code_field="diagnosis",
        )
        
        data = [
            {"diagnosis": "J06.9"},   # Valid
            {"diagnosis": "A00"},     # Valid
            {"diagnosis": "invalid"}, # Invalid
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert result.data[0]["is_valid_icd10"] is True
        assert result.data[1]["is_valid_icd10"] is True
        assert result.data[2]["is_valid_icd10"] is False


class TestNPIValidatorTransform:
    """Tests for NPIValidatorTransform."""

    def test_validate_npi(self):
        """Test NPI validation."""
        transform = NPIValidatorTransform(name="validate_npi")
        
        data = [
            {"npi": "1234567897"},  # Valid (passes Luhn)
            {"npi": "1234567890"},  # Invalid
            {"npi": "123"},         # Invalid length
        ]
        
        result = transform.transform(data)
        
        assert result.success
        assert result.data[0]["is_valid_npi"] is True
        assert result.data[1]["is_valid_npi"] is False
        assert result.data[2]["is_valid_npi"] is False


class TestTransformPipeline:
    """Tests for TransformPipeline."""

    def test_pipeline_execution(self):
        """Test executing transform pipeline."""
        pipeline = TransformPipeline(name="test_pipeline")
        pipeline.add(FilterTransform(
            name="filter_active",
            predicate=lambda r: r.get("is_active"),
        ))
        pipeline.add(MapTransform(
            name="uppercase",
            mapper=lambda r: {**r, "name": r["name"].upper()},
        ))
        pipeline.add(SelectTransform(
            name="select",
            fields=["id", "name"],
        ))
        
        data = [
            {"id": 1, "name": "john", "is_active": True},
            {"id": 2, "name": "jane", "is_active": False},
            {"id": 3, "name": "bob", "is_active": True},
        ]
        
        result = pipeline.execute(data)
        
        assert result.success
        assert result.output_count == 2
        assert result.data[0]["name"] == "JOHN"

    def test_pipeline_lineage(self):
        """Test pipeline lineage tracking."""
        pipeline = TransformPipeline(name="test_pipeline")
        pipeline.add(FilterTransform(name="filter", predicate=lambda r: True))
        pipeline.add(MapTransform(name="map", mapper=lambda r: r))
        
        data = [{"id": 1}]
        pipeline.execute(data)
        
        lineage = pipeline.get_lineage()
        
        assert len(lineage) == 2
        assert lineage[0]["transform"] == "filter"
        assert lineage[1]["transform"] == "map"


# =============================================================================
# AUDIT LOGGER TESTS
# =============================================================================


class TestAuditLogger:
    """Tests for AuditLogger."""

    def test_log_entry(self):
        """Test logging an audit entry."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            entry = logger.log_action(
                action=AuditAction.DATA_READ,
                stage="read",
                record_count=100,
            )
            
            assert entry.pipeline_id == "test"
            assert entry.action == AuditAction.DATA_READ
            assert entry.entry_hash != ""

    def test_hash_chain(self):
        """Test hash chain integrity."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            entry1 = logger.log_action(AuditAction.PIPELINE_START, stage="init")
            entry2 = logger.log_action(AuditAction.DATA_READ, stage="read")
            
            # Second entry should reference first entry's hash
            assert entry2.previous_hash == entry1.entry_hash

    def test_verify_integrity(self):
        """Test integrity verification."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            logger.log_action(AuditAction.PIPELINE_START, stage="init")
            logger.log_action(AuditAction.DATA_READ, stage="read")
            logger.log_action(AuditAction.PIPELINE_COMPLETE, stage="done")
            
            is_valid, errors = logger.verify_chain_integrity()
            
            assert is_valid
            assert len(errors) == 0

    def test_read_all(self):
        """Test reading all entries."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            logger.log_action(AuditAction.PIPELINE_START, stage="init")
            logger.log_action(AuditAction.DATA_READ, stage="read")
            
            entries = logger.read_all()
            
            assert len(entries) == 2
            assert entries[0].action == AuditAction.PIPELINE_START

    def test_convenience_methods(self):
        """Test convenience logging methods."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            logger.log_pipeline_start(details={"source": "test.csv"})
            logger.log_data_read("test.csv", record_count=100)
            logger.log_transform("filter", input_count=100, output_count=80)
            logger.log_data_write("output.csv", record_count=80)
            logger.log_pipeline_complete(record_count=80, duration_ms=1000)
            
            entries = logger.read_all()
            
            assert len(entries) == 5

    def test_phi_access_logging(self):
        """Test PHI access logging."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            entry = logger.log_phi_access(
                resource_id="patient_123",
                access_reason="treatment",
                fields_accessed=["ssn", "dob"],
            )
            
            assert entry.action == AuditAction.PHI_ACCESS
            assert entry.level == AuditLevel.WARNING
            assert "ssn" in entry.details["fields_accessed"]

    def test_get_summary(self):
        """Test summary statistics."""
        with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as f:
            logger = AuditLogger(f.name, pipeline_id="test")
            
            logger.log_action(AuditAction.PIPELINE_START, stage="init")
            logger.log_action(AuditAction.DATA_READ, stage="read")
            logger.log_action(AuditAction.ERROR, level=AuditLevel.ERROR, stage="error")
            
            summary = logger.get_summary()
            
            assert summary["total_entries"] == 3
            assert "actions" in summary


# =============================================================================
# LINEAGE TRACKER TESTS
# =============================================================================


class TestLineageTracker:
    """Tests for LineageTracker."""

    def test_record_lineage(self):
        """Test recording lineage."""
        tracker = LineageTracker(pipeline_id="test")
        
        record = tracker.record(
            source_type=SourceType.FILE,
            source_location="/data/input.csv",
            transformation="filter",
            destination_type=SourceType.FILE,
            destination_location="/data/output.csv",
            input_records=100,
            output_records=80,
        )
        
        assert record.source_type == SourceType.FILE
        assert record.input_records == 100
        assert record.output_records == 80

    def test_get_by_source(self):
        """Test querying by source."""
        tracker = LineageTracker()
        
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/data/a.csv",
            transformation="t1",
            destination_type=SourceType.FILE,
            destination_location="/data/b.csv",
            input_records=100,
            output_records=100,
        )
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/data/b.csv",
            transformation="t2",
            destination_type=SourceType.FILE,
            destination_location="/data/c.csv",
            input_records=100,
            output_records=90,
        )
        
        records = tracker.get_by_source("/data/a.csv")
        
        assert len(records) == 1
        assert records[0].transformation == "t1"

    def test_get_descendants(self):
        """Test getting downstream lineage."""
        tracker = LineageTracker()
        
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/a",
            transformation="t1",
            destination_type=SourceType.FILE,
            destination_location="/b",
            input_records=100,
            output_records=100,
        )
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/b",
            transformation="t2",
            destination_type=SourceType.FILE,
            destination_location="/c",
            input_records=100,
            output_records=100,
        )
        
        descendants = tracker.get_descendants("/a")
        
        assert len(descendants) == 2

    def test_impact_analysis(self):
        """Test impact analysis."""
        tracker = LineageTracker()
        
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/raw/data.csv",
            transformation="clean",
            destination_type=SourceType.FILE,
            destination_location="/clean/data.csv",
            input_records=1000,
            output_records=950,
        )
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/clean/data.csv",
            transformation="transform",
            destination_type=SourceType.DATABASE,
            destination_location="warehouse.patients",
            input_records=950,
            output_records=950,
        )
        
        impact = tracker.impact_analysis("/raw/data.csv")
        
        assert len(impact["affected_destinations"]) == 2
        assert "warehouse.patients" in impact["affected_destinations"]

    def test_summary(self):
        """Test lineage summary."""
        tracker = LineageTracker(pipeline_id="test")
        
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/a",
            transformation="t1",
            destination_type=SourceType.FILE,
            destination_location="/b",
            input_records=100,
            output_records=90,
            records_filtered=10,
        )
        
        summary = tracker.summary()
        
        assert summary["total_transformations"] == 1
        assert summary["total_input_records"] == 100
        assert summary["total_filtered_records"] == 10

    def test_save_and_load(self):
        """Test saving and loading lineage."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "lineage.json"
            
            tracker1 = LineageTracker(pipeline_id="test")
            tracker1.record(
                source_type=SourceType.FILE,
                source_location="/a",
                transformation="t1",
                destination_type=SourceType.FILE,
                destination_location="/b",
                input_records=100,
                output_records=100,
            )
            tracker1.save(path)
            
            tracker2 = LineageTracker.load(path)
            
            assert tracker2.pipeline_id == "test"
            assert len(tracker2._records) == 1

    def test_export_graph(self):
        """Test exporting lineage graph."""
        tracker = LineageTracker()
        
        tracker.record(
            source_type=SourceType.FILE,
            source_location="/a",
            transformation="t1",
            destination_type=SourceType.FILE,
            destination_location="/b",
            input_records=100,
            output_records=100,
        )
        
        graph = tracker.export_graph()
        
        assert "nodes" in graph
        assert "edges" in graph
        assert len(graph["nodes"]) >= 2
        assert len(graph["edges"]) >= 2
