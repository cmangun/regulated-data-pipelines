# Regulated Data Pipelines

[![CI](https://github.com/cmangun/regulated-data-pipelines/actions/workflows/ci.yml/badge.svg)](https://github.com/cmangun/regulated-data-pipelines/actions/workflows/ci.yml)

Compliant ETL patterns for regulated environments with built-in audit logging and lineage tracking.

## Overview

This library provides production-grade ETL patterns designed for regulated industries (healthcare, finance, pharmaceuticals):

- **Audit Logging**: Immutable, append-only logs of all operations
- **Data Lineage**: Track data from source to destination
- **Schema Validation**: Pydantic-based data validation
- **Deterministic Transforms**: Reproducible, testable transformations

## Quickstart

```bash
# Clone
git clone https://github.com/cmangun/regulated-data-pipelines.git
cd regulated-data-pipelines

# Setup virtual environment
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows

# Install
pip install -e ".[dev]"

# Run tests
pytest

# Run sample pipeline
python -m pipeline.run \
  --input data/sample_input.csv \
  --output out/output.csv \
  --audit out/audit.jsonl
```

## Usage

### CLI

```bash
# Basic pipeline run
pipeline --input data/input.csv --output data/output.csv --audit logs/audit.jsonl
```

### Python API

```python
from pathlib import Path
from pipeline import run_pipeline, PipelineConfig

config = PipelineConfig(
    input_path=Path("data/input.csv"),
    output_path=Path("data/output.csv"),
    audit_path=Path("logs/audit.jsonl"),
)

result = run_pipeline(config)

print(f"Success: {result.success}")
print(f"Records: {result.input_records} → {result.output_records}")
print(f"Hash: {result.output_hash}")
```

### Audit Logging

```python
from pipeline import AuditLogger

audit = AuditLogger("logs/audit.jsonl")

# Log operations
audit.log_start("pipeline-123", "read", "read_csv")
audit.log_complete("pipeline-123", "read", "read_csv", record_count=1000)

# Read audit trail
entries = audit.read_all()
for entry in entries:
    print(f"{entry.timestamp}: {entry.action} - {entry.status}")
```

### Lineage Tracking

```python
from pipeline import LineageTracker

lineage = LineageTracker()

# Record transformation
record = lineage.record(
    source_type="file",
    source_location="s3://bucket/input.csv",
    source_hash="abc123...",
    transformation="normalize",
    destination_type="file",
    destination_location="s3://bucket/output.csv",
    destination_hash="def456...",
    input_records=1000,
    output_records=950,
    records_filtered=50,
)

# Query lineage
ancestors = lineage.get_ancestors(record.lineage_id)
summary = lineage.summary()
```

## Audit Log Format

Audit logs are stored as JSON Lines (JSONL) for immutability and streaming:

```jsonl
{"entry_id":"abc-123","timestamp":"2024-01-15T10:30:00Z","pipeline_id":"p-001","stage":"read","action":"read_csv","status":"started"}
{"entry_id":"def-456","timestamp":"2024-01-15T10:30:01Z","pipeline_id":"p-001","stage":"read","action":"read_csv","status":"completed","record_count":1000}
```

## Compliance Features

| Feature | Description |
|---------|-------------|
| Immutable Logs | Append-only audit trail |
| Data Hashing | SHA-256 checksums for all files |
| Lineage Tracking | Full source-to-destination tracking |
| Schema Validation | Pydantic-based input validation |
| Deterministic | Same input = same output |

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Runner                          │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────────┐    │
│  │  Read   │─▶│Validate │─▶│Transform│─▶│   Write     │    │
│  └────┬────┘  └────┬────┘  └────┬────┘  └──────┬──────┘    │
│       │            │            │              │            │
│       ▼            ▼            ▼              ▼            │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Audit Logger (append-only)              │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │              Lineage Tracker (DAG)                   │   │
│  └─────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Next Iterations

- [ ] Add streaming/batch mode selection
- [ ] Add database source/sink support
- [ ] Add S3/GCS/Azure Blob connectors
- [ ] Add cryptographic audit log signing
- [ ] Add OpenLineage integration
- [ ] Add data quality rules engine

## License

MIT © Christopher Mangun

---

**Portfolio**: [field-deployed-engineer.vercel.app](https://field-deployed-engineer.vercel.app/)  
**Contact**: Christopher Mangun — Brooklyn, NY
