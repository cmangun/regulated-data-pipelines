# Regulated Data Pipelines

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-106%20passed-brightgreen.svg)](tests/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![HIPAA Compliant](https://img.shields.io/badge/HIPAA-Compliant-green.svg)](docs/compliance.md)

**Production-grade healthcare ETL patterns with PII detection, audit logging, data quality validation, and lineage tracking for HIPAA-compliant environments.**

## Overview

This library provides enterprise-ready data pipeline components designed for healthcare and pharmaceutical environments where regulatory compliance (HIPAA, FDA 21 CFR Part 11) is mandatory. Every component includes built-in audit trails, data lineage tracking, and compliance-ready documentation.

### Key Features

- **Healthcare Data Models** - Pydantic models for Patient, Encounter, Lab Results, and Claims with built-in validation
- **PII Detection & Masking** - Automated detection of 18+ HIPAA Safe Harbor identifiers with multiple masking strategies
- **Safe Harbor De-identification** - HIPAA-compliant de-identification following Safe Harbor guidelines
- **Data Quality Framework** - Configurable validation rules with quality dimension scoring (DAMA standards)
- **Transform Pipeline** - Composable, audited data transformations with full lineage
- **Audit Logging** - Tamper-evident, hash-chained audit logs with integrity verification
- **Lineage Tracking** - Complete data provenance with impact analysis capabilities

## Installation

```bash
# Basic installation
pip install -e .

# With development dependencies
pip install -e ".[dev]"

# With database connectors
pip install -e ".[connectors]"
```

## Quick Start

### Healthcare Data Models

```python
from datetime import date
from pipeline import Patient, Gender, Encounter, EncounterType

# Create a validated patient record
patient = Patient(
    mrn="MRN001",
    ssn="123-45-6789",  # Auto-normalized to XXX-XX-XXXX
    first_name="John",
    last_name="Doe",
    date_of_birth=date(1990, 5, 15),
    gender=Gender.MALE,
    email="john.doe@example.com",  # Validated and lowercased
    phone="5551234567",  # Auto-formatted to (555) 123-4567
    zip_code="123456789",  # Auto-formatted to 12345-6789
)

# Access computed properties
print(f"Age: {patient.age}")
print(f"Full Name: {patient.full_name}")

# Get PHI fields (for compliance tracking)
phi_fields = patient.phi_fields()
# ['mrn', 'ssn', 'first_name', 'last_name', 'date_of_birth', ...]

# Export with PHI redacted
safe_data = patient.to_safe_dict()
# {'mrn': '[REDACTED]', 'ssn': '[REDACTED]', 'gender': 'male', ...}
```

### PII Detection

```python
from pipeline import PIIDetector, PIIMasker, MaskingStrategy

detector = PIIDetector()
masker = PIIMasker(default_strategy=MaskingStrategy.PARTIAL)

# Scan text for PII
text = "Patient John Smith (SSN: 123-45-6789) can be reached at john@example.com"
result = detector.scan_text(text)

print(f"PII Found: {result.has_pii}")
print(f"Types: {result.pii_types_found}")
# Types: {PIIType.SSN, PIIType.EMAIL}

# Mask detected PII
masked_text = masker.mask_text(text, result.matches)
# "Patient John Smith (SSN: ***-**-6789) can be reached at j***@example.com"
```

### HIPAA Safe Harbor De-identification

```python
from pipeline import SafeHarborDeidentifier

deidentifier = SafeHarborDeidentifier()

patient_data = {
    "first_name": "John",
    "last_name": "Doe",
    "date_of_birth": date(1990, 6, 15),
    "zip_code": "12345-6789",
    "ssn": "123-45-6789",
    "gender": "male",  # Not PHI - preserved
}

safe_data = deidentifier.deidentify_patient(patient_data)
# {
#     "first_name": "[REDACTED]",
#     "last_name": "[REDACTED]",
#     "date_of_birth": 1990,  # Year only
#     "zip_code": "12300",    # First 3 digits + 00
#     "ssn": "[REDACTED]",
#     "gender": "male",       # Preserved (not PHI)
# }
```

### Data Quality Validation

```python
from pipeline import (
    DataQualityValidator,
    NotNullRule,
    RegexRule,
    EnumRule,
    create_patient_validator,
)

# Use pre-built healthcare validators
validator = create_patient_validator()

records = [
    {"patient_id": "P001", "mrn": "MRN001", "first_name": "John", ...},
    {"patient_id": None, "mrn": "", "first_name": "", ...},  # Invalid
]

report = validator.validate_batch(records)

print(f"Total Records: {report.total_records}")
print(f"Valid Records: {report.valid_records}")
print(f"Overall Score: {report.overall_score:.1f}%")
print(f"Completeness: {report.dimension_scores['completeness']:.1f}%")
```

### Transform Pipeline

```python
from pipeline import (
    TransformPipeline,
    FilterTransform,
    AgeCalculatorTransform,
    ICD10ValidatorTransform,
    SelectTransform,
)

# Build a composable pipeline
pipeline = TransformPipeline(name="patient_etl")

pipeline.add(FilterTransform(
    name="filter_active",
    predicate=lambda r: r.get("is_active", False),
))
pipeline.add(AgeCalculatorTransform(
    name="calculate_age",
))
pipeline.add(ICD10ValidatorTransform(
    name="validate_diagnoses",
    code_field="primary_diagnosis",
))
pipeline.add(SelectTransform(
    name="select_output_fields",
    fields=["patient_id", "age", "primary_diagnosis", "is_valid_icd10"],
))

# Execute with full audit trail
result = pipeline.execute(patient_records)

print(f"Input: {result.input_count}, Output: {result.output_count}")
print(f"Duration: {result.duration_ms}ms")

# Get lineage for compliance
lineage = pipeline.get_lineage()
```

### Audit Logging

```python
from pipeline import AuditLogger, AuditAction

# Create tamper-evident audit log
logger = AuditLogger(
    audit_path="/var/log/pipeline/audit.jsonl",
    pipeline_id="patient_etl_001",
    user_id="system_service",
)

# Log pipeline lifecycle
logger.log_pipeline_start(details={"source": "ehr_export.csv"})
logger.log_data_read("ehr_export.csv", record_count=10000)
logger.log_transform("filter", input_count=10000, output_count=8500)
logger.log_phi_access("patient_batch", access_reason="treatment", 
                      fields_accessed=["ssn", "dob"])
logger.log_data_write("warehouse.patients", record_count=8500)
logger.log_pipeline_complete(record_count=8500, duration_ms=45000)

# Verify audit log integrity
is_valid, errors = logger.verify_chain_integrity()
assert is_valid, f"Audit log tampered: {errors}"
```

### Data Lineage

```python
from pipeline import LineageTracker, SourceType

tracker = LineageTracker(pipeline_id="patient_etl")

# Record transformations
tracker.record(
    source_type=SourceType.FILE,
    source_location="/data/raw/patients.csv",
    transformation="clean_and_validate",
    destination_type=SourceType.DATABASE,
    destination_location="warehouse.patients_staging",
    input_records=10000,
    output_records=9500,
    records_filtered=500,
    source_hash="abc123",
    destination_hash="def456",
)

# Impact analysis - what's affected if source changes?
impact = tracker.impact_analysis("/data/raw/patients.csv")
print(f"Affected destinations: {impact['affected_destinations']}")

# Export lineage graph
graph = tracker.export_graph()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA PIPELINE                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌────────────┐  │
│   │   Source    │───▶│  Transform  │───▶│   Quality   │───▶│   Sink     │  │
│   │   Reader    │    │   Pipeline  │    │   Validate  │    │   Writer   │  │
│   └─────────────┘    └─────────────┘    └─────────────┘    └────────────┘  │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                         AUDIT LOGGER                                 │  │
│   │              (Hash-chained, Tamper-evident Logs)                    │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│   ┌─────────────────────────────────────────────────────────────────────┐  │
│   │                        LINEAGE TRACKER                               │  │
│   │              (Data Provenance & Impact Analysis)                    │  │
│   └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Module Reference

### Models (`pipeline.models`)

| Model | Description | Key Validations |
|-------|-------------|-----------------|
| `Patient` | Patient demographics | SSN format, phone normalization, email validation, ZIP code format |
| `Encounter` | Clinical encounters | ICD-10 diagnosis codes, CPT procedure codes, timing validation |
| `LabResult` | Laboratory results | LOINC codes, reference range abnormal detection |
| `Claim` | Insurance claims | NPI Luhn validation, claim status workflow |

### PII Detection (`pipeline.pii`)

| Class | Description |
|-------|-------------|
| `PIIDetector` | Scans text/data for 18+ PII types with confidence scoring |
| `PIIMasker` | Masks PII with configurable strategies (redact, asterisk, hash, partial, token) |
| `SafeHarborDeidentifier` | HIPAA Safe Harbor compliant de-identification |

### Quality (`pipeline.quality`)

| Rule | Description |
|------|-------------|
| `NotNullRule` | Validates field is not null |
| `NotEmptyRule` | Validates string is not empty/whitespace |
| `RangeRule` | Validates numeric range (min/max) |
| `RegexRule` | Validates against regex pattern |
| `EnumRule` | Validates against allowed values |
| `UniqueRule` | Validates uniqueness (detects duplicates) |
| `DateRangeRule` | Validates date range |
| `CustomRule` | Custom validation function |

### Transforms (`pipeline.transforms`)

| Transform | Description |
|-----------|-------------|
| `FilterTransform` | Filter records by predicate |
| `MapTransform` | Apply mapping function to records |
| `SelectTransform` | Select/rename fields |
| `DeriveTransform` | Derive new fields |
| `DeduplicateTransform` | Remove duplicates by key |
| `AgeCalculatorTransform` | Calculate age from DOB |
| `ICD10ValidatorTransform` | Validate ICD-10 codes |
| `NPIValidatorTransform` | Validate NPI (Luhn algorithm) |

## Compliance Features

### HIPAA Safe Harbor (18 Identifiers)

This library tracks and can redact all 18 HIPAA Safe Harbor identifiers:

1. Names
2. Geographic data (smaller than state)
3. Dates (except year) for ages under 90
4. Phone numbers
5. Fax numbers
6. Email addresses
7. Social Security numbers
8. Medical record numbers
9. Health plan beneficiary numbers
10. Account numbers
11. Certificate/license numbers
12. Vehicle identifiers
13. Device identifiers
14. Web URLs
15. IP addresses
16. Biometric identifiers
17. Full-face photographs
18. Any other unique identifier

### Audit Trail Requirements

- **Immutable logs**: Append-only JSONL format
- **Hash chain**: Each entry links to previous via SHA-256
- **Integrity verification**: Detect tampering at any point
- **PHI access logging**: Track all access to protected data
- **Retention support**: Export to CSV for long-term storage

## Development

```bash
# Clone repository
git clone https://github.com/cmangun/regulated-data-pipelines.git
cd regulated-data-pipelines

# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run tests with coverage
pytest tests/ --cov=pipeline --cov-report=html

# Type checking
mypy src/

# Linting
ruff check src/ tests/
```

## Project Structure

```
regulated-data-pipelines/
├── src/pipeline/
│   ├── __init__.py          # Package exports
│   ├── audit.py             # Audit logging with hash chains
│   ├── lineage.py           # Data lineage tracking
│   ├── models/
│   │   ├── __init__.py
│   │   └── healthcare.py    # Patient, Encounter, Claim models
│   ├── pii/
│   │   ├── __init__.py
│   │   └── detector.py      # PII detection and masking
│   ├── quality/
│   │   ├── __init__.py
│   │   └── validator.py     # Data quality framework
│   └── transforms/
│       ├── __init__.py
│       └── etl.py           # Transform pipeline
├── tests/
│   ├── test_models.py       # 20 tests
│   ├── test_pii.py          # 28 tests
│   ├── test_quality.py      # 27 tests
│   └── test_transforms_audit.py  # 31 tests
├── pyproject.toml
└── README.md
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

**Christopher Mangun**  
Healthcare AI Consultant  
[healthcare-ai-consultant.com](https://healthcare-ai-consultant.com)

---

*Built for regulated healthcare environments where compliance is non-negotiable.*
