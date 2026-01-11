"""
Regulated Data Pipelines

HIPAA-compliant ETL patterns with PII detection, audit logging,
data quality validation, and lineage tracking for healthcare environments.

Features:
- Healthcare data models (Patient, Encounter, LabResult, Claim)
- PII detection and Safe Harbor de-identification
- Data quality validation framework
- Transformation pipelines with audit trail
- Hash-chain audit logging with integrity verification
- Full data lineage tracking
"""

__version__ = "1.0.0"
__author__ = "Christopher Mangun"
__email__ = "cmangun@gmail.com"

# Audit and Lineage
from .audit import (
    AuditAction,
    AuditLevel,
    AuditEntry,
    AuditLogger,
)
from .lineage import (
    NodeType,
    SourceType,
    LineageNode,
    LineageEdge,
    LineageRecord,
    LineageTracker,
)

# Models
from .models import (
    Gender,
    EncounterType,
    EncounterStatus,
    LabStatus,
    ClaimStatus,
    HealthcareBaseModel,
    Patient,
    Diagnosis,
    Procedure,
    Encounter,
    LabResult,
    ClaimLine,
    Claim,
)

# PII
from .pii import (
    PIIType,
    PIIPattern,
    PIIMatch,
    PIIDetectionResult,
    PIIDetector,
    MaskingStrategy,
    PIIMasker,
    SafeHarborDeidentifier,
)

# Quality
from .quality import (
    Severity,
    QualityDimension,
    ValidationRule,
    NotNullRule,
    NotEmptyRule,
    RangeRule,
    RegexRule,
    EnumRule,
    UniqueRule,
    DateRangeRule,
    CustomRule,
    QualityIssue,
    FieldQualityReport,
    DataQualityReport,
    DataQualityValidator,
    create_patient_validator,
    create_encounter_validator,
    create_lab_result_validator,
)

# Transforms
from .transforms import (
    TransformResult,
    BaseTransform,
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


__all__ = [
    # Version
    "__version__",
    # Audit
    "AuditAction",
    "AuditLevel",
    "AuditEntry",
    "AuditLogger",
    # Lineage
    "NodeType",
    "SourceType",
    "LineageNode",
    "LineageEdge",
    "LineageRecord",
    "LineageTracker",
    # Models
    "Gender",
    "EncounterType",
    "EncounterStatus",
    "LabStatus",
    "ClaimStatus",
    "HealthcareBaseModel",
    "Patient",
    "Diagnosis",
    "Procedure",
    "Encounter",
    "LabResult",
    "ClaimLine",
    "Claim",
    # PII
    "PIIType",
    "PIIPattern",
    "PIIMatch",
    "PIIDetectionResult",
    "PIIDetector",
    "MaskingStrategy",
    "PIIMasker",
    "SafeHarborDeidentifier",
    # Quality
    "Severity",
    "QualityDimension",
    "ValidationRule",
    "NotNullRule",
    "NotEmptyRule",
    "RangeRule",
    "RegexRule",
    "EnumRule",
    "UniqueRule",
    "DateRangeRule",
    "CustomRule",
    "QualityIssue",
    "FieldQualityReport",
    "DataQualityReport",
    "DataQualityValidator",
    "create_patient_validator",
    "create_encounter_validator",
    "create_lab_result_validator",
    # Transforms
    "TransformResult",
    "BaseTransform",
    "FilterTransform",
    "MapTransform",
    "SelectTransform",
    "DeriveTransform",
    "DeduplicateTransform",
    "AgeCalculatorTransform",
    "ICD10ValidatorTransform",
    "NPIValidatorTransform",
    "TransformPipeline",
]
