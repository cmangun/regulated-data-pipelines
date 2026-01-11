"""
Data Quality Framework

Comprehensive data quality validation for healthcare data pipelines.
Supports schema validation, business rules, statistical checks, and quality scoring.
"""

from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


# =============================================================================
# QUALITY ENUMS
# =============================================================================


class Severity(str, Enum):
    """Severity level for quality issues."""
    ERROR = "error"      # Fails validation, blocks pipeline
    WARNING = "warning"  # Logged but doesn't block
    INFO = "info"        # Informational only


class QualityDimension(str, Enum):
    """Data quality dimensions (based on DAMA standards)."""
    ACCURACY = "accuracy"           # Data correctly represents reality
    COMPLETENESS = "completeness"   # Required data is present
    CONSISTENCY = "consistency"     # Data agrees across sources
    TIMELINESS = "timeliness"       # Data is current
    VALIDITY = "validity"           # Data conforms to rules
    UNIQUENESS = "uniqueness"       # No unwanted duplicates


# =============================================================================
# VALIDATION RULES
# =============================================================================


@dataclass
class ValidationRule(ABC):
    """Base class for validation rules."""
    
    name: str
    description: str
    dimension: QualityDimension
    severity: Severity = Severity.ERROR
    
    @abstractmethod
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        """Validate a value against the rule."""
        pass


@dataclass
class NotNullRule(ValidationRule):
    """Ensure value is not null/None."""
    
    dimension: QualityDimension = QualityDimension.COMPLETENESS
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        return value is not None


@dataclass
class NotEmptyRule(ValidationRule):
    """Ensure string value is not empty."""
    
    dimension: QualityDimension = QualityDimension.COMPLETENESS
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        if value is None:
            return False
        if isinstance(value, str):
            return len(value.strip()) > 0
        return True


@dataclass
class RangeRule(ValidationRule):
    """Ensure numeric value is within range."""
    
    min_value: float | None = None
    max_value: float | None = None
    dimension: QualityDimension = QualityDimension.VALIDITY
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        if value is None:
            return True  # Null check is separate
        try:
            num = float(value)
            if self.min_value is not None and num < self.min_value:
                return False
            if self.max_value is not None and num > self.max_value:
                return False
            return True
        except (TypeError, ValueError):
            return False


@dataclass
class RegexRule(ValidationRule):
    """Ensure value matches regex pattern."""
    
    pattern: str = ""
    dimension: QualityDimension = QualityDimension.VALIDITY
    
    def __post_init__(self) -> None:
        self._compiled = re.compile(self.pattern) if self.pattern else None
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        if value is None:
            return True
        if self._compiled is None:
            return True
        return bool(self._compiled.match(str(value)))


@dataclass
class EnumRule(ValidationRule):
    """Ensure value is one of allowed values."""
    
    allowed_values: list[Any] = field(default_factory=list)
    case_sensitive: bool = True
    dimension: QualityDimension = QualityDimension.VALIDITY
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        if value is None:
            return True
        
        if self.case_sensitive:
            return value in self.allowed_values
        else:
            value_lower = str(value).lower()
            return value_lower in [str(v).lower() for v in self.allowed_values]


@dataclass
class UniqueRule(ValidationRule):
    """Track uniqueness across records."""
    
    dimension: QualityDimension = QualityDimension.UNIQUENESS
    _seen_values: set[Any] = field(default_factory=set, init=False)
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        if value is None:
            return True
        if value in self._seen_values:
            return False
        self._seen_values.add(value)
        return True
    
    def reset(self) -> None:
        """Reset seen values for new validation run."""
        self._seen_values.clear()


@dataclass
class DateRangeRule(ValidationRule):
    """Ensure date is within valid range."""
    
    min_date: datetime | None = None
    max_date: datetime | None = None
    dimension: QualityDimension = QualityDimension.VALIDITY
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        if value is None:
            return True
        
        if isinstance(value, str):
            try:
                value = datetime.fromisoformat(value)
            except ValueError:
                return False
        
        if not isinstance(value, datetime):
            return False
        
        if self.min_date and value < self.min_date:
            return False
        if self.max_date and value > self.max_date:
            return False
        
        return True


@dataclass
class CustomRule(ValidationRule):
    """Custom validation with callable function."""
    
    validator: Callable[[Any, dict[str, Any] | None], bool] = field(default=lambda v, c: True)
    dimension: QualityDimension = QualityDimension.VALIDITY
    
    def validate(self, value: Any, context: dict[str, Any] | None = None) -> bool:
        return self.validator(value, context)


# =============================================================================
# VALIDATION RESULTS
# =============================================================================


class QualityIssue(BaseModel):
    """A single data quality issue."""
    
    field: str
    rule_name: str
    dimension: QualityDimension
    severity: Severity
    message: str
    value: Any = None
    row_index: int | None = None
    
    class Config:
        use_enum_values = True


class FieldQualityReport(BaseModel):
    """Quality report for a single field."""
    
    field: str
    total_records: int
    null_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    issues: list[QualityIssue] = Field(default_factory=list)
    
    @property
    def completeness_rate(self) -> float:
        """Percentage of non-null values."""
        if self.total_records == 0:
            return 0.0
        return (self.total_records - self.null_count) / self.total_records
    
    @property
    def validity_rate(self) -> float:
        """Percentage of valid values."""
        non_null = self.total_records - self.null_count
        if non_null == 0:
            return 1.0  # All null is 100% valid (nullness is separate)
        return self.valid_count / non_null


class DataQualityReport(BaseModel):
    """Comprehensive data quality report."""
    
    dataset_name: str
    total_records: int
    valid_records: int = 0
    invalid_records: int = 0
    
    field_reports: dict[str, FieldQualityReport] = Field(default_factory=dict)
    issues: list[QualityIssue] = Field(default_factory=list)
    
    # Dimension scores
    dimension_scores: dict[str, float] = Field(default_factory=dict)
    
    # Timestamps
    validated_at: datetime = Field(default_factory=datetime.utcnow)
    duration_ms: int = 0
    
    @property
    def overall_score(self) -> float:
        """Calculate overall quality score (0-100)."""
        if not self.dimension_scores:
            return 100.0 if self.invalid_records == 0 else 0.0
        return sum(self.dimension_scores.values()) / len(self.dimension_scores)
    
    @property
    def has_errors(self) -> bool:
        """Check if any error-level issues exist."""
        return any(i.severity == Severity.ERROR for i in self.issues)
    
    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.ERROR)
    
    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == Severity.WARNING)


# =============================================================================
# DATA QUALITY VALIDATOR
# =============================================================================


class DataQualityValidator:
    """
    Validate data quality against defined rules.
    
    Supports:
    - Field-level validation rules
    - Cross-field validation
    - Statistical checks
    - Quality scoring by dimension
    """
    
    def __init__(self, dataset_name: str = "dataset"):
        self.dataset_name = dataset_name
        self.field_rules: dict[str, list[ValidationRule]] = {}
        self.row_rules: list[Callable[[dict[str, Any]], list[QualityIssue]]] = []
    
    def add_field_rule(self, field: str, rule: ValidationRule) -> "DataQualityValidator":
        """Add a validation rule for a field."""
        if field not in self.field_rules:
            self.field_rules[field] = []
        self.field_rules[field].append(rule)
        return self
    
    def add_row_rule(
        self,
        rule: Callable[[dict[str, Any]], list[QualityIssue]],
    ) -> "DataQualityValidator":
        """Add a cross-field validation rule."""
        self.row_rules.append(rule)
        return self
    
    def validate_record(
        self,
        record: dict[str, Any],
        row_index: int | None = None,
    ) -> list[QualityIssue]:
        """Validate a single record."""
        issues: list[QualityIssue] = []
        
        # Field-level validation
        for field_name, rules in self.field_rules.items():
            value = record.get(field_name)
            
            for rule in rules:
                if not rule.validate(value, record):
                    issues.append(QualityIssue(
                        field=field_name,
                        rule_name=rule.name,
                        dimension=rule.dimension,
                        severity=rule.severity,
                        message=f"{rule.description}: {rule.name} failed",
                        value=value,
                        row_index=row_index,
                    ))
        
        # Row-level validation
        for row_rule in self.row_rules:
            row_issues = row_rule(record)
            for issue in row_issues:
                issue.row_index = row_index
            issues.extend(row_issues)
        
        return issues
    
    def validate_batch(
        self,
        records: list[dict[str, Any]],
    ) -> DataQualityReport:
        """Validate a batch of records."""
        import time
        start_time = time.time()
        
        all_issues: list[QualityIssue] = []
        valid_count = 0
        invalid_count = 0
        
        # Initialize field reports
        field_reports: dict[str, FieldQualityReport] = {}
        for field in self.field_rules:
            field_reports[field] = FieldQualityReport(
                field=field,
                total_records=len(records),
            )
        
        # Reset unique rules
        for rules in self.field_rules.values():
            for rule in rules:
                if isinstance(rule, UniqueRule):
                    rule.reset()
        
        # Validate each record
        for idx, record in enumerate(records):
            record_issues = self.validate_record(record, idx)
            
            if any(i.severity == Severity.ERROR for i in record_issues):
                invalid_count += 1
            else:
                valid_count += 1
            
            # Update field reports
            for field in self.field_rules:
                value = record.get(field)
                report = field_reports[field]
                
                if value is None:
                    report.null_count += 1
                else:
                    field_issues = [i for i in record_issues if i.field == field]
                    if field_issues:
                        report.invalid_count += 1
                        report.issues.extend(field_issues)
                    else:
                        report.valid_count += 1
            
            all_issues.extend(record_issues)
        
        # Calculate dimension scores
        dimension_scores = self._calculate_dimension_scores(
            records, field_reports, all_issues
        )
        
        duration_ms = int((time.time() - start_time) * 1000)
        
        return DataQualityReport(
            dataset_name=self.dataset_name,
            total_records=len(records),
            valid_records=valid_count,
            invalid_records=invalid_count,
            field_reports=field_reports,
            issues=all_issues,
            dimension_scores=dimension_scores,
            duration_ms=duration_ms,
        )
    
    def _calculate_dimension_scores(
        self,
        records: list[dict[str, Any]],
        field_reports: dict[str, FieldQualityReport],
        issues: list[QualityIssue],
    ) -> dict[str, float]:
        """Calculate quality scores by dimension."""
        scores: dict[str, float] = {}
        
        # Completeness: average of non-null rates
        if field_reports:
            completeness_rates = [r.completeness_rate for r in field_reports.values()]
            scores[QualityDimension.COMPLETENESS.value] = (
                sum(completeness_rates) / len(completeness_rates) * 100
            )
        
        # Validity: percentage of records without validity errors
        validity_issues = [i for i in issues if i.dimension == QualityDimension.VALIDITY]
        if records:
            affected_rows = len(set(i.row_index for i in validity_issues if i.row_index is not None))
            scores[QualityDimension.VALIDITY.value] = (
                (len(records) - affected_rows) / len(records) * 100
            )
        
        # Uniqueness
        uniqueness_issues = [i for i in issues if i.dimension == QualityDimension.UNIQUENESS]
        if records:
            scores[QualityDimension.UNIQUENESS.value] = (
                (len(records) - len(uniqueness_issues)) / len(records) * 100
            )
        
        return scores


# =============================================================================
# COMMON HEALTHCARE VALIDATORS
# =============================================================================


def create_patient_validator() -> DataQualityValidator:
    """Create a validator with common patient data rules."""
    validator = DataQualityValidator("patient_data")
    
    # Required fields
    validator.add_field_rule("patient_id", NotNullRule(
        name="patient_id_required",
        description="Patient ID is required",
    ))
    validator.add_field_rule("mrn", NotEmptyRule(
        name="mrn_required",
        description="MRN is required",
    ))
    validator.add_field_rule("first_name", NotEmptyRule(
        name="first_name_required",
        description="First name is required",
    ))
    validator.add_field_rule("last_name", NotEmptyRule(
        name="last_name_required",
        description="Last name is required",
    ))
    validator.add_field_rule("date_of_birth", NotNullRule(
        name="dob_required",
        description="Date of birth is required",
    ))
    
    # Format validations
    validator.add_field_rule("ssn", RegexRule(
        name="ssn_format",
        description="SSN format validation",
        pattern=r"^\d{3}-\d{2}-\d{4}$",
        severity=Severity.WARNING,
    ))
    validator.add_field_rule("email", RegexRule(
        name="email_format",
        description="Email format validation",
        pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        severity=Severity.WARNING,
    ))
    validator.add_field_rule("zip_code", RegexRule(
        name="zip_format",
        description="ZIP code format",
        pattern=r"^\d{5}(-\d{4})?$",
        severity=Severity.WARNING,
    ))
    
    # Enum validations
    validator.add_field_rule("gender", EnumRule(
        name="gender_valid",
        description="Gender must be valid value",
        allowed_values=["male", "female", "other", "unknown"],
        case_sensitive=False,
    ))
    validator.add_field_rule("state", RegexRule(
        name="state_format",
        description="State must be 2-letter code",
        pattern=r"^[A-Z]{2}$",
        severity=Severity.WARNING,
    ))
    
    # Uniqueness
    validator.add_field_rule("patient_id", UniqueRule(
        name="patient_id_unique",
        description="Patient ID must be unique",
    ))
    validator.add_field_rule("mrn", UniqueRule(
        name="mrn_unique",
        description="MRN must be unique",
    ))
    
    return validator


def create_encounter_validator() -> DataQualityValidator:
    """Create a validator with common encounter data rules."""
    validator = DataQualityValidator("encounter_data")
    
    # Required fields
    validator.add_field_rule("encounter_id", NotNullRule(
        name="encounter_id_required",
        description="Encounter ID is required",
    ))
    validator.add_field_rule("patient_id", NotNullRule(
        name="patient_id_required",
        description="Patient ID is required",
    ))
    validator.add_field_rule("encounter_type", NotEmptyRule(
        name="type_required",
        description="Encounter type is required",
    ))
    
    # Enum validations
    validator.add_field_rule("encounter_type", EnumRule(
        name="encounter_type_valid",
        description="Encounter type must be valid",
        allowed_values=["inpatient", "outpatient", "emergency", "telehealth", "home_health"],
        case_sensitive=False,
    ))
    validator.add_field_rule("status", EnumRule(
        name="status_valid",
        description="Status must be valid",
        allowed_values=["planned", "arrived", "in_progress", "finished", "cancelled"],
        case_sensitive=False,
    ))
    
    # Uniqueness
    validator.add_field_rule("encounter_id", UniqueRule(
        name="encounter_id_unique",
        description="Encounter ID must be unique",
    ))
    
    # Cross-field validation: end time after start time
    def validate_timing(record: dict[str, Any]) -> list[QualityIssue]:
        issues = []
        start = record.get("actual_start")
        end = record.get("actual_end")
        
        if start and end:
            if isinstance(start, str):
                start = datetime.fromisoformat(start)
            if isinstance(end, str):
                end = datetime.fromisoformat(end)
            
            if end < start:
                issues.append(QualityIssue(
                    field="actual_end",
                    rule_name="end_after_start",
                    dimension=QualityDimension.CONSISTENCY,
                    severity=Severity.ERROR,
                    message="End time must be after start time",
                    value={"start": str(start), "end": str(end)},
                ))
        
        return issues
    
    validator.add_row_rule(validate_timing)
    
    return validator


def create_lab_result_validator() -> DataQualityValidator:
    """Create a validator for lab results."""
    validator = DataQualityValidator("lab_results")
    
    # Required fields
    validator.add_field_rule("result_id", NotNullRule(
        name="result_id_required",
        description="Result ID is required",
    ))
    validator.add_field_rule("patient_id", NotNullRule(
        name="patient_id_required",
        description="Patient ID is required",
    ))
    validator.add_field_rule("loinc_code", NotEmptyRule(
        name="loinc_required",
        description="LOINC code is required",
    ))
    validator.add_field_rule("test_name", NotEmptyRule(
        name="test_name_required",
        description="Test name is required",
    ))
    validator.add_field_rule("collected_at", NotNullRule(
        name="collected_at_required",
        description="Collection date is required",
    ))
    
    # Status validation
    validator.add_field_rule("status", EnumRule(
        name="status_valid",
        description="Status must be valid",
        allowed_values=["pending", "preliminary", "final", "corrected", "cancelled"],
        case_sensitive=False,
    ))
    
    # Uniqueness
    validator.add_field_rule("result_id", UniqueRule(
        name="result_id_unique",
        description="Result ID must be unique",
    ))
    
    return validator


# =============================================================================
# EXPORTS
# =============================================================================


__all__ = [
    # Enums
    "Severity",
    "QualityDimension",
    # Rules
    "ValidationRule",
    "NotNullRule",
    "NotEmptyRule",
    "RangeRule",
    "RegexRule",
    "EnumRule",
    "UniqueRule",
    "DateRangeRule",
    "CustomRule",
    # Results
    "QualityIssue",
    "FieldQualityReport",
    "DataQualityReport",
    # Validator
    "DataQualityValidator",
    # Factories
    "create_patient_validator",
    "create_encounter_validator",
    "create_lab_result_validator",
]
