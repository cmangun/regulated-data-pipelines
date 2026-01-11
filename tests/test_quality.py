"""Tests for data quality validation framework."""

from datetime import datetime

import pytest

from pipeline.quality import (
    Severity,
    QualityDimension,
    NotNullRule,
    NotEmptyRule,
    RangeRule,
    RegexRule,
    EnumRule,
    UniqueRule,
    DateRangeRule,
    CustomRule,
    DataQualityValidator,
    create_patient_validator,
    create_encounter_validator,
)


# =============================================================================
# VALIDATION RULE TESTS
# =============================================================================


class TestNotNullRule:
    """Tests for NotNullRule."""

    def test_valid_value(self):
        """Test with non-null value."""
        rule = NotNullRule(name="test", description="Test rule")
        
        assert rule.validate("value") is True
        assert rule.validate(0) is True
        assert rule.validate("") is True

    def test_null_value(self):
        """Test with null value."""
        rule = NotNullRule(name="test", description="Test rule")
        
        assert rule.validate(None) is False


class TestNotEmptyRule:
    """Tests for NotEmptyRule."""

    def test_valid_value(self):
        """Test with non-empty value."""
        rule = NotEmptyRule(name="test", description="Test rule")
        
        assert rule.validate("hello") is True
        assert rule.validate("  hello  ") is True

    def test_empty_value(self):
        """Test with empty value."""
        rule = NotEmptyRule(name="test", description="Test rule")
        
        assert rule.validate("") is False
        assert rule.validate("   ") is False
        assert rule.validate(None) is False


class TestRangeRule:
    """Tests for RangeRule."""

    def test_within_range(self):
        """Test value within range."""
        rule = RangeRule(
            name="test", description="Test rule",
            min_value=0, max_value=100,
        )
        
        assert rule.validate(50) is True
        assert rule.validate(0) is True
        assert rule.validate(100) is True

    def test_outside_range(self):
        """Test value outside range."""
        rule = RangeRule(
            name="test", description="Test rule",
            min_value=0, max_value=100,
        )
        
        assert rule.validate(-1) is False
        assert rule.validate(101) is False

    def test_null_allowed(self):
        """Test that null values are allowed."""
        rule = RangeRule(
            name="test", description="Test rule",
            min_value=0, max_value=100,
        )
        assert rule.validate(None) is True


class TestRegexRule:
    """Tests for RegexRule."""

    def test_matching_pattern(self):
        """Test value matching pattern."""
        rule = RegexRule(
            name="test", description="Test rule",
            pattern=r"^\d{5}$",
        )
        assert rule.validate("12345") is True

    def test_non_matching_pattern(self):
        """Test value not matching pattern."""
        rule = RegexRule(
            name="test", description="Test rule",
            pattern=r"^\d{5}$",
        )
        assert rule.validate("1234") is False
        assert rule.validate("123456") is False
        assert rule.validate("abcde") is False


class TestEnumRule:
    """Tests for EnumRule."""

    def test_valid_enum(self):
        """Test valid enum value."""
        rule = EnumRule(
            name="test", description="Test rule",
            allowed_values=["A", "B", "C"],
        )
        assert rule.validate("A") is True
        assert rule.validate("B") is True

    def test_invalid_enum(self):
        """Test invalid enum value."""
        rule = EnumRule(
            name="test", description="Test rule",
            allowed_values=["A", "B", "C"],
        )
        assert rule.validate("D") is False

    def test_case_insensitive(self):
        """Test case-insensitive enum."""
        rule = EnumRule(
            name="test", description="Test rule",
            allowed_values=["Male", "Female"],
            case_sensitive=False,
        )
        assert rule.validate("male") is True
        assert rule.validate("FEMALE") is True


class TestUniqueRule:
    """Tests for UniqueRule."""

    def test_unique_values(self):
        """Test unique values."""
        rule = UniqueRule(name="test", description="Test rule")
        
        assert rule.validate("A") is True
        assert rule.validate("B") is True
        assert rule.validate("C") is True

    def test_duplicate_values(self):
        """Test duplicate detection."""
        rule = UniqueRule(name="test", description="Test rule")
        
        assert rule.validate("A") is True
        assert rule.validate("A") is False  # Duplicate

    def test_reset(self):
        """Test reset functionality."""
        rule = UniqueRule(name="test", description="Test rule")
        
        rule.validate("A")
        rule.reset()
        assert rule.validate("A") is True


class TestDateRangeRule:
    """Tests for DateRangeRule."""

    def test_within_range(self):
        """Test date within range."""
        rule = DateRangeRule(
            name="test", description="Test rule",
            min_date=datetime(2020, 1, 1),
            max_date=datetime(2024, 12, 31),
        )
        assert rule.validate(datetime(2022, 6, 15)) is True

    def test_outside_range(self):
        """Test date outside range."""
        rule = DateRangeRule(
            name="test", description="Test rule",
            min_date=datetime(2020, 1, 1),
            max_date=datetime(2024, 12, 31),
        )
        assert rule.validate(datetime(2019, 12, 31)) is False
        assert rule.validate(datetime(2025, 1, 1)) is False


class TestCustomRule:
    """Tests for CustomRule."""

    def test_custom_validator(self):
        """Test custom validation function."""
        def is_even(value, context):
            return value is None or value % 2 == 0
        
        rule = CustomRule(
            name="test", description="Test rule",
            validator=is_even,
        )
        
        assert rule.validate(2) is True
        assert rule.validate(4) is True
        assert rule.validate(3) is False


# =============================================================================
# DATA QUALITY VALIDATOR TESTS
# =============================================================================


class TestDataQualityValidator:
    """Tests for DataQualityValidator."""

    def test_validate_single_record(self):
        """Test validating a single record."""
        validator = DataQualityValidator("test")
        validator.add_field_rule("name", NotEmptyRule(
            name="name_required", description="Name required"
        ))
        
        # Valid record
        issues = validator.validate_record({"name": "John"})
        assert len(issues) == 0
        
        # Invalid record
        issues = validator.validate_record({"name": ""})
        assert len(issues) == 1
        assert issues[0].field == "name"

    def test_validate_batch(self):
        """Test batch validation."""
        validator = DataQualityValidator("test")
        validator.add_field_rule("id", NotNullRule(
            name="id_required", description="ID required"
        ))
        validator.add_field_rule("id", UniqueRule(
            name="id_unique", description="ID unique"
        ))
        
        records = [
            {"id": "1", "name": "A"},
            {"id": "2", "name": "B"},
            {"id": "1", "name": "C"},  # Duplicate
            {"id": None, "name": "D"},  # Null
        ]
        
        report = validator.validate_batch(records)
        
        assert report.total_records == 4
        assert report.invalid_records == 2  # Duplicate + Null
        assert report.has_errors is True

    def test_quality_report(self):
        """Test quality report generation."""
        validator = DataQualityValidator("test")
        validator.add_field_rule("value", RangeRule(
            name="value_range", description="Value range",
            min_value=0, max_value=100,
        ))
        
        records = [
            {"value": 50},
            {"value": 75},
            {"value": 150},  # Out of range
            {"value": None},  # Null
        ]
        
        report = validator.validate_batch(records)
        
        assert report.total_records == 4
        assert "value" in report.field_reports
        
        field_report = report.field_reports["value"]
        assert field_report.null_count == 1
        assert field_report.invalid_count == 1
        assert field_report.valid_count == 2

    def test_dimension_scores(self):
        """Test quality dimension scoring."""
        validator = DataQualityValidator("test")
        validator.add_field_rule("name", NotNullRule(
            name="name_required", description="Name required"
        ))
        
        records = [
            {"name": "A"},
            {"name": "B"},
            {"name": None},
            {"name": None},
        ]
        
        report = validator.validate_batch(records)
        
        # Completeness should be 50% (2 out of 4 non-null)
        assert QualityDimension.COMPLETENESS.value in report.dimension_scores
        completeness = report.dimension_scores[QualityDimension.COMPLETENESS.value]
        assert completeness == 50.0

    def test_overall_score(self):
        """Test overall quality score."""
        validator = DataQualityValidator("test")
        validator.add_field_rule("id", NotNullRule(
            name="id_required", description="ID required"
        ))
        
        records = [{"id": "1"}, {"id": "2"}]
        report = validator.validate_batch(records)
        
        # All records valid, should have high score
        assert report.overall_score > 90

    def test_row_rule(self):
        """Test cross-field validation."""
        validator = DataQualityValidator("test")
        
        def validate_date_order(record):
            from pipeline.quality import QualityIssue
            issues = []
            start = record.get("start_date")
            end = record.get("end_date")
            
            if start and end and end < start:
                issues.append(QualityIssue(
                    field="end_date",
                    rule_name="date_order",
                    dimension=QualityDimension.CONSISTENCY,
                    severity=Severity.ERROR,
                    message="End date before start date",
                ))
            return issues
        
        validator.add_row_rule(validate_date_order)
        
        # Valid record
        issues = validator.validate_record({
            "start_date": datetime(2024, 1, 1),
            "end_date": datetime(2024, 1, 15),
        })
        assert len(issues) == 0
        
        # Invalid record
        issues = validator.validate_record({
            "start_date": datetime(2024, 1, 15),
            "end_date": datetime(2024, 1, 1),
        })
        assert len(issues) == 1


# =============================================================================
# HEALTHCARE VALIDATOR FACTORY TESTS
# =============================================================================


class TestPatientValidator:
    """Tests for patient data validator."""

    def test_valid_patient(self):
        """Test valid patient record."""
        validator = create_patient_validator()
        
        record = {
            "patient_id": "pat_001",
            "mrn": "MRN12345",
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": datetime(1990, 1, 1),
            "gender": "male",
            "ssn": "123-45-6789",
            "email": "john@example.com",
            "zip_code": "12345",
            "state": "NY",
        }
        
        issues = validator.validate_record(record)
        
        # Should have no errors (may have warnings)
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_missing_required_fields(self):
        """Test patient with missing required fields."""
        validator = create_patient_validator()
        
        record = {
            "patient_id": None,  # Required
            "mrn": "",           # Required
            "first_name": "",    # Required
            "last_name": "",     # Required
        }
        
        issues = validator.validate_record(record)
        
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) >= 4

    def test_invalid_formats(self):
        """Test patient with invalid field formats."""
        validator = create_patient_validator()
        
        record = {
            "patient_id": "pat_001",
            "mrn": "MRN12345",
            "first_name": "John",
            "last_name": "Doe",
            "date_of_birth": datetime(1990, 1, 1),
            "ssn": "invalid-ssn",      # Invalid format
            "email": "not-an-email",   # Invalid format
            "zip_code": "abc",         # Invalid format
        }
        
        issues = validator.validate_record(record)
        
        # Should have warnings for invalid formats
        warnings = [i for i in issues if i.severity == Severity.WARNING]
        assert len(warnings) >= 2


class TestEncounterValidator:
    """Tests for encounter data validator."""

    def test_valid_encounter(self):
        """Test valid encounter record."""
        validator = create_encounter_validator()
        
        record = {
            "encounter_id": "enc_001",
            "patient_id": "pat_001",
            "encounter_type": "outpatient",
            "status": "finished",
            "actual_start": datetime(2024, 1, 15, 10, 0),
            "actual_end": datetime(2024, 1, 15, 11, 0),
        }
        
        issues = validator.validate_record(record)
        
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) == 0

    def test_invalid_timing(self):
        """Test encounter with invalid timing."""
        validator = create_encounter_validator()
        
        record = {
            "encounter_id": "enc_001",
            "patient_id": "pat_001",
            "encounter_type": "outpatient",
            "status": "finished",
            "actual_start": datetime(2024, 1, 15, 11, 0),
            "actual_end": datetime(2024, 1, 15, 10, 0),  # Before start!
        }
        
        issues = validator.validate_record(record)
        
        errors = [i for i in issues if i.severity == Severity.ERROR]
        assert len(errors) >= 1
        assert any("time" in i.message.lower() for i in errors)
