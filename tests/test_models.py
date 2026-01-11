"""Tests for healthcare data models."""

from datetime import date, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from pipeline.models import (
    Patient,
    Gender,
    Encounter,
    EncounterType,
    EncounterStatus,
    Diagnosis,
    Procedure,
    LabResult,
    LabStatus,
    Claim,
    ClaimLine,
    ClaimStatus,
)


# =============================================================================
# PATIENT TESTS
# =============================================================================


class TestPatient:
    """Tests for Patient model."""

    def test_create_patient_minimal(self):
        """Test creating patient with minimal required fields."""
        patient = Patient(
            mrn="MRN001",
            first_name="John",
            last_name="Doe",
            date_of_birth=date(1990, 5, 15),
        )
        
        assert patient.mrn == "MRN001"
        assert patient.first_name == "John"
        assert patient.last_name == "Doe"
        assert patient.date_of_birth == date(1990, 5, 15)
        assert patient.gender == Gender.UNKNOWN
        assert patient.is_active is True

    def test_create_patient_full(self):
        """Test creating patient with all fields."""
        patient = Patient(
            mrn="MRN002",
            ssn="123-45-6789",
            first_name="Jane",
            last_name="Smith",
            middle_name="Marie",
            date_of_birth=date(1985, 3, 20),
            gender=Gender.FEMALE,
            email="jane.smith@example.com",
            phone="555-123-4567",
            address_line1="123 Main St",
            address_line2="Apt 4B",
            city="New York",
            state="NY",
            zip_code="10001",
        )
        
        assert patient.full_name == "Jane Marie Smith"
        assert patient.ssn == "123-45-6789"
        assert patient.email == "jane.smith@example.com"

    def test_ssn_validation_formats(self):
        """Test SSN format normalization."""
        # Various formats should normalize to XXX-XX-XXXX
        patient1 = Patient(
            mrn="M1", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
            ssn="123456789",
        )
        assert patient1.ssn == "123-45-6789"
        
        patient2 = Patient(
            mrn="M2", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
            ssn="123 45 6789",
        )
        assert patient2.ssn == "123-45-6789"

    def test_ssn_validation_invalid(self):
        """Test SSN validation rejects invalid formats."""
        with pytest.raises(ValidationError):
            Patient(
                mrn="M1", first_name="A", last_name="B",
                date_of_birth=date(1990, 1, 1),
                ssn="12345",  # Too short
            )

    def test_phone_normalization(self):
        """Test phone number normalization."""
        patient = Patient(
            mrn="M1", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
            phone="5551234567",
        )
        assert patient.phone == "(555) 123-4567"

    def test_email_validation(self):
        """Test email validation."""
        # Valid email
        patient = Patient(
            mrn="M1", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
            email="Test@Example.COM",
        )
        assert patient.email == "test@example.com"
        
        # Invalid email
        with pytest.raises(ValidationError):
            Patient(
                mrn="M1", first_name="A", last_name="B",
                date_of_birth=date(1990, 1, 1),
                email="invalid-email",
            )

    def test_zip_code_validation(self):
        """Test ZIP code validation."""
        # 5-digit ZIP
        patient1 = Patient(
            mrn="M1", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
            zip_code="12345",
        )
        assert patient1.zip_code == "12345"
        
        # ZIP+4
        patient2 = Patient(
            mrn="M2", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
            zip_code="123456789",
        )
        assert patient2.zip_code == "12345-6789"

    def test_age_calculation(self):
        """Test age calculation."""
        patient = Patient(
            mrn="M1", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
        )
        # Age should be calculated based on today
        expected_age = date.today().year - 1990 - (
            (date.today().month, date.today().day) < (1, 1)
        )
        assert patient.age == expected_age

    def test_phi_fields(self):
        """Test PHI field identification."""
        patient = Patient(
            mrn="M1", first_name="A", last_name="B",
            date_of_birth=date(1990, 1, 1),
        )
        
        phi = patient.phi_fields()
        
        assert "mrn" in phi
        assert "ssn" in phi
        assert "first_name" in phi
        assert "last_name" in phi
        assert "date_of_birth" in phi
        assert "email" in phi
        assert "phone" in phi

    def test_safe_dict(self):
        """Test PHI redaction in safe_dict."""
        patient = Patient(
            mrn="MRN001",
            ssn="123-45-6789",
            first_name="John",
            last_name="Doe",
            date_of_birth=date(1990, 1, 1),
        )
        
        safe = patient.to_safe_dict()
        
        assert safe["mrn"] == "[REDACTED]"
        assert safe["ssn"] == "[REDACTED]"
        assert safe["first_name"] == "[REDACTED]"
        assert safe["last_name"] == "[REDACTED]"
        assert safe["patient_id"] is not None  # Non-PHI field preserved


# =============================================================================
# ENCOUNTER TESTS
# =============================================================================


class TestEncounter:
    """Tests for Encounter model."""

    def test_create_encounter(self):
        """Test creating an encounter."""
        encounter = Encounter(
            patient_id="pat_123",
            encounter_type=EncounterType.OUTPATIENT,
        )
        
        assert encounter.patient_id == "pat_123"
        assert encounter.encounter_type == EncounterType.OUTPATIENT
        assert encounter.status == EncounterStatus.PLANNED

    def test_encounter_with_diagnoses(self):
        """Test encounter with diagnoses."""
        encounter = Encounter(
            patient_id="pat_123",
            encounter_type=EncounterType.INPATIENT,
            diagnoses=[
                Diagnosis(
                    code="J06.9",
                    description="Acute upper respiratory infection",
                    is_primary=True,
                ),
                Diagnosis(
                    code="R50.9",
                    description="Fever",
                ),
            ],
        )
        
        assert len(encounter.diagnoses) == 2
        assert encounter.primary_diagnosis.code == "J06.9"

    def test_encounter_with_procedures(self):
        """Test encounter with procedures."""
        encounter = Encounter(
            patient_id="pat_123",
            encounter_type=EncounterType.OUTPATIENT,
            procedures=[
                Procedure(
                    code="99213",
                    description="Office visit, established patient",
                    quantity=1,
                ),
            ],
        )
        
        assert len(encounter.procedures) == 1
        assert encounter.procedures[0].code == "99213"

    def test_encounter_timing_validation(self):
        """Test encounter timing validation."""
        # Valid timing
        encounter = Encounter(
            patient_id="pat_123",
            encounter_type=EncounterType.OUTPATIENT,
            actual_start=datetime(2024, 1, 15, 10, 0),
            actual_end=datetime(2024, 1, 15, 11, 0),
        )
        assert encounter.duration_minutes == 60
        
        # Invalid timing (end before start)
        with pytest.raises(ValidationError):
            Encounter(
                patient_id="pat_123",
                encounter_type=EncounterType.OUTPATIENT,
                actual_start=datetime(2024, 1, 15, 11, 0),
                actual_end=datetime(2024, 1, 15, 10, 0),
            )

    def test_diagnosis_code_format(self):
        """Test ICD-10 code format validation."""
        # Valid codes
        dx1 = Diagnosis(code="A00", description="Cholera")
        dx2 = Diagnosis(code="A00.1", description="Cholera, El Tor")
        dx3 = Diagnosis(code="Z99.89", description="Other dependence")
        
        assert dx1.code == "A00"
        assert dx2.code == "A00.1"
        assert dx3.code == "Z99.89"
        
        # Invalid code format
        with pytest.raises(ValidationError):
            Diagnosis(code="invalid", description="Bad code")


# =============================================================================
# LAB RESULT TESTS
# =============================================================================


class TestLabResult:
    """Tests for LabResult model."""

    def test_create_lab_result(self):
        """Test creating a lab result."""
        result = LabResult(
            patient_id="pat_123",
            loinc_code="2345-7",
            test_name="Glucose",
            value=95.0,
            value_numeric=95.0,
            unit="mg/dL",
            collected_at=datetime(2024, 1, 15, 8, 0),
        )
        
        assert result.patient_id == "pat_123"
        assert result.value == 95.0
        assert result.status == LabStatus.PENDING

    def test_abnormal_detection(self):
        """Test automatic abnormal flag detection."""
        # Normal value
        normal = LabResult(
            patient_id="pat_123",
            loinc_code="2345-7",
            test_name="Glucose",
            value=95.0,
            value_numeric=95.0,
            unit="mg/dL",
            reference_low=70.0,
            reference_high=100.0,
            collected_at=datetime(2024, 1, 15),
        )
        assert normal.is_abnormal is False
        
        # High value
        high = LabResult(
            patient_id="pat_123",
            loinc_code="2345-7",
            test_name="Glucose",
            value=150.0,
            value_numeric=150.0,
            unit="mg/dL",
            reference_low=70.0,
            reference_high=100.0,
            collected_at=datetime(2024, 1, 15),
        )
        assert high.is_abnormal is True
        
        # Low value
        low = LabResult(
            patient_id="pat_123",
            loinc_code="2345-7",
            test_name="Glucose",
            value=50.0,
            value_numeric=50.0,
            unit="mg/dL",
            reference_low=70.0,
            reference_high=100.0,
            collected_at=datetime(2024, 1, 15),
        )
        assert low.is_abnormal is True


# =============================================================================
# CLAIM TESTS
# =============================================================================


class TestClaim:
    """Tests for Claim model."""

    def test_create_claim(self):
        """Test creating a claim."""
        claim = Claim(
            patient_id="pat_123",
            payer_id="PAYER001",
            member_id="MEM123456",
            billing_provider_npi="1234567897",  # Valid NPI (passes Luhn)
            diagnosis_codes=["J06.9"],
            lines=[
                ClaimLine(
                    line_number=1,
                    procedure_code="99213",
                    charge_amount=150.00,
                    service_date_from=date(2024, 1, 15),
                ),
            ],
            total_charge=150.00,
            service_date_from=date(2024, 1, 15),
            service_date_to=date(2024, 1, 15),
        )
        
        assert claim.status == ClaimStatus.DRAFT
        assert len(claim.lines) == 1
        assert not claim.is_adjudicated

    def test_npi_validation(self):
        """Test NPI Luhn validation."""
        # Valid NPI
        claim = Claim(
            patient_id="pat_123",
            payer_id="PAYER001",
            member_id="MEM123456",
            billing_provider_npi="1234567897",
            diagnosis_codes=["J06.9"],
            lines=[
                ClaimLine(
                    line_number=1,
                    procedure_code="99213",
                    charge_amount=150.00,
                    service_date_from=date(2024, 1, 15),
                ),
            ],
            total_charge=150.00,
            service_date_from=date(2024, 1, 15),
            service_date_to=date(2024, 1, 15),
        )
        assert claim.billing_provider_npi == "1234567897"
        
        # Invalid NPI (fails Luhn check)
        with pytest.raises(ValidationError):
            Claim(
                patient_id="pat_123",
                payer_id="PAYER001",
                member_id="MEM123456",
                billing_provider_npi="1234567890",  # Invalid checksum
                diagnosis_codes=["J06.9"],
                lines=[
                    ClaimLine(
                        line_number=1,
                        procedure_code="99213",
                        charge_amount=150.00,
                        service_date_from=date(2024, 1, 15),
                    ),
                ],
                total_charge=150.00,
                service_date_from=date(2024, 1, 15),
                service_date_to=date(2024, 1, 15),
            )

    def test_claim_adjudication_status(self):
        """Test claim adjudication status."""
        claim = Claim(
            patient_id="pat_123",
            payer_id="PAYER001",
            member_id="MEM123456",
            billing_provider_npi="1234567897",
            diagnosis_codes=["J06.9"],
            lines=[
                ClaimLine(
                    line_number=1,
                    procedure_code="99213",
                    charge_amount=150.00,
                    service_date_from=date(2024, 1, 15),
                ),
            ],
            total_charge=150.00,
            service_date_from=date(2024, 1, 15),
            service_date_to=date(2024, 1, 15),
            status=ClaimStatus.APPROVED,
        )
        
        assert claim.is_adjudicated is True
