"""
Healthcare Data Models

HIPAA-compliant Pydantic models for healthcare data processing.
All models include validation, serialization, and PHI tracking.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from enum import Enum
from typing import Annotated, Any
from uuid import uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


# =============================================================================
# ENUMS
# =============================================================================


class Gender(str, Enum):
    """Biological sex for clinical purposes."""
    MALE = "male"
    FEMALE = "female"
    OTHER = "other"
    UNKNOWN = "unknown"


class EncounterType(str, Enum):
    """Healthcare encounter classification."""
    INPATIENT = "inpatient"
    OUTPATIENT = "outpatient"
    EMERGENCY = "emergency"
    TELEHEALTH = "telehealth"
    HOME_HEALTH = "home_health"


class EncounterStatus(str, Enum):
    """Encounter lifecycle status."""
    PLANNED = "planned"
    ARRIVED = "arrived"
    IN_PROGRESS = "in_progress"
    FINISHED = "finished"
    CANCELLED = "cancelled"


class LabStatus(str, Enum):
    """Laboratory result status."""
    PENDING = "pending"
    PRELIMINARY = "preliminary"
    FINAL = "final"
    CORRECTED = "corrected"
    CANCELLED = "cancelled"


class ClaimStatus(str, Enum):
    """Insurance claim status."""
    DRAFT = "draft"
    SUBMITTED = "submitted"
    PENDING = "pending"
    APPROVED = "approved"
    DENIED = "denied"
    PARTIAL = "partial"
    PAID = "paid"


# =============================================================================
# BASE MODEL
# =============================================================================


class HealthcareBaseModel(BaseModel):
    """Base model with healthcare-specific configuration."""

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        use_enum_values=True,
        extra="forbid",
    )

    def phi_fields(self) -> list[str]:
        """Return list of PHI field names for this model."""
        return []

    def to_safe_dict(self) -> dict[str, Any]:
        """Export model with PHI fields redacted."""
        data = self.model_dump()
        for field in self.phi_fields():
            if field in data:
                data[field] = "[REDACTED]"
        return data


# =============================================================================
# PATIENT MODEL
# =============================================================================


class Patient(HealthcareBaseModel):
    """
    Patient demographic record.

    Contains PHI fields that require special handling:
    - mrn (Medical Record Number)
    - ssn (Social Security Number)
    - first_name, last_name
    - date_of_birth
    - address fields
    - phone, email
    """

    # Identifiers
    patient_id: str = Field(default_factory=lambda: f"pat_{uuid4().hex[:12]}")
    mrn: str = Field(..., min_length=1, max_length=20, description="Medical Record Number")
    ssn: str | None = Field(default=None, description="Social Security Number (XXX-XX-XXXX)")

    # Demographics
    first_name: str = Field(..., min_length=1, max_length=100)
    last_name: str = Field(..., min_length=1, max_length=100)
    middle_name: str | None = Field(default=None, max_length=100)
    date_of_birth: date
    gender: Gender = Gender.UNKNOWN

    # Contact
    email: str | None = Field(default=None, max_length=255)
    phone: str | None = Field(default=None, max_length=20)

    # Address
    address_line1: str | None = Field(default=None, max_length=200)
    address_line2: str | None = Field(default=None, max_length=200)
    city: str | None = Field(default=None, max_length=100)
    state: str | None = Field(default=None, min_length=2, max_length=2)
    zip_code: str | None = Field(default=None, max_length=10)

    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    is_active: bool = True

    @field_validator("ssn")
    @classmethod
    def validate_ssn(cls, v: str | None) -> str | None:
        """Validate SSN format (XXX-XX-XXXX)."""
        if v is None:
            return None
        # Remove any formatting
        clean = re.sub(r"[^0-9]", "", v)
        if len(clean) != 9:
            raise ValueError("SSN must be 9 digits")
        # Format consistently
        return f"{clean[:3]}-{clean[3:5]}-{clean[5:]}"

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, v: str | None) -> str | None:
        """Normalize phone number format."""
        if v is None:
            return None
        clean = re.sub(r"[^0-9]", "", v)
        if len(clean) == 10:
            return f"({clean[:3]}) {clean[3:6]}-{clean[6:]}"
        elif len(clean) == 11 and clean[0] == "1":
            return f"({clean[1:4]}) {clean[4:7]}-{clean[7:]}"
        return v

    @field_validator("zip_code")
    @classmethod
    def validate_zip(cls, v: str | None) -> str | None:
        """Validate ZIP code format."""
        if v is None:
            return None
        clean = re.sub(r"[^0-9]", "", v)
        if len(clean) == 5:
            return clean
        elif len(clean) == 9:
            return f"{clean[:5]}-{clean[5:]}"
        raise ValueError("ZIP code must be 5 or 9 digits")

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str | None) -> str | None:
        """Basic email validation."""
        if v is None:
            return None
        if "@" not in v or "." not in v.split("@")[-1]:
            raise ValueError("Invalid email format")
        return v.lower()

    def phi_fields(self) -> list[str]:
        """PHI fields per HIPAA Safe Harbor."""
        return [
            "mrn", "ssn", "first_name", "last_name", "middle_name",
            "date_of_birth", "email", "phone",
            "address_line1", "address_line2", "city", "state", "zip_code",
        ]

    @property
    def full_name(self) -> str:
        """Full name string."""
        parts = [self.first_name]
        if self.middle_name:
            parts.append(self.middle_name)
        parts.append(self.last_name)
        return " ".join(parts)

    @property
    def age(self) -> int:
        """Calculate current age in years."""
        today = date.today()
        return today.year - self.date_of_birth.year - (
            (today.month, today.day) < (self.date_of_birth.month, self.date_of_birth.day)
        )


# =============================================================================
# ENCOUNTER MODEL
# =============================================================================


class Diagnosis(HealthcareBaseModel):
    """ICD-10 diagnosis code."""
    code: str = Field(..., pattern=r"^[A-Z][0-9]{2}(\.[0-9A-Z]{1,4})?$")
    description: str
    is_primary: bool = False
    diagnosed_at: datetime | None = None


class Procedure(HealthcareBaseModel):
    """CPT/HCPCS procedure code."""
    code: str = Field(..., min_length=5, max_length=5)
    description: str
    quantity: int = Field(default=1, ge=1)
    performed_at: datetime | None = None


class Encounter(HealthcareBaseModel):
    """
    Healthcare encounter/visit record.

    Links to patient and contains clinical information.
    """

    encounter_id: str = Field(default_factory=lambda: f"enc_{uuid4().hex[:12]}")
    patient_id: str
    
    # Encounter details
    encounter_type: EncounterType
    status: EncounterStatus = EncounterStatus.PLANNED
    
    # Timing
    scheduled_start: datetime | None = None
    actual_start: datetime | None = None
    actual_end: datetime | None = None
    
    # Location
    facility_id: str | None = None
    department: str | None = None
    room: str | None = None
    
    # Providers
    attending_provider_id: str | None = None
    referring_provider_id: str | None = None
    
    # Clinical
    chief_complaint: str | None = Field(default=None, max_length=500)
    diagnoses: list[Diagnosis] = Field(default_factory=list)
    procedures: list[Procedure] = Field(default_factory=list)
    notes: str | None = None
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def validate_timing(self) -> "Encounter":
        """Ensure timing fields are logically consistent."""
        if self.actual_start and self.actual_end:
            if self.actual_end < self.actual_start:
                raise ValueError("End time cannot be before start time")
        return self

    def phi_fields(self) -> list[str]:
        """Encounter-level PHI fields."""
        return ["patient_id", "notes", "chief_complaint"]

    @property
    def duration_minutes(self) -> int | None:
        """Calculate encounter duration in minutes."""
        if self.actual_start and self.actual_end:
            delta = self.actual_end - self.actual_start
            return int(delta.total_seconds() / 60)
        return None

    @property
    def primary_diagnosis(self) -> Diagnosis | None:
        """Get primary diagnosis if set."""
        for dx in self.diagnoses:
            if dx.is_primary:
                return dx
        return self.diagnoses[0] if self.diagnoses else None


# =============================================================================
# LAB RESULT MODEL
# =============================================================================


class LabResult(HealthcareBaseModel):
    """
    Laboratory test result.

    Includes reference ranges and abnormal flag logic.
    """

    result_id: str = Field(default_factory=lambda: f"lab_{uuid4().hex[:12]}")
    patient_id: str
    encounter_id: str | None = None
    
    # Test identification
    loinc_code: str = Field(..., description="LOINC code for the test")
    test_name: str
    
    # Result
    value: float | str
    value_numeric: float | None = None
    unit: str | None = None
    
    # Reference range
    reference_low: float | None = None
    reference_high: float | None = None
    reference_text: str | None = None
    
    # Status
    status: LabStatus = LabStatus.PENDING
    is_abnormal: bool | None = None
    is_critical: bool = False
    
    # Timing
    collected_at: datetime
    resulted_at: datetime | None = None
    
    # Performing lab
    performing_lab: str | None = None
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)

    @model_validator(mode="after")
    def compute_abnormal(self) -> "LabResult":
        """Auto-compute abnormal flag based on reference range."""
        if self.value_numeric is not None and self.is_abnormal is None:
            if self.reference_low is not None and self.value_numeric < self.reference_low:
                self.is_abnormal = True
            elif self.reference_high is not None and self.value_numeric > self.reference_high:
                self.is_abnormal = True
            elif self.reference_low is not None or self.reference_high is not None:
                self.is_abnormal = False
        return self

    def phi_fields(self) -> list[str]:
        """Lab result PHI fields."""
        return ["patient_id"]


# =============================================================================
# CLAIM MODEL
# =============================================================================


class ClaimLine(HealthcareBaseModel):
    """Individual line item on an insurance claim."""
    
    line_number: int = Field(ge=1)
    procedure_code: str
    modifier: str | None = None
    diagnosis_pointers: list[int] = Field(default_factory=list)
    units: int = Field(default=1, ge=1)
    charge_amount: Annotated[float, Field(ge=0)]
    allowed_amount: Annotated[float, Field(ge=0)] | None = None
    paid_amount: Annotated[float, Field(ge=0)] | None = None
    
    # Dates
    service_date_from: date
    service_date_to: date | None = None


class Claim(HealthcareBaseModel):
    """
    Insurance claim record.

    Contains billing and payment information.
    """

    claim_id: str = Field(default_factory=lambda: f"clm_{uuid4().hex[:12]}")
    patient_id: str
    encounter_id: str | None = None
    
    # Claim type
    claim_type: str = Field(default="professional")  # professional, institutional
    
    # Payer
    payer_id: str
    payer_name: str | None = None
    member_id: str
    group_number: str | None = None
    
    # Provider
    billing_provider_npi: str = Field(..., min_length=10, max_length=10)
    rendering_provider_npi: str | None = None
    facility_npi: str | None = None
    
    # Diagnoses (ICD-10)
    diagnosis_codes: list[str] = Field(..., min_length=1)
    
    # Line items
    lines: list[ClaimLine] = Field(..., min_length=1)
    
    # Amounts
    total_charge: Annotated[float, Field(ge=0)]
    total_allowed: Annotated[float, Field(ge=0)] | None = None
    total_paid: Annotated[float, Field(ge=0)] | None = None
    patient_responsibility: Annotated[float, Field(ge=0)] | None = None
    
    # Status
    status: ClaimStatus = ClaimStatus.DRAFT
    
    # Dates
    service_date_from: date
    service_date_to: date
    submitted_at: datetime | None = None
    adjudicated_at: datetime | None = None
    
    # Metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    @field_validator("billing_provider_npi", "rendering_provider_npi", "facility_npi")
    @classmethod
    def validate_npi(cls, v: str | None) -> str | None:
        """Validate NPI using Luhn algorithm."""
        if v is None:
            return None
        if not v.isdigit() or len(v) != 10:
            raise ValueError("NPI must be exactly 10 digits")
        # Luhn check (with 80840 prefix for NPI)
        npi_with_prefix = "80840" + v
        total = 0
        for i, digit in enumerate(reversed(npi_with_prefix)):
            n = int(digit)
            if i % 2 == 0:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        if total % 10 != 0:
            raise ValueError("Invalid NPI checksum")
        return v

    def phi_fields(self) -> list[str]:
        """Claim PHI fields."""
        return ["patient_id", "member_id"]

    @property
    def is_adjudicated(self) -> bool:
        """Check if claim has been adjudicated."""
        return self.status in (
            ClaimStatus.APPROVED, ClaimStatus.DENIED,
            ClaimStatus.PARTIAL, ClaimStatus.PAID
        )


# =============================================================================
# EXPORTS
# =============================================================================


__all__ = [
    # Enums
    "Gender",
    "EncounterType",
    "EncounterStatus",
    "LabStatus",
    "ClaimStatus",
    # Models
    "HealthcareBaseModel",
    "Patient",
    "Diagnosis",
    "Procedure",
    "Encounter",
    "LabResult",
    "ClaimLine",
    "Claim",
]
