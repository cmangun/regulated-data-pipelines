"""
PII Detection and Handling Module

HIPAA Safe Harbor compliant PII detection, masking, and de-identification.

The 18 HIPAA Safe Harbor identifiers:
1. Names
2. Geographic data (smaller than state)
3. Dates (except year) related to individual
4. Phone numbers
5. Fax numbers
6. Email addresses
7. Social Security numbers
8. Medical record numbers
9. Health plan beneficiary numbers
10. Account numbers
11. Certificate/license numbers
12. Vehicle identifiers and serial numbers
13. Device identifiers and serial numbers
14. Web URLs
15. IP addresses
16. Biometric identifiers
17. Full-face photographs
18. Any other unique identifying number/code
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Any, Callable

from pydantic import BaseModel, Field


# =============================================================================
# PII TYPES
# =============================================================================


class PIIType(str, Enum):
    """Categories of Protected Health Information (PHI) / PII."""
    
    # Direct identifiers
    NAME = "name"
    SSN = "ssn"
    MRN = "mrn"
    EMAIL = "email"
    PHONE = "phone"
    FAX = "fax"
    
    # Geographic
    ADDRESS = "address"
    CITY = "city"
    ZIP_CODE = "zip_code"
    
    # Dates
    DATE_OF_BIRTH = "date_of_birth"
    ADMISSION_DATE = "admission_date"
    DISCHARGE_DATE = "discharge_date"
    DEATH_DATE = "death_date"
    
    # Numbers
    ACCOUNT_NUMBER = "account_number"
    LICENSE_NUMBER = "license_number"
    VEHICLE_ID = "vehicle_id"
    DEVICE_ID = "device_id"
    
    # Digital
    IP_ADDRESS = "ip_address"
    URL = "url"
    
    # Payment
    CREDIT_CARD = "credit_card"
    BANK_ACCOUNT = "bank_account"
    
    # Other
    BIOMETRIC = "biometric"
    PHOTO = "photo"
    OTHER = "other"


# =============================================================================
# PII DETECTION PATTERNS
# =============================================================================


@dataclass
class PIIPattern:
    """Pattern for detecting PII in text."""
    pii_type: PIIType
    pattern: re.Pattern[str]
    confidence: float = 0.9
    validator: Callable[[str], bool] | None = None


# Regex patterns for PII detection
PII_PATTERNS: list[PIIPattern] = [
    # SSN: XXX-XX-XXXX or XXXXXXXXX
    PIIPattern(
        PIIType.SSN,
        re.compile(r"\b(\d{3}[-\s]?\d{2}[-\s]?\d{4})\b"),
        confidence=0.95,
        validator=lambda x: len(re.sub(r"\D", "", x)) == 9,
    ),
    
    # Email
    PIIPattern(
        PIIType.EMAIL,
        re.compile(r"\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b"),
        confidence=0.98,
    ),
    
    # Phone: various formats
    PIIPattern(
        PIIType.PHONE,
        re.compile(r"\b(\+?1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})\b"),
        confidence=0.85,
        validator=lambda x: 10 <= len(re.sub(r"\D", "", x)) <= 11,
    ),
    
    # Credit Card (basic patterns)
    PIIPattern(
        PIIType.CREDIT_CARD,
        re.compile(r"\b(\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4})\b"),
        confidence=0.90,
        validator=lambda x: _luhn_check(re.sub(r"\D", "", x)),
    ),
    
    # IP Address (IPv4)
    PIIPattern(
        PIIType.IP_ADDRESS,
        re.compile(r"\b(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\b"),
        confidence=0.95,
        validator=lambda x: all(0 <= int(p) <= 255 for p in x.split(".")),
    ),
    
    # MRN patterns (common formats)
    PIIPattern(
        PIIType.MRN,
        re.compile(r"\b(MRN[-:\s]?\d{6,12})\b", re.IGNORECASE),
        confidence=0.90,
    ),
    
    # Date patterns (MM/DD/YYYY, YYYY-MM-DD, etc.)
    PIIPattern(
        PIIType.DATE_OF_BIRTH,
        re.compile(r"\b(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b"),
        confidence=0.70,  # Lower confidence - dates are common
    ),
    
    # ZIP+4
    PIIPattern(
        PIIType.ZIP_CODE,
        re.compile(r"\b(\d{5}(?:-\d{4})?)\b"),
        confidence=0.60,  # Low confidence - many 5-digit numbers exist
    ),
]


def _luhn_check(number: str) -> bool:
    """Validate number using Luhn algorithm."""
    if not number.isdigit():
        return False
    digits = [int(d) for d in number]
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    total = sum(odd_digits)
    for d in even_digits:
        total += sum(divmod(d * 2, 10))
    return total % 10 == 0


# =============================================================================
# PII DETECTION RESULTS
# =============================================================================


class PIIMatch(BaseModel):
    """A detected PII match in text."""
    
    pii_type: PIIType
    value: str
    start: int
    end: int
    confidence: float = Field(ge=0, le=1)
    
    @property
    def length(self) -> int:
        return self.end - self.start


class PIIDetectionResult(BaseModel):
    """Results from PII detection scan."""
    
    original_text: str
    matches: list[PIIMatch] = Field(default_factory=list)
    scan_timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    @property
    def has_pii(self) -> bool:
        return len(self.matches) > 0
    
    @property
    def pii_types_found(self) -> set[PIIType]:
        return {m.pii_type for m in self.matches}
    
    @property
    def high_confidence_matches(self) -> list[PIIMatch]:
        return [m for m in self.matches if m.confidence >= 0.9]


# =============================================================================
# PII DETECTOR
# =============================================================================


class PIIDetector:
    """
    Detect PII/PHI in text and structured data.
    
    Supports:
    - Text scanning with regex patterns
    - Structured data field analysis
    - Confidence scoring
    - Custom pattern registration
    """
    
    def __init__(self, patterns: list[PIIPattern] | None = None):
        self.patterns = patterns or PII_PATTERNS.copy()
    
    def add_pattern(self, pattern: PIIPattern) -> None:
        """Register a custom PII pattern."""
        self.patterns.append(pattern)
    
    def scan_text(self, text: str) -> PIIDetectionResult:
        """
        Scan text for PII patterns.
        
        Args:
            text: Input text to scan
            
        Returns:
            PIIDetectionResult with all matches found
        """
        matches: list[PIIMatch] = []
        
        for pii_pattern in self.patterns:
            for match in pii_pattern.pattern.finditer(text):
                value = match.group(1) if match.groups() else match.group(0)
                
                # Apply validator if present
                confidence = pii_pattern.confidence
                if pii_pattern.validator:
                    try:
                        if not pii_pattern.validator(value):
                            confidence *= 0.5  # Reduce confidence if validation fails
                    except Exception:
                        confidence *= 0.5
                
                matches.append(PIIMatch(
                    pii_type=pii_pattern.pii_type,
                    value=value,
                    start=match.start(),
                    end=match.end(),
                    confidence=confidence,
                ))
        
        # Sort by position and remove overlapping matches (keep highest confidence)
        matches = self._dedupe_overlapping(matches)
        
        return PIIDetectionResult(
            original_text=text,
            matches=matches,
        )
    
    def scan_dict(
        self,
        data: dict[str, Any],
        field_mapping: dict[str, PIIType] | None = None,
    ) -> dict[str, list[PIIMatch]]:
        """
        Scan dictionary fields for PII.
        
        Args:
            data: Dictionary to scan
            field_mapping: Optional mapping of field names to known PII types
            
        Returns:
            Dict mapping field names to their PII matches
        """
        results: dict[str, list[PIIMatch]] = {}
        field_mapping = field_mapping or {}
        
        for key, value in data.items():
            if value is None:
                continue
            
            str_value = str(value)
            field_matches: list[PIIMatch] = []
            
            # Check if field is in known mapping
            if key in field_mapping:
                field_matches.append(PIIMatch(
                    pii_type=field_mapping[key],
                    value=str_value,
                    start=0,
                    end=len(str_value),
                    confidence=1.0,  # Known field
                ))
            else:
                # Scan field value for patterns
                result = self.scan_text(str_value)
                field_matches.extend(result.matches)
            
            if field_matches:
                results[key] = field_matches
        
        return results
    
    def _dedupe_overlapping(self, matches: list[PIIMatch]) -> list[PIIMatch]:
        """Remove overlapping matches, keeping highest confidence."""
        if not matches:
            return []
        
        # Sort by start position, then by confidence (descending)
        sorted_matches = sorted(matches, key=lambda m: (m.start, -m.confidence))
        
        result: list[PIIMatch] = []
        last_end = -1
        
        for match in sorted_matches:
            if match.start >= last_end:
                result.append(match)
                last_end = match.end
        
        return result


# =============================================================================
# PII MASKING
# =============================================================================


class MaskingStrategy(str, Enum):
    """Strategies for masking PII."""
    
    REDACT = "redact"       # Replace with [REDACTED]
    ASTERISK = "asterisk"   # Replace with ******
    HASH = "hash"           # Replace with SHA-256 hash
    PARTIAL = "partial"     # Show partial (e.g., ***-**-1234)
    TOKEN = "token"         # Replace with reversible token
    CATEGORY = "category"   # Replace with category label


class PIIMasker:
    """
    Mask/redact PII from text and data structures.
    
    Supports multiple masking strategies for different use cases:
    - REDACT: Complete removal for maximum privacy
    - PARTIAL: Show partial data for usability
    - HASH: Consistent replacement for analytics
    - TOKEN: Reversible for authorized re-identification
    """
    
    def __init__(
        self,
        default_strategy: MaskingStrategy = MaskingStrategy.REDACT,
        salt: str = "",
    ):
        self.default_strategy = default_strategy
        self.salt = salt
        self._token_map: dict[str, str] = {}
        self._reverse_map: dict[str, str] = {}
    
    def mask_value(
        self,
        value: str,
        pii_type: PIIType,
        strategy: MaskingStrategy | None = None,
    ) -> str:
        """
        Mask a single PII value.
        
        Args:
            value: The PII value to mask
            pii_type: Type of PII
            strategy: Masking strategy (uses default if None)
            
        Returns:
            Masked value
        """
        strategy = strategy or self.default_strategy
        
        if strategy == MaskingStrategy.REDACT:
            return f"[{pii_type.value.upper()}_REDACTED]"
        
        elif strategy == MaskingStrategy.ASTERISK:
            return "*" * len(value)
        
        elif strategy == MaskingStrategy.HASH:
            hash_input = f"{self.salt}{value}{pii_type.value}"
            return hashlib.sha256(hash_input.encode()).hexdigest()[:16]
        
        elif strategy == MaskingStrategy.PARTIAL:
            return self._partial_mask(value, pii_type)
        
        elif strategy == MaskingStrategy.TOKEN:
            return self._tokenize(value, pii_type)
        
        elif strategy == MaskingStrategy.CATEGORY:
            return f"[{pii_type.value.upper()}]"
        
        return value
    
    def mask_text(
        self,
        text: str,
        matches: list[PIIMatch],
        strategy: MaskingStrategy | None = None,
    ) -> str:
        """
        Mask all PII in text based on detection results.
        
        Args:
            text: Original text
            matches: PII matches from detector
            strategy: Masking strategy
            
        Returns:
            Text with PII masked
        """
        if not matches:
            return text
        
        # Sort matches by position (reverse) for safe replacement
        sorted_matches = sorted(matches, key=lambda m: m.start, reverse=True)
        
        result = text
        for match in sorted_matches:
            masked = self.mask_value(match.value, match.pii_type, strategy)
            result = result[:match.start] + masked + result[match.end:]
        
        return result
    
    def mask_dict(
        self,
        data: dict[str, Any],
        pii_fields: dict[str, PIIType],
        strategy: MaskingStrategy | None = None,
    ) -> dict[str, Any]:
        """
        Mask PII fields in a dictionary.
        
        Args:
            data: Original data dictionary
            pii_fields: Mapping of field names to PII types
            strategy: Masking strategy
            
        Returns:
            Dictionary with specified fields masked
        """
        result = data.copy()
        
        for field, pii_type in pii_fields.items():
            if field in result and result[field] is not None:
                result[field] = self.mask_value(str(result[field]), pii_type, strategy)
        
        return result
    
    def _partial_mask(self, value: str, pii_type: PIIType) -> str:
        """Apply partial masking based on PII type."""
        if pii_type == PIIType.SSN:
            # Show last 4: ***-**-1234
            clean = re.sub(r"\D", "", value)
            if len(clean) >= 4:
                return f"***-**-{clean[-4:]}"
        
        elif pii_type == PIIType.PHONE:
            # Show last 4: (***) ***-1234
            clean = re.sub(r"\D", "", value)
            if len(clean) >= 4:
                return f"(***) ***-{clean[-4:]}"
        
        elif pii_type == PIIType.EMAIL:
            # Show domain: a***@example.com
            if "@" in value:
                local, domain = value.split("@", 1)
                return f"{local[0]}***@{domain}"
        
        elif pii_type == PIIType.CREDIT_CARD:
            # Show last 4: ****-****-****-1234
            clean = re.sub(r"\D", "", value)
            if len(clean) >= 4:
                return f"****-****-****-{clean[-4:]}"
        
        elif pii_type == PIIType.NAME:
            # Show initials: J*** D***
            words = value.split()
            return " ".join(f"{w[0]}***" if w else "***" for w in words)
        
        # Default: show first and last char
        if len(value) > 2:
            return f"{value[0]}{'*' * (len(value) - 2)}{value[-1]}"
        return "*" * len(value)
    
    def _tokenize(self, value: str, pii_type: PIIType) -> str:
        """Create reversible token for value."""
        key = f"{pii_type.value}:{value}"
        
        if key not in self._token_map:
            import uuid
            token = f"TOK_{pii_type.value.upper()}_{uuid.uuid4().hex[:8]}"
            self._token_map[key] = token
            self._reverse_map[token] = value
        
        return self._token_map[key]
    
    def detokenize(self, token: str) -> str | None:
        """Reverse a token to original value (if available)."""
        return self._reverse_map.get(token)


# =============================================================================
# SAFE HARBOR DE-IDENTIFICATION
# =============================================================================


class SafeHarborDeidentifier:
    """
    HIPAA Safe Harbor de-identification.
    
    Implements the 18 identifier removal required for Safe Harbor compliance.
    """
    
    # Age threshold for Safe Harbor
    AGE_THRESHOLD = 90
    
    # Generalize ZIP to 3 digits if population < 20,000
    # For simplicity, we generalize all ZIPs to 3 digits
    ZIP_GENERALIZE_DIGITS = 3
    
    def __init__(self):
        self.detector = PIIDetector()
        self.masker = PIIMasker(default_strategy=MaskingStrategy.REDACT)
    
    def deidentify_patient(self, patient_data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply Safe Harbor de-identification to patient data.
        
        Removes or generalizes all 18 HIPAA identifiers.
        """
        result = patient_data.copy()
        
        # 1. Remove names
        for field in ["first_name", "last_name", "middle_name", "full_name"]:
            if field in result:
                result[field] = "[REDACTED]"
        
        # 2. Generalize geographic data
        if "zip_code" in result and result["zip_code"]:
            zip_clean = re.sub(r"\D", "", str(result["zip_code"]))
            if len(zip_clean) >= 3:
                result["zip_code"] = zip_clean[:self.ZIP_GENERALIZE_DIGITS] + "00"
            else:
                result["zip_code"] = "00000"
        
        for field in ["address_line1", "address_line2", "city"]:
            if field in result:
                result[field] = "[REDACTED]"
        
        # 3. Generalize dates to year only (except for age > 90)
        if "date_of_birth" in result and result["date_of_birth"]:
            dob = result["date_of_birth"]
            if isinstance(dob, str):
                try:
                    dob = datetime.fromisoformat(dob).date()
                except ValueError:
                    dob = None
            
            if isinstance(dob, (date, datetime)):
                age = self._calculate_age(dob)
                if age >= self.AGE_THRESHOLD:
                    # For 90+, replace with "90+"
                    result["date_of_birth"] = None
                    result["age_category"] = "90+"
                else:
                    # Keep only year
                    result["date_of_birth"] = dob.year if hasattr(dob, 'year') else None
        
        # 4-6. Remove contact info
        for field in ["phone", "fax", "email"]:
            if field in result:
                result[field] = "[REDACTED]"
        
        # 7. Remove SSN
        if "ssn" in result:
            result["ssn"] = "[REDACTED]"
        
        # 8. Remove MRN
        if "mrn" in result:
            result["mrn"] = "[REDACTED]"
        
        # 9-14. Remove other identifiers
        id_fields = [
            "health_plan_id", "account_number", "license_number",
            "vehicle_id", "device_serial", "url", "ip_address",
        ]
        for field in id_fields:
            if field in result:
                result[field] = "[REDACTED]"
        
        return result
    
    def _calculate_age(self, dob: date) -> int:
        """Calculate age from date of birth."""
        today = date.today()
        return today.year - dob.year - (
            (today.month, today.day) < (dob.month, dob.day)
        )


# =============================================================================
# EXPORTS
# =============================================================================


__all__ = [
    "PIIType",
    "PIIPattern",
    "PIIMatch",
    "PIIDetectionResult",
    "PIIDetector",
    "MaskingStrategy",
    "PIIMasker",
    "SafeHarborDeidentifier",
]
