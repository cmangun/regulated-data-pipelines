"""Tests for PII detection and handling."""

import pytest

from pipeline.pii import (
    PIIType,
    PIIDetector,
    PIIMasker,
    MaskingStrategy,
    SafeHarborDeidentifier,
)


# =============================================================================
# PII DETECTOR TESTS
# =============================================================================


class TestPIIDetector:
    """Tests for PII detection."""

    def test_detect_ssn(self):
        """Test SSN detection."""
        detector = PIIDetector()
        
        # Standard format
        result = detector.scan_text("My SSN is 123-45-6789")
        assert result.has_pii
        assert PIIType.SSN in result.pii_types_found
        assert result.matches[0].value == "123-45-6789"
        
        # Without dashes
        result2 = detector.scan_text("SSN: 123456789")
        assert result2.has_pii
        assert PIIType.SSN in result2.pii_types_found

    def test_detect_email(self):
        """Test email detection."""
        detector = PIIDetector()
        
        result = detector.scan_text("Contact me at john.doe@example.com")
        
        assert result.has_pii
        assert PIIType.EMAIL in result.pii_types_found
        assert result.matches[0].value == "john.doe@example.com"
        assert result.matches[0].confidence >= 0.95

    def test_detect_phone(self):
        """Test phone number detection."""
        detector = PIIDetector()
        
        # Various formats
        texts = [
            "Call me at (555) 123-4567",
            "Phone: 555-123-4567",
            "Tel: 5551234567",
            "Call 1-555-123-4567",
        ]
        
        for text in texts:
            result = detector.scan_text(text)
            assert result.has_pii, f"Failed to detect phone in: {text}"
            assert PIIType.PHONE in result.pii_types_found

    def test_detect_credit_card(self):
        """Test credit card detection."""
        detector = PIIDetector()
        
        # Valid card number (passes Luhn)
        result = detector.scan_text("Card: 4111-1111-1111-1111")
        
        assert result.has_pii
        assert PIIType.CREDIT_CARD in result.pii_types_found
        assert result.matches[0].confidence >= 0.85

    def test_detect_ip_address(self):
        """Test IP address detection."""
        detector = PIIDetector()
        
        result = detector.scan_text("Connected from 192.168.1.1")
        
        assert result.has_pii
        assert PIIType.IP_ADDRESS in result.pii_types_found
        assert result.matches[0].value == "192.168.1.1"

    def test_detect_mrn(self):
        """Test MRN detection."""
        detector = PIIDetector()
        
        result = detector.scan_text("Patient MRN:123456789")
        
        assert result.has_pii
        assert PIIType.MRN in result.pii_types_found

    def test_no_pii_text(self):
        """Test text without PII."""
        detector = PIIDetector()
        
        result = detector.scan_text("The quick brown fox jumps over the lazy dog")
        
        assert not result.has_pii
        assert len(result.matches) == 0

    def test_multiple_pii(self):
        """Test detecting multiple PII in one text."""
        detector = PIIDetector()
        
        text = """
        Patient John Smith (SSN: 123-45-6789)
        Email: john.smith@example.com
        Phone: (555) 123-4567
        """
        
        result = detector.scan_text(text)
        
        assert result.has_pii
        assert len(result.pii_types_found) >= 3

    def test_scan_dict(self):
        """Test scanning dictionary fields."""
        detector = PIIDetector()
        
        data = {
            "name": "John Doe",
            "email": "john@example.com",
            "ssn": "123-45-6789",
            "notes": "Patient reported no issues",
        }
        
        field_mapping = {
            "ssn": PIIType.SSN,
            "email": PIIType.EMAIL,
        }
        
        results = detector.scan_dict(data, field_mapping)
        
        assert "ssn" in results
        assert "email" in results
        assert results["ssn"][0].confidence == 1.0  # Known field

    def test_high_confidence_matches(self):
        """Test filtering high confidence matches."""
        detector = PIIDetector()
        
        result = detector.scan_text("SSN: 123-45-6789, ZIP: 12345")
        
        high_conf = result.high_confidence_matches
        
        # SSN should be high confidence
        assert any(m.pii_type == PIIType.SSN for m in high_conf)


# =============================================================================
# PII MASKER TESTS
# =============================================================================


class TestPIIMasker:
    """Tests for PII masking."""

    def test_redact_strategy(self):
        """Test REDACT masking strategy."""
        masker = PIIMasker(default_strategy=MaskingStrategy.REDACT)
        
        masked = masker.mask_value("123-45-6789", PIIType.SSN)
        
        assert masked == "[SSN_REDACTED]"

    def test_asterisk_strategy(self):
        """Test ASTERISK masking strategy."""
        masker = PIIMasker(default_strategy=MaskingStrategy.ASTERISK)
        
        masked = masker.mask_value("123-45-6789", PIIType.SSN)
        
        assert masked == "***********"
        assert len(masked) == len("123-45-6789")

    def test_hash_strategy(self):
        """Test HASH masking strategy."""
        masker = PIIMasker(default_strategy=MaskingStrategy.HASH, salt="test")
        
        masked1 = masker.mask_value("123-45-6789", PIIType.SSN)
        masked2 = masker.mask_value("123-45-6789", PIIType.SSN)
        
        # Same input should produce same hash
        assert masked1 == masked2
        assert len(masked1) == 16

    def test_partial_strategy_ssn(self):
        """Test PARTIAL masking for SSN."""
        masker = PIIMasker(default_strategy=MaskingStrategy.PARTIAL)
        
        masked = masker.mask_value("123-45-6789", PIIType.SSN)
        
        assert masked == "***-**-6789"  # Last 4 visible

    def test_partial_strategy_phone(self):
        """Test PARTIAL masking for phone."""
        masker = PIIMasker(default_strategy=MaskingStrategy.PARTIAL)
        
        masked = masker.mask_value("(555) 123-4567", PIIType.PHONE)
        
        assert masked == "(***) ***-4567"

    def test_partial_strategy_email(self):
        """Test PARTIAL masking for email."""
        masker = PIIMasker(default_strategy=MaskingStrategy.PARTIAL)
        
        masked = masker.mask_value("john.doe@example.com", PIIType.EMAIL)
        
        assert masked == "j***@example.com"

    def test_partial_strategy_credit_card(self):
        """Test PARTIAL masking for credit card."""
        masker = PIIMasker(default_strategy=MaskingStrategy.PARTIAL)
        
        masked = masker.mask_value("4111-1111-1111-1111", PIIType.CREDIT_CARD)
        
        assert masked == "****-****-****-1111"

    def test_token_strategy(self):
        """Test TOKEN masking strategy."""
        masker = PIIMasker(default_strategy=MaskingStrategy.TOKEN)
        
        token1 = masker.mask_value("123-45-6789", PIIType.SSN)
        token2 = masker.mask_value("123-45-6789", PIIType.SSN)
        
        # Same input should produce same token
        assert token1 == token2
        assert token1.startswith("TOK_SSN_")
        
        # Token should be reversible
        original = masker.detokenize(token1)
        assert original == "123-45-6789"

    def test_category_strategy(self):
        """Test CATEGORY masking strategy."""
        masker = PIIMasker(default_strategy=MaskingStrategy.CATEGORY)
        
        masked = masker.mask_value("123-45-6789", PIIType.SSN)
        
        assert masked == "[SSN]"

    def test_mask_text(self):
        """Test masking PII in text."""
        detector = PIIDetector()
        masker = PIIMasker(default_strategy=MaskingStrategy.REDACT)
        
        text = "Contact John at john@example.com or 555-123-4567"
        result = detector.scan_text(text)
        
        masked = masker.mask_text(text, result.matches)
        
        assert "john@example.com" not in masked
        assert "[EMAIL_REDACTED]" in masked

    def test_mask_dict(self):
        """Test masking dictionary fields."""
        masker = PIIMasker(default_strategy=MaskingStrategy.REDACT)
        
        data = {
            "name": "John Doe",
            "ssn": "123-45-6789",
            "age": 35,
        }
        
        pii_fields = {
            "ssn": PIIType.SSN,
            "name": PIIType.NAME,
        }
        
        masked = masker.mask_dict(data, pii_fields)
        
        assert masked["ssn"] == "[SSN_REDACTED]"
        assert masked["name"] == "[NAME_REDACTED]"
        assert masked["age"] == 35  # Non-PII preserved


# =============================================================================
# SAFE HARBOR TESTS
# =============================================================================


class TestSafeHarborDeidentifier:
    """Tests for HIPAA Safe Harbor de-identification."""

    def test_remove_names(self):
        """Test name removal."""
        deidentifier = SafeHarborDeidentifier()
        
        patient_data = {
            "first_name": "John",
            "last_name": "Doe",
            "middle_name": "Robert",
        }
        
        result = deidentifier.deidentify_patient(patient_data)
        
        assert result["first_name"] == "[REDACTED]"
        assert result["last_name"] == "[REDACTED]"
        assert result["middle_name"] == "[REDACTED]"

    def test_generalize_zip(self):
        """Test ZIP code generalization."""
        deidentifier = SafeHarborDeidentifier()
        
        patient_data = {"zip_code": "12345-6789"}
        
        result = deidentifier.deidentify_patient(patient_data)
        
        # Should be generalized to first 3 digits + 00
        assert result["zip_code"] == "12300"

    def test_generalize_date(self):
        """Test date generalization."""
        from datetime import date
        
        deidentifier = SafeHarborDeidentifier()
        
        # Under 90 years old - keep year only
        patient_data = {"date_of_birth": date(1990, 6, 15)}
        result = deidentifier.deidentify_patient(patient_data)
        assert result["date_of_birth"] == 1990

    def test_over_90_age(self):
        """Test 90+ age handling."""
        from datetime import date
        
        deidentifier = SafeHarborDeidentifier()
        
        # Over 90 years old - remove DOB entirely
        patient_data = {"date_of_birth": date(1920, 1, 1)}
        result = deidentifier.deidentify_patient(patient_data)
        
        assert result["date_of_birth"] is None
        assert result.get("age_category") == "90+"

    def test_remove_contact_info(self):
        """Test contact information removal."""
        deidentifier = SafeHarborDeidentifier()
        
        patient_data = {
            "phone": "(555) 123-4567",
            "fax": "(555) 123-4568",
            "email": "john@example.com",
        }
        
        result = deidentifier.deidentify_patient(patient_data)
        
        assert result["phone"] == "[REDACTED]"
        assert result["fax"] == "[REDACTED]"
        assert result["email"] == "[REDACTED]"

    def test_remove_identifiers(self):
        """Test identifier removal."""
        deidentifier = SafeHarborDeidentifier()
        
        patient_data = {
            "ssn": "123-45-6789",
            "mrn": "MRN12345",
        }
        
        result = deidentifier.deidentify_patient(patient_data)
        
        assert result["ssn"] == "[REDACTED]"
        assert result["mrn"] == "[REDACTED]"

    def test_remove_geographic_data(self):
        """Test geographic data removal."""
        deidentifier = SafeHarborDeidentifier()
        
        patient_data = {
            "address_line1": "123 Main St",
            "address_line2": "Apt 4B",
            "city": "New York",
        }
        
        result = deidentifier.deidentify_patient(patient_data)
        
        assert result["address_line1"] == "[REDACTED]"
        assert result["address_line2"] == "[REDACTED]"
        assert result["city"] == "[REDACTED]"

    def test_preserve_non_phi(self):
        """Test that non-PHI fields are preserved."""
        deidentifier = SafeHarborDeidentifier()
        
        patient_data = {
            "first_name": "John",  # PHI - should be removed
            "gender": "male",      # Non-PHI - should be preserved
            "is_active": True,     # Non-PHI - should be preserved
        }
        
        result = deidentifier.deidentify_patient(patient_data)
        
        assert result["first_name"] == "[REDACTED]"
        assert result["gender"] == "male"
        assert result["is_active"] is True
