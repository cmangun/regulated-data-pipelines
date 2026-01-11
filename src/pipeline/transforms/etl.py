"""
Healthcare Data Transformations

HIPAA-compliant data transformation operations with full audit trail.
All transforms are deterministic and reproducible.
"""

from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


# =============================================================================
# TRANSFORM RESULT
# =============================================================================


class TransformResult(BaseModel, Generic[T]):
    """Result of a transformation operation."""
    
    success: bool
    data: T | None = None
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
    
    # Metrics
    input_count: int = 0
    output_count: int = 0
    filtered_count: int = 0
    failed_count: int = 0
    
    # Audit
    transform_name: str = ""
    started_at: datetime = Field(default_factory=datetime.utcnow)
    completed_at: datetime | None = None
    duration_ms: int = 0
    
    # Lineage
    input_hash: str = ""
    output_hash: str = ""
    
    @property
    def transform_ratio(self) -> float:
        """Ratio of output to input records."""
        if self.input_count == 0:
            return 0.0
        return self.output_count / self.input_count


# =============================================================================
# BASE TRANSFORM
# =============================================================================


@dataclass
class BaseTransform(ABC, Generic[T]):
    """Base class for all data transformations."""
    
    name: str
    description: str = ""
    version: str = "1.0.0"
    
    @abstractmethod
    def transform(self, data: T) -> TransformResult[T]:
        """Apply transformation to data."""
        pass
    
    def _compute_hash(self, data: Any) -> str:
        """Compute deterministic hash of data."""
        import json
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]


# =============================================================================
# RECORD TRANSFORMS
# =============================================================================


@dataclass
class FilterTransform(BaseTransform[list[dict[str, Any]]]):
    """Filter records based on a predicate."""
    
    predicate: Callable[[dict[str, Any]], bool] = field(default=lambda x: True)
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Filter records."""
        import time
        start = time.time()
        
        result_data = []
        filtered = 0
        
        for record in data:
            try:
                if self.predicate(record):
                    result_data.append(record)
                else:
                    filtered += 1
            except Exception:
                filtered += 1
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            filtered_count=filtered,
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


@dataclass
class MapTransform(BaseTransform[list[dict[str, Any]]]):
    """Apply a mapping function to each record."""
    
    mapper: Callable[[dict[str, Any]], dict[str, Any]] = field(default=lambda x: x)
    skip_errors: bool = False
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Map records."""
        import time
        start = time.time()
        
        result_data = []
        errors = []
        failed = 0
        
        for idx, record in enumerate(data):
            try:
                mapped = self.mapper(record)
                result_data.append(mapped)
            except Exception as e:
                failed += 1
                errors.append(f"Record {idx}: {str(e)}")
                if not self.skip_errors:
                    break
        
        duration = int((time.time() - start) * 1000)
        success = failed == 0 or self.skip_errors
        
        return TransformResult(
            success=success,
            data=result_data if success else None,
            errors=errors,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            failed_count=failed,
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data) if result_data else "",
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


@dataclass
class SelectTransform(BaseTransform[list[dict[str, Any]]]):
    """Select specific fields from records."""
    
    fields: list[str] = field(default_factory=list)
    rename: dict[str, str] = field(default_factory=dict)
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Select fields."""
        import time
        start = time.time()
        
        result_data = []
        
        for record in data:
            new_record = {}
            for f in self.fields:
                value = record.get(f)
                new_key = self.rename.get(f, f)
                new_record[new_key] = value
            result_data.append(new_record)
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


@dataclass
class DeriveTransform(BaseTransform[list[dict[str, Any]]]):
    """Derive new fields from existing data."""
    
    derivations: dict[str, Callable[[dict[str, Any]], Any]] = field(default_factory=dict)
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Derive new fields."""
        import time
        start = time.time()
        
        result_data = []
        errors = []
        
        for idx, record in enumerate(data):
            new_record = record.copy()
            
            for field_name, derivation in self.derivations.items():
                try:
                    new_record[field_name] = derivation(record)
                except Exception as e:
                    errors.append(f"Record {idx}, field {field_name}: {str(e)}")
                    new_record[field_name] = None
            
            result_data.append(new_record)
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            warnings=errors if errors else [],
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


@dataclass  
class DeduplicateTransform(BaseTransform[list[dict[str, Any]]]):
    """Remove duplicate records based on key fields."""
    
    key_fields: list[str] = field(default_factory=list)
    keep: str = "first"  # "first" or "last"
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Deduplicate records."""
        import time
        start = time.time()
        
        seen: dict[tuple, dict[str, Any]] = {}
        
        for record in data:
            key = tuple(record.get(f) for f in self.key_fields)
            
            if key not in seen or self.keep == "last":
                seen[key] = record
        
        result_data = list(seen.values())
        duplicates = len(data) - len(result_data)
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            filtered_count=duplicates,
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


# =============================================================================
# HEALTHCARE-SPECIFIC TRANSFORMS
# =============================================================================


@dataclass
class AgeCalculatorTransform(BaseTransform[list[dict[str, Any]]]):
    """Calculate age from date of birth."""
    
    dob_field: str = "date_of_birth"
    output_field: str = "age"
    reference_date: datetime | None = None
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Calculate ages."""
        from datetime import date
        import time
        start = time.time()
        
        ref = self.reference_date or datetime.now()
        if isinstance(ref, datetime):
            ref = ref.date()
        
        result_data = []
        errors = []
        
        for idx, record in enumerate(data):
            new_record = record.copy()
            dob = record.get(self.dob_field)
            
            if dob:
                try:
                    if isinstance(dob, str):
                        dob = datetime.fromisoformat(dob).date()
                    elif isinstance(dob, datetime):
                        dob = dob.date()
                    
                    if isinstance(dob, date):
                        age = ref.year - dob.year - (
                            (ref.month, ref.day) < (dob.month, dob.day)
                        )
                        new_record[self.output_field] = age
                    else:
                        new_record[self.output_field] = None
                except Exception as e:
                    errors.append(f"Record {idx}: {str(e)}")
                    new_record[self.output_field] = None
            else:
                new_record[self.output_field] = None
            
            result_data.append(new_record)
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            warnings=errors if errors else [],
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


@dataclass
class ICD10ValidatorTransform(BaseTransform[list[dict[str, Any]]]):
    """Validate and normalize ICD-10 diagnosis codes."""
    
    code_field: str = "diagnosis_code"
    output_valid_field: str = "is_valid_icd10"
    normalize: bool = True
    ICD10_PATTERN: str = r"^[A-Z]\d{2}(\.\d{1,4})?$"
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Validate ICD-10 codes."""
        import re
        import time
        start = time.time()
        
        pattern = re.compile(self.ICD10_PATTERN)
        result_data = []
        invalid_count = 0
        
        for record in data:
            new_record = record.copy()
            code = record.get(self.code_field)
            
            if code:
                code_str = str(code).upper().strip()
                
                if self.normalize:
                    code_str = re.sub(r"[^A-Z0-9.]", "", code_str)
                    new_record[self.code_field] = code_str
                
                is_valid = bool(pattern.match(code_str))
                new_record[self.output_valid_field] = is_valid
                
                if not is_valid:
                    invalid_count += 1
            else:
                new_record[self.output_valid_field] = None
            
            result_data.append(new_record)
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            failed_count=invalid_count,
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )


@dataclass
class NPIValidatorTransform(BaseTransform[list[dict[str, Any]]]):
    """Validate NPI (National Provider Identifier) numbers."""
    
    npi_field: str = "npi"
    output_valid_field: str = "is_valid_npi"
    
    def transform(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Validate NPIs using Luhn algorithm."""
        import time
        start = time.time()
        
        result_data = []
        invalid_count = 0
        
        for record in data:
            new_record = record.copy()
            npi = record.get(self.npi_field)
            
            if npi:
                is_valid = self._validate_npi(str(npi))
                new_record[self.output_valid_field] = is_valid
                
                if not is_valid:
                    invalid_count += 1
            else:
                new_record[self.output_valid_field] = None
            
            result_data.append(new_record)
        
        duration = int((time.time() - start) * 1000)
        
        return TransformResult(
            success=True,
            data=result_data,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(result_data),
            failed_count=invalid_count,
            input_hash=self._compute_hash(data),
            output_hash=self._compute_hash(result_data),
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )
    
    def _validate_npi(self, npi: str) -> bool:
        """Validate NPI using Luhn algorithm with 80840 prefix."""
        if not npi.isdigit() or len(npi) != 10:
            return False
        
        npi_with_prefix = "80840" + npi
        total = 0
        
        for i, digit in enumerate(reversed(npi_with_prefix)):
            n = int(digit)
            if i % 2 == 0:
                n *= 2
                if n > 9:
                    n -= 9
            total += n
        
        return total % 10 == 0


# =============================================================================
# TRANSFORM PIPELINE
# =============================================================================


class TransformPipeline:
    """
    Chain multiple transforms together.
    
    Executes transforms in sequence with full lineage tracking.
    """
    
    def __init__(self, name: str = "pipeline"):
        self.name = name
        self.transforms: list[BaseTransform] = []
        self.results: list[TransformResult] = []
    
    def add(self, transform: BaseTransform) -> "TransformPipeline":
        """Add a transform to the pipeline."""
        self.transforms.append(transform)
        return self
    
    def execute(
        self,
        data: list[dict[str, Any]],
    ) -> TransformResult[list[dict[str, Any]]]:
        """Execute all transforms in sequence."""
        import time
        start = time.time()
        
        self.results = []
        current_data = data
        total_filtered = 0
        total_failed = 0
        all_errors: list[str] = []
        all_warnings: list[str] = []
        
        input_hash = self._compute_hash(data)
        
        for transform in self.transforms:
            result = transform.transform(current_data)
            self.results.append(result)
            
            total_filtered += result.filtered_count
            total_failed += result.failed_count
            all_warnings.extend(result.warnings)
            
            if not result.success:
                all_errors.extend(result.errors)
                return TransformResult(
                    success=False,
                    data=None,
                    errors=all_errors,
                    warnings=all_warnings,
                    transform_name=f"{self.name}:{transform.name}",
                    input_count=len(data),
                    output_count=0,
                    filtered_count=total_filtered,
                    failed_count=total_failed,
                    input_hash=input_hash,
                    output_hash="",
                    duration_ms=int((time.time() - start) * 1000),
                    completed_at=datetime.utcnow(),
                )
            
            current_data = result.data or []
        
        duration = int((time.time() - start) * 1000)
        output_hash = self._compute_hash(current_data)
        
        return TransformResult(
            success=True,
            data=current_data,
            errors=all_errors,
            warnings=all_warnings,
            transform_name=self.name,
            input_count=len(data),
            output_count=len(current_data),
            filtered_count=total_filtered,
            failed_count=total_failed,
            input_hash=input_hash,
            output_hash=output_hash,
            duration_ms=duration,
            completed_at=datetime.utcnow(),
        )
    
    def _compute_hash(self, data: Any) -> str:
        """Compute deterministic hash of data."""
        import json
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(json_str.encode()).hexdigest()[:16]
    
    def get_lineage(self) -> list[dict[str, Any]]:
        """Get lineage information for all executed transforms."""
        return [
            {
                "transform": r.transform_name,
                "input_count": r.input_count,
                "output_count": r.output_count,
                "filtered_count": r.filtered_count,
                "input_hash": r.input_hash,
                "output_hash": r.output_hash,
                "duration_ms": r.duration_ms,
            }
            for r in self.results
        ]


# =============================================================================
# EXPORTS
# =============================================================================


__all__ = [
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
