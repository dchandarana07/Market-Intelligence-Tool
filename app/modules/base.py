"""
Base module interface for Market Intelligence modules.

All data collection modules (Jobs, Courses, Trends, Lightcast) must implement
this interface to ensure consistent behavior across the system.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional
import pandas as pd


class ModuleStatus(Enum):
    """Status of a module execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"  # Some data collected but with errors


@dataclass
class ValidationError:
    """Represents a validation error for an input field."""
    field: str
    message: str


@dataclass
class ValidationResult:
    """Result of input validation."""
    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)

    @classmethod
    def success(cls) -> "ValidationResult":
        return cls(is_valid=True, errors=[])

    @classmethod
    def failure(cls, errors: list[ValidationError]) -> "ValidationResult":
        return cls(is_valid=False, errors=errors)

    def add_error(self, field: str, message: str) -> None:
        self.errors.append(ValidationError(field=field, message=message))
        self.is_valid = False


@dataclass
class ModuleResult:
    """Result of a module execution."""
    status: ModuleStatus
    data: dict[str, pd.DataFrame] = field(default_factory=dict)  # tab_name -> DataFrame
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    @property
    def total_rows(self) -> int:
        return sum(len(df) for df in self.data.values())

    @classmethod
    def success(
        cls,
        data: dict[str, pd.DataFrame],
        metadata: Optional[dict] = None,
        warnings: Optional[list[str]] = None,
    ) -> "ModuleResult":
        return cls(
            status=ModuleStatus.COMPLETED,
            data=data,
            metadata=metadata or {},
            warnings=warnings or [],
        )

    @classmethod
    def failure(cls, errors: list[str]) -> "ModuleResult":
        return cls(status=ModuleStatus.FAILED, errors=errors)

    @classmethod
    def partial(
        cls,
        data: dict[str, pd.DataFrame],
        errors: list[str],
        warnings: Optional[list[str]] = None,
    ) -> "ModuleResult":
        return cls(
            status=ModuleStatus.PARTIAL,
            data=data,
            errors=errors,
            warnings=warnings or [],
        )


@dataclass
class InputField:
    """Definition of an input field for a module."""
    name: str
    label: str
    field_type: str  # text, number, select, multiselect, checkbox
    required: bool = False
    default: Any = None
    placeholder: str = ""
    help_text: str = ""
    options: Optional[list[dict[str, str]]] = None  # For select/multiselect
    min_value: Optional[float] = None  # For numbers
    max_value: Optional[float] = None  # For numbers
    is_advanced: bool = False  # Show in advanced section


@dataclass
class OutputColumn:
    """Definition of an output column."""
    name: str
    description: str
    data_type: str  # string, number, date, url, list


class BaseModule(ABC):
    """
    Abstract base class for all Market Intelligence modules.

    Each module must implement:
    - name: Unique identifier
    - display_name: Human-readable name
    - description: What the module does
    - input_fields: List of input field definitions
    - output_columns: List of output column definitions
    - validate_inputs(): Input validation
    - execute(): Main execution logic
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for the module (e.g., 'jobs', 'courses')."""
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """Human-readable name (e.g., 'Job Postings')."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """Description of what the module does."""
        pass

    @property
    @abstractmethod
    def input_fields(self) -> list[InputField]:
        """List of input fields for this module."""
        pass

    @property
    @abstractmethod
    def output_columns(self) -> dict[str, list[OutputColumn]]:
        """
        Output columns by sheet/tab name.
        E.g., {"jobs": [...], "bls_data": [...]}
        """
        pass

    @abstractmethod
    def validate_inputs(self, inputs: dict[str, Any]) -> ValidationResult:
        """
        Validate the inputs before execution.

        Args:
            inputs: Dictionary of input field names to values.

        Returns:
            ValidationResult with is_valid and any errors.
        """
        pass

    @abstractmethod
    async def execute(self, inputs: dict[str, Any]) -> ModuleResult:
        """
        Execute the module with the given inputs.

        Args:
            inputs: Validated input dictionary.

        Returns:
            ModuleResult with status, data, errors, and metadata.
        """
        pass

    def get_default_inputs(self) -> dict[str, Any]:
        """Get default values for all input fields."""
        return {
            field.name: field.default
            for field in self.input_fields
            if field.default is not None
        }

    def get_basic_fields(self) -> list[InputField]:
        """Get only the basic (non-advanced) input fields."""
        return [f for f in self.input_fields if not f.is_advanced]

    def get_advanced_fields(self) -> list[InputField]:
        """Get only the advanced input fields."""
        return [f for f in self.input_fields if f.is_advanced]

    def is_available(self) -> bool:
        """
        Check if this module is available (dependencies met).
        Override in subclasses to check API keys, etc.
        """
        return True

    def get_availability_message(self) -> Optional[str]:
        """
        Return a message explaining why the module is unavailable.
        Override in subclasses.
        """
        return None
