"""
Data Validation Modules

This package provides schema validation and data quality checking for
fantasy football data fetchers.

Modules:
    schema_validator: Validates DataFrame output against canonical schema
    data_quality: Anomaly detection and data integrity checks

Usage:
    from data_fetchers.validation import SchemaValidator, DataQualityValidator

    validator = SchemaValidator()
    errors = validator.validate_player_data(df)
    if errors:
        raise ValidationError(errors)

    quality = DataQualityValidator()
    issues = quality.check_player_data(df)
    for issue in issues:
        if issue.severity == 'error':
            raise DataQualityError(issue.message)
"""

from .schema_validator import (
    SchemaValidator,
    ValidationResult,
    ValidationError,
)
from .data_quality import (
    DataQualityValidator,
    DataQualityIssue,
    DataQualityReport,
)

__all__ = [
    # Schema validation
    "SchemaValidator",
    "ValidationResult",
    "ValidationError",
    # Data quality
    "DataQualityValidator",
    "DataQualityIssue",
    "DataQualityReport",
]
