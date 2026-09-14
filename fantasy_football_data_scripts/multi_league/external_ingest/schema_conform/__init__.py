"""schema_conform — PHASE 1.7 DDL-conformance for external uploads."""

from ._orchestrator import run
from ._abort import (
    SchemaConformAbort,
    AbortGate,
    SchemaConformFailure,
    FuzzyAmbiguityFailure,
    EXIT_CODE_SCHEMA_CONFORM_ABORT,
)

__all__ = [
    "run",
    "SchemaConformAbort",
    "AbortGate",
    "SchemaConformFailure",
    "FuzzyAmbiguityFailure",
    "EXIT_CODE_SCHEMA_CONFORM_ABORT",
]
