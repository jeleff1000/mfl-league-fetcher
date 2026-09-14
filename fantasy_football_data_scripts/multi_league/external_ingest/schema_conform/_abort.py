"""Abort exception + failure dataclasses for the 4 hard-abort gates."""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum


EXIT_CODE_SCHEMA_CONFORM_ABORT = 17  # distinct from generic exit codes


class AbortGate(Enum):
    UNMAPPED_REQUIRED = 1  # required IDENTITY slot has no binding
    LOW_CONFIDENCE_REQUIRED = 2  # required IDENTITY bound but score < CONFIDENCE_FLOOR
    SLOT_COLLISION = 3  # two source cols both ≥ floor for same slot, gap < gap
    COVERAGE_BELOW_THRESHOLD = 4  # post-derivation coverage / consistency failure


SUGGESTIONS = {
    AbortGate.UNMAPPED_REQUIRED: "Source file may be missing this column entirely, or its name/values diverge "
    "from any DDL slot.",
    AbortGate.LOW_CONFIDENCE_REQUIRED: "Closest match was below confidence threshold. Check sample values — does the "
    "source col contain the data you'd expect for this slot?",
    AbortGate.SLOT_COLLISION: "Two source columns both look like good matches. Most common cause: file contains "
    "both manager_guid and opponent_guid and we couldn't tell which is which "
    "(co-occurrence anchor was weak).",
    AbortGate.COVERAGE_BELOW_THRESHOLD: "Manager identity resolved for fewer rows than expected. Either a manager name "
    "doesn't match canonical (whitespace, suffix, unicode), or rows are missing "
    "manager values.",
}


@dataclass
class SchemaConformFailure:
    table: str
    gate: AbortGate
    ddl_slot: str | None
    source_cols_considered: list[str]
    sample_source_values: dict[str, list[str]]
    best_score: float | None
    second_best_score: float | None
    second_best_slot: str | None
    suggested_action: str
    invariant_failures: list[str]


@dataclass
class FuzzyAmbiguityFailure(SchemaConformFailure):
    """Specialization carrying the top-N fuzzy candidates for UI disambiguation."""

    source_manager_name: str = ""
    candidates: list[tuple[str, int]] = field(default_factory=list)


class SchemaConformAbort(Exception):
    """Raised when any of the 4 hard-abort gates fires."""

    def __init__(self, failures: list[SchemaConformFailure]):
        self.failures = failures
        first = failures[0] if failures else None
        msg = first and f"{first.gate.name}: {first.suggested_action}" or "abort"
        super().__init__(msg)
