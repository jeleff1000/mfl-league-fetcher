from multi_league.external_ingest.schema_conform._abort import (
    AbortGate,
    SchemaConformAbort,
    SchemaConformFailure,
    FuzzyAmbiguityFailure,
    EXIT_CODE_SCHEMA_CONFORM_ABORT,
)


def test_abort_gates_exist():
    assert AbortGate.UNMAPPED_REQUIRED.value == 1
    assert AbortGate.LOW_CONFIDENCE_REQUIRED.value == 2
    assert AbortGate.SLOT_COLLISION.value == 3
    assert AbortGate.COVERAGE_BELOW_THRESHOLD.value == 4


def test_failure_dataclass_required_fields():
    f = SchemaConformFailure(
        table="matchup",
        gate=AbortGate.UNMAPPED_REQUIRED,
        ddl_slot="manager_guid",
        source_cols_considered=["mystery_id"],
        sample_source_values={"mystery_id": ["abc", "def"]},
        best_score=None,
        second_best_score=None,
        second_best_slot=None,
        suggested_action="Source missing this column or values diverge from any DDL slot.",
        invariant_failures=[],
    )
    assert f.gate == AbortGate.UNMAPPED_REQUIRED


def test_fuzzy_ambiguity_failure_has_candidates():
    f = FuzzyAmbiguityFailure(
        table="matchup",
        gate=AbortGate.COVERAGE_BELOW_THRESHOLD,
        ddl_slot="manager_guid",
        source_cols_considered=["manager"],
        sample_source_values={"manager": ["Dan"]},
        best_score=None,
        second_best_score=None,
        second_best_slot=None,
        suggested_action="Multiple matches.",
        invariant_failures=[],
        source_manager_name="Dan",
        candidates=[("Daniel", 92), ("Dave", 90)],
    )
    assert f.candidates == [("Daniel", 92), ("Dave", 90)]


def test_abort_carries_failures():
    failures = [
        SchemaConformFailure(
            table="matchup",
            gate=AbortGate.UNMAPPED_REQUIRED,
            ddl_slot="manager_guid",
            source_cols_considered=[],
            sample_source_values={},
            best_score=None,
            second_best_score=None,
            second_best_slot=None,
            suggested_action="x",
            invariant_failures=[],
        )
    ]
    err = SchemaConformAbort(failures=failures)
    assert err.failures == failures


def test_exit_code_distinct():
    assert EXIT_CODE_SCHEMA_CONFORM_ABORT == 17  # any non-zero, distinct value
