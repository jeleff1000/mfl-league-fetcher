from __future__ import annotations

import inspect
import json
from pathlib import Path

import pytest

from scripts.sota_recon.witness_gate import admissibility
from scripts.sota_recon.witness_gate.admissibility import (
    EvidenceCandidate,
    VerifiedDatasetCoverage,
    select_evidence,
)
from scripts.sota_recon.witness_gate.models import AdmissibilityRule, Grain, ObservationRole


def rule(
    rule_id: str,
    dataset_id: str,
    role: ObservationRole,
    precedence: int,
    year_start: int = 1920,
    year_end: int = 2025,
    atom_id: str = "passing.attempts",
    grain: Grain = Grain.PLAYER_GAME,
) -> AdmissibilityRule:
    return AdmissibilityRule(
        contract_version="1",
        rule_id=rule_id,
        atom_id=atom_id,
        dataset_id=dataset_id,
        role=role,
        year_start=year_start,
        year_end=year_end,
        precedence=precedence,
        grain=grain,
        season_type="REG",
    )


def candidate(
    dataset_id: str,
    value: float | None,
    lineage_id: str,
    *,
    provenance_kind: str,
    shard_manifest_id: str | None = None,
    claim_kind: str = "positive",
) -> EvidenceCandidate:
    return EvidenceCandidate(
        dataset_id=dataset_id,
        value=value,
        lineage_id=lineage_id,
        provenance_kind=provenance_kind,
        shard_manifest_id=shard_manifest_id,
        claim_kind=claim_kind,
    )


def coverage_registry(
    tmp_path: Path,
    *,
    dataset_id: str = "profootballarchives:player_game_participation",
    coverage_complete: bool = False,
    shard_hashes: tuple[str, ...] = ("a" * 64,),
):
    source, dataset = dataset_id.split(":", 1)
    manifest = tmp_path / f"{source}-{dataset}-IMPORT_MANIFEST.json"
    manifest.write_text(
        json.dumps(
            {
                "source": source,
                "dataset": dataset,
                "coverage_complete": coverage_complete,
                "manifest_pin": "ffassets-run:run-1:sha256:pin",
                "shard_manifests": [
                    {"shard_id": index, "sha256": value}
                    for index, value in enumerate(shard_hashes)
                ],
            }
        ),
        encoding="utf-8",
    )
    return admissibility.load_coverage_registry((manifest,))


def test_mirrors_cannot_outvote_higher_precedence_canonical_source() -> None:
    rules = (
        rule("canonical", "nfl", ObservationRole.AUTHORITATIVE, 10),
        rule("mirror-a", "mirror-a", ObservationRole.CORROBORATING, 50),
        rule("mirror-b", "mirror-b", ObservationRole.CORROBORATING, 50),
        rule("mirror-c", "mirror-c", ObservationRole.CORROBORATING, 50),
    )
    evidence = (
        candidate("nfl", 31, "nfl", provenance_kind="unpartitioned"),
        candidate("mirror-a", 30, "pfr", provenance_kind="unpartitioned"),
        candidate("mirror-b", 30, "pfr", provenance_kind="unpartitioned"),
        candidate("mirror-c", 30, "pfr", provenance_kind="unpartitioned"),
    )

    decision = select_evidence(
        atom_id="passing.attempts",
        year=2001,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=evidence,
        rules=rules,
    )

    assert decision.outcome == "authoritative"
    assert decision.value == 31
    assert decision.selected_dataset_ids == ("nfl",)


def test_inadmissible_box_score_and_wrong_era_cannot_promote() -> None:
    box_rule = rule("box", "box", ObservationRole.INADMISSIBLE, 10, atom_id="participation.played")
    modern_rule = rule("modern", "nfl", ObservationRole.AUTHORITATIVE, 10, year_start=1978)

    box = select_evidence(
        atom_id="participation.played",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(candidate("box", 1, "pfr", provenance_kind="unpartitioned"),),
        rules=(box_rule,),
    )
    modern = select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(candidate("nfl", 12, "nfl", provenance_kind="unpartitioned"),),
        rules=(modern_rule,),
    )

    assert box.outcome == "inadmissible"
    assert modern.outcome == "inadmissible"


def test_equal_precedence_disagreement_requires_collision_review() -> None:
    rules = (
        rule("a", "source-a", ObservationRole.CORROBORATING, 20),
        rule("b", "source-b", ObservationRole.CORROBORATING, 20),
    )

    decision = select_evidence(
        atom_id="passing.attempts",
        year=1960,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate("source-a", 20, "a", provenance_kind="unpartitioned"),
            candidate("source-b", 21, "b", provenance_kind="unpartitioned"),
        ),
        rules=rules,
    )

    assert decision.outcome == "collision_review"
    assert decision.value is None


def test_explicit_fallback_and_unique_identity_evidence_are_supported() -> None:
    fallback = rule("fallback", "statscrew", ObservationRole.FALLBACK, 90)
    identity = rule(
        "identity",
        "roster",
        ObservationRole.IDENTITY_ONLY,
        10,
        atom_id="identity.player_id",
        grain=Grain.PLAYER_SEASON,
    )

    fallback_decision = select_evidence(
        atom_id="passing.attempts",
        year=1940,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate("statscrew", 8, "statscrew", provenance_kind="unpartitioned"),
        ),
        rules=(fallback,),
    )
    identity_decision = select_evidence(
        atom_id="identity.player_id",
        year=1940,
        grain=Grain.PLAYER_SEASON,
        competition="NFL",
        season_type="REG",
        candidates=(candidate("roster", 123, "roster", provenance_kind="unpartitioned"),),
        rules=(identity,),
    )

    assert fallback_decision.outcome == "fallback"
    assert identity_decision.outcome == "corroborating"
    assert identity_decision.selected_role is ObservationRole.IDENTITY_ONLY


def test_new_gate_does_not_import_legacy_vote_module() -> None:
    assert "witness_votes" not in inspect.getsource(admissibility)


def test_verified_partial_partition_can_promote_positive_evidence(tmp_path: Path) -> None:
    registry = coverage_registry(tmp_path)
    decision = select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate(
                "profootballarchives:player_game_participation",
                12,
                "ffassets-run:run-1:sha256:pin",
                provenance_kind="partitioned",
                shard_manifest_id="a" * 64,
            ),
        ),
        rules=(
            rule(
                "pfa",
                "profootballarchives:player_game_participation",
                ObservationRole.CORROBORATING,
                10,
            ),
        ),
        coverage_registry=registry,
    )

    assert decision.outcome == "corroborating"
    assert decision.value == 12
    assert decision.selected_lineage_ids == ("a" * 64,)


def test_partial_partition_requires_exact_shard_lineage_for_positive_evidence(tmp_path: Path) -> None:
    registry = coverage_registry(tmp_path)
    decision = select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate(
                "profootballarchives:player_game_participation",
                12,
                "ffassets-run:run-1:sha256:pin",
                provenance_kind="partitioned",
            ),
        ),
        rules=(
            rule(
                "pfa",
                "profootballarchives:player_game_participation",
                ObservationRole.CORROBORATING,
                10,
            ),
        ),
        coverage_registry=registry,
    )

    assert decision.outcome == "inadmissible"


def test_partial_partition_rejects_bogus_nonblank_shard_lineage(tmp_path: Path) -> None:
    registry = coverage_registry(tmp_path)
    decision = select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate(
                "profootballarchives:player_game_participation",
                12,
                "ffassets-run:run-1:sha256:pin",
                provenance_kind="partitioned",
                shard_manifest_id="b" * 64,
            ),
        ),
        rules=(
            rule(
                "pfa",
                "profootballarchives:player_game_participation",
                ObservationRole.CORROBORATING,
                10,
            ),
        ),
        coverage_registry=registry,
    )

    assert decision.outcome == "inadmissible"


def test_partition_coverage_dataset_must_match_candidate(tmp_path: Path) -> None:
    registry = coverage_registry(tmp_path)
    decision = select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate(
                "statscrew:team_season_roster",
                12,
                "ffassets-run:run-1:sha256:pin",
                provenance_kind="partitioned",
                shard_manifest_id="a" * 64,
            ),
        ),
        rules=(
            rule(
                "statscrew",
                "statscrew:team_season_roster",
                ObservationRole.CORROBORATING,
                10,
            ),
        ),
        coverage_registry=registry,
    )

    assert decision.outcome == "inadmissible"


def test_partial_partition_cannot_prove_zero_or_absence(tmp_path: Path) -> None:
    registry = coverage_registry(tmp_path)
    candidates = (
        candidate(
            "profootballarchives:player_game_participation",
            0,
            "ffassets-run:run-1:sha256:pin",
            provenance_kind="partitioned",
            shard_manifest_id="a" * 64,
        ),
        candidate(
            "profootballarchives:player_game_participation",
            None,
            "ffassets-run:run-1:sha256:pin",
            provenance_kind="partitioned",
            claim_kind="absence",
        ),
    )
    admissibility_rule = rule(
        "pfa",
        "profootballarchives:player_game_participation",
        ObservationRole.CORROBORATING,
        10,
    )

    for item in candidates:
        decision = select_evidence(
            atom_id="passing.attempts",
            year=1946,
            grain=Grain.PLAYER_GAME,
            competition="NFL",
            season_type="REG",
            candidates=(item,),
            rules=(admissibility_rule,),
            coverage_registry=registry,
        )
        assert decision.outcome == "inadmissible"


def test_complete_partition_can_prove_zero_or_absence(tmp_path: Path) -> None:
    registry = coverage_registry(tmp_path, coverage_complete=True)
    admissibility_rule = rule(
        "pfa",
        "profootballarchives:player_game_participation",
        ObservationRole.CORROBORATING,
        10,
    )

    for value, claim_kind in ((0, "positive"), (None, "absence")):
        decision = select_evidence(
            atom_id="passing.attempts",
            year=1946,
            grain=Grain.PLAYER_GAME,
            competition="NFL",
            season_type="REG",
            candidates=(
                candidate(
                    "profootballarchives:player_game_participation",
                    value,
                    "ffassets-run:run-1:sha256:pin",
                    provenance_kind="partitioned",
                    claim_kind=claim_kind,
                ),
            ),
            rules=(admissibility_rule,),
            coverage_registry=registry,
        )
        assert decision.outcome == "corroborating"


def test_partitioned_positive_without_registry_is_inadmissible() -> None:
    decision = select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate(
                "profootballarchives:player_game_participation",
                12,
                "ffassets-run:run-1:sha256:pin",
                provenance_kind="partitioned",
                shard_manifest_id="a" * 64,
            ),
        ),
        rules=(
            rule(
                "pfa",
                "profootballarchives:player_game_participation",
                ObservationRole.CORROBORATING,
                10,
            ),
        ),
    )

    assert decision.outcome == "inadmissible"


def test_unpartitioned_candidate_cannot_carry_shard_leaf() -> None:
    with pytest.raises(ValueError, match="unpartitioned.*shard_manifest_id"):
        candidate(
            "nfl",
            12,
            "nfl",
            provenance_kind="unpartitioned",
            shard_manifest_id="a" * 64,
        )


def test_evidence_candidate_requires_explicit_provenance() -> None:
    with pytest.raises(TypeError, match="provenance_kind"):
        EvidenceCandidate(dataset_id="nfl", value=12, lineage_id="nfl")


def test_unpartitioned_positive_is_admissible_but_negative_claims_are_not() -> None:
    admissibility_rule = rule("nfl", "nfl", ObservationRole.AUTHORITATIVE, 10)
    expected = (
        (12, "positive", "authoritative"),
        (0, "positive", "inadmissible"),
        (None, "absence", "inadmissible"),
    )

    for value, claim_kind, expected_outcome in expected:
        decision = select_evidence(
            atom_id="passing.attempts",
            year=2001,
            grain=Grain.PLAYER_GAME,
            competition="NFL",
            season_type="REG",
            candidates=(
                candidate(
                    "nfl",
                    value,
                    "nfl",
                    provenance_kind="unpartitioned",
                    claim_kind=claim_kind,
                ),
            ),
            rules=(admissibility_rule,),
        )
        assert decision.outcome == expected_outcome


def _forged_coverage(**overrides) -> VerifiedDatasetCoverage:
    values = {
        "dataset_id": "profootballarchives:player_game_participation",
        "coverage_complete": False,
        "manifest_pin": "ffassets-run:run-1:sha256:pin",
        "observed_shard_manifest_ids": frozenset({"a" * 64}),
        **overrides,
    }
    coverage = object.__new__(VerifiedDatasetCoverage)
    for field_name, value in values.items():
        object.__setattr__(coverage, field_name, value)
    return coverage


def _select_with_registry(registry) -> object:
    dataset_id = "profootballarchives:player_game_participation"
    return select_evidence(
        atom_id="passing.attempts",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            candidate(
                dataset_id,
                12,
                "ffassets-run:run-1:sha256:pin",
                provenance_kind="partitioned",
                shard_manifest_id="a" * 64,
            ),
        ),
        rules=(rule("pfa", dataset_id, ObservationRole.CORROBORATING, 10),),
        coverage_registry=registry,
    )


def test_select_evidence_accepts_valid_verified_coverage_registry() -> None:
    dataset_id = "profootballarchives:player_game_participation"
    decision = _select_with_registry(
        {
            dataset_id: VerifiedDatasetCoverage(
                dataset_id,
                False,
                "ffassets-run:run-1:sha256:pin",
                frozenset({"a" * 64}),
            )
        }
    )

    assert decision.outcome == "corroborating"
    assert decision.selected_lineage_ids == ("a" * 64,)


@pytest.mark.parametrize(
    ("registry", "message"),
    [
        ({"wrong:dataset": _forged_coverage()}, "registry key.*coverage dataset_id"),
        ({"": _forged_coverage()}, "canonical dataset ID"),
        ({"profootballarchives:player_game_participation": object()}, "VerifiedDatasetCoverage"),
        (
            {"Bad Dataset": _forged_coverage(dataset_id="Bad Dataset")},
            "canonical dataset ID",
        ),
        (
            {"profootballarchives:player_game_participation": _forged_coverage(coverage_complete=1)},
            "coverage_complete.*boolean",
        ),
        (
            {"profootballarchives:player_game_participation": _forged_coverage(manifest_pin=" ")},
            "manifest_pin",
        ),
        (
            {
                "profootballarchives:player_game_participation": _forged_coverage(
                    manifest_pin=123
                )
            },
            "manifest_pin",
        ),
        (
            {
                "profootballarchives:player_game_participation": _forged_coverage(
                    observed_shard_manifest_ids=frozenset()
                )
            },
            "observed.*nonempty",
        ),
        (
            {
                "profootballarchives:player_game_participation": _forged_coverage(
                    observed_shard_manifest_ids=frozenset({"not-a-hash"})
                )
            },
            "64-hex",
        ),
        (
            {
                "profootballarchives:player_game_participation": _forged_coverage(
                    observed_shard_manifest_ids={"a" * 64}
                )
            },
            "frozenset",
        ),
    ],
)
def test_select_evidence_rejects_malformed_coverage_registry(registry, message: str) -> None:
    with pytest.raises((TypeError, ValueError), match=message):
        _select_with_registry(registry)


def test_verified_coverage_constructor_rejects_intrinsically_invalid_state() -> None:
    with pytest.raises(ValueError, match="observed.*nonempty"):
        VerifiedDatasetCoverage(
            "profootballarchives:player_game_participation", False, "pin", frozenset()
        )
