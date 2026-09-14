from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.admissibility import (
    AdmissionDecision,
    EvidenceCandidate,
    VerifiedDatasetCoverage,
)
from scripts.sota_recon.witness_gate.gate_planes import (
    Finding,
    GateManifest,
    build_gate_manifests,
    build_manifest,
    candidate_admission_finding,
)
from scripts.sota_recon.witness_gate.models import (
    AdmissibilityRule,
    GatePlane,
    Grain,
    ObservationRole,
)


def test_source_outage_does_not_rewrite_candidate_evidence() -> None:
    candidate_findings = (
        Finding(
            code="CANDIDATE_ADMISSIBLE",
            severity="info",
            scope={"atom_id": "passing.attempts", "candidate_id": "c1"},
            evidence_refs=("proof:abc",),
            remediation="none",
        ),
    )
    source_pass = build_manifest(GatePlane.SOURCE_HEALTH, "source:1", ())
    source_fail = build_manifest(
        GatePlane.SOURCE_HEALTH,
        "source:2",
        (
            Finding(
                code="SOURCE_MISSING_SHARD",
                severity="fail",
                scope={"dataset_id": "nflcom.player_logs"},
                evidence_refs=("inventory:1",),
                remediation="restore or repin shard",
            ),
        ),
    )

    before = build_manifest(
        GatePlane.CANDIDATE_PROMOTION,
        "candidate:1",
        candidate_findings,
        upstream=(source_pass,),
    )
    after = build_manifest(
        GatePlane.CANDIDATE_PROMOTION,
        "candidate:2",
        candidate_findings,
        upstream=(source_fail,),
    )

    assert before.content_fingerprint == after.content_fingerprint
    assert before.manifest_fingerprint != after.manifest_fingerprint


def test_promotion_collision_does_not_mark_source_unhealthy() -> None:
    source = build_manifest(GatePlane.SOURCE_HEALTH, "source:1", ())
    candidate = build_manifest(
        GatePlane.CANDIDATE_PROMOTION,
        "candidate:1",
        (
            Finding(
                code="EVIDENCE_COLLISION",
                severity="review",
                scope={"atom_id": "fumbles.lost"},
                evidence_refs=("nfl:1", "pfr:1"),
                remediation="collision review",
            ),
        ),
        upstream=(source,),
    )

    assert source.status == "pass"
    assert candidate.status == "review"


def test_release_fails_when_required_upstream_fails() -> None:
    global_health = build_manifest(
        GatePlane.GLOBAL_RESEARCH_HEALTH,
        "global:1",
        (
            Finding(
                code="UNMAPPED_FIELD",
                severity="fail",
                scope={"field": "mystery"},
                evidence_refs=("inventory:1",),
                remediation="contract the field",
            ),
        ),
    )
    candidate = build_manifest(GatePlane.CANDIDATE_PROMOTION, "candidate:1", ())

    release = build_manifest(
        GatePlane.RELEASE,
        "release:1",
        (),
        upstream=(global_health, candidate),
    )

    assert release.status == "fail"
    assert any(item.code == "UPSTREAM_GATE_FAILED" for item in release.findings)


def test_gate_dependency_graph_is_one_way() -> None:
    release = GateManifest.empty(GatePlane.RELEASE, "release:1")

    with pytest.raises(ValueError, match="cannot depend"):
        build_manifest(GatePlane.SOURCE_HEALTH, "source:1", (), upstream=(release,))


def test_new_law_violation_is_hard_failure_not_review() -> None:
    manifest = build_manifest(
        GatePlane.GLOBAL_RESEARCH_HEALTH,
        "global:law",
        (
            Finding(
                code="LAW_VIOLATION",
                severity="fail",
                scope={"law_id": "fumbles_lost_le_fumbles"},
                evidence_refs=("row:1",),
                remediation="fix or fine-tune the versioned law contract",
            ),
        ),
    )

    assert manifest.status == "fail"


def test_missing_pfa_shard_degrades_source_health_only() -> None:
    before = build_gate_manifests(
        source_universe_version="sources-v1",
        candidate_version="candidate-v1",
        candidate_findings=(
            Finding("CANDIDATE_ADMISSIBLE", "info", {"candidate_id": "c1"}, ("leaf:1",), "none"),
        ),
    )
    after = build_gate_manifests(
        source_universe_version="sources-v1",
        candidate_version="candidate-v1",
        source_findings=(
            Finding(
                "SOURCE_MISSING_SHARD",
                "fail",
                {"dataset_id": "profootballarchives:player_game_participation", "shard_id": 19},
                ("manifest:pfa",),
                "restore or repin shard",
            ),
        ),
        candidate_findings=before[GatePlane.CANDIDATE_PROMOTION].findings,
    )

    assert after[GatePlane.SOURCE_HEALTH].status == "fail"
    assert after[GatePlane.CANDIDATE_PROMOTION].status == "pass"
    assert (
        before[GatePlane.CANDIDATE_PROMOTION].content_fingerprint
        == after[GatePlane.CANDIDATE_PROMOTION].content_fingerprint
    )


def test_unresolved_collision_blocks_release() -> None:
    manifests = build_gate_manifests(
        source_universe_version="sources-v1",
        candidate_version="candidate-v1",
        unresolved_collision_ids=("collision-2", "collision-1"),
    )

    assert manifests[GatePlane.GLOBAL_RESEARCH_HEALTH].status == "pass"
    assert manifests[GatePlane.SOURCE_HEALTH].status == "pass"
    assert manifests[GatePlane.CANDIDATE_PROMOTION].status == "pass"
    release = manifests[GatePlane.RELEASE]
    assert release.status == "fail"
    assert [item.scope["collision_id"] for item in release.findings] == [
        "collision-1",
        "collision-2",
    ]


def test_partial_pfa_positive_evidence_can_promote_candidate() -> None:
    dataset_id = "profootballarchives:player_game_participation"
    leaf = "a" * 64
    pin = "ffassets-run:run-1:sha256:pin"
    candidate_finding = candidate_admission_finding(
        "c1",
        atom_id="participation.played",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(EvidenceCandidate(dataset_id, 1, pin, "partitioned", leaf),),
        rules=(
            AdmissibilityRule(
                "1", "pfa-participation", "participation.played", dataset_id,
                ObservationRole.CORROBORATING, 1920, 2025, 10,
                Grain.PLAYER_GAME, "NFL", "REG",
            ),
        ),
        coverage_registry={dataset_id: VerifiedDatasetCoverage(dataset_id, False, pin, frozenset({leaf}))},
    )
    manifests = build_gate_manifests(
        source_universe_version="sources-v1",
        candidate_version="candidate-v1",
        source_findings=(
            Finding("SOURCE_MISSING_SHARD", "fail", {"shard_id": 19}, ("manifest:pfa",), "restore"),
        ),
        candidate_findings=(candidate_finding,),
    )

    assert manifests[GatePlane.CANDIDATE_PROMOTION].status == "pass"
    assert manifests[GatePlane.CANDIDATE_PROMOTION].findings[0].evidence_refs == (leaf,)
    assert manifests[GatePlane.RELEASE].status == "fail"


def test_partial_pfa_zero_remains_blocked_from_candidate_promotion() -> None:
    dataset_id = "profootballarchives:player_game_participation"
    leaf = "a" * 64
    pin = "ffassets-run:run-1:sha256:pin"
    manifest = build_gate_manifests(
        source_universe_version="sources-v1",
        candidate_version="candidate-v1",
        candidate_findings=(
            candidate_admission_finding(
                "c1",
                atom_id="participation.played",
                year=1946,
                grain=Grain.PLAYER_GAME,
                competition="NFL",
                season_type="REG",
                candidates=(EvidenceCandidate(dataset_id, 0, pin, "partitioned", leaf),),
                rules=(
                    AdmissibilityRule(
                        "1", "pfa-participation", "participation.played", dataset_id,
                        ObservationRole.CORROBORATING, 1920, 2025, 10,
                        Grain.PLAYER_GAME, "NFL", "REG",
                    ),
                ),
                coverage_registry={
                    dataset_id: VerifiedDatasetCoverage(dataset_id, False, pin, frozenset({leaf}))
                },
            ),
        ),
    )[GatePlane.CANDIDATE_PROMOTION]

    assert manifest.status == "fail"
    assert manifest.findings[0].code == "CANDIDATE_INADMISSIBLE"


@pytest.mark.parametrize(
    ("value", "claim_kind", "leaf", "with_registry"),
    [
        (1, "positive", "b" * 64, True),
        (1, "positive", "", True),
        (1, "positive", "a" * 64, False),
        (0, "positive", "a" * 64, True),
        (None, "absence", None, True),
    ],
)
def test_candidate_promotion_rechecks_partial_partition_contract(
    value: int | None, claim_kind: str, leaf: str | None, with_registry: bool
) -> None:
    dataset_id = "profootballarchives:player_game_participation"
    pin = "ffassets-run:run-1:sha256:pin"
    registered_leaf = "a" * 64
    finding = candidate_admission_finding(
        "c1",
        atom_id="participation.played",
        year=1946,
        grain=Grain.PLAYER_GAME,
        competition="NFL",
        season_type="REG",
        candidates=(
            EvidenceCandidate(dataset_id, value, pin, "partitioned", leaf, claim_kind),
        ),
        rules=(
            AdmissibilityRule(
                "1", "pfa-participation", "participation.played", dataset_id,
                ObservationRole.CORROBORATING, 1920, 2025, 10,
                Grain.PLAYER_GAME, "NFL", "REG",
            ),
        ),
        coverage_registry=(
            {dataset_id: VerifiedDatasetCoverage(dataset_id, False, pin, frozenset({registered_leaf}))}
            if with_registry else {}
        ),
    )

    assert finding.code == "CANDIDATE_INADMISSIBLE"
    assert finding.severity == "fail"


def test_candidate_promotion_rejects_fabricated_admission_decision_api() -> None:
    fabricated = AdmissionDecision(
        "authoritative", 1, ("pfa",), ("forged-leaf",), ObservationRole.AUTHORITATIVE, ("rule",)
    )

    with pytest.raises(TypeError):
        candidate_admission_finding("c1", fabricated)


def test_candidate_promotion_rejects_mismatched_coverage_registry_identity() -> None:
    dataset_id = "profootballarchives:player_game_participation"
    coverage = VerifiedDatasetCoverage(dataset_id, False, "pin", frozenset({"a" * 64}))

    with pytest.raises(ValueError, match="registry key.*coverage dataset_id"):
        candidate_admission_finding(
            "c1",
            atom_id="participation.played",
            year=1946,
            grain=Grain.PLAYER_GAME,
            competition="NFL",
            season_type="REG",
            candidates=(EvidenceCandidate(dataset_id, 1, "pin", "partitioned", "a" * 64),),
            rules=(
                AdmissibilityRule(
                    "1", "pfa", "participation.played", dataset_id,
                    ObservationRole.CORROBORATING, 1920, 2025, 10,
                    Grain.PLAYER_GAME, "NFL", "REG",
                ),
            ),
            coverage_registry={"wrong:dataset": coverage},
        )


def test_gate_versions_must_be_nonempty_strings() -> None:
    with pytest.raises(ValueError, match="source_universe_version and candidate_version"):
        build_gate_manifests(source_universe_version=None, candidate_version="candidate-v1")
