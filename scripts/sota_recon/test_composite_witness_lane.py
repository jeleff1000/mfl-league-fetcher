"""Behavior tests for the closed composite-witness contract.

Each test names the production mutation it is intended to catch so this suite
guards the lane's public behavior rather than its implementation details.
"""

from __future__ import annotations

import json
from pathlib import Path

from scripts.sota_recon.composite_witness_lane import (
    CompositeSpec,
    LaneObservation,
    LaneResult,
    artifact_manifest_sha256,
    load_specs,
    receipt_is_current,
    spec_receipt_is_current,
    validate_specs,
    write_receipt,
)


PFR_AWARD_SOURCES = (
    "pfr_adj_passing",
    "pfr_adv_defense",
    "pfr_adv_defense_post",
    "pfr_adv_recrush",
    "pfr_adv_recrush_post",
    "pfr_adv_rushrec",
    "pfr_adv_rushrec_post",
    "pfr_defense_post",
    "pfr_games_played",
    "pfr_kicking_post",
    "pfr_passing_adv_post",
    "pfr_passing_adv_season",
    "pfr_passing_post",
    "pfr_player_defense",
    "pfr_player_kicking",
    "pfr_player_punting",
    "pfr_player_returns",
    "pfr_player_scoring",
    "pfr_player_season_passing",
    "pfr_player_season_rec_rush",
    "pfr_player_season_rush_rec",
    "pfr_punting_post",
    "pfr_recrush_post",
    "pfr_returns_post",
    "pfr_rushrec_post",
    "pfr_scoring_post",
)

NFLCOM_L7_TABLES = (
    ("nflcom_player_situational", "Field Position|player_situational_L7"),
    ("nflcom_player_situational", "Game Halves|player_situational_L7"),
    ("nflcom_player_situational", "Home vs Road|player_situational_L7"),
    ("nflcom_player_situational", "Margin of Victory|player_situational_L7"),
    ("nflcom_player_situational", "Point Differential|player_situational_L7"),
    ("nflcom_player_situational", "Quarters|player_situational_L7"),
    ("nflcom_player_situational", "Stadium Surfaces|player_situational_L7"),
    ("nflcom_player_splits", "Days|player_splits_L7"),
    ("nflcom_player_splits", "Months|player_splits_L7"),
    ("nflcom_player_splits", "Opponents by Group|player_splits_L7"),
    ("nflcom_player_splits", "Opponents by Team|player_splits_L7"),
    ("nflcom_player_splits", "Outcomes|player_splits_L7"),
    ("nflcom_player_splits", "Stadiums|player_splits_L7"),
)


def row_key(source: str, table_key: str, column: str) -> str:
    return f"{source}|{table_key}|{column}"


def authorized_row_keys() -> set[str]:
    """Hand-curated authorization, independent from the checked-in contract."""
    keys = {row_key(source, "*", "awards") for source in PFR_AWARD_SOURCES}
    keys.update(
        row_key(source, table_key, column)
        for source, table_key in NFLCOM_L7_TABLES
        for column in ("1_19", "20_29", "30_39", "40_49", "50_59", "60")
    )
    keys.update(
        row_key(source, "*", f"fg{kind}{bucket}")
        for source in ("pfr_player_kicking", "pfr_kicking_post")
        for kind in ("m", "a")
        for bucket in range(1, 6)
    )
    keys.update(
        {
            row_key("pfr_player_scoring", "*", "two_pt_md"),
            row_key("pfr_scoring_post", "*", "two_pt_md"),
        }
    )
    return keys


def fixture_spec(**changes: object) -> CompositeSpec:
    values: dict[str, object] = {
        "spec_id": "fixture",
        "cohort": "fixture",
        "row_keys": (row_key("fixture_source", "fixture_table", "fixture_column"),),
        "sources": ("fixture_source",),
        "derivation": "FIXTURE_NAMED_DERIVATION",
        "kind": "SCALAR_OBSERVATION",
        "targets": ("fixture_target",),
        "unit": "count",
        "natural_grain": "player_game",
        "aggregation_class": "SUM",
        "crosswalk_receipt": "fixture_crosswalk",
        "evidence": "fixture evidence",
        "internal_targets": (),
    }
    values.update(changes)
    return CompositeSpec(**values)  # type: ignore[arg-type]


def fixture_stat_contracts() -> dict[str, dict[str, object]]:
    return {
        "fixture_target": {
            "unit": "count",
            "natural_grain": "player_game",
            "aggregation_class": "SUM",
        }
    }


def fixture_dossier_rows() -> list[dict[str, object]]:
    return [
        {
            "source": "fixture_source",
            "table_key": "fixture_table",
            "column": "fixture_column",
        }
    ]


def test_authorized_contract_is_closed_and_has_126_rows():
    """Catches automatic cohort growth, removed members, and altered ownership."""
    specs = load_specs()
    row_keys = [key for spec in specs for key in spec.row_keys]
    by_cohort = {}
    for spec in specs:
        by_cohort[spec.cohort] = by_cohort.get(spec.cohort, 0) + len(spec.row_keys)

    assert len(row_keys) == 126
    assert len(set(row_keys)) == 126
    assert set(row_keys) == authorized_row_keys()
    assert by_cohort == {
        "pfr_awards": 26,
        "nflcom_l7": 78,
        "pfr_field_goals": 20,
        "pfr_two_point_total": 2,
    }


def test_contract_rejects_constraint_target_claimed_as_scalar():
    """Catches constraint inflation into external scalar witness depth."""
    spec = fixture_spec(kind="CONSTRAINT_OBSERVATION", scalar_credit=True)
    problems = validate_specs(
        [spec], dossier_rows=fixture_dossier_rows(), stat_contracts=fixture_stat_contracts()
    )
    assert "constraint cannot claim scalar witness credit" in problems


def test_contract_rejects_duplicate_unknown_and_incompatible_members():
    """Catches duplicate ownership, unknown targets, and contract mismatches."""
    first = fixture_spec()
    second = fixture_spec(
        spec_id="duplicate",
        row_keys=(first.row_keys[0], row_key("missing", "table", "column")),
        targets=("missing_target",),
        unit="yards",
        natural_grain="player_season",
        aggregation_class="MAX",
    )
    problems = validate_specs(
        [first, second], dossier_rows=fixture_dossier_rows(), stat_contracts=fixture_stat_contracts()
    )

    assert any("duplicate row key" in problem for problem in problems)
    assert any("unknown dossier row key" in problem for problem in problems)
    assert any("unknown canonical target" in problem for problem in problems)

    same_spec_repeat = fixture_spec(row_keys=(first.row_keys[0], first.row_keys[0]))
    assert any(
        "duplicate row key" in problem
        for problem in validate_specs(
            [same_spec_repeat], dossier_rows=fixture_dossier_rows(), stat_contracts=fixture_stat_contracts()
        )
    )


def test_only_named_pfr_awards_contract_may_span_heterogeneous_target_contracts():
    """Catches a generic neutral bypass for unit or aggregation mismatches."""
    award_rows = tuple(row_key(source, "*", "awards") for source in PFR_AWARD_SOURCES)
    award_dossier = [
        {"source": source, "table_key": "*", "column": "awards"}
        for source in PFR_AWARD_SOURCES
    ]
    award_contracts = {
        "hof": {"unit": None, "natural_grain": "player_static", "aggregation_class": "ANY"},
        "allpro": {"unit": "count", "natural_grain": "player_static", "aggregation_class": "FIRST"},
        "probowls": {"unit": "count", "natural_grain": "player_static", "aggregation_class": "FIRST"},
    }
    exact = fixture_spec(
        spec_id="pfr-awards",
        cohort="pfr_awards",
        row_keys=award_rows,
        sources=("pfr_all_pro_members", "pfr_pro_bowl_members"),
        derivation="pfr_typed_awards_local",
        targets=("hof", "allpro", "probowls"),
        unit=None,
        natural_grain="player_static",
        aggregation_class="MIXED_TARGET_CONTRACT",
        internal_targets=("hof",),
    )

    assert validate_specs([exact], award_dossier, award_contracts) == []

    wrong_sources = fixture_spec(
        spec_id="near-miss-sources",
        cohort=exact.cohort,
        row_keys=exact.row_keys,
        sources=("pfr_hof_members", "pfr_pro_bowl_members"),
        derivation=exact.derivation,
        targets=exact.targets,
        unit=exact.unit,
        natural_grain=exact.natural_grain,
        aggregation_class=exact.aggregation_class,
        internal_targets=exact.internal_targets,
    )
    assert any(
        "internal targets are only allowed" in problem
        for problem in validate_specs([wrong_sources], award_dossier, award_contracts)
    )

    wrong_derivation = fixture_spec(
        spec_id="near-miss-derivation",
        cohort=exact.cohort,
        row_keys=exact.row_keys,
        sources=exact.sources,
        derivation="generic_neutral_awards",
        targets=exact.targets,
        unit=exact.unit,
        natural_grain=exact.natural_grain,
        aggregation_class=exact.aggregation_class,
    )
    wrong_targets = fixture_spec(
        spec_id="near-miss-targets",
        cohort=exact.cohort,
        row_keys=exact.row_keys,
        sources=exact.sources,
        derivation=exact.derivation,
        targets=("allpro", "hof", "probowls"),
        unit=exact.unit,
        natural_grain=exact.natural_grain,
        aggregation_class=exact.aggregation_class,
    )
    wrong_kind = fixture_spec(
        spec_id="near-miss-kind",
        cohort=exact.cohort,
        row_keys=exact.row_keys,
        sources=exact.sources,
        derivation=exact.derivation,
        kind="CONSTRAINT_OBSERVATION",
        targets=exact.targets,
        unit=exact.unit,
        natural_grain=exact.natural_grain,
        aggregation_class=exact.aggregation_class,
    )

    assert any(
        "heterogeneous target contract is only allowed" in problem
        for problem in validate_specs([wrong_derivation], award_dossier, award_contracts)
    )
    assert any(
        "heterogeneous target contract is only allowed" in problem
        for problem in validate_specs([wrong_targets], award_dossier, award_contracts)
    )
    assert any(
        "heterogeneous target contract is only allowed" in problem
        for problem in validate_specs([wrong_kind], award_dossier, award_contracts)
    )

    wrong_unit_contracts = {target: dict(contract) for target, contract in award_contracts.items()}
    wrong_unit_contracts["allpro"]["unit"] = "yards"
    wrong_aggregation_contracts = {target: dict(contract) for target, contract in award_contracts.items()}
    wrong_aggregation_contracts["probowls"]["aggregation_class"] = "MAX"
    assert any(
        "pfr typed awards target contract mismatch" in problem
        for problem in validate_specs([exact], award_dossier, wrong_unit_contracts)
    )
    assert any(
        "pfr typed awards target contract mismatch" in problem
        for problem in validate_specs([exact], award_dossier, wrong_aggregation_contracts)
    )


def test_scalar_targets_excludes_constraint_observations():
    """Catches a constraint observation incorrectly adding witness-depth targets."""
    result = LaneResult(
        observations=(
            LaneObservation("scalar", "pfr", "SCALAR_OBSERVATION", ("made",), 1, 0, "PASS"),
            LaneObservation("constraint", "pfr", "CONSTRAINT_OBSERVATION", ("missed",), 1, 0, "PASS"),
        ),
        compared_rows=2,
        rejected_rows=0,
        errors=(),
    )

    assert result.scalar_targets == frozenset({"made"})


def test_receipt_freshness_binds_contract_artifacts_executor_and_crosswalks(tmp_path: Path):
    """Catches stale receipts accepted after any binding input changes."""
    artifact = tmp_path / "source.json"
    artifact.write_text('{"source": "fixture"}', encoding="utf-8")
    receipt_path = tmp_path / "receipt.json"
    manifest = artifact_manifest_sha256((artifact,))
    write_receipt(
        receipt_path,
        contract_hash="a" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v1",
        crosswalk_receipts={"fixture_crosswalk": "PASS"},
        observations=(),
        compared_rows=0,
        rejected_rows=0,
    )

    assert receipt_is_current(
        receipt_path,
        contract_hash="a" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v1",
        required_crosswalk_receipts={"fixture_crosswalk": "PASS"},
    )
    assert not receipt_is_current(
        receipt_path,
        contract_hash="b" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v1",
        required_crosswalk_receipts={"fixture_crosswalk": "PASS"},
    )
    assert not receipt_is_current(
        receipt_path,
        contract_hash="a" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v2",
        required_crosswalk_receipts={"fixture_crosswalk": "PASS"},
    )
    assert not receipt_is_current(
        receipt_path,
        contract_hash="a" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v1",
        required_crosswalk_receipts={"fixture_crosswalk": "FAIL"},
    )

    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    receipt["crosswalk_statuses"] = {"fixture_crosswalk": "PASS", "unexpected": "PASS"}
    receipt["crosswalk_receipts"] = ["fixture_crosswalk", "unexpected"]
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert not receipt_is_current(
        receipt_path,
        contract_hash="a" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v1",
        required_crosswalk_receipts={"fixture_crosswalk": "PASS"},
    )

    receipt["crosswalk_statuses"] = {"fixture_crosswalk": "PASS"}
    receipt["crosswalk_receipts"] = []
    receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
    assert not receipt_is_current(
        receipt_path,
        contract_hash="a" * 64,
        source_manifest_hash=manifest,
        executor_version="fixture-v1",
        required_crosswalk_receipts={"fixture_crosswalk": "PASS"},
    )


def test_per_spec_freshness_requires_pass_and_optionally_license(tmp_path: Path):
    """Catches resolution-only consumers accepting pending or unlicensed spec receipts."""
    specs = load_specs()
    statuses = {}
    for spec in specs:
        external_targets = tuple(target for target in spec.targets if target not in spec.internal_targets)
        result = {
            "observations": [{
                "spec_id": spec.spec_id,
                "source": spec.sources[0],
                "kind": spec.kind,
                "targets": list(external_targets),
                "compared_rows": 1,
                "rejected_rows": 0,
                "status": "PASS",
                "value": 1,
                "evidence": [],
            }],
            "internal_resolutions": [{
                "spec_id": spec.spec_id,
                "source": "player_bio",
                "targets": list(spec.internal_targets),
                "compared_rows": 1,
                "rejected_rows": 0,
                "status": "PASS",
                "value": 1,
                "evidence": [],
            }] if spec.internal_targets else [],
            "compared_rows": 1,
            "rejected_rows": 0,
            "errors": [],
        }
        statuses[spec.spec_id] = {
            "spec_id": spec.spec_id,
            "spec_contract_sha256": "a" * 64,
            "source_manifest_sha256": "b" * 64,
            "resolution_status": "PASS",
            "license_status": "FAIL" if spec.spec_id == "pfr-awards-v1" else "PASS",
            "crosswalk_statuses": {spec.crosswalk_receipt: "PASS"},
            "result": result,
        }
    receipt = tmp_path / "receipt.json"
    receipt.write_text(json.dumps({"version": 1, "executor_version": "v1", "spec_statuses": statuses}), encoding="utf-8")
    kwargs = {
        "spec_id": "pfr-awards-v1",
        "contract_hash": "a" * 64,
        "source_manifest_hash": "b" * 64,
        "executor_version": "v1",
        "required_crosswalk_receipts": {"bio_pfr_nflid": "PASS"},
    }
    assert spec_receipt_is_current(receipt, **kwargs, require_license=False)
    assert not spec_receipt_is_current(receipt, **kwargs, require_license=True)
