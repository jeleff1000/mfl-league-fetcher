"""End-to-end behavior tests for local composite witness orchestration."""

from __future__ import annotations

import builtins
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys

import duckdb
import pytest

from scripts.sota_recon import composite_witness_lane as lane
from scripts.sota_recon.composite_witness_lane import (
    EXECUTOR_VERSION,
    EXECUTORS,
    LaneObservation,
    LanePaths,
    LaneResult,
    InternalResolution,
    execute_lane,
    load_specs,
    main,
    spec_receipt_is_current,
    write_lane_receipt,
)


SPEC_IDS = {
    "pfr-awards-v1",
    "nflcom-l7-fg-buckets-v1",
    "pfr-field-goals-buckets-1-4-v1",
    "pfr-field-goals-50-plus-v1",
    "pfr-two-point-total-v1",
}
PFR_SPEC_IDS = SPEC_IDS - {"nflcom-l7-fg-buckets-v1"}
FG_TARGETS = (
    "fg_made_0_19", "fg_missed_0_19",
    "fg_made_20_29", "fg_missed_20_29",
    "fg_made_30_39", "fg_missed_30_39",
    "fg_made_40_49", "fg_missed_40_49",
    "fg_made_50_59", "fg_made_60_", "fg_missed_50_59", "fg_missed_60_",
)


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE fixture (" + ", ".join(f'\"{column}\" VARCHAR' for column in columns) + ")")
        con.executemany("INSERT INTO fixture VALUES (" + ", ".join("?" for _ in columns) + ")", rows)
        con.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()


def _dossier_row(key: str) -> dict[str, str]:
    parts = key.split("|")
    return {"source": parts[0], "table_key": "|".join(parts[1:-1]), "column": parts[-1]}


@pytest.fixture
def paths(tmp_path: Path) -> LanePaths:
    specs = load_specs()
    dossier = tmp_path / "column-dossier.json"
    dossier.write_text(
        json.dumps({"rows": [_dossier_row(key) for spec in specs for key in spec.row_keys]}),
        encoding="utf-8",
    )

    bio = tmp_path / "player_bio.parquet"
    _write_parquet(
        bio,
        ("pfr_id", "NFL_player_id", "hof", "allpro", "probowls"),
        [("P1", "N1", "true", "2", "3")],
    )

    canonical_columns = (
        "NFL_player_id", "year", "week", "season_type", *FG_TARGETS,
        "passing_2pt_conversions", "receiving_2pt_conversions", "rushing_2pt_conversions",
    )
    canonical_values = (
        "N1", "2022", "1", "REG",
        "1", "1", "1", "1", "1", "1", "1", "1", "1", "0", "1", "0",
        "1", "1", "1",
    )
    v26 = tmp_path / "v26.parquet"
    _write_parquet(v26, canonical_columns, [canonical_values, (*canonical_values[:3], "POST", *canonical_values[4:])])

    source_paths: dict[str, Path] = {}
    allpro = tmp_path / "pfr_all_pro_members.parquet"
    probowl = tmp_path / "pfr_pro_bowl_members.parquet"
    _write_parquet(allpro, ("pfr_id", "year", "all_pro_string"), [("P1", "2021", "AP: 1st Tm"), ("P1", "2022", "AP: 1st Tm")])
    _write_parquet(probowl, ("pfr_id", "year"), [("P1", "2020"), ("P1", "2021"), ("P1", "2022")])
    source_paths.update(pfr_all_pro_members=allpro, pfr_pro_bowl_members=probowl)

    kicking_columns = ("pfr_id", "year_id", *(f"fg{kind}{bucket}" for bucket in range(1, 6) for kind in ("m", "a")))
    kicking_values = ("P1", "2022", *(value for _ in range(5) for value in ("1", "2")))
    for source in ("pfr_player_kicking", "pfr_kicking_post"):
        source_path = tmp_path / f"{source}.parquet"
        _write_parquet(source_path, kicking_columns, [kicking_values])
        source_paths[source] = source_path

    for source in ("pfr_player_scoring", "pfr_scoring_post"):
        source_path = tmp_path / f"{source}.parquet"
        _write_parquet(source_path, ("pfr_id", "year_id", "two_pt_md"), [("P1", "2022", "3")])
        source_paths[source] = source_path

    crosswalk = tmp_path / "nflcom_slug_pfrid.parquet"
    _write_parquet(crosswalk, ("nflcom_slug", "pfr_id"), [("good-kicker", "P1")])
    source_paths["nflcom_slug_pfrid"] = crosswalk
    for source, table, layout in (
        ("nflcom_player_situational", "Field Position", "player_situational_L7"),
        ("nflcom_player_splits", "Days", "player_splits_L7"),
    ):
        source_path = tmp_path / f"{source}.parquet"
        _write_parquet(
            source_path,
            ("nflcom_slug", "season", "_table", "_layout", "split_value", "player", "1_19", "20_29", "30_39", "40_49", "50_59", "60"),
            [("good-kicker", "2022", table, layout, "fixture split", "Fixture Kicker", "1-2", "1-2", "1-2", "1-2", "1-2", "0-0")],
        )
        source_paths[source] = source_path

    return LanePaths(dossier, v26, bio, tmp_path / "receipt.json", source_paths)


def _by_id(paths: LanePaths):
    return {status.spec_id: status for status in execute_lane(paths)}


def _write_current_receipt(paths: LanePaths):
    statuses = execute_lane(paths)
    write_lane_receipt(
        paths.receipt,
        contract_hash=lane.contract_sha256(),
        executor_version=EXECUTOR_VERSION,
        statuses=statuses,
    )
    return {status.spec_id: status for status in statuses}


def _is_current(paths: LanePaths, status, *, require_license: bool = True) -> bool:
    return spec_receipt_is_current(
        paths.receipt,
        spec_id=status.spec_id,
        contract_hash=status.spec_contract_sha256,
        source_manifest_hash=status.source_manifest_sha256,
        executor_version=EXECUTOR_VERSION,
        required_crosswalk_receipts=dict(status.crosswalk_statuses),
        require_license=require_license,
    )


def test_local_execution_is_partial_only_because_l7_is_pending(paths: LanePaths):
    """Catches a global PASS or PFR failure masking the one intentional L7 hold."""
    statuses = _by_id(paths)
    assert statuses["nflcom-l7-fg-buckets-v1"].resolution_status == "PENDING"
    assert statuses["nflcom-l7-fg-buckets-v1"].license_status == "PENDING"
    assert all(statuses[spec_id].resolution_status == "PASS" for spec_id in PFR_SPEC_IDS)
    assert all(statuses[spec_id].license_status == "PASS" for spec_id in PFR_SPEC_IDS)


def test_receipt_separates_external_scalars_constraints_internal_and_pending(paths: LanePaths):
    """Catches HOF, constraints, or pending NFL.com facts leaking into external scalars."""
    _write_current_receipt(paths)
    receipt = json.loads(paths.receipt.read_text(encoding="utf-8"))
    targets = {
        target
        for observation in receipt["external_scalar_observations"]
        for target in observation["targets"]
    }
    assert targets == {"allpro", "probowls", *FG_TARGETS[:8]}
    assert "hof" not in targets
    assert receipt["internal_resolutions"][0]["targets"] == ["hof"]
    assert {item["spec_id"] for item in receipt["constraint_observations"]} == {
        "pfr-field-goals-50-plus-v1", "pfr-two-point-total-v1"
    }
    assert all(item["kind"] != "CONSTRAINT_OBSERVATION" for item in receipt["external_scalar_observations"])
    assert not any(item["spec_id"].startswith("nflcom") for item in receipt["external_scalar_observations"])
    assert receipt["spec_statuses"]["nflcom-l7-fg-buckets-v1"]["result"]["observations"]
    assert receipt["resolution_status"] == receipt["license_status"] == "PARTIAL"


def test_changed_one_spec_artifact_stales_only_that_spec(paths: LanePaths):
    """Catches lane-wide artifact hashing that invalidates unrelated specifications."""
    old = _write_current_receipt(paths)
    _write_parquet(
        paths.source_paths["pfr_all_pro_members"],
        ("pfr_id", "year", "all_pro_string"),
        [("P1", "2022", "AP: 1st Tm")],
    )
    new = _by_id(paths)
    assert not _is_current(paths, new["pfr-awards-v1"])
    assert all(_is_current(paths, new[spec_id], require_license=False) for spec_id in SPEC_IDS - {"pfr-awards-v1", "nflcom-l7-fg-buckets-v1"})
    assert old["pfr-field-goals-buckets-1-4-v1"].source_manifest_sha256 == new["pfr-field-goals-buckets-1-4-v1"].source_manifest_sha256


def test_nfl_crosswalk_receipt_data_stales_only_nfl_dependency_digest(
    paths: LanePaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches hashing the complete crosswalk contract into every spec manifest."""
    crosswalk_path = tmp_path / "crosswalk-receipts.json"
    payload = json.loads(lane.CROSSWALK_CONTRACT_PATH.read_text(encoding="utf-8"))
    crosswalk_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(lane, "CROSSWALK_CONTRACT_PATH", crosswalk_path)
    old = _write_current_receipt(paths)

    nfl = next(item for item in payload["receipts"] if item["receipt_id"] == "nflcom_slug_pfrid")
    nfl["measured"]["review_probe"] = "nfl-only-change"
    crosswalk_path.write_text(json.dumps(payload), encoding="utf-8")
    new = _by_id(paths)

    assert old["nflcom-l7-fg-buckets-v1"].source_manifest_sha256 != new["nflcom-l7-fg-buckets-v1"].source_manifest_sha256
    for spec_id in PFR_SPEC_IDS:
        assert old[spec_id].source_manifest_sha256 == new[spec_id].source_manifest_sha256
        assert _is_current(paths, new[spec_id])


def test_bio_crosswalk_receipt_data_stales_only_pfr_dependency_digests(
    paths: LanePaths, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches hashing a PFR-only identity receipt into the NFL dependency digest."""
    crosswalk_path = tmp_path / "crosswalk-receipts.json"
    payload = json.loads(lane.CROSSWALK_CONTRACT_PATH.read_text(encoding="utf-8"))
    crosswalk_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(lane, "CROSSWALK_CONTRACT_PATH", crosswalk_path)
    old = _write_current_receipt(paths)

    bio = next(item for item in payload["receipts"] if item["receipt_id"] == "bio_pfr_nflid")
    bio["measured"]["review_probe"] = "pfr-only-change"
    crosswalk_path.write_text(json.dumps(payload), encoding="utf-8")
    new = _by_id(paths)

    assert old["nflcom-l7-fg-buckets-v1"].source_manifest_sha256 == new["nflcom-l7-fg-buckets-v1"].source_manifest_sha256
    for spec_id in PFR_SPEC_IDS:
        assert old[spec_id].source_manifest_sha256 != new[spec_id].source_manifest_sha256
        assert not _is_current(paths, new[spec_id])


def test_stale_contract_and_failed_crosswalk_invalidate_only_edited_spec(paths: LanePaths):
    """Catches freshness reading a lane-wide hash or crosswalk map."""
    statuses = _write_current_receipt(paths)
    payload = json.loads(paths.receipt.read_text(encoding="utf-8"))
    payload["contract_sha256"] = "whole-contract-provenance-does-not-drive-spec-freshness"
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")
    assert _is_current(paths, statuses["pfr-awards-v1"])

    payload["spec_statuses"]["pfr-awards-v1"]["spec_contract_sha256"] = "0" * 64
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")
    assert not _is_current(paths, statuses["pfr-awards-v1"])
    assert _is_current(paths, statuses["pfr-two-point-total-v1"])

    payload["spec_statuses"]["pfr-awards-v1"]["spec_contract_sha256"] = statuses["pfr-awards-v1"].spec_contract_sha256
    payload["spec_statuses"]["pfr-awards-v1"]["crosswalk_statuses"] = {"bio_pfr_nflid": "FAIL"}
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")
    assert not _is_current(paths, statuses["pfr-awards-v1"])
    assert _is_current(paths, statuses["pfr-two-point-total-v1"])


def test_missing_executor_fails_only_its_spec(paths: LanePaths, monkeypatch: pytest.MonkeyPatch):
    """Catches missing dispatch being ignored or poisoning unrelated executors."""
    monkeypatch.delitem(EXECUTORS, "pfr_two_point_total")
    statuses = _by_id(paths)
    assert statuses["pfr-two-point-total-v1"].resolution_status == "FAIL"
    assert all(statuses[spec_id].resolution_status == "PASS" for spec_id in PFR_SPEC_IDS - {"pfr-two-point-total-v1"})
    assert statuses["nflcom-l7-fg-buckets-v1"].resolution_status == "PENDING"


@pytest.mark.parametrize("mutation", ["rejected", "mismatched_spec"])
def test_bad_executor_result_fails_only_its_spec(paths: LanePaths, monkeypatch: pytest.MonkeyPatch, mutation: str):
    """Catches rejected rows or foreign spec IDs being granted a PASS receipt."""
    original = EXECUTORS["pfr_two_point_total"]

    def bad_executor(spec, lane_paths):
        result = original(spec, lane_paths)
        if mutation == "rejected":
            return replace(result, rejected_rows=1)
        observation = replace(result.observations[0], spec_id="foreign-spec")
        return replace(result, observations=(observation, *result.observations[1:]))

    monkeypatch.setitem(EXECUTORS, "pfr_two_point_total", bad_executor)
    statuses = _by_id(paths)
    assert statuses["pfr-two-point-total-v1"].resolution_status == "FAIL"
    assert all(statuses[spec_id].resolution_status == "PASS" for spec_id in PFR_SPEC_IDS - {"pfr-two-point-total-v1"})


@pytest.mark.parametrize(
    "forgery", ["scalarized_constraint", "internal_target", "item_rejection", "missing_required_target"]
)
def test_execution_semantically_rejects_forged_executor_passes(
    paths: LanePaths, monkeypatch: pytest.MonkeyPatch, forgery: str
) -> None:
    """Catches a shape-valid executor result minting forbidden PASS semantics."""
    original = EXECUTORS["pfr_two_point_total"]

    def forged_executor(spec, lane_paths):
        result = original(spec, lane_paths)
        item = result.observations[0]
        if forgery == "scalarized_constraint":
            item = replace(item, kind="SCALAR_OBSERVATION")
        elif forgery == "internal_target":
            item = replace(item, targets=("hof",))
        elif forgery == "item_rejection":
            item = replace(item, rejected_rows=1)
        else:
            return replace(result, observations=tuple(
                replace(observation, targets=observation.targets[:-1])
                for observation in result.observations
            ))
        return replace(result, observations=(item, *result.observations[1:]))

    monkeypatch.setitem(EXECUTORS, "pfr_two_point_total", forged_executor)
    status = _by_id(paths)["pfr-two-point-total-v1"]

    assert status.resolution_status == status.license_status == "FAIL"
    assert any("semantic validation" in error for error in status.result.errors)


def test_execution_rejects_internal_resolution_outside_declared_partition(
    paths: LanePaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches an executor laundering an external awards target as internal resolution."""
    original = EXECUTORS["pfr_awards"]

    def forged_executor(spec, lane_paths):
        result = original(spec, lane_paths)
        forged = InternalResolution(spec.spec_id, "player_bio", ("allpro",), 1, 0, "PASS", 2)
        return replace(result, internal_resolutions=(*result.internal_resolutions, forged))

    monkeypatch.setitem(EXECUTORS, "pfr_awards", forged_executor)
    status = _by_id(paths)["pfr-awards-v1"]

    assert status.resolution_status == status.license_status == "FAIL"
    assert any("semantic validation" in error for error in status.result.errors)


def test_freshness_rejects_extra_missing_and_malformed_spec_maps(paths: LanePaths):
    """Catches a receipt that is not closed over exactly the five enrolled specs."""
    statuses = _write_current_receipt(paths)
    target = statuses["pfr-awards-v1"]
    payload = json.loads(paths.receipt.read_text(encoding="utf-8"))
    payload["spec_statuses"]["extra-spec"] = dict(payload["spec_statuses"]["pfr-awards-v1"])
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")
    assert not _is_current(paths, target)
    del payload["spec_statuses"]["extra-spec"]
    del payload["spec_statuses"]["pfr-two-point-total-v1"]
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")
    assert not _is_current(paths, target)
    payload["spec_statuses"] = []
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")
    assert not _is_current(paths, target)


def test_writer_rejects_a_forged_pass_over_rejected_rows(paths: LanePaths):
    """Catches callers bypassing execute_lane with a malformed PASS SpecStatus."""
    statuses = list(execute_lane(paths))
    index = next(i for i, status in enumerate(statuses) if status.spec_id == "pfr-two-point-total-v1")
    statuses[index] = replace(statuses[index], result=replace(statuses[index].result, rejected_rows=1))
    with pytest.raises(ValueError, match="status/result mismatch"):
        write_lane_receipt(
            paths.receipt,
            contract_hash=lane.contract_sha256(),
            executor_version=EXECUTOR_VERSION,
            statuses=statuses,
        )


@pytest.mark.parametrize(
    "forgery", ["scalarized_constraint", "internal_target", "item_rejection", "missing_required_target"]
)
def test_writer_rejects_semantically_forged_pass_items(paths: LanePaths, forgery: str) -> None:
    """Catches direct writer callers bypassing the executor semantic boundary."""
    statuses = list(execute_lane(paths))
    index = next(i for i, status in enumerate(statuses) if status.spec_id == "pfr-two-point-total-v1")
    status = statuses[index]
    item = status.result.observations[0]
    if forgery == "scalarized_constraint":
        item = replace(item, kind="SCALAR_OBSERVATION")
    elif forgery == "internal_target":
        item = replace(item, targets=("hof",))
    elif forgery == "item_rejection":
        item = replace(item, rejected_rows=1)
    else:
        statuses[index] = replace(status, result=replace(
            status.result,
            observations=tuple(
                replace(observation, targets=observation.targets[:-1])
                for observation in status.result.observations
            ),
        ))
    if forgery != "missing_required_target":
        statuses[index] = replace(status, result=replace(
            status.result, observations=(item, *status.result.observations[1:])
        ))

    with pytest.raises(ValueError, match="status/result mismatch"):
        write_lane_receipt(
            paths.receipt,
            contract_hash=lane.contract_sha256(),
            executor_version=EXECUTOR_VERSION,
            statuses=statuses,
        )


@pytest.mark.parametrize(
    "forgery", ["scalarized_constraint", "internal_target", "item_rejection", "missing_required_target"]
)
def test_freshness_rehydrates_and_rejects_semantically_forged_pass_items(
    paths: LanePaths, forgery: str
) -> None:
    """Catches freshness trusting stored PASS labels without result semantics."""
    statuses = _write_current_receipt(paths)
    target = statuses["pfr-two-point-total-v1"]
    payload = json.loads(paths.receipt.read_text(encoding="utf-8"))
    item = payload["spec_statuses"][target.spec_id]["result"]["observations"][0]
    if forgery == "scalarized_constraint":
        item["kind"] = "SCALAR_OBSERVATION"
    elif forgery == "internal_target":
        item["targets"] = ["hof"]
    elif forgery == "item_rejection":
        item["rejected_rows"] = 1
    else:
        for observation in payload["spec_statuses"][target.spec_id]["result"]["observations"]:
            observation["targets"] = observation["targets"][:-1]
    paths.receipt.write_text(json.dumps(payload), encoding="utf-8")

    assert not _is_current(paths, target)


def test_cross_spec_duplicate_ownership_blocks_dispatch(
    paths: LanePaths, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Catches per-spec validation missing duplicate ownership across two specs."""
    specs = list(load_specs())
    duplicate = specs[0].row_keys[0]
    specs[1] = replace(specs[1], row_keys=(duplicate, *specs[1].row_keys[1:]))
    calls = 0

    def forbidden_executor(spec, lane_paths):
        nonlocal calls
        calls += 1
        return LaneResult((), 0, 0, ())

    monkeypatch.setattr(lane, "load_specs", lambda path=lane.CONTRACT_PATH: tuple(specs))
    for key in tuple(EXECUTORS):
        monkeypatch.setitem(EXECUTORS, key, forbidden_executor)

    statuses = execute_lane(paths)

    assert calls == 0
    assert all(status.resolution_status == status.license_status == "FAIL" for status in statuses)
    assert all(any("duplicate row key" in error for error in status.result.errors) for status in statuses)


def test_main_execute_local_is_deterministic_and_does_not_import_network_clients(paths: LanePaths, monkeypatch: pytest.MonkeyPatch):
    """Catches CLI orchestration importing a network, browser, or request client."""
    real_import = builtins.__import__
    forbidden = {"requests", "httpx", "urllib3", "selenium", "playwright"}

    def guarded_import(name, *args, **kwargs):
        if name.split(".", 1)[0] in forbidden:
            raise AssertionError(f"network client imported: {name}")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded_import)
    assert main(["--execute-local"], paths=paths) == 0
    first = paths.receipt.read_bytes()
    assert main(["--execute-local"], paths=paths) == 0
    assert paths.receipt.read_bytes() == first


def test_production_help_exposes_only_local_execution():
    """Catches a URL, browser, request, fetch, scrape, capture, or recapture CLI option."""
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.sota_recon.composite_witness_lane", "--help"],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "--execute-local" in completed.stdout
    for option in ("--url", "--browser", "--request", "--fetch", "--scrape", "--capture", "--recapture", "--compact"):
        assert option not in completed.stdout
