"""Local-contract behavior tests for typed PFR awards witnesses."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest

from scripts.scrape_pfr_context_tables import page_specs
from scripts.sota_recon.composite_witness_lane import LanePaths, load_specs
from scripts.sota_recon.composite_witness_pfr_awards import execute_pfr_awards
from scripts.sota_recon.sources import registry


def _write_parquet(path: Path, columns: tuple[str, ...], rows: list[tuple[object, ...]]) -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE fixture AS SELECT * FROM (SELECT 1) WHERE false")
        con.execute("DROP TABLE fixture")
        con.execute("CREATE TABLE fixture (" + ", ".join(f'\"{name}\" VARCHAR' for name in columns) + ")")
        con.executemany("INSERT INTO fixture VALUES (" + ", ".join("?" for _ in columns) + ")", rows)
        con.execute("COPY fixture TO ? (FORMAT PARQUET)", [str(path)])
    finally:
        con.close()


def _paths(tmp_path: Path, *, bio: list[tuple[object, ...]] | None = None) -> LanePaths:
    allpro, probowl, bio_path, canonical_path = (
        tmp_path / "allpro.parquet", tmp_path / "probowl.parquet",
        tmp_path / "bio.parquet", tmp_path / "canonical.parquet",
    )
    _write_parquet(allpro, ("pfr_id", "year", "all_pro_string"), [("P1", "2022", "AP: 1st Tm"), ("P1", "2022", "AP: 1st Tm"), ("P1", "2023", "AP: 1st Tm")])
    _write_parquet(probowl, ("pfr_id", "year"), [("P1", "2020"), ("P1", "2020"), ("P1", "2021"), ("P1", "2022")])
    _write_parquet(bio_path, ("pfr_id", "NFL_player_id", "hof", "allpro", "probowls"), bio or [("P1", "N1", "true", "2", "3")])
    _write_parquet(canonical_path, ("NFL_player_id",), [("N1",)])
    return LanePaths(
        tmp_path / "dossier.json", canonical_path, bio_path, tmp_path / "receipt.json",
        {"pfr_all_pro_members": allpro, "pfr_pro_bowl_members": probowl},
    )


def _spec():
    return next(spec for spec in load_specs() if spec.spec_id == "pfr-awards-v1")


@pytest.fixture
def spec():
    return _spec()


def test_awards_use_two_local_pfr_sources_and_internal_hof(spec) -> None:
    """Catches an awards contract that treats Hall membership as external evidence."""
    assert spec.sources == ("pfr_all_pro_members", "pfr_pro_bowl_members")
    assert spec.internal_targets == ("hof",)
    assert set(spec.targets) == {"hof", "allpro", "probowls"}


def test_hof_is_internal_resolution_without_external_scalar_credit(tmp_path: Path) -> None:
    """Catches the executor issuing Hall of Fame external scalar witness credit."""
    result = execute_pfr_awards(_spec(), _paths(tmp_path))

    assert result.scalar_targets == {"allpro", "probowls"}
    assert [(r.source, r.targets, r.value, r.status) for r in result.internal_resolutions] == [
        ("player_bio", ("hof",), 1, "PASS")
    ]


def test_typed_memberships_pass_when_canonical_award_cells_are_unpopulated(tmp_path: Path) -> None:
    """Catches a valid external membership being rejected merely because its target is a gap."""
    result = execute_pfr_awards(
        _spec(), _paths(tmp_path, bio=[("P1", "N1", None, None, None)])
    )

    assert result.errors == ()
    assert {(item.targets[0], item.value, item.status) for item in result.observations} == {
        ("allpro", 2, "PASS"), ("probowls", 3, "PASS")
    }
    assert result.internal_resolutions == ()


def test_no_hof_capture_or_external_source_is_registered() -> None:
    """Catches reintroducing a static Hall page or an external Hall relation."""
    source_registry = registry(include_subject=False)
    specs = page_specs(SimpleNamespace(source=["allpro", "probowl"], year_min=2025, year_max=2025))

    assert "pfr_hof_members" not in source_registry
    assert all(spec.source_kind != "hof" for spec in specs)


def test_award_membership_uses_dedicated_typed_pages_and_distinct_seasons(tmp_path: Path) -> None:
    """Catches parsing a season-row awards string or double-counting repeated memberships."""
    result = execute_pfr_awards(_spec(), _paths(tmp_path))

    assert {item.targets[0]: item.value for item in result.observations} == {"allpro": 2, "probowls": 3}
    assert {item.status for item in result.observations} == {"PASS"}


def test_player_bio_cannot_be_substituted_as_an_external_awards_relation(tmp_path: Path) -> None:
    """Catches treating the internal crosswalk relation as a PFR source fact."""
    paths = _paths(tmp_path)
    paths = LanePaths(paths.dossier, paths.v26, paths.player_bio, paths.receipt, {"pfr_all_pro_members": paths.player_bio})

    result = execute_pfr_awards(_spec(), paths)

    assert any("missing registered source path" in error or "missing required columns" in error for error in result.errors)
    assert not any(item.status == "PASS" for item in result.observations)


def test_pending_canonical_schema_never_exposes_hof_as_external_scalar(tmp_path: Path) -> None:
    """Catches a pending internal schema check leaking Hall status into scalar depth."""
    paths = _paths(tmp_path)
    _write_parquet(paths.player_bio, ("pfr_id", "NFL_player_id", "hof", "allpro"), [
        ("P1", "N1", "true", "2"),
    ])

    result = execute_pfr_awards(_spec(), paths)

    assert {item.status for item in result.observations} == {"PENDING"}
    assert all("hof" not in item.targets for item in result.observations)
    assert "hof" not in result.scalar_targets


def test_non_bijective_bio_crosswalk_blocks_all_award_passes(tmp_path: Path) -> None:
    """Catches mapping a PFR id to several canonical players."""
    result = execute_pfr_awards(_spec(), _paths(tmp_path, bio=[("P1", "N1", "true", "2", "3"), ("P1", "N2", "true", "2", "3")]))

    assert any("one-to-one" in error for error in result.errors)
    assert not any(item.status == "PASS" for item in result.observations)


def test_altered_dedicated_page_membership_cannot_pass_from_player_bio(tmp_path: Path) -> None:
    """Catches using player_bio awards as evidence when an external page row changes."""
    paths = _paths(tmp_path)
    _write_parquet(paths.source_paths["pfr_all_pro_members"], ("pfr_id", "year", "all_pro_string"), [
        ("P1", "2022", "AP: 1st Tm"), ("P1", "2023", "AP: 2nd Tm"),
    ])

    result = execute_pfr_awards(_spec(), paths)

    assert {item.status for item in result.observations} == {"MISMATCH"}
    assert not any(item.status == "PASS" for item in result.observations)


def test_canonical_zero_members_are_explicitly_compared_as_closed_world_absences(tmp_path: Path) -> None:
    """Catches silently omitting a canonical zero-award player from comparison."""
    result = execute_pfr_awards(_spec(), _paths(tmp_path, bio=[
        ("P1", "N1", "true", "2", "3"),
        ("P0", "N0", "false", "0", "0"),
    ]))

    zero = [item for item in result.observations if dict(item.evidence)["NFL_player_id"] == "N0"]
    assert {item.targets[0]: item.value for item in zero} == {"allpro": 0, "probowls": 0}
    assert {item.status for item in zero} == {"PASS"}
    assert [(r.targets, r.value, r.status) for r in result.internal_resolutions] == [
        (("hof",), 0, "PASS"),
        (("hof",), 1, "PASS"),
    ]


def test_missing_positive_membership_is_a_closed_world_mismatch(tmp_path: Path) -> None:
    """Catches an omitted external positive fact yielding an empty successful result."""
    result = execute_pfr_awards(_spec(), _paths(tmp_path, bio=[
        ("P1", "N1", "true", "2", "3"),
        ("P0", "N0", "false", "1", "0"),
    ]))

    zero = [item for item in result.observations if dict(item.evidence)["NFL_player_id"] == "N0"]
    assert {item.status for item in result.observations} == {"MISMATCH"}
    assert dict(next(item.evidence for item in zero if item.targets == ("allpro",)))["source_value"] == "0"
    assert not any(item.status == "PASS" for item in result.observations)


def test_source_membership_without_a_crosswalk_identity_is_rejected(tmp_path: Path) -> None:
    """Catches ignoring a typed page fact that has no stable canonical identity."""
    paths = _paths(tmp_path, bio=[("P0", "N0", None, None, None)])
    result = execute_pfr_awards(_spec(), paths)

    assert any("unresolved identity" in error for error in result.errors)
    assert not any(item.status == "PASS" for item in result.observations)


def test_core_import_deterministically_enrolls_all_builtin_composite_executors() -> None:
    """Catches core-only imports relying on unrelated executor import order."""
    code = """
import json
from scripts.sota_recon.composite_witness_lane import EXECUTORS
print(json.dumps({key: f'{fn.__module__}.{fn.__name__}' for key, fn in sorted(EXECUTORS.items())}))
"""
    completed = subprocess.run(
        [sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[2],
        check=True, capture_output=True, text=True,
    )

    assert json.loads(completed.stdout) == {
        "nflcom_l7": "scripts.sota_recon.composite_witness_nflcom.execute_nflcom_l7",
        "pfr_awards": "scripts.sota_recon.composite_witness_pfr_awards.execute_pfr_awards",
        "pfr_field_goals": "scripts.sota_recon.composite_witness_pfr_fg.execute_pfr_field_goals",
        "pfr_two_point_total": "scripts.sota_recon.composite_witness_pfr_two_pt.execute_pfr_two_point",
    }


def test_feature_first_imports_leave_the_core_builtin_dispatch_complete() -> None:
    """Catches core bootstrap dereferencing a partially initialized feature module."""
    expected = {
        "nflcom_l7": "scripts.sota_recon.composite_witness_nflcom.execute_nflcom_l7",
        "pfr_awards": "scripts.sota_recon.composite_witness_pfr_awards.execute_pfr_awards",
        "pfr_field_goals": "scripts.sota_recon.composite_witness_pfr_fg.execute_pfr_field_goals",
        "pfr_two_point_total": "scripts.sota_recon.composite_witness_pfr_two_pt.execute_pfr_two_point",
    }
    for feature in (
        "scripts.sota_recon.composite_witness_nflcom",
        "scripts.sota_recon.composite_witness_pfr_awards",
        "scripts.sota_recon.composite_witness_pfr_fg",
        "scripts.sota_recon.composite_witness_pfr_two_pt",
    ):
        code = f"""
import importlib
import json
importlib.import_module({feature!r})
from scripts.sota_recon.composite_witness_lane import EXECUTORS
print(json.dumps({{key: f'{{fn.__module__}}.{{fn.__name__}}' for key, fn in sorted(EXECUTORS.items())}}))
"""
        completed = subprocess.run(
            [sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[2],
            check=True, capture_output=True, text=True,
        )
        assert json.loads(completed.stdout) == expected


def test_core_rejects_a_completed_builtin_module_missing_its_executor() -> None:
    """Catches silently deferring a preloaded module that cannot ever self-register."""
    code = """
import importlib.machinery
import sys
import types
from scripts.sota_recon import composite_witness_lane as core

name = 'scripts.sota_recon.composite_witness_pfr_awards'
incomplete = types.ModuleType(name)
incomplete.__spec__ = importlib.machinery.ModuleSpec(name, loader=None)
sys.modules[name] = incomplete
core.EXECUTORS.clear()
try:
    core._register_builtin_executors()
except RuntimeError as exc:
    print(str(exc))
else:
    raise SystemExit('completed incomplete module was silently accepted')
"""
    completed = subprocess.run(
        [sys.executable, "-c", code], cwd=Path(__file__).resolve().parents[2],
        check=True, capture_output=True, text=True,
    )

    assert "builtin composite executor" in completed.stdout
    assert "execute_pfr_awards" in completed.stdout
