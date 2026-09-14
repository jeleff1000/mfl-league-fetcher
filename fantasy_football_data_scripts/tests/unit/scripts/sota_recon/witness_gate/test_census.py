from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.sota_recon.witness_gate.census import compare_discoveries, load_census


FIXTURES = Path(__file__).parent / "fixtures"


def test_load_census_is_versioned_and_hash_is_deterministic() -> None:
    first = load_census(FIXTURES / "census_valid.json")
    second = load_census(FIXTURES / "census_valid.json")

    assert first.source_universe_version == "fixture-v1"
    assert first.fingerprint == second.fingerprint
    assert len(first.datasets) == 2


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda body: body["datasets"].append(dict(body["datasets"][0])), "duplicate dataset_id"),
        (
            lambda body: body["datasets"][1].update(
                {"physical_globs": body["datasets"][0]["physical_globs"]}
            ),
            "physical glob",
        ),
        (lambda body: body["datasets"][0].update({"year_start": 2025, "year_end": 1920}), "year range"),
        (lambda body: body["datasets"][0].update({"producer": {"kind": "ff_assets"}}), "manifest pin"),
    ],
)
def test_invalid_census_fails_closed(tmp_path: Path, mutation, message: str) -> None:
    body = json.loads((FIXTURES / "census_valid.json").read_text(encoding="utf-8"))
    mutation(body)
    path = tmp_path / "invalid.json"
    path.write_text(json.dumps(body), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        load_census(path)


def test_pending_ff_assets_pin_is_explicit_and_parseable(tmp_path: Path) -> None:
    body = json.loads((FIXTURES / "census_valid.json").read_text(encoding="utf-8"))
    body["datasets"][0]["producer"] = {"kind": "ff_assets", "pin_status": "pending_pin"}
    path = tmp_path / "pending.json"
    path.write_text(json.dumps(body), encoding="utf-8")

    census = load_census(path)

    assert census.datasets[0].producer.pin_status == "pending_pin"


def test_unreviewed_discovery_is_a_failing_delta() -> None:
    census = load_census(FIXTURES / "census_valid.json")
    current = json.loads((FIXTURES / "census_discovery_delta.json").read_text(encoding="utf-8"))

    delta = compare_discoveries(census.discoveries, current)

    assert delta.has_changes
    assert [item.url for item in delta.added] == ["https://www.nfl.com/teams/example/roster/1921"]
    assert delta.removed == ()
