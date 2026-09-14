from __future__ import annotations

import json

import pytest

from top10_stability_policy import (
    AnnualPolicyResult,
    PolicyManifest,
    build_forward_policy,
    load_stability_policy,
    policy_threshold,
    write_stability_policy,
)


def _row(year: int, minimum: int | None, *, state: str = "stable", population: int = 400,
         fully_observable: bool = True) -> AnnualPolicyResult:
    return AnnualPolicyResult(
        dataset="matchup", grain="season", metric="start_rate", position="ALL",
        base_cohort="12t/flx/half/4pt", members=("12t/flx/half/4pt",), year=year,
        population=population, state=state, min_n=minimum,
        fully_observable=fully_observable,
    )


def _manifest() -> PolicyManifest:
    return PolicyManifest(
        source_hash="source", cluster_map_hash="clusters", contract_hash="contract",
        git_commit="abc123", seed=20260727, repetitions=500,
        generated_at="2026-07-27T00:00:00Z",
    )


def test_forward_threshold_is_maximum_fully_observed_annual_minimum() -> None:
    policy = build_forward_policy([_row(2023, 35), _row(2024, 50), _row(2025, 40)], _manifest())

    entry = policy.entries[0]
    assert entry.forward_threshold == 50
    assert entry.state == "stable"
    assert entry.evidence_years == (2023, 2024, 2025)


def test_recent_right_censoring_blocks_forward_threshold() -> None:
    policy = build_forward_policy(
        [_row(2024, 50), _row(2025, None, state="right_censored", population=80,
                              fully_observable=False)],
        _manifest(),
    )

    assert policy.entries[0].forward_threshold is None
    assert policy.entries[0].state == "right_censored"


def test_policy_lookup_fails_closed_for_missing_or_censored_entries() -> None:
    policy = build_forward_policy([_row(2025, 50)], _manifest())

    assert policy_threshold(policy, dataset="matchup", grain="season", metric="start_rate",
                            position="ALL", base_cohort="12t/flx/half/4pt") == 50
    assert policy_threshold(policy, dataset="draft", grain="season", metric="adp",
                            position="ALL", base_cohort="12t/flx/half/4pt") is None


def test_written_policy_round_trips_and_keeps_annual_evidence(tmp_path) -> None:
    policy = build_forward_policy([_row(2024, 35), _row(2025, 50)], _manifest())
    path = tmp_path / "policy.json"

    write_stability_policy(policy, path)
    loaded = load_stability_policy(path)

    assert loaded == policy
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["entries"][0]["annual_evidence"][1]["min_n"] == 50


def test_overlapping_member_definitions_fail_closed() -> None:
    bad = _row(2025, 50)
    duplicate = AnnualPolicyResult(**{**bad.__dict__, "members": ("x", "x")})
    with pytest.raises(ValueError, match="members"):
        build_forward_policy([duplicate], _manifest())
