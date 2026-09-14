"""Receipts for the ONE stat contract registry (master plan §6 + §25.7, executed 2026-07-25).

Validates the COMMITTED stat_contracts.v1.json (no D: dependency, CI-safe):
  - coverage gate: one row per stat_id, no duplicates, all declared grains covered
  - every conflict is adjudicated (rule_id present in adjudication_rules, receipt non-empty)
  - §25.7 regression receipts: the live-verified legacy metadata defects can never re-enter
  - self-consistency: rate rows carry components or a typed direct-rate exception; MAX rows are
    never fact-adjusted; polarity/sort are separate fields
  - consumer parity: the aggregation_sets block equals the frozen pre-switch legacy sets, and the
    two switched consumers (aggregate_nfl_stats_fly, recon_aggregate) actually read it
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
REG_PATH = HERE / "witness_gate" / "contracts" / "stat_contracts.v1.json"

_FFS = HERE.parent.parent / "fantasy_football_data_scripts"
if str(_FFS) not in sys.path:
    sys.path.insert(0, str(_FFS))


@pytest.fixture(scope="module")
def reg():
    return json.loads(REG_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def by_id(reg):
    return {r["stat_id"]: r for r in reg["stats"]}


def test_registry_exists_and_versioned(reg):
    assert reg["contract_version"] == "1"
    assert reg["counts"]["stats"] == len(reg["stats"]) > 1100


def test_exactly_one_row_per_stat(reg, by_id):
    assert len(by_id) == len(reg["stats"]), "duplicate stat_id rows"


def test_grain_coverage_complete(reg, by_id):
    # every column counted per grain appears in exactly one row that declares that grain
    for grain, n in reg["counts"]["by_grain"].items():
        rows = [r for r in reg["stats"] if grain in r["grains"]]
        assert len(rows) == n, f"{grain}: {len(rows)} contract rows != {n} live columns"


def test_all_conflicts_adjudicated(reg):
    rules = reg["adjudication_rules"]
    assert reg["conflicts"], "conflict report missing -- five sources cannot fully agree"
    for c in reg["conflicts"]:
        assert c["rule_id"] in rules, f"conflict {c['stat']}:{c['field']} has unknown rule"
        assert len(rules[c["rule_id"]]) > 40, "rule receipt is not a real receipt"
        assert c["resolution"] is not None


def test_sec25_7_regressions_cannot_reenter(by_id):
    # live-verified defects in the legacy research metadata (§25.7 receipts)
    assert by_id["age"]["unit"] == "age_years"
    assert by_id["age"]["benefit_polarity"] == "contextual"
    assert by_id["air_yards_share"]["unit"] == "ratio"
    for col in ("fumbles_lost", "sacks_suffered", "fg_missed", "pat_missed", "gwfg_missed",
                "sack_yards_lost", "receiving_target_interceptions", "passing_interceptions",
                "points_allowed", "dst_points_allowed"):
        assert by_id[col]["benefit_polarity"] == "negative", col
    # variant polarity inheritance (the per-game/per-season generator flip bug)
    assert by_id["passing_int_pct"]["benefit_polarity"] == "negative"
    assert by_id["sack_pct"]["benefit_polarity"] == "negative"
    # polarity and sort direction are separate fields, always
    for r in by_id.values():
        assert "benefit_polarity" in r and "default_sort_direction" in r


def test_rate_rows_carry_components_or_typed_exception(reg):
    for r in reg["stats"]:
        if r["aggregation_class"] in ("RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE"):
            rate = r.get("rate", {})
            ok = rate.get("components") or rate.get("direct_rate_no_components") \
                or rate.get("weighted_by")
            assert ok, f"{r['stat_id']}: rate row without components or typed exception"


def test_max_class_sane(by_id):
    for col in ("fg_long", "passing_long", "receiving_long", "rushing_long",
                "punt_long", "kickoff_return_long", "punt_return_long"):
        assert by_id[col]["aggregation_class"] == "MAX", col
        assert by_id[col]["season_derivation"] == "max_weekly"


def test_fact_adjusted_never_max_or_rate(reg, by_id):
    for col in reg["aggregation_sets"]["fact_adjusted_season_cols"]:
        assert by_id[col]["aggregation_class"] == "SUM", col


def test_evidence_gap_1993(by_id):
    gaps = by_id["passing_epa"]["eras"]["evidence_gaps"]
    assert any(g["year"] == 1993 for g in gaps)


def test_aggregation_sets_parity_with_frozen_legacy(reg):
    from scripts.sota_recon.witness_gate import build_stat_contracts as B
    s = reg["aggregation_sets"]
    assert set(s["derived_rate_cols"]) == B.LEGACY_AGG_DERIVED_RATE_COLS
    assert {k: tuple(v) for k, v in s["derived_rate_dependencies"].items()} == \
        B.LEGACY_AGG_DERIVED_RATE_DEPENDENCIES
    assert s["weighted_avg_cols"] == B.LEGACY_AGG_WEIGHTED_AVG_COLS
    assert set(s["max_cols"]) == B.LEGACY_AGG_MAX_COLS
    assert set(s["fact_adjusted_season_cols"]) == B.LEGACY_RA_FACT_ADJUSTED
    assert set(s["non_additive_exact"]) == B.LEGACY_RA_NONADD_EXACT
    assert tuple(s["non_additive_prefixes"]) == B.LEGACY_RA_NONADD_PREFIX


def test_consumers_read_registry():
    from multi_league.core.stat_contracts_loader import aggregation_sets
    sets = aggregation_sets()
    import multi_league.data_fetchers.aggregate_nfl_stats_fly as AGG
    assert AGG.DERIVED_RATE_COLS is sets.derived_rate_cols
    assert AGG.MAX_COLS is sets.max_cols
    assert AGG.WEIGHTED_AVG_COLS is sets.weighted_avg_cols
    assert AGG.DERIVED_RATE_DEPENDENCIES is sets.derived_rate_dependencies
    from scripts.sota_recon import recon_aggregate as RA
    assert RA._FACT_ADJUSTED is sets.fact_adjusted_season_cols
    assert RA._NONADD_EXACT is sets.non_additive_exact
    assert tuple(RA._NONADD_PREFIX) == sets.non_additive_prefixes


def test_identity_context_never_aggregate(reg):
    for r in reg["stats"]:
        if r["family"] in ("identity", "context", "provenance"):
            assert r["aggregation_class"] == "NON_AGGREGATABLE", r["stat_id"]
            assert r["benefit_polarity"] == "neutral", r["stat_id"]


def test_ngs_rate_contracts_cannot_be_summed(by_id):
    for col in ("ngs_aggressiveness", "ngs_rush_efficiency"):
        assert by_id[col]["aggregation_class"] == "WEIGHTED_RECOMPUTE", col
        assert by_id[col]["unit"] == "ratio", col
        assert by_id[col]["rate"]["direct_rate_no_components"] is True, col

    assert by_id["ngs_rush_yards_over_expected"]["unit"] == "yards"


def test_tolerance_policy_declared_on_every_stat(reg):
    """§17.1 (wired 2026-07-26): tolerance is a declared per-stat policy, default EXACT.
    A non-EXACT policy must carry a numeric tolerance, a basis, and a receipt --
    otherwise it is an ad-hoc TOL and illegal."""
    for r in reg["stats"]:
        pol = r.get("tolerance_policy")
        assert pol is not None, r["stat_id"]
        if pol["policy"] == "EXACT":
            assert set(pol) == {"policy"}, r["stat_id"]
        else:
            assert pol["policy"] == "DECLARED", r["stat_id"]
            assert isinstance(pol.get("tolerance"), (int, float)), r["stat_id"]
            assert pol.get("basis") and pol.get("receipt"), r["stat_id"]
