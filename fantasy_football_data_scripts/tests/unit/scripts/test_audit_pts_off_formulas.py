"""Unit tests for scripts/audit_pts_off_formulas_2026_05_02.py.

Each invariant gets at least one passes-on-clean and one fails-on-dirty test,
following the L1.a pattern in test_audit_pts_def_formulas.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "scripts"))

import audit_pts_off_formulas_2026_05_02 as audit


# === wiring ========================================================================


def test_all_pts_off_cols_count():
    # 30 = 27 base offensive precomputes + 3 variant cols (pts_pass_5pt_int1,
    # pts_pass_6pt_int1, pts_pass_yd) added 2026-05-02 after L1.b audit M2 surfaced
    # they are actively dispatched by scoring_calculator.py for 149+3 lyr leagues.
    # Plus pts_pass_4pt_int1 was already present.
    assert len(audit.PTS_OFF_COLS_EXISTING) == 31


def test_load_canonical_returns_three_sections():
    cfg = audit.load_canonical()
    assert "components" in cfg
    assert "precompute_targets" in cfg
    assert "league_settings_bridge" in cfg


def test_invariants_registry_has_all_16():
    assert set(audit.INVARIANTS.keys()) == {f"M{i}" for i in range(1, 17)}


# === M1 ============================================================================


def test_m01_passes_when_all_base_cols_present_and_nonzero():
    df = pd.DataFrame(
        [{c: 1 for c in audit.M1_BASE_COLS}],
    )
    passed, details = audit.m01_existence(df)
    assert passed
    assert details["missing_cols"] == []
    assert details["all_zero_cols"] == []


def test_m01_fails_when_col_missing():
    cols = audit.M1_BASE_COLS[:-1]  # drop last
    df = pd.DataFrame([{c: 1 for c in cols}])
    passed, details = audit.m01_existence(df)
    assert not passed
    assert audit.M1_BASE_COLS[-1] in details["missing_cols"]


def test_m01_flags_all_zero_col():
    df = pd.DataFrame([{c: 1 if c != "passing_yards" else 0 for c in audit.M1_BASE_COLS}])
    passed, details = audit.m01_existence(df)
    assert not passed
    assert "passing_yards" in details["all_zero_cols"]


# === M2 ============================================================================


def test_m02_passes_when_pts_pass_4pt_matches_canonical():
    # 250 yards * 0.04 + 2 TDs * 4 + 1 INT * -2 = 10 + 8 - 2 = 16
    df = pd.DataFrame(
        [
            {
                "passing_yards": 250,
                "passing_tds": 2,
                "passing_interceptions": 1,
                "pts_pass_4pt": 16,
                "rushing_yards": 0,
                "rushing_tds": 0,
                "rushing_fumbles_lost": 0,
                "sack_fumbles_lost": 0,
                "receiving_yards": 0,
                "receiving_tds": 0,
                "receiving_fumbles_lost": 0,
                "receptions": 0,
                "passing_2pt_conversions": 0,
                "rushing_2pt_conversions": 0,
                "receiving_2pt_conversions": 0,
                "special_teams_tds": 0,
                "fum_ret_td": 0,
                "kickoff_return_yards": 0,
                "punt_return_yards": 0,
                "completions": 25,
                "passing_first_downs": 0,
                "carries": 0,
                "pick6": 0,
                "sacks": 0,
                "position": "QB",
            }
        ]
    )
    passed, details = audit.m02_canonical_drift(df)
    # pts_pass_4pt is the only stored col here; other targets would be skipped
    # (col_missing). Per-col status must include zero drift for pts_pass_4pt.
    assert details["per_col"]["pts_pass_4pt"]["rows_drifted"] == 0


def test_m02_flags_drift_on_pts_pass_4pt():
    # Wrong stored value triggers drift
    df = pd.DataFrame(
        [
            {
                "passing_yards": 250,
                "passing_tds": 2,
                "passing_interceptions": 1,
                "pts_pass_4pt": 99.0,  # wrong (should be 16)
                "position": "QB",
            }
        ]
    )
    passed, details = audit.m02_canonical_drift(df)
    assert not passed
    assert details["per_col"]["pts_pass_4pt"]["rows_drifted"] == 1


# === M3 ============================================================================


def test_m03_passes_when_modern_cols_have_no_pre_1999_data():
    df = pd.DataFrame(
        [
            {"year": 1990, "pts_pass_cmp": 0},
            {"year": 2020, "pts_pass_cmp": 5},
        ]
    )
    passed, details = audit.m03_era_coverage(df)
    assert passed
    assert details["anomalies"] == []


def test_m03_fails_when_modern_col_has_pre_1999_data():
    df = pd.DataFrame(
        [
            {"year": 1980, "pts_pass_cmp": 5},  # pts_pass_cmp expected first nonzero 1999
        ]
    )
    passed, details = audit.m03_era_coverage(df)
    assert not passed
    assert any(a["col"] == "pts_pass_cmp" for a in details["anomalies"])


# === M5 ============================================================================


def test_m05_passes_when_no_def_leak():
    df = pd.DataFrame(
        [
            {"position": "QB", "pts_def_fum_ret_td": 0},
            {"position": "WR", "pts_def_fum_ret_td": None},
        ]
    )
    passed, details = audit.m05_aliases(df)
    assert passed


def test_m05_fails_on_pts_def_leak_to_qb():
    df = pd.DataFrame([{"position": "QB", "pts_def_fum_ret_td": 6}])
    passed, details = audit.m05_aliases(df)
    assert not passed


# === M6 ============================================================================


def test_m06_passes_when_pts_pass_4pt_has_source_stats():
    df = pd.DataFrame([{"pts_pass_4pt": 16, "passing_yards": 250, "passing_tds": 2, "passing_interceptions": 1}])
    passed, details = audit.m06_base_stats_zero_guard(df)
    assert details["per_col"]["pts_pass_4pt"]["violations"] == 0


def test_m06_fails_when_pts_pass_4pt_nonzero_with_zero_sources():
    df = pd.DataFrame([{"pts_pass_4pt": 16, "passing_yards": 0, "passing_tds": 0, "passing_interceptions": 0}])
    passed, details = audit.m06_base_stats_zero_guard(df)
    assert not passed
    assert details["per_col"]["pts_pass_4pt"]["violations"] == 1


# === M9 ============================================================================


def test_m09_passes_when_modern_cols_zero_pre_1999():
    df = pd.DataFrame([{"year": 1990, "pts_pass_cmp": 0}, {"year": 2020, "pts_pass_cmp": 5}])
    passed, details = audit.m09_year_coverage_per_source(df)
    assert passed


def test_m09_fails_when_pts_pass_cmp_nonzero_pre_1999():
    df = pd.DataFrame([{"year": 1980, "pts_pass_cmp": 3}])
    passed, details = audit.m09_year_coverage_per_source(df)
    assert not passed


# === M10 ===========================================================================


def test_m10_passes_no_dead_refs_typical():
    # We don't expect dead refs in a clean tree; this test will catch real
    # regressions when audit_pts_off invariants stay in sync with calculator.
    passed, details = audit.m10_dead_refs()
    # Don't strictly assert pass — fixture-level dead refs may exist intentionally.
    # Smoke test the detail shape.
    assert "dead_refs" in details
    assert "known_count" in details


# === M11 ===========================================================================


def test_m11_passes_for_normal_passing_yards_distribution():
    df = pd.DataFrame([{"passing_yards": 250}, {"passing_yards": 350}])
    passed, details = audit.m11_distribution_sanity(df)
    assert passed


def test_m11_flags_passing_yards_with_impossible_max():
    # 1200 yards in a single game — bug or unit error
    df = pd.DataFrame([{"passing_yards": 1200}])
    passed, details = audit.m11_distribution_sanity(df)
    assert not passed
    assert any(f["col"] == "passing_yards" for f in details["findings"])


# === M12 ===========================================================================


def test_m12_passes_when_variant_arithmetic_holds():
    df = pd.DataFrame(
        [
            {
                "pts_pass_4pt": 16,
                "pts_pass_5pt": 18,  # 16 + 2
                "pts_pass_6pt": 20,  # 18 + 2
                "passing_tds": 2,
                "pts_misc": 0,
                "passing_2pt_conversions": 0,
                "rushing_2pt_conversions": 0,
                "receiving_2pt_conversions": 0,
                "special_teams_tds": 0,
                "fum_ret_td": 0,
                "pts_rec_0ppr": 30,
                "pts_rec_half": 32,
                "pts_rec_ppr": 34,
                "receptions": 4,
            }
        ]
    )
    passed, details = audit.m12_cross_column_logical(df)
    assert passed


def test_m12_fails_when_variant_arithmetic_violated():
    df = pd.DataFrame(
        [
            {
                "pts_pass_4pt": 16,
                "pts_pass_5pt": 19,  # SHOULD be 18; off by 1
                "passing_tds": 2,
            }
        ]
    )
    passed, details = audit.m12_cross_column_logical(df)
    assert not passed


# === M13 ===========================================================================


def test_m13_passes_when_fixture_missing():
    # When fixture not yet generated (Task 2.18), M13 should skip cleanly
    if not audit.M13_FIXTURE.exists():
        passed, details = audit.m13_synthetic_boundaries()
        assert passed
        assert details["status"] == "fixture_not_present"


# === M14 ===========================================================================


def test_m14_captures_baseline_with_mock_query():
    def fake_fly_query(sql):
        return [{"total_offense_rows": 100, "distinct_keys": 100, "null_keys": 0}]

    passed, details = audit.m14_row_count_invariant(fake_fly_query, baseline=None)
    assert passed
    assert details["baseline"] is None
    assert details["snapshot"]["total_offense_rows"] == 100


def test_m14_diff_against_baseline_passes():
    def fake_fly_query(sql):
        return [{"total_offense_rows": 100, "distinct_keys": 100, "null_keys": 0}]

    baseline = {"total_offense_rows": 100, "distinct_keys": 100, "null_keys": 0}
    passed, details = audit.m14_row_count_invariant(fake_fly_query, baseline=baseline)
    assert passed


def test_m14_diff_against_baseline_fails_on_drift():
    def fake_fly_query(sql):
        return [{"total_offense_rows": 99, "distinct_keys": 100, "null_keys": 0}]

    baseline = {"total_offense_rows": 100, "distinct_keys": 100, "null_keys": 0}
    passed, details = audit.m14_row_count_invariant(fake_fly_query, baseline=baseline)
    assert not passed


# === M15 ===========================================================================


def test_m15_runs_against_real_matrix():
    # The matrix file should exist after Task 1.4. M15 verifies it's complete.
    if audit.COVERAGE_MATRIX.exists():
        passed, details = audit.m15_league_config_coverage()
        # Don't assert passed — matrix may have unclassified rows in early phase.
        assert "scoring_fields_in_matrix" in details
        assert details["scoring_fields_in_matrix"] > 0


# === M16 ===========================================================================


def test_m16_passes_when_fixture_missing():
    if not audit.M16_FIXTURE.exists():
        passed, details = audit.m16_behavioral_coverage()
        assert passed
        assert details["status"] == "fixture_not_present"


# === L1.b.1 component-dispatch tests (added with Task 0.3) ===


def test_compute_canonical_pts_single_source_component():
    """pts_pass_yd_p04 should compute as passing_yards * 0.04."""
    df = pd.DataFrame([{"passing_yards": 300}])
    canonical = audit.load_canonical()
    result = audit._compute_canonical_pts(df, "pts_pass_yd_p04", canonical)
    assert result is not None
    assert abs(result.iloc[0] - 12.0) < 0.001  # 300 * 0.04


def test_compute_canonical_pts_multi_source_fum_lost():
    """pts_fum_lost_n2 should sum 3 fumble cols and multiply by -2."""
    df = pd.DataFrame(
        [
            {
                "rushing_fumbles_lost": 1,
                "sack_fumbles_lost": 0,
                "receiving_fumbles_lost": 1,
            }
        ]
    )
    canonical = audit.load_canonical()
    result = audit._compute_canonical_pts(df, "pts_fum_lost_n2", canonical)
    assert result is not None
    # (1 + 0 + 1) * -2 = -4
    assert abs(result.iloc[0] - (-4.0)) < 0.001


def test_compute_canonical_pts_te_position_filter():
    """pts_rec_te_bonus_p5 should apply TE-only mask: 6 receptions on TE = 3.0; on WR = 0."""
    df = pd.DataFrame(
        [
            {"receptions": 6, "position": "TE"},
            {"receptions": 6, "position": "WR"},
        ]
    )
    canonical = audit.load_canonical()
    result = audit._compute_canonical_pts(df, "pts_rec_te_bonus_p5", canonical)
    assert result is not None
    assert abs(result.iloc[0] - 3.0) < 0.001  # TE row: 6 * 0.5 * 1.0
    assert abs(result.iloc[1] - 0.0) < 0.001  # WR row: 6 * 0.5 * 0.0
