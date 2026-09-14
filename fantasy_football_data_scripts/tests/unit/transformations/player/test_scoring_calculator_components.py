"""TDD tests for L1.b.1 component dispatch helpers in scoring_calculator.

Verifies _pick_component() and _snap_float_fuzz() — used by per-league
SQL builders to translate (scoring_field, multiplier) into either a
precomputed component col (top mult) or a runtime atomic-stat expression
(edge mult).
"""

from __future__ import annotations

import pytest
import polars as pl
import duckdb

from fantasy_football_data_scripts.multi_league.transformations.player.modules.scoring_calculator import (
    calculate_fantasy_points_from_precalc,
    get_scoring_columns,
    _pick_component,
    _snap_float_fuzz,
)


# === _snap_float_fuzz ===


def test_snap_float_fuzz_yahoo_004():
    # Yahoo serializes 0.04 as 0.03999999910593033
    assert _snap_float_fuzz(0.03999999910593033) == 0.04


def test_snap_float_fuzz_yahoo_005():
    assert _snap_float_fuzz(0.05000000074505806) == 0.05


def test_snap_float_fuzz_yahoo_01():
    assert _snap_float_fuzz(0.10000000149011612) == 0.1


def test_snap_float_fuzz_no_snap_for_distinct():
    # 0.06 is not within 1e-6 of any canonical value — return as-is
    assert _snap_float_fuzz(0.06) == 0.06


def test_snap_float_fuzz_exact_canonical_unchanged():
    assert _snap_float_fuzz(0.04) == 0.04
    assert _snap_float_fuzz(1.0) == 1.0


def test_snap_float_fuzz_negative():
    # Negative multipliers (passing_interceptions etc.) — not in fuzz table
    # but should pass through unchanged
    assert _snap_float_fuzz(-2.0) == -2.0
    assert _snap_float_fuzz(-1.0) == -1.0


# === _pick_component (component lookup) ===


def test_pick_component_pass_yd_default_returns_precompute_col():
    """0.04 mult on pass_yd hits the top-mult precompute."""
    expr = _pick_component("pass_yd", 0.04)
    assert "pts_pass_yd_p04" in expr
    assert "COALESCE" in expr


def test_pick_component_pass_yd_yahoo_fuzz_snaps():
    """Yahoo 0.03999... fuzz snaps to canonical 0.04 -> still hits precompute."""
    expr = _pick_component("pass_yd", 0.03999999910593033)
    assert "pts_pass_yd_p04" in expr


def test_pick_component_pass_yd_edge_mult_returns_runtime_expr():
    """0.05 mult is not in precompute set -> runtime atomic expression."""
    expr = _pick_component("pass_yd", 0.05)
    assert "passing_yards" in expr
    assert "0.05" in expr
    assert "pts_pass_yd_p04" not in expr


def test_pick_component_pass_td_4():
    expr = _pick_component("pass_td", 4)
    assert "pts_pass_td_4" in expr


def test_pick_component_pass_td_6():
    expr = _pick_component("pass_td", 6)
    assert "pts_pass_td_6" in expr


def test_pick_component_pass_td_5_falls_through_to_runtime():
    """5pt didn't meet 95% threshold so no pts_pass_td_5 component exists."""
    expr = _pick_component("pass_td", 5)
    assert "passing_tds" in expr and "* 5" in expr
    assert "pts_pass_td_5" not in expr


def test_pick_component_pass_int_n2():
    expr = _pick_component("pass_int", -2)
    assert "pts_pass_int_n2" in expr


def test_pick_component_pass_int_n1():
    expr = _pick_component("pass_int", -1)
    assert "pts_pass_int_n1" in expr


def test_pick_component_rec_half():
    expr = _pick_component("rec", 0.5)
    assert "pts_rec_p5" in expr


def test_pick_component_rec_ppr():
    expr = _pick_component("rec", 1.0)
    assert "pts_rec_1" in expr


def test_pick_component_fum_lost_n2_multi_source():
    """fum_lost runtime fallback should sum 3 atomic source cols."""
    expr = _pick_component("fum_lost", -2)
    assert "pts_fum_lost_n2" in expr  # this multiplier hits the precompute


def test_pick_component_fum_lost_edge_runtime():
    """Edge mult on fum_lost — runtime path needs all 3 source cols."""
    expr = _pick_component("fum_lost", -3)
    assert "rushing_fumbles_lost" in expr
    assert "sack_fumbles_lost" in expr
    assert "receiving_fumbles_lost" in expr


def test_pick_component_unknown_stat_key_raises():
    with pytest.raises(ValueError, match="Unknown stat_key"):
        _pick_component("nonexistent_stat", 1.0)


def test_legacy_yahoo_scoring_columns_score_pass_fd_and_turnover_return_yards():
    columns = get_scoring_columns(
        {
            "scoring": [
                {"stat_id": "79", "stat": "Passing 1st Downs", "points": 0.5},
                {"stat_id": "66", "stat": "Turnover Return Yards", "points": 0.1},
            ]
        }
    )

    assert ("passing_first_downs", 0.5) in columns["corrections"]
    assert ("def_interception_yards", 0.1) in columns["corrections"]
    assert ("fumble_recovery_yards_own", 0.1) in columns["corrections"]
    assert ("fumble_recovery_yards_opp", 0.1) in columns["corrections"]

    df = pl.DataFrame(
        {
            "pts_pass_4pt": [0.0],
            "passing_first_downs": [4],
            "def_interception_yards": [10],
            "fumble_recovery_yards_own": [5],
            "fumble_recovery_yards_opp": [7],
        }
    )
    scored = calculate_fantasy_points_from_precalc(df, columns)

    assert scored["fantasy_points"][0] == pytest.approx(4 * 0.5 + (10 + 5 + 7) * 0.1)


# === Task B.2: build_components_fantasy_points_sql ===

from fantasy_football_data_scripts.multi_league.transformations.player.modules.scoring_calculator import (
    build_components_fantasy_points_sql,
    build_yahoo_stat_id_fantasy_points_sql,
)


def test_build_sql_minimal_sleeper_redraft_half_ppr():
    """Half-PPR redraft: pass_td=4, pass_int=-2, rush_yd=0.1, rec=0.5, fum_lost=-2 etc."""
    rules = {
        "scoring_settings": {
            "pass_td": 4,
            "pass_yd": 0.04,
            "pass_int": -2,
            "rush_yd": 0.1,
            "rush_td": 6,
            "rec": 0.5,
            "rec_yd": 0.1,
            "rec_td": 6,
            "fum_lost": -2,
        }
    }
    sql = build_components_fantasy_points_sql(rules)
    # Top-mult components hit precompute path
    assert "pts_pass_yd_p04" in sql
    assert "pts_pass_td_4" in sql
    assert "pts_pass_int_n2" in sql
    assert "pts_rush_yd_p1" in sql
    assert "pts_rush_td_6" in sql
    assert "pts_rec_yd_p1" in sql
    assert "pts_rec_td_6" in sql
    assert "pts_rec_p5" in sql
    assert "pts_fum_lost_n2" in sql


def test_build_sql_edge_mult_falls_through_to_runtime():
    """5pt pass_td has no component (didn't meet 95% threshold) — runtime path."""
    rules = {"scoring_settings": {"pass_td": 5}}
    sql = build_components_fantasy_points_sql(rules)
    assert "passing_tds" in sql
    assert "* 5" in sql
    assert "pts_pass_td_5" not in sql


def test_build_sql_sleeper_top_level_dict():
    """Sleeper format also accepts flat dict without scoring_settings wrapper."""
    rules = {"pass_td": 4, "rec": 1.0}
    sql = build_components_fantasy_points_sql(rules)
    assert "pts_pass_td_4" in sql
    assert "pts_rec_1" in sql


def test_build_sql_with_bonus_flags():
    rules = {
        "scoring_settings": {
            "pass_td": 4,
            "pass_yd": 0.04,
            "pass_int": -2,
            "bonus_pass_yd_300": 3,
            "bonus_rec_yd_100": 1.5,
        }
    }
    sql = build_components_fantasy_points_sql(rules)
    assert "bonus_pass_300yd" in sql
    assert "* 3" in sql
    assert "bonus_rec_100yd" in sql
    assert "* 1.5" in sql


def test_build_sql_with_te_bonus():
    rules = {
        "scoring_settings": {
            "pass_td": 4,
            "pass_yd": 0.04,
            "rec": 1.0,
            "rec_yd": 0.1,
            "rec_td": 6,
            "bonus_rec_te": 0.5,
        }
    }
    sql = build_components_fantasy_points_sql(rules)
    assert "pts_rec_te_bonus_p5" in sql


def test_build_sql_zero_skipped():
    """Scoring keys with value 0 should NOT appear in the expression."""
    rules = {
        "scoring_settings": {
            "pass_td": 4,
            "pass_yd": 0.04,
            "pass_cmp": 0,  # skip
            "rush_att": 0,  # skip
        }
    }
    sql = build_components_fantasy_points_sql(rules)
    # Active terms present
    assert "pts_pass_yd_p04" in sql
    assert "pts_pass_td_4" in sql
    # Inactive terms absent (no mention of completions or carries)
    assert "completions" not in sql
    assert "carries" not in sql
    assert "pts_pass_cmp" not in sql
    assert "pts_rush_att" not in sql


def test_build_yahoo_stat_id_sql_scores_h_town_bonus_and_two_point_case():
    rules = {
        "scoring_settings": {
            "pass_cmp": 0.25,
            "pass_yd": 0.033333333333333,
            "pass_td": 5,
            "pass_int": -3,
            "pass_2pt": 2,
            "rush_2pt": 2,
            "rec_2pt": 2,
            "bonus_pass_yd_300": 5,
        }
    }
    expr = build_yahoo_stat_id_fantasy_points_sql(rules, table_alias="p")

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE player_fantasy (
            yahoo_stat_2 DOUBLE,
            yahoo_stat_4 DOUBLE,
            yahoo_stat_5 DOUBLE,
            yahoo_stat_6 DOUBLE,
            yahoo_stat_16 DOUBLE
        )
    """)
    conn.execute("INSERT INTO player_fantasy VALUES (27, 305, 2, 0, 1)")
    scored = conn.execute(f"SELECT {expr} AS points FROM player_fantasy p").fetchone()[0]

    assert scored == pytest.approx(33.916666666666565)


def test_build_yahoo_stat_id_sql_scores_idp_and_return_yards_from_yahoo_feed():
    rules = {
        "scoring_settings": {
            "st_yd": 0.1,
            "idp_tkl_solo": 1.5,
            "idp_tkl_ast": 1,
            "idp_tkl_loss": 2.5,
        }
    }
    expr = build_yahoo_stat_id_fantasy_points_sql(rules, table_alias="p")

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE player_fantasy (
            yahoo_stat_14 DOUBLE,
            yahoo_stat_38 DOUBLE,
            yahoo_stat_39 DOUBLE,
            yahoo_stat_65 DOUBLE
        )
    """)
    conn.execute("INSERT INTO player_fantasy VALUES (55, 0, 2, 0)")
    scored = conn.execute(f"SELECT {expr} AS points FROM player_fantasy p").fetchone()[0]

    assert scored == pytest.approx(7.5)


def test_build_yahoo_stat_id_sql_scores_dst_and_kicker_from_yahoo_feed():
    rules = {
        "scoring_settings": {
            "fgm_40_49": 4,
            "xpm": 1,
            "sack": 1,
            "int": 2,
            "def_td": 6,
            "pts_allow_0": 10,
            "def_2pt": 2,
        }
    }
    expr = build_yahoo_stat_id_fantasy_points_sql(rules, table_alias="p")

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE player_fantasy (
            yahoo_stat_22 DOUBLE,
            yahoo_stat_29 DOUBLE,
            yahoo_stat_32 DOUBLE,
            yahoo_stat_33 DOUBLE,
            yahoo_stat_35 DOUBLE,
            yahoo_stat_50 DOUBLE,
            yahoo_stat_82 DOUBLE
        )
    """)
    conn.execute("INSERT INTO player_fantasy VALUES (1, 3, 4, 2, 1, 1, 1)")
    scored = conn.execute(f"SELECT {expr} AS points FROM player_fantasy p").fetchone()[0]

    assert scored == pytest.approx(33)


def test_component_sql_scores_non_precomputed_yahoo_bonus_thresholds():
    rules = {
        "scoring_settings": {
            "rush_yd": 0.1,
            "bonus_rush_yd_75": 5,
            "bonus_st_yd_100": 2,
        }
    }
    sql = build_components_fantasy_points_sql(
        rules,
        include_bonus_flags=False,
        available_columns={"rushing_yards", "kickoff_return_yards", "punt_return_yards"},
    )

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE stats (
            rushing_yards DOUBLE,
            kickoff_return_yards DOUBLE,
            punt_return_yards DOUBLE
        )
    """)
    conn.execute("INSERT INTO stats VALUES (80, 75, 30)")
    scored = conn.execute(f"SELECT {sql} AS points FROM stats").fetchone()[0]

    assert scored == pytest.approx(15)


def test_component_sql_scores_fleet_discovered_high_yardage_bonus_thresholds():
    rules = {
        "scoring_settings": {
            "bonus_pass_yd_500": 7,
            "bonus_rush_yd_300": 11,
            "bonus_rec_yd_300": 13,
        }
    }
    sql = build_components_fantasy_points_sql(
        rules,
        include_bonus_flags=False,
        available_columns={"passing_yards", "rushing_yards", "receiving_yards"},
    )

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE stats (
            passing_yards DOUBLE,
            rushing_yards DOUBLE,
            receiving_yards DOUBLE
        )
    """)
    conn.execute("INSERT INTO stats VALUES (501, 300, 299)")
    scored = conn.execute(f"SELECT {sql} AS points FROM stats").fetchone()[0]

    assert scored == pytest.approx(18)


def test_component_sql_scores_odd_yahoo_modifier_bonus_thresholds():
    rules = {
        "scoring_settings": {
            "bonus_pass_yd_325": 1,
            "bonus_rush_yd_85": 3,
            "bonus_rush_yd_123": 7,
            "bonus_rec_yd_140": 4,
            "bonus_st_yd_225": 5,
            "bonus_def_st_yd_250": 6,
            "bonus_def_st_yd_275": 8,
        }
    }
    sql = build_components_fantasy_points_sql(
        rules,
        include_bonus_flags=False,
        available_columns={
            "passing_yards",
            "rushing_yards",
            "receiving_yards",
            "kickoff_return_yards",
            "punt_return_yards",
            "dst_return_yards",
        },
    )

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE stats (
            passing_yards DOUBLE,
            rushing_yards DOUBLE,
            receiving_yards DOUBLE,
            kickoff_return_yards DOUBLE,
            punt_return_yards DOUBLE,
            dst_return_yards DOUBLE
        )
    """)
    conn.execute("INSERT INTO stats VALUES (325, 123, 140, 100, 125, 275)")
    scored = conn.execute(f"SELECT {sql} AS points FROM stats").fetchone()[0]

    assert scored == pytest.approx(34)


def test_yahoo_stat_id_sql_scores_odd_modifier_bonus_thresholds():
    rules = {
        "scoring_settings": {
            "bonus_pass_yd_325": 1,
            "bonus_rush_yd_85": 3,
            "bonus_rush_yd_123": 7,
            "bonus_rec_yd_140": 4,
            "bonus_st_yd_225": 5,
            "bonus_def_st_yd_250": 6,
            "bonus_def_st_yd_275": 8,
        }
    }
    expr = build_yahoo_stat_id_fantasy_points_sql(
        rules,
        table_alias="p",
        available_columns={"yahoo_stat_4", "yahoo_stat_9", "yahoo_stat_12", "yahoo_stat_14", "yahoo_stat_48"},
    )

    conn = duckdb.connect(":memory:")
    conn.execute("""
        CREATE TABLE player_fantasy (
            yahoo_stat_4 DOUBLE,
            yahoo_stat_9 DOUBLE,
            yahoo_stat_12 DOUBLE,
            yahoo_stat_14 DOUBLE,
            yahoo_stat_48 DOUBLE
        )
    """)
    conn.execute("INSERT INTO player_fantasy VALUES (325, 123, 140, 225, 275)")
    scored = conn.execute(f"SELECT {expr} AS points FROM player_fantasy p").fetchone()[0]

    assert scored == pytest.approx(34)


def test_build_yahoo_stat_id_sql_returns_none_without_yahoo_stat_columns():
    expr = build_yahoo_stat_id_fantasy_points_sql(
        {"scoring_settings": {"pass_td": 5}},
        table_alias="p",
        available_columns={"player", "fantasy_points"},
    )

    assert expr is None


def test_build_sql_empty_rules_returns_zero():
    sql = build_components_fantasy_points_sql({})
    # Function should handle no scoring fields gracefully
    assert sql.strip() == "0.0" or sql.strip() == "(0.0)"


def test_build_sql_yahoo_fuzz_snaps():
    """Sleeper-side: 0.03999... fuzz still hits the precompute."""
    rules = {"scoring_settings": {"pass_yd": 0.03999999910593033}}
    sql = build_components_fantasy_points_sql(rules)
    assert "pts_pass_yd_p04" in sql


# === Task B.4: compute_fantasy_points_sql_from_settings_row ===

from fantasy_football_data_scripts.multi_league.transformations.player.modules.scoring_calculator import (
    compute_fantasy_points_sql_from_settings_row,
)


def test_settings_row_basic_half_ppr():
    """DDL row with scoring_* keys + non-scoring metadata produces correct SQL."""
    row = {
        "year": 2024,
        "platform": "sleeper",
        "league_key": "abc123",
        "num_teams": 12,
        "scoring_type": "half",
        "scoring_pass_td": 4.0,
        "scoring_pass_yd": 0.04,
        "scoring_pass_int": -2.0,
        "scoring_rush_yd": 0.1,
        "scoring_rush_td": 6.0,
        "scoring_rec_yd": 0.1,
        "scoring_rec_td": 6.0,
        "scoring_rec": 0.5,
        "scoring_fum_lost": -2.0,
        "roster_qb": 1,
        "roster_rb": 2,
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    # Top-mult components hit precompute path
    assert "pts_pass_yd_p04" in sql
    assert "pts_pass_td_4" in sql
    assert "pts_pass_int_n2" in sql
    assert "pts_rush_yd_p1" in sql
    assert "pts_rec_p5" in sql
    # Non-scoring metadata not included
    assert "year" not in sql
    assert "platform" not in sql
    assert "num_teams" not in sql
    assert "roster_qb" not in sql


def test_settings_row_drops_null_and_nan():
    """NULL / NaN scoring fields don't appear in the SQL (no contribution)."""
    import math

    row = {
        "scoring_pass_td": 4.0,
        "scoring_pass_yd": 0.04,
        "scoring_rec_yd": None,  # NULL — drop
        "scoring_rush_yd": math.nan,  # NaN — drop
        "scoring_pass_int": 0,  # zero — drop
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "pts_pass_yd_p04" in sql
    assert "pts_pass_td_4" in sql
    # NULL/NaN/zero scoring fields produce no terms
    assert "rec_yd" not in sql
    assert "rushing_yards" not in sql
    assert "pts_pass_int" not in sql


def test_settings_row_with_bonus_keys():
    """bonus_* keys flow through (already Sleeper-flat, no scoring_ prefix)."""
    row = {
        "scoring_pass_td": 4.0,
        "scoring_pass_yd": 0.04,
        "bonus_pass_yd_300": 3.0,
        "bonus_rec_yd_100": 1.0,
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "bonus_pass_300yd" in sql
    assert "* 3" in sql
    assert "bonus_rec_100yd" in sql


def test_settings_row_uses_canonical_ddl_reader_for_flat_scoring():
    row = {
        "scoring_pass_td": 4.0,
        "scoring_rec": 0.0,
        "scoring_variant_first_year": 2024,
        "scoring_not_a_real_key": 99,
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "pts_pass_td_4" in sql
    assert "variant_first_year" not in sql
    assert "not_a_real_key" not in sql


def test_settings_row_with_te_bonus():
    row = {
        "scoring_pass_td": 4.0,
        "scoring_pass_yd": 0.04,
        "scoring_rec": 1.0,
        "scoring_rec_yd": 0.1,
        "scoring_rec_td": 6.0,
        "scoring_bonus_rec_te": 0.5,  # Note: scoring_ prefix on this key
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    # bonus_rec_te (after strip) maps to rec_te_bonus stat_key per _SLEEPER_KEY_TO_STAT
    assert "pts_rec_te_bonus_p5" in sql


def test_settings_row_yahoo_format_fuzz_snaps():
    """Yahoo-imported rows store 0.04 as 0.03999999910593033 in DDL — should snap."""
    row = {
        "scoring_pass_yd": 0.03999999910593033,
        "scoring_pass_td": 4.0,
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "pts_pass_yd_p04" in sql  # fuzz snaps to canonical


def test_settings_row_empty_returns_zero():
    row = {"year": 2024, "platform": "sleeper", "num_teams": 12}
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert sql.strip() == "0.0"


def test_settings_row_edge_mult_falls_through_to_runtime():
    """Edge multipliers (e.g., scoring_pass_td=5) take the runtime atomic path."""
    row = {"scoring_pass_td": 5.0, "scoring_pass_yd": 0.04}
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "passing_tds" in sql
    assert "* 5" in sql


def test_settings_row_with_phase4_bracket_mults():
    """Phase 4 bracket scoring (scoring_pass_td_40p etc.) hits component precomputes."""
    row = {
        "scoring_pass_td": 4.0,
        "scoring_pass_td_40p": 2.0,
        "scoring_rec_40p": 1.0,
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "pts_pass_td_40plus_2" in sql
    assert "pts_rec_40plus_1" in sql


def test_settings_row_with_pbp_derived_bucket_rules():
    row = {
        "scoring_pass_cmp_50p": 1.0,
        "scoring_rec_0_4": 0.5,
        "scoring_rec_5_9": 0.5,
        "scoring_rec_10_19": 0.5,
        "scoring_rec_20_29": 1.0,
        "scoring_rec_30_39": 1.0,
        "scoring_st_tkl_solo": 1.0,
    }
    sql = compute_fantasy_points_sql_from_settings_row(row)
    assert "completions_50plus" in sql
    assert "receptions_0_4" in sql
    assert "receptions_5_9" in sql
    assert "receptions_10_19" in sql
    assert "receptions_20_29" in sql
    assert "receptions_30_39" in sql
    assert "special_teams_tackles_solo" in sql


# === B.5 follow-up: fix st_td/def_st_td double-count ===


def test_build_sql_aliased_keys_no_double_count():
    """st_td and def_st_td both map to the same stat_key — output should
    emit the term ONCE, not twice (was double-counting special-teams TDs)."""
    rules = {"scoring_settings": {"st_td": 6, "def_st_td": 6}}
    sql = build_components_fantasy_points_sql(rules)
    # Should appear exactly once, not twice
    assert sql.count("pts_st_td_6") == 1, f"Expected 1 occurrence, got: {sql!r}"


def test_build_sql_aliased_keys_with_other_components():
    """Aliased ST keys + other scoring don't interact."""
    rules = {
        "scoring_settings": {
            "pass_td": 4,
            "pass_yd": 0.04,
            "st_td": 6,
            "def_st_td": 6,
        }
    }
    sql = build_components_fantasy_points_sql(rules)
    assert sql.count("pts_pass_td_4") == 1
    assert sql.count("pts_pass_yd_p04") == 1
    assert sql.count("pts_st_td_6") == 1


def test_build_sql_alias_conflict_first_value_wins():
    sql = build_components_fantasy_points_sql({"scoring_settings": {"st_td": 6, "def_st_td": 8}})

    assert sql.count("pts_st_td_6") == 1
    assert "special_teams_tds, 0) * 8.0" not in sql


def test_build_sql_alias_qualifies_component_and_runtime_columns():
    sql = build_components_fantasy_points_sql(
        {"scoring_settings": {"pass_td": 5, "pass_yd": 0.04, "pass_inc": -0.25}},
        table_alias="s",
    )

    assert "COALESCE(s.pts_pass_yd_p04, 0)" in sql
    assert "COALESCE(s.passing_tds, 0) * 5.0" in sql
    assert "COALESCE(s.attempts, 0)" in sql
    assert "COALESCE(s.completions, 0)" in sql


def test_build_sql_can_exclude_import_separate_bonus_terms():
    sql = build_components_fantasy_points_sql(
        {
            "scoring_settings": {
                "rec": 1.0,
                "bonus_rec_te": 0.5,
                "bonus_rec_rb": 0.25,
                "bonus_pass_yd_300": 3.0,
            }
        },
        table_alias="s",
        position_sql="COALESCE(pb.nfl_position, p.position)",
        include_bonus_flags=False,
        include_position_bonuses=False,
    )

    assert "COALESCE(s.pts_rec_1, 0)" in sql
    assert "bonus_pass_300yd" not in sql
    assert "pts_rec_te_bonus_p5" not in sql
    assert "pb.nfl_position" not in sql


def test_build_sql_skips_missing_optional_scalar_sources():
    sql = build_components_fantasy_points_sql(
        {"scoring_settings": {"pass_cmp_50p": 1.0, "pass_td": 4}},
        available_columns={"pts_pass_td_4"},
    )

    assert "pts_pass_td_4" in sql
    assert "completions_50plus" not in sql


def test_build_sql_zeroes_missing_optional_multi_sources():
    sql = build_components_fantasy_points_sql(
        {"scoring_settings": {"fum_ret_yd": 0.1}},
        available_columns={"fumble_recovery_yards_own"},
    )

    assert "fumble_recovery_yards_own" in sql
    assert "fumble_recovery_yards_opp" not in sql
    assert " + 0" in sql or "+ 0" in sql
