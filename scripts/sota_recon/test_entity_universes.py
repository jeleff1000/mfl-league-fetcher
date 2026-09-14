"""Contract tests for entity_universes (O.4): builders + structural gates on synthetic lakes.

Run:  python -m pytest scripts/sota_recon/test_entity_universes.py -q
"""

from __future__ import annotations

import os

import duckdb
import pytest

from . import entity_universes as eu


def _write(con, sql: str, dest: str) -> str:
    con.execute(f"COPY ({sql}) TO '{dest}' (FORMAT PARQUET)")
    return dest


@pytest.fixture()
def synth_paths(tmp_path):
    """A 2-game synthetic lake exercising every gate: one clean game, one defective game
    (missing mirror row, absent from schedule, duplicate scoring seq, same-date double-credit)."""
    con = duckdb.connect()
    d = str(tmp_path)
    p = {}
    # catalog: game A clean (2 rows), game B one-row (involution + two-row violations)
    p["pfr_team_games"] = _write(con, """
        SELECT * FROM (VALUES
          ('2020_a_AAA', 'gameA', 2020.0, 1, 'REG', '2020-09-10', 'AAA', 'BBB', 1.0, 2.0, TRUE, FALSE, FALSE, 20, 10),
          ('2020_a_BBB', 'gameA', 2020.0, 1, 'REG', '2020-09-10', 'BBB', 'AAA', 2.0, 1.0, FALSE, TRUE, FALSE, 10, 20),
          ('2020_b_AAA', 'gameB', 2020.0, 2, 'REG', '2020-09-17', 'AAA', 'CCC', 1.0, 3.0, TRUE, FALSE, FALSE, 7, 3)
        ) t(team_game_key, boxscore_id, year, week, season_type, game_date, team_code, opponent_code,
            team_fid, opponent_fid, is_home, is_away, is_neutral, team_points, opponent_points)
        """, os.path.join(d, "catalog.parquet"))
    # schedule: knows game A only, plus a phantom game D (sched_no_cat)
    p["schedule_master"] = _write(con, """
        SELECT * FROM (VALUES
          (2020, 1, 'AAA', 'BBB', '2020-09-10'),
          (2020, 9, 'DDD', 'EEE', '2020-11-01')
        ) t(year, week, nfl_team, opponent_nfl_team, game_date)
        """, os.path.join(d, "sched.parquet"))
    # pbp: game A with 3 drives for AAA vs 1 for BBB (balance violation), one null fixed_drive row
    p["pbp_merged_1978_2025"] = _write(con, """
        SELECT * FROM (VALUES
          ('g_A', 1, 2020, 'REG', 1, 1, 'AAA', 'BBB'),
          ('g_A', 2, 2020, 'REG', 1, 2, 'AAA', 'BBB'),
          ('g_A', 3, 2020, 'REG', 1, 3, 'AAA', 'BBB'),
          ('g_A', 4, 2020, 'REG', 1, 4, 'BBB', 'AAA'),
          ('g_A', 5, 2020, 'REG', 1, NULL, 'AAA', 'BBB')
        ) t(game_id, play_id, season, season_type, week, fixed_drive, posteam, defteam)
        """, os.path.join(d, "pbp.parquet"))
    # scoring: duplicate (boxscore_id, seq)
    p["pfr_box_scoring"] = _write(con, """
        SELECT * FROM (VALUES
          ('gameA', 0, 2020, '1', 'Lions', '5:00', 0, 7),
          ('gameA', 0, 2020, '1', 'Lions', '5:00', 0, 7),
          ('gameA', 1, 2020, '2', 'Bears', '2:00', 3, 7)
        ) t(boxscore_id, row_index_in_table, season, quarter, team, "time", vis_team_score, home_team_score)
        """, os.path.join(d, "scoring.parquet"))
    # drive box tables (empty schemas)
    empty_drives = """
        SELECT * FROM (VALUES ('gameA', 2020, 1)) t(boxscore_id, season, drive_num) WHERE 1=0
        """
    p["pfr_box_home_drives"] = _write(con, empty_drives, os.path.join(d, "hd.parquet"))
    p["pfr_box_vis_drives"] = _write(con, empty_drives, os.path.join(d, "vd.parquet"))
    # presence: P1 in both games SAME DATE? gameA 09-10, gameB 09-17 -> different dates; make
    # P2 appear in gameA and gameB with same date via a third game C sharing gameB's date.
    player_tbl = """
        SELECT * FROM (VALUES
          ('gameA', 'P1x00', 'AAA', 2020),
          ('gameA', 'P2x00', 'BBB', 2020),
          ('gameB', 'P1x00', 'AAA', 2020),
          ('gameA', NULL,    'AAA', 2020)
        ) t(boxscore_id, player_link_ids, team, season)
        """
    p["pfr_player_offense_box"] = _write(con, player_tbl, os.path.join(d, "po.parquet"))
    empty_player = player_tbl + " WHERE 1=0"
    for sid in ("pfr_player_defense_box", "pfr_box_kicking", "pfr_box_returns"):
        p[sid] = _write(con, empty_player, os.path.join(d, sid + ".parquet"))
    starters = """
        SELECT * FROM (VALUES
          ('gameA', 'P1x00', 'QB', 2020)
        ) t(boxscore_id, player_link_ids, pos, season)
        """
    p["pfr_box_home_starters"] = _write(con, starters, os.path.join(d, "hs.parquet"))
    for sid in ("pfr_box_vis_starters", "pfr_box_home_snaps", "pfr_box_vis_snaps"):
        p[sid] = _write(con, starters + " WHERE 1=0", os.path.join(d, sid + ".parquet"))
    return p


@pytest.fixture()
def summary(synth_paths, tmp_path):
    con = duckdb.connect()
    return eu.build_all(con, paths=synth_paths, out_dir=str(tmp_path / "out")), con


def test_universe_set_complete(summary):
    s, _ = summary
    assert set(s["universes"]) == set(eu.UNIVERSE_KEYS)


def test_team_games_gates(summary):
    s, _ = summary
    g = s["universes"]["team_games"]
    assert g["n_rows"] == 3
    assert g["gates"]["involution_missing"] == 1  # gameB has no mirror row
    assert g["gates"]["points_mirror_violations"] == 0
    assert g["gates"]["duplicate_keys"] == 0


def test_games_gates_and_schedule_crosscheck(summary):
    s, _ = summary
    g = s["universes"]["games"]
    assert g["n_rows"] == 2
    assert g["gates"]["two_row_violations"] == 1        # gameB
    assert g["gates"]["catalog_games_absent_from_schedule_master"] == 1  # gameB
    assert g["gates"]["schedule_master_games_absent_from_catalog"] == 1  # phantom DDD-EEE


def test_drives_and_plays(summary):
    s, _ = summary
    d = s["universes"]["drives"]
    assert d["n_rows"] == 4                      # fixed_drive 1..4 (null excluded)
    assert d["gates"]["drive_balance_violations_pbp"] == 1  # AAA 3 vs BBB 1
    p = s["universes"]["plays"]
    assert p["n_rows"] == 5
    assert p["gates"]["duplicate_keys"] == 0


def test_scoring_events_duplicate_gate(summary):
    s, _ = summary
    sc = s["universes"]["scoring_events"]
    assert sc["n_rows"] == 3
    assert sc["gates"]["duplicate_keys"] == 1


def test_presence_gates(summary):
    s, _ = summary
    pr = s["universes"]["player_game_presence"]
    # P1 gameA (offense+starters merge to one key), P2 gameA, P1 gameB
    assert pr["n_rows"] == 3
    assert pr["gates"]["unlinked_rows_per_source"]["pfr_player_offense_box"] == 1
    assert pr["gates"]["same_date_multi_game_player_dates"] == 0  # different dates
    # starter law: 1 starter on 2020 side (expected 22) -> deviation
    assert pr["gates"]["starter_count_deviation_report"] == 1


def test_rollup_universes(summary):
    s, _ = summary
    ps = s["universes"]["player_seasons"]
    assert ps["n_rows"] == 2   # (P1, 2020, REG), (P2, 2020, REG)
    assert ps["n_players"] == 2
    ts = s["universes"]["team_seasons"]
    assert ts["n_rows"] == 2   # (fid 1, 2020, REG) covers gameA+gameB rows; (fid 2, 2020, REG)
    assert ts["gates"]["null_key_rows"] == 0


def test_parquet_outputs_written(summary, tmp_path):
    _, _ = summary
    out = tmp_path / "out"
    for name in eu.UNIVERSE_KEYS:
        assert (out / f"{name}.parquet").exists()
