"""
sota_recon/entity_universes.py  --  O.4: canonical expected-key ENTITY UNIVERSES (C-plane substrate)

Master plan §25.4 D2 / addendum O.4: "Year minimum/maximum is not coverage. Build canonical
expected universes for: games, team-games, drives, plays, scoring events, player-game presence,
player-season rows, team-season rows."

Eight universes, each an enumerated expected-KEY set built from the authoritative NON-SUBJECT
sources (the game catalog, PFR box tables, pbp_merged), never from v26 itself — so coverage
comparisons against the subject are non-circular. Every universe row is a key that some source
claims exists; the C-plane runner (recon_kc_planes.py) compares each source's observed key set
against these with set arithmetic (expected-observed, observed-expected), model-gated.

Universe          key                              authority
games             boxscore_id                      pfr_team_games (catalog), x-checked vs schedule_master
team_games        (boxscore_id, team_code)         pfr_team_games
drives            (game_source, game_key, drive)   pbp_merged fixed_drive 1999+ | pfr home/vis_drives 1998+
                                                   1978-97 = UNAVAILABLE_IN_CURRENT_SOURCE_UNIVERSE
plays             (game_id, play_id)               pbp_merged 1978+
scoring_events    (boxscore_id, event_seq)         pfr_box_scoring
player_game_presence (boxscore_id, pfr_id)         union of 8 appearance witnesses, per-source flags
player_seasons    (pfr_id, year, season_type)      rollup of player_game_presence x games
team_seasons      (team_fid, year, season_type)    rollup of team_games

Structural gates (findings, never fixes — counts land in the summary JSON):
  games.two_row_violations        catalog games without exactly 2 team rows (50 known, incl 1944 self-play)
  team_games.involution_missing   rows whose mirrored opponent row is absent
  team_games.points_mirror        team_points != opponent row's opponent_points
  drives.balance_violations       per-game | drives(A) - drives(B) | > 1 (pbp era, report-only)
  plays.duplicate_keys            duplicate (game_id, play_id)
  scoring_events.duplicate_keys   duplicate (boxscore_id, event_seq)
  presence.same_date_multi_game   pfr_id credited in >1 game on one calendar date (K-plane law)
  presence.unlinked_rows          appearance rows with no pfr_id (per source)
  starters.eleven_violations      (boxscore_id, side) starter count != 11 (era-scoped report, §22)

Outputs (regenerable; parquets to the lake, compact summary committed):
  {DATA_LAKE}/derived/entity_universes/{name}.parquet
  docs/entity-universes-summary.json   (counts, per-year vectors, gate results, source pins)

Run:  python -m scripts.sota_recon.entity_universes
"""

from __future__ import annotations

import json
import os

import duckdb

from .recon_common import canon_team_sql, connect, utc_stamp
from .sources import DATA_LAKE, registry

OUT_DIR = os.path.join(DATA_LAKE, "derived", "entity_universes")
SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "docs", "entity-universes-summary.json")

# Appearance witnesses for player_game_presence: source_id -> (flag_name, has_team_col, side)
PRESENCE_SOURCES = {
    "pfr_player_offense_box": ("offense_box", True, None),
    "pfr_player_defense_box": ("defense_box", True, None),
    "pfr_box_kicking": ("kicking_box", True, None),
    "pfr_box_returns": ("returns_box", True, None),
    "pfr_box_home_starters": ("starters", False, "home"),
    "pfr_box_vis_starters": ("starters", False, "vis"),
    "pfr_box_home_snaps": ("snaps", False, "home"),
    "pfr_box_vis_snaps": ("snaps", False, "vis"),
}

UNIVERSE_KEYS = {
    "games": ["boxscore_id"],
    "team_games": ["boxscore_id", "team_code"],
    "drives": ["game_source", "game_key", "drive_num"],
    "plays": ["game_id", "play_id"],
    "scoring_events": ["boxscore_id", "event_seq"],
    "player_game_presence": ["boxscore_id", "pfr_id"],
    "player_seasons": ["pfr_id", "year", "season_type"],
    "team_seasons": ["team_fid", "year", "season_type"],
}


def _paths() -> dict[str, str]:
    reg = registry(include_subject=True)
    return {sid: s.path for sid, s in reg.items()}


def _year_vector(con: duckdb.DuckDBPyConnection, rel: str, year_expr: str = "year") -> dict[str, int]:
    rows = con.execute(
        f"SELECT {year_expr} AS y, COUNT(*) FROM {rel} WHERE {year_expr} IS NOT NULL GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {str(int(y)): int(n) for y, n in rows}


def register_inputs(con: duckdb.DuckDBPyConnection, paths: dict[str, str] | None = None) -> None:
    """Register every input as a view. Tests may pass synthetic paths."""
    paths = paths or _paths()
    con.execute(f"CREATE OR REPLACE VIEW catalog AS SELECT * FROM '{paths['pfr_team_games']}'")
    con.execute(f"CREATE OR REPLACE VIEW sched AS SELECT * FROM '{paths['schedule_master']}'")
    con.execute(f"CREATE OR REPLACE VIEW pbp AS SELECT * FROM '{paths['pbp_merged_1978_2025']}'")
    con.execute(f"CREATE OR REPLACE VIEW scoring_box AS SELECT * FROM '{paths['pfr_box_scoring']}'")
    con.execute(f"CREATE OR REPLACE VIEW home_drives AS SELECT * FROM '{paths['pfr_box_home_drives']}'")
    con.execute(f"CREATE OR REPLACE VIEW vis_drives AS SELECT * FROM '{paths['pfr_box_vis_drives']}'")
    for sid in PRESENCE_SOURCES:
        con.execute(f"CREATE OR REPLACE VIEW src_{sid} AS SELECT * FROM '{paths[sid]}'")


def build_team_games(con: duckdb.DuckDBPyConnection) -> dict:
    canon_t = canon_team_sql("team_code")
    canon_o = canon_team_sql("opponent_code")
    con.execute(
        f"""
        CREATE OR REPLACE TABLE u_team_games AS
        SELECT team_game_key,
               boxscore_id,
               CAST(year AS INTEGER)                    AS year,
               CAST(week AS INTEGER)                    AS week,
               season_type,
               game_date,
               team_code,
               {canon_t}                                AS team_canon,
               opponent_code,
               {canon_o}                                AS opponent_canon,
               CAST(team_fid AS INTEGER)                AS team_fid,
               CAST(opponent_fid AS INTEGER)            AS opponent_fid,
               is_home, is_away, is_neutral,
               team_points, opponent_points
        FROM catalog
        """
    )
    involution_missing = con.execute(
        """
        SELECT COUNT(*) FROM u_team_games a
        LEFT JOIN u_team_games b
          ON a.boxscore_id = b.boxscore_id
         AND a.team_code = b.opponent_code AND a.opponent_code = b.team_code
        WHERE b.boxscore_id IS NULL
        """
    ).fetchone()[0]
    points_mirror = con.execute(
        """
        SELECT COUNT(*) FROM u_team_games a
        JOIN u_team_games b
          ON a.boxscore_id = b.boxscore_id
         AND a.team_code = b.opponent_code AND a.opponent_code = b.team_code
        WHERE a.team_points IS DISTINCT FROM b.opponent_points
        """
    ).fetchone()[0]
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT boxscore_id, team_code FROM u_team_games GROUP BY 1,2 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_team_games").fetchone()[0],
        "gates": {
            "involution_missing": int(involution_missing),
            "points_mirror_violations": int(points_mirror),
            "duplicate_keys": int(dup),
        },
        "per_year": _year_vector(con, "u_team_games"),
    }


def build_games(con: duckdb.DuckDBPyConnection) -> dict:
    con.execute(
        """
        CREATE OR REPLACE TABLE u_games AS
        SELECT boxscore_id,
               MIN(year)        AS year,
               MIN(week)        AS week,
               MIN(season_type) AS season_type,
               MIN(game_date)   AS game_date,
               COUNT(*)         AS n_team_rows,
               MIN(team_canon)  AS team_lo,
               MAX(team_canon)  AS team_hi
        FROM u_team_games
        GROUP BY boxscore_id
        """
    )
    two_row = con.execute("SELECT COUNT(*) FROM u_games WHERE n_team_rows != 2").fetchone()[0]
    # Independent-calendar cross-check vs schedule_master on (game_date, unordered canon team pair).
    canon_a = canon_team_sql("nfl_team")
    canon_b = canon_team_sql("opponent_nfl_team")
    con.execute(
        f"""
        CREATE OR REPLACE TEMP VIEW sched_games AS
        SELECT DISTINCT CAST(game_date AS VARCHAR) AS game_date,
               LEAST({canon_a}, {canon_b})    AS team_lo,
               GREATEST({canon_a}, {canon_b}) AS team_hi
        FROM sched
        """
    )
    cat_no_sched = con.execute(
        """
        SELECT COUNT(*) FROM u_games g
        LEFT JOIN sched_games s
          ON CAST(g.game_date AS VARCHAR) = s.game_date AND g.team_lo = s.team_lo AND g.team_hi = s.team_hi
        WHERE s.game_date IS NULL
        """
    ).fetchone()[0]
    sched_no_cat = con.execute(
        """
        SELECT COUNT(*) FROM sched_games s
        LEFT JOIN u_games g
          ON CAST(g.game_date AS VARCHAR) = s.game_date AND g.team_lo = s.team_lo AND g.team_hi = s.team_hi
        WHERE g.boxscore_id IS NULL
        """
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_games").fetchone()[0],
        "gates": {
            "two_row_violations": int(two_row),
            "catalog_games_absent_from_schedule_master": int(cat_no_sched),
            "schedule_master_games_absent_from_catalog": int(sched_no_cat),
        },
        "per_year": _year_vector(con, "u_games"),
    }


def build_drives(con: duckdb.DuckDBPyConnection) -> dict:
    """pbp fixed_drive 1999+ UNION pfr box drives 1998+. 1978-97 has no drive-grain source:
    UNAVAILABLE_IN_CURRENT_SOURCE_UNIVERSE (typed terminal state, recorded in summary)."""
    con.execute(
        """
        CREATE OR REPLACE TABLE u_drives AS
        SELECT 'pbp' AS game_source,
               game_id AS game_key,
               CAST(fixed_drive AS INTEGER) AS drive_num,
               CAST(season AS INTEGER)      AS year,
               season_type,
               MIN(posteam)                 AS posteam,
               COUNT(*)                     AS n_plays
        FROM pbp
        WHERE fixed_drive IS NOT NULL
        GROUP BY game_id, fixed_drive, season, season_type
        UNION ALL
        SELECT 'pfr_box' AS game_source,
               boxscore_id AS game_key,
               CAST(drive_num AS INTEGER)   AS drive_num,
               CAST(season AS INTEGER)      AS year,
               'home'                       AS season_type,  -- side marker, remapped below
               NULL                         AS posteam,
               NULL                         AS n_plays
        FROM home_drives
        WHERE drive_num IS NOT NULL
        UNION ALL
        SELECT 'pfr_box', boxscore_id, CAST(drive_num AS INTEGER), CAST(season AS INTEGER),
               'vis', NULL, NULL
        FROM vis_drives
        WHERE drive_num IS NOT NULL
        """
    )
    # side marker abuse of season_type only for pfr_box rows; rename properly
    con.execute(
        """
        CREATE OR REPLACE TABLE u_drives AS
        SELECT game_source, game_key, drive_num, year,
               CASE WHEN game_source = 'pbp' THEN season_type END AS season_type,
               CASE WHEN game_source = 'pfr_box' THEN season_type END AS box_side,
               posteam, n_plays
        FROM u_drives
        """
    )
    # PFR box drives are numbered per side, so the key is (game, side, drive_num); fold side in.
    dup = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT game_source, game_key, COALESCE(box_side, ''), drive_num
          FROM u_drives GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)
        """
    ).fetchone()[0]
    balance = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT game_key FROM (
            SELECT game_key, posteam, COUNT(*) AS c
            FROM u_drives
            WHERE game_source = 'pbp' AND posteam IS NOT NULL
            GROUP BY 1, 2)
          GROUP BY game_key
          HAVING MAX(c) - MIN(c) > 1)
        """
    ).fetchone()[0]
    null_posteam = con.execute(
        "SELECT COUNT(*) FROM u_drives WHERE game_source = 'pbp' AND posteam IS NULL"
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_drives").fetchone()[0],
        "era_floor": {"pbp": 1999, "pfr_box": 1998},
        "unavailable_1978_1997": "UNAVAILABLE_IN_CURRENT_SOURCE_UNIVERSE (pbp drive ids null 1978-98; pfr drive tables start 1998)",
        "gates": {
            "duplicate_keys": int(dup),
            "drive_balance_violations_pbp": int(balance),
            "null_posteam_drives_pbp": int(null_posteam),
        },
        "per_year": _year_vector(con, "u_drives"),
    }


def build_plays(con: duckdb.DuckDBPyConnection) -> dict:
    con.execute(
        """
        CREATE OR REPLACE TABLE u_plays AS
        SELECT game_id, play_id,
               CAST(season AS INTEGER) AS year,
               week, season_type, posteam, defteam
        FROM pbp
        """
    )
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT game_id, play_id FROM u_plays GROUP BY 1,2 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_plays").fetchone()[0],
        "gates": {"duplicate_keys": int(dup)},
        "per_year": _year_vector(con, "u_plays"),
    }


def build_scoring_events(con: duckdb.DuckDBPyConnection) -> dict:
    con.execute(
        """
        CREATE OR REPLACE TABLE u_scoring_events AS
        SELECT boxscore_id,
               CAST(row_index_in_table AS INTEGER) AS event_seq,
               CAST(season AS INTEGER)             AS year,
               quarter, team, "time",
               vis_team_score, home_team_score
        FROM scoring_box
        """
    )
    dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT boxscore_id, event_seq FROM u_scoring_events GROUP BY 1,2 HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    orphan = con.execute(
        """
        SELECT COUNT(DISTINCT s.boxscore_id) FROM u_scoring_events s
        LEFT JOIN u_games g ON s.boxscore_id = g.boxscore_id
        WHERE g.boxscore_id IS NULL
        """
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_scoring_events").fetchone()[0],
        "gates": {"duplicate_keys": int(dup), "boxscore_ids_absent_from_games": int(orphan)},
        "per_year": _year_vector(con, "u_scoring_events"),
    }


def build_player_game_presence(con: duckdb.DuckDBPyConnection) -> dict:
    selects = []
    unlinked = {}
    for sid, (flag, has_team, side) in PRESENCE_SOURCES.items():
        team_expr = "team" if has_team else "NULL"
        side_expr = f"'{side}'" if side else "NULL"
        unlinked[sid] = int(
            con.execute(
                f"SELECT COUNT(*) FROM src_{sid} WHERE player_link_ids IS NULL OR TRIM(player_link_ids) = ''"
            ).fetchone()[0]
        )
        selects.append(
            f"""
            SELECT boxscore_id,
                   TRIM(player_link_ids)  AS pfr_id,
                   '{flag}'               AS evidence,
                   {team_expr}            AS team,
                   {side_expr}            AS side,
                   CAST(season AS INTEGER) AS year
            FROM src_{sid}
            WHERE player_link_ids IS NOT NULL AND TRIM(player_link_ids) != ''
            """
        )
    union = "\nUNION ALL\n".join(selects)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE u_player_game_presence AS
        SELECT boxscore_id, pfr_id,
               MIN(year)                                              AS year,
               COUNT(DISTINCT evidence)                               AS n_evidence_kinds,
               BOOL_OR(evidence = 'offense_box')                      AS in_offense_box,
               BOOL_OR(evidence = 'defense_box')                      AS in_defense_box,
               BOOL_OR(evidence = 'kicking_box')                      AS in_kicking_box,
               BOOL_OR(evidence = 'returns_box')                      AS in_returns_box,
               BOOL_OR(evidence = 'starters')                         AS in_starters,
               BOOL_OR(evidence = 'snaps')                            AS in_snaps,
               MIN(team)                                              AS team,
               MIN(side)                                              AS side
        FROM ({union})
        GROUP BY boxscore_id, pfr_id
        """
    )
    same_date = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT p.pfr_id, g.game_date
          FROM u_player_game_presence p
          JOIN u_games g ON p.boxscore_id = g.boxscore_id
          GROUP BY 1, 2
          HAVING COUNT(DISTINCT p.boxscore_id) > 1)
        """
    ).fetchone()[0]
    # §22 starter-count law, report-only, era-scoped: PFR starter tables list both platoons,
    # so expected = 22 per side in the free-substitution era (1950+), 11 in the one-platoon era
    # (verified against the live distribution: modal 22 for 1950+, modal 11 pre-1950).
    eleven = con.execute(
        """
        SELECT COUNT(*) FROM (
          SELECT p.boxscore_id, p.side,
                 CASE WHEN MIN(g.year) >= 1950 THEN 22 ELSE 11 END AS expected
          FROM u_player_game_presence p
          JOIN u_games g ON p.boxscore_id = g.boxscore_id
          WHERE p.in_starters AND p.side IS NOT NULL
          GROUP BY 1, 2
          HAVING COUNT(*) != expected)
        """
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_player_game_presence").fetchone()[0],
        "gates": {
            "same_date_multi_game_player_dates": int(same_date),
            "starter_count_deviation_report": int(eleven),
            "unlinked_rows_per_source": unlinked,
        },
        "per_year": _year_vector(con, "u_player_game_presence"),
    }


def build_player_seasons(con: duckdb.DuckDBPyConnection) -> dict:
    con.execute(
        """
        CREATE OR REPLACE TABLE u_player_seasons AS
        SELECT p.pfr_id,
               g.year,
               g.season_type,
               COUNT(DISTINCT p.boxscore_id) AS n_games
        FROM u_player_game_presence p
        JOIN u_games g ON p.boxscore_id = g.boxscore_id
        GROUP BY 1, 2, 3
        """
    )
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_player_seasons").fetchone()[0],
        "n_players": con.execute("SELECT COUNT(DISTINCT pfr_id) FROM u_player_seasons").fetchone()[0],
        "per_year": _year_vector(con, "u_player_seasons"),
    }


def build_team_seasons(con: duckdb.DuckDBPyConnection) -> dict:
    con.execute(
        """
        CREATE OR REPLACE TABLE u_team_seasons AS
        SELECT team_fid,
               MIN(team_canon) AS team_canon,
               year,
               season_type,
               COUNT(*) AS n_games
        FROM u_team_games
        GROUP BY team_fid, year, season_type
        """
    )
    # League-structure vector (§22): teams + game-count spread per regular season year.
    league = con.execute(
        """
        SELECT year, COUNT(*) AS n_teams, MIN(n_games) AS min_g, MAX(n_games) AS max_g
        FROM u_team_seasons WHERE season_type = 'REG' AND year IS NOT NULL
        GROUP BY year ORDER BY year
        """
    ).fetchall()
    null_keys = con.execute(
        "SELECT COUNT(*) FROM u_team_seasons WHERE year IS NULL OR team_fid IS NULL"
    ).fetchone()[0]
    return {
        "n_rows": con.execute("SELECT COUNT(*) FROM u_team_seasons").fetchone()[0],
        "gates": {"null_key_rows": int(null_keys)},
        "league_structure": {str(int(y)): {"teams": int(t), "games_min": int(a), "games_max": int(b)} for y, t, a, b in league},
        "per_year": _year_vector(con, "u_team_seasons"),
    }


BUILDERS = {
    "team_games": build_team_games,   # first: games derives from it
    "games": build_games,
    "drives": build_drives,
    "plays": build_plays,
    "scoring_events": build_scoring_events,
    "player_game_presence": build_player_game_presence,
    "player_seasons": build_player_seasons,
    "team_seasons": build_team_seasons,
}


def build_all(con: duckdb.DuckDBPyConnection | None = None,
              paths: dict[str, str] | None = None,
              out_dir: str | None = None,
              write_parquet: bool = True) -> dict:
    con = con or connect()
    register_inputs(con, paths)
    summary: dict = {"generated_utc": utc_stamp(), "universes": {}}
    for name, fn in BUILDERS.items():
        summary["universes"][name] = fn(con)
        summary["universes"][name]["key"] = UNIVERSE_KEYS[name]
        if write_parquet:
            od = out_dir or OUT_DIR
            os.makedirs(od, exist_ok=True)
            dest = os.path.join(od, f"{name}.parquet").replace("'", "''")
            con.execute(f"COPY u_{name} TO '{dest}' (FORMAT PARQUET)")
    return summary


def main() -> int:
    con = connect()
    summary = build_all(con)
    reg = registry(include_subject=True)
    summary["source_pins"] = {
        sid: reg[sid].path
        for sid in sorted({"pfr_team_games", "schedule_master", "pbp_merged_1978_2025",
                           "pfr_box_scoring", "pfr_box_home_drives", "pfr_box_vis_drives",
                           *PRESENCE_SOURCES})
    }
    out = os.path.abspath(SUMMARY_PATH)
    with open(out, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, default=str)
    for name, u in summary["universes"].items():
        gates = u.get("gates", {})
        print(f"{name:22s} rows={u['n_rows']:>9,}  gates={ {k: v for k, v in gates.items() if not isinstance(v, dict)} }")
    print(f"summary -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
