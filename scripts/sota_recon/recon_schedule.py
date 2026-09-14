"""
sota_recon/recon_schedule.py  --  LANE: schedule-authority anchor

Validates v26 against an INDEPENDENT game calendar (_master_schedule_1920_2025).
Because the schedule is built from a different source than the player stats, this
breaks the circularity trap: existence, date, week, opponent and score are checked
against an outside authority rather than against the subject's own keys.

It also surfaces game_date and home_away as BACKFILL CANDIDATES, since v26 currently
carries neither column (727 cols, but no game_date / is_home).

Outputs:
  schedule_match_summary.csv      - match rate raw vs team-code-normalized, by era
  schedule_def_no_game.csv        - v26 DEF team-games with NO schedule game (phantom suspects)
  schedule_game_no_def.csv        - schedule games with NO v26 DEF row (missing-game suspects)
  schedule_score_mismatch.csv     - DEF score != schedule score (after match)
  schedule_calendar_violations.csv- Jan/Feb game tagged to wrong season year (playoff boundary)
  schedule_backfill_game_date.csv - game_date + home_away to backfill onto v26 player_weeks
  manifest.json
"""

from __future__ import annotations

import os

from .asymmetry_registry import tol
from .recon_common import canon_team_sql, connect, dump_csv, era_of, lane_dir, source_pin, write_manifest
from .sources import registry

LANE = "schedule_anchor"


def run(run_dir: str) -> dict:
    reg = registry()
    v26 = reg["v26_release"].path
    sched = reg["schedule_master"].path
    out = lane_dir(run_dir, LANE)
    con = connect()

    ct = canon_team_sql  # shorthand (string fallback for raw-match comparison only)

    # Join key is the YEAR-AWARE franchise number, not the team code string. A static
    # code map collides relocated franchises (e.g. STL means Cardinals pre-1988 but the
    # map would fold it into RAM), producing false mismatches. Franchise number has
    # guaranteed continuity across relocations/renames (BAL Colts=26 vs BAL Ravens=21).
    con.execute(f"""
        CREATE TEMP VIEW def_games AS
        SELECT DISTINCT
            year, CAST(week AS INTEGER) AS week,
            nfl_team, opponent_nfl_team,
            nfl_franchise_number AS team_c,
            opponent_nfl_franchise_number AS opp_c,
            pts_def_team_pts AS def_pts,
            points_allowed AS def_allowed,
            {era_of('year')} AS era
        FROM '{v26}'
        WHERE position = 'DEF'
    """)
    # schedule games, keyed on franchise number (one row per team per game already)
    con.execute(f"""
        CREATE TEMP VIEW sched AS
        SELECT
            year, week, nfl_team, opponent_nfl_team,
            CAST(franchise_id AS INTEGER) AS team_c,
            CAST(opponent_franchise_id AS INTEGER) AS opp_c,
            team_pts, opp_pts, game_date, season_phase, home_away
        FROM '{sched}'
    """)

    def_n = con.execute("SELECT COUNT(*) FROM def_games").fetchone()[0]
    sched_n = con.execute("SELECT COUNT(*) FROM sched").fetchone()[0]

    # --- match rate: raw exact vs normalized -----------------------------------------
    raw_match = con.execute(f"""
        SELECT COUNT(*) FROM def_games d
        JOIN sched s ON d.year=s.year AND d.week=s.week
                    AND d.nfl_team=s.nfl_team AND d.opponent_nfl_team=s.opponent_nfl_team
    """).fetchone()[0]
    norm_match = con.execute(f"""
        SELECT COUNT(*) FROM def_games d
        JOIN sched s ON d.year=s.year AND d.week=s.week
                    AND d.team_c=s.team_c AND d.opp_c=s.opp_c
    """).fetchone()[0]

    dump_csv(con, f"""
        SELECT era,
               COUNT(*) AS def_games,
               COUNT(*) FILTER (WHERE m.match_raw) AS matched_raw,
               COUNT(*) FILTER (WHERE m.match_norm) AS matched_norm
        FROM (
            SELECT d.*,
                   EXISTS(SELECT 1 FROM sched s WHERE d.year=s.year AND d.week=s.week
                          AND d.nfl_team=s.nfl_team AND d.opponent_nfl_team=s.opponent_nfl_team) AS match_raw,
                   EXISTS(SELECT 1 FROM sched s WHERE d.year=s.year AND d.week=s.week
                          AND d.team_c=s.team_c AND d.opp_c=s.opp_c) AS match_norm
            FROM def_games d
        ) m
        GROUP BY era ORDER BY era
    """, os.path.join(out, "schedule_match_summary.csv"))

    # --- DEF team-games with NO schedule game (after normalization) ------------------
    def_no_game_n = dump_csv(con, f"""
        SELECT d.year, d.week, d.nfl_team, d.opponent_nfl_team, d.team_c, d.opp_c, d.era
        FROM def_games d
        LEFT JOIN sched s ON d.year=s.year AND d.week=s.week AND d.team_c=s.team_c AND d.opp_c=s.opp_c
        WHERE s.year IS NULL
        ORDER BY d.year, d.week
    """, os.path.join(out, "schedule_def_no_game.csv"))

    # --- schedule games with NO v26 DEF row (missing-game suspects) ------------------
    game_no_def_n = dump_csv(con, f"""
        SELECT s.year, s.week, s.nfl_team, s.opponent_nfl_team, s.team_c, s.opp_c, s.season_phase,
               {era_of('s.year')} AS era
        FROM sched s
        LEFT JOIN def_games d ON d.year=s.year AND d.week=s.week AND d.team_c=s.team_c AND d.opp_c=s.opp_c
        WHERE d.year IS NULL
        ORDER BY s.year, s.week
    """, os.path.join(out, "schedule_game_no_def.csv"))

    # --- score mismatch vs independent schedule -------------------------------------
    st = tol("schedule_score")
    score_mm_n = dump_csv(con, f"""
        SELECT d.year, d.week, d.nfl_team, d.opponent_nfl_team,
               d.def_pts, s.team_pts AS sched_team_pts,
               d.def_allowed, s.opp_pts AS sched_opp_pts
        FROM def_games d
        JOIN sched s ON d.year=s.year AND d.week=s.week AND d.team_c=s.team_c AND d.opp_c=s.opp_c
        WHERE d.def_pts IS NOT NULL AND s.team_pts IS NOT NULL
          AND ( ABS(d.def_pts - s.team_pts) > {st}
             OR ABS(COALESCE(d.def_allowed,-1) - COALESCE(s.opp_pts,-1)) > {st} )
        ORDER BY ABS(d.def_pts - s.team_pts) DESC
    """, os.path.join(out, "schedule_score_mismatch.csv"))

    # --- calendar / playoff year-boundary violations --------------------------------
    # A game played in Jan/Feb belongs to the PRIOR season year. If the schedule's own
    # year disagrees with (calendar_year adjusted for Jan/Feb), flag it.
    calendar_n = dump_csv(con, f"""
        SELECT year, week, nfl_team, opponent_nfl_team, season_phase,
               game_date,
               EXTRACT(year FROM game_date) AS cal_year,
               EXTRACT(month FROM game_date) AS cal_month
        FROM sched
        WHERE game_date IS NOT NULL
          AND EXTRACT(month FROM game_date) IN (1,2)
          AND year <> EXTRACT(year FROM game_date) - 1
        ORDER BY year, week
    """, os.path.join(out, "schedule_calendar_violations.csv"))

    # --- backfill candidates: game_date + home_away onto v26 player_weeks -----------
    # Maps every v26 player_week to its schedule game_date / home_away via the team-game.
    backfill_n = dump_csv(con, f"""
        WITH pw AS (
            SELECT player_week, year, CAST(week AS INTEGER) AS week,
                   nfl_franchise_number AS team_c, opponent_nfl_franchise_number AS opp_c
            FROM '{v26}'
        )
        SELECT pw.player_week, s.game_date, s.home_away
        FROM pw JOIN sched s
          ON pw.year=s.year AND pw.week=s.week AND pw.team_c=s.team_c AND pw.opp_c=s.opp_c
        WHERE s.game_date IS NOT NULL
    """, os.path.join(out, "schedule_backfill_game_date.csv"))

    manifest = {
        "lane": LANE,
        "status": "review" if (score_mm_n or calendar_n or def_no_game_n) else "pass",
        "counts": {
            "v26_def_games": int(def_n),
            "schedule_games": int(sched_n),
            "matched_raw": int(raw_match),
            "matched_normalized": int(norm_match),
            "match_pct_raw": round(100.0*raw_match/def_n, 2) if def_n else None,
            "match_pct_normalized": round(100.0*norm_match/def_n, 2) if def_n else None,
            "def_games_no_schedule": int(def_no_game_n),
            "schedule_games_no_def": int(game_no_def_n),
            "score_mismatches": int(score_mm_n),
            "calendar_year_violations": int(calendar_n),
            "backfill_game_date_rows": int(backfill_n),
        },
        "artifacts": {
            "match_summary": os.path.join(out, "schedule_match_summary.csv"),
            "def_no_game": os.path.join(out, "schedule_def_no_game.csv"),
            "game_no_def": os.path.join(out, "schedule_game_no_def.csv"),
            "score_mismatch": os.path.join(out, "schedule_score_mismatch.csv"),
            "calendar_violations": os.path.join(out, "schedule_calendar_violations.csv"),
            "backfill_game_date": os.path.join(out, "schedule_backfill_game_date.csv"),
        },
        "notes": [
            "Independent calendar authority; matched on normalized franchise codes.",
            "schedule_game_no_def in modern era => candidate MISSING game in v26.",
            "v26 lacks game_date/is_home; backfill_game_date is the candidate to add them.",
            "calendar_year_violations catch playoff games tagged to the wrong season year.",
        ],
    }
    manifest.update(source_pin())
    write_manifest(os.path.join(out, "manifest.json"), manifest)
    con.close()
    return manifest


if __name__ == "__main__":
    from .recon_common import new_run_dir
    import json
    print(json.dumps(run(new_run_dir())["counts"], indent=2))
