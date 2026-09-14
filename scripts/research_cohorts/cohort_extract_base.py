"""THE base extract: every input for all 13 stats, on the 7-dimension cohort, one year a pass.

Everything downstream reads this parquet instead of rescanning 269M rows, which is what made
each earlier iteration cost 20+ minutes and six OOMs.

Honours the runbook's performance rules:
  P1 integer cohort ids, never a string in a hot GROUP BY
  P2 no COUNT(DISTINCT <string>) per player-league -- one row per (league, year, week,
     player), so COUNT(*) is equivalent and free
  P3 season length from a small side aggregate
  P5 one year per pass
  P6 bounded temp, so an overrun fails the query and not the host

And the definitional rules:
  R9  absence is the zero; roster% denominates on the ELIGIBLE LIVE league set, carried here
      as elig_leagues rather than inferred from rows present
  R10 teams is the position SLOT market (position_slots_contract), not num_teams
  R11 inactive = team PLAYED and player did not. A BYE is neither active nor inactive
  R11b healthy start% = starts / ACTIVE weeks (a different denominator from start%)
  R7  best ball and dynasty are DIMENSIONS here, not filters
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from cohort_format_sql import cohort_league_settings_sql


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--games", type=Path,
                    default=Path("D:/league-history-data/nfl/derived/entity_universes/team_games.parquet"))
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--memory-mb", type=int, default=1500)
    ap.add_argument("--temp-gb", type=int, default=6)
    ap.add_argument("--tmp", type=Path,
                    default=Path("C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp"))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    y = a.year
    # Per-run temp dir. Concurrent DuckDB processes sharing one temp directory delete each
    # other's spill files ("Failed to delete file ... cannot find the file specified").
    a.tmp = a.tmp / f"run_{y}_{os.getpid()}"
    a.tmp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(config={"memory_limit": f"{a.memory_mb}MB", "threads": 2,
                                 "temp_directory": str(a.tmp)})
    for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
              f"PRAGMA max_temp_directory_size='{a.temp_gb}GB'"):
        con.execute(s)
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{a.ops.as_posix()}' AS ops (READ_ONLY)")

    # ---- cohort: 7 dimensions, as a dense integer id (P1) ----
    con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                cohort_league_settings_sql(position_slots=True).replace("public.", "lake.public."))
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lgc AS
    SELECT f.db_name, p.pos,
           concat_ws('|', CASE p.pos WHEN 'QB' THEN f.teams_QB WHEN 'RB' THEN f.teams_RB
                                     WHEN 'WR' THEN f.teams_WR WHEN 'TE' THEN f.teams_TE
                                     ELSE f.teams END,
                     f.roster, f.ppr, f.td, f.bracket, f.lineup_mode, f.league_type) AS cohort
    FROM fmt f CROSS JOIN (SELECT 'QB' pos UNION ALL SELECT 'RB' UNION ALL SELECT 'WR'
         UNION ALL SELECT 'TE' UNION ALL SELECT 'K' UNION ALL SELECT 'DEF'
         UNION ALL SELECT 'DL' UNION ALL SELECT 'LB' UNION ALL SELECT 'DB') p
    WHERE f.year={y} AND f.roster IS NOT NULL AND f.ppr IS NOT NULL AND f.td IS NOT NULL
      AND f.bracket IS NOT NULL
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE cdim AS SELECT cohort, "
                "ROW_NUMBER() OVER (ORDER BY cohort) AS cid FROM (SELECT DISTINCT cohort FROM lgc)")
    con.execute("CREATE OR REPLACE TEMP TABLE lgcid AS SELECT l.db_name, l.pos, c.cid "
                "FROM lgc l JOIN cdim c USING (cohort)")
    # R9: the eligible-league denominator, per (cohort, position)
    con.execute("CREATE OR REPLACE TEMP TABLE elig AS "
                "SELECT cid, pos, COUNT(*) AS elig_leagues FROM lgcid GROUP BY 1,2")

    # ---- NFL side: position, snaps, and the BYE discriminator (R11) ----
    # Team schedule keyed on team_code, which matches the supertable's nfl_team abbreviation.
    # (team_canon is PFR-coded -- CLT for Indianapolis -- and would not join.)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE played AS
    SELECT DISTINCT team_code AS tm, CAST(week AS INTEGER) AS wk
    FROM read_parquet('{a.games.as_posix()}') WHERE year={y}
    """)
    # Per (player, week): did he appear, and for which team.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE nflwk AS
    SELECT NFL_player_id AS pid, CAST("week" AS INTEGER) AS wk, MAX(nfl_team) AS tm,
           MAX(CASE WHEN UPPER(TRIM(position)) IN ('DST','D/ST','DEF') THEN 'DEF'
                    ELSE UPPER(TRIM(position)) END) AS pos,
           MAX(CASE WHEN COALESCE(offense_snaps,0)+COALESCE(defense_snaps,0)
                         +COALESCE(special_teams_snaps,0) > 0 THEN 1 ELSE 0 END) AS snapped
    FROM ops.nfl_historical.nfl_player_stats_all
    WHERE "year"={y} AND NFL_player_id IS NOT NULL AND position IS NOT NULL
      AND position NOT LIKE '%,%'
    GROUP BY 1,2
    """)
    # Position and team are player-SEASON facts, so a week he never played still resolves.
    # Joining position per WEEK silently dropped every non-played week, which is exactly the
    # week an inactive is -- so inactive came back as 0.00 for every offensive position.
    con.execute("""
    CREATE OR REPLACE TEMP TABLE nfl AS
    SELECT pid, MAX(pos) AS pos, MAX(tm) AS tm FROM nflwk GROUP BY 1
    """)
    # R11: active = his team PLAYED and he appeared. inactive = team played, he did not.
    # A bye is neither. DST has no snap counts of its own, so a team defence is active
    # whenever its team played.
    con.execute("""
    CREATE OR REPLACE TEMP TABLE act AS
    SELECT n.pid, p.wk,
           CASE WHEN n.pos='DEF' THEN 1
                ELSE COALESCE(w.snapped, 0) END AS is_active
    FROM nfl n
    JOIN played p ON p.tm = n.tm
    LEFT JOIN nflwk w ON w.pid=n.pid AND w.wk=p.wk
    """)
    # R12: LAMAR is a supertable LOOKUP, already cohort-specific. Pick the column the
    # league's own (teams, roster, ppr, td) selects. Nothing to derive and nothing to
    # measure -- which also fixes its pooling rules a priori: it can never pool across
    # those four (a different level IS a different column) and always pools across
    # bracket/lineup_mode/league_type (they do not appear in the column name at all).
    lam_cases = []
    for t in ("10t", "12t"):
        for r in ("flx", "sflx", "idp"):
            for pp in ("std", "half", "ppr"):
                for td in ("4pt", "6pt"):
                    lam_cases.append(
                        f"WHEN f.teams_lam='{t}' AND f.roster='{r}' AND f.ppr='{pp}' "
                        f"AND f.td='{td}' THEN s.lamar_{t}_{r}_{pp}_{td}")
    LAM = "CASE " + " ".join(lam_cases) + " ELSE NULL END"

    # P3: season length once, per league
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lamdim AS
    SELECT db_name,
           CASE WHEN teams_RB IS NULL OR teams_RB='ALL' THEN teams ELSE teams_RB END AS teams_lam,
           roster, ppr, td
    FROM fmt WHERE year={y}
    """)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lamar AS
    SELECT pf.db_name, pf.NFL_player_id AS pid, SUM({LAM}) AS lamar
    FROM lake.public.player_fantasy pf
    JOIN lamdim f ON f.db_name=pf.db_name
    JOIN ops.nfl_historical.nfl_player_stats_all s
      ON s.NFL_player_id=pf.NFL_player_id AND s."year"=pf.year AND s."week"=pf.week
    WHERE pf.year={y} AND pf.NFL_player_id IS NOT NULL
    GROUP BY 1,2
    """)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE lw AS
    SELECT db_name, COUNT(DISTINCT week) AS wks,
           COUNT(DISTINCT CASE WHEN is_playoffs=1 THEN week END) AS po_wks
    FROM lake.public.player_fantasy WHERE year={y} GROUP BY 1
    """)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE base AS
    SELECT n.pos AS position, g.cid, pf.db_name, pf.NFL_player_id AS player, {y} AS year,
           MAX(w.wks) AS wks, MAX(w.po_wks) AS po_wks,
           COUNT(*)                                            AS rostered_wks,
           SUM(CAST(pf.is_started AS INTEGER))                 AS starts,
           SUM(pf.fantasy_points)                              AS total_pts,
           SUM(CASE WHEN pf.is_started=1 THEN pf.fantasy_points END) AS pts_started,
           SUM(COALESCE(ac.is_active,0))                       AS active_wks,
           SUM(CASE WHEN ac.wk IS NOT NULL AND COALESCE(ac.is_active,0)=0
                    THEN 1 ELSE 0 END)                         AS inactive_wks,
           SUM(CASE WHEN COALESCE(ac.is_active,0)=1
                    THEN CAST(pf.is_started AS INTEGER) END)   AS healthy_starts,
           SUM(CASE WHEN pf.is_started=1 THEN TRY_CAST(pf.win AS DOUBLE) END) AS wins,
           SUM(CASE WHEN pf.is_started=1 THEN 1 ELSE 0 END)    AS win_den,
           SUM(pf.clutch_equity)                               AS clutch,
           SUM(CASE WHEN pf.is_playoffs=1 THEN pf.clutch_equity ELSE 0 END) AS clutch_po,
           SUM(CASE WHEN pf.is_playoffs=1 AND pf.is_started=1
                    THEN 1 ELSE 0 END)                         AS po_starts,
           MAX(CASE WHEN pf.champion=1 AND pf.is_started=1 THEN 1 ELSE 0 END) AS champ,
           MAX(CASE WHEN pf.final_playoff_seed IS NOT NULL OR COALESCE(pf.made_playoffs,0)=1
                      OR CAST(pf.champion AS INTEGER)=1 THEN 1 ELSE 0 END) AS po_sig
    FROM lake.public.player_fantasy pf
    JOIN nfl n ON n.pid=pf.NFL_player_id
    LEFT JOIN act ac ON ac.pid=pf.NFL_player_id AND ac.wk=pf.week
    JOIN lgcid g ON g.db_name=pf.db_name AND g.pos=n.pos
    JOIN lw w ON w.db_name=pf.db_name
    WHERE pf.year={y} AND pf.NFL_player_id IS NOT NULL
    GROUP BY 1,2,3,4
    """)
    con.execute("""
    CREATE OR REPLACE TEMP TABLE final AS
    SELECT b.*, e.elig_leagues, l.lamar
    FROM base b JOIN elig e ON e.cid=b.cid AND e.pos=b.position
    LEFT JOIN lamar l ON l.db_name=b.db_name AND l.pid=b.player
    """)
    n = con.execute("SELECT COUNT(*) FROM final").fetchone()[0]
    con.execute(f"COPY final TO '{a.out.as_posix()}' (FORMAT PARQUET)")
    con.execute(f"COPY cdim TO '{str(a.out).replace('.parquet', '.cdim.parquet')}' (FORMAT PARQUET)")
    con.close()
    shutil.rmtree(a.tmp, ignore_errors=True)
    print(f"[{y}] {n:,} player-league rows -> {a.out}", flush=True)


if __name__ == "__main__":
    main()
