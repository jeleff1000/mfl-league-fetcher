"""pilot_report.py -- conformance + cohort-delta report for the extra-platform pilots.

Reads the pilot snapshots (fleaflicker + mfl), prints per-league contract health and
what the pilot adds vs the main corpus lake per (teams, ppr, td, year) cohort cell.
Read-only everywhere; the main lake is never written.

    py -3 scripts/fleaflicker_corpus/pilot_report.py
"""
from __future__ import annotations

from pathlib import Path

import duckdb

CORPUS = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
PILOTS = [CORPUS / "corpus_pilot_extraplatform.duckdb", CORPUS / "corpus_pilot_mfl.duckdb"]
MAIN = CORPUS / "corpus_snapshot.duckdb"

COHORT_KEY = """
  num_teams,
  CASE WHEN scoring_rec >= 0.75 THEN 'ppr' WHEN scoring_rec >= 0.25 THEN 'half'
       WHEN scoring_rec IS NULL THEN NULL ELSE 'std' END AS ppr,
  CASE WHEN scoring_pass_td >= 5.5 THEN '6pt' WHEN scoring_pass_td IS NULL THEN NULL
       ELSE '4pt' END AS td,
  year
"""


def main() -> None:
    con = duckdb.connect()
    pilot_views = []
    for i, path in enumerate(p for p in PILOTS if p.exists()):
        con.execute(f"ATTACH '{path.as_posix()}' AS p{i} (READ_ONLY)")
        pilot_views.append(f"SELECT * FROM p{i}.public.league_settings")
    if not pilot_views:
        raise SystemExit("no pilot snapshots found")
    con.execute(f"CREATE VIEW pilot_ls AS {' UNION ALL BY NAME '.join(pilot_views)}")

    print("=== per-league contract health ===")
    pf_views = []
    for i, path in enumerate(p for p in PILOTS if p.exists()):
        pf_views.append(f"SELECT * FROM p{i}.public.player_fantasy")
    con.execute(f"CREATE VIEW pilot_pf AS {' UNION ALL BY NAME '.join(pf_views)}")
    print(con.execute("""
        SELECT ls.platform, ls.db_name, COUNT(DISTINCT ls.year) AS yrs,
               MIN(ls.year) AS y0, MAX(ls.year) AS y1,
               ANY_VALUE(ls.num_teams) AS teams,
               ROUND(ANY_VALUE(ls.scoring_rec), 2) AS rec,
               ANY_VALUE(ls.scoring_pass_td) AS ptd,
               ANY_VALUE(ls.is_dynasty) AS dyn,
               ANY_VALUE(ls.playoff_start_week) AS po_wk
        FROM pilot_ls ls GROUP BY 1, 2 ORDER BY 1, 2
    """).fetchdf().to_string(index=False))

    print("\n=== player-week contract population (per platform) ===")
    print(con.execute("""
        SELECT ls.platform,
               COUNT(*) AS pw_rows,
               ROUND(100.0 * COUNT(pf.NFL_player_id) / COUNT(*), 1) AS pct_nfl_id,
               ROUND(100.0 * SUM(pf.is_started) / COUNT(*), 1) AS pct_started,
               ROUND(100.0 * COUNT(pf.fantasy_points) / COUNT(*), 1) AS pct_points,
               ROUND(100.0 * COUNT(pf.win) / COUNT(*), 1) AS pct_win,
               SUM(pf.champion) AS champ_rows,
               ROUND(100.0 * COUNT(pf.manager_lamar) / COUNT(*), 1) AS pct_lamar,
               ROUND(100.0 * COUNT(pf.clutch_equity) / COUNT(*), 1) AS pct_clutch
        FROM pilot_pf pf JOIN pilot_ls ls ON ls.db_name = pf.db_name AND ls.year = pf.year
        GROUP BY 1
    """).fetchdf().to_string(index=False))

    print("\n=== cohort cells the pilot adds (vs main lake, level-4 key) ===")
    if MAIN.exists():
        con.execute(f"ATTACH '{MAIN.as_posix()}' AS main (READ_ONLY)")
        print(con.execute(f"""
            WITH pilot_cells AS (
                SELECT {COHORT_KEY}, COUNT(DISTINCT db_name) AS pilot_leagues
                FROM pilot_ls GROUP BY 1, 2, 3, 4
            ),
            main_cells AS (
                SELECT {COHORT_KEY}, COUNT(DISTINCT db_name) AS main_leagues
                FROM main.public.league_settings GROUP BY 1, 2, 3, 4
            )
            SELECT p.num_teams AS teams, p.ppr, p.td, p.year,
                   p.pilot_leagues, COALESCE(m.main_leagues, 0) AS main_leagues,
                   CASE WHEN COALESCE(m.main_leagues, 0) = 0 THEN 'NEW CELL' ELSE '' END AS note
            FROM pilot_cells p
            LEFT JOIN main_cells m USING (num_teams, ppr, td, year)
            ORDER BY p.year, p.num_teams, p.ppr, p.td
        """).fetchdf().to_string(index=False))
    else:
        print("(main corpus snapshot not found; skipped)")

    print("\n=== 2015-2016 league-years added (Sleeper can never fill these) ===")
    print(con.execute("""
        SELECT platform, year, COUNT(DISTINCT db_name) AS leagues
        FROM pilot_ls WHERE year IN (2015, 2016) GROUP BY 1, 2 ORDER BY 2, 1
    """).fetchdf().to_string(index=False))
    con.close()


if __name__ == "__main__":
    main()
