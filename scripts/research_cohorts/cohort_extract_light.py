"""Light extraction: one bounded pass -> a small parquet all analysis runs off.

Sized to never spill: a per-cohort league SAMPLE (variance converges long before the
population does), hard memory + temp caps, and no DISTINCT over the full table --
duplicates collapse inside the aggregate via MAX on the flags.

The true population size is a cheap COUNT and enters only through the finite-population
correction downstream, so sampling costs nothing in the answer.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import pandas as pd

from cohort_precision_closed_form import COHORTS, ELIGIBILITY


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--sample", type=int, default=1200, help="leagues sampled per cohort")
    ap.add_argument("--memory-mb", type=int, default=1200)
    ap.add_argument("--threads", type=int, default=2)
    ap.add_argument("--temp-gb", type=int, default=2)
    ap.add_argument("--tmp", type=Path,
                    default=Path("C:/Users/joeye/AppData/Local/Temp/claude/ddbtmp"))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.tmp.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(config={"memory_limit": f"{a.memory_mb}MB", "threads": a.threads,
                                 "temp_directory": str(a.tmp)})
    con.execute("SET enable_progress_bar=false")
    con.execute("SET preserve_insertion_order=false")
    # Hard ceiling: spilling past this fails the query instead of filling the system disk.
    con.execute(f"PRAGMA max_temp_directory_size='{a.temp_gb}GB'")
    con.execute(f"ATTACH '{a.snapshot.as_posix()}' AS lake (READ_ONLY)")
    con.execute(f"ATTACH '{a.ops.as_posix()}' AS ops (READ_ONLY)")
    y = a.year

    elig = ",\n".join(f"  CASE WHEN ({e})>0 THEN '{p}' END" for p, e in ELIGIBILITY.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE league_cohort AS
    SELECT db_name, cohort, position FROM lake.public.league_settings,
      {COHORTS}, UNNEST(list_filter([\n{elig}\n], x -> x IS NOT NULL)) AS p(position)
    WHERE "year"={y} AND NOT COALESCE(sleeper_best_ball, FALSE)
    """)

    # TRUE population size per (cohort, position) -- the FPC input. Cheap: metadata only.
    pop = con.execute("""
        SELECT cohort, position, COUNT(DISTINCT db_name) AS population_leagues
        FROM league_cohort GROUP BY 1,2""").fetchdf()

    # Sample leagues per cohort. Deterministic (hash order), so re-runs are comparable.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE samp AS
    SELECT db_name, cohort FROM (
      SELECT db_name, cohort,
             ROW_NUMBER() OVER (PARTITION BY cohort ORDER BY hash(db_name)) AS rn
      FROM (SELECT DISTINCT db_name, cohort FROM league_cohort)
    ) WHERE rn <= {a.sample}
    """)
    con.execute("CREATE OR REPLACE TEMP TABLE samp_db AS SELECT DISTINCT db_name FROM samp")
    n_db = con.execute("SELECT COUNT(*) FROM samp_db").fetchone()[0]
    print(f"[{y}] sampling {n_db:,} distinct leagues", flush=True)

    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE nfl AS
    SELECT NFL_player_id, CAST("week" AS INTEGER) AS week,
           MAX(CASE WHEN UPPER(TRIM(position)) IN ('DST','D/ST','DEF') THEN 'DEF'
                    ELSE UPPER(TRIM(position)) END) AS position
    FROM ops.nfl_historical.nfl_player_stats_all
    WHERE "year"={y} AND NFL_player_id IS NOT NULL AND position IS NOT NULL
    GROUP BY 1,2
    """)

    # Restrict to the sample BEFORE any aggregation; dedup happens inside the GROUP BY
    # (MAX on flags, and starts is recomputed from distinct weeks) with no DISTINCT pass.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE cell AS
    WITH s AS (
      SELECT pf.db_name, pf.week, pf.NFL_player_id, pf.manager,
             MAX(CAST(pf.is_started AS INTEGER)) AS is_started,
             MAX(COALESCE(pf.clutch_equity,0)) AS clutch_equity,
             MAX(CAST(pf.champion AS INTEGER)) AS champion,
             MAX(CASE WHEN pf.is_playoffs=1 THEN 1 ELSE 0 END) AS po_week,
             MAX(CASE WHEN pf.final_playoff_seed IS NOT NULL
                        OR COALESCE(pf.made_playoffs,0)=1
                        OR CAST(pf.champion AS INTEGER)=1 THEN 1 ELSE 0 END) AS po_sig,
             MAX(pf.team_points) AS team_points
      FROM lake.public.player_fantasy pf
      JOIN samp_db d ON d.db_name = pf.db_name
      WHERE pf.year={y} AND pf.NFL_player_id IS NOT NULL
      GROUP BY 1,2,3,4
    )
    SELECT n.position, s.db_name, s.NFL_player_id AS player,
           SUM(s.is_started)                                     AS starts,
           MAX(CASE WHEN s.champion=1 AND s.is_started=1 THEN 1 ELSE 0 END) AS champ,
           MAX(CASE WHEN s.po_week=1  AND s.is_started=1 THEN 1 ELSE 0 END) AS po,
           MAX(s.po_sig)                                         AS po_sig,
           SUM(s.clutch_equity)                                  AS clutch
    FROM s JOIN nfl n ON n.NFL_player_id=s.NFL_player_id AND n.week=s.week
    GROUP BY 1,2,3
    """)

    wks = con.execute(f"""
        SELECT db_name, COUNT(DISTINCT week) AS wks
        FROM lake.public.player_fantasy pf JOIN samp_db d USING (db_name)
        WHERE pf.year={y} GROUP BY 1""").fetchdf()

    cells = con.execute("SELECT * FROM cell").fetchdf()
    # Carry position eligibility for the SAMPLED leagues: it is the denominator, and
    # without it a kicker gets denominated on leagues that carry no K slot.
    samp = con.execute("""
        SELECT lc.db_name, lc.cohort, lc.position
        FROM league_cohort lc JOIN samp s ON s.db_name=lc.db_name AND s.cohort=lc.cohort
    """).fetchdf()
    con.close()

    cells = cells.merge(wks, on="db_name", how="left")
    cells["year"] = y
    cells.to_parquet(a.out, index=False)
    pop.to_parquet(a.out.with_suffix(".pop.parquet"), index=False)
    samp.to_parquet(a.out.with_suffix(".samp.parquet"), index=False)
    print(f"[{y}] wrote {len(cells):,} player-league cells -> {a.out}", flush=True)


if __name__ == "__main__":
    main()
