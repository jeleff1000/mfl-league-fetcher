# -*- coding: utf-8 -*-
"""residual_probe.py -- feasibility probe for the one-unknown RESIDUAL_SOLVER.

For a stat with a PFR season witness: classify every witness-joined player-season by
whether our weekly rows uniquely determine the missing data.

  n == g, sums match      -> validated
  n == g, sums differ     -> value-disagreement queue
  n == g-1                -> ONE UNKNOWN: missing week's value = season - SUM(weeks), unique
  n <  g-1                -> under-determined: store constrained residual, never allocate
  n >  g                  -> overcount anomaly queue (dup rows / witness g wrong / POST leak)

Production rules (see runbook §8b): "known" must mean cell IS NOT NULL (coverage mask, not
COALESCE-0); the residual VALUE is unique but weekly PLACEMENT needs appearance evidence
(starters/snaps/NC logs/newspaper lineups) unless the residual is 0.

    python -m scripts.sota_recon.witness_audit_v2.residual_probe
"""
import glob
from pathlib import Path

import duckdb

SUP = sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
             key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
T1 = "D:/league-history-data/nfl/raw/pfr/players/tables/rushing_and_receiving/_combined.parquet"
T2 = "D:/league-history-data/nfl/raw/pfr/players/tables/receiving_and_rushing/_combined.parquet"


def run(stat_witness="rush_yds", stat_super="rushing_yards", y_lo=1932, y_hi=1977):
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'")
    q = con.execute(f"""
    WITH w AS (
      SELECT pfr_id, TRY_CAST(year_id AS INT) y,
             MAX(TRY_CAST(games AS INT)) g, MAX(TRY_CAST({stat_witness} AS DOUBLE)) sy
      FROM (SELECT pfr_id, year_id, games, {stat_witness} FROM read_parquet('{T1}')
            UNION ALL SELECT pfr_id, year_id, games, {stat_witness} FROM read_parquet('{T2}'))
      WHERE regexp_matches(CAST(year_id AS VARCHAR),'^[0-9]{{4}}$') GROUP BY 1,2),
    b AS (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL),
    o AS (SELECT NFL_player_id id, CAST(year AS INT) y, COUNT(*) n,
                 SUM(COALESCE({stat_super},0)) s
          FROM read_parquet('{SUP}') WHERE season_type='REG' GROUP BY 1,2)
    SELECT CASE WHEN o.n=w.g AND ABS(o.s-COALESCE(w.sy,0))<=1 THEN '1_validated'
                WHEN o.n=w.g THEN '2_complete_but_mismatch'
                WHEN o.n=w.g-1 THEN '3_one_unknown_solvable'
                WHEN o.n<w.g-1 THEN '4_under_determined'
                ELSE '5_overcount_anomaly' END cls,
           COUNT(*) ps,
           COUNT(*) FILTER (WHERE o.n=w.g-1 AND ABS(COALESCE(w.sy,0)-o.s)>0.5) residual_nonzero
    FROM w JOIN b USING(pfr_id) JOIN o ON o.id=b.NFL_player_id AND o.y=w.y
    WHERE w.y BETWEEN {y_lo} AND {y_hi} AND w.g IS NOT NULL
    GROUP BY 1 ORDER BY 1""").fetchall()
    print(f"{stat_super} REG {y_lo}-{y_hi}: {sum(r[1] for r in q):,} witness-joined player-seasons")
    for r in q:
        extra = f"   (nonzero residual: {r[2]})" if r[0].startswith("3") else ""
        print(f"   {r[0]:26} {r[1]:>7,}{extra}")


if __name__ == "__main__":
    run()
