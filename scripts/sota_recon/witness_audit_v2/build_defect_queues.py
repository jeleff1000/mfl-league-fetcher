# -*- coding: utf-8 -*-
"""build_defect_queues.py -- emit receipted CSV queues for defects that must NOT be auto-fixed.

Deletion discipline / no-allocation discipline: these classes need per-row adjudication, so we
localize them with receipts and stop. Nothing here mutates the super table.

  Q1 targets_gt_attempts  -- hard-bound violation at team-game grain (a team cannot target more
                            receivers than it threw passes). Suspected root cause: build_targets_
                            backfill_v26's GREATEST(pbp, receptions, existing) never-decrease rule
                            can push targets above attempts when the pre-existing value was wrong.
  Q2 season_overcount     -- v26 has MORE player-weeks than the PFR season witness's games-played
                            (n > g). Could be dup rows, POST leak, or a wrong witness g -> row
                            deletion is NEVER automatic (deletion discipline).

    python -m scripts.sota_recon.witness_audit_v2.build_defect_queues
"""
import glob
from pathlib import Path

import duckdb

OUT = Path("D:/league-history-data/nfl/derived/validation/witness_audit_2026_07_16")
SUP = sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
             key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
T1 = "D:/league-history-data/nfl/raw/pfr/players/tables/rushing_and_receiving/_combined.parquet"
T2 = "D:/league-history-data/nfl/raw/pfr/players/tables/receiving_and_rushing/_combined.parquet"
V = f"read_parquet('{Path(SUP).as_posix()}')"
D = lambda c: f'COALESCE(TRY_CAST("{c}" AS DOUBLE),0)'


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'"); con.execute("PRAGMA threads=4")
    con.execute("PRAGMA disable_progress_bar")

    q1 = OUT / "queue_targets_gt_attempts.csv"
    con.execute(f"""COPY (
        SELECT yr AS year, wk AS week, season_type, nfl_franchise_number,
               tgt_sum AS targets, att_sum AS attempts, tgt_sum - att_sum AS excess
        FROM (SELECT CAST(year AS INT) AS yr, CAST(week AS INT) AS wk, season_type,
                     nfl_franchise_number, SUM({D('targets')}) AS tgt_sum, SUM({D('attempts')}) AS att_sum
              FROM {V} WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL AND year>=1978
              GROUP BY 1,2,3,4) t
        WHERE tgt_sum > att_sum ORDER BY excess DESC
    ) TO '{q1.as_posix()}' (HEADER, DELIMITER ',')""")
    n1 = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{q1.as_posix()}')").fetchone()[0]

    q2 = OUT / "queue_season_overcount.csv"
    con.execute(f"""COPY (
        WITH w AS (
          SELECT pfr_id, TRY_CAST(year_id AS INT) y, MAX(TRY_CAST(games AS INT)) g
          FROM (SELECT pfr_id, year_id, games FROM read_parquet('{T1}')
                UNION ALL SELECT pfr_id, year_id, games FROM read_parquet('{T2}'))
          WHERE regexp_matches(CAST(year_id AS VARCHAR),'^[0-9]{{4}}$') GROUP BY 1,2),
        b AS (SELECT DISTINCT pfr_id, NFL_player_id FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL),
        o AS (SELECT NFL_player_id id, CAST(year AS INT) y, COUNT(*) n
              FROM {V} WHERE season_type='REG' GROUP BY 1,2)
        SELECT b.NFL_player_id, w.pfr_id, w.y AS year, o.n AS v26_player_weeks,
               w.g AS witness_games_played, o.n - w.g AS excess_rows
        FROM w JOIN b USING(pfr_id) JOIN o ON o.id=b.NFL_player_id AND o.y=w.y
        WHERE w.y BETWEEN 1932 AND 1977 AND w.g IS NOT NULL AND o.n > w.g
        ORDER BY excess_rows DESC
    ) TO '{q2.as_posix()}' (HEADER, DELIMITER ',')""")
    n2 = con.execute(f"SELECT COUNT(*) FROM read_csv_auto('{q2.as_posix()}')").fetchone()[0]
    con.close()
    print(f"Q1 targets>attempts  : {n1:>5} team-games -> {q1.name}")
    print(f"Q2 season overcount  : {n2:>5} player-seasons -> {q2.name}")
    print("Both are ADJUDICATION queues -- no automatic mutation.")


if __name__ == "__main__":
    main()
