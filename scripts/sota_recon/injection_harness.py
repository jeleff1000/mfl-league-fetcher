"""
sota_recon/injection_harness.py  --  error-injection harness v0 (WS7e): verify the verifiers.

Masterson and the NGS summed averages were RECALL failures of the checking system, not
data-acquisition failures. This harness makes recall measurable: copy a sandbox slice,
inject each known defect class, and assert the lanes catch every one. Run whenever lane
or contract code changes; every new prod bug adds an injection case here (the data-QA
regression test).

v0 defect taxonomy (from the plan doc's case studies):
  bound_violation      impossible cell (Masterson class: INT > attempts)
  duplicate_row        same player_week twice with identical (opponent, date)
  phantom_dup          duplicate with NULL game_date + nfl_position (wave43 class)
  summed_average       season rate column stored as SUM of weekly averages (NGS class)

    python -m scripts.sota_recon.injection_harness
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import duckdb

from . import recon_bounds
from .sources import latest_v26

YEAR = 2024  # sandbox slice year


def _q(p: str) -> str:
    return Path(p).as_posix()


def run() -> dict:
    src = _q(latest_v26())
    tmpdir = tempfile.mkdtemp(prefix="sota_inject_")
    wk_sb = _q(os.path.join(tmpdir, "weekly_sandbox.parquet"))
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")

    # --- build weekly sandbox with three injected defects -------------------------------
    con.execute(f"""
        CREATE TEMP TABLE base AS SELECT * FROM '{src}' WHERE year = {YEAR}""")
    victim, phantom_victim = [r[0] for r in con.execute("""
        SELECT player_week FROM base
        WHERE attempts >= 10 AND passing_interceptions IS NOT NULL
        ORDER BY player_week LIMIT 2""").fetchall()]
    # 1) bound violation: INT > attempts on the victim row
    con.execute("UPDATE base SET passing_interceptions = attempts + 5 WHERE player_week = ?",
                [victim])
    # 2) duplicate row: exact copy -> dup group with n <> distinct (opponent, date)
    con.execute("INSERT INTO base SELECT * FROM base WHERE player_week = ?", [victim])
    # 3) phantom dup: copy with NULL date + position (the wave43 leftover signature)
    con.execute("""
        INSERT INTO base SELECT * REPLACE (NULL AS game_date, NULL AS nfl_position)
        FROM base WHERE player_week = ? LIMIT 1""", [phantom_victim])
    con.execute(f"COPY base TO '{wk_sb}' (FORMAT PARQUET)")

    caught = {}
    # lane 1: bounds
    b = recon_bounds.run(wk_sb)
    caught["bound_violation"] = b["by_bound"].get("passing_int_le_attempts", 0) >= 1

    # lane 2+3: dup classifier (same queries as run_invariants)
    bad = con.execute(f"""
        WITH dups AS (SELECT player_week FROM '{wk_sb}' WHERE player_week IS NOT NULL
                      GROUP BY player_week HAVING COUNT(*) > 1)
        SELECT
          COUNT(*) FILTER (WHERE n <> dg) AS not_distinct,
          COUNT(*) FILTER (WHERE nullish > 0) AS phantomish
        FROM (
          SELECT player_week, COUNT(*) AS n,
                 COUNT(DISTINCT (opponent_nfl_team, game_date)) AS dg,
                 COUNT(*) FILTER (WHERE game_date IS NULL OR nfl_position IS NULL) AS nullish
          FROM '{wk_sb}' WHERE player_week IN (SELECT player_week FROM dups)
          GROUP BY player_week)""").fetchone()
    caught["duplicate_row"] = bad[0] >= 1
    caught["phantom_dup"] = bad[1] >= 1

    # lane 4: summed-average on a season sandbox -- corrupt passing_cpoe to the weekly SUM
    from . import recon_rate_fingerprint as fp
    sq = fp.season_parquet()
    ssb = _q(os.path.join(tmpdir, "season_sandbox.parquet"))
    con.execute(f"""
        COPY (
          SELECT s.* REPLACE (w.cpoe_sum AS passing_cpoe)
          FROM '{sq}' s
          LEFT JOIN (SELECT NFL_player_id, year, SUM(passing_cpoe) AS cpoe_sum
                     FROM '{src}' WHERE season_type='REG' GROUP BY 1,2) w
            ON s.NFL_player_id = w.NFL_player_id AND s.year = w.year
          WHERE s.year = {YEAR}
        ) TO '{ssb}' (FORMAT PARQUET)""")
    con.close()

    orig_season = fp.season_parquet
    fp.season_parquet = lambda: ssb
    try:
        rows = fp.run(year_min=YEAR)
    finally:
        fp.season_parquet = orig_season
    cpoe = next(r for r in rows if r["column"] == "passing_cpoe")
    caught["summed_average"] = cpoe["verdict"] == "SUMMED-AVERAGE"

    recall = sum(caught.values()) / len(caught)
    return {"caught": caught, "recall": recall, "sandbox": tmpdir}


if __name__ == "__main__":
    r = run()
    for k, v in r["caught"].items():
        print(f"  {k:20s} {'CAUGHT' if v else 'MISSED'}")
    print(f"INJECTION RECALL: {r['recall']:.0%}")
    raise SystemExit(0 if r["recall"] == 1.0 else 1)
