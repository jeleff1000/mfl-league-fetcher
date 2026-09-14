"""WEEK-ALIGNMENT AUDIT: does the plane's week index agree with the
catalog's, per (team, season)?

The ctx-repair failure proved "same (team, year, week) = same game" is
FALSE for some ancient seasons. This audit aligns each team-season's plane
game sequence to the catalog's by matching OPPONENT SEQUENCES (order
preserved): a season is ALIGNED when plane weeks map 1:1 onto catalog
weeks with identical opponents at zero offset; OFFSET(k) when a constant
shift explains it; SCRAMBLED otherwise. Output: per-team-season verdicts +
a misalignment census by era. Nothing is repaired here -- this is the
LICENSE for any future context repair.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "week_alignment_map.parquet"
RECEIPT = LAKE / "week_alignment_receipt.json"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()

    # DH-SAFE (corrected 2026-08-04): the first version collapsed each
    # team-week with ANY_VALUE(opponent), so every ancient DOUBLEHEADER
    # week read as a mismatch -- manufacturing 55 "scrambled" seasons and
    # a phantom "130 missing games" project. The plane holds both games;
    # the audit must key on (team, yr, wk, OPPONENT), never collapse.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE plane_seq AS
    SELECT DISTINCT nfl_team AS team, CAST(year AS INT) AS yr,
           TRY_CAST(week AS INT) AS wk, opponent_nfl_team AS opp
    FROM read_parquet('{wk}')
    WHERE season_type = 'REG' AND nfl_team IS NOT NULL
      AND opponent_nfl_team IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cat_seq AS
    SELECT team_code AS team, CAST(year AS INT) AS yr,
           TRY_CAST(week AS INT) AS wk, opponent_code AS opp
    FROM '{games}' WHERE season_type = 'REG'""")

    rows = con.execute("""
    WITH joined AS (
      SELECT p.team, p.yr,
        COUNT(*) AS plane_games,
        COUNT(c.opp) AS matched_weeks,
        COUNT(c.opp) AS same_opp,
        COUNT(*) FILTER (WHERE c1.opp IS NOT NULL
          AND LOWER(TRIM(p.opp)) = LOWER(TRIM(c1.opp))) AS opp_plus1,
        COUNT(*) FILTER (WHERE cm1.opp IS NOT NULL
          AND LOWER(TRIM(p.opp)) = LOWER(TRIM(cm1.opp))) AS opp_minus1
      FROM plane_seq p
      LEFT JOIN cat_seq c ON c.team = p.team AND c.yr = p.yr
        AND c.wk = p.wk AND LOWER(TRIM(c.opp)) = LOWER(TRIM(p.opp))
      LEFT JOIN cat_seq c1 ON c1.team = p.team AND c1.yr = p.yr
        AND c1.wk = p.wk + 1
      LEFT JOIN cat_seq cm1 ON cm1.team = p.team AND cm1.yr = p.yr
        AND cm1.wk = p.wk - 1
      GROUP BY 1, 2)
    SELECT team, yr, plane_games, matched_weeks, same_opp,
           opp_plus1, opp_minus1,
      CASE
        WHEN plane_games > 0 AND same_opp = plane_games THEN 'ALIGNED'
        WHEN plane_games > 0 AND same_opp >= plane_games - 1
             AND plane_games >= 6 THEN 'ALIGNED_MINUS1'
        WHEN opp_plus1 > same_opp AND opp_plus1 >= plane_games - 2
             THEN 'OFFSET_PLUS1'
        WHEN opp_minus1 > same_opp AND opp_minus1 >= plane_games - 2
             THEN 'OFFSET_MINUS1'
        WHEN matched_weeks = 0 THEN 'NO_CATALOG_TEAM'
        ELSE 'SCRAMBLED'
      END AS verdict
    FROM joined""").fetchall()
    import pandas as pd
    df = pd.DataFrame(rows, columns=[
        "team", "yr", "plane_games", "matched_weeks", "same_opp",
        "opp_plus1", "opp_minus1", "verdict"])
    con.register("df", df)
    con.execute(f"COPY (SELECT * FROM df) TO '{OUT.as_posix()}' "
                "(FORMAT parquet)")
    census = (df.assign(dec=(df.yr // 10) * 10)
                .groupby(["dec", "verdict"]).size().unstack(fill_value=0))
    RECEIPT.write_text(json.dumps({
        "date": time.strftime("%Y-%m-%d %H:%M"),
        "team_seasons": len(df),
        "verdicts": df.verdict.value_counts().to_dict(),
        "by_decade": {str(k): v.to_dict() for k, v in census.iterrows()},
        "law": ("context repair is licensed ONLY for ALIGNED team-seasons; "
                "OFFSET seasons need week-index correction first; "
                "SCRAMBLED seasons are an ingestion audit")},
        indent=1), encoding="utf-8")
    print(json.dumps(df.verdict.value_counts().to_dict(), indent=1))


if __name__ == "__main__":
    main()
