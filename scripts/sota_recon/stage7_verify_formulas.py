"""STAGE 7 FORMULA VERIFIER: a formula is VOUCHED, never assumed.

Each candidate (including scale variants -- 0-1 vs 0-100 conventions and the
NFL passer-rating form) is tested on 2015-2024 single-row weeks. A variant
that agrees >= 99% with the stored column is LICENSED into the registry;
columns with no winning variant are queued, never guessed.

Output: stage7_formulas.v1.json (the licensed registry) + a receipt with the
losing variants' scores (the naive number beside the real one).
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
REGISTRY = Path(__file__).parent / "witness_gate" / "contracts" / "stage7_formulas.v1.json"
RECEIPT = LAKE / "stage7_formula_verification.json"
TOL = 0.05   # sources print rates to one decimal
MIN_N = 500

R = "TRY_CAST(rushing_yards AS DOUBLE)"
RECV = "TRY_CAST(receiving_yards AS DOUBLE)"
P = "TRY_CAST(passing_yards AS DOUBLE)"
ATT = "TRY_CAST(attempts AS DOUBLE)"
CMP = "TRY_CAST(completions AS DOUBLE)"
TD = "TRY_CAST(passing_tds AS DOUBLE)"
INT = "TRY_CAST(passing_interceptions AS DOUBLE)"
SK = "TRY_CAST(sacks_suffered AS DOUBLE)"
SKY = "TRY_CAST(sack_yards_lost AS DOUBLE)"

PASSER_RATING = f"""(
  LEAST(GREATEST(({CMP}/{ATT} - 0.3) * 5, 0), 2.375)
+ LEAST(GREATEST(({P}/{ATT} - 3) * 0.25, 0), 2.375)
+ LEAST(GREATEST(({TD}/{ATT}) * 20, 0), 2.375)
+ LEAST(GREATEST(2.375 - ({INT}/{ATT}) * 25, 0), 2.375)
) / 6 * 100"""

#: column -> {variant_name: (expr, guard)}   guard = denominator > 0 filter
CANDIDATES = {
    "passing_yards_per_attempt": {"yds/att": (f"{P}/{ATT}", ATT)},
    "rushing_yards_per_carry": {
        "yds/car": (f"{R}/TRY_CAST(carries AS DOUBLE)",
                    "TRY_CAST(carries AS DOUBLE)")},
    "receiving_yards_per_reception": {
        "yds/rec": (f"{RECV}/TRY_CAST(receptions AS DOUBLE)",
                    "TRY_CAST(receptions AS DOUBLE)")},
    "receiving_yards_per_target": {
        "yds/tgt": (f"{RECV}/TRY_CAST(targets AS DOUBLE)",
                    "TRY_CAST(targets AS DOUBLE)")},
    "punt_yards_per_punt": {
        "yds/punt": ("TRY_CAST(punt_yards AS DOUBLE)/TRY_CAST(punts AS DOUBLE)",
                     "TRY_CAST(punts AS DOUBLE)")},
    "pat_pct": {
        "made/att*100": ("TRY_CAST(pat_made AS DOUBLE)/TRY_CAST(pat_att AS DOUBLE)*100",
                         "TRY_CAST(pat_att AS DOUBLE)"),
        "made/att": ("TRY_CAST(pat_made AS DOUBLE)/TRY_CAST(pat_att AS DOUBLE)",
                     "TRY_CAST(pat_att AS DOUBLE)")},
    "fg_pct": {
        "made/att*100": ("TRY_CAST(fg_made AS DOUBLE)/TRY_CAST(fg_att AS DOUBLE)*100",
                         "TRY_CAST(fg_att AS DOUBLE)"),
        "made/att": ("TRY_CAST(fg_made AS DOUBLE)/TRY_CAST(fg_att AS DOUBLE)",
                     "TRY_CAST(fg_att AS DOUBLE)")},
    "completion_pct": {
        "cmp/att*100": (f"{CMP}/{ATT}*100", ATT),
        "cmp/att": (f"{CMP}/{ATT}", ATT)},
    "passing_td_pct": {
        "td/att*100": (f"{TD}/{ATT}*100", ATT),
        "td/att": (f"{TD}/{ATT}", ATT)},
    "passing_int_pct": {
        "int/att*100": (f"{INT}/{ATT}*100", ATT),
        "int/att": (f"{INT}/{ATT}", ATT)},
    "catch_pct": {
        "rec/tgt*100": ("TRY_CAST(receptions AS DOUBLE)/TRY_CAST(targets AS DOUBLE)*100",
                        "TRY_CAST(targets AS DOUBLE)"),
        "rec/tgt": ("TRY_CAST(receptions AS DOUBLE)/TRY_CAST(targets AS DOUBLE)",
                    "TRY_CAST(targets AS DOUBLE)")},
    "yards_per_touch": {
        "(ruyd+reyd)/(car+rec)": (
            f"(COALESCE({R},0)+COALESCE({RECV},0))"
            "/(COALESCE(TRY_CAST(carries AS DOUBLE),0)"
            "+COALESCE(TRY_CAST(receptions AS DOUBLE),0))",
            "(COALESCE(TRY_CAST(carries AS DOUBLE),0)"
            "+COALESCE(TRY_CAST(receptions AS DOUBLE),0))")},
    "passing_adjusted_yards_per_attempt": {
        "AY/A": (f"({P} + 20*{TD} - 45*{INT})/{ATT}", ATT)},
    "passing_net_yards_per_attempt": {
        "NY/A": (f"({P} - COALESCE({SKY},0))/({ATT} + COALESCE({SK},0))",
                 f"({ATT} + COALESCE({SK},0))")},
    "passing_adjusted_net_yards_per_attempt": {
        "ANY/A": (f"({P} + 20*{TD} - 45*{INT} - COALESCE({SKY},0))"
                  f"/({ATT} + COALESCE({SK},0))",
                  f"({ATT} + COALESCE({SK},0))")},
    "passer_rating": {"nfl_formula": (PASSER_RATING, ATT)},
    "target_share": {
        "tgt/team_tgt": (
            "TRY_CAST(targets AS DOUBLE)/NULLIF(SUM(TRY_CAST(targets AS DOUBLE))"
            " OVER (PARTITION BY year, week, nfl_team), 0)",
            "TRY_CAST(targets AS DOUBLE)")},
    "air_yards_share": {
        "ay/team_ay": (
            "TRY_CAST(air_yards AS DOUBLE)/NULLIF(SUM(TRY_CAST(air_yards AS DOUBLE))"
            " OVER (PARTITION BY year, week, nfl_team), 0)",
            "TRY_CAST(air_yards AS DOUBLE)")},
}


def main() -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW modern AS
    SELECT * FROM read_parquet('{wk}')
    WHERE season_type = 'REG' AND CAST(year AS INT) BETWEEN 2015 AND 2024
    QUALIFY COUNT(*) OVER (PARTITION BY NFL_player_id, year, week) = 1""")
    licensed, queued, scores = {}, [], {}
    for col, variants in CANDIDATES.items():
        best = None
        scores[col] = {}
        for vname, (expr, guard) in variants.items():
            try:
                n, ok = con.execute(f"""
                SELECT COUNT(*), COUNT(*) FILTER (
                  WHERE ABS(TRY_CAST({col} AS DOUBLE) - ({expr})) <= {TOL})
                FROM (SELECT *, {expr} AS __rc, {guard} AS __g FROM modern)
                WHERE __g > 0 AND {col} IS NOT NULL""").fetchone()
            except Exception as e:
                scores[col][vname] = f"ERROR {str(e).splitlines()[0][:60]}"
                continue
            agree = ok / n if n else 0.0
            scores[col][vname] = {"n": n, "agree": round(agree, 4)}
            if n >= MIN_N and agree >= 0.99 and (
                    best is None or agree > best[1]):
                best = (vname, agree, expr, guard)
        if best:
            licensed[col] = {"variant": best[0], "agree": round(best[1], 4),
                             "expr": best[2], "guard": best[3]}
        else:
            queued.append(col)
    REGISTRY.write_text(json.dumps(
        {"version": "v1", "generated": time.strftime("%Y-%m-%d %H:%M"),
         "vouch_window": "2015-2024", "min_agree": 0.99, "tol": TOL,
         "licensed": licensed, "queued_no_winner": queued},
        indent=1), encoding="utf-8")
    RECEIPT.write_text(json.dumps(
        {"scores_all_variants": scores, "licensed": sorted(licensed),
         "queued": queued}, indent=1), encoding="utf-8")
    return {"licensed": len(licensed), "queued": queued}


if __name__ == "__main__":
    print(json.dumps(main(), indent=1))
