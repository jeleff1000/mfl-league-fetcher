"""REPAIR 2a (released by repair_signoff, Joe 2026-08-02): the season/career
planes SUM weekly rates -- convicted cross-root with three proof styles
(pfr_rate_wave, nflcom_wave, the pbp_ratio specs). A summed completion_pct of
343 means nothing; the repair recomputes every convicted rate from its season
COMPONENT sums.

Passer rating carries a SECOND convicted mechanism: the stored season value is
the MEAN of weekly ratings (~86 vs true 73-78). Recomputed from components
like everything else.

This is a STANDALONE pass over the built planes (the season builders carry
uncommitted concurrent work and are not touched). Output goes SIDE-BY-SIDE
under repaired_2a/ -- the swap happens at the promote checkpoint with
before/after evidence, per approve-before-ship.

Abstention: a rate with NULL components or a zero denominator becomes NULL.
Never COALESCE-0.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon.recon_rate_fingerprint import season_parquet

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUTDIR = LAKE / "repaired_2a"
RECEIPT = LAKE / "repair_2a_receipt.json"

CLAMP = "LEAST(GREATEST({x}, 0), 2.375)"


def rating(cmp_, att, yds, td, ints):
    a = CLAMP.format(x=f"(({cmp_})*1.0/NULLIF({att},0) - 0.3) * 5")
    b = CLAMP.format(x=f"(({yds})*1.0/NULLIF({att},0) - 3) * 0.25")
    c = CLAMP.format(x=f"({td})*20.0/NULLIF({att},0)")
    d = CLAMP.format(x=f"2.375 - ({ints})*25.0/NULLIF({att},0)")
    return f"(({a}) + ({b}) + ({c}) + ({d})) / 6 * 100"


#: convicted rate -> (recompute expr over season-component columns, guard denom)
RATES: dict[str, tuple[str, str]] = {
    "completion_pct": ("100.0*completions/NULLIF(attempts,0)", "attempts"),
    "passing_yards_per_attempt": ("passing_yards*1.0/NULLIF(attempts,0)", "attempts"),
    "passing_adjusted_yards_per_attempt": (
        "(passing_yards + 20.0*passing_tds - 45.0*passing_interceptions)"
        "/NULLIF(attempts,0)", "attempts"),
    "passing_net_yards_per_attempt": (
        "(passing_yards - sack_yards_lost)*1.0"
        "/NULLIF(attempts + sacks_suffered,0)", "attempts + sacks_suffered"),
    "passing_adjusted_net_yards_per_attempt": (
        "(passing_yards + 20.0*passing_tds - 45.0*passing_interceptions"
        " - sack_yards_lost)/NULLIF(attempts + sacks_suffered,0)",
        "attempts + sacks_suffered"),
    "net_yards_per_attempt": (
        "(passing_yards - sack_yards_lost)*1.0"
        "/NULLIF(attempts + sacks_suffered,0)", "attempts + sacks_suffered"),
    "catch_pct": ("100.0*receptions/NULLIF(targets,0)", "targets"),
    "receiving_yards_per_reception": (
        "receiving_yards*1.0/NULLIF(receptions,0)", "receptions"),
    "receiving_yards_per_target": (
        "receiving_yards*1.0/NULLIF(targets,0)", "targets"),
    "rushing_yards_per_carry": ("rushing_yards*1.0/NULLIF(carries,0)", "carries"),
    "yards_per_touch": (
        "(rushing_yards + receiving_yards)*1.0/NULLIF(total_touches,0)",
        "total_touches"),
    "pat_pct": ("100.0*pat_made/NULLIF(pat_att,0)", "pat_att"),
    "punt_yards_per_punt": ("punt_yards*1.0/NULLIF(punts,0)", "punts"),
    "passer_rating": (
        rating("completions", "attempts", "passing_yards", "passing_tds",
               "passing_interceptions"), "attempts"),
    "receiving_pass_rating": (
        rating("receptions", "targets", "receiving_yards", "receiving_tds",
               "receiving_target_interceptions"), "targets"),
}


def repair_plane(con: duckdb.DuckDBPyConnection, src: Path, label: str) -> dict:
    out = OUTDIR / f"{label}.parquet"
    have = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM '{src.as_posix()}' LIMIT 0").fetchall()}
    todo = {k: v for k, v in RATES.items() if k in have}
    replacements = ", ".join(
        f"CASE WHEN {den} IS NOT NULL AND ({den}) > 0 THEN {expr} "
        f"ELSE NULL END AS {col}"
        for col, (expr, den) in todo.items())
    con.execute(f"""
    COPY (SELECT * REPLACE ({replacements}) FROM '{src.as_posix()}')
    TO '{out.as_posix()}' (FORMAT parquet)""")

    deltas = {}
    for col, (expr, den) in todo.items():
        n, changed, mb, ma = con.execute(f"""
        SELECT COUNT(*),
          COUNT(*) FILTER (WHERE ABS(COALESCE(TRY_CAST(b.{col} AS DOUBLE), -1e18)
                                   - COALESCE(TRY_CAST(a.{col} AS DOUBLE), -1e18)) > 0.005),
          MEDIAN(TRY_CAST(b.{col} AS DOUBLE)), MEDIAN(TRY_CAST(a.{col} AS DOUBLE))
        FROM '{src.as_posix()}' b
        POSITIONAL JOIN '{out.as_posix()}' a""").fetchone()
        deltas[col] = {"rows": n, "changed": changed,
                       "median_before": None if mb is None else round(float(mb), 2),
                       "median_after": None if ma is None else round(float(ma), 2)}
    return {"plane": label, "source": str(src), "out": str(out),
            "columns": deltas}


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    OUTDIR.mkdir(exist_ok=True)
    season = Path(season_parquet())
    results = [repair_plane(con, season, "season")]
    career = season.parent / season.name.replace("season", "career")
    if career.exists() and career != season:
        results.append(repair_plane(con, career, "career"))
    receipt = {"wave": "repair_2a_season_rates", "date": "2026-08-02",
               "released_by": "repair_signoff APPROVED 2026-08-02",
               "planes": results,
               "swap": "NOT SWAPPED -- promote checkpoint shows before/after"}
    RECEIPT.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print(json.dumps(receipt, indent=2)[:2400])
