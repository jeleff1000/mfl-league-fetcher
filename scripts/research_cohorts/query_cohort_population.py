"""Query full lake cohort populations without downloading the lake.

Produces one row per year and exact cohort cell, plus a position-scoped view of the
eligible league denominators used by the matchup tables.  The query runs in Actions
against the restored public research lake; only the small CSV results are emitted.
"""
from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
# HERE is .../scripts/research_cohorts; its repo root is two parents up.
ROOT = HERE.parent.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))


def main() -> int:
    from cohort_format_sql import cohort_league_settings_sql
    from build_research_matchup_cohort import DENOM_SQL, YEARS, YEAR_PREDICATE
    from local_reader import LocalReader

    out = Path(os.environ.get("COHORT_COUNTS_OUT", "out"))
    out.mkdir(parents=True, exist_ok=True)
    reader = LocalReader()
    con = duckdb.connect()
    con.execute("SET memory_limit='3000MB'; SET threads=2; SET enable_progress_bar=false;")

    # One row per league-year setting.  This is the non-position-scoped population count;
    # it is the right answer for cohort thickness, while DENOM_SQL below is position scoped.
    settings_sql = cohort_league_settings_sql(position_slots=True)
    rows = reader.query(f"""
        WITH ls AS ({settings_sql})
        SELECT year, teams, roster, ppr, td, bracket, league_type, lineup_mode,
               keeper_mode, COUNT(DISTINCT db_name) AS n_leagues
        FROM ls
        WHERE year BETWEEN {min(YEARS)} AND {max(YEARS)}
        GROUP BY ALL
        ORDER BY year, teams, roster, ppr, td, bracket, league_type, lineup_mode,
                 keeper_mode
    """, "___leagues")
    _write(out / "cohort_league_counts.csv", rows)

    # Position-eligible denominators, including the bracket and format rollups emitted by
    # the production matchup builder.  Keep only the fully specified cohort rung here so a
    # reader does not mistake ALL rollups for additional leagues.
    denom_rows = reader.query(DENOM_SQL, "___leagues")
    exact = [r for r in denom_rows if all(r.get(k) != "ALL" for k in (
        "teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode",
        "keeper_mode"))]
    _write(out / "cohort_position_eligible_counts_exact.csv", exact)

    # Always include 2003-2010's mandated pooled cohort (all dimensions are ALL), which is
    # intentionally absent from the exact-format filter above.
    pooled = [r for r in denom_rows if all(r.get(k) == "ALL" for k in (
        "teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode",
        "keeper_mode"))]
    _write(out / "cohort_position_eligible_counts_pooled_2003_2010.csv", pooled)

    print(f"cohort rows: {len(rows):,}")
    print(f"position exact rows: {len(exact):,}")
    print(f"position pooled rows: {len(pooled):,}")
    for row in reader.query("SELECT year, COUNT(DISTINCT db_name) AS n_leagues FROM public.league_settings GROUP BY 1 ORDER BY 1", "___leagues"):
        print(f"YEAR {row['year']}: {row['n_leagues']:,} league-years")
    reader.close()
    con.close()
    return 0


def _write(path: Path, rows: list[dict]) -> None:
    if not rows:
        raise RuntimeError(f"query returned no rows for {path.name}")
    keys = list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {path} ({len(rows):,} rows)")


if __name__ == "__main__":
    raise SystemExit(main())
