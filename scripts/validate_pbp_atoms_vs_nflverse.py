#!/usr/bin/env python3
"""
Validation gate: reconcile our merged-PBP player-week atoms against nflverse's
published `stats_player_week_{year}` — the public reference.

Because our merged PBP *is* nflverse PBP for 1999+, aggregating it through the real
production path (`build_event_sql()` -> `create_weekly_rollup()` in
aggregate_merged_pbp_for_supertable_audit.py) must reproduce nflverse's published
player-week values. This tool measures the exact-match rate per stat so every atom
passes the same public-value gate before it is integrated into the super table.

This is READ-ONLY with respect to the production formulas: it imports and runs
`build_event_sql()` as-is; it never edits it. Run it for one year or several to
find where each stat holds and where it breaks.

Usage:
  python scripts/validate_pbp_atoms_vs_nflverse.py --years 2023
  python scripts/validate_pbp_atoms_vs_nflverse.py --years 2023,2015,1999
  python scripts/validate_pbp_atoms_vs_nflverse.py --year-range 1999-2023

nflverse source (fetched live via httpfs, no local copy):
  https://github.com/nflverse/nflverse-data/releases/download/player_stats/stats_player_week_{year}.parquet
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "fantasy_football_data_scripts"))

# Reuse the exact production aggregation path — no reimplementation of formulas.
from scripts.aggregate_merged_pbp_for_supertable_audit import (  # noqa: E402
    DEFAULT_BIO,
    DEFAULT_BIO_REPAIRED,
    DEFAULT_PBP,
    MAX_COLUMNS,
    create_bio_lookup,
    create_pbp_base,
    create_weekly_rollup,
)

NFLVERSE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/"
    "player_stats/stats_player_week_{year}.parquet"
)

# (label, our_rollup_col, nflverse_col, tolerance, agg)
# agg: 'sum' (counts/yards, summed across role rows) or 'max' (long-distance stats).
# Only atoms nflverse publishes per player-week appear here — that IS the gate universe.
STAT_PAIRS: list[tuple[str, str, str, float, str]] = [
    # --- Passing ---
    ("attempts", "attempts", "attempts", 0, "sum"),
    ("completions", "completions", "completions", 0, "sum"),
    ("passing_yards", "passing_yards", "passing_yards", 0, "sum"),
    ("passing_tds", "passing_tds", "passing_tds", 0, "sum"),
    ("passing_interceptions", "passing_interceptions", "passing_interceptions", 0, "sum"),
    ("sacks_suffered", "sacks_suffered", "sacks_suffered", 0, "sum"),
    # --- Rushing ---
    ("carries", "carries", "carries", 0, "sum"),
    ("rushing_yards", "rushing_yards", "rushing_yards", 0, "sum"),
    ("rushing_tds", "rushing_tds", "rushing_tds", 0, "sum"),
    # --- Receiving ---
    ("targets", "targets", "targets", 0, "sum"),
    ("receptions", "receptions", "receptions", 0, "sum"),
    ("receiving_yards", "receiving_yards", "receiving_yards", 0, "sum"),
    ("receiving_tds", "receiving_tds", "receiving_tds", 0, "sum"),
    # --- Special teams TDs ---
    ("special_teams_tds", "special_teams_tds", "special_teams_tds", 0, "sum"),
    # --- Defense ---
    ("def_tackles_solo", "def_tackles_solo", "def_tackles_solo", 0, "sum"),
    ("def_tackle_assists", "def_tackle_assists", "def_tackle_assists", 0, "sum"),
    ("def_tackles_with_assist", "def_tackles_with_assist", "def_tackles_with_assist", 0, "sum"),
    ("def_tackles_for_loss", "def_tackles_for_loss", "def_tackles_for_loss", 0, "sum"),
    ("def_fumbles_forced", "def_fumbles_forced", "def_fumbles_forced", 0, "sum"),
    ("def_sacks", "def_sacks", "def_sacks", 0.0, "sum"),  # half-sacks -> DOUBLE both sides
    ("def_qb_hits", "def_qb_hits", "def_qb_hits", 0, "sum"),
    ("def_interceptions", "def_interceptions", "def_interceptions", 0, "sum"),
    ("def_interception_yards", "def_interception_yards", "def_interception_yards", 0, "sum"),
    ("def_pass_defended", "def_pass_defended", "def_pass_defended", 0, "sum"),
    ("def_safeties", "def_safeties", "def_safeties", 0, "sum"),
    # --- Fumble recovery yards (own/opp split) ---
    ("fumble_recovery_yards_own", "fumble_recovery_yards_own", "fumble_recovery_yards_own", 0, "sum"),
    ("fumble_recovery_yards_opp", "fumble_recovery_yards_opp", "fumble_recovery_yards_opp", 0, "sum"),
    # --- Returns ---
    ("punt_returns", "punt_returns", "punt_returns", 0, "sum"),
    ("punt_return_yards", "punt_return_yards", "punt_return_yards", 0, "sum"),
    ("kickoff_returns", "kickoff_returns", "kickoff_returns", 0, "sum"),
    ("kickoff_return_yards", "kickoff_return_yards", "kickoff_return_yards", 0, "sum"),
    # --- Kicking ---
    ("fg_made", "fg_made", "fg_made", 0, "sum"),
    ("fg_att", "fg_att", "fg_att", 0, "sum"),
    ("fg_missed", "fg_missed", "fg_missed", 0, "sum"),
    ("fg_blocked", "fg_blocked", "fg_blocked", 0, "sum"),
    ("fg_long", "fg_long", "fg_long", 0, "max"),
    ("fg_made_0_19", "fg_made_0_19", "fg_made_0_19", 0, "sum"),
    ("fg_made_20_29", "fg_made_20_29", "fg_made_20_29", 0, "sum"),
    ("fg_made_30_39", "fg_made_30_39", "fg_made_30_39", 0, "sum"),
    ("fg_made_40_49", "fg_made_40_49", "fg_made_40_49", 0, "sum"),
    ("fg_made_50_59", "fg_made_50_59", "fg_made_50_59", 0, "sum"),
    ("fg_made_60plus", "fg_made_60plus", "fg_made_60_", 0, "sum"),
    ("pat_made", "pat_made", "pat_made", 0, "sum"),
    ("pat_att", "pat_att", "pat_att", 0, "sum"),
    ("pat_missed", "pat_missed", "pat_missed", 0, "sum"),
    ("pat_blocked", "pat_blocked", "pat_blocked", 0, "sum"),
    # --- Advanced efficiency (EPA-derived floor ~1999; air_yards ~2006) ---
    ("passing_epa", "passing_epa", "passing_epa", 0.01, "sum"),
    ("rushing_epa", "rushing_epa", "rushing_epa", 0.01, "sum"),
    ("receiving_epa", "receiving_epa", "receiving_epa", 0.01, "sum"),
    ("passing_air_yards", "passing_air_yards", "passing_air_yards", 0, "sum"),
    ("receiving_air_yards", "receiving_air_yards", "receiving_air_yards", 0, "sum"),
    ("passing_2pt_conversions", "passing_2pt_conversions", "passing_2pt_conversions", 0, "sum"),
    ("rushing_2pt_conversions", "rushing_2pt_conversions", "rushing_2pt_conversions", 0, "sum"),
    ("receiving_2pt_conversions", "receiving_2pt_conversions", "receiving_2pt_conversions", 0, "sum"),
]

# Stats where nflverse's own published field is degenerate/unreliable, so it is not a
# valid gate reference. fumble_recovery_yards_*: nflverse stats_player_week stores a
# constant ~1 per recovery, NOT actual return yards (confirmed 2023). Our values (real
# return yards) are the correct ones; these are shown for visibility but never counted
# as gate failures.
INFORMATIONAL = {"fumble_recovery_yards_own", "fumble_recovery_yards_opp"}

# Rate stats stored as SUM-able parts (numerator + denominator atoms) whose ratio must
# match a nflverse published mean. (label, our_num_col, our_den_col, nflverse_col, tol)
RATIO_PAIRS: list[tuple[str, str, str, str, float]] = [
    ("passing_cpoe", "passing_cpoe_sum", "passing_cpoe_n", "passing_cpoe", 0.05),
    # PACR/RACR are yards-per-air-yard ratios of atoms we already validated 100%.
    ("pacr", "passing_yards", "passing_air_yards", "pacr", 0.02),
    ("racr", "receiving_yards", "receiving_air_yards", "racr", 0.02),
]

# Composite metrics we CREATE by summing validated component atoms. Each component
# must be a STAT_PAIRS label (so it lives in the cmp table on both sides). Correct by
# construction — total = sum of components that each already match nflverse.
# (label, [component labels], tol)
COMPOSITE_PAIRS: list[tuple[str, list[str], float]] = [
    ("total_epa", ["passing_epa", "rushing_epa", "receiving_epa"], 0.02),
]


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_ours(con: duckdb.DuckDBPyConnection) -> None:
    """Collapse the rollup to one row per (player_id, week) for REG, so split
    pbp_player_ids that map to the same NFL_player_id are summed together."""
    our_cols = sorted({p[1] for p in STAT_PAIRS} | {r[1] for r in RATIO_PAIRS} | {r[2] for r in RATIO_PAIRS})
    exprs = []
    for col in our_cols:
        agg = "MAX" if col in MAX_COLUMNS else "SUM"
        exprs.append(f"{agg}(COALESCE({q(col)}, 0)) AS {q(col)}")
    con.execute(
        f"""
        CREATE TEMP TABLE ours AS
        SELECT
            CAST(NFL_player_id AS VARCHAR) AS player_id,
            CAST(week AS INTEGER) AS week,
            ANY_VALUE(player) AS player,
            {", ".join(exprs)}
        FROM pbp_player_week_rollup
        WHERE season_type = 'REG'
          AND NFL_player_id IS NOT NULL
          AND TRIM(CAST(NFL_player_id AS VARCHAR)) <> ''
        GROUP BY 1, 2
        """
    )


def build_nfl(con: duckdb.DuckDBPyConnection, year: int) -> int:
    nfl_cols = sorted({p[2] for p in STAT_PAIRS})
    ratio_cols = sorted({r[3] for r in RATIO_PAIRS})  # kept raw (NULL = absent) for ratio compare
    url = NFLVERSE_URL.format(year=year)
    con.execute(
        f"""
        CREATE TEMP TABLE nfl AS
        SELECT
            CAST(player_id AS VARCHAR) AS player_id,
            CAST(week AS INTEGER) AS week,
            player_display_name AS player,
            {", ".join(f"COALESCE({q(c)}, 0) AS {q(c)}" for c in nfl_cols)},
            {", ".join(f"CAST({q(c)} AS DOUBLE) AS {q(c)}" for c in ratio_cols)}
        FROM read_parquet('{url}')
        WHERE season_type = 'REG'
          AND player_id IS NOT NULL
        """
    )
    return int(con.execute("SELECT COUNT(*) FROM nfl").fetchone()[0])


def composite_scorecard(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Composites = sum of component atoms, compared ours-vs-nflverse from the cmp table."""
    out = []
    for label, comps, tol in COMPOSITE_PAIRS:
        o_sum = " + ".join(q("o_" + c) for c in comps)
        n_sum = " + ".join(q("n_" + c) for c in comps)
        row = con.execute(
            f"""
            WITH j AS (SELECT ({o_sum}) AS ov, ({n_sum}) AS nv FROM cmp)
            SELECT COUNT(*) FILTER (WHERE ABS(ov) > 0.005 OR ABS(nv) > 0.005) AS universe,
                   COUNT(*) FILTER (WHERE (ABS(ov) > 0.005 OR ABS(nv) > 0.005) AND ABS(ov - nv) <= {tol}) AS matched
            FROM j
            """
        ).fetchone()
        out.append((label, row[0], row[1]))
    return out


def ratio_scorecard(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    """Rate stats: compare our SUM(num)/SUM(den) to nflverse's published mean, on
    player-weeks where our denominator > 0 and nflverse published a value."""
    out = []
    for label, num, den, nflc, tol in RATIO_PAIRS:
        row = con.execute(
            f"""
            WITH j AS (
                SELECT o.{q(num)} / o.{q(den)} AS ov, n.{q(nflc)} AS nv
                FROM ours o INNER JOIN nfl n ON o.player_id = n.player_id AND o.week = n.week
                WHERE o.{q(den)} > 0 AND n.{q(nflc)} IS NOT NULL
            )
            SELECT COUNT(*) AS universe,
                   COUNT(*) FILTER (WHERE ABS(ov - nv) <= {tol}) AS matched
            FROM j
            """
        ).fetchone()
        out.append((label, row[0], row[1]))
    return out


def build_cmp(con: duckdb.DuckDBPyConnection) -> None:
    cols = []
    for label, our_col, nfl_col, _tol, _agg in STAT_PAIRS:
        cols.append(f"COALESCE(o.{q(our_col)}, 0) AS {q('o_' + label)}")
        cols.append(f"COALESCE(n.{q(nfl_col)}, 0) AS {q('n_' + label)}")
    con.execute(
        f"""
        CREATE TEMP TABLE cmp AS
        SELECT
            COALESCE(o.player_id, n.player_id) AS player_id,
            COALESCE(o.week, n.week) AS week,
            COALESCE(n.player, o.player) AS player,
            (o.player_id IS NOT NULL) AS in_ours,
            (n.player_id IS NOT NULL) AS in_nfl,
            {", ".join(cols)}
        FROM ours o
        FULL OUTER JOIN nfl n
          ON o.player_id = n.player_id AND o.week = n.week
        """
    )


def scorecard(con: duckdb.DuckDBPyConnection) -> list[tuple]:
    selects = []
    for label, _our, _nfl, tol, _agg in STAT_PAIRS:
        o = q("o_" + label)
        n = q("n_" + label)
        universe = f"({o} <> 0 OR {n} <> 0)"
        matched = f"({universe} AND ABS({o} - {n}) <= {tol})"
        selects.append(
            f"""SELECT
                '{label}' AS stat,
                COUNT(*) FILTER (WHERE {universe}) AS universe,
                COUNT(*) FILTER (WHERE {matched}) AS matched,
                COUNT(*) FILTER (WHERE {universe} AND NOT ({matched})) AS mism,
                COUNT(*) FILTER (WHERE {universe} AND NOT in_nfl) AS ours_only,
                COUNT(*) FILTER (WHERE {universe} AND NOT in_ours) AS nfl_only
            FROM cmp"""
        )
    sql = "\nUNION ALL\n".join(selects)
    # Preserve declared order.
    order = {p[0]: i for i, p in enumerate(STAT_PAIRS)}
    rows = con.execute(sql).fetchall()
    return sorted(rows, key=lambda r: order[r[0]])


def examples(con: duckdb.DuckDBPyConnection, label: str, tol: float, limit: int = 8) -> list[tuple]:
    o = q("o_" + label)
    n = q("n_" + label)
    return con.execute(
        f"""
        SELECT player_id, player, week, {o} AS ours, {n} AS nflverse, ({o} - {n}) AS diff,
               in_ours, in_nfl
        FROM cmp
        WHERE ({o} <> 0 OR {n} <> 0) AND ABS({o} - {n}) > {tol}
        ORDER BY ABS({o} - {n}) DESC, player_id
        LIMIT {limit}
        """
    ).fetchall()


def validate_year(con: duckdb.DuckDBPyConnection, pbp: Path, bio: Path, year: int, show_examples: bool) -> dict:
    for tbl in ("bio_source", "bio_lookup", "pbp_base", "pbp_player_events", "pbp_player_week_rollup", "ours", "nfl", "cmp"):
        con.execute(f"DROP TABLE IF EXISTS {tbl}")

    create_bio_lookup(con, bio)
    create_pbp_base(con, pbp, year, year)
    create_weekly_rollup(con)
    build_ours(con)
    nfl_rows = build_nfl(con, year)
    build_cmp(con)
    rows = scorecard(con)

    print(f"\n{'=' * 78}")
    print(f"  YEAR {year}   (nflverse REG player-weeks: {nfl_rows:,})")
    print(f"{'=' * 78}")
    print(f"  {'stat':<26} {'match%':>8} {'univ':>7} {'mism':>6} {'ours_only':>10} {'nfl_only':>9}")
    print(f"  {'-' * 26} {'-' * 8} {'-' * 7} {'-' * 6} {'-' * 10} {'-' * 9}")

    below = []
    for stat, universe, matched, mism, ours_only, nfl_only in rows:
        pct = (matched / universe * 100.0) if universe else 100.0
        if stat in INFORMATIONAL:
            flag = "  (info: nflverse ref degenerate)"
        else:
            flag = "" if pct >= 99.95 else ("  <<" if pct < 99.0 else "  <")
        print(f"  {stat:<26} {pct:>7.2f}% {universe:>7,} {mism:>6,} {ours_only:>10,} {nfl_only:>9,}{flag}")
        if universe and pct < 99.95 and stat not in INFORMATIONAL:
            below.append((stat, pct))

    for label, universe, matched in ratio_scorecard(con):
        pct = (matched / universe * 100.0) if universe else 100.0
        flag = "" if pct >= 99.95 else ("  <<" if pct < 99.0 else "  <")
        print(f"  {label:<26} {pct:>7.2f}% {universe:>7,} {universe - matched:>6,} {'ratio':>10} {'':>9}{flag}")
        if universe and pct < 99.95:
            below.append((label, pct))

    for label, universe, matched in composite_scorecard(con):
        pct = (matched / universe * 100.0) if universe else 100.0
        flag = "" if pct >= 99.95 else ("  <<" if pct < 99.0 else "  <")
        print(f"  {label:<26} {pct:>7.2f}% {universe:>7,} {universe - matched:>6,} {'composite':>10} {'':>9}{flag}")
        if universe and pct < 99.95:
            below.append((label, pct))

    if show_examples and below:
        tol_by = {p[0]: p[3] for p in STAT_PAIRS}
        print(f"\n  --- mismatch examples (largest |diff| first) ---")
        for stat, pct in below:
            print(f"\n  [{stat}] {pct:.2f}%")
            for pid, player, wk, ours, nflv, diff, in_o, in_n in examples(con, stat, tol_by[stat]):
                tag = "both" if (in_o and in_n) else ("OURS-only" if in_o else "NFL-only")
                print(f"    wk{wk:>2}  {str(player)[:24]:<24} ours={ours:>7}  nfl={nflv:>7}  d={diff:>6}  [{tag}]")

    return {"year": year, "below": below, "rows": rows}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--years", type=str, default="2023", help="Comma list, e.g. 2023,2015,1999")
    p.add_argument("--year-range", type=str, default=None, help="Inclusive range, e.g. 1999-2023")
    p.add_argument("--pbp", type=Path, default=DEFAULT_PBP)
    p.add_argument("--bio", type=Path, default=None)
    p.add_argument("--no-examples", action="store_true", help="Suppress per-stat mismatch samples")
    p.add_argument("--threads", type=int, default=8)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.year_range:
        lo, hi = (int(x) for x in args.year_range.split("-"))
        years = list(range(lo, hi + 1))
    else:
        years = [int(y) for y in args.years.split(",") if y.strip()]

    pbp = args.pbp
    bio = args.bio or (DEFAULT_BIO_REPAIRED if DEFAULT_BIO_REPAIRED.exists() else DEFAULT_BIO)
    if not pbp.exists():
        raise FileNotFoundError(f"PBP not found: {pbp}")
    if not bio.exists():
        raise FileNotFoundError(f"Bio not found: {bio}")

    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"PRAGMA threads={max(1, args.threads)}")

    all_below: list[tuple[int, str, float]] = []
    for year in years:
        res = validate_year(con, pbp, bio, year, show_examples=not args.no_examples)
        for stat, pct in res["below"]:
            all_below.append((year, stat, pct))

    print(f"\n{'=' * 78}")
    if all_below:
        print(f"  STATS BELOW 99.95% ({len(all_below)}):")
        for year, stat, pct in all_below:
            print(f"    {year}  {stat:<26} {pct:.2f}%")
    else:
        print("  ALL STATS 100% (>=99.95%) ACROSS ALL YEARS.")
    print(f"{'=' * 78}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
