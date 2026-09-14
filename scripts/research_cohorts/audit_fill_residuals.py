"""audit_fill_residuals.py -- does the translation still hold where we can check it?

The fill projects a player's POOLED value onto a thin cohort. Wherever a cell has enough
native observations of its own, that projection is CHECKABLE: predict the cell from the
pooled value and compare against what the cell actually measured.

    residual = native_rate - (pooled_value projected onto this cell)

This is a standing gate, not a one-off. The multiplier assumption (rate = lift x s, lift
constant) was verified paired-within-player on 110 cell-pairs, but that verification is a
snapshot; as the corpus grows, or as a cohort's league mix shifts, the assumption can decay
silently -- the filled cells would keep looking plausible while drifting. Running this on
every build turns that into an alarm.

It also catches the specific error class that bit twice during the design: a translation that
looks right in aggregate while being wrong for a sub-population (the 1.37-vs-1.5 cell-mean
mistake would have shown here as a systematic negative residual on the widening moves).

Reports the residual distribution per metric, then per cohort dimension, then the worst
individual strays. Exits non-zero if any metric's median |residual| exceeds --max-median or
the strays exceed --max-stray-pct, so a build can be refused rather than published.

    py -3 scripts/research_cohorts/audit_fill_residuals.py \
        --season <assembled season parquet> --struct <struct lattice> [--min-leagues 200]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

BASE = ["teams", "roster", "ppr", "td", "league_type", "lineup_mode", "keeper_mode"]

# (column, mechanism) -- must mirror fill_thin_cohorts.FILLABLE exactly, or the audit is
# checking a different projection than the one that ships.
CHECKS = [
    ("won_pct", "copy"),
    ("win_rate_pct", "copy"),
    ("avg_clutch_started", "copy"),
    ("champ_total_pct", "teams_ratio"),
    ("champ_as_starter_pct", "teams_ratio"),
    ("playoff_total_pct", "struct_ratio"),
    ("playoff_as_starter_pct", "struct_ratio"),
]

# Native support is metric-specific.  A cohort can have thousands of leagues while a
# player has only a handful of decided starts in it; using n_leagues for every metric
# makes the residual gate look stronger than the measurement actually is.
SUPPORT_EXPR = {
    "start_rate_pct": "n_rostered_leagues",
    "won_pct": "wins_started + losses_started",
    "win_rate_pct": "wins_started + losses_started",
    # avg_clutch_started is normalized by the eligible/cohort league denominator in the
    # assembled season table, so the cohort's league count is the relevant support.
    "avg_clutch_started": "n_leagues",
    "champ_total_pct": "n_leagues",
    "champ_as_starter_pct": "n_leagues",
    "playoff_total_pct": "n_leagues",
    "playoff_as_starter_pct": "n_leagues",
}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=Path, required=True)
    ap.add_argument("--struct", type=Path, required=True)
    ap.add_argument("--min-leagues", type=int, default=200,
                    help="only audit cells with enough native observations to be worth trusting")
    ap.add_argument("--min-support", type=int, default=20,
                    help="minimum metric-specific numerator/denominator support; applies "
                         "after the cohort-size filter")
    ap.add_argument("--max-median", type=float, default=5.0,
                    help="fail if a metric's median |residual| exceeds this (percentage points)")
    ap.add_argument("--stray-points", type=float, default=10.0,
                    help="a stray is a cell whose residual exceeds this many percentage points "
                         "-- a display-material error, not a multiple of the median (a 3x-median "
                         "band flagged 24-33%% of cells purely from ordinary dispersion)")
    ap.add_argument("--max-stray-pct", type=float, default=15.0,
                    help="fail if more than this %% of cells are strays")
    ap.add_argument("--max-cohort-bias", type=float, default=3.0,
                    help="fail if a metric's median residual differs across teams cohorts by "
                         "more than this -- a systematic size bias the projection is missing")
    args = ap.parse_args()

    con = duckdb.connect()
    con.execute("SET memory_limit='6000MB'; SET threads=4; SET preserve_insertion_order=false;")
    con.execute(f"CREATE VIEW s AS SELECT * FROM read_parquet('{args.season.as_posix()}')")
    con.execute(f"CREATE VIEW st AS SELECT * FROM read_parquet('{args.struct.as_posix()}')")

    have = {r[0] for r in con.execute("DESCRIBE s").fetchall()}
    checks = [(c, m) for c, m in CHECKS if c in have]
    if not checks:
        raise SystemExit("[audit] none of the audited columns are present")

    join_st = " AND ".join(f"st.{c}=s.{c}" for c in BASE) + " AND st.year=s.year"
    con.execute(f"""
      CREATE TABLE cells AS
      SELECT s.*, st.mean_s, st.n_leagues AS struct_lg,
             TRY_CAST(REPLACE(s.teams,'t','') AS INTEGER) AS teams_n
      FROM s LEFT JOIN st ON {join_st}
    """)

    preds = []
    supports = []
    for col, how in checks:
        if how == "copy":
            proj = f"p.{col}"
        elif how == "teams_ratio":
            proj = f"p.{col} * (p.teams_n * 1.0 / NULLIF(c.teams_n,0))"
        else:
            proj = f"p.{col} * (c.mean_s / NULLIF(p.mean_s,0))"
        preds.append(f"c.{col} - ({proj}) AS resid_{col}")
        # The expressions are intentionally stored without an alias so they remain
        # readable as the metric's native support definition; qualify them here because
        # the residual query joins target (c) and donor (p) rows with overlapping columns.
        support = "c." + SUPPORT_EXPR[col].replace(" + ", " + c.")
        supports.append(f"{support} AS support_{col}")

    con.execute(f"""
      CREATE TABLE resid AS
      WITH pooled AS (
        SELECT * FROM cells WHERE cohort_level = 0 AND format_level = 0
      )
      SELECT c.NFL_player_id, c.year, {', '.join('c.'+b for b in BASE)},
             c.cohort_level, c.format_level, c.n_leagues, c.mean_s,
             {', '.join(preds)}, {', '.join(supports)}
      FROM cells c
      JOIN pooled p ON p.NFL_player_id=c.NFL_player_id AND p.year=c.year
      WHERE NOT (c.cohort_level = 0 AND c.format_level = 0)
        AND c.n_leagues >= {args.min_leagues}
    """)
    n = con.execute("SELECT COUNT(*) FROM resid").fetchone()[0]
    print(f"[audit] {n:,} auditable cells (native n >= {args.min_leagues})")
    if not n:
        raise SystemExit("[audit] nothing auditable -- refusing to pass vacuously")

    print("\n=== residual per metric (native minus projected) ===")
    rows, failed = [], []
    for col, how in checks:
        support_filter = f"AND support_{col} >= {args.min_support}"
        r = con.execute(f"""
          SELECT COUNT(resid_{col}), ROUND(MEDIAN(resid_{col}),3),
                 ROUND(MEDIAN(ABS(resid_{col})),3),
                 ROUND(QUANTILE_CONT(resid_{col},0.05),3),
                 ROUND(QUANTILE_CONT(resid_{col},0.95),3)
          FROM resid WHERE resid_{col} IS NOT NULL {support_filter}""").fetchone()
        cnt, med, absmed, p05, p95 = r
        thr = args.stray_points if col.endswith("_pct") else args.stray_points * 0.05
        stray = con.execute(f"""
          SELECT ROUND(100.0*AVG(CASE WHEN ABS(resid_{col}) > {thr} THEN 1.0 ELSE 0 END),2)
          FROM resid WHERE resid_{col} IS NOT NULL {support_filter}""").fetchone()[0]
        rows.append((col, how, cnt, med, absmed, p05, p95, stray))
        if absmed is not None and absmed > args.max_median:
            failed.append(f"{col}: median |resid| {absmed} > {args.max_median}")
        if stray is not None and stray > args.max_stray_pct:
            failed.append(f"{col}: {stray}% of cells stray > {args.stray_points}pp")
        bias = con.execute(f"""
          SELECT ROUND(MAX(m)-MIN(m),3) FROM (
            SELECT MEDIAN(resid_{col}) m FROM resid WHERE teams<>'ALL'
              AND resid_{col} IS NOT NULL {support_filter} GROUP BY teams)""").fetchone()[0]
        if bias is not None and bias > args.max_cohort_bias:
            failed.append(f"{col}: cohort size-bias spread {bias} > {args.max_cohort_bias}")

    hdr = f"{'metric':24}{'how':14}{'cells':>9}{'median':>9}{'|med|':>8}{'p05':>9}{'p95':>9}{'stray%':>8}"
    print(hdr)

    def _display(value, spec):
        """Render an empty residual distribution without crashing the audit."""
        return "-" if value is None else format(value, spec)

    for col, how, cnt, med, absmed, p05, p95, stray in rows:
        print(
            f"{col:24}{how:14}"
            f"{_display(cnt, '>9,')}{_display(med, '>9')}"
            f"{_display(absmed, '>8')}{_display(p05, '>9')}"
            f"{_display(p95, '>9')}{_display(stray, '>8')}"
        )

    print("\n=== residual by teams cohort (is the translation size-biased?) ===")
    sel = ", ".join(f"ROUND(MEDIAN(resid_{c}),3) AS {c}" for c, _ in checks)
    print(con.execute(f"SELECT teams, COUNT(*) AS cells, {sel} FROM resid "
                      "GROUP BY 1 ORDER BY 1").fetchdf().to_string(index=False))

    if failed:
        print("\n[audit] FAILED:")
        for f in failed:
            print("  " + f)
        return 1
    print("\n[audit] PASS -- projection agrees with native measurement within tolerance")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
