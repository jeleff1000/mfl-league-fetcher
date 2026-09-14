"""qa_sweep.py -- verify the 8 served pages end to end, in one command.

Joe 2026-07-20: "8 pages with 10-20ish columns each. We should be able to quickly verify
everything. How many cohorts we have, how populated each cohort is for each of the 8 pages
using our ladder, and the sample sizes needed for r of .75/.85/.95 ... every column's top 50
and bottom 50 all look normal."

Reads the SERVED bundle (research_merge_bundle.duckdb) -- what users actually see -- not the
source parquets. Emits:

  1. COHORT CENSUS      -- rows per page, per rung, per slug
  2. LADDER TABLE       -- r=0.75/0.85/0.95 sample sizes per metric class (ladder_thresholds.json)
  3. BOUNDS + EXCEPTIONS-- every served column checked against what it can physically be;
                           violations printed to console (this is the part you read)
  4. TOP/BOTTOM 50 DUMP -- every column, with player names, written to a report file

Console output is an EXCEPTION report by design: a clean sweep prints a short summary, so
anything printed is something to look at.

    py -3 scripts/research_cohorts/qa_sweep.py
    py -3 scripts/research_cohorts/qa_sweep.py --slug 12t_flx_ppr_4pt --top 50
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

from build_wide_bundle import BUNDLE_PATH, SLUGS, TABLES

OUT_DIR = Path(os.environ.get(
    "RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
OPS_CACHE = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb")
THRESHOLDS = OUT_DIR / "ladder_thresholds.json"
DEFAULT_SLUG = "12t_flx_ppr_4pt"

# What each served stat can PHYSICALLY be. A violation is a defect, not a taste question.
# (lo, hi, note) -- None means unbounded on that side.
BOUNDS: dict[str, tuple[float | None, float | None, str]] = {
    # rates are shares of a denominator that contains them
    "start_rate": (0, 100, "share of eligible league-weeks"),
    "healthy_start_rate": (0, 100, "share of snap/active-eligible league-weeks"),
    "draft_rate": (0, 100, "share of cohort leagues"),
    "add_rate":   (0, 100, "share of eligible leagues"),
    "won":        (0, 100, "start-weighted, <= start_rate"),
    "lost":       (0, 100, "start-weighted, <= start_rate"),
    "champ_wk":   (0, 100, "legacy: share of champ-eligible leagues"),
    # T7: one denominator -- leagues where the position is eligible. as-starter <= total.
    "champ_total":     (0, 100, "share of eligible leagues, on the champion's roster"),
    "champ_started":   (0, 100, "share of eligible leagues, in the title-game lineup"),
    "playoff_total":   (0, 100, "share of eligible leagues, on a playoff roster"),
    "playoff_started": (0, 100, "share of eligible leagues, started a playoff game"),
    "inactive_weeks":  (0, 600, "rostered but not on an NFL field"),
    "faab":       (0, 100, "% of budget, <=100 by identity"),
    # scoring / value
    "ppg":        (0, 70, "highest real NFL fantasy week ~61 PPR; 6pt+bonuses ~90 weekly"),
    "lamar":      (-100, 700, "canonical season LAMAR; Saquon 2024 = 309"),
    "add_lamar":  (-100, 700, "canonical ROS add value"),
    "drop_regret": (-100, 700, "canonical ROS value after the drop"),
    # clutch is now a per-eligible-LEAGUE season TOTAL of title-odds impact, not a per-week
    # average, so its scale is larger and not yet calibrated against a clean build. Bound is
    # deliberately loose -- it exists to catch corruption, not to police the metric.
    "clutch":     (-60, 60, "season total title-odds impact per eligible league"),
    # capital
    "adp":        (1, 3100, "32-team superflex drafts reach ~3,030 picks (ledger D9)"),
    "cost":       (0, None, "raw auction dollars; the comparable unit is cost_pct"),
    # counts are counts
    "n_leagues":  (0, None, ""), "n_drafted": (0, None, ""), "n_add": (0, None, ""),
    "n_drop": (0, None, ""), "n_add_lg": (0, None, ""), "started_weeks": (0, None, ""),
    "started_lg": (0, None, ""), "champ_elig": (0, None, ""), "n_years": (0, 30, ""),
}
# scores are absolute and UNCAPPED by ruling (2026-07-20) -- deliberately unbounded above,
# so they are reported but never flagged on magnitude alone
UNCAPPED = {"draft_score", "draft_score_healthy", "best_pick", "best_score", "title_run",
            "add_grade", "market_delta", "faab_bid"}

# The player's OWN sample for each page. Carried into every dump because an extreme value is
# only interpretable next to it: ledger D10 found 138 of the 221 |clutch|>8 rows came from
# players with a single started week. A tail made of 1-start players is a gating question,
# not a data defect.
SAMPLE_COL = {
    "research_matchup": "started_weeks", "research_matchup_career": "started_weeks",
    "research_matchup_weekly": "started_lg",
    "research_draft": "n_drafted", "research_draft_career": "n_drafted",
    "research_transactions": "n_add", "research_transactions_career": "n_add",
    "research_transactions_weekly": "n_add_lg",
}
# metrics whose tails are known to be sample-driven -> report the tail's sample profile
SAMPLE_SENSITIVE = {"clutch", "ppg", "lamar", "add_lamar", "drop_regret", "won", "lost"}


def player_names(con: duckdb.DuckDBPyConnection) -> None:
    """Names come from the bundle itself so the sweep matches what the pages render."""
    try:
        con.execute("SELECT 1 FROM research_player_names LIMIT 1")
    except duckdb.CatalogException:
        con.execute("CREATE TEMP VIEW research_player_names AS "
                    "SELECT NULL::VARCHAR NFL_player_id, NULL::VARCHAR player, "
                    "NULL::VARCHAR \"position\" WHERE false")


def cohort_census(con, tables, slug) -> list[str]:
    lines = ["", "=" * 78, "1. COHORT CENSUS -- rows per page per rung "
             f"(slug {slug}; rung 4 = fully stratified)", "=" * 78]
    lines.append(f"{'page':34s} {'rows':>9s} {'rung4':>8s} {'rung3':>8s} "
                 f"{'rung2':>8s} {'rung0':>8s} {'unserved':>9s}")
    for name, _ds, _src, _idc, laddered, _stats in tables:
        try:
            if laddered:
                r = con.execute(f"""SELECT COUNT(*),
                    COUNT(*) FILTER (WHERE level_{slug}=4), COUNT(*) FILTER (WHERE level_{slug}=3),
                    COUNT(*) FILTER (WHERE level_{slug}=2), COUNT(*) FILTER (WHERE level_{slug}=0),
                    COUNT(*) FILTER (WHERE level_{slug} IS NULL) FROM {name}""").fetchone()
            else:
                n = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
                r = (n, n, 0, 0, 0, 0)   # weekly pages are rung-4 only by construction
        except duckdb.CatalogException:
            lines.append(f"{name:34s} {'MISSING':>9s}")
            continue
        lines.append(f"{name:34s} {r[0]:>9,} {r[1]:>8,} {r[2]:>8,} {r[3]:>8,} {r[4]:>8,} {r[5]:>9,}")
    return lines


def ladder_table() -> list[str]:
    lines = ["", "=" * 78, "2. LADDER -- leagues needed per metric class", "=" * 78]
    if not THRESHOLDS.exists():
        lines.append(f"  MISSING {THRESHOLDS} -- run ladder_stab.py")
        return lines
    doc = json.loads(THRESHOLDS.read_text(encoding="utf-8"))
    tg = doc.get("targets", [])
    lines.append(f"  version {doc.get('version')}   targets {tg}")
    lines.append(f"  {'metric':16s} {'class':10s} " + " ".join(f"{'r'+str(int(t*100)):>10s}" for t in tg))
    for metric, res in sorted(doc.get("metrics", {}).items()):
        cells = " ".join(f"{(res.get(f'n_r{int(t*100)}') or '>grid'):>10}" for t in tg)
        lines.append(f"  {metric:16s} {res.get('class',''):10s} {cells}")
    return lines


def sweep_columns(con, tables, slug, topn, report) -> list[str]:
    """Bounds + exception report; full top/bottom dumps go to the report file."""
    problems: list[str] = []
    report.write(f"# QA sweep -- top/bottom {topn} per served column (slug {slug})\n")
    for name, _ds, _src, idc, _lad, stats in tables:
        try:
            describe = con.execute(f"DESCRIBE {name}").fetchall()
            have = {r[0] for r in describe}
            types = {r[0]: str(r[1]).upper() for r in describe}
        except duckdb.CatalogException:
            problems.append(f"  [{name}] TABLE MISSING from the bundle")
            continue
        idcols = [c for c in idc if c in have]
        for stat in sorted(stats):
            col = f"{stat}_{slug}"
            if col not in have:
                problems.append(f"  [{name}.{stat}] COLUMN MISSING ({col})")
                continue
            # Some served source/provenance columns are text (for example ADP source
            # labels).  They still need null/bounds coverage, but DuckDB cannot compute
            # a numeric median for them.  Keep the sweep running so one text column does
            # not prevent the matchup tables from being audited.
            is_numeric = any(t in types.get(col, "") for t in (
                "INT", "DECIMAL", "DOUBLE", "FLOAT", "REAL", "HUGEINT"))
            median_sql = f", QUANTILE_CONT({col},0.5)" if is_numeric else ", NULL"
            row = con.execute(f"""SELECT COUNT(*), COUNT({col}), MIN({col}), MAX({col})
                {median_sql} FROM {name}""").fetchone()
            n, nn, lo, hi, med = row
            if nn == 0:
                problems.append(f"  [{name}.{stat}] ALL NULL across {n:,} rows")
                continue
            scol = SAMPLE_COL.get(name)
            scol = f"{scol}_{slug}" if scol and f"{scol}_{slug}" in have else None
            extra_cols = [scol] if scol else []
            b = BOUNDS.get(stat)
            if b and stat not in UNCAPPED:
                blo, bhi, note = b
                if blo is not None and lo is not None and lo < blo - 1e-6:
                    problems.append(f"  [{name}.{stat}] MIN {lo:,.2f} < {blo} ({note})")
                if bhi is not None and hi is not None and hi > bhi + 1e-6:
                    problems.append(f"  [{name}.{stat}] MAX {hi:,.2f} > {bhi} ({note})")
            # Dump STRATIFIED by season-cohort (Joe 2026-07-20). Every served metric is a
            # within-cohort quantity -- clutch most obviously -- so a global ranking just
            # collects small-sample tails from every season at once and tells you nothing
            # about whether a season's board is sane. The column already fixes the cohort
            # (it is {stat}_{slug}), so partitioning by year gives the season-cohort combo.
            # Career tables have no year and are a single group per cohort by construction.
            sel = ", ".join(idcols + ["player", '"position"', col] + extra_cols)
            strat = "year" if "year" in have else None
            for label, order in (("TOP", "DESC"), ("BOTTOM", "ASC")):
                head = (f"\n## {name}.{stat} -- {label} {topn} per season-cohort"
                        if strat else f"\n## {name}.{stat} -- {label} {topn} (career: one group)")
                report.write(f"{head}  (n={nn:,}/{n:,}, min={lo}, median={med}, max={hi})\n")
                try:
                    if strat:
                        q = f"""WITH r AS (
                              SELECT {sel}, ROW_NUMBER() OVER (
                                PARTITION BY year ORDER BY {col} {order}) AS rk
                              FROM {name} LEFT JOIN research_player_names USING (NFL_player_id)
                              WHERE {col} IS NOT NULL)
                            SELECT * EXCLUDE (rk) FROM r WHERE rk <= {topn}
                            ORDER BY year DESC, {col} {order}"""
                    else:
                        q = f"""SELECT {sel} FROM {name}
                            LEFT JOIN research_player_names USING (NFL_player_id)
                            WHERE {col} IS NOT NULL ORDER BY {col} {order} LIMIT {topn}"""
                    report.write(con.execute(q).df().to_string(index=False) + "\n")
                except Exception as e:
                    report.write(f"  (dump failed: {str(e).splitlines()[0][:120]})\n")
            # Is this season-cohort's top 25 built on real usage, or on 1-start noise?
            # Reported as a WARNING, never a bound failure -- it is a gating decision.
            # RELATIVE test, not an absolute floor: "sample <= 1" misses the real failure.
            # In the 2025 clutch board Malik Willis (13 started league-weeks) outranked Puka
            # Nacua (3,099) -- nothing there is <=1, yet the board is still noise. Compare the
            # leaders' median sample against the cohort's own median instead.
            if stat in SAMPLE_SENSITIVE and scol and "year" in have:
                try:
                    r2 = con.execute(f"""WITH r AS (
                          SELECT year, {scol} AS smp, ROW_NUMBER() OVER (
                            PARTITION BY year ORDER BY {col} DESC) AS rk
                          FROM {name} WHERE {col} IS NOT NULL AND {scol} IS NOT NULL)
                        SELECT (SELECT MEDIAN(smp) FROM r WHERE rk <= {topn}),
                               (SELECT MEDIAN(smp) FROM r),
                               (SELECT MIN(smp) FROM r WHERE rk <= {topn})""").fetchone()
                    top_med, all_med, top_min = r2
                    # The GROUP median hides it -- the leaders as a set usually do out-rank
                    # the typical row. What matters is whether ANY leader is drawn from the
                    # thin end: Malik Willis at 13 league-weeks sitting above Puka Nacua at
                    # 3,099 is the failure, and the top-25 median stays healthy throughout.
                    if top_min is not None and all_med and top_min < 0.1 * all_med:
                        problems.append(
                            f"  [{name}.{stat}] THIN LEADER: a per-season top-{topn} entry has "
                            f"{SAMPLE_COL[name]}={top_min:,.0f} against a cohort median of "
                            f"{all_med:,.0f} (leaders' median {top_med:,.0f}) -- the board is "
                            f"ranking noise above usage; needs a player-own-sample gate")
                except Exception:
                    pass
    return problems


def position_checks(con, tables, slug) -> list[str]:
    """A flx board may only contain positions a flx league can start. Ledger D3 shipped
    without this gate and IDP polluted the boards; the 2025 clutch board still carries a
    punter and a DL, so it is checked here rather than trusted."""
    if "_flx_" not in slug:
        return []
    out: list[str] = []
    legal = "('QB','RB','WR','TE','K','DEF')"
    for name, _ds, _src, _idc, _lad, stats in tables:
        anchor = next((s for s in ("start_rate", "adp", "add_rate") if s in stats), None)
        if not anchor:
            continue
        col = f"{anchor}_{slug}"
        try:
            bad = con.execute(f"""SELECT COUNT(*), STRING_AGG(DISTINCT n."position", ',')
                FROM {name} t JOIN research_player_names n USING (NFL_player_id)
                WHERE t.{col} IS NOT NULL AND n."position" IS NOT NULL
                  AND n."position" NOT IN {legal}""").fetchone()
        except duckdb.CatalogException:
            continue
        if bad and bad[0]:
            out.append(f"  [{name}] POSITION LEAK: {bad[0]:,} served rows in a flx cohort "
                       f"carry non-flx positions ({bad[1]}) -- the flx position gate is not "
                       f"holding")
    return out


def identity_checks(con, slug) -> list[str]:
    """Relationships that must hold by construction, not by luck."""
    out: list[str] = []
    for tbl in ("research_matchup", "research_matchup_weekly", "research_matchup_career"):
        try:
            worst = con.execute(f"""SELECT MAX(ABS(won_{slug} + lost_{slug} - start_rate_{slug}))
                FROM {tbl} WHERE won_{slug} IS NOT NULL AND start_rate_{slug} IS NOT NULL""").fetchone()[0]
        except duckdb.CatalogException:
            continue
        if worst is not None and worst > 0.5:
            out.append(f"  [{tbl}] won+lost != start_rate (worst {worst:.3f}) "
                       f"-- the start-weighted record identity is broken")
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bundle", type=Path, default=BUNDLE_PATH)
    ap.add_argument("--slug", default=DEFAULT_SLUG, choices=SLUGS)
    ap.add_argument("--top", type=int, default=25,
                    help="rows per season-cohort in the dump (Joe: top 25 per season-cohort)")
    args = ap.parse_args()

    if not args.bundle.exists():
        raise SystemExit(f"bundle not found: {args.bundle} (run the cycle first)")
    con = duckdb.connect(str(args.bundle), read_only=True)
    player_names(con)

    print("\n".join(cohort_census(con, TABLES, args.slug)))
    print("\n".join(ladder_table()))

    rpt_path = OUT_DIR / "qa_sweep_report.txt"
    with open(rpt_path, "w", encoding="utf-8") as rpt:
        problems = sweep_columns(con, TABLES, args.slug, args.top, rpt)
    problems += position_checks(con, TABLES, args.slug)
    problems += identity_checks(con, args.slug)

    print("\n" + "=" * 78)
    print("3. EXCEPTIONS -- anything printed here needs a look")
    print("=" * 78)
    if problems:
        for p in problems:
            print(p)
        print(f"\n  {len(problems)} exception(s).")
    else:
        print("  none -- every served column is inside its physical bounds.")
    print(f"\n4. TOP/BOTTOM {args.top} dump for every column -> {rpt_path}")
    con.close()


if __name__ == "__main__":
    main()
