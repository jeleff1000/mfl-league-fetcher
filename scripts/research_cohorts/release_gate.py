"""Release gate + build manifest for the research merge bundle (handoff 2026-07-18 §2.9).

Boards are HUMAN SMOKE CHECKS only -- this is the real gate, run BEFORE any /merge-ops or
the coupled frontend deploy:
  * hard value bounds (impossible-movement checks) per metric family;
  * won+lost = expected starts; unknown/tied starts fail closed instead of
    silently disappearing from the W-L identity;
  * no fully-hollow identity rows;
  * per-table row counts vs the previous bundle;
  * top-K board diffs vs the previous bundle (overlap, entrants, leavers);
  * a build manifest: corpus fingerprint, main SHA, builder file hashes, threshold table.

Exit code 0 = all gates pass; 1 = at least one FAIL. Writes the report next to the bundle
as release_gate_report.json either way.
"""
from __future__ import annotations

import hashlib
import json
import argparse
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from build_wide_bundle import (
    ADAPTIVE_TABLES, BUNDLE_PATH, MATCHUP_BRACKETS, MIN_STABLE, SLUGS, TABLES,
)

REPO = Path(__file__).resolve().parents[2]
_configured_corpus = Path(os.environ.get(
    "RESEARCH_CORPUS_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb"))
CORPUS = (_configured_corpus if _configured_corpus.exists() else Path(os.environ.get(
    "RESEARCH_SNAPSHOT_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb")))
BUILDERS = ["build_research_matchup_cohort.py", "build_research_draft_cohort.py",
            "build_research_txn_cohort.py", "build_research_cohort_grades.py",
            "build_research_career_rollup.py", "build_weekly_matchup.py",
            "build_weekly_txn.py", "build_wide_bundle.py"]

# Hard physical bounds per served metric family. A value outside its bound is impossible
# under ANY sane flx league -- one poison row fails the release. Scales differ by grain:
# matchup lamar/ppg are PER-STARTED-WEEK; draft/txn lamar are SEASON TOTALS (legit to ~±350,
# LT-2006-style); adp is a raw pick number and dynasty-startup drafts pooled at coarse rungs
# reach 1,500+ picks (that's the §0.6 unit-mixing question, not corruption -- bound loosely).
BOUNDS = {
    "ppg": (-10.0, 60.0), "start_rate": (0.0, 100.0),
    "healthy_start_rate": (0.0, 100.0), "won": (0.0, 100.0),
    "lost": (0.0, 100.0), "win_rate": (0.0, 100.0),
    "champ_wk": (0.0, 100.0), "playoff": (0.0, 100.0),
    # T7 served lanes: rates over ELIGIBLE leagues, so <=100 by construction. The gate is
    # what turns a denominator regression into a failed release instead of a live board.
    "champ_total": (0.0, 100.0), "champ_started": (0.0, 100.0),
    "playoff_total": (0.0, 100.0), "playoff_started": (0.0, 100.0),
    # T8 availability counts: a season is at most ~22 weeks; career pools multiple seasons.
    "active_weeks": (0.0, 600.0), "inactive_weeks": (0.0, 600.0),
    "expected_wins": (0.0, 500.0), "expected_losses": (0.0, 500.0),
    "expected_starts": (0.0, 500.0), "expected_champs": (0.0, 100.0),
    "expected_playoffs": (0.0, 100.0),
    # clutch was REDEFINED 2026-07-20 (Joe): per-ELIGIBLE-LEAGUE season TOTAL of title-odds
    # impact, not a per-week average -- a legitimately larger scale (flx max 32.5, all-cohort
    # 59.9 on the clean rebuild; 475/477 |clutch|>8 flx rows have 9+ started weeks, i.e. real
    # sample, not thin-cohort noise). The old +/-8 was the pre-redefinition per-week bound and
    # would false-fail 477 flx rows. Matches qa_sweep's (-60,60) "catch corruption, not police
    # the metric" bound. Thin cohorts are handled by the confidence label, not this bound.
    "clutch": (-60.0, 60.0),
    "lamar": (-40.0, 40.0), "draft_rate": (0.0, 100.0), "add_rate": (0.0, 100.0),
    "faab": (0.0, 100.0), "adp": (0.5, 2000.0),
    # points = canonical season fantasy TOTAL from the super table. Its floor is NOT 0: a
    # drafted low-usage player who netted negative (INTs, fumbles, missed FGs, sacks) has a
    # small negative season total -- measured min -3.24 on the clean rebuild, 0 rows below -50.
    # The old 0.0 floor false-failed 24 draft/draft_career point lanes. -50 still catches the
    # 1e10-scale scoring poison it exists to catch.
    "points": (-50.0, 800.0),
}
# per-(table, stat) overrides where the family scale differs from the default above
TABLE_BOUNDS = {
    ("research_draft", "lamar"): (-600.0, 600.0),
    # Career draft points/LAMAR are whole-career canonical sums, not season values.
    ("research_draft_career", "points"): (-500.0, 20000.0),
    ("research_draft_career", "lamar"): (-5000.0, 10000.0),
    ("research_transactions", "add_lamar"): (-600.0, 600.0),
    ("research_transactions_career", "add_lamar"): (-600.0, 600.0),
    # Weekly canonical scoring legitimately reaches 60.28 in 6-point passing formats;
    # weekly LAMAR reaches 57.0 on elite outlier games in the clean public pilot.
    ("research_matchup_weekly", "ppg"): (-10.0, 70.0),
    ("research_matchup_weekly", "lamar"): (-35.0, 65.0),
    ("research_matchup_weekly", "expected_wins"): (0.0, 1.0),
    ("research_matchup_weekly", "expected_losses"): (0.0, 1.0),
    ("research_matchup_weekly", "expected_starts"): (0.0, 1.0),
    # Weekly clutch is intentionally volatile in thin IDP cells; season/career remain at
    # +/-60.  This catches a scale regression (e.g. 2,341 instead of 23.41) without rejecting
    # legitimate one-week leverage outliers.
    ("research_matchup_weekly", "clutch"): (-100.0, 100.0),
    # Matchup points are canonical scoring variants: weekly values include historical IDP
    # scoring, season values are population-observed totals, and career values sum seasons.
    ("research_matchup_weekly", "points"): (-1000.0, 10000.0),
    ("research_matchup", "points"): (-10000.0, 250000.0),
    # One expected start per active NFL week; 18 is the physical season ceiling and
    # accommodates the 17-game era without weakening the corruption gate to career scale.
    ("research_matchup", "expected_wins"): (0.0, 18.0),
    ("research_matchup", "expected_losses"): (0.0, 18.0),
    ("research_matchup", "expected_starts"): (0.0, 18.0),
    ("research_matchup", "lamar"): (-500.0, 800.0),
    ("research_matchup_career", "lamar"): (-5000.0, 10000.0),
    ("research_matchup_career", "points"): (-500.0, 50000.0),
    # Career clutch is a champion-population-weighted average of season clutch values,
    # not a career sum.  Keep the same corruption bound as weekly/season clutch so a
    # regression back to SUM(season averages) cannot pass the release gate.
    ("research_matchup_career", "clutch"): (-60.0, 60.0),
}
TOPK = 25
# board metric per table for the top-K diff
BOARD_METRIC = {"research_draft": "draft_score", "research_draft_career": "draft_score",
                "research_transactions": "title_run", "research_transactions_career": "title_run",
                "research_matchup": "lamar", "research_matchup_career": "lamar"}
BOARD_SLUG = "12t_flx_ppr_4pt"


def bundle_table_specs():
    """Yield every published table, including one ordinary-width matchup table per bracket."""
    for spec in TABLES:
        name, dataset, *_ = spec
        if dataset != "matchup":
            yield spec
            continue
        for bracket in MATCHUP_BRACKETS:
            published = name if bracket == "6po" else f"{name}_{bracket}"
            yield (published, *spec[1:])


def metric_bounds(table: str, stat: str) -> tuple[float, float]:
    """Return the physical sanity bounds for a metric at its actual table grain."""
    base_table = next((table.removesuffix(f"_{bracket}") for bracket in MATCHUP_BRACKETS
                       if bracket != "6po" and table.endswith(f"_{bracket}")), table)
    override = TABLE_BOUNDS.get((base_table, stat))
    return override if override is not None else BOUNDS[stat]


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _git_sha() -> str:
    try:
        return subprocess.run(["git", "-C", str(REPO), "rev-parse", "HEAD"],
                              capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return "unknown"


def _corpus_fingerprint() -> dict:
    """Cheap corpus identity: size+mtime+row counts (a full SHA of a multi-GB DB is slow and
    the lake is append-only between releases -- counts move when content moves)."""
    st = CORPUS.stat()
    fp = {"path": str(CORPUS), "bytes": st.st_size,
          "mtime_utc": datetime.fromtimestamp(st.st_mtime, tz=timezone.utc).isoformat()}
    try:
        con = duckdb.connect(str(CORPUS), read_only=True)
        fp["league_year_settings_rows"] = con.execute(
            "SELECT COUNT(*) FROM public.league_settings").fetchone()[0]
        fp["player_fantasy_rows"] = con.execute(
            "SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
        con.close()
    except Exception as exc:  # lake locked by a writer -- fingerprint stays partial, loudly
        fp["row_counts"] = f"unavailable: {exc}"
    return fp


def matchup_identity_violations(
    con: duckdb.DuckDBPyConnection,
    table: str,
    slug: str,
    *,
    weekly: bool,
    year: int | None = None,
) -> dict[str, int]:
    """Return violations of the expected-record identities for one matchup slug."""
    have = {row[1] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    expected = f"expected_wins_{slug}"
    lost = f"expected_losses_{slug}"
    starts = f"expected_starts_{slug}"
    start_rate = f"start_rate_{slug}"
    win_rate = f"win_rate_{slug}"
    if not {expected, lost, starts, start_rate}.issubset(have):
        return {}

    scope = f" AND year = {year}" if year is not None and "year" in have else ""

    errors: dict[str, int] = {}
    # Every expected start must have a confirmed W or L.  A one-sided bound
    # allows unresolved starts to disappear from both expected-outcome lanes,
    # which is exactly the missing-outcome bug this gate is meant to catch.
    record_missing = con.execute(f'''SELECT COUNT(*) FROM {table}
      WHERE "{starts}" IS NOT NULL AND "{starts}" > 0{scope}
        AND ("{expected}" IS NULL OR "{lost}" IS NULL
             OR NOT isfinite(CAST("{starts}" AS DOUBLE))
             OR NOT isfinite(CAST("{expected}" AS DOUBLE))
             OR NOT isfinite(CAST("{lost}" AS DOUBLE)))''').fetchone()[0]
    if record_missing:
        errors["expected_record_missing"] = record_missing
    record_bad = con.execute(f'''SELECT COUNT(*) FROM {table}
       WHERE "{expected}" IS NOT NULL AND "{lost}" IS NOT NULL AND "{starts}" IS NOT NULL{scope}
         AND isfinite(CAST("{expected}" AS DOUBLE))
         AND isfinite(CAST("{lost}" AS DOUBLE))
         AND isfinite(CAST("{starts}" AS DOUBLE))
         AND ABS("{expected}" + "{lost}" - "{starts}") > 0.0002''').fetchone()[0]
    if record_bad:
        errors["expected_record"] = record_bad

    if weekly:
        start_bad = con.execute(f'''SELECT COUNT(*) FROM {table}
          WHERE "{starts}" IS NOT NULL AND "{start_rate}" IS NOT NULL{scope}
            AND ABS("{starts}" - "{start_rate}" / 100.0) > 0.0002''').fetchone()[0]
        if start_bad:
            errors["weekly_start_rate"] = start_bad

    # Displayed Win% is wins in starts, i.e. expected wins divided by expected
    # starts.  This is intentionally checked at every grain; using raw decided
    # win/loss counts at season or career grain changes the weighting when weekly
    # league support differs.
    if {win_rate, expected, starts} <= have:
        win_bad = con.execute(f'''SELECT COUNT(*) FROM {table}
          WHERE "{win_rate}" IS NOT NULL AND "{expected}" IS NOT NULL{scope}
            AND "{starts}" > 0
            AND ABS("{win_rate}" - 100.0 * "{expected}" / "{starts}") > 0.0002''').fetchone()[0]
        if win_bad:
            errors["win_rate_expected_starts"] = win_bad
    return errors


def adaptive_table_violations(
    con: duckdb.DuckDBPyConnection, table: str
) -> dict[str, int]:
    """Validate the one-row-per-request/player adaptive serving surface."""
    have = {row[1] for row in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
    request = {
        "q_teams", "q_roster", "q_ppr", "q_td", "q_bracket",
        "q_league_type", "q_lineup_mode", "NFL_player_id", "pos_grp",
    }
    if not request.issubset(have):
        return {"missing_request_columns": 1}
    identity = ["q_teams", "q_roster", "q_ppr", "q_td", "q_bracket",
                "q_league_type", "q_lineup_mode", "NFL_player_id", "pos_grp"]
    if "year" in have:
        identity.append("year")
    if "week" in have:
        identity.append("week")
    key = ", ".join(f'"{column}"' for column in identity)
    duplicate = con.execute(
        f"SELECT COUNT(*) FROM (SELECT {key}, COUNT(*) AS n FROM {table} "
        f"GROUP BY {key} HAVING COUNT(*) > 1)"
    ).fetchone()[0]
    missing_request = con.execute(
        f"SELECT COUNT(*) FROM {table} WHERE "
        + " OR ".join(f'"{column}" IS NULL' for column in request)
    ).fetchone()[0]
    out: dict[str, int] = {}
    if duplicate:
        out["duplicate_request_player"] = duplicate
    if missing_request:
        out["null_request_key"] = missing_request
    return out


def run_gate(bundle_path: Path = BUNDLE_PATH, matchup_only: bool = False,
             replacement_year: int | None = None) -> int:
    previous = bundle_path.with_suffix(".previous.duckdb")
    con = duckdb.connect(str(bundle_path), read_only=True)
    prev = duckdb.connect(str(previous), read_only=True) if previous.exists() else None
    failures: list[str] = []
    report: dict = {"generated_utc": datetime.now(timezone.utc).isoformat(),
                    "bundle": str(bundle_path), "checks": {}}

    def _cols(c: duckdb.DuckDBPyConnection, table: str) -> set[str]:
        return {r[1] for r in c.execute(f"PRAGMA table_info('{table}')").fetchall()}

    def _table_names(c: duckdb.DuckDBPyConnection) -> set[str]:
        return {r[0] for r in c.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main'"
        ).fetchall()}

    all_specs = list(bundle_table_specs())
    table_specs = [s for s in all_specs if s[1] == "matchup"] if matchup_only else all_specs
    table_names = {s[0] for s in table_specs}
    present_tables = _table_names(con)
    semantic_specs = [s for s in table_specs
                      if replacement_year is None or "year" in s[3]]

    # ---- 1. hard bounds ----
    bounds_out = {}
    for name, dataset, _src, idc, _lad, stats in semantic_specs:
        have = _cols(con, name)
        for stat in stats:
            if (name, stat) not in TABLE_BOUNDS and stat not in BOUNDS:
                continue
            lo, hi = metric_bounds(name, stat)
            for s in SLUGS:
                col = f"{stat}_{s}"
                if col not in have:
                    continue
                scope = f" WHERE year = {replacement_year}" if replacement_year is not None else ""
                # Permit harmless IEEE rounding at a hard boundary (for example
                # 100.00000000000001 or -1e-17), while still rejecting real excursions.
                eps = 1e-9
                row = con.execute(
                    f'SELECT MIN("{col}"), MAX("{col}"), COUNT(*) FILTER '
                    f'(NOT isfinite(CAST("{col}" AS DOUBLE)) OR "{col}" < {lo - eps} OR "{col}" > {hi + eps}) '
                    f"FROM {name}{scope}").fetchone()
                if row[2]:
                    failures.append(f"BOUNDS {name}.{col}: {row[2]} rows outside [{lo},{hi}] "
                                    f"(min={row[0]}, max={row[1]})")
                    bounds_out[f"{name}.{col}"] = {"min": row[0], "max": row[1], "violations": row[2]}
    report["checks"]["bounds_violations"] = bounds_out

    # The old display-lane check `won + lost = start%` was removed.  Those lanes are rounded
    # eligible-league display percentages and are not the authoritative outcome record.  The
    # current contract is checked below from additive expected_wins/expected_losses/
    # expected_starts primitives at weekly, season, and career grain.
    report["checks"]["record_identity_violations"] = {}

    # ---- 2b. explicit expected-record identities ----
    expected_out = {}
    for name in ("research_matchup", "research_matchup_weekly", "research_matchup_career",
                 "research_matchup_4po", "research_matchup_8po",
                 "research_matchup_weekly_4po", "research_matchup_weekly_8po",
                 "research_matchup_career_4po", "research_matchup_career_8po"):
        if name not in table_names:
            continue
        if replacement_year is not None and "year" not in _cols(con, name):
            continue
        for slug in SLUGS:
            violations = matchup_identity_violations(
                con, name, slug, weekly=name == "research_matchup_weekly",
                year=replacement_year,
            )
            if violations:
                expected_out[f"{name}.{slug}"] = violations
                for identity, count in violations.items():
                    failures.append(
                        f"EXPECTED_RECORD {name} slug {slug} {identity}: {count} rows"
                    )
    report["checks"]["expected_record_identity_violations"] = expected_out

    # Adaptive tables are deliberately narrow and do not carry the legacy slug
    # columns. Validate their request lattice separately so a missing fallback or
    # duplicate request cell cannot slip through the legacy gate.
    adaptive_out = {}
    for name in ADAPTIVE_TABLES:
        if name not in present_tables:
            failures.append(f"MISSING adaptive table: {name}")
            continue
        violations = adaptive_table_violations(con, name)
        if violations:
            adaptive_out[name] = violations
            for identity, count in violations.items():
                failures.append(f"ADAPTIVE {name} {identity}: {count}")
    report["checks"]["adaptive_table_violations"] = adaptive_out

    # ---- 3. hollow rows ----
    hollow_out = {}
    for name, _ds, _src, idc, _lad, stats in table_specs:
        have = _cols(con, name)
        conds = " AND ".join(f"{st}_{s} IS NULL" for s in SLUGS for st in stats
                             if f"{st}_{s}" in have)
        scope = f" WHERE year = {replacement_year}" if replacement_year is not None and "year" in have else ""
        n = con.execute(f"SELECT COUNT(*) FILTER ({conds}) FROM {name}{scope}").fetchone()[0]
        hollow_out[name] = n
        if n:
            failures.append(f"HOLLOW {name}: {n} rows with no value in any slug")
    report["checks"]["hollow_rows"] = hollow_out

    # ---- 4. row counts vs previous ----
    counts = {}
    for name, *_ in table_specs:
        cur = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        old = None
        if prev is not None:
            try:
                old = prev.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            except duckdb.CatalogException:
                pass
        counts[name] = {"rows": cur, "previous": old,
                        "pct_change": (round(100.0 * (cur - old) / old, 1) if old else None)}
        if cur == 0:
            failures.append(f"EMPTY {name}: 0 rows")
    report["checks"]["row_counts"] = counts

    # ---- 4b. research historical scope ----
    excluded_out = {}
    for name, _ds, _src, idc, _lad, _stats in table_specs:
        if "year" not in idc:
            continue
        n_bad = con.execute(f"SELECT COUNT(*) FROM {name} WHERE year BETWEEN 1999 AND 2002").fetchone()[0]
        if n_bad:
            failures.append(f"EXCLUDED_YEARS {name}: {n_bad} rows from 1999-2002")
            excluded_out[name] = n_bad
    report["checks"]["excluded_year_rows"] = excluded_out

    # ---- 5. top-K board diffs vs previous ----
    boards = {}
    if prev is not None:
        board_tables = dict(BOARD_METRIC)
        for name, metric in list(BOARD_METRIC.items()):
            if name not in table_names:
                continue
            if name.startswith("research_matchup"):
                for bracket in ("4po", "8po"):
                    board_tables[f"{name}_{bracket}"] = metric
        for name, metric in board_tables.items():
            col = f"{metric}_{BOARD_SLUG}"
            q = (f'SELECT NFL_player_id FROM {name} WHERE "{col}" IS NOT NULL '
                 f'ORDER BY "{col}" DESC LIMIT {TOPK}')
            try:
                new_ids = [r[0] for r in con.execute(q).fetchall()]
                old_ids = [r[0] for r in prev.execute(q).fetchall()]
            except duckdb.Error as exc:
                boards[name] = {"error": str(exc)}
                continue
            overlap = len(set(new_ids) & set(old_ids))
            boards[name] = {"metric": col, "overlap": overlap, "k": TOPK,
                            "entrants": sorted(set(new_ids) - set(old_ids)),
                            "leavers": sorted(set(old_ids) - set(new_ids))}
    report["checks"]["topk_boards"] = boards

    # ---- manifest ----
    report["manifest"] = {
        "main_sha": _git_sha(),
        "corpus_fingerprint": _corpus_fingerprint(),
        "min_stable_thresholds": MIN_STABLE,
        "builder_sha256": {b: _sha256(Path(__file__).parent / b)
                          for b in BUILDERS if (Path(__file__).parent / b).exists()},
        "bundle_bytes": bundle_path.stat().st_size,
    }

    report["failures"] = failures
    report["pass"] = not failures
    out = bundle_path.parent / "release_gate_report.json"
    out.write_text(json.dumps(report, indent=2, default=str))
    con.close()
    if prev is not None:
        prev.close()

    print(f"[release-gate] {'PASS' if not failures else 'FAIL'} -> {out}")
    for f in failures[:40]:
        print("  FAIL:", f)
    if len(failures) > 40:
        print(f"  ... and {len(failures) - 40} more")
    for name, c in counts.items():
        print(f"  rows {name}: {c['rows']:,}"
              + (f" (prev {c['previous']:,}, {c['pct_change']:+}%)" if c["previous"] else ""))
    return 0 if not failures else 1


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--matchup-only", action="store_true")
    ap.add_argument("--replacement-year", type=int, default=None)
    ap.add_argument("--bundle", type=Path, default=BUNDLE_PATH)
    ns = ap.parse_args()
    sys.exit(run_gate(ns.bundle, matchup_only=ns.matchup_only,
                      replacement_year=ns.replacement_year))
