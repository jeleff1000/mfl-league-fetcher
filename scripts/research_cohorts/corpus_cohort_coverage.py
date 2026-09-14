"""corpus_cohort_coverage.py -- what we HAVE, what we could still GRAB, per cohort and platform.

Answers the three questions that decide where the crawl fleet points next:

  discovered  league-years the platform has told us exist (seeds / inventory plans)
  imported    league-years actually folded into the corpus lake
  net         discovered - imported, i.e. the pool still available to grab

...stratified by platform (yahoo / mfl / fleaflicker / sleeper), and joined to
two things that decide whether a cohort is USEFUL rather than merely populated:

  ADP       does external market ADP exist for that season+size? (nfl_market_adp)
  ladder    does the cohort's imported count clear the sample-size thresholds in
            ladder_thresholds.json (n_r75 / n_r85 / n_r95)? A cohort is
            "satisfied" for a metric once it clears that metric's n.

Discovery caveat, stated in the output rather than hidden: Yahoo inventory is
enumerated WITHOUT a settings fetch (seasons are independent; see
SeasonEnumerationAdapter), so a Yahoo league-year's cohort is unknown until it
is imported. Yahoo discovery is therefore reported per season, and its cohort
mix can only be projected from what has landed so far.

    py -3 scripts/research_cohorts/corpus_cohort_coverage.py
    py -3 scripts/research_cohorts/corpus_cohort_coverage.py --json out.json
"""
from __future__ import annotations

import argparse
import glob
import json
from collections import defaultdict
from pathlib import Path

import duckdb

LAKE = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
COHORTS = Path("D:/league-history-data/fantasy_leagues/cohort_aggregates")
SEEDS = Path("D:/yahoo_oauth/league-history-workers/corpus_seed")

IDP_COLS = ("roster_IDP", "roster_DL", "roster_LB", "roster_DB", "roster_DB_LB", "roster_DL_LB")


def platform_of(db_name: str, declared: str | None) -> str:
    """Prefer the folded platform column; fall back to the db_name prefix."""
    value = (declared or "").strip().lower()
    if value in {"yahoo", "mfl", "fleaflicker", "sleeper", "espn"}:
        return value
    if db_name.startswith("smpl_yahoo"):
        return "yahoo"
    if db_name.startswith("smpl_mfl"):
        return "mfl"
    if db_name.startswith("smpl_ffl"):
        return "fleaflicker"
    return "sleeper"


def cohort_of(row: dict) -> str:
    """Same convention as league_cohort_map / corpus validation."""
    teams = "10t" if int(row.get("num_teams") or 0) <= 11 else "12t"
    if sum(int(row.get(c) or 0) for c in IDP_COLS) > 0:
        roster = "idp"
    elif int(row.get("roster_SUPER_FLEX") or 0) > 0:
        roster = "sflx"
    else:
        roster = "flx"
    rec = float(row.get("scoring_rec") or 0)
    ppr = "std" if rec == 0 else "half" if rec < 0.75 else "ppr"
    td = "6pt" if float(row.get("scoring_pass_td") or 0) >= 5 else "4pt"
    return f"{teams}_{roster}_{ppr}_{td}"


def lake_sources() -> list[Path]:
    paths = [Path(p) for p in glob.glob(str(LAKE / "slices" / "**" / "*.duckdb"), recursive=True)]
    for name in ("corpus_snapshot.duckdb", "corpus_pilot_extraplatform.duckdb", "corpus_pilot_mfl.duckdb"):
        candidate = LAKE / name
        if candidate.exists():
            paths.append(candidate)
    return paths


def read_imported() -> tuple[list[dict], list[str]]:
    """One row per landed league-year, with cohort dims, platform, and draft presence."""
    rows: list[dict] = []
    notes: list[str] = []
    seen: set[tuple[str, int]] = set()
    for path in lake_sources():
        try:
            con = duckdb.connect(str(path), read_only=True)
        except Exception as exc:
            notes.append(f"unreadable: {path.name} ({str(exc).splitlines()[0][:60]})")
            continue
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info('public.league_settings')").fetchall()}
            if not cols:
                continue
            wanted = [
                "db_name", "year", "num_teams", "scoring_rec", "scoring_pass_td",
                "roster_SUPER_FLEX", *IDP_COLS,
            ]
            if "platform" in cols:
                wanted.append("platform")
            select = ", ".join(f'"{c}"' if c in cols else f'NULL AS "{c}"' for c in wanted)
            settings = con.execute(f"SELECT {select} FROM public.league_settings").fetchdf()

            drafted: set[tuple[str, int]] = set()
            try:
                drafted = {
                    (str(r[0]), int(r[1]))
                    for r in con.execute(
                        "SELECT db_name, year FROM public.draft "
                        "WHERE pick IS NOT NULL OR cost IS NOT NULL GROUP BY 1,2"
                    ).fetchall()
                }
            except Exception:
                pass

            for record in settings.to_dict("records"):
                db_name = str(record.get("db_name") or "")
                try:
                    year = int(record.get("year") or 0)
                except (TypeError, ValueError):
                    continue
                if not db_name or not year or (db_name, year) in seen:
                    continue
                seen.add((db_name, year))
                rows.append(
                    {
                        "db_name": db_name,
                        "year": year,
                        "platform": platform_of(db_name, record.get("platform")),
                        "cohort_slug": cohort_of(record),
                        "num_teams": int(record.get("num_teams") or 0),
                        "has_draft": (db_name, year) in drafted,
                    }
                )
        finally:
            con.close()
    return rows, notes


def read_discovered() -> tuple[dict[str, dict[int, int]], list[str]]:
    """Per platform: {season: league-years the platform says exist}."""
    out: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    notes: list[str] = []

    # Yahoo: the cached inventory plan, mirrored into the lake by the harvest step.
    for candidate in (
        LAKE / "discovery" / "yahoo_inventory_plan.json",
        LAKE / "slices" / "yahoo_inventory_plan.json",
    ):
        if candidate.exists():
            payload = json.loads(candidate.read_text(encoding="utf-8"))
            season_hist = payload.get("by_season")
            if isinstance(season_hist, dict):
                # Redacted season histogram written by the harvest step.
                for season, count in season_hist.items():
                    try:
                        out["yahoo"][int(season)] += int(count)
                    except (TypeError, ValueError):
                        continue
            else:
                for task in payload.get("tasks") or payload.get("candidates") or []:
                    try:
                        out["yahoo"][int(task["season"])] += 1
                    except (KeyError, TypeError, ValueError):
                        continue
            break
    else:
        notes.append("yahoo discovery plan not mirrored locally -- run the harvest step")

    for platform, seed in (("mfl", "mfl_crawl_seed.json"), ("fleaflicker", "fleaflicker_crawl_seed.json")):
        path = SEEDS / seed
        if not path.exists():
            notes.append(f"{platform} seed missing ({seed})")
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        for league in payload.get("leagues", []):
            try:
                out[platform][int(league.get("year") or 0)] += 1
            except (TypeError, ValueError):
                continue

    drain = SEEDS / "drain_leagues.parquet"
    if drain.exists():
        con = duckdb.connect()
        try:
            cols = {c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{drain.as_posix()}'").fetchall()}
            year_col = next((c for c in ("year", "season") if c in cols), None)
            if year_col:
                for year, count in con.execute(
                    f"SELECT {year_col}, COUNT(*) FROM '{drain.as_posix()}' GROUP BY 1"
                ).fetchall():
                    try:
                        out["sleeper"][int(year)] += int(count)
                    except (TypeError, ValueError):
                        continue
            else:
                # No season column: one row per league, seasons unknown.
                total = con.execute(f"SELECT COUNT(*) FROM '{drain.as_posix()}'").fetchone()[0]
                out["sleeper"][0] += int(total)
                notes.append("sleeper drain has no season column -- counted as leagues, not league-years")
        finally:
            con.close()
    else:
        notes.append("sleeper drain_leagues.parquet missing")
    return out, notes


def adp_coverage() -> dict[tuple[int, str], int]:
    """{(year, teams_bucket): player rows} from the external market ADP table."""
    path = COHORTS / "nfl_market_adp.parquet"
    if not path.exists():
        return {}
    con = duckdb.connect()
    try:
        rows = con.execute(
            f"SELECT year, teams, COUNT(*) FROM '{path.as_posix()}' GROUP BY 1,2"
        ).fetchall()
    finally:
        con.close()
    out: dict[tuple[int, str], int] = defaultdict(int)
    for year, teams, count in rows:
        try:
            bucket = "10t" if int(teams or 0) <= 11 else "12t"
            out[(int(year), bucket)] += int(count)
        except (TypeError, ValueError):
            continue
    return out


def ladder_targets() -> dict[str, dict[str, int | None]]:
    path = COHORTS / "ladder_thresholds.json"
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return {
        metric: {tier: spec.get(f"n_r{tier}") for tier in ("75", "85", "95")}
        for metric, spec in (payload.get("metrics") or {}).items()
    }


def ladder_tier(count: int, targets: dict[str, int | None]) -> str:
    """Highest reliability tier this sample size supports for a metric."""
    for tier in ("95", "85", "75"):
        need = targets.get(tier)
        if need and count >= need:
            return f"r{tier}"
    return "below r75"


def build_report() -> dict:
    imported, import_notes = read_imported()
    discovered, discovery_notes = read_discovered()
    adp = adp_coverage()
    ladder = ladder_targets()

    by_platform: dict[str, int] = defaultdict(int)
    by_cohort: dict[str, int] = defaultdict(int)
    by_cohort_platform: dict[tuple[str, str], int] = defaultdict(int)
    by_year_platform: dict[tuple[int, str], int] = defaultdict(int)
    draft_by_cohort: dict[str, int] = defaultdict(int)
    for row in imported:
        by_platform[row["platform"]] += 1
        by_cohort[row["cohort_slug"]] += 1
        by_cohort_platform[(row["cohort_slug"], row["platform"])] += 1
        by_year_platform[(row["year"], row["platform"])] += 1
        if row["has_draft"]:
            draft_by_cohort[row["cohort_slug"]] += 1

    platform_rows = []
    for platform in ("yahoo", "mfl", "fleaflicker", "sleeper"):
        found = sum(discovered.get(platform, {}).values())
        landed = by_platform.get(platform, 0)
        platform_rows.append(
            {
                "platform": platform,
                "discovered": found,
                "imported": landed,
                "net": max(0, found - landed) if found else None,
            }
        )

    cohort_rows = []
    for cohort, count in sorted(by_cohort.items(), key=lambda kv: -kv[1]):
        drafts = draft_by_cohort.get(cohort, 0)
        cohort_rows.append(
            {
                "cohort_slug": cohort,
                "imported": count,
                "with_draft": drafts,
                "draft_pct": round(100 * drafts / count, 1) if count else 0.0,
                "by_platform": {
                    platform: by_cohort_platform.get((cohort, platform), 0)
                    for platform in ("yahoo", "mfl", "fleaflicker", "sleeper")
                    if by_cohort_platform.get((cohort, platform), 0)
                },
                "ladder": {
                    metric: ladder_tier(count, targets)
                    for metric, targets in ladder.items()
                    if any(targets.values())
                },
            }
        )

    year_rows = []
    years = sorted({year for year, _ in by_year_platform} | {
        year for platform in discovered.values() for year in platform if year
    })
    for year in years:
        landed = {p: by_year_platform.get((year, p), 0) for p in ("yahoo", "mfl", "fleaflicker", "sleeper")}
        found = {p: discovered.get(p, {}).get(year, 0) for p in ("yahoo", "mfl", "fleaflicker", "sleeper")}
        year_rows.append(
            {
                "year": year,
                "imported": sum(landed.values()),
                "discovered": sum(found.values()),
                "net": max(0, sum(found.values()) - sum(landed.values())),
                "imported_by_platform": {k: v for k, v in landed.items() if v},
                "discovered_by_platform": {k: v for k, v in found.items() if v},
                "adp_10t": adp.get((year, "10t"), 0),
                "adp_12t": adp.get((year, "12t"), 0),
            }
        )

    # "Where to go next": cohorts short of the first reliability rung, hardest first.
    def _shortfall_rank(row: dict) -> tuple:
        start_below = row["ladder"].get("start_pct") == "below r75"
        adp_below = row["ladder"].get("adp") == "below r75"
        return (not (start_below or adp_below), row["imported"])

    shortfall = [
        {
            "cohort_slug": row["cohort_slug"],
            "imported": row["imported"],
            "with_draft": row["with_draft"],
            "start_pct": row["ladder"].get("start_pct"),
            "adp": row["ladder"].get("adp"),
        }
        for row in sorted(cohort_rows, key=_shortfall_rank)
        if row["ladder"].get("start_pct") == "below r75" or row["ladder"].get("adp") == "below r75"
    ]

    return {
        "totals": {
            "imported": len(imported),
            "discovered": sum(sum(v.values()) for v in discovered.values()),
            "cohorts": len(by_cohort),
            "with_draft": sum(draft_by_cohort.values()),
        },
        "platforms": platform_rows,
        "shortfall": shortfall,
        "cohorts": cohort_rows,
        "years": year_rows,
        "ladder_targets": ladder,
        "notes": import_notes + discovery_notes,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", type=Path, default=None, help="Write the full report as JSON")
    args = parser.parse_args()

    report = build_report()
    totals = report["totals"]
    print(
        f"[corpus] imported {totals['imported']:,} league-years across "
        f"{totals['cohorts']} cohorts | discovered {totals['discovered']:,}"
    )
    print("\nplatform      discovered    imported         net")
    for row in report["platforms"]:
        net = "n/a" if row["net"] is None else f"{row['net']:,}"
        print(f"  {row['platform']:<12}{row['discovered']:>10,}{row['imported']:>12,}{net:>12}")

    print("\ntop cohorts (imported / with draft / ladder: start_pct, adp)")
    for row in report["cohorts"][:12]:
        ladder = row["ladder"]
        print(
            f"  {row['cohort_slug']:<20}{row['imported']:>6,}{row['with_draft']:>8,}"
            f"  start_pct={ladder.get('start_pct', '-'):<9} adp={ladder.get('adp', '-')}"
        )

    if report["shortfall"]:
        print("\nwhere to go next (cohorts below r75 -- hardest first):")
        for row in report["shortfall"][:12]:
            print(
                f"  {row['cohort_slug']:<20}{row['imported']:>6,}  "
                f"start_pct={row['start_pct']:<9} adp={row['adp']}"
            )

    if report["notes"]:
        print("\nnotes:")
        for note in report["notes"]:
            print(f"  - {note}")

    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
        print(f"\n[corpus] wrote {args.json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
