#!/usr/bin/env python3
"""Build stratified draft optimizer league suites from centralized Fly data.

The suites are deterministic QA inputs for the Next.js optimizer backtest:

  python scripts/draft_optimizer_suite.py --output-dir artifacts/draft_optimizer_suite/latest

It writes:
  - draft_optimizer_suites.json
  - strategic_leagues.txt
  - tournament_leagues.txt

The strategic suite is small and format-dense. The tournament suite is larger
and meant for confidence sweeps after optimizer math changes.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
import sys
from typing import Any
from collections.abc import Callable

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"

for module_path in (REPO_ROOT, SCRIPTS_ROOT):
    path_str = str(module_path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.core.readers.fly_reader import FlyReader


EXCLUDED_DBS = {"demo_league"}


@dataclass(frozen=True)
class LeagueSummary:
    db_name: str
    platform: str
    years: int
    min_year: int
    max_year: int
    min_teams: int
    max_teams: int
    rounds: int
    bench: int
    flex_slots: int
    draft_kinds: str
    superflex: int
    idp: int
    median: int
    dynasty: int
    keeper: int
    min_ppr: float
    max_ppr: float
    pass_td: float
    idp_slots: int
    qb_slots: int
    sf_slots: int
    team_count_variants: int
    ppr_variants: int
    draft_kind_variants: int
    picks: int
    cost_picks: int
    score: int
    tags: list[str]
    categories: list[str]


def load_env() -> None:
    for name in [".env", ".env.local", "frontend/.env", "frontend/.env.local"]:
        path = REPO_ROOT / name
        if not path.exists():
            continue
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def suite_sql(min_years: int, min_max_year: int) -> str:
    return f"""
WITH draft_year AS (
  SELECT db_name, CAST(year AS INTEGER) AS year, ANY_VALUE(platform) AS platform,
         COUNT(*) AS picks, COUNT(DISTINCT manager) AS managers,
         SUM(CASE WHEN COALESCE(cost,0)>0 THEN 1 ELSE 0 END) AS cost_picks,
         SUM(CASE WHEN COALESCE(is_keeper,0)=1 OR LOWER(COALESCE(draft_category,''))='keeper' THEN 1 ELSE 0 END) AS keeper_picks,
         MAX(COALESCE(round,0)) AS max_round,
         STRING_AGG(DISTINCT LOWER(COALESCE(draft_type,'')), ',') AS draft_type_tokens
  FROM public.draft
  WHERE db_name IS NOT NULL AND year IS NOT NULL
  GROUP BY db_name, year
),
settings AS (
  SELECT db_name, CAST(year AS INTEGER) AS year, ANY_VALUE(platform) AS settings_platform,
         MAX(COALESCE(num_teams,0)) AS num_teams,
         MAX(COALESCE(draft_rounds,0)) AS draft_rounds,
         MAX(COALESCE(uses_median,0)) AS uses_median,
         MAX(COALESCE(is_dynasty,0)) AS is_dynasty,
         ANY_VALUE(LOWER(COALESCE(league_type,''))) AS league_type,
         MAX(COALESCE(max_keepers,0)) AS max_keepers,
         MAX(COALESCE(scoring_rec,0)) AS scoring_rec,
         MAX(COALESCE(scoring_pass_td,4)) AS scoring_pass_td,
         MAX(COALESCE(roster_QB,0)) AS roster_qb,
         MAX(COALESCE(roster_FLX,0)) AS roster_flex,
         MAX(COALESCE(roster_SUPER_FLEX,0)) AS roster_super_flex,
         MAX(COALESCE(roster_REC_FLEX,0)) AS roster_rec_flex,
         MAX(COALESCE(roster_LB,0)) AS roster_lb,
         MAX(COALESCE(roster_DL,0)) AS roster_dl,
         MAX(COALESCE(roster_DB,0)) AS roster_db,
         MAX(COALESCE(roster_IDP,0)) AS roster_idp,
         MAX(COALESCE(roster_DB_LB,0)) AS roster_db_lb,
         MAX(COALESCE(roster_DL_LB,0)) AS roster_dl_lb,
         MAX(COALESCE(roster_BN,0)) AS roster_bn,
         MAX(COALESCE(sleeper_taxi_slots,0)) AS taxi_slots,
         MAX(COALESCE(sleeper_pick_trading,0)) AS pick_trading
  FROM public.league_settings
  WHERE db_name IS NOT NULL AND year IS NOT NULL
  GROUP BY db_name, year
),
keeper_cfg AS (
  SELECT db_name,
         MAX(COALESCE(enabled,0)) AS keeper_enabled,
         MAX(COALESCE(max_keepers,0)) AS keeper_cfg_max
  FROM public.keeper_config
  GROUP BY db_name
),
league_year AS (
  SELECT d.db_name, d.year, COALESCE(d.platform, s.settings_platform, 'unknown') AS platform,
         CASE
           WHEN d.draft_type_tokens SIMILAR TO '.*(auction|live|offline).*'
             OR d.cost_picks >= d.picks * 0.25 THEN 'auction'
           ELSE 'snake'
         END AS draft_kind,
         COALESCE(NULLIF(s.num_teams,0), d.managers) AS num_teams,
         COALESCE(NULLIF(s.draft_rounds,0), NULLIF(d.max_round,0), 0) AS draft_rounds,
         d.picks,
         d.cost_picks,
         d.keeper_picks,
         COALESCE(s.uses_median,0) AS uses_median,
         CASE
           WHEN COALESCE(s.is_dynasty,0)>0
             OR s.league_type LIKE '%dynasty%'
             OR COALESCE(s.taxi_slots,0)>0
             OR COALESCE(s.pick_trading,0)>0 THEN 1
           ELSE 0
         END AS is_dynasty,
         CASE
           WHEN d.keeper_picks>0
             OR COALESCE(s.max_keepers,0)>0
             OR COALESCE(k.keeper_enabled,0)>0
             OR COALESCE(k.keeper_cfg_max,0)>0 THEN 1
           ELSE 0
         END AS has_keepers,
         CASE WHEN COALESCE(s.roster_super_flex,0)>0 OR COALESCE(s.roster_qb,0)>=2 THEN 1 ELSE 0 END AS is_superflex,
         CASE
           WHEN COALESCE(s.roster_lb,0)+COALESCE(s.roster_dl,0)+COALESCE(s.roster_db,0)
              + COALESCE(s.roster_idp,0)+COALESCE(s.roster_db_lb,0)+COALESCE(s.roster_dl_lb,0)>0
           THEN 1 ELSE 0
         END AS is_idp,
         COALESCE(s.scoring_rec,0) AS scoring_rec,
         COALESCE(s.scoring_pass_td,4) AS scoring_pass_td,
         COALESCE(s.roster_qb,0) AS roster_qb,
         COALESCE(s.roster_super_flex,0) AS roster_super_flex,
         COALESCE(s.roster_flex,0)+COALESCE(s.roster_rec_flex,0) AS flex_slots,
         COALESCE(s.roster_bn,0) AS bench,
         COALESCE(s.roster_lb,0)+COALESCE(s.roster_dl,0)+COALESCE(s.roster_db,0)
           + COALESCE(s.roster_idp,0)+COALESCE(s.roster_db_lb,0)+COALESCE(s.roster_dl_lb,0) AS idp_slots
  FROM draft_year d
  LEFT JOIN settings s ON d.db_name=s.db_name AND d.year=s.year
  LEFT JOIN keeper_cfg k ON d.db_name=k.db_name
),
league AS (
  SELECT db_name,
         ANY_VALUE(platform) AS platform,
         COUNT(*) AS years,
         MIN(year) AS min_year,
         MAX(year) AS max_year,
         MAX(num_teams) AS max_teams,
         MIN(num_teams) AS min_teams,
         MAX(draft_rounds) AS rounds,
         MAX(bench) AS bench,
         MAX(flex_slots) AS flex_slots,
         STRING_AGG(DISTINCT draft_kind, ',') AS draft_kinds,
         MAX(is_superflex) AS superflex,
         MAX(is_idp) AS idp,
         MAX(uses_median) AS median,
         MAX(is_dynasty) AS dynasty,
         MAX(has_keepers) AS keeper,
         MIN(scoring_rec) AS min_ppr,
         MAX(scoring_rec) AS max_ppr,
         MAX(scoring_pass_td) AS pass_td,
         MAX(idp_slots) AS idp_slots,
         MAX(roster_qb) AS qb_slots,
         MAX(roster_super_flex) AS sf_slots,
         COUNT(DISTINCT num_teams) AS team_count_variants,
         COUNT(DISTINCT scoring_rec) AS ppr_variants,
         COUNT(DISTINCT draft_kind) AS draft_kind_variants,
         SUM(picks) AS picks,
         SUM(cost_picks) AS cost_picks
  FROM league_year
  GROUP BY db_name
)
SELECT *
FROM league
WHERE years >= {int(min_years)}
  AND max_year >= {int(min_max_year)}
ORDER BY years DESC, picks DESC
"""


def category_specs() -> list[tuple[str, Callable[[dict[str, Any]], bool]]]:
    return [
        (
            "yahoo_redraft_snake",
            lambda r: r["platform"] == "yahoo"
            and r["draft_kinds"] == "snake"
            and not r["keeper"]
            and not r["dynasty"]
            and not r["idp"]
            and not r["superflex"],
        ),
        ("auction_or_draft_switch", lambda r: "auction" in r["draft_kinds"]),
        ("espn", lambda r: r["platform"] == "espn"),
        ("sleeper_keeper_dynasty", lambda r: r["platform"] == "sleeper" and (r["keeper"] or r["dynasty"])),
        ("superflex_no_idp", lambda r: r["superflex"] and not r["idp"]),
        ("superflex_plus_idp", lambda r: r["superflex"] and r["idp"]),
        ("idp_no_superflex", lambda r: r["idp"] and not r["superflex"]),
        ("median", lambda r: bool(r["median"])),
        ("large_14_plus", lambda r: r["max_teams"] >= 14),
        ("team_count_change", lambda r: r["team_count_variants"] > 1),
        ("ppr_change", lambda r: r["ppr_variants"] > 1),
        ("standard_scoring", lambda r: r["max_ppr"] == 0),
        ("full_ppr", lambda r: r["max_ppr"] >= 1),
        ("six_pt_pass", lambda r: r["pass_td"] >= 6),
        ("deep_bench_long_draft", lambda r: r["bench"] >= 8 or r["rounds"] >= 20),
    ]


def score_row(row: dict[str, Any]) -> int:
    score = int(row["years"]) * 2
    if row["max_year"] >= 2025:
        score += 5
    for key in ["superflex", "idp", "median", "dynasty", "keeper"]:
        if row[key]:
            score += 4
    if "auction" in str(row["draft_kinds"]):
        score += 4
    if row["max_teams"] >= 14:
        score += 4
    if row["bench"] >= 8:
        score += 2
    if row["rounds"] >= 20:
        score += 2
    if row["pass_td"] >= 6:
        score += 2
    if row["team_count_variants"] > 1:
        score += 3
    if row["ppr_variants"] > 1:
        score += 3
    if row["draft_kind_variants"] > 1:
        score += 3
    return score


def tags_for(row: dict[str, Any]) -> list[str]:
    tags = [str(row["platform"]), str(row["draft_kinds"])]
    for label, key in [("SF", "superflex"), ("IDP", "idp"), ("MED", "median"), ("DYN", "dynasty"), ("KEEP", "keeper")]:
        if row[key]:
            tags.append(label)
    if row["max_teams"] >= 14:
        tags.append(f"{row['max_teams']}tm")
    if row["pass_td"] >= 6:
        tags.append("6pt")
    if row["team_count_variants"] > 1:
        tags.append("team-change")
    if row["ppr_variants"] > 1:
        tags.append("ppr-change")
    if row["draft_kind_variants"] > 1:
        tags.append("draft-change")
    if row["bench"] >= 8:
        tags.append(f"BN{row['bench']}")
    if row["rounds"] >= 20:
        tags.append(f"R{row['rounds']}")
    return tags


def categories_for(row: dict[str, Any]) -> list[str]:
    return [name for name, predicate in category_specs() if predicate(row)]


def to_summary(row: dict[str, Any]) -> LeagueSummary:
    enriched = dict(row)
    enriched["score"] = score_row(row)
    enriched["tags"] = tags_for(row)
    enriched["categories"] = categories_for(row)
    typed = {
        "db_name": str(enriched["db_name"]),
        "platform": str(enriched["platform"]),
        "years": int(enriched["years"]),
        "min_year": int(enriched["min_year"]),
        "max_year": int(enriched["max_year"]),
        "min_teams": int(enriched["min_teams"]),
        "max_teams": int(enriched["max_teams"]),
        "rounds": int(enriched["rounds"]),
        "bench": int(enriched["bench"]),
        "flex_slots": int(enriched["flex_slots"]),
        "draft_kinds": str(enriched["draft_kinds"]),
        "superflex": int(enriched["superflex"]),
        "idp": int(enriched["idp"]),
        "median": int(enriched["median"]),
        "dynasty": int(enriched["dynasty"]),
        "keeper": int(enriched["keeper"]),
        "min_ppr": float(enriched["min_ppr"]),
        "max_ppr": float(enriched["max_ppr"]),
        "pass_td": float(enriched["pass_td"]),
        "idp_slots": int(enriched["idp_slots"]),
        "qb_slots": int(enriched["qb_slots"]),
        "sf_slots": int(enriched["sf_slots"]),
        "team_count_variants": int(enriched["team_count_variants"]),
        "ppr_variants": int(enriched["ppr_variants"]),
        "draft_kind_variants": int(enriched["draft_kind_variants"]),
        "picks": int(enriched["picks"]),
        "cost_picks": int(enriched["cost_picks"]),
        "score": int(enriched["score"]),
        "tags": list(enriched["tags"]),
        "categories": list(enriched["categories"]),
    }
    return LeagueSummary(**typed)


def select_round_robin(leagues: list[LeagueSummary], size: int) -> list[LeagueSummary]:
    by_category: dict[str, list[LeagueSummary]] = {}
    for category, _predicate in category_specs():
        by_category[category] = sorted(
            [league for league in leagues if category in league.categories],
            key=lambda league: (-league.score, -league.years, league.db_name),
        )

    selected: list[LeagueSummary] = []
    seen: set[str] = set()
    while len(selected) < size:
        added = False
        for category, _predicate in category_specs():
            candidates = by_category[category]
            for candidate in candidates:
                if candidate.db_name in seen:
                    continue
                selected.append(candidate)
                seen.add(candidate.db_name)
                added = True
                break
            if len(selected) >= size:
                break
        if not added:
            break

    if len(selected) < size:
        for league in sorted(leagues, key=lambda item: (-item.score, -item.years, item.db_name)):
            if league.db_name in seen:
                continue
            selected.append(league)
            seen.add(league.db_name)
            if len(selected) >= size:
                break
    return selected


def coverage(leagues: list[LeagueSummary]) -> dict[str, Any]:
    category_counts = {name: 0 for name, _predicate in category_specs()}
    for league in leagues:
        for category in league.categories:
            category_counts[category] += 1
    return {
        "leagues": len(leagues),
        "platforms": counts(league.platform for league in leagues),
        "draft_kinds": counts(kind for league in leagues for kind in league.draft_kinds.split(",")),
        "categories": category_counts,
    }


def counts(values: Any) -> dict[str, int]:
    out: dict[str, int] = {}
    for value in values:
        key = str(value)
        out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items()))


def write_outputs(report: dict[str, Any], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "draft_optimizer_suites.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    for suite_name in ["strategic", "tournament"]:
        leagues = [row["db_name"] for row in report["suites"][suite_name]["leagues"]]
        (output_dir / f"{suite_name}_leagues.txt").write_text(",".join(leagues) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Select draft optimizer QA suites from Fly.")
    parser.add_argument("--strategic-size", type=int, default=24)
    parser.add_argument("--tournament-size", type=int, default=96)
    parser.add_argument("--min-years", type=int, default=3)
    parser.add_argument("--min-max-year", type=int, default=2023)
    parser.add_argument("--output-dir", default="artifacts/draft_optimizer_suite/latest")
    parser.add_argument("--no-write", action="store_true")
    args = parser.parse_args()

    load_env()
    reader = FlyReader()
    rows = reader.query(suite_sql(args.min_years, args.min_max_year), database="___leagues")
    leagues = [to_summary(row) for row in rows]
    leagues = [league for league in leagues if league.categories and league.db_name.lower() not in EXCLUDED_DBS]
    leagues.sort(key=lambda item: (-item.score, -item.years, item.db_name))

    strategic = select_round_robin(leagues, args.strategic_size)
    selected_ids = {league.db_name for league in strategic}
    remaining = [league for league in leagues if league.db_name not in selected_ids]
    tournament = strategic + select_round_robin(remaining, max(0, args.tournament_size - len(strategic)))

    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source": "___leagues.public.draft + ___leagues.public.league_settings",
        "eligible_leagues": len(leagues),
        "filters": {
            "min_years": args.min_years,
            "min_max_year": args.min_max_year,
        },
        "suites": {
            "strategic": {
                "coverage": coverage(strategic),
                "leagues": [asdict(league) for league in strategic],
            },
            "tournament": {
                "coverage": coverage(tournament),
                "leagues": [asdict(league) for league in tournament],
            },
        },
    }

    if not args.no_write:
        write_outputs(report, REPO_ROOT / args.output_dir)

    print(
        json.dumps(
            {
                "output_dir": None if args.no_write else str(REPO_ROOT / args.output_dir),
                "eligible_leagues": len(leagues),
                "strategic": {
                    "n": len(strategic),
                    "leagues": [league.db_name for league in strategic],
                    "coverage": coverage(strategic),
                },
                "tournament": {
                    "n": len(tournament),
                    "leagues": [league.db_name for league in tournament],
                    "coverage": coverage(tournament),
                },
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
