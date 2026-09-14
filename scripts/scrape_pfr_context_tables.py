"""Scrape non-boxscore PFR context tables into parquet datasets.

This complements ``scrape_pfr_boxscore_tables.py``. It targets season and
team-context pages that are useful for the historical super table and GM sim:
coaches, team advanced stats, awards voting, All-Pro, Pro Bowl, combine, and
optional team roster pages.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from scrape_pfr_boxscore_tables import (
    PFR_BASE,
    fetch_html,
    make_session,
    parse_tables,
    safe_name,
    write_parquet,
)


DEFAULT_OUT_DIR = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\pfr_context")
DEFAULT_BOX_SCORE_INDEX = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized" r"\pfr_boxscores\boxscore_index.parquet"
)


@dataclass(frozen=True)
class PageSpec:
    source_kind: str
    page_key: str
    url: str
    year: int | None = None
    team: str | None = None


@dataclass(frozen=True)
class SourceDef:
    source_kind: str
    default_min_year: int
    default_max_year: int
    url_for_year: Callable[[int], str]
    keep_table: Callable[[str], bool]


def table_in(ids: set[str]) -> Callable[[str], bool]:
    return lambda table_id: table_id in ids


SOURCE_DEFS: dict[str, SourceDef] = {
    "coaches": SourceDef(
        source_kind="coaches",
        default_min_year=1920,
        default_max_year=2025,
        url_for_year=lambda year: f"{PFR_BASE}/years/{year}/coaches.htm",
        keep_table=table_in({"coaches"}),
    ),
    "advanced": SourceDef(
        source_kind="advanced",
        default_min_year=2018,
        default_max_year=2025,
        url_for_year=lambda year: f"{PFR_BASE}/years/{year}/advanced.htm",
        keep_table=table_in(
            {
                "air_yards",
                "accuracy",
                "pressure",
                "play_type",
                "advanced_rushing",
                "advanced_receiving",
            }
        ),
    ),
    "awards": SourceDef(
        source_kind="awards",
        default_min_year=1950,
        default_max_year=2025,
        url_for_year=lambda year: f"{PFR_BASE}/awards/awards_{year}.htm",
        keep_table=lambda table_id: table_id.startswith("voting_"),
    ),
    "allpro": SourceDef(
        source_kind="allpro",
        default_min_year=1920,
        default_max_year=2025,
        url_for_year=lambda year: f"{PFR_BASE}/years/{year}/allpro.htm",
        keep_table=table_in({"all_pro"}),
    ),
    "probowl": SourceDef(
        source_kind="probowl",
        default_min_year=1920,
        default_max_year=2025,
        url_for_year=lambda year: f"{PFR_BASE}/years/{year}/probowl.htm",
        keep_table=table_in({"pro_bowl"}),
    ),
    "combine": SourceDef(
        source_kind="combine",
        default_min_year=2000,
        default_max_year=2025,
        url_for_year=lambda year: f"{PFR_BASE}/draft/{year}-combine.htm",
        keep_table=table_in({"combine"}),
    ),
}


def append_csv(path: Path, row: dict[str, Any], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def done_pages(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        return {
            row.get("page_key", "")
            for row in csv.DictReader(handle)
            if row.get("status") == "ok" and row.get("page_key")
        }


def page_specs(args: argparse.Namespace) -> list[PageSpec]:
    specs: list[PageSpec] = []
    for source in args.source:
        if source == "rosters":
            specs.extend(roster_page_specs(args))
            continue
        source_def = SOURCE_DEFS[source]
        min_year = max(args.year_min or source_def.default_min_year, source_def.default_min_year)
        max_year = min(args.year_max or source_def.default_max_year, source_def.default_max_year)
        for year in range(min_year, max_year + 1):
            specs.append(
                PageSpec(
                    source_kind=source_def.source_kind,
                    page_key=f"{source_def.source_kind}:{year}",
                    url=source_def.url_for_year(year),
                    year=year,
                )
            )
    return specs


def roster_page_specs(args: argparse.Namespace) -> list[PageSpec]:
    teams: set[str] = {safe_name(team).lower() for team in args.roster_team or [] if team}
    min_year = args.roster_year_min or args.year_min or 1920
    max_year = args.roster_year_max or args.year_max or 2025

    if args.roster_from_boxscore_index:
        index = pd.read_parquet(args.roster_from_boxscore_index)
        source = index[
            (index["season"].astype("int64") >= min_year) & (index["season"].astype("int64") <= max_year)
        ].copy()
        if teams:
            source = source[source["home_stathead_id"].astype(str).str.lower().isin(teams)]
        pairs = (
            source[["season", "home_stathead_id"]]
            .dropna()
            .drop_duplicates()
            .sort_values(["season", "home_stathead_id"], kind="stable")
            .itertuples(index=False, name=None)
        )
    else:
        if not teams:
            raise SystemExit("--source rosters requires --roster-team or --roster-from-boxscore-index")
        pairs = ((year, team) for year in range(min_year, max_year + 1) for team in sorted(teams))

    specs: list[PageSpec] = []
    for year, team in pairs:
        year_int = int(year)
        team_id = str(team).lower()
        specs.append(
            PageSpec(
                source_kind="rosters",
                page_key=f"rosters:{year_int}:{team_id}",
                url=f"{PFR_BASE}/teams/{team_id}/{year_int}_roster.htm",
                year=year_int,
                team=team_id,
            )
        )
    return specs


def keep_table(source_kind: str, table_id: str) -> bool:
    if source_kind == "rosters":
        return table_id in {"roster", "starters"}
    return SOURCE_DEFS[source_kind].keep_table(table_id)


def enrich_rows(rows: list[dict[str, Any]], spec: PageSpec) -> list[dict[str, Any]]:
    enriched = []
    for row in rows:
        enriched.append(
            {
                "source_kind": spec.source_kind,
                "page_key": spec.page_key,
                "year": spec.year,
                "team_id": spec.team,
                "page_url": spec.url,
                **row,
            }
        )
    return enriched


def part_path(out_dir: Path, table_id: str, page_key: str) -> Path:
    return out_dir / "tables" / safe_name(table_id) / f"page_key={safe_name(page_key)}.parquet"


def scrape_context(args: argparse.Namespace) -> None:
    args.out_dir.mkdir(parents=True, exist_ok=True)
    session = make_session(not args.no_edge_cdp, referer=PFR_BASE)
    progress_path = args.out_dir / "progress.csv"
    done_path = args.out_dir / "page_done.csv"
    completed = done_pages(done_path) if args.skip_existing else set()
    specs = page_specs(args)
    if args.limit:
        specs = specs[: args.limit]

    progress_fields = ["page_key", "source_kind", "year", "team", "status", "tables", "rows", "seconds", "error", "url"]
    done_fields = [
        "page_key",
        "source_kind",
        "year",
        "team",
        "status",
        "tables",
        "rows",
        "seconds",
        "completed_at_utc",
        "url",
    ]
    manifest_rows: list[dict[str, Any]] = []

    for index, spec in enumerate(specs, start=1):
        if spec.page_key in completed:
            print(f"[{index}/{len(specs)}] skip existing {spec.page_key}", flush=True)
            continue
        started = time.time()
        print(f"[{index}/{len(specs)}] {spec.page_key}", flush=True)
        try:
            page_html = fetch_html(session, spec.url, args)
            parsed_tables = parse_tables(page_html, spec.url)
            kept = [table for table in parsed_tables if keep_table(spec.source_kind, table.table_id)]
            row_count = 0
            for table in kept:
                rows = enrich_rows(table.rows, spec)
                row_count += len(rows)
                path = part_path(args.out_dir, table.table_id, spec.page_key)
                write_parquet(path, rows)
                manifest_rows.append(
                    {
                        "page_key": spec.page_key,
                        "source_kind": spec.source_kind,
                        "year": spec.year,
                        "team": spec.team,
                        "table_id": table.table_id,
                        "rows": len(rows),
                        "part_path": str(path),
                    }
                )
            seconds = round(time.time() - started, 2)
            append_csv(
                done_path,
                {
                    "page_key": spec.page_key,
                    "source_kind": spec.source_kind,
                    "year": spec.year,
                    "team": spec.team,
                    "status": "ok",
                    "tables": len(kept),
                    "rows": row_count,
                    "seconds": seconds,
                    "completed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "url": spec.url,
                },
                done_fields,
            )
            append_csv(
                progress_path,
                {
                    "page_key": spec.page_key,
                    "source_kind": spec.source_kind,
                    "year": spec.year,
                    "team": spec.team,
                    "status": "ok",
                    "tables": len(kept),
                    "rows": row_count,
                    "seconds": seconds,
                    "url": spec.url,
                },
                progress_fields,
            )
            print(f"  tables={len(kept)} rows={row_count}", flush=True)
        except Exception as exc:
            append_csv(
                progress_path,
                {
                    "page_key": spec.page_key,
                    "source_kind": spec.source_kind,
                    "year": spec.year,
                    "team": spec.team,
                    "status": "error",
                    "tables": 0,
                    "rows": 0,
                    "seconds": round(time.time() - started, 2),
                    "error": repr(exc),
                    "url": spec.url,
                },
                progress_fields,
            )
            print(f"  ERROR {repr(exc)}", flush=True)
            if args.stop_on_error:
                raise

        if index < len(specs) and args.sleep:
            time.sleep(args.sleep)

    if manifest_rows:
        write_parquet(
            args.out_dir / f"context_table_parts_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.parquet",
            manifest_rows,
        )
    summary = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "inputs": len(specs),
        "new_part_rows": len(manifest_rows),
        "tables_dir": str(args.out_dir / "tables"),
        "progress": str(progress_path),
    }
    (args.out_dir / "context_scrape_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


def compact_context(args: argparse.Namespace) -> None:
    table_root = args.out_dir / "tables"
    if not table_root.exists():
        raise SystemExit(f"Missing tables directory: {table_root}")
    selected_tables = {safe_name(table) for table in args.table or []}
    for table_dir in sorted(path for path in table_root.iterdir() if path.is_dir()):
        if selected_tables and table_dir.name not in selected_tables:
            continue
        part_files = sorted(table_dir.glob("page_key=*.parquet"))
        if not part_files:
            continue
        frames = [pd.read_parquet(path) for path in part_files]
        df = pd.concat(frames, ignore_index=True)
        subset = [col for col in ["source_kind", "page_key", "table_id", "row_index_in_table"] if col in df.columns]
        if subset:
            df = df.drop_duplicates(subset=subset, keep="last")
        out_path = table_dir / "_combined.parquet"
        df.to_parquet(out_path, index=False)
        print(f"{table_dir.name}: {len(df):,} rows -> {out_path}", flush=True)

    manifest_rows = []
    for combined_path in sorted(table_root.glob("*/_combined.parquet")):
        try:
            import pyarrow.parquet as pq

            row_count = pq.ParquetFile(combined_path).metadata.num_rows
        except Exception:
            row_count = len(pd.read_parquet(combined_path))
        manifest_rows.append(
            {
                "table_id": combined_path.parent.name,
                "rows": int(row_count),
                "path": str(combined_path),
            }
        )
    if manifest_rows:
        write_parquet(args.out_dir / "compact_manifest.parquet", manifest_rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    scrape = sub.add_parser("scrape", help="Scrape selected PFR context pages.")
    scrape.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    scrape.add_argument(
        "--source",
        action="append",
        choices=sorted([*SOURCE_DEFS.keys(), "rosters"]),
        required=True,
        help="Source to scrape; repeat for multiple.",
    )
    scrape.add_argument("--year-min", type=int)
    scrape.add_argument("--year-max", type=int)
    scrape.add_argument("--limit", type=int)
    scrape.add_argument("--sleep", type=float, default=3.0)
    scrape.add_argument("--fetch-mode", choices=["auto", "requests", "cdp"], default="auto")
    scrape.add_argument("--challenge-retries", type=int, default=3)
    scrape.add_argument("--challenge-sleep", type=float, default=60.0)
    scrape.add_argument("--skip-existing", action="store_true")
    scrape.add_argument("--stop-on-error", action="store_true")
    scrape.add_argument("--no-edge-cdp", action="store_true")
    scrape.add_argument("--roster-team", action="append")
    scrape.add_argument("--roster-year-min", type=int)
    scrape.add_argument("--roster-year-max", type=int)
    scrape.add_argument("--roster-from-boxscore-index", type=Path, default=DEFAULT_BOX_SCORE_INDEX)
    scrape.set_defaults(func=scrape_context)

    compact = sub.add_parser("compact", help="Create _combined.parquet for context table datasets.")
    compact.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    compact.add_argument("--table", action="append", help="Compact only one table id; repeat for multiple.")
    compact.set_defaults(func=compact_context)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
