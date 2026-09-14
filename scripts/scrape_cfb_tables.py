"""Scrape Sports-Reference CFB tables into resume-safe parquet datasets."""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

import scrape_pfr_boxscore_tables as sr_scrape


REPO_ROOT = Path(__file__).resolve().parents[1]
CFB_BASE = "https://www.sports-reference.com/cfb"
SPORTS_REFERENCE_BASE = "https://www.sports-reference.com"
DEFAULT_OUT_DIR = REPO_ROOT / "tmp" / "cfb_2025_pilot"

YEAR_PAGE_SLUGS = [
    ("year_summary", "{year}.html"),
    ("year_schedule", "{year}-schedule.html"),
    ("year_leaders", "{year}-leaders.html"),
    ("year_passing", "{year}-passing.html"),
    ("year_rushing", "{year}-rushing.html"),
    ("year_receiving", "{year}-receiving.html"),
    ("year_kicking", "{year}-kicking.html"),
    ("year_punting", "{year}-punting.html"),
    ("year_scoring", "{year}-scoring.html"),
    ("year_team_offense", "{year}-team-offense.html"),
    ("year_team_defense", "{year}-team-defense.html"),
    ("year_special_teams", "{year}-special-teams.html"),
    ("year_standings", "{year}-standings.html"),
    ("year_bowls", "{year}-bowls.html"),
    ("year_ratings", "{year}-ratings.html"),
    ("year_polls", "{year}-polls.html"),
    ("year_preseason_odds", "{year}-preseason-odds.html"),
    ("year_coaches", "{year}-coaches.html"),
]

DONE_FIELDS = [
    "done_key",
    "dataset",
    "source_kind",
    "year",
    "status",
    "tables",
    "rows",
    "seconds",
    "completed_at_utc",
    "url",
]
PROGRESS_FIELDS = [
    "done_key",
    "dataset",
    "source_kind",
    "year",
    "status",
    "tables",
    "rows",
    "seconds",
    "error",
    "url",
]
DONE_STATUSES = {"ok", "skipped_repeated_errors", "skipped_zero_yield"}


@dataclass(frozen=True)
class PageSpec:
    done_key: str
    dataset: str
    source_kind: str
    year: int
    url: str
    metadata: dict[str, Any]


def configure_cdp(port: int) -> None:
    sr_scrape.CDP_JSON_URL = f"http://127.0.0.1:{port}/json"
    sr_scrape.CDP_VERSION_URL = f"http://127.0.0.1:{port}/json/version"
    sr_scrape._CDP_SCRAPE_TAB_WS_URL = None
    sr_scrape._CDP_SCRAPE_TAB_ORIGIN = None


def canonical_url(url: str) -> str:
    parts = urllib.parse.urlsplit(str(url or ""))
    return urllib.parse.urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def append_csv(path: Path, row: dict[str, Any], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def done_keys(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open("r", newline="", encoding="utf-8") as handle:
        return {
            row["done_key"]
            for row in csv.DictReader(handle)
            if row.get("status") in DONE_STATUSES and row.get("done_key")
        }


def error_counts(path: Path) -> dict[str, int]:
    counts: dict[str, int] = {}
    if not path.exists():
        return counts
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("status") == "error" and row.get("done_key"):
                counts[row["done_key"]] = counts.get(row["done_key"], 0) + 1
    return counts


def done_stats(path: Path) -> dict[str, int]:
    stats = {"pages": 0, "rows": 0, "tables": 0}
    if not path.exists():
        return stats
    with path.open("r", newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("status") not in DONE_STATUSES:
                continue
            stats["pages"] += 1
            for key in ["rows", "tables"]:
                try:
                    stats[key] += int(float(row.get(key) or 0))
                except ValueError:
                    pass
    return stats


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    sr_scrape.write_parquet(path, rows)


def parse_cfb_player_id(url: str) -> str:
    path = urllib.parse.urlsplit(canonical_url(url)).path
    match = re.search(r"/cfb/players/([^/.]+)\.html$", path)
    if match:
        return match.group(1)
    match = re.search(r"/cfb/players/([^/]+)/", path)
    return match.group(1) if match else ""


def parse_cfb_school_id(url: str) -> str:
    path = urllib.parse.urlsplit(canonical_url(url)).path
    match = re.search(r"/cfb/schools/([^/]+)/\d{4}\.html$", path)
    return match.group(1) if match else ""


def parse_cfb_game_id(url: str) -> str:
    name = Path(urllib.parse.urlsplit(canonical_url(url)).path).name
    return name.rsplit(".", 1)[0] if name.endswith(".html") else ""


def game_date_from_id(game_id: str) -> str:
    match = re.match(r"^(\d{4}-\d{2}-\d{2})-", game_id or "")
    return match.group(1) if match else ""


def first_url_containing(row: dict[str, Any], needle: str) -> str:
    for key, value in row.items():
        if not key.endswith("_urls") or not isinstance(value, str):
            continue
        for candidate in value.split(";"):
            if needle in candidate:
                return canonical_url(candidate)
    return ""


def urls_containing(row: dict[str, Any], needle: str) -> list[tuple[str, str, str]]:
    hits = []
    for key, value in row.items():
        if not key.endswith("_urls") or not isinstance(value, str):
            continue
        base = key[: -len("_urls")]
        texts = str(row.get(f"{base}_link_texts", "") or "").split(";")
        for index, candidate in enumerate(value.split(";")):
            if needle in candidate:
                text = texts[index] if index < len(texts) else str(row.get(base, "") or "")
                hits.append((canonical_url(candidate), base, text))
    return hits


def year_url(year: int, slug: str) -> str:
    return f"{CFB_BASE}/years/{slug.format(year=year)}"


def fetch_session(args: argparse.Namespace):
    configure_cdp(args.cdp_port)
    if getattr(args, "fetch_mode", "auto") == "cdp":
        return requests.Session()
    return sr_scrape.make_session(not args.no_edge_cdp, referer=SPORTS_REFERENCE_BASE)


def build_index(args: argparse.Namespace) -> None:
    args.out_dir.mkdir(parents=True, exist_ok=True)
    session = fetch_session(args)
    year_rows = [
        {"done_key": key, "source_kind": key, "year": args.year, "url": year_url(args.year, slug)}
        for key, slug in YEAR_PAGE_SLUGS
    ]

    summary_url = year_url(args.year, "{year}.html")
    schedule_url = year_url(args.year, "{year}-schedule.html")
    standings_url = year_url(args.year, "{year}-standings.html")

    print(f"[index] summary {summary_url}", flush=True)
    try:
        summary_html = sr_scrape.fetch_html(session, summary_url, args)
        for table in sr_scrape.parse_tables(summary_html, summary_url):
            if table.table_id != "conferences":
                continue
            for row in table.rows:
                conf_url = first_url_containing(row, "/cfb/conferences/")
                if conf_url:
                    conf_id = Path(urllib.parse.urlsplit(conf_url).path).parts[-2]
                    year_rows.append(
                        {
                            "done_key": f"conference_{conf_id}",
                            "source_kind": "conference",
                            "year": args.year,
                            "url": conf_url,
                        }
                    )
    except Exception as exc:
        print(f"  summary unavailable for {args.year}: {repr(exc)}", flush=True)
        if args.stop_on_error:
            raise

    print(f"[index] schedule {schedule_url}", flush=True)
    schedule_tables = []
    try:
        schedule_html = sr_scrape.fetch_html(session, schedule_url, args)
        schedule_tables = sr_scrape.parse_tables(schedule_html, schedule_url)
    except Exception as exc:
        print(f"  schedule unavailable for {args.year}: {repr(exc)}", flush=True)
        if args.stop_on_error:
            raise
    schedule_table = next((table for table in schedule_tables if table.table_id == "schedule"), None)
    boxscore_rows = []
    if schedule_table:
        for row in schedule_table.rows:
            game_url = first_url_containing(row, "/cfb/boxscores/")
            game_id = parse_cfb_game_id(game_url)
            if not game_id:
                continue
            boxscore_rows.append(
                {
                    "game_id": game_id,
                    "year": args.year,
                    "game_date": game_date_from_id(game_id),
                    "week_number": row.get("week_number", ""),
                    "winner_school_name": row.get("winner_school_name", ""),
                    "winner_points": row.get("winner_points", ""),
                    "game_location": row.get("game_location", ""),
                    "loser_school_name": row.get("loser_school_name", ""),
                    "loser_points": row.get("loser_points", ""),
                    "notes": row.get("notes", ""),
                    "url": game_url,
                }
            )
    write_parquet(args.out_dir / "schedule_raw.parquet", schedule_table.rows if schedule_table else [])

    print(f"[index] standings {standings_url}", flush=True)
    standings_tables = []
    try:
        standings_html = sr_scrape.fetch_html(session, standings_url, args)
        standings_tables = sr_scrape.parse_tables(standings_html, standings_url)
    except Exception as exc:
        print(f"  standings unavailable for {args.year}: {repr(exc)}", flush=True)
        if args.stop_on_error:
            raise

    team_rows_by_id: dict[str, dict[str, Any]] = {}
    for table in standings_tables:
        if table.table_id != "standings":
            continue
        for row in table.rows:
            team_url = first_url_containing(row, "/cfb/schools/")
            team_id = parse_cfb_school_id(team_url)
            if team_id:
                team_rows_by_id[team_id] = {
                    "team_id": team_id,
                    "school_name": row.get("school_name", ""),
                    "conf_abbr": row.get("conf_abbr", ""),
                    "wins": row.get("wins", ""),
                    "losses": row.get("losses", ""),
                    "srs": row.get("srs", ""),
                    "sos": row.get("sos", ""),
                    "year": args.year,
                    "url": team_url,
                }
    for table in schedule_tables:
        for row in table.rows:
            for team_url, _, team_name in urls_containing(row, "/cfb/schools/"):
                team_id = parse_cfb_school_id(team_url)
                if team_id and team_id not in team_rows_by_id:
                    team_rows_by_id[team_id] = {
                        "team_id": team_id,
                        "school_name": team_name,
                        "conf_abbr": "",
                        "wins": "",
                        "losses": "",
                        "srs": "",
                        "sos": "",
                        "year": args.year,
                        "url": team_url,
                    }

    year_df = pd.DataFrame(year_rows).drop_duplicates("done_key").sort_values("done_key", kind="stable")
    team_df = pd.DataFrame(team_rows_by_id.values())
    box_df = pd.DataFrame(boxscore_rows)
    if not team_df.empty:
        team_df = team_df.sort_values("team_id", kind="stable")
    if not box_df.empty:
        box_df = box_df.drop_duplicates("game_id").sort_values(["game_date", "game_id"], kind="stable")
    year_df.to_parquet(args.out_dir / "year_page_index.parquet", index=False)
    team_df.to_parquet(args.out_dir / "team_index.parquet", index=False)
    box_df.to_parquet(args.out_dir / "boxscore_index.parquet", index=False)
    summary = {
        "year": args.year,
        "year_pages": int(len(year_df)),
        "teams": int(len(team_df)),
        "boxscores": int(len(box_df)),
        "out_dir": str(args.out_dir),
    }
    (args.out_dir / "index_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


def part_path(dataset_dir: Path, table_id: str, filename: str) -> Path:
    return dataset_dir / "tables" / sr_scrape.safe_name(table_id) / filename


def enrich_rows(rows: list[dict[str, Any]], spec: PageSpec, scraped_at_utc: str) -> list[dict[str, Any]]:
    return [
        {
            "dataset": spec.dataset,
            "source_kind": spec.source_kind,
            "done_key": spec.done_key,
            "year": spec.year,
            "page_url": spec.url,
            "scraped_at_utc": scraped_at_utc,
            **spec.metadata,
            **row,
        }
        for row in rows
    ]


def specs_for_pages(args: argparse.Namespace) -> list[PageSpec]:
    if args.group == "context":
        index_path = args.out_dir / "year_page_index.parquet"
        dataset = "context"
        if not index_path.exists():
            raise SystemExit(f"Missing {index_path}; run build-index first.")
        df = pd.read_parquet(index_path)
        if args.source_kind:
            df = df[df["source_kind"].astype(str).isin(set(args.source_kind))].copy()
        return (
            [
                PageSpec(str(row["done_key"]), dataset, str(row["source_kind"]), int(row["year"]), str(row["url"]), {})
                for row in df.head(args.limit).to_dict("records")
            ]
            if args.limit
            else [
                PageSpec(str(row["done_key"]), dataset, str(row["source_kind"]), int(row["year"]), str(row["url"]), {})
                for row in df.to_dict("records")
            ]
        )

    index_path = args.out_dir / "team_index.parquet"
    if not index_path.exists():
        raise SystemExit(f"Missing {index_path}; run build-index first.")
    df = pd.read_parquet(index_path)
    if args.team_id:
        df = df[df["team_id"].astype(str).isin(set(args.team_id))].copy()
    if args.limit:
        df = df.head(args.limit).copy()
    return [
        PageSpec(
            str(row["team_id"]),
            "team_pages",
            "team_season",
            int(row["year"]),
            str(row["url"]),
            {
                "team_id": row.get("team_id", ""),
                "school_name": row.get("school_name", ""),
                "conf_abbr": row.get("conf_abbr", ""),
            },
        )
        for row in df.to_dict("records")
    ]


def scrape_pages(args: argparse.Namespace) -> None:
    session = fetch_session(args)
    specs = specs_for_pages(args)
    dataset_dir = args.out_dir / ("context" if args.group == "context" else "team_pages")
    dataset_dir.mkdir(parents=True, exist_ok=True)
    completed = done_keys(dataset_dir / "page_done.csv") if args.skip_existing else set()
    for index, spec in enumerate(specs, start=1):
        if spec.done_key in completed:
            if index % 1000 == 0 or index == len(specs):
                print(f"[{index}/{len(specs)}] skipped existing through {spec.done_key}", flush=True)
            continue
        started = time.time()
        print(f"[{index}/{len(specs)}] {spec.dataset}:{spec.done_key} {spec.url}", flush=True)
        try:
            html = sr_scrape.fetch_html(session, spec.url, args)
            tables = sr_scrape.parse_tables(html, spec.url)
            scraped_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            rows_written = 0
            for table in tables:
                rows = enrich_rows(table.rows, spec, scraped_at_utc)
                rows_written += len(rows)
                if spec.dataset == "team_pages":
                    filename = (
                        f"team_id={sr_scrape.safe_name(str(spec.metadata.get('team_id', spec.done_key)))}.parquet"
                    )
                else:
                    filename = f"page_key={sr_scrape.safe_name(spec.done_key)}.parquet"
                write_parquet(part_path(dataset_dir, table.table_id, filename), rows)
            done_row = {
                "done_key": spec.done_key,
                "dataset": spec.dataset,
                "source_kind": spec.source_kind,
                "year": spec.year,
                "status": "ok",
                "tables": len(tables),
                "rows": rows_written,
                "seconds": round(time.time() - started, 2),
                "completed_at_utc": scraped_at_utc,
                "url": spec.url,
            }
            append_csv(dataset_dir / "page_done.csv", done_row, DONE_FIELDS)
            append_csv(dataset_dir / "progress.csv", done_row, PROGRESS_FIELDS)
            print(f"  tables={len(tables)} rows={rows_written}", flush=True)
        except Exception as exc:
            error_text = repr(exc)
            append_csv(
                dataset_dir / "progress.csv",
                {
                    "done_key": spec.done_key,
                    "dataset": spec.dataset,
                    "source_kind": spec.source_kind,
                    "year": spec.year,
                    "status": "error",
                    "tables": 0,
                    "rows": 0,
                    "seconds": round(time.time() - started, 2),
                    "error": repr(exc),
                    "url": spec.url,
                },
                PROGRESS_FIELDS,
            )
            print(f"  ERROR {repr(exc)}", flush=True)
            if args.stop_on_error:
                raise
        if index < len(specs) and args.sleep:
            time.sleep(args.sleep)


def scrape_boxscores(args: argparse.Namespace) -> None:
    session = fetch_session(args)
    index_path = args.out_dir / "boxscore_index.parquet"
    if not index_path.exists():
        raise SystemExit(f"Missing {index_path}; run build-index first.")
    df = pd.read_parquet(index_path)
    if not df.empty:
        df = df.sort_values(["game_date", "game_id"], kind="stable")
    if args.game_id:
        df = df[df["game_id"].astype(str).isin(set(args.game_id))].copy()
    if args.limit:
        df = df.head(args.limit).copy()
    dataset_dir = args.out_dir / "boxscores"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    completed = done_keys(dataset_dir / "page_done.csv") if args.skip_existing else set()
    previous_errors = error_counts(dataset_dir / "progress.csv") if args.skip_existing else {}
    stats = done_stats(dataset_dir / "page_done.csv") if args.skip_existing else {"pages": 0, "rows": 0, "tables": 0}
    rows_in = df.to_dict("records")
    for index, row_in in enumerate(rows_in, start=1):
        game_id = str(row_in["game_id"])
        if game_id in completed:
            if index % 1000 == 0 or index == len(rows_in):
                print(f"[{index}/{len(rows_in)}] skipped existing through {game_id}", flush=True)
            continue
        if (
            args.zero_row_stop_after
            and stats["pages"] >= args.zero_row_stop_after
            and stats["rows"] == 0
            and stats["tables"] == 0
        ):
            skipped_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            remaining = [r for r in rows_in[index - 1 :] if str(r["game_id"]) not in completed]
            for remaining_row in remaining:
                remaining_id = str(remaining_row["game_id"])
                remaining_url = str(remaining_row["url"])
                skip_row = {
                    "done_key": remaining_id,
                    "dataset": "boxscores",
                    "source_kind": "boxscore",
                    "year": remaining_row.get("year", ""),
                    "status": "skipped_zero_yield",
                    "tables": 0,
                    "rows": 0,
                    "seconds": 0,
                    "completed_at_utc": skipped_at_utc,
                    "error": f"skipped remaining boxscores after {stats['pages']} handled pages yielded zero tables/rows",
                    "url": remaining_url,
                }
                append_csv(dataset_dir / "page_done.csv", skip_row, DONE_FIELDS)
                append_csv(dataset_dir / "progress.csv", skip_row, PROGRESS_FIELDS)
                completed.add(remaining_id)
                stats["pages"] += 1
            print(
                f"[{index}/{len(rows_in)}] skipped {len(remaining)} remaining boxscores after zero-table yield",
                flush=True,
            )
            break
        if args.max_page_errors and previous_errors.get(game_id, 0) >= args.max_page_errors:
            url = str(row_in["url"])
            skipped_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            skip_row = {
                "done_key": game_id,
                "dataset": "boxscores",
                "source_kind": "boxscore",
                "year": row_in.get("year", ""),
                "status": "skipped_repeated_errors",
                "tables": 0,
                "rows": 0,
                "seconds": 0,
                "completed_at_utc": skipped_at_utc,
                "error": f"skipped after {previous_errors.get(game_id, 0)} prior errors",
                "url": url,
            }
            append_csv(dataset_dir / "page_done.csv", skip_row, DONE_FIELDS)
            append_csv(dataset_dir / "progress.csv", skip_row, PROGRESS_FIELDS)
            completed.add(game_id)
            stats["pages"] += 1
            print(f"[{index}/{len(rows_in)}] skipped after repeated errors: {game_id}", flush=True)
            continue
        started = time.time()
        url = str(row_in["url"])
        print(f"[{index}/{len(rows_in)}] boxscore:{game_id} {url}", flush=True)
        try:
            html = sr_scrape.fetch_html(session, url, args)
            tables = sr_scrape.parse_tables(html, url)
            scraped_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            rows_written = 0
            for table in tables:
                rows = [
                    {
                        "dataset": "boxscores",
                        "source_kind": "boxscore",
                        "done_key": game_id,
                        "game_id": game_id,
                        "year": row_in.get("year", ""),
                        "game_date": row_in.get("game_date", ""),
                        "week_number": row_in.get("week_number", ""),
                        "winner_school_name": row_in.get("winner_school_name", ""),
                        "loser_school_name": row_in.get("loser_school_name", ""),
                        "page_url": url,
                        "scraped_at_utc": scraped_at_utc,
                        **table_row,
                    }
                    for table_row in table.rows
                ]
                rows_written += len(rows)
                write_parquet(
                    part_path(dataset_dir, table.table_id, f"game_id={sr_scrape.safe_name(game_id)}.parquet"), rows
                )
            done_row = {
                "done_key": game_id,
                "dataset": "boxscores",
                "source_kind": "boxscore",
                "year": row_in.get("year", ""),
                "status": "ok",
                "tables": len(tables),
                "rows": rows_written,
                "seconds": round(time.time() - started, 2),
                "completed_at_utc": scraped_at_utc,
                "url": url,
            }
            append_csv(dataset_dir / "page_done.csv", done_row, DONE_FIELDS)
            append_csv(dataset_dir / "progress.csv", done_row, PROGRESS_FIELDS)
            stats["pages"] += 1
            stats["rows"] += rows_written
            stats["tables"] += len(tables)
            print(f"  tables={len(tables)} rows={rows_written}", flush=True)
        except Exception as exc:
            error_text = repr(exc)
            append_csv(
                dataset_dir / "progress.csv",
                {
                    "done_key": game_id,
                    "dataset": "boxscores",
                    "source_kind": "boxscore",
                    "year": row_in.get("year", ""),
                    "status": "error",
                    "tables": 0,
                    "rows": 0,
                    "seconds": round(time.time() - started, 2),
                    "error": error_text,
                    "url": url,
                },
                PROGRESS_FIELDS,
            )
            previous_errors[game_id] = previous_errors.get(game_id, 0) + 1
            print(f"  ERROR {error_text}", flush=True)
            if (
                args.max_page_errors
                and "TimeoutError" in error_text
                and previous_errors.get(game_id, 0) >= args.max_page_errors
            ):
                skipped_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                skip_row = {
                    "done_key": game_id,
                    "dataset": "boxscores",
                    "source_kind": "boxscore",
                    "year": row_in.get("year", ""),
                    "status": "skipped_repeated_errors",
                    "tables": 0,
                    "rows": 0,
                    "seconds": 0,
                    "completed_at_utc": skipped_at_utc,
                    "error": f"skipped after {previous_errors.get(game_id, 0)} timeout errors",
                    "url": url,
                }
                append_csv(dataset_dir / "page_done.csv", skip_row, DONE_FIELDS)
                append_csv(dataset_dir / "progress.csv", skip_row, PROGRESS_FIELDS)
                completed.add(game_id)
                stats["pages"] += 1
                print(f"  skipped after repeated timeout errors: {game_id}", flush=True)
                continue
            if args.stop_on_error and not (args.max_page_errors and "TimeoutError" in error_text):
                raise
        if index < len(rows_in) and args.sleep:
            time.sleep(args.sleep)


def source_files_for_player_index(out_dir: Path) -> list[Path]:
    files = []
    for dataset in ["context", "team_pages", "boxscores"]:
        root = out_dir / dataset / "tables"
        if root.exists():
            files.extend(sorted(path for path in root.glob("*/*.parquet") if path.name != "_combined.parquet"))
            files.extend(sorted(root.glob("*/_combined.parquet")))
    return files


def build_player_index(args: argparse.Namespace) -> None:
    players: dict[str, dict[str, Any]] = {}
    files = source_files_for_player_index(args.out_dir)
    for file_index, path in enumerate(files, start=1):
        if file_index % 200 == 0:
            print(f"[player-index] scanned {file_index:,}/{len(files):,}", flush=True)
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            print(f"  skip unreadable {path}: {repr(exc)}", flush=True)
            continue
        table_id = path.parent.name
        for row in df.to_dict("records"):
            for url, base, text in urls_containing(row, "/cfb/players/"):
                cfb_player_id = parse_cfb_player_id(url)
                if not cfb_player_id:
                    continue
                candidate = {
                    "cfb_player_id": cfb_player_id,
                    "player": text or row.get(base, "") or row.get("player", "") or row.get("name_display", ""),
                    "url": canonical_url(url),
                    "first_source_dataset": str(row.get("dataset", "")),
                    "first_source_table": table_id,
                    "first_source_key": str(row.get("done_key", "")),
                }
                current = players.get(cfb_player_id)
                if current is None or (not current.get("player") and candidate.get("player")):
                    players[cfb_player_id] = candidate
    out = pd.DataFrame(players.values())
    if not out.empty:
        out = out.sort_values("cfb_player_id", kind="stable")
    out.to_parquet(args.out_dir / "player_index.parquet", index=False)
    print(
        json.dumps({"players": int(len(out)), "path": str(args.out_dir / "player_index.parquet")}, indent=2), flush=True
    )


def player_url(cfb_player_id: str, page_kind: str, year: int) -> str:
    base = f"{CFB_BASE}/players/{cfb_player_id}"
    if page_kind == "main":
        return f"{base}.html"
    if page_kind == "gamelog":
        return f"{base}/gamelog/{year}/"
    if page_kind == "splits":
        return f"{base}/splits/{year}/"
    if page_kind == "gamelog_career":
        return f"{base}/gamelog/"
    if page_kind == "splits_career":
        return f"{base}/splits/"
    raise ValueError(f"Unknown player page kind: {page_kind}")


def scrape_players(args: argparse.Namespace) -> None:
    session = fetch_session(args)
    index_path = args.out_dir / "player_index.parquet"
    if not index_path.exists():
        raise SystemExit(f"Missing {index_path}; run build-player-index first.")
    df = pd.read_parquet(index_path).sort_values("cfb_player_id", kind="stable")
    if args.cfb_player_id:
        df = df[df["cfb_player_id"].astype(str).isin(set(args.cfb_player_id))].copy()
    if args.limit:
        df = df.head(args.limit).copy()
    page_kinds = args.page_kind or ["main", "gamelog", "splits"]
    specs = []
    for row in df.to_dict("records"):
        cfb_player_id = str(row["cfb_player_id"])
        for kind in page_kinds:
            specs.append(
                PageSpec(
                    f"{cfb_player_id}|{kind}",
                    "players",
                    f"player_{kind}",
                    args.year,
                    player_url(cfb_player_id, kind, args.year),
                    {"cfb_player_id": cfb_player_id, "player": row.get("player", "")},
                )
            )
    dataset_dir = args.out_dir / "players"
    dataset_dir.mkdir(parents=True, exist_ok=True)
    completed = done_keys(dataset_dir / "page_done.csv") if args.skip_existing else set()
    for index, spec in enumerate(specs, start=1):
        if spec.done_key in completed:
            if index % 1000 == 0 or index == len(specs):
                print(f"[{index}/{len(specs)}] skipped existing through {spec.done_key}", flush=True)
            continue
        started = time.time()
        print(f"[{index}/{len(specs)}] player:{spec.done_key} {spec.url}", flush=True)
        try:
            html = sr_scrape.fetch_html(session, spec.url, args)
            tables = sr_scrape.parse_tables(html, spec.url)
            scraped_at_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            rows_written = 0
            page_key = spec.source_kind.replace("player_", "")
            for table in tables:
                rows = enrich_rows(table.rows, spec, scraped_at_utc)
                rows_written += len(rows)
                filename = (
                    f"cfb_player_id={sr_scrape.safe_name(str(spec.metadata['cfb_player_id']))}"
                    f"__page_key={sr_scrape.safe_name(page_key)}.parquet"
                )
                write_parquet(part_path(dataset_dir, table.table_id, filename), rows)
            done_row = {
                "done_key": spec.done_key,
                "dataset": "players",
                "source_kind": spec.source_kind,
                "year": args.year,
                "status": "ok",
                "tables": len(tables),
                "rows": rows_written,
                "seconds": round(time.time() - started, 2),
                "completed_at_utc": scraped_at_utc,
                "url": spec.url,
            }
            append_csv(dataset_dir / "page_done.csv", done_row, DONE_FIELDS)
            append_csv(dataset_dir / "progress.csv", done_row, PROGRESS_FIELDS)
            print(f"  tables={len(tables)} rows={rows_written}", flush=True)
        except Exception as exc:
            append_csv(
                dataset_dir / "progress.csv",
                {
                    "done_key": spec.done_key,
                    "dataset": "players",
                    "source_kind": spec.source_kind,
                    "year": args.year,
                    "status": "error",
                    "tables": 0,
                    "rows": 0,
                    "seconds": round(time.time() - started, 2),
                    "error": repr(exc),
                    "url": spec.url,
                },
                PROGRESS_FIELDS,
            )
            print(f"  ERROR {repr(exc)}", flush=True)
            if args.stop_on_error:
                raise
        if index < len(specs) and args.sleep:
            time.sleep(args.sleep)


def discover_columns(files: list[Path], table_id: str) -> list[str]:
    columns = []
    seen = set()
    for index, path in enumerate(files, start=1):
        schema = pq.read_schema(path)
        for name in schema.names:
            if name not in seen:
                seen.add(name)
                columns.append(name)
        if index % 1000 == 0:
            print(f"  {table_id}: scanned schemas {index:,}/{len(files):,}", flush=True)
    return columns


def dataframe_to_string_table(df: pd.DataFrame, columns: list[str], schema: pa.Schema) -> pa.Table:
    out = pd.DataFrame(index=df.index)
    for column in columns:
        if column in df.columns:
            out[column] = df[column].where(df[column].notna(), None).astype("string")
        else:
            out[column] = pd.Series([None] * len(df), dtype="string")
    return pa.Table.from_pandas(out, schema=schema, preserve_index=False)


def compact_dataset(args: argparse.Namespace) -> None:
    dataset_dir = args.out_dir / args.dataset
    table_root = dataset_dir / "tables"
    if not table_root.exists():
        print(
            json.dumps({"dataset": args.dataset, "tables": 0, "rows": 0, "status": "no_tables"}, indent=2), flush=True
        )
        return
    selected = set(args.table or [])
    table_dirs = [path for path in sorted(table_root.iterdir()) if path.is_dir()]
    if selected:
        table_dirs = [path for path in table_dirs if path.name in selected]
    manifest_rows = []
    for table_index, table_dir in enumerate(table_dirs, start=1):
        table_id = table_dir.name
        part_files = sorted(path for path in table_dir.glob("*.parquet") if path.name != "_combined.parquet")
        out_path = table_dir / "_combined.parquet"
        if not part_files:
            if out_path.exists():
                manifest_rows.append(
                    {
                        "table_id": table_id,
                        "rows": int(pq.ParquetFile(out_path).metadata.num_rows),
                        "path": str(out_path),
                        "status": "existing",
                    }
                )
            continue
        read_files = ([out_path] if out_path.exists() else []) + part_files
        print(f"[{table_index}/{len(table_dirs)}] {args.dataset}:{table_id} files={len(read_files):,}", flush=True)
        columns = discover_columns(read_files, table_id)
        schema = pa.schema([(column, pa.string()) for column in columns])
        tmp_path = table_dir / "_combined.tmp.parquet"
        if tmp_path.exists():
            tmp_path.unlink()
        source_rows = 0
        with pq.ParquetWriter(tmp_path, schema=schema, compression="zstd", use_dictionary=True) as writer:
            for file_index, path in enumerate(read_files, start=1):
                df = pd.read_parquet(path)
                source_rows += len(df)
                if not df.empty:
                    writer.write_table(dataframe_to_string_table(df, columns, schema))
                if file_index % 1000 == 0:
                    print(f"  {table_id}: streamed {file_index:,}/{len(read_files):,}", flush=True)
        combined_rows = pq.ParquetFile(tmp_path).metadata.num_rows
        if combined_rows != source_rows:
            raise RuntimeError(f"{table_id}: source rows {source_rows} != combined rows {combined_rows}")
        if out_path.exists():
            out_path.unlink()
        tmp_path.rename(out_path)
        deleted_files = 0
        deleted_bytes = 0
        if args.delete_parts:
            resolved_table = table_dir.resolve()
            for path in part_files:
                if path.resolve().parent != resolved_table:
                    raise RuntimeError(f"Refusing to delete outside table directory: {path}")
                deleted_bytes += path.stat().st_size
                path.unlink()
                deleted_files += 1
        manifest_rows.append(
            {
                "table_id": table_id,
                "rows": int(combined_rows),
                "path": str(out_path),
                "status": "ok",
                "part_files": len(part_files),
                "deleted_files": deleted_files,
                "deleted_bytes": deleted_bytes,
            }
        )
    if manifest_rows:
        pd.DataFrame(manifest_rows).to_parquet(dataset_dir / "compact_manifest.parquet", index=False)
        print(
            json.dumps(
                {
                    "dataset": args.dataset,
                    "tables": len(manifest_rows),
                    "rows": int(sum(r["rows"] for r in manifest_rows)),
                },
                indent=2,
            ),
            flush=True,
        )


def status(args: argparse.Namespace) -> None:
    out: dict[str, Any] = {"out_dir": str(args.out_dir)}
    for name in ["year_page_index", "team_index", "boxscore_index", "player_index"]:
        path = args.out_dir / f"{name}.parquet"
        if path.exists():
            out[name.replace("_index", "s")] = int(len(pd.read_parquet(path)))
    datasets: dict[str, Any] = {}
    for dataset in ["context", "team_pages", "boxscores", "players"]:
        dataset_dir = args.out_dir / dataset
        row: dict[str, Any] = {}
        done_path = dataset_dir / "page_done.csv"
        if done_path.exists():
            done = pd.read_csv(done_path)
            ok = done[done["status"].isin(DONE_STATUSES)].copy()
            row["completed_pages"] = int(len(ok))
            row["completed_rows"] = int(pd.to_numeric(ok["rows"], errors="coerce").fillna(0).sum())
            skipped = ok[ok["status"] != "ok"].copy()
            if not skipped.empty:
                row["skipped_pages"] = int(len(skipped))
            if not ok.empty:
                row["last_done"] = str(ok.iloc[-1]["done_key"])
                row["last_completed_at_utc"] = str(ok.iloc[-1]["completed_at_utc"])
        table_root = dataset_dir / "tables"
        if table_root.exists():
            row["part_files"] = len([p for p in table_root.glob("*/*.parquet") if p.name != "_combined.parquet"])
            row["combined_files"] = len(list(table_root.glob("*/_combined.parquet")))
        manifest_path = dataset_dir / "compact_manifest.parquet"
        if manifest_path.exists():
            manifest = pd.read_parquet(manifest_path)
            row["combined_rows"] = int(pd.to_numeric(manifest["rows"], errors="coerce").fillna(0).sum())
        if row:
            datasets[dataset] = row
    out["datasets"] = datasets
    print(json.dumps(out, indent=2), flush=True)


def add_fetch_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--sleep", type=float, default=4.0)
    parser.add_argument("--fetch-mode", choices=["auto", "requests", "cdp"], default="cdp")
    parser.add_argument("--challenge-retries", type=int, default=2)
    parser.add_argument("--challenge-sleep", type=float, default=20.0)
    parser.add_argument("--max-page-errors", type=int, default=0)
    parser.add_argument("--zero-row-stop-after", type=int, default=0)
    parser.add_argument("--cdp-port", type=int, default=9223)
    parser.add_argument("--no-edge-cdp", action="store_true")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    index = sub.add_parser("build-index")
    add_fetch_args(index)
    index.add_argument("--year", type=int, default=2025)
    index.set_defaults(func=build_index)

    pages = sub.add_parser("scrape-pages")
    add_fetch_args(pages)
    pages.add_argument("--group", choices=["context", "team"], required=True)
    pages.add_argument("--source-kind", action="append")
    pages.add_argument("--team-id", action="append")
    pages.add_argument("--limit", type=int)
    pages.set_defaults(func=scrape_pages)

    box = sub.add_parser("scrape-boxscores")
    add_fetch_args(box)
    box.add_argument("--game-id", action="append")
    box.add_argument("--limit", type=int)
    box.set_defaults(func=scrape_boxscores)

    players = sub.add_parser("build-player-index")
    players.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    players.set_defaults(func=build_player_index)

    scrape_players_cmd = sub.add_parser("scrape-players")
    add_fetch_args(scrape_players_cmd)
    scrape_players_cmd.add_argument("--year", type=int, default=2025)
    scrape_players_cmd.add_argument(
        "--page-kind",
        action="append",
        choices=["main", "gamelog", "splits", "gamelog_career", "splits_career"],
    )
    scrape_players_cmd.add_argument("--cfb-player-id", action="append")
    scrape_players_cmd.add_argument("--limit", type=int)
    scrape_players_cmd.set_defaults(func=scrape_players)

    compact = sub.add_parser("compact")
    compact.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    compact.add_argument("--dataset", choices=["context", "team_pages", "boxscores", "players"], required=True)
    compact.add_argument("--table", action="append")
    compact.add_argument("--delete-parts", action="store_true")
    compact.set_defaults(func=compact_dataset)

    stat = sub.add_parser("status")
    stat.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    stat.set_defaults(func=status)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
