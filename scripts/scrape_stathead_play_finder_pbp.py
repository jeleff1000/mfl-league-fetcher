"""Scrape Stathead Play Finder Individual Plays rows from a backfill plan.

This consumes ``stathead_pbp_backfill_offense_queries_*.csv`` created by
``build_stathead_pbp_backfill_plan.py``. Each query is one offense-vs-defense
side for a matchup-season. Full games are reconstructed later by joining the two
oriented sides on date/team pair.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import html
import json
import re
import time
import urllib.parse
import urllib.request
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import requests
import websockets


DEFAULT_PLAN = Path("tmp/stathead_pbp_backfill_plan/stathead_pbp_backfill_offense_queries_1978_1998.csv")
DEFAULT_OUT_DIR = Path("tmp/stathead_pbp_backfill_raw")

PFR_BASE = "https://www.pro-football-reference.com"


class TableParser(HTMLParser):
    def __init__(self, table_id: str):
        super().__init__(convert_charrefs=True)
        self.table_id = table_id
        self.in_table = False
        self.table_depth = 0
        self.in_row = False
        self.skip_row = False
        self.in_cell = False
        self.capture_html = False
        self.html_depth = 0
        self.active_link_index: int | None = None
        self.row: list[dict[str, Any]] = []
        self.cell: dict[str, Any] | None = None
        self.rows: list[list[dict[str, Any]]] = []

    def handle_starttag(self, tag: str, attrs_list: list[tuple[str, str | None]]) -> None:
        attrs = {k: v or "" for k, v in attrs_list}
        if tag == "table" and attrs.get("id") == self.table_id:
            self.in_table = True
            self.table_depth = 1
            return
        if self.in_table and tag == "table":
            self.table_depth += 1
        if self.in_table and tag == "tr":
            self.in_row = True
            self.skip_row = "thead" in attrs.get("class", "").split()
            self.row = []
            return
        if self.in_row and tag in {"th", "td"}:
            self.in_cell = True
            self.capture_html = attrs.get("data-stat") == "description"
            self.html_depth = 0
            self.cell = {
                "data_stat": attrs.get("data-stat", ""),
                "text": [],
                "html": [],
                "links": [],
            }
            if self.capture_html:
                self.cell["html"].append(self.get_starttag_text() or "")
            return
        if self.in_cell and self.cell is not None:
            if self.capture_html:
                self.cell["html"].append(self.get_starttag_text() or "")
                self.html_depth += 1
            if tag == "a":
                href = attrs.get("href", "")
                if href:
                    self.cell["links"].append({"text": "", "href": absolute_url(href)})
                    self.active_link_index = len(self.cell["links"]) - 1

    def handle_data(self, data: str) -> None:
        if self.in_cell and self.cell is not None:
            self.cell["text"].append(data)
            if self.capture_html:
                self.cell["html"].append(html.escape(data, quote=False))
            if self.active_link_index is not None:
                self.cell["links"][self.active_link_index]["text"] += data

    def handle_entityref(self, name: str) -> None:
        if self.in_cell and self.capture_html and self.cell is not None:
            self.cell["html"].append(f"&{name};")

    def handle_charref(self, name: str) -> None:
        if self.in_cell and self.capture_html and self.cell is not None:
            self.cell["html"].append(f"&#{name};")

    def handle_endtag(self, tag: str) -> None:
        if self.in_cell and tag in {"th", "td"} and self.cell is not None:
            if self.capture_html:
                self.cell["html"].append(f"</{tag}>")
            self.cell["text"] = " ".join("".join(self.cell["text"]).split())
            self.cell["html"] = "".join(self.cell["html"])
            self.row.append(self.cell)
            self.cell = None
            self.in_cell = False
            self.capture_html = False
            self.active_link_index = None
            return
        if self.in_cell and tag == "a":
            self.active_link_index = None
        if self.in_cell and self.capture_html and self.cell is not None:
            self.cell["html"].append(f"</{tag}>")
            return
        if self.in_row and tag == "tr":
            if self.row and not self.skip_row:
                self.rows.append(self.row)
            self.row = []
            self.in_row = False
            return
        if self.in_table and tag == "table":
            self.table_depth -= 1
            if self.table_depth <= 0:
                self.in_table = False


def absolute_url(href: str) -> str:
    href = html.unescape(href or "")
    if not href:
        return ""
    if href.startswith("http"):
        return href
    if href.startswith("//"):
        return "https:" + href
    return urllib.parse.urljoin(PFR_BASE, href)


def pfr_id_from_url(url: str) -> str:
    if not url:
        return ""
    name = Path(urllib.parse.urlsplit(url).path).name
    return name.rsplit(".", 1)[0]


def find_next(page_html: str) -> str:
    for match in re.finditer(r"<a\b([^>]*)>(.*?)</a>", page_html, flags=re.IGNORECASE | re.DOTALL):
        attrs = match.group(1)
        text = re.sub(r"<[^>]+>", "", match.group(2))
        if "next" not in attrs.lower() and "Next Page" not in text:
            continue
        href_match = re.search(r'href="([^"]+)"', attrs)
        if href_match:
            return html.unescape(href_match.group(1))
    return ""


def parse_individual_plays(page_html: str, source_url: str) -> list[dict[str, Any]]:
    parser = TableParser("all_plays")
    parser.feed(page_html)
    rows = []
    for row_index, cells in enumerate(parser.rows):
        if cells and cells[0].get("data_stat") == "game_date" and cells[0].get("text") == "Date":
            continue
        row: dict[str, Any] = {
            "row_index_in_page": len(rows),
            "source_url": source_url,
        }
        for cell in cells:
            key = cell["data_stat"]
            if not key:
                continue
            row[key] = cell["text"]
            links = cell.get("links") or []
            if links:
                row[f"{key}_links_json"] = json.dumps(links, ensure_ascii=False)
                row[f"{key}_player_names"] = ";".join(link["text"] for link in links)
                row[f"{key}_pfr_player_ids"] = ";".join(pfr_id_from_url(link["href"]) for link in links)
                row[f"{key}_player_urls"] = ";".join(link["href"] for link in links)
                if key == "game_date":
                    row["boxscore_url"] = links[0]["href"]
                    row["boxscore_id"] = pfr_id_from_url(links[0]["href"])
                elif key in {"team", "opp"}:
                    row[f"{key}_stathead_id"] = pfr_id_from_url(links[0]["href"])
            if key == "description" and cell.get("html"):
                # Trim the wrapping td so downstream parsers can still use links.
                description_html = re.sub(r"^<td[^>]*>|</td>$", "", cell["html"], flags=re.IGNORECASE | re.DOTALL)
                row["description_html"] = description_html
        rows.append(row)
    return rows


async def get_cdp_context() -> dict[str, Any]:
    tabs = json.load(urllib.request.urlopen("http://127.0.0.1:9222/json"))
    version = json.load(urllib.request.urlopen("http://127.0.0.1:9222/json/version"))
    tab = next(
        t for t in tabs if t.get("type") == "page" and "sports-reference.com/stathead/football" in t.get("url", "")
    )
    async with websockets.connect(tab["webSocketDebuggerUrl"], max_size=25_000_000) as ws:
        msg_id = 0

        async def cdp(method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
            nonlocal msg_id
            msg_id += 1
            await ws.send(json.dumps({"id": msg_id, "method": method, "params": params or {}}))
            while True:
                msg = json.loads(await ws.recv())
                if msg.get("id") == msg_id:
                    return msg

        await cdp("Network.enable")
        cookies = (await cdp("Network.getAllCookies"))["result"]["cookies"]
    return {"user_agent": version["User-Agent"], "cookies": cookies}


def make_session(context: dict[str, Any]) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": context["user_agent"],
            "Referer": "https://www.sports-reference.com/stathead/football/play_finder.cgi",
        }
    )
    for cookie in context["cookies"]:
        if "sports-reference.com" in cookie.get("domain", ""):
            session.cookies.set(
                cookie["name"],
                cookie["value"],
                domain=cookie.get("domain"),
                path=cookie.get("path", "/"),
            )
    return session


def read_plan(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def filter_plan(rows: list[dict[str, str]], args: argparse.Namespace) -> list[dict[str, str]]:
    selected = rows
    if args.query_key:
        keys = set(args.query_key)
        selected = [row for row in selected if row["query_key"] in keys]
    if args.group_key:
        groups = set(args.group_key)
        selected = [row for row in selected if row["query_group_key"] in groups]
    if args.season:
        seasons = {str(season) for season in args.season}
        selected = [row for row in selected if row["season"] in seasons]
    if args.start_after:
        selected = [row for row in selected if row["query_key"] > args.start_after]
    if args.limit:
        selected = selected[: args.limit]
    return selected


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def scrape_query(session: requests.Session, query: dict[str, str], out_dir: Path, sleep: float) -> dict[str, Any]:
    query_key = query["query_key"]
    query_dir = out_dir / "queries"
    query_dir.mkdir(parents=True, exist_ok=True)
    page_url = query["url"]
    all_rows = []
    pages = []
    page_index = 0

    while page_url:
        started = time.time()
        response = fetch_with_retries(session, page_url)
        rows = parse_individual_plays(response.text, page_url)
        for row_index, row in enumerate(rows):
            row.update(
                {
                    "query_key": query_key,
                    "query_group_key": query["query_group_key"],
                    "season": query["season"],
                    "team_id": query["team_id"],
                    "opp_id": query["opp_id"],
                    "offense_team_label": query["offense_team_label"],
                    "defense_team_label": query["defense_team_label"],
                    "game_dates_expected": query["game_dates"],
                    "week_labels_expected": query["week_labels"],
                    "row_index_in_query": len(all_rows) + row_index,
                    "source_page_index": page_index,
                }
            )
        all_rows.extend(rows)
        pages.append(
            {
                "page_index": page_index,
                "rows": len(rows),
                "seconds": round(time.time() - started, 2),
                "url": page_url,
            }
        )
        next_href = find_next(response.text)
        page_url = urllib.parse.urljoin(page_url, next_href) if next_href else ""
        page_index += 1
        if page_url and sleep:
            time.sleep(sleep)

    query_csv = query_dir / f"{query_key}.csv"
    write_csv(query_csv, all_rows)
    meta = {
        "query_key": query_key,
        "query_group_key": query["query_group_key"],
        "rows": len(all_rows),
        "pages": pages,
        "csv": str(query_csv),
        "url": query["url"],
    }
    (query_dir / f"{query_key}.json").write_text(json.dumps(meta, indent=2), encoding="utf-8")
    return meta


def fetch_with_retries(
    session: requests.Session, url: str, retries: int = 4, base_sleep: float = 8.0
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=75)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(base_sleep * (2**attempt))
    assert last_error is not None
    raise last_error


def existing_query_meta(out_dir: Path, query_key: str) -> dict[str, Any] | None:
    meta_path = out_dir / "queries" / f"{query_key}.json"
    csv_path = out_dir / "queries" / f"{query_key}.csv"
    if not meta_path.exists() or not csv_path.exists():
        return None
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    meta["status"] = "skipped_existing"
    return meta


def append_progress(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["query_key", "query_group_key", "status", "rows", "csv", "seconds", "error", "url"]
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--query-key", action="append")
    parser.add_argument("--group-key", action="append")
    parser.add_argument("--season", type=int, action="append")
    parser.add_argument("--start-after")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--sleep", type=float, default=1.0)
    parser.add_argument("--combined-name", default="combined.csv")
    parser.add_argument("--skip-existing", action="store_true")
    parser.add_argument("--no-combined", action="store_true")
    parser.add_argument("--stop-on-error", action="store_true")
    args = parser.parse_args()

    plan_rows = read_plan(args.plan)
    selected = filter_plan(plan_rows, args)
    if not selected:
        raise SystemExit("No plan rows matched the requested filters.")

    context = asyncio.run(get_cdp_context())
    session = make_session(context)

    args.out_dir.mkdir(parents=True, exist_ok=True)
    metas = []
    combined_rows = []
    progress_path = args.out_dir / "progress.csv"
    for i, query in enumerate(selected, start=1):
        print(f"[{i}/{len(selected)}] scraping {query['query_key']}", flush=True)
        started = time.time()
        meta = existing_query_meta(args.out_dir, query["query_key"]) if args.skip_existing else None
        if meta:
            print(f"  skipped existing rows={meta.get('rows')} file={meta.get('csv')}", flush=True)
        else:
            try:
                meta = scrape_query(session, query, args.out_dir, args.sleep)
                meta["status"] = "ok"
                print(f"  rows={meta['rows']} file={meta['csv']}", flush=True)
            except Exception as exc:
                meta = {
                    "query_key": query["query_key"],
                    "query_group_key": query["query_group_key"],
                    "status": "error",
                    "rows": 0,
                    "csv": "",
                    "url": query["url"],
                    "error": repr(exc),
                }
                print(f"  ERROR {meta['error']}", flush=True)
                if args.stop_on_error:
                    append_progress(
                        progress_path,
                        {**meta, "seconds": round(time.time() - started, 2)},
                    )
                    raise
        metas.append(meta)
        append_progress(
            progress_path,
            {
                "query_key": meta.get("query_key", query["query_key"]),
                "query_group_key": meta.get("query_group_key", query["query_group_key"]),
                "status": meta.get("status", "ok"),
                "rows": meta.get("rows", 0),
                "csv": meta.get("csv", ""),
                "seconds": round(time.time() - started, 2),
                "error": meta.get("error", ""),
                "url": meta.get("url", query["url"]),
            },
        )
        if not args.no_combined and meta.get("csv"):
            with Path(meta["csv"]).open(newline="", encoding="utf-8") as handle:
                combined_rows.extend(csv.DictReader(handle))
        if i < len(selected) and args.sleep:
            time.sleep(args.sleep)

    combined_path = args.out_dir / args.combined_name
    if not args.no_combined:
        write_csv(combined_path, combined_rows)
    manifest = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "plan": str(args.plan),
        "selected_queries": len(selected),
        "ok_queries": sum(1 for meta in metas if meta.get("status") in {"ok", "skipped_existing"}),
        "error_queries": sum(1 for meta in metas if meta.get("status") == "error"),
        "total_rows": sum(int(meta.get("rows") or 0) for meta in metas),
        "combined_csv": "" if args.no_combined else str(combined_path),
        "progress_csv": str(progress_path),
        "queries": metas,
    }
    (args.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
