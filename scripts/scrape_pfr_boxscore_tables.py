"""Harvest PFR/Stathead game and boxscore tables into parquet datasets.

This script has two phases:

1. ``team-games`` scrapes a Stathead Team Game Finder URL and writes:
   - ``team_games_raw.parquet``: one row per team-game result row
   - ``boxscore_index.parquet``: one row per unique PFR boxscore URL

2. ``boxscores`` fetches PFR boxscore pages from that index and writes each
   discovered table id as its own appendable parquet dataset:
   ``tables/<table_id>/part-*.parquet``.

The downloader can reuse a logged-in Microsoft Edge session launched with
``--remote-debugging-port=9222``. That is the intended path for Stathead.
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
from collections import defaultdict
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import websockets


DEFAULT_TEAM_GAMES_URL = (
    "https://www.sports-reference.com/stathead/football/team-game-finder.cgi?"
    "request=1&order_by_asc=1&order_by=date&timeframe=seasons&year_min=1920&year_max=2025&comp_type=E"
)
DEFAULT_OUT_DIR = Path(r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized\pfr_boxscores")
PFR_BASE = "https://www.pro-football-reference.com"
SPORTS_REFERENCE_BASE = "https://www.sports-reference.com"
CDP_JSON_URL = "http://127.0.0.1:9222/json"
CDP_VERSION_URL = "http://127.0.0.1:9222/json/version"
_CDP_SCRAPE_TAB_WS_URL: str | None = None
_CDP_SCRAPE_TAB_ORIGIN: str | None = None
_CDP_SCRAPE_TAB_ID: str | None = None


def clean_text(value: str) -> str:
    return " ".join(html.unescape(value or "").split())


def absolute_url(href: str, base_url: str = PFR_BASE) -> str:
    href = html.unescape(href or "").strip()
    if not href:
        return ""
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("//"):
        return "https:" + href
    return urllib.parse.urljoin(base_url, href)


def id_from_url(url: str) -> str:
    if not url:
        return ""
    path = urllib.parse.urlsplit(url).path
    name = Path(path).name
    return name.rsplit(".", 1)[0]


def boxscore_id_from_url(url: str) -> str:
    boxscore_id = id_from_url(url)
    return boxscore_id if re.match(r"^\d{9}[a-z]{2,4}$", boxscore_id) else ""


def season_from_boxscore_id(boxscore_id: str) -> int | None:
    if not re.match(r"^\d{8}", boxscore_id or ""):
        return None
    year = int(boxscore_id[:4])
    month = int(boxscore_id[4:6])
    return year - 1 if month <= 2 else year


def date_from_boxscore_id(boxscore_id: str) -> str:
    if not re.match(r"^\d{8}", boxscore_id or ""):
        return ""
    return f"{boxscore_id[:4]}-{boxscore_id[4:6]}-{boxscore_id[6:8]}"


def home_stathead_id_from_boxscore_id(boxscore_id: str) -> str:
    if not re.match(r"^\d{9}", boxscore_id or ""):
        return ""
    return boxscore_id[9:]


def origin_from_url(url: str) -> str:
    parts = urllib.parse.urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.=-]+", "_", value or "").strip("_")
    return cleaned or "unknown"


def uncomment_pfr_tables(page_html: str) -> str:
    """PFR hides many tables inside HTML comments; preserve only table comments."""

    def replace_comment(match: re.Match[str]) -> str:
        body = match.group(1)
        return body if "<table" in body.lower() else ""

    return re.sub(r"<!--(.*?)-->", replace_comment, page_html, flags=re.DOTALL)


def extract_table_chunks(page_html: str) -> list[str]:
    clean = uncomment_pfr_tables(page_html)
    return [
        m.group(0)
        for m in re.finditer(r"<table\b.*?</table>", clean, flags=re.IGNORECASE | re.DOTALL)
        if re.search(r"""<table\b[^>]*\bid=['"][^'"]+['"]""", m.group(0), flags=re.IGNORECASE)
    ]


def find_next(page_html: str, source_url: str) -> str:
    for match in re.finditer(r"<a\b([^>]*)>(.*?)</a>", page_html, flags=re.IGNORECASE | re.DOTALL):
        attrs = match.group(1)
        text = clean_text(re.sub(r"<[^>]+>", "", match.group(2)))
        attrs_l = attrs.lower()
        if "next" not in attrs_l and text.lower() != "next page":
            continue
        href_match = re.search(r"""href=['"]([^'"]+)['"]""", attrs)
        if href_match:
            return absolute_url(href_match.group(1), source_url)
    return ""


def looks_like_browser_challenge(page_html: str) -> bool:
    lowered = page_html[:10_000].lower()
    return "just a moment" in lowered or "checking if the site connection is secure" in lowered


@dataclass
class ParsedTable:
    table_id: str
    caption: str
    rows: list[dict[str, Any]]


class GenericTableParser(HTMLParser):
    """Small dependency-free table parser that preserves PFR data-stat keys."""

    def __init__(self, source_url: str):
        super().__init__(convert_charrefs=True)
        self.source_url = source_url
        self.table_id = ""
        self.caption_parts: list[str] = []
        self.in_caption = False
        self.in_thead = False
        self.in_row = False
        self.row_class = ""
        self.row_attrs: dict[str, str] = {}
        self.in_cell = False
        self.cell: dict[str, Any] | None = None
        self.current_row: list[dict[str, Any]] = []
        self.header_candidates: list[list[str]] = []
        self.body_rows: list[dict[str, Any]] = []
        self.active_link_index: int | None = None

    def handle_starttag(self, tag: str, attrs_list: list[tuple[str, str | None]]) -> None:
        attrs = {k: v or "" for k, v in attrs_list}
        if tag == "table":
            self.table_id = attrs.get("id", "") or attrs.get("data-table-id", "")
            return
        if tag == "caption":
            self.in_caption = True
            return
        if tag == "thead":
            self.in_thead = True
            return
        if tag == "tr":
            self.in_row = True
            self.row_class = attrs.get("class", "")
            self.row_attrs = attrs
            self.current_row = []
            return
        if self.in_row and tag in {"th", "td"}:
            self.in_cell = True
            self.cell = {
                "tag": tag,
                "data_stat": attrs.get("data-stat", ""),
                "class": attrs.get("class", ""),
                "text_parts": [],
                "links": [],
            }
            return
        if self.in_cell and self.cell is not None and tag == "a":
            href = absolute_url(attrs.get("href", ""), self.source_url)
            if href:
                self.cell["links"].append({"text": "", "href": href, "id": id_from_url(href)})
                self.active_link_index = len(self.cell["links"]) - 1

    def handle_data(self, data: str) -> None:
        if self.in_caption:
            self.caption_parts.append(data)
        if self.in_cell and self.cell is not None:
            self.cell["text_parts"].append(data)
            if self.active_link_index is not None:
                self.cell["links"][self.active_link_index]["text"] += data

    def handle_endtag(self, tag: str) -> None:
        if self.in_cell and tag == "a":
            self.active_link_index = None
            return
        if self.in_cell and tag in {"th", "td"} and self.cell is not None:
            self.cell["text"] = clean_text("".join(self.cell.pop("text_parts", [])))
            for link in self.cell["links"]:
                link["text"] = clean_text(link.get("text", ""))
            self.current_row.append(self.cell)
            self.cell = None
            self.in_cell = False
            self.active_link_index = None
            return
        if tag == "caption":
            self.in_caption = False
            return
        if tag == "thead":
            self.in_thead = False
            return
        if tag == "tr" and self.in_row:
            self.finish_row()
            self.in_row = False

    def finish_row(self) -> None:
        if not self.current_row:
            return

        is_repeated_header = "thead" in self.row_class.split()
        data_stats = [
            cell["data_stat"]
            for cell in self.current_row
            if cell.get("data_stat") and "over_header" not in cell.get("class", "").split()
        ]
        all_header_cells = all(cell.get("tag") == "th" for cell in self.current_row)
        if self.in_thead or (all_header_cells and data_stats):
            if data_stats:
                self.header_candidates.append(data_stats)
            return
        if is_repeated_header:
            return

        row: dict[str, Any] = {
            "source_url": self.source_url,
            "table_id": self.table_id,
            "table_caption": clean_text("".join(self.caption_parts)),
            "row_index_in_table": len(self.body_rows),
            "tr_data_row": self.row_attrs.get("data-row", ""),
        }
        seen: dict[str, int] = {}
        for idx, cell in enumerate(self.current_row):
            key = cell.get("data_stat") or self.header_key(idx) or f"col_{idx}"
            if not key or "over_header" in cell.get("class", "").split():
                continue
            if key in seen:
                seen[key] += 1
                key = f"{key}_{seen[key]}"
            else:
                seen[key] = 0
            row[key] = cell.get("text", "")
            links = cell.get("links") or []
            if links:
                row[f"{key}_links_json"] = json.dumps(links, ensure_ascii=False)
                row[f"{key}_link_texts"] = ";".join(link.get("text", "") for link in links)
                row[f"{key}_link_ids"] = ";".join(link.get("id", "") for link in links)
                row[f"{key}_urls"] = ";".join(link.get("href", "") for link in links)
        self.body_rows.append(row)

    def header_key(self, idx: int) -> str:
        if not self.header_candidates:
            return ""
        header = max(self.header_candidates, key=len)
        return header[idx] if idx < len(header) else ""

    def parsed(self) -> ParsedTable:
        return ParsedTable(
            table_id=self.table_id or "unknown",
            caption=clean_text("".join(self.caption_parts)),
            rows=self.body_rows,
        )


def parse_tables(page_html: str, source_url: str) -> list[ParsedTable]:
    tables_by_id: dict[str, ParsedTable] = {}
    for chunk in extract_table_chunks(page_html):
        parser = GenericTableParser(source_url)
        parser.feed(chunk)
        parsed = parser.parsed()
        if not parsed.rows or parsed.table_id == "unknown":
            continue
        current = tables_by_id.get(parsed.table_id)
        if current is None or len(parsed.rows) > len(current.rows):
            tables_by_id[parsed.table_id] = parsed
    return list(tables_by_id.values())


async def get_cdp_context() -> dict[str, Any]:
    try:
        tabs = json.load(urllib.request.urlopen(CDP_JSON_URL, timeout=5))
        version = json.load(urllib.request.urlopen(CDP_VERSION_URL, timeout=5))
    except Exception as exc:
        raise RuntimeError(
            "Edge remote debugging is not available on 127.0.0.1:9222. "
            "Launch Edge with --remote-debugging-port=9222 and log in before running this scraper."
        ) from exc

    page_tabs = [tab for tab in tabs if tab.get("type") == "page" and tab.get("webSocketDebuggerUrl")]
    preferred = [
        tab
        for tab in page_tabs
        if "sports-reference.com" in tab.get("url", "") or "pro-football-reference.com" in tab.get("url", "")
    ]
    if not preferred and not page_tabs:
        raise RuntimeError("Edge remote debugging is running, but no debuggable page tabs were found.")
    tab = (preferred or page_tabs)[0]

    async with websockets.connect(tab["webSocketDebuggerUrl"], max_size=25_000_000, open_timeout=20) as ws:
        msg_id = 0

        async def cdp(
            method: str,
            params: dict[str, Any] | None = None,
            response_timeout: float = 20.0,
        ) -> dict[str, Any]:
            nonlocal msg_id
            msg_id += 1
            this_id = msg_id
            await ws.send(json.dumps({"id": this_id, "method": method, "params": params or {}}))
            deadline = time.time() + response_timeout
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for CDP response to {method}")
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(0.5, remaining)))
                if msg.get("id") == this_id:
                    return msg

        await cdp("Network.enable", response_timeout=20)
        cookies = (await cdp("Network.getAllCookies", response_timeout=20))["result"]["cookies"]
    return {"user_agent": version.get("User-Agent", ""), "cookies": cookies, "tab_url": tab.get("url", "")}


def create_cdp_scrape_tab() -> str:
    global _CDP_SCRAPE_TAB_WS_URL, _CDP_SCRAPE_TAB_ID
    if _CDP_SCRAPE_TAB_WS_URL:
        return _CDP_SCRAPE_TAB_WS_URL

    last_error: Exception | None = None
    for method in ("PUT", "GET"):
        try:
            request = urllib.request.Request(f"{CDP_JSON_URL}/new?about:blank", method=method)
            tab = json.load(urllib.request.urlopen(request, timeout=5))
            ws_url = tab.get("webSocketDebuggerUrl")
            if ws_url:
                _CDP_SCRAPE_TAB_WS_URL = ws_url
                _CDP_SCRAPE_TAB_ID = tab.get("id")
                try:
                    tabs = json.load(urllib.request.urlopen(CDP_JSON_URL, timeout=5))
                    for old_tab in tabs:
                        old_id = old_tab.get("id")
                        if (
                            old_tab.get("type") == "page"
                            and old_id
                            and old_id != _CDP_SCRAPE_TAB_ID
                            and "sports-reference.com" in str(old_tab.get("url", ""))
                        ):
                            urllib.request.urlopen(f"{CDP_JSON_URL}/close/{old_id}", timeout=2).read()
                except Exception:
                    pass
                return ws_url
        except Exception as exc:
            last_error = exc

    raise RuntimeError("Could not create or find a debuggable Edge page tab.") from last_error


def close_cdp_scrape_tab() -> None:
    global _CDP_SCRAPE_TAB_WS_URL, _CDP_SCRAPE_TAB_ORIGIN, _CDP_SCRAPE_TAB_ID
    if _CDP_SCRAPE_TAB_ID:
        try:
            tabs = json.load(urllib.request.urlopen(CDP_JSON_URL, timeout=2))
            page_tabs = [tab for tab in tabs if tab.get("type") == "page" and tab.get("webSocketDebuggerUrl")]
            if len(page_tabs) > 1:
                urllib.request.urlopen(f"{CDP_JSON_URL}/close/{_CDP_SCRAPE_TAB_ID}", timeout=2).read()
        except Exception:
            pass
    _CDP_SCRAPE_TAB_WS_URL = None
    _CDP_SCRAPE_TAB_ORIGIN = None
    _CDP_SCRAPE_TAB_ID = None


async def fetch_html_via_cdp_async(url: str, timeout: float = 45.0) -> str:
    global _CDP_SCRAPE_TAB_ORIGIN
    ws_url = create_cdp_scrape_tab()
    async with websockets.connect(ws_url, max_size=50_000_000, open_timeout=20) as ws:
        msg_id = 0

        async def cdp(
            method: str,
            params: dict[str, Any] | None = None,
            response_timeout: float = 60.0,
        ) -> dict[str, Any]:
            nonlocal msg_id
            msg_id += 1
            this_id = msg_id
            await ws.send(json.dumps({"id": msg_id, "method": method, "params": params or {}}))
            deadline = time.time() + response_timeout
            while True:
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(f"Timed out waiting for CDP response to {method}")
                msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(0.5, remaining)))
                if msg.get("id") == this_id:
                    return msg

        await cdp("Page.enable", response_timeout=20)
        await cdp("Network.enable", response_timeout=20)
        await cdp(
            "Network.setBlockedURLs",
            {
                "urls": [
                    "*.avif",
                    "*.gif",
                    "*.jpeg",
                    "*.jpg",
                    "*.png",
                    "*.webp",
                    "*://*.adform.net/*",
                    "*://*.adnxs.com/*",
                    "*://*.adsrvr.org/*",
                    "*://*.criteo.com/*",
                    "*://*.doubleclick.net/*",
                    "*://*.openx.net/*",
                    "*://*.pubmatic.com/*",
                    "*://*.rubiconproject.com/*",
                    "*://*.yieldmo.com/*",
                ]
            },
            response_timeout=20,
        )
        await cdp("Runtime.enable", response_timeout=20)
        target_origin = origin_from_url(url)
        if _CDP_SCRAPE_TAB_ORIGIN != target_origin:
            await navigate_cdp_page(cdp, ws, target_origin + "/", timeout=timeout)
            _CDP_SCRAPE_TAB_ORIGIN = target_origin

        if "sports-reference.com/cfb/" in url:
            return await navigate_and_read_html(cdp, ws, url, timeout=timeout)

        fetched = await fetch_html_via_browser_fetch(cdp, url)
        if fetched and not looks_like_browser_challenge(fetched):
            return fetched

        # Browser fetch can be blocked by site policy or return a challenge in
        # some sessions. Fall back to full navigation, which is slower but uses
        # the same logged-in browser context.
        return await navigate_and_read_html(cdp, ws, url, timeout=timeout)


async def navigate_cdp_page(
    cdp,
    ws,
    url: str,
    timeout: float = 90.0,
) -> None:
    nav = await cdp("Page.navigate", {"url": url}, response_timeout=30)
    if nav.get("result", {}).get("errorText"):
        raise RuntimeError(f"CDP navigation failed: {nav['result']['errorText']}")

    deadline = time.time() + timeout
    loaded = False
    while time.time() < deadline:
        try:
            msg = json.loads(await asyncio.wait_for(ws.recv(), timeout=max(0.5, deadline - time.time())))
        except TimeoutError:
            break
        if msg.get("method") == "Page.loadEventFired":
            loaded = True
            break
    if not loaded:
        # Some challenge/interstitial pages never emit a normal load event.
        # Try to read the DOM anyway; Runtime.evaluate will fail if unusable.
        pass

    ready_deadline = time.time() + min(10.0, max(0.0, deadline - time.time()))
    while time.time() < ready_deadline:
        result = await cdp(
            "Runtime.evaluate",
            {"expression": "document.readyState", "returnByValue": True},
            response_timeout=10,
        )
        state = result.get("result", {}).get("result", {}).get("value")
        if state in {"interactive", "complete"}:
            break
        await asyncio.sleep(0.25)


async def fetch_html_via_browser_fetch(cdp, url: str) -> str:
    expression = f"""
    (async () => {{
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 25000);
      try {{
        const response = await fetch({json.dumps(url)}, {{
          credentials: "include",
          cache: "no-store",
          signal: controller.signal
        }});
        const text = await response.text();
        return JSON.stringify({{
          ok: response.ok,
          status: response.status,
          url: response.url,
          text
        }});
      }} finally {{
        clearTimeout(timer);
      }}
    }})()
    """
    result = await cdp(
        "Runtime.evaluate",
        {
            "expression": expression,
            "returnByValue": True,
            "awaitPromise": True,
        },
        response_timeout=35,
    )
    if "exceptionDetails" in result:
        return ""
    raw = result.get("result", {}).get("result", {}).get("value") or ""
    if not raw:
        return ""
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return ""
    if int(payload.get("status") or 0) >= 400:
        return ""
    return payload.get("text") or ""


async def navigate_and_read_html(cdp, ws, url: str, timeout: float = 90.0) -> str:
    await navigate_cdp_page(cdp, ws, url, timeout=timeout)

    result = await cdp(
        "Runtime.evaluate",
        {
            "expression": "document.documentElement.outerHTML",
            "returnByValue": True,
            "awaitPromise": True,
        },
        response_timeout=30,
    )
    if "exceptionDetails" in result:
        raise RuntimeError(f"CDP DOM extraction failed: {result['exceptionDetails']}")
    return result.get("result", {}).get("result", {}).get("value") or ""


def fetch_html_via_cdp(url: str) -> str:
    return asyncio.run(asyncio.wait_for(fetch_html_via_cdp_async(url), timeout=75.0))


def fetch_html_via_cdp_with_retries(
    url: str,
    retries: int = 1,
    base_sleep: float = 1.0,
) -> str:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            return fetch_html_via_cdp(url)
        except Exception as exc:
            last_error = exc
            if attempt >= retries:
                break
            # Force a fresh debuggable tab on the next attempt. Edge can leave
            # the previous CDP socket half-open after a transient handshake or
            # Runtime.evaluate timeout.
            close_cdp_scrape_tab()
            time.sleep(base_sleep * (2**attempt))
    assert last_error is not None
    raise last_error


def make_session(use_edge_cdp: bool, referer: str = PFR_BASE) -> requests.Session:
    session = requests.Session()
    user_agent = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) " "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
    if use_edge_cdp:
        context = asyncio.run(get_cdp_context())
        user_agent = context.get("user_agent") or user_agent
        for cookie in context["cookies"]:
            domain = cookie.get("domain", "")
            if "sports-reference.com" in domain or "pro-football-reference.com" in domain:
                session.cookies.set(
                    cookie["name"],
                    cookie["value"],
                    domain=domain,
                    path=cookie.get("path", "/"),
                )
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": referer,
        }
    )
    return session


def fetch_with_retries(
    session: requests.Session,
    url: str,
    retries: int = 4,
    base_sleep: float = 8.0,
) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            response = session.get(url, timeout=75)
            if response.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable HTTP {response.status_code}", response=response)
            if response.status_code >= 400:
                response.raise_for_status()
            response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            response = getattr(exc, "response", None)
            status = getattr(response, "status_code", None)
            if status is not None and status not in {429, 500, 502, 503, 504}:
                break
            if attempt >= retries:
                break
            time.sleep(base_sleep * (2**attempt))
    assert last_error is not None
    raise last_error


def challenge_retries(args: argparse.Namespace) -> int:
    return max(0, int(getattr(args, "challenge_retries", 3) or 0))


def challenge_retry_sleep(args: argparse.Namespace, attempt: int) -> float:
    base = float(getattr(args, "challenge_sleep", 60.0) or 0.0)
    return base * (attempt + 1)


def _fetch_html_once(session: requests.Session, url: str, args: argparse.Namespace) -> str:
    mode = getattr(args, "fetch_mode", "auto")
    if mode not in {"auto", "requests", "cdp"}:
        raise ValueError(f"Unknown fetch mode: {mode}")
    if mode == "cdp":
        if getattr(args, "no_edge_cdp", False):
            raise RuntimeError("--fetch-mode cdp cannot be used with --no-edge-cdp")
        return fetch_html_via_cdp_with_retries(url)

    try:
        response = fetch_with_retries(session, url)
        if mode == "auto" and looks_like_browser_challenge(response.text) and not getattr(args, "no_edge_cdp", False):
            print("  requests returned a browser challenge; falling back to Edge CDP", flush=True)
            return fetch_html_via_cdp_with_retries(url)
        return response.text
    except requests.HTTPError as exc:
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if mode == "auto" and status == 403 and not getattr(args, "no_edge_cdp", False):
            print("  requests returned HTTP 403; falling back to Edge CDP", flush=True)
            return fetch_html_via_cdp_with_retries(url)
        raise


def fetch_html(session: requests.Session, url: str, args: argparse.Namespace) -> str:
    retries = challenge_retries(args)
    for attempt in range(retries + 1):
        page_html = _fetch_html_once(session, url, args)
        if not looks_like_browser_challenge(page_html):
            return page_html
        if attempt >= retries:
            break
        delay = challenge_retry_sleep(args, attempt)
        print(
            f"  browser challenge returned; waiting {delay:g}s before retry {attempt + 1}/{retries}",
            flush=True,
        )
        if delay > 0:
            time.sleep(delay)
    raise RuntimeError("Browser challenge page returned instead of PFR/Stathead HTML")


def frame_for_parquet(rows: list[dict[str, Any]]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for col in df.columns:
        df[col] = df[col].map(
            lambda value: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
        )
    return df.convert_dtypes()


def write_parquet(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame_for_parquet(rows).to_parquet(path, index=False)


def append_progress(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "kind",
        "key",
        "status",
        "rows",
        "tables",
        "seconds",
        "error",
        "url",
    ]
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def extract_boxscore_url(row: dict[str, Any]) -> str:
    for key, value in row.items():
        if not key.endswith("_urls") or not isinstance(value, str):
            continue
        for candidate in value.split(";"):
            if "/boxscores/" in candidate and candidate.endswith(".htm"):
                return absolute_url(candidate, PFR_BASE)
    return ""


def add_boxscore_metadata(row: dict[str, Any], boxscore_url: str) -> dict[str, Any]:
    boxscore_id = boxscore_id_from_url(boxscore_url)
    return {
        **row,
        "boxscore_url": boxscore_url,
        "boxscore_id": boxscore_id,
        "game_date": date_from_boxscore_id(boxscore_id),
        "season": season_from_boxscore_id(boxscore_id),
        "home_stathead_id": home_stathead_id_from_boxscore_id(boxscore_id),
    }


def scrape_team_games(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    session = make_session(not args.no_edge_cdp, referer=SPORTS_REFERENCE_BASE)
    progress_path = out_dir / "progress.csv"

    page_url = args.team_games_url
    page_index = 0
    all_rows: list[dict[str, Any]] = []
    while page_url:
        if args.max_pages is not None and page_index >= args.max_pages:
            break
        started = time.time()
        print(f"[team-games] page {page_index + 1}: {page_url}", flush=True)
        try:
            page_html = fetch_html(session, page_url, args)
            tables = parse_tables(page_html, page_url)
            page_rows = []
            for table in tables:
                for row in table.rows:
                    boxscore_url = extract_boxscore_url(row)
                    if boxscore_url:
                        page_rows.append(
                            add_boxscore_metadata(
                                {
                                    **row,
                                    "source_page_index": page_index,
                                    "source_page_url": page_url,
                                },
                                boxscore_url,
                            )
                        )
            all_rows.extend(page_rows)
            append_progress(
                progress_path,
                {
                    "kind": "team_games_page",
                    "key": str(page_index),
                    "status": "ok",
                    "rows": len(page_rows),
                    "seconds": round(time.time() - started, 2),
                    "url": page_url,
                },
            )
            print(f"  rows with boxscores: {len(page_rows)}", flush=True)
            next_url = find_next(page_html, page_url)
            page_url = next_url
            page_index += 1
            if page_url and args.sleep:
                time.sleep(args.sleep)
        except Exception as exc:
            append_progress(
                progress_path,
                {
                    "kind": "team_games_page",
                    "key": str(page_index),
                    "status": "error",
                    "rows": 0,
                    "seconds": round(time.time() - started, 2),
                    "error": repr(exc),
                    "url": page_url,
                },
            )
            raise

    if not all_rows:
        raise SystemExit("No team-game rows with boxscore URLs were found.")

    raw_path = out_dir / "team_games_raw.parquet"
    write_parquet(raw_path, all_rows)

    index_rows_by_id: dict[str, dict[str, Any]] = {}
    for row in all_rows:
        boxscore_id = row.get("boxscore_id") or ""
        if boxscore_id and boxscore_id not in index_rows_by_id:
            index_rows_by_id[boxscore_id] = {
                "boxscore_id": boxscore_id,
                "boxscore_url": row.get("boxscore_url", ""),
                "game_date": row.get("game_date", ""),
                "season": row.get("season"),
                "home_stathead_id": row.get("home_stathead_id", ""),
                "first_seen_table_id": row.get("table_id", ""),
                "first_seen_source_page": row.get("source_page_url", ""),
            }
    index_rows = sorted(
        index_rows_by_id.values(), key=lambda r: (str(r.get("game_date", "")), str(r.get("boxscore_id", "")))
    )
    index_path = out_dir / "boxscore_index.parquet"
    write_parquet(index_path, index_rows)

    manifest = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "team_games_url": args.team_games_url,
        "pages_scraped": page_index,
        "team_game_rows": len(all_rows),
        "unique_boxscores": len(index_rows),
        "team_games_raw": str(raw_path),
        "boxscore_index": str(index_path),
        "progress": str(progress_path),
    }
    (out_dir / "team_games_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2), flush=True)


def read_boxscore_inputs(args: argparse.Namespace) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if args.url:
        for url in args.url:
            full_url = absolute_url(url, PFR_BASE)
            boxscore_id = boxscore_id_from_url(full_url)
            rows.append(
                {
                    "boxscore_id": boxscore_id,
                    "boxscore_url": full_url,
                    "game_date": date_from_boxscore_id(boxscore_id),
                    "season": season_from_boxscore_id(boxscore_id),
                    "home_stathead_id": home_stathead_id_from_boxscore_id(boxscore_id),
                }
            )
    else:
        index_path = args.index or args.out_dir / "boxscore_index.parquet"
        if not index_path.exists():
            raise SystemExit(f"Missing boxscore index: {index_path}")
        df = pd.read_parquet(index_path)
        rows.extend(df.to_dict("records"))

    if args.start_after:
        rows = [row for row in rows if str(row.get("boxscore_id", "")) > args.start_after]
    if args.season:
        seasons = {int(v) for v in args.season}
        rows = [row for row in rows if row.get("season") in seasons]
    if args.limit:
        rows = rows[: args.limit]
    return rows


def successful_boxscores(progress_path: Path) -> set[str]:
    if not progress_path.exists():
        return set()
    with progress_path.open(newline="", encoding="utf-8") as handle:
        return {
            row["key"]
            for row in csv.DictReader(handle)
            if row.get("kind") == "boxscore"
            and row.get("status") == "ok"
            and row.get("key")
            and int(row.get("rows") or 0) > 0
            and int(row.get("tables") or 0) > 0
        }


def completed_boxscores_path(out_dir: Path) -> Path:
    return out_dir / "boxscore_done.csv"


def append_completed_boxscore(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "boxscore_id",
        "status",
        "rows",
        "tables",
        "part_files",
        "table_ids",
        "seconds",
        "completed_at_utc",
        "url",
    ]
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow(row)


def expected_table_counts(progress_path: Path) -> dict[str, int]:
    if not progress_path.exists():
        return {}
    expected: dict[str, int] = {}
    with progress_path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("kind") != "boxscore" or row.get("status") != "ok" or not row.get("key"):
                continue
            tables = int(row.get("tables") or 0)
            rows = int(row.get("rows") or 0)
            if tables > 0 and rows > 0:
                expected[row["key"]] = max(expected.get(row["key"], 0), tables)
    return expected


def completed_boxscores_from_done_file(path: Path) -> set[str]:
    if not path.exists():
        return set()
    with path.open(newline="", encoding="utf-8") as handle:
        return {
            row["boxscore_id"]
            for row in csv.DictReader(handle)
            if row.get("boxscore_id")
            and row.get("status") == "ok"
            and int(row.get("rows") or 0) > 0
            and int(row.get("tables") or 0) > 0
        }


def parquet_table_part_files(table_root: Path) -> list[Path]:
    if not table_root.exists():
        return []
    files = []
    for table_dir in sorted(path for path in table_root.iterdir() if path.is_dir()):
        files.extend(sorted(table_dir.glob("part-*.parquet")))
        files.extend(sorted(table_dir.glob("boxscore_id=*.parquet")))
    return files


def completed_boxscores_from_parquet(out_dir: Path, progress_path: Path) -> set[str]:
    """Find boxscores that are durably present in table parquet files.

    This deliberately does not trust progress.csv by itself. A process can die
    after appending progress but before flushing a batch. For older batch files,
    require at least the progress-reported number of distinct table ids for that
    boxscore before considering it durable.
    """

    expected = expected_table_counts(progress_path)
    found_tables: dict[str, set[str]] = defaultdict(set)
    for path in parquet_table_part_files(out_dir / "tables"):
        table_id = path.parent.name
        if path.name.startswith("boxscore_id="):
            boxscore_id = path.stem.split("=", 1)[1]
            if boxscore_id:
                found_tables[boxscore_id].add(table_id)
            continue
        try:
            df = pd.read_parquet(path, columns=["boxscore_id", "table_id"])
        except Exception:
            try:
                df = pd.read_parquet(path, columns=["boxscore_id"])
                df["table_id"] = table_id
            except Exception:
                continue
        if df.empty or "boxscore_id" not in df.columns:
            continue
        if "table_id" not in df.columns:
            df["table_id"] = table_id
        distinct = df[["boxscore_id", "table_id"]].dropna().drop_duplicates()
        for boxscore_id, group in distinct.groupby("boxscore_id"):
            if boxscore_id:
                found_tables[str(boxscore_id)].update(str(v) for v in group["table_id"].dropna())

    durable = set()
    for boxscore_id, table_ids in found_tables.items():
        required = expected.get(boxscore_id, 1)
        if len(table_ids) >= required:
            durable.add(boxscore_id)
    return durable


def durable_boxscores(out_dir: Path, progress_path: Path) -> set[str]:
    done_ids = completed_boxscores_from_done_file(completed_boxscores_path(out_dir))
    parquet_ids = completed_boxscores_from_parquet(out_dir, progress_path)
    return done_ids | parquet_ids


def boxscore_metadata(input_row: dict[str, Any]) -> dict[str, Any]:
    boxscore_url = absolute_url(str(input_row.get("boxscore_url") or ""), PFR_BASE)
    boxscore_id = str(input_row.get("boxscore_id") or boxscore_id_from_url(boxscore_url))
    return {
        "boxscore_id": boxscore_id,
        "boxscore_url": boxscore_url,
        "game_date": str(input_row.get("game_date") or date_from_boxscore_id(boxscore_id)),
        "season": input_row.get("season")
        if pd.notna(input_row.get("season"))
        else season_from_boxscore_id(boxscore_id),
        "home_stathead_id": str(input_row.get("home_stathead_id") or home_stathead_id_from_boxscore_id(boxscore_id)),
    }


def scrape_one_boxscore(
    session: requests.Session,
    input_row: dict[str, Any],
    args: argparse.Namespace,
) -> dict[str, list[dict[str, Any]]]:
    meta = boxscore_metadata(input_row)
    page_html = fetch_html(session, meta["boxscore_url"], args)
    rows_by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for table in parse_tables(page_html, meta["boxscore_url"]):
        table_id = table.table_id or "unknown"
        for row in table.rows:
            rows_by_table[table_id].append({**meta, **row, "table_id": table_id, "table_caption": table.caption})
    if not rows_by_table or sum(len(rows) for rows in rows_by_table.values()) == 0:
        raise RuntimeError("No PFR stat tables parsed from boxscore HTML")
    return rows_by_table


def write_boxscore_batch(
    out_dir: Path,
    rows_by_table: dict[str, list[dict[str, Any]]],
    batch_key: str,
) -> list[dict[str, Any]]:
    manifest_rows = []
    for table_id, rows in sorted(rows_by_table.items()):
        if not rows:
            continue
        table_dir = out_dir / "tables" / safe_name(table_id)
        part_path = table_dir / f"part-{batch_key}.parquet"
        write_parquet(part_path, rows)
        manifest_rows.append(
            {
                "table_id": table_id,
                "rows": len(rows),
                "part_path": str(part_path),
                "batch_key": batch_key,
            }
        )
    return manifest_rows


def write_boxscore_parts(
    out_dir: Path,
    rows_by_table: dict[str, list[dict[str, Any]]],
    boxscore_id: str,
) -> list[dict[str, Any]]:
    manifest_rows = []
    for table_id, rows in sorted(rows_by_table.items()):
        if not rows:
            continue
        table_dir = out_dir / "tables" / safe_name(table_id)
        part_path = table_dir / f"boxscore_id={safe_name(boxscore_id)}.parquet"
        write_parquet(part_path, rows)
        manifest_rows.append(
            {
                "boxscore_id": boxscore_id,
                "table_id": table_id,
                "rows": len(rows),
                "part_path": str(part_path),
                "part_key": boxscore_id,
            }
        )
    return manifest_rows


def scrape_boxscores(args: argparse.Namespace) -> None:
    out_dir = args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    session = make_session(not args.no_edge_cdp, referer=PFR_BASE)
    progress_path = out_dir / "progress.csv"
    done = durable_boxscores(out_dir, progress_path) if args.skip_existing else set()
    inputs = read_boxscore_inputs(args)
    if not inputs:
        raise SystemExit("No boxscore inputs matched the requested filters.")

    all_manifest_rows: list[dict[str, Any]] = []

    for i, input_row in enumerate(inputs, start=1):
        meta = boxscore_metadata(input_row)
        boxscore_id = meta["boxscore_id"]
        if not boxscore_id or not meta["boxscore_url"]:
            continue
        if boxscore_id in done:
            print(f"[{i}/{len(inputs)}] skip existing {boxscore_id}", flush=True)
            continue

        started = time.time()
        print(f"[{i}/{len(inputs)}] boxscore {boxscore_id}", flush=True)
        try:
            rows_by_table = scrape_one_boxscore(session, meta, args)
            table_count = len(rows_by_table)
            row_count = sum(len(rows) for rows in rows_by_table.values())
            part_rows = write_boxscore_parts(out_dir, rows_by_table, boxscore_id)
            all_manifest_rows.extend(part_rows)
            append_completed_boxscore(
                completed_boxscores_path(out_dir),
                {
                    "boxscore_id": boxscore_id,
                    "status": "ok",
                    "rows": row_count,
                    "tables": table_count,
                    "part_files": len(part_rows),
                    "table_ids": ";".join(sorted(rows_by_table)),
                    "seconds": round(time.time() - started, 2),
                    "completed_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                    "url": meta["boxscore_url"],
                },
            )
            append_progress(
                progress_path,
                {
                    "kind": "boxscore",
                    "key": boxscore_id,
                    "status": "ok",
                    "rows": row_count,
                    "tables": table_count,
                    "seconds": round(time.time() - started, 2),
                    "url": meta["boxscore_url"],
                },
            )
            print(f"  tables={table_count} rows={row_count}", flush=True)
        except Exception as exc:
            append_progress(
                progress_path,
                {
                    "kind": "boxscore",
                    "key": boxscore_id,
                    "status": "error",
                    "rows": 0,
                    "tables": 0,
                    "seconds": round(time.time() - started, 2),
                    "error": repr(exc),
                    "url": meta["boxscore_url"],
                },
            )
            print(f"  ERROR {repr(exc)}", flush=True)
            if args.stop_on_error:
                raise

        if i < len(inputs) and args.sleep:
            time.sleep(args.sleep)

    if all_manifest_rows:
        run_manifest_path = out_dir / f"boxscore_table_parts_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}.parquet"
        write_parquet(run_manifest_path, all_manifest_rows)
    summary = {
        "generated_at_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "inputs": len(inputs),
        "part_rows": len(all_manifest_rows),
        "progress": str(progress_path),
        "tables_dir": str(out_dir / "tables"),
    }
    (out_dir / "boxscore_scrape_manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2), flush=True)


def compact_tables(args: argparse.Namespace) -> None:
    table_root = args.out_dir / "tables"
    if not table_root.exists():
        raise SystemExit(f"Missing tables directory: {table_root}")
    selected_tables = {safe_name(table) for table in getattr(args, "table", None) or []}
    compacted = []
    for table_dir in sorted(path for path in table_root.iterdir() if path.is_dir()):
        if selected_tables and table_dir.name not in selected_tables:
            continue
        part_files = sorted(table_dir.glob("part-*.parquet")) + sorted(table_dir.glob("boxscore_id=*.parquet"))
        if not part_files:
            continue
        frames = [pd.read_parquet(path) for path in part_files]
        df = pd.concat(frames, ignore_index=True)
        if {"boxscore_id", "row_index_in_table"}.issubset(df.columns):
            subset = ["boxscore_id", "row_index_in_table"]
            if "table_id" in df.columns:
                subset.append("table_id")
            df = df.drop_duplicates(subset=subset, keep="last")
        out_path = table_dir / "_combined.parquet"
        df.to_parquet(out_path, index=False)
        compacted.append({"table_id": table_dir.name, "rows": len(df), "path": str(out_path)})
        print(f"{table_dir.name}: {len(df):,} rows -> {out_path}", flush=True)

    manifest_rows = []
    for combined_path in sorted(table_root.glob("*/_combined.parquet")):
        table_id = combined_path.parent.name
        try:
            import pyarrow.parquet as pq

            row_count = pq.ParquetFile(combined_path).metadata.num_rows
        except Exception:
            row_count = len(pd.read_parquet(combined_path, columns=[]))
        manifest_rows.append({"table_id": table_id, "rows": int(row_count), "path": str(combined_path)})
    if manifest_rows:
        write_parquet(args.out_dir / "compact_manifest.parquet", manifest_rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    team_games = sub.add_parser("team-games", help="Scrape Stathead Team Game Finder into parquet.")
    team_games.add_argument("--team-games-url", default=DEFAULT_TEAM_GAMES_URL)
    team_games.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    team_games.add_argument("--sleep", type=float, default=4.0)
    team_games.add_argument("--max-pages", type=int)
    team_games.add_argument("--fetch-mode", choices=["auto", "requests", "cdp"], default="auto")
    team_games.add_argument("--challenge-retries", type=int, default=3)
    team_games.add_argument("--challenge-sleep", type=float, default=60.0)
    team_games.add_argument("--no-edge-cdp", action="store_true")
    team_games.set_defaults(func=scrape_team_games)

    boxscores = sub.add_parser("boxscores", help="Scrape PFR boxscore tables into per-table parquet datasets.")
    boxscores.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    boxscores.add_argument("--index", type=Path)
    boxscores.add_argument("--url", action="append", help="Scrape a specific boxscore URL; can be repeated.")
    boxscores.add_argument("--season", type=int, action="append")
    boxscores.add_argument("--start-after")
    boxscores.add_argument("--limit", type=int)
    boxscores.add_argument("--batch-size", type=int, default=25, help="Legacy option; writes are now per-boxscore.")
    boxscores.add_argument("--sleep", type=float, default=4.0)
    boxscores.add_argument("--fetch-mode", choices=["auto", "requests", "cdp"], default="auto")
    boxscores.add_argument("--challenge-retries", type=int, default=3)
    boxscores.add_argument("--challenge-sleep", type=float, default=60.0)
    boxscores.add_argument("--skip-existing", action="store_true")
    boxscores.add_argument("--stop-on-error", action="store_true")
    boxscores.add_argument("--no-edge-cdp", action="store_true")
    boxscores.set_defaults(func=scrape_boxscores)

    compact = sub.add_parser("compact", help="Create _combined.parquet for every table dataset.")
    compact.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    compact.add_argument("--table", action="append", help="Compact only one table id; can be repeated.")
    compact.set_defaults(func=compact_tables)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
