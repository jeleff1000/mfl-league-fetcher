#!/usr/bin/env python3
"""Local, read-only Yahoo web-session quick import for KMFFL 2025.

This is deliberately a web-page acquisition tool, not an OAuth/API replacement.
It uses a local browser cookie export to save authenticated Yahoo pages and
source tables into a local DuckDB file.  When Fly credentials are available it
compares the extracted standings and draft against Fly's ``kmffl`` rows and
exits non-zero on any mismatch.  Derived application fields are intentionally
not presented as web-page parity.

Example::

    python scripts/quick_import_kmffl_2025_web.py \
      --cookie-jar C:\\Users\\joeye\\Downloads\\cookiejar.json \
      --output-dir D:\\yahoo-local\\kmffl-2025

Cookie files and raw HTML must stay outside the repository.  Cookies are held
in memory only and are never written to the manifest, DuckDB, logs, Fly, or
the hosted application.
"""

from __future__ import annotations

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import html as html_lib
import hashlib
import json
import numbers
import os
import re
import sys
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urljoin, urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import requests

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except AttributeError:
    pass


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEAGUE_KEY = "461.l.90939"
DEFAULT_YEAR = 2025
DEFAULT_DB_NAME = "kmffl"
# Empirically safe provisional web-session pacing: Yahoo returned HTTP 200 for
# serialized probes down to zero configured delay, including roster/draft/
# transaction pages. Keep a 0.5s floor for production imports so the worker
# does not intentionally run at the network's maximum throughput. Any 999
# response still stops/retries through fetch_cached_page.
DEFAULT_REQUEST_DELAY = 0.5
# A live 2025 worker run hit HTTP 999 after 147 successful uncached pages;
# retrying after 60s still returned 999, while a several-minute cooldown
# cleared the session. Use a conservative bounded recovery ladder.
# Browser-cookie sessions should fail fast when Yahoo's web anti-automation
# layer returns 999.  The old API-style 60/120/300s sequence made a single
# rejected page look like a hung import and consumed the entire worker window.
THROTTLE_BACKOFF_SECONDS = (5.0, 15.0, 30.0)
KMFFL_LEAGUE_KEYS = {
    2015: "348.l.89552",
    2016: "359.l.257287",
    2017: "371.l.8307",
    2018: "380.l.1872",
    2019: "390.l.107505",
    2020: "399.l.3642",
    2021: "406.l.38187",
    2022: "414.l.413370",
    2023: "423.l.109695",
    2024: "449.l.198278",
    2025: "461.l.90939",
}
YAHOO_DEFENSE_IDS = {
    "arizona": "100022",
    "atlanta": "100001",
    "baltimore": "100033",
    "buffalo": "100002",
    "carolina": "100029",
    "chicago": "100003",
    "cincinnati": "100004",
    "cleveland": "100005",
    "dallas": "100006",
    "denver": "100007",
    "detroit": "100008",
    "green-bay": "100009",
    "houston": "100034",
    "indianapolis": "100011",
    "jacksonville": "100030",
    "kansas-city": "100012",
    "las-vegas": "100013",
    "la-chargers": "100024",
    "la-rams": "100014",
    "miami": "100015",
    "minnesota": "100016",
    "new-england": "100017",
    "new-orleans": "100018",
    "ny-giants": "100019",
    "ny-jets": "100020",
    "philadelphia": "100021",
    "pittsburgh": "100023",
    "san-francisco": "100025",
    "seattle": "100026",
    "tampa-bay": "100027",
    "tennessee": "100010",
    "washington": "100028",
}
FLY_PARITY_TABLES = (
    "league_settings",
    "matchup",
    "player_fantasy",
    "draft",
    "transactions",
    "schedule",
)
PAGE_MODULES = (
    "home",
    "settings",
    "standings",
    "matchups",
    "roster",
    "draft",
    "transactions",
)
PAGE_PATHS = {
    "home": "",
    "settings": "/settings",
    # This legacy module query is the server-rendered standings table; the
    # newer /standings route serves the client-side team-stats shell.
    "standings": "?module=standings&lhst=stand",
    "matchups": "/matchup",
    "roster": "/roster",
    "draft": "/draftresults",
    "transactions": "/transactions",
}
YAHOO_MY_LEAGUES_URL = "https://football.fantasysports.yahoo.com/f1/myleagues"
LOGIN_HOSTS = {"login.yahoo.com", "login.yahoo.net"}
ROSTER_FETCH_WORKERS = 4
MIN_COOKIE_REQUEST_INTERVAL = 1.0


class YahooThrottleError(RuntimeError):
    """Yahoo rejected a request with its anti-automation throttle response."""


@dataclass(frozen=True)
class StandingComparison:
    matches: bool
    differences: list[str]


@dataclass(frozen=True)
class PageResult:
    module: str
    url: str
    final_url: str
    status: int
    content_type: str
    bytes: int
    sha256: str
    output_file: str


class RequestPacer:
    """Serialize request starts across concurrent cookie-page fetches."""

    def __init__(self, interval_seconds: float) -> None:
        self.interval_seconds = max(0.0, float(interval_seconds))
        self._lock = threading.Lock()
        self._next_allowed = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait_seconds = max(0.0, self._next_allowed - now)
            self._next_allowed = max(now, self._next_allowed) + self.interval_seconds
        if wait_seconds:
            time.sleep(wait_seconds)


def validate_cookie_path(path: Path, repository_root: Path = PROJECT_ROOT) -> Path:
    """Require an explicitly supplied cookie file outside the repository."""
    resolved = path.expanduser().resolve()
    root = repository_root.expanduser().resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        pass
    else:
        raise ValueError("cookie jar must be outside the repository")
    if not resolved.is_file():
        raise FileNotFoundError(f"cookie jar does not exist: {resolved}")
    return resolved


def _cookie_records(raw: str) -> list[dict[str, str]]:
    """Read Netscape/tab exports or common JSON cookie exports."""
    stripped = raw.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            # Browser exports occasionally wrap a long cookie value inside a
            # JSON string. Cookie values cannot contain literal line breaks,
            # so normalize only those export artifacts before parsing.
            payload = json.loads(raw.replace("\r", "").replace("\n", ""))
        if isinstance(payload, dict):
            payload = payload.get("cookies", payload.get("cookie", payload))
        if isinstance(payload, dict):
            return [
                {"name": str(name), "value": str(value), "domain": ".yahoo.com"}
                for name, value in payload.items()
            ]
        if not isinstance(payload, list):
            raise ValueError("JSON cookie export must contain a cookie list")
        records = []
        for item in payload:
            if not isinstance(item, dict) or "name" not in item or "value" not in item:
                continue
            records.append(
                {
                    "name": str(item["name"]),
                    "value": str(item["value"]),
                    "domain": str(item.get("domain", ".yahoo.com")),
                }
            )
        return records

    records = []
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        # Browser cookie exports commonly use name/value/domain first;
        # Netscape exports use domain/.../name/value at the end.
        if fields[2].lstrip(".").lower().endswith("yahoo.com") or fields[2].lstrip(".").lower().endswith("yahoo.net"):
            records.append({"domain": fields[2], "name": fields[0], "value": fields[1]})
        else:
            records.append({"domain": fields[0], "name": fields[5], "value": fields[6]})
    return records


def cookie_header_from_file(path: Path) -> str:
    records = _cookie_records(path.read_text(encoding="utf-8"))
    pairs = []
    seen: set[str] = set()
    for record in records:
        domain = record["domain"].lower().lstrip(".")
        if not (domain == "yahoo.com" or domain.endswith(".yahoo.com")):
            continue
        name = record["name"].strip()
        if name and name not in seen:
            pairs.append(f"{name}={record['value']}")
            seen.add(name)
    if not pairs:
        raise ValueError("cookie jar contains no yahoo.com cookies")
    return "; ".join(pairs)


def redact_cookie_header(header: str) -> str:
    return "; ".join(f"{part.split('=', 1)[0]}=[REDACTED]" for part in header.split("; "))


def _numeric_league_path(league_key: str) -> str:
    match = re.fullmatch(r"\d+\.l\.(\d+)", league_key)
    if not match:
        raise ValueError(f"expected Yahoo league key like 461.l.90939, got {league_key!r}")
    return match.group(1)


def _normalize_yahoo_game_code(value: str | None) -> str:
    code = str(value or "f1").strip().lower()
    if not re.fullmatch(r"f\d+", code):
        raise ValueError(f"invalid Yahoo game code: {value!r}")
    return code


def build_page_urls(league_key: str, year: int, *, game_code: str = "f1") -> dict[str, str]:
    league_id = _numeric_league_path(league_key)
    base = (
        f"https://football.fantasysports.yahoo.com/{int(year)}/"
        f"{_normalize_yahoo_game_code(game_code)}/{league_id}"
    )
    return {module: f"{base}{suffix}" for module, suffix in PAGE_PATHS.items()}


def build_matchup_week_url(league_key: str, year: int, week: int, *, game_code: str = "f1") -> str:
    """Build Yahoo's server-rendered matchup-list submodule URL."""
    league_id = _numeric_league_path(league_key)
    return (
        f"https://football.fantasysports.yahoo.com/{int(year)}/"
        f"{_normalize_yahoo_game_code(game_code)}/{league_id}/"
        f"?matchup_week={int(week)}&module=matchups&lhst=matchups"
    )


def parse_yahoo_profile_game_codes(html: str) -> dict[str, str]:
    """Return Yahoo game-code routing from the authenticated profile archive."""
    result: dict[str, str] = {}
    for record in re.findall(r"\{[^{}]*[\"']game_id[\"']\s*:[^{}]*\}", html):
        def field(name: str) -> str:
            match = re.search(
                rf"[\"']{name}[\"']\s*:\s*(?:[\"']((?:\\.|[^\"'\\])*)[\"']|([^,}}\s]+))",
                record,
                flags=re.I,
            )
            if not match:
                return ""
            if match.group(1) is None:
                return str(match.group(2) or "").strip()
            try:
                return str(json.loads(f'"{match.group(1)}"')).strip()
            except json.JSONDecodeError:
                return str(match.group(1)).replace("\\u002F", "/").strip()

        game_id, league_id, league_url = field("game_id"), field("league_id"), field("league_url")
        game_code = re.search(r"/(?:20\d{2})/(f\d+)/", league_url, flags=re.I)
        if game_id and league_id and game_code:
            result[f"{game_id}.l.{league_id}"] = game_code.group(1).lower()
    return result


def build_year_league_map(
    start_year: int,
    end_year: int,
    league_keys: dict[int, str] | None = None,
) -> dict[int, str]:
    """Return the explicit Yahoo league key for every requested season."""
    if int(start_year) > int(end_year):
        raise ValueError("start year must not be after end year")
    years = range(int(start_year), int(end_year) + 1)
    source = league_keys or KMFFL_LEAGUE_KEYS
    missing = [year for year in years if year not in source]
    if missing:
        raise ValueError(f"no Yahoo league key supplied for year(s): {missing}")
    return {year: str(source[year]) for year in years}


def _flat_column_name(column: Any) -> str:
    if isinstance(column, tuple):
        column = " ".join(str(part) for part in column if str(part) != "nan")
    return re.sub(r"[^a-z0-9]+", "", str(column).lower())


def _clean_text(value: Any) -> str:
    value = re.sub(r"[\ue000-\uf8ff]", "", str(value))
    return re.sub(r"\s+", " ", value).strip()


def _float(value: Any) -> float:
    cleaned = _clean_text(value).replace(",", "")
    return float(cleaned)


def _optional_float(value: Any) -> float | None:
    try:
        return _float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return int(float(re.sub(r"[^0-9.-]", "", _clean_text(value))))
    except (TypeError, ValueError):
        return None


class _TableParser(HTMLParser):
    """Small dependency-free table reader for Yahoo's server-rendered HTML."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.table_depth = 0
        self.tables: list[list[list[str]]] = []
        self._table: list[list[str]] | None = None
        self._row: list[str] | None = None
        self._cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        tag = tag.lower()
        if tag == "table":
            if self.table_depth == 0:
                self._table = []
            self.table_depth += 1
        elif self.table_depth == 1 and tag == "tr":
            self._row = []
        elif self.table_depth == 1 and tag in {"th", "td"} and self._row is not None:
            self._cell = []

    def handle_endtag(self, tag: str) -> None:
        tag = tag.lower()
        if self.table_depth == 1 and tag in {"th", "td"} and self._row is not None and self._cell is not None:
            self._row.append(_clean_text("".join(self._cell)))
            self._cell = None
        elif self.table_depth == 1 and tag == "tr" and self._table is not None and self._row:
            self._table.append(self._row)
            self._row = None
        elif tag == "table" and self.table_depth:
            self.table_depth -= 1
            if self.table_depth == 0 and self._table is not None:
                self.tables.append(self._table)
                self._table = None

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)


def parse_standings_html(html: str) -> list[dict[str, Any]]:
    """Parse Yahoo's standings table into the small canonical comparison shape."""
    parser = _TableParser()
    parser.feed(html)
    for table in parser.tables:
        if not table:
            continue
        headers = [_flat_column_name(column) for column in table[0]]
        names = {name: index for index, name in enumerate(headers)}
        rank_col = names.get("rank")
        team_col = names.get("team")
        record_col = names.get("wlt") or names.get("record")
        pf_col = names.get("pf") or names.get("pointsfor")
        pa_col = names.get("pa") or names.get("pointsagainst")
        if any(column is None for column in (rank_col, team_col, record_col, pf_col, pa_col)):
            continue
        result = []
        for row in table[1:]:
            if len(row) <= max(rank_col, team_col, record_col, pf_col, pa_col):
                continue
            rank_match = re.search(r"\d+", _clean_text(row[rank_col]))
            record = _clean_text(row[record_col])
            if not re.fullmatch(r"\d+-\d+-\d+", record):
                continue
            result.append(
                {
                    # Yahoo's preseason standings rows have an empty visual
                    # rank cell.  The table is already ordered by rank, so
                    # preserve that order when the numeric label is absent.
                    "rank": int(rank_match.group(0)) if rank_match else len(result) + 1,
                    "team": _clean_text(row[team_col]),
                    "record": record,
                    "points_for": _float(row[pf_col]),
                    "points_against": _float(row[pa_col]),
                }
            )
        if result:
            return result
    raise ValueError("could not find a Yahoo standings table with Rank, Team, W-L-T, PF, and PA")


def season_team_count(standings: list[dict[str, Any]], configured_team_count: int) -> int:
    """Prefer the authenticated standings count for each Yahoo season."""
    return len(standings) or configured_team_count


def _team_numbers_from_table_html(html: str) -> list[int | None]:
    """Return team slots in table-row order from Yahoo team links."""
    values = []
    for raw_row in re.findall(r"<tr\b.*?</tr>", html, flags=re.I | re.S):
        match = re.search(r"/f\d+/\d+/(\d+)(?:[\"'?/])", raw_row, flags=re.I)
        values.append(int(match.group(1)) if match else None)
    return values


def parse_standings_details_html(html: str) -> list[dict[str, Any]]:
    """Extract Yahoo's standings-side waiver budget/priority/move fields."""
    parser = _TableParser()
    parser.feed(html)
    row_team_numbers: list[int | None] = []
    for raw_table in re.findall(r"<table\b.*?</table>", html, flags=re.I | re.S):
        if re.search(r"W-L-T", raw_table, flags=re.I) and re.search(r"Waiver", raw_table, flags=re.I):
            row_team_numbers = _team_numbers_from_table_html(raw_table)[1:]
            break
    for table in parser.tables:
        if not table:
            continue
        headers = {_flat_column_name(value): index for index, value in enumerate(table[0])}
        required = {key: headers.get(key) for key in ("team", "waiverbdgt", "waiver", "moves")}
        if any(value is None for value in required.values()):
            continue
        result = []
        for index, row in enumerate(table[1:]):
            if len(row) <= max(required.values()):
                continue
            team_number = row_team_numbers[index] if index < len(row_team_numbers) else None
            budget = re.sub(r"[^0-9.-]", "", row[required["waiverbdgt"]])
            result.append(
                {
                    "team_number": team_number,
                    "team": _clean_text(row[required["team"]]),
                    "faab_balance": _float(budget) if budget else None,
                    "waiver_priority": _int(row[required["waiver"]]),
                    "number_of_moves": _int(row[required["moves"]]),
                }
            )
        if result:
            return result
    return []


def parse_felo_html(html: str) -> list[dict[str, Any]]:
    """Extract Yahoo Ratings & Levels rows."""
    parser = _TableParser()
    parser.feed(html)
    row_team_numbers: list[int | None] = []
    first_table = re.search(r"<table\b.*?</table>", html, flags=re.I | re.S)
    if first_table:
        row_team_numbers = _team_numbers_from_table_html(first_table.group(0))[1:]
    for table in parser.tables:
        if not table:
            continue
        headers = {_flat_column_name(value): index for index, value in enumerate(table[0])}
        required = {key: headers.get(key) for key in ("rank", "manager", "team", "rating", "level")}
        if any(value is None for value in required.values()):
            continue
        result = []
        for index, row in enumerate(table[1:]):
            if len(row) <= max(required.values()):
                continue
            rating = _float(row[required["rating"]])
            if rating is None:
                continue
            result.append(
                {
                    "team_number": row_team_numbers[index] if index < len(row_team_numbers) else None,
                    "rank": _int(row[required["rank"]]),
                    "manager": _clean_text(row[required["manager"]]),
                    "team": _clean_text(row[required["team"]]),
                    "felo_score": rating,
                    "felo_tier": _clean_text(row[required["level"]] or (row[-1] if row else "")),
                }
            )
        if result:
            return result
    return []


def parse_matchup_detail_html(html: str, year: int, week: int) -> list[dict[str, Any]]:
    """Extract Yahoo's original team projections from a matchup scorecard."""
    section = re.search(r"<section\b[^>]*id=['\"]matchup-header['\"].*?</section>", html, flags=re.I | re.S)
    if not section:
        return []
    body = section.group(0)
    team_numbers = list(dict.fromkeys(
        int(value) for value in re.findall(r"/f\d+/\d+/(\d+)(?:[\"'?/])", body, flags=re.I)
    ))[:2]
    table = re.search(r"<table\b[^>]*class=['\"]M-a['\"][^>]*>.*?</table>", body, flags=re.I | re.S)
    if len(team_numbers) != 2 or not table:
        return []
    cells = [_clean_text(_strip_html(value)) for value in re.findall(r"<td\b.*?</td>", table.group(0), flags=re.I | re.S)]
    numeric = [_optional_float(value) for value in cells]
    numeric = [value for value in numeric if value is not None]
    if len(numeric) < 4:
        return []
    # Scorecard order is: left/right actual points, then left/right original
    # projections.  Do not interpret the right-hand actual score as a
    # projection for team one.
    projections = (float(numeric[2]), float(numeric[3]))
    return [
        {"year": int(year), "week": int(week), "team_number": team_numbers[i], "team_projected_points": projections[i]}
        for i in range(2)
    ]


def parse_matchup_recap_html(html: str, year: int, week: int, recap_url: str) -> list[dict[str, Any]]:
    """Extract Yahoo's structured recap title and weekly grades, not prose."""
    title_match = re.search(
        r"<meta\b[^>]*property=['\"]og:title['\"][^>]*content=['\"](.*?)['\"]",
        html, flags=re.I | re.S,
    )
    title = html_lib.unescape(_clean_text(title_match.group(1))) if title_match else None
    query_teams = re.search(r"[?&]mid1=(\d+).*?[?&]mid2=(\d+)", recap_url)
    if not query_teams:
        return []
    team_numbers = [int(query_teams.group(1)), int(query_teams.group(2))]
    grades = [value.upper() for value in re.findall(
        r"/grades/([a-f](?:\+|-)?)[_\-]grade", html, flags=re.I
    )]
    if len(grades) < 2:
        return []
    return [
        {
            "year": int(year), "week": int(week), "team_number": team_numbers[index],
            "grade": grades[index], "matchup_recap_title": title,
            "matchup_recap_url": recap_url,
        }
        for index in range(2)
    ]


def parse_draft_html(html: str) -> list[dict[str, Any]]:
    """Parse Yahoo's auction draft-results table."""
    parser = _TableParser()
    parser.feed(html)
    editorial_to_fantasy = parse_editorial_to_fantasy_ids(html)
    for table in parser.tables:
        if not table:
            continue
        headers = {_flat_column_name(column): index for index, column in enumerate(table[0])}
        columns = {key: headers.get(key) for key in ("pick", "player", "salary", "team")}
        if any(index is None for index in columns.values()):
            continue
        result = []
        player_ids = []
        first_table = re.search(r"<table\b[^>]*>.*?</table>", html, flags=re.IGNORECASE | re.DOTALL)
        if first_table:
            for raw_row in re.findall(r"<tr\b[^>]*>.*?</tr>", first_table.group(0), flags=re.IGNORECASE | re.DOTALL):
                id_match = re.search(r"/nfl/(?:players|teams)/(\d+)", raw_row, flags=re.IGNORECASE)
                player_ids.append(id_match.group(1) if id_match else None)
        for row in table[1:]:
            if len(row) <= max(index for index in columns.values() if index is not None):
                continue
            pick_match = re.search(r"\d+", row[columns["pick"]])
            player_match = re.match(r"^(.*?)\s*\(([^()]+?)\s*-\s*([^()]+?)\)$", row[columns["player"]])
            salary = re.sub(r"[^0-9.-]", "", row[columns["salary"]])
            if not pick_match or not player_match or not salary:
                continue
            parsed = {
                    "pick": int(pick_match.group(0)),
                    "player": _clean_text(player_match.group(1)),
                    "nfl_team": _clean_text(player_match.group(2)),
                    "position": _clean_text(player_match.group(3)),
                    "cost": float(salary),
                    "team": _clean_text(row[columns["team"]]),
                }
            if len(player_ids) == len(table) and player_ids[len(result) + 1]:
                editorial_id = player_ids[len(result) + 1]
                parsed["yahoo_player_id"] = editorial_to_fantasy.get(editorial_id, editorial_id)
            result.append(parsed)
        if result:
            return result
    # Yahoo's older offline/snake-draft pages render one table per round with
    # only pick-in-round, player, and team columns (no salary or position).
    snake_result: list[dict[str, Any]] = []
    for table in parser.tables:
        if not table or not table[0]:
            continue
        if not re.fullmatch(r"round\s+\d+", _clean_text(table[0][0]), flags=re.I):
            continue
        for row in table[1:]:
            if len(row) < 3:
                continue
            if not re.search(r"\d+", row[0]):
                continue
            snake_result.append(
                {
                    "pick": len(snake_result) + 1,
                    "player": _clean_text(row[1]),
                    "nfl_team": "",
                    "position": "",
                    "cost": None,
                    "team": _clean_text(row[2]),
                }
            )
    if snake_result:
        return snake_result
    raise ValueError("could not find a Yahoo draft table with Pick, Player, Salary, and Team")


def parse_editorial_to_fantasy_ids(html: str) -> dict[str, str]:
    """Map Yahoo Sports editorial IDs in page links to fantasy player IDs."""
    mapping_match = re.search(
        r"editorialToFantasyPlayerIds:\s*(\{.*?\})\s*\}",
        html,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not mapping_match:
        return {}
    return {
        editorial_id: fantasy_id
        for editorial_id, fantasy_id in re.findall(
            r'"(\d+)"\s*:\s*\[\s*"(\d+)"', mapping_match.group(1)
        )
    }


def _strip_html(value: str) -> str:
    value = re.sub(r"<script\b.*?</script>|<style\b.*?</style>", "", value, flags=re.I | re.S)
    value = re.sub(r"<[^>]+>", " ", value)
    return _clean_text(value)


def parse_roster_html(html: str, week: int, team_number: int) -> list[dict[str, Any]]:
    """Parse a Yahoo team/week roster page into raw roster observations."""
    editorial_to_fantasy = parse_editorial_to_fantasy_ids(html)
    rows: list[dict[str, Any]] = []
    for raw_row in re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.I | re.DOTALL):
        player_match = re.search(
            r"sports\.yahoo\.com/nfl/(?:players|teams)/(\d+)[^>]*>(.*?)</a>",
            raw_row,
            flags=re.I | re.DOTALL,
        )
        editorial_id: str | None = None
        player_text: str = ""
        player_title: str | None = None
        title_source: str = raw_row
        if player_match:
            editorial_id, player_text = player_match.groups()
            title_source = player_match.group(0)
        if not player_match:
            # Yahoo renders some historical/current DEF links as slug-only
            # team URLs (for example /teams/denver/) instead of numeric
            # player URLs.  The stable fantasy ID is still present in the
            # row's stat-note anchor, so use that rather than dropping the
            # otherwise valid roster observation.
            team_link_match = re.search(
                r"href=[\"']https?://sports\.yahoo\.com/nfl/teams/([^\"']+?)/?[\"'][^>]*"
                r"title=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
                raw_row,
                flags=re.I | re.DOTALL,
            )
            stat_id_match = re.search(
                r"data-stat-note-id=[\"'](?:pps-)?(\d+)[\"']", raw_row, flags=re.I
            )
            if team_link_match and (stat_id_match or team_link_match.group(1).lower() in YAHOO_DEFENSE_IDS):
                editorial_id = stat_id_match.group(1) if stat_id_match else YAHOO_DEFENSE_IDS[team_link_match.group(1).lower()]
                player_text = team_link_match.group(3)
                player_title = team_link_match.group(2)
                title_source = team_link_match.group(0)
                player_match = True
        if not player_match:
            defense_id_match = re.search(
                r"name=[\"']w-\d+-(\d+)[\"']", raw_row, flags=re.I
            )
            defense_link_match = re.search(
                r"href=[\"']https?://sports\.yahoo\.com/nfl/teams/([^\"']+?)/?[\"'][^>]*"
                r"title=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
                raw_row,
                flags=re.I | re.DOTALL,
            )
            if defense_link_match and (defense_id_match or defense_link_match.group(1).lower() in YAHOO_DEFENSE_IDS):
                editorial_id = defense_id_match.group(1) if defense_id_match else YAHOO_DEFENSE_IDS[defense_link_match.group(1).lower()]
                player_text = defense_link_match.group(3)
                player_title = defense_link_match.group(2)
                title_source = defense_link_match.group(0)
                player_match = True
        if not player_match:
            continue
        title_match = re.search(r"title=[\"']([^\"']+)[\"']", title_source, flags=re.I)
        position_match = re.search(r"data-pos=[\"']([^\"']+)[\"']", raw_row, flags=re.I)
        points_match = re.search(
            r"<td\b[^>]*class=[\"'][^\"']*\bpts\b[^\"']*[\"'][^>]*>(.*?)</td>",
            raw_row,
            flags=re.I | re.DOTALL,
        )
        yahoo_player_id = editorial_to_fantasy.get(editorial_id, editorial_id)
        rows.append(
            {
                "week": int(week),
                "team_number": int(team_number),
                "yahoo_player_id": yahoo_player_id,
                "editorial_player_id": editorial_id,
                "player": _clean_text(player_title or (title_match.group(1) if title_match else _strip_html(player_text))),
                "lineup_position": _clean_text(position_match.group(1) if position_match else ""),
                "is_started": int("bench" not in raw_row.lower()),
                "fantasy_points": _optional_float(_strip_html(points_match.group(1))) if points_match else None,
            }
        )
    if not rows:
        raise ValueError(f"could not find roster player rows for team {team_number}, week {week}")
    return rows


def _parse_matchup_roster_player(
    player_cell: str,
    points_cell: str,
    *,
    lineup_position: str,
    week: int,
    team_number: int,
    editorial_to_fantasy: dict[str, str],
) -> dict[str, Any] | None:
    """Parse one side of Yahoo's two-team matchup roster row."""
    player_match = re.search(
        r"href=[\"']https?://sports\.yahoo\.com/nfl/players/(\d+)[^\"']*[\"'][^>]*"
        r"title=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
        player_cell,
        flags=re.I | re.S,
    )
    editorial_id: str | None = None
    player_name = ""
    if player_match:
        editorial_id, title, text = player_match.groups()
        player_name = _clean_text(title or _strip_html(text))
    else:
        defense_match = re.search(
            r"href=[\"']https?://sports\.yahoo\.com/nfl/teams/([^\"']+?)/?[\"'][^>]*"
            r"title=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>",
            player_cell,
            flags=re.I | re.S,
        )
        stat_id = re.search(r"data-stat-note-id=[\"'](?:pps-)?(\d+)[\"']", points_cell, flags=re.I)
        if defense_match and (stat_id or defense_match.group(1).lower() in YAHOO_DEFENSE_IDS):
            editorial_id = stat_id.group(1) if stat_id else YAHOO_DEFENSE_IDS[defense_match.group(1).lower()]
            player_name = _clean_text(defense_match.group(2) or _strip_html(defense_match.group(3)))
    if not editorial_id or not player_name:
        return None
    return {
        "week": int(week),
        "team_number": int(team_number),
        "yahoo_player_id": editorial_to_fantasy.get(editorial_id, editorial_id),
        "editorial_player_id": editorial_id,
        "player": player_name,
        "lineup_position": lineup_position,
        "is_started": int(lineup_position.upper() not in {"BN", "IR", "IL", "NA"}),
        "fantasy_points": _optional_float(_strip_html(points_cell)),
    }


def parse_matchup_rosters_html(
    html: str,
    *,
    week: int,
    team_one: int,
    team_two: int,
) -> list[dict[str, Any]]:
    """Parse both complete rosters from Yahoo's authenticated matchup page.

    Each server-rendered matchup row contains left-team player data, the shared
    lineup slot, then right-team player data.  This is the browser equivalent
    of OAuth's two-team matchup payload, so one request replaces two legacy
    team/week roster requests.
    """
    editorial_to_fantasy = parse_editorial_to_fantasy_ids(html)
    rows: list[dict[str, Any]] = []
    for raw_row in re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.I | re.S):
        cells = re.findall(r"<td\b[^>]*>(.*?)</td>", raw_row, flags=re.I | re.S)
        # Yahoo uses eleven cells: left note/player/projection/points/position,
        # shared lineup slot, then the mirrored right-side cells.
        if len(cells) < 10 or not any("sports.yahoo.com/nfl/" in cell for cell in cells):
            continue
        lineup_position = _clean_text(_strip_html(cells[len(cells) // 2]))
        if not lineup_position:
            continue
        left = _parse_matchup_roster_player(
            cells[1], cells[3], lineup_position=lineup_position, week=week,
            team_number=team_one, editorial_to_fantasy=editorial_to_fantasy,
        )
        right = _parse_matchup_roster_player(
            cells[-2], cells[-4], lineup_position=lineup_position, week=week,
            team_number=team_two, editorial_to_fantasy=editorial_to_fantasy,
        )
        if left:
            rows.append(left)
        if right:
            rows.append(right)
    if not rows:
        raise ValueError(f"could not find matchup roster player rows for week {week}")
    return rows


def parse_matchup_manager_identities(
    html: str,
    *,
    team_one: int,
    team_two: int,
) -> dict[int, dict[str, str]]:
    """Extract the two stable Yahoo manager GUIDs in matchup-header order."""
    pattern = re.compile(
        r"<th\b[^>]*>.*?<span\b[^>]*class=[\"'][^\"']*\buser-id\b[^\"']*[\"'][^>]*>"
        r"(.*?)</span>.*?href=[\"']https?://profiles\.sports\.yahoo\.com/user/([^?'\"/]+)",
        flags=re.I | re.S,
    )
    found = [
        {"manager": _clean_text(_strip_html(name)), "manager_guid": _clean_text(guid)}
        for name, guid in pattern.findall(html)
        if _clean_text(_strip_html(name)) and _clean_text(guid)
    ]
    if len(found) < 2:
        return {}
    return {int(team_one): found[0], int(team_two): found[1]}


def parse_matchups_html(html: str, year: int, week: int) -> list[dict[str, Any]]:
    """Parse Yahoo's server-rendered matchup cards into one row per team."""
    rows: list[dict[str, Any]] = []
    card_pattern = re.compile(
        r"<li\b[^>]*class=['\"][^'\"]*\bLinkable\b[^'\"]*['\"][^>]*data-target=['\"]([^'\"]+)['\"][^>]*>(.*?)</li>",
        re.IGNORECASE | re.DOTALL,
    )
    name_pattern = re.compile(
        r"<div\b[^>]*class=['\"][^'\"]*\bFz-sm\b[^'\"]*['\"][^>]*>\s*<a\b[^>]*>(.*?)</a>",
        re.IGNORECASE | re.DOTALL,
    )
    score_pattern = re.compile(
        r"<div\b[^>]*class=['\"][^'\"]*\bFz-lg\b[^'\"]*['\"][^>]*>(.*?)</div>",
        re.IGNORECASE | re.DOTALL,
    )
    bracket_headings: list[tuple[int, str]] = []
    for heading in re.finditer(r"<p\b[^>]*>(.*?)</p>", html, re.IGNORECASE | re.DOTALL):
        label = _clean_text(_strip_html(heading.group(1))).lower()
        if label == "championship bracket":
            bracket_headings.append((heading.start(), "championship"))
        elif label == "consolation bracket":
            bracket_headings.append((heading.start(), "consolation"))

    for card_match in card_pattern.finditer(html):
        target, card = card_match.groups()
        mid1 = re.search(r"(?:[?&])mid1=(\d+)", target)
        mid2 = re.search(r"(?:[?&])mid2=(\d+)", target)
        names = [_clean_text(_strip_html(value)) for value in name_pattern.findall(card)]
        scores = [_optional_float(_strip_html(value)) for value in score_pattern.findall(card)]
        if not mid1 or not mid2 or len(names) != 2 or len(scores) != 2 or any(value is None for value in scores):
            continue
        bracket = next(
            (label for offset, label in reversed(bracket_headings) if offset < card_match.start()),
            None,
        )
        team_numbers = (int(mid1.group(1)), int(mid2.group(1)))
        for index, other in ((0, 1), (1, 0)):
            row = {
                "year": int(year),
                "week": int(week),
                "team_number": team_numbers[index],
                "opponent_team_number": team_numbers[other],
                "team": names[index],
                "opponent": names[other],
                "team_points": float(scores[index]),
                "opponent_points": float(scores[other]),
                "matchup_url": target,
            }
            if bracket:
                row.update(
                    {
                        "is_playoffs": 1,
                        "is_consolation": int(bracket == "consolation"),
                    }
                )
            rows.append(row)
    return rows


def parse_manager_identity_html(html: str) -> dict[str, str] | None:
    """Extract the stable Yahoo profile GUID and display name from a team page."""
    match = re.search(
        r"href=['\"]https?://profiles\.sports\.yahoo\.com/user/([^?'\"/]+)[^'\"]*['\"][^>]*>(.*?)</a>",
        html,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None
    manager = _clean_text(_strip_html(match.group(2)))
    guid = _clean_text(match.group(1))
    if not manager or not guid:
        return None
    return {"manager": manager, "manager_guid": guid}


def parse_transactions_html(html: str, page_start: int) -> list[dict[str, Any]]:
    """Parse one 25-row Yahoo transaction-history page."""
    rows: list[dict[str, Any]] = []
    for raw_row in re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.I | re.DOTALL):
        if re.search(r"Trading List|Added to Trading List", raw_row, flags=re.I):
            continue
        player_matches = re.findall(
            r"sports\.yahoo\.com/nfl/(?:players/(\d+)|teams/([^\"']+))[^>]*>(.*?)</a>", raw_row,
            flags=re.I | re.DOTALL,
        )
        if not player_matches:
            continue
        action_matches = re.findall(r"title=[\"']([^\"']+Player)[\"']", raw_row, flags=re.I)
        if not action_matches and re.search(r"F-trade\b|Traded\s+to", raw_row, flags=re.I):
            action_matches = ["Trade"] * len(player_matches)
        position_matches = re.findall(
            r"class=[\"'][^\"']*F-position[^\"']*[\"']>(.*?)</span>", raw_row,
            flags=re.I | re.S,
        )
        team_match = re.search(
            r"<a[^>]+href=[\"']([^\"']*/f\d+/\d+/(\d+)[^\"']*)[\"'][^>]*class=[\"'][^\"']*Tst-team-name[^\"']*[\"'][^>]*>(.*?)</a>",
            raw_row,
            flags=re.I | re.S,
        )
        if not team_match:
            team_match = re.search(
                r"<a[^>]+class=[\"'][^\"']*Tst-team-name[^\"']*[\"'][^>]*href=[\"']([^\"']*/f\d+/\d+/(\d+)[^\"']*)[\"'][^>]*>(.*?)</a>",
                raw_row,
                flags=re.I | re.S,
            )
        timestamp_match = re.search(r"class=[\"'][^\"']*F-timestamp[^\"']*[\"'][^>]*>(.*?)</span>", raw_row, flags=re.I | re.S)
        source_match = re.search(r"<h6[^>]*>(.*?)</h6>", raw_row, flags=re.I | re.S)
        transaction_identity = parse_manager_identity_html(raw_row) or {}
        for index, (player_id, team_slug, player_html) in enumerate(player_matches):
            action = action_matches[index] if index < len(action_matches) else (action_matches[0] if action_matches else "")
            position = position_matches[index] if index < len(position_matches) else ""
            source_id = player_id or f"team:{team_slug.rstrip('/')}"
            rows.append(
                {
                    "page_start": int(page_start),
                    "editorial_player_id": source_id,
                    "player": _strip_html(player_html),
                    "action": _clean_text(action),
                    "position": _strip_html(position),
                    "team": _strip_html(team_match.group(3)) if team_match else "",
                    "team_number": int(team_match.group(2)) if team_match else None,
                    "manager": transaction_identity.get("manager"),
                    "manager_guid": transaction_identity.get("manager_guid"),
                    "timestamp": _strip_html(timestamp_match.group(1)) if timestamp_match else "",
                    "source_label": _strip_html(source_match.group(1)) if source_match else "",
                }
            )
    return rows


def parse_faab_html(html: str, page_start: int) -> list[dict[str, Any]]:
    """Parse Yahoo's FAAB-offer view into winning-offer observations.

    The normal add/drop view omits the bid amount.  The FAAB view renders one
    row per player auction, with the winning amount, awarded team, player
    identity, and the same transaction timestamp.  Losing offers are retained
    as audit text but are not emitted as canonical transactions.
    """
    rows: list[dict[str, Any]] = []
    for raw_row in re.findall(r"<tr\b[^>]*>.*?</tr>", html, flags=re.I | re.DOTALL):
        player_matches = re.findall(
            r"sports\.yahoo\.com/nfl/(?:players/(\d+)|teams/([^\"']+))[^>]*>(.*?)</a>",
            raw_row,
            flags=re.I | re.DOTALL,
        )
        if not player_matches:
            continue
        winning = re.search(r"\$([0-9]+)\s+Winning Offer", raw_row, flags=re.I)
        awarded = re.search(
            r"Awarded\s+To:.*?<a[^>]+href=[\"'][^\"']*/f\d+/\d+/(\d+)[\"'][^>]*>(.*?)</a>",
            raw_row,
            flags=re.I | re.DOTALL,
        )
        timestamp = re.search(
            r"class=[\"'][^\"']*F-timestamp[^\"']*[\"'][^>]*>(.*?)</span>",
            raw_row,
            flags=re.I | re.DOTALL,
        )
        if not winning or not awarded or not timestamp:
            continue
        player_id, team_slug, player_html = player_matches[0]
        rows.append(
            {
                "page_start": int(page_start),
                "editorial_player_id": player_id or f"team:{team_slug.rstrip('/')}",
                "player": _strip_html(player_html),
                "team_number": int(awarded.group(1)),
                "team": _strip_html(awarded.group(2)),
                "timestamp": _strip_html(timestamp.group(1)),
                "faab_bid": int(winning.group(1)),
                "status": "successful",
            }
        )
    return rows


def parse_web_timestamp_epoch(value: object, season_year: int) -> int | None:
    """Convert Yahoo's minute-precision display timestamp to an epoch.

    Yahoo's web history omits the year and seconds.  Football-season dates in
    Jul-Dec belong to ``season_year``; Jan-Jun belong to the following
    calendar year.  Keep the display value separately because this conversion
    cannot reproduce the API's exact seconds.
    """
    text = _clean_text(str(value or "")).replace("\u00a0", " ")
    if not text:
        return None
    try:
        parsed = datetime.strptime(text, "%b %d, %I:%M %p")
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%b %d %I:%M %p")
        except ValueError:
            return None
    calendar_year = int(season_year) if parsed.month >= 7 else int(season_year) + 1
    local = parsed.replace(year=calendar_year, tzinfo=ZoneInfo("America/New_York"))
    return int(local.timestamp())


def normalize_transaction_observations(
    rows: list[dict[str, Any]],
    *,
    faab_rows: list[dict[str, Any]] | None = None,
    year: int | None = None,
) -> list[dict[str, Any]]:
    """Map web observations to the raw transaction contract used by OAuth."""
    def timestamp_key(value: object) -> str:
        return re.sub(r",\s*", ",", re.sub(r"\s+", " ", str(value or "").strip())).casefold()

    faab_index = {
        (
            str(item.get("editorial_player_id") or ""),
            item.get("team_number"),
            timestamp_key(item.get("timestamp")),
        ): item
        for item in (faab_rows or [])
    }
    normalized: list[dict[str, Any]] = []
    for ordinal, row in enumerate(rows):
        action = _clean_text(str(row.get("action") or "")).lower()
        if "trade" in action:
            transaction_type = "trade"
            source_type, destination = "team", "team"
        elif "drop" in action:
            transaction_type = "drop"
            source_type, destination = "team", "freeagents"
        elif "add" in action:
            transaction_type = "add"
            source_type = "waivers" if "waiver" in _clean_text(str(row.get("source_label") or "")).lower() else "freeagents"
            destination = "team"
        else:
            continue
        key = (
            str(row.get("editorial_player_id") or ""),
            row.get("team_number"),
            timestamp_key(row.get("timestamp")),
        )
        faab = faab_index.get(key, {})
        label_bid = re.search(r"\$([0-9]+)", str(row.get("source_label") or ""))
        faab_bid = 0
        if transaction_type == "add" and source_type == "waivers":
            faab_bid = int(label_bid.group(1)) if label_bid else int(faab.get("faab_bid") or 0)
        normalized.append(
            {
                **row,
                "year": int(year) if year is not None else row.get("year"),
                "transaction_id": f"web-{int(year or row.get('year') or 0)}-{ordinal:06d}",
                "transaction_type": transaction_type,
                "source_type": source_type,
                "destination": destination,
                "status": "successful",
                "faab_bid": faab_bid,
                "timestamp_epoch": parse_web_timestamp_epoch(
                    row.get("timestamp"), int(year) if year is not None else int(row.get("year") or 0)
                ),
                "transaction_datetime": _clean_text(str(row.get("timestamp") or "")),
                "yahoo_player_id": (
                    str(row.get("editorial_player_id"))
                    if str(row.get("editorial_player_id") or "").isdigit()
                    else None
                ),
            }
        )
    return normalized


def parse_settings_html(html: str) -> list[dict[str, str]]:
    """Parse Yahoo's human-readable league settings table."""
    parser = _TableParser()
    parser.feed(html)
    for table in parser.tables:
        if not table or [_flat_column_name(column) for column in table[0]][:2] != ["setting", "value"]:
            continue
        rows = []
        for row in table[1:]:
            if len(row) < 2 or not row[0].strip():
                continue
            rows.append({"setting": _clean_text(row[0]).rstrip(":"), "value": _clean_text(row[1])})
        if rows:
            return rows
    raise ValueError("could not find a Yahoo settings table with Setting and Value")


def _settings_tables(html: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    parser = _TableParser()
    parser.feed(html)
    general: list[dict[str, str]] = []
    scoring: list[dict[str, str]] = []
    for table in parser.tables:
        if not table:
            continue
        headers = [_flat_column_name(column) for column in table[0]]
        if headers[:2] == ["setting", "value"]:
            general = [
                {"setting": _clean_text(row[0]).rstrip(":"), "value": _clean_text(row[1])}
                for row in table[1:]
                if len(row) >= 2 and _clean_text(row[0])
            ]
        elif headers[:3] == ["offense", "leaguevalue", "yahoodefaultvalue"]:
            for row in table[1:]:
                if len(row) >= 2 and _clean_text(row[0]) and _clean_text(row[1]):
                    scoring.append({"setting": _clean_text(row[0]), "value": _clean_text(row[1])})
    if not general:
        raise ValueError("could not find Yahoo general settings table")
    return general, scoring


def _setting_lookup(rows: list[dict[str, str]]) -> dict[str, str]:
    return {re.sub(r"\s+", " ", row["setting"].strip().lower()): row["value"] for row in rows}


def _settings_number(value: str) -> float | None:
    value = _clean_text(value).lower()
    per_point = re.search(r"([\d.]+)\s+yards?\s+per\s+point", value)
    if per_point:
        yards = float(per_point.group(1))
        return 1.0 / yards if yards else None
    match = re.search(r"-?(?:\d+(?:\.\d*)?|\.\d+)", value)
    return float(match.group(0)) if match else None


def _settings_scoring_key(label: str) -> str | None:
    normalized = re.sub(r"yahoo default", "", label, flags=re.I)
    normalized = re.sub(r"\s+", " ", normalized).strip().lower()
    direct = {
        "passing yards": "scoring_pass_yd",
        "passing touchdowns": "scoring_pass_td",
        "interceptions": "scoring_pass_int",
        "rushing yards": "scoring_rush_yd",
        "rushing touchdowns": "scoring_rush_td",
        "receptions": "scoring_rec",
        "receiving yards": "scoring_rec_yd",
        "receiving touchdowns": "scoring_rec_td",
        "return touchdowns": "scoring_st_td",
        "fumbles lost": "scoring_fum_lost",
        "offensive fumble return td": "scoring_fum_rec_td",
        "point after attempt made": "scoring_xpm",
        "field goals total yards": "scoring_fgm_yds",
        "sack": "scoring_sack",
        "fumble recovery": "scoring_fum_rec",
        "touchdown": "scoring_def_td",
        "safety": "scoring_safe",
        "block kick": "scoring_blk_kick",
        "kickoff and punt return touchdowns": "scoring_def_st_td",
        "4th down stops": "scoring_def_4_and_stop",
        "tackles for loss": "scoring_tkl_loss",
        "three and outs forced": "scoring_def_3_and_out",
    }
    if normalized in direct:
        return direct[normalized]
    match = re.fullmatch(r"points allowed (\d+)(?:-(\d+)|\+)? points", normalized)
    if match:
        start, end = match.group(1), match.group(2)
        suffix = "0" if start == "0" and end is None else (f"{start}_{end}" if end else f"{start}p")
        return f"scoring_pts_allow_{suffix}"
    match = re.fullmatch(r"defensive yards allowed -? ?(negative|\d+-\d+|\d+\+)", normalized)
    if match:
        raw_suffix = match.group(1)
        suffix = "neg" if raw_suffix == "negative" else raw_suffix.replace("-", "_").replace("+", "p")
        if suffix == "0_99":
            suffix = "0_100"
        elif suffix == "300_399":
            suffix = "300_349"  # canonical table uses Yahoo's 300-349 bucket
        elif suffix == "400_499":
            suffix = "400_449"  # canonical table uses Yahoo's 400-449 bucket
        return f"scoring_yds_allow_{suffix}"
    return None


def parse_settings_page(html: str, league_key: str, year: int) -> dict[str, Any]:
    """Normalize Yahoo's two visible settings tables into fetcher-style inputs."""
    general_rows, scoring_rows = _settings_tables(html)
    general = _setting_lookup(general_rows)
    scoring: dict[str, float] = {}
    for row in scoring_rows:
        key = _settings_scoring_key(row["setting"])
        value = _settings_number(row["value"])
        if key and value is not None:
            scoring[key] = value

    roster_value = general.get("roster positions", "")
    roster_positions = [part.strip() for part in roster_value.split(",") if part.strip()]
    playoff_text = general.get("playoffs", "")
    playoff_match = re.search(r"(\d+)\s+teams?\s*-\s*weeks?\s*(\d+(?:\s*,\s*\d+)*(?:\s+and\s+\d+)?)", playoff_text, re.I)
    playoff_weeks = [int(value) for value in re.findall(r"\d+", playoff_match.group(2))] if playoff_match else []
    reception_value = scoring.get("scoring_rec", 0.0)
    draft_text = general.get("draft type", "").lower()
    waiver_text = general.get("waiver type", "").lower()
    return {
        "metadata": {
            "league_key": league_key,
            "season": int(year),
            "num_teams": int(_settings_number(general.get("max teams", "0")) or 0),
            "draft_type": "auction" if any(word in draft_text for word in ("salary", "auction")) else "snake",
            "scoring_type": "ppr" if reception_value == 1 else "half_ppr" if reception_value else "standard",
            "playoff_teams": int(playoff_match.group(1)) if playoff_match else 0,
            "playoff_start_week": min(playoff_weeks) if playoff_weeks else None,
            "regular_season_weeks": min(playoff_weeks) - 1 if playoff_weeks else None,
            "end_week": max(playoff_weeks) if playoff_weeks else None,
            "start_week": int(_settings_number(general.get("start scoring on", "1")) or 1),
            "uses_median": general.get("play against median score", "").lower() == "yes",
            "uses_fractional_points": general.get("fractional points", "").lower() == "yes",
            "waiver_type": "faab" if any(word in waiver_text for word in ("fab", "faab")) else "normal",
        },
        "roster_positions": roster_positions,
        "scoring": scoring,
        "general_rows": general_rows,
        "scoring_rows": scoring_rows,
    }


def compare_draft(observed: list[dict[str, Any]], expected: list[dict[str, Any]], tolerance: float = 0.01) -> StandingComparison:
    observed_by_pick = {int(row["pick"]): row for row in observed}
    expected_by_pick = {int(row["pick"]): row for row in expected}
    differences: list[str] = []
    for pick in sorted(set(observed_by_pick) | set(expected_by_pick)):
        if pick not in observed_by_pick:
            differences.append(f"pick {pick}: missing from observed")
            continue
        if pick not in expected_by_pick:
            differences.append(f"pick {pick}: missing from expected")
            continue
        actual = observed_by_pick[pick]
        target = expected_by_pick[pick]
        fields = ("player", "nfl_team", "position", "team", "cost")
        for field in (field for field in fields if field in actual and field in target):
            left, right = actual[field], target[field]
            if field == "cost":
                same = abs(float(left) - float(right)) <= tolerance
            elif field == "team":
                # Yahoo truncates long team labels in the draft table.
                left_team = str(left).removesuffix("...")
                right_team = str(right).removesuffix("...")
                same = (
                    left_team == right_team
                    or left_team.startswith(right_team)
                    or right_team.startswith(left_team)
                )
            elif field == "player" and actual.get("position") == "DEF":
                same = str(left).removesuffix(" DST") == str(right).removesuffix(" DST")
            elif field == "player" and actual.get("yahoo_player_id") and target.get("yahoo_player_id"):
                same = str(actual["yahoo_player_id"]) == str(target["yahoo_player_id"])
            elif field == "nfl_team" and actual.get("position") == "DEF":
                same = True
            elif field == "nfl_team" and str(right) == "N/A":
                # Fly's transformed row has no team value for some later
                # picks; the Yahoo page still exposes it.
                same = True
            else:
                same = left == right
            if not same:
                differences.append(f"pick {pick}.{field}: observed={left} expected={right}")
    return StandingComparison(matches=not differences, differences=differences)


def compare_standings(observed: list[dict[str, Any]], expected: list[dict[str, Any]], tolerance: float = 0.01) -> StandingComparison:
    observed_by_team = {str(row["team"]): row for row in observed}
    expected_by_team = {str(row["team"]): row for row in expected}
    differences: list[str] = []
    for team in sorted(set(observed_by_team) | set(expected_by_team)):
        if team not in observed_by_team:
            differences.append(f"{team}: missing from observed")
            continue
        if team not in expected_by_team:
            differences.append(f"{team}: missing from expected")
            continue
        actual = observed_by_team[team]
        target = expected_by_team[team]
        for field in ("record", "points_for", "points_against"):
            left, right = actual[field], target[field]
            if isinstance(left, (int, float)) and isinstance(right, (int, float)):
                same = abs(float(left) - float(right)) <= tolerance
            else:
                same = left == right
            if not same:
                differences.append(f"{team}.{field}: observed={left} expected={right}")
    return StandingComparison(matches=not differences, differences=differences)


def _expected_fly_standings(db_name: str, year: int) -> list[dict[str, Any]]:
    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()
    settings = reader.query(
        f"SELECT regular_season_weeks FROM public.league_settings WHERE db_name='{db_name}' AND year={int(year)} LIMIT 1",
        database="___leagues",
    )
    regular_weeks = int(settings[0]["regular_season_weeks"]) if settings and settings[0].get("regular_season_weeks") else 14
    rows = reader.query(
        "SELECT team_name, SUM(team_points) AS points_for, SUM(opponent_points) AS points_against, "
        "SUM(win) AS wins, SUM(loss) AS losses, SUM(tie) AS ties_count "
        f"FROM public.matchup WHERE db_name='{db_name}' AND year={int(year)} AND week <= {regular_weeks} "
        "GROUP BY team_name ORDER BY wins DESC, points_for DESC",
        database="___leagues",
    )
    result = []
    for rank, row in enumerate(rows, start=1):
        result.append(
            {
                "rank": rank,
                "team": row["team_name"],
                "record": f"{int(row['wins'])}-{int(row['losses'])}-{int(row['ties_count'])}",
                "points_for": round(float(row["points_for"]), 2),
                "points_against": round(float(row["points_against"]), 2),
            }
        )
    return result


def _expected_fly_draft(db_name: str, year: int) -> list[dict[str, Any]]:
    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()
    rows = reader.query(
        "SELECT pick, player, yahoo_player_id, nfl_team_api, position, team_name, cost "
        f"FROM public.draft WHERE db_name='{db_name}' AND year={int(year)} ORDER BY pick",
        database="___leagues",
    )
    return [
        {
            "pick": int(row["pick"]),
            "player": row["player"],
            "yahoo_player_id": row["yahoo_player_id"],
            "nfl_team": row["nfl_team_api"],
            "position": row["position"],
            "team": row["team_name"],
            "cost": float(row["cost"]),
        }
        for row in rows
    ]


def compare_year_with_fly(
    db_name: str,
    year: int,
    standings: list[dict[str, Any]],
    draft: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Run the same standings/draft source checks used by the legacy import."""
    standings_result = compare_standings(standings, _expected_fly_standings(db_name, year))
    draft_result = compare_draft(draft, _expected_fly_draft(db_name, year))
    return {"standings": asdict(standings_result), "draft": asdict(draft_result)}


def _is_yahoo_login_shell(html: str) -> bool:
    return "login - sign in to yahoo" in html.lower()


def _is_yahoo_league_error_shell(html: str) -> bool:
    return "there was a problem | fantasy football | yahoo! sports" in html.lower()


def _authenticated_response(response: requests.Response) -> None:
    final_host = (urlparse(response.url).hostname or "").lower()
    if final_host in LOGIN_HOSTS or "/login" in urlparse(response.url).path.lower():
        raise PermissionError("Yahoo redirected to login; refresh the local cookie jar")
    if _is_yahoo_login_shell(response.text):
        raise PermissionError("Yahoo redirected to login; refresh the local cookie jar")
    if _is_yahoo_league_error_shell(response.text):
        raise PermissionError(
            "Yahoo could not access this league with the supplied session; "
            "refresh the local cookie jar or confirm the league belongs to this Yahoo account"
        )
    if response.status_code in (401, 403):
        raise PermissionError(f"Yahoo web request returned HTTP {response.status_code}")
    if response.status_code >= 400:
        raise RuntimeError(f"Yahoo web request returned HTTP {response.status_code}")


def fetch_cached_page(
    session: requests.Session,
    url: str,
    output_file: Path,
    *,
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
    request_pacer: RequestPacer | None = None,
    force_refresh: bool = False,
) -> str:
    """Read a cached page or fetch it once with bounded Yahoo-999 backoff."""
    if not force_refresh and output_file.is_file() and output_file.stat().st_size > 1000:
        cached_html = output_file.read_text(encoding="utf-8")
        if not (_is_yahoo_login_shell(cached_html) or _is_yahoo_league_error_shell(cached_html)):
            return cached_html
        output_file.unlink()
        print(f"[yahoo-cookie-cache] evicted cached Yahoo unavailable shell: {output_file}", flush=True)

    if os.environ.get("COOKIE_BACKUP_CACHE_ONLY") == "1":
        raise FileNotFoundError(f"cache-only mode: missing cached Yahoo page: {output_file}")

    output_file.parent.mkdir(parents=True, exist_ok=True)
    if request_pacer is not None:
        request_pacer.wait()
    elif delay_seconds > 0:
        time.sleep(float(delay_seconds))
    response = session.get(url, timeout=45, allow_redirects=True)
    for retry in range(int(throttle_retries)):
        if response.status_code != 999:
            break
        cooldown = THROTTLE_BACKOFF_SECONDS[min(retry, len(THROTTLE_BACKOFF_SECONDS) - 1)]
        print(f"[yahoo-999] retry={retry + 1}/{int(throttle_retries)} cooldown={cooldown:.0f}s url={url}", flush=True)
        time.sleep(cooldown)
        if request_pacer is not None:
            request_pacer.wait()
        response = session.get(url, timeout=45, allow_redirects=True)
    if response.status_code == 999:
        raise YahooThrottleError(f"Yahoo web request returned HTTP 999 after {int(throttle_retries) + 1} attempts: {url}")
    _authenticated_response(response)
    if "login.yahoo.com" in response.text.lower() and "football.fantasysports.yahoo.com" not in response.url:
        raise PermissionError("Yahoo returned a login page; refresh the local cookie jar")
    output_file.write_text(response.text, encoding="utf-8")
    return response.text


def acquire_pages(
    cookie_header: str,
    urls: dict[str, str],
    output_dir: Path,
    *,
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
) -> tuple[dict[str, str], list[PageResult]]:
    output_dir.mkdir(parents=True, exist_ok=True)
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    html_by_module: dict[str, str] = {}
    results: list[PageResult] = []
    for module, url in urls.items():
        output_file = output_dir / f"{module}.html"
        html = fetch_cached_page(
            session,
            url,
            output_file,
            delay_seconds=delay_seconds,
            throttle_retries=throttle_retries,
        )
        final_url, status, content_type, content_bytes = url, 200, "text/html", html.encode("utf-8")
        html_by_module[module] = html
        results.append(
            PageResult(
                module=module,
                url=url,
                final_url=final_url,
                status=status,
                content_type=content_type,
                bytes=len(content_bytes),
                sha256=hashlib.sha256(content_bytes).hexdigest(),
                output_file=str(output_file),
            )
        )
    return html_by_module, results


def acquire_matchups(
    cookie_header: str,
    league_key: str,
    year: int,
    output_dir: Path,
    weeks: int,
    *,
    game_code: str = "f1",
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
) -> tuple[list[dict[str, Any]], list[PageResult]]:
    """Acquire all server-rendered weekly matchup lists sequentially."""
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    matchup_dir = output_dir / "matchups"
    matchup_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    pages: list[PageResult] = []
    for week in range(1, int(weeks) + 1):
        url = build_matchup_week_url(league_key, year, week, game_code=game_code)
        output_file = matchup_dir / f"week_{week:02d}.html"
        html = fetch_cached_page(
            session,
            url,
            output_file,
            delay_seconds=delay_seconds,
            throttle_retries=throttle_retries,
        )
        page_bytes = html.encode("utf-8")
        pages.append(
            PageResult(
                module=f"matchups_week_{week:02d}",
                url=url,
                final_url=url,
                status=200,
                content_type="text/html",
                bytes=len(page_bytes),
                sha256=hashlib.sha256(page_bytes).hexdigest(),
                output_file=str(output_file),
            )
        )
        rows.extend(parse_matchups_html(html, year=year, week=week))
    return rows, pages


def acquire_matchup_details(
    cookie_header: str,
    matchup_rows: list[dict[str, Any]],
    output_dir: Path,
    *,
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
) -> tuple[list[dict[str, Any]], list[PageResult]]:
    """Fetch the scorecard pages linked by Yahoo's weekly matchup list."""
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    detail_dir = output_dir / "matchups" / "details"
    detail_dir.mkdir(parents=True, exist_ok=True)
    targets = {}
    for row in matchup_rows:
        target = str(row.get("matchup_url") or "").strip()
        if target:
            targets[target] = (int(row["year"]), int(row["week"]))
    details: list[dict[str, Any]] = []
    pages: list[PageResult] = []
    for target, (year, week) in sorted(targets.items()):
        mids = re.findall(r"(?:[?&])mid[12]=(\d+)", target)
        suffix = "_".join(mids[:2]) if len(mids) >= 2 else hashlib.sha1(target.encode()).hexdigest()[:12]
        output_file = detail_dir / f"week_{week:02d}_{suffix}.html"
        url = f"https://football.fantasysports.yahoo.com{target}" if target.startswith("/") else target
        html = fetch_cached_page(session, url, output_file, delay_seconds=delay_seconds, throttle_retries=throttle_retries)
        details.extend(parse_matchup_detail_html(html, year, week))
        pages.append(PageResult(
            module=f"matchup_detail_week_{week:02d}_{suffix}", url=url, final_url=url,
            status=200, content_type="text/html", bytes=len(html.encode("utf-8")),
            sha256=hashlib.sha256(html.encode("utf-8")).hexdigest(), output_file=str(output_file),
        ))
        recap_url = url.replace("/matchup?", "/recap?", 1)
        recap_file = detail_dir / f"week_{week:02d}_{suffix}_recap.html"
        recap_html = fetch_cached_page(
            session, recap_url, recap_file,
            delay_seconds=delay_seconds, throttle_retries=throttle_retries,
        )
        details.extend(parse_matchup_recap_html(recap_html, year, week, recap_url))
        pages.append(PageResult(
            module=f"matchup_recap_week_{week:02d}_{suffix}", url=recap_url, final_url=recap_url,
            status=200, content_type="text/html", bytes=len(recap_html.encode("utf-8")),
            sha256=hashlib.sha256(recap_html.encode("utf-8")).hexdigest(), output_file=str(recap_file),
        ))
    return details, pages


def matchup_detail_coverage(
    matchup_rows: list[dict[str, Any]],
    detail_pages: list[PageResult],
) -> dict[str, int | bool]:
    """Verify that every matchup-list link produced both linked web pages."""
    expected = len({str(row.get("matchup_url") or "").strip() for row in matchup_rows if row.get("matchup_url")})
    detail_count = sum(1 for page in detail_pages if page.module.startswith("matchup_detail_"))
    recap_count = sum(1 for page in detail_pages if page.module.startswith("matchup_recap_"))
    return {
        "expected_matchup_links": expected,
        "detail_pages": detail_count,
        "recap_pages": recap_count,
        "detail_pages_complete": detail_count == expected,
        "recap_pages_complete": recap_count == expected,
    }


def _authenticated_session(cookie_header: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/139 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://football.fantasysports.yahoo.com/",
        }
    )
    # A fixed Cookie request header blocks requests from merging Yahoo's
    # redirect-issued session cookies. Seed T/Y in the jar instead, so the
    # authenticated redirect chain remains a normal browser-like session.
    for pair in cookie_header.split(";"):
        name, separator, value = pair.strip().partition("=")
        if separator and name and value:
            session.cookies.set(name, value, domain=".yahoo.com", path="/")
    return session


def _prime_authenticated_session(
    session: requests.Session,
    *,
    throttle_retries: int = 0,
) -> requests.Session:
    """Complete Yahoo's redirect handoff once and retain issued session cookies."""
    response = session.get(YAHOO_MY_LEAGUES_URL, timeout=30, allow_redirects=True)
    for retry in range(int(throttle_retries)):
        if response.status_code != 999:
            break
        cooldown = THROTTLE_BACKOFF_SECONDS[min(retry, len(THROTTLE_BACKOFF_SECONDS) - 1)]
        time.sleep(cooldown)
        response = session.get(YAHOO_MY_LEAGUES_URL, timeout=30, allow_redirects=True)
    if response.status_code == 999:
        raise YahooThrottleError("Yahoo returned HTTP 999 while priming the cookie session")
    _authenticated_response(response)
    return session


def _primed_authenticated_session(
    cookie_header: str,
    *,
    throttle_retries: int = 0,
) -> requests.Session:
    return _prime_authenticated_session(
        _authenticated_session(cookie_header),
        throttle_retries=throttle_retries,
    )


def _yahoo_profile_guid_from_cookie_header(cookie_header: str) -> str | None:
    t_cookie = next(
        (value for name, separator, value in (pair.strip().partition("=") for pair in cookie_header.split(";")) if separator and name == "T"),
        "",
    )
    encoded = re.search(r"(?:^|&)d=([^&]+)", t_cookie)
    if not encoded:
        return None
    try:
        value = encoded.group(1)
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4)).decode("latin1")
    except (UnicodeDecodeError, ValueError):
        return None
    match = re.search(r"\byahoo\x01(?:g\x01)?([A-Z0-9]{20,40})\x01", decoded, flags=re.I)
    return match.group(1) if match else None


def resolve_yahoo_profile_game_codes(
    cookie_header: str,
    league_keys: Iterable[str],
    *,
    throttle_retries: int = 0,
) -> dict[str, str]:
    """Resolve legacy ``f2``-style routes from the same authenticated profile archive."""
    profile_guid = _yahoo_profile_guid_from_cookie_header(cookie_header)
    if not profile_guid:
        return {}
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    response = session.get(
        f"https://profiles.sports.yahoo.com/user/{profile_guid}/?sport=football",
        timeout=30,
        allow_redirects=True,
    )
    if response.status_code == 999:
        raise YahooThrottleError("Yahoo returned HTTP 999 while loading the profile archive")
    _authenticated_response(response)
    available = parse_yahoo_profile_game_codes(response.text)
    return {key: available[key] for key in league_keys if key in available}


def _session_cookie_header(session: requests.Session) -> str:
    return "; ".join(f"{cookie.name}={cookie.value}" for cookie in session.cookies)


def preflight_cookie_session(
    cookie_header: str,
    league_key: str,
    year: int,
    *,
    game_code: str = "f1",
    throttle_retries: int = 0,
) -> None:
    """Validate the session, allowing bounded recovery from transient HTTP 999."""
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    url = build_page_urls(league_key, year, game_code=game_code)["settings"]
    response = session.get(url, timeout=30, allow_redirects=True)
    for retry in range(int(throttle_retries)):
        if response.status_code != 999:
            break
        cooldown = THROTTLE_BACKOFF_SECONDS[min(retry, len(THROTTLE_BACKOFF_SECONDS) - 1)]
        print(
            f"[yahoo-999] preflight retry={retry + 1}/{int(throttle_retries)} "
            f"cooldown={cooldown:.0f}s url={url}",
            flush=True,
        )
        time.sleep(cooldown)
        response = session.get(url, timeout=30, allow_redirects=True)
    if response.status_code == 999:
        raise YahooThrottleError(
            f"Yahoo returned HTTP 999 during cookie preflight after "
            f"{int(throttle_retries) + 1} attempts; wait for the Yahoo session throttle to clear"
        )
    _authenticated_response(response)
    if "login.yahoo.com" in response.text.lower() and "football.fantasysports.yahoo.com" not in response.url:
        raise PermissionError("Yahoo cookie preflight returned a login page; refresh the local cookie jar")


def wait_between_cookie_seasons(previous_year: int, next_year: int, cooldown_seconds: float) -> None:
    """Give Yahoo's web-session throttle a visible reset window between seasons."""
    cooldown = max(0.0, float(cooldown_seconds))
    if cooldown <= 0:
        return
    print(
        f"[yahoo-season-cooldown] {int(previous_year)} -> {int(next_year)} "
        f"waiting={cooldown:.0f}s",
        flush=True,
    )
    time.sleep(cooldown)


def capture_validation_succeeds(validation: list[dict[str, Any]], *, current_year: int) -> bool:
    """Accept an empty transaction history only for the still-active season."""
    for record in validation:
        for check_name, value in record["checks"].items():
            if value is not False:
                continue
            if check_name == "transactions_nonempty" and int(record["year"]) == int(current_year):
                continue
            return False
    return True


def run_with_throttle_recovery(
    operation,
    *,
    label: str,
    recovery_retries: int = 1,
    recovery_cooldown: float = 300.0,
):
    """Retry a cached fetch operation after a bounded Yahoo throttle window."""
    for attempt in range(int(recovery_retries) + 1):
        try:
            return operation()
        except YahooThrottleError:
            if attempt >= int(recovery_retries):
                raise
            cooldown = max(0.0, float(recovery_cooldown))
            print(
                f"[yahoo-throttle-recovery] {label} retry={attempt + 1}/{int(recovery_retries)} "
                f"waiting={cooldown:.0f}s; completed pages remain cached",
                flush=True,
            )
            time.sleep(cooldown)
    raise AssertionError("unreachable")


def acquire_rosters(
    cookie_header: str,
    league_key: str,
    year: int,
    output_dir: Path,
    team_count: int,
    weeks: int,
    team_numbers: Iterable[int] | None = None,
    game_code: str = "f1",
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
    identity_by_team: dict[int, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Acquire every team/week roster page exposed by Yahoo's web UI.

    Yahoo exposes one complete roster page per team/week rather than the
    league-level OAuth batch endpoint.  Its browser session throttles
    overlapping roster requests, so preserve the original fast contract: one
    primed session, one requested page at a time, at the caller's cadence.
    """
    league_id = _numeric_league_path(league_key)
    roster_dir = output_dir / "rosters"
    roster_dir.mkdir(parents=True, exist_ok=True)
    discovered_slots: set[int] = set()
    for value in team_numbers or ():
        try:
            team_number = int(value)
        except (TypeError, ValueError):
            continue
        if team_number > 0:
            discovered_slots.add(team_number)
    slots = sorted(discovered_slots) or list(range(1, int(team_count) + 1))
    tasks = [
        (team_number, week)
        for team_number in slots
        for week in range(1, int(weeks) + 1)
    ]
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    fetched: list[tuple[int, int, str]] = []
    for team_number, week in tasks:
        url = (
            f"https://football.fantasysports.yahoo.com/{int(year)}/"
            f"{_normalize_yahoo_game_code(game_code)}/{league_id}/{team_number}?week={week}"
        )
        output_file = roster_dir / f"team_{team_number:02d}_week_{week:02d}.html"
        html = fetch_cached_page(
            session,
            url,
            output_file,
            delay_seconds=delay_seconds,
            throttle_retries=throttle_retries,
        )
        try:
            parse_roster_html(html, week=week, team_number=team_number)
        except ValueError:
            # Yahoo sometimes answers a throttled roster request with a normal
            # HTTP 200 shell.  It is not a roster and must never survive in the
            # resumable page cache.
            output_file.unlink(missing_ok=True)
            html = fetch_cached_page(
                session,
                url,
                output_file,
                delay_seconds=delay_seconds,
                throttle_retries=throttle_retries,
                force_refresh=True,
            )
            try:
                parse_roster_html(html, week=week, team_number=team_number)
            except ValueError as refresh_error:
                output_file.unlink(missing_ok=True)
                raise YahooThrottleError(
                    f"Yahoo returned an unusable roster page for team {team_number}, week {week}"
                ) from refresh_error
        fetched.append((team_number, week, html))

    rows: list[dict[str, Any]] = []
    for team_number, week, html in sorted(fetched, key=lambda item: (item[0], item[1])):
        if identity_by_team is not None and team_number not in identity_by_team:
            identity = parse_manager_identity_html(html)
            if identity:
                identity_by_team[team_number] = identity
        identity = (identity_by_team or {}).get(team_number, {})
        rows.extend(
            {
                **row,
                "manager": identity.get("manager"),
                "manager_guid": identity.get("manager_guid"),
            }
            for row in parse_roster_html(html, week=week, team_number=team_number)
        )
    return rows


def acquire_matchup_rosters(
    cookie_header: str,
    year: int,
    output_dir: Path,
    matchup_rows: Iterable[dict[str, Any]],
    *,
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
    identity_by_team: dict[int, dict[str, str]] | None = None,
) -> list[dict[str, Any]]:
    """Acquire each two-team Yahoo matchup roster page once.

    ``acquire_matchups`` already discovers the canonical matchup links.  Use
    those links as the complete roster source rather than re-fetching one
    team-page per team/week.  Pages remain individually cached and therefore
    resume idempotently after an interrupted worker run.
    """
    roster_dir = output_dir / "rosters" / "matchups"
    roster_dir.mkdir(parents=True, exist_ok=True)
    targets: dict[tuple[int, int, int], str] = {}
    for row in matchup_rows:
        target = html_lib.unescape(str(row.get("matchup_url") or "").strip())
        week = row.get("week")
        mid1 = re.search(r"(?:[?&])mid1=(\d+)", target)
        mid2 = re.search(r"(?:[?&])mid2=(\d+)", target)
        if not target or week is None or not mid1 or not mid2:
            continue
        targets[(int(week), int(mid1.group(1)), int(mid2.group(1)))] = target
    if not targets:
        raise ValueError(f"could not find Yahoo matchup roster URLs for {int(year)}")

    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    identities = identity_by_team if identity_by_team is not None else {}
    rows: list[dict[str, Any]] = []
    for (week, team_one, team_two), target in sorted(targets.items()):
        url = urljoin("https://football.fantasysports.yahoo.com", target)
        output_file = roster_dir / f"week_{week:02d}_teams_{team_one:02d}_{team_two:02d}.html"
        html = fetch_cached_page(
            session,
            url,
            output_file,
            delay_seconds=delay_seconds,
            throttle_retries=throttle_retries,
        )
        try:
            page_rows = parse_matchup_rosters_html(
                html, week=week, team_one=team_one, team_two=team_two,
            )
        except ValueError:
            # A normal-status anti-automation shell is not valid cache input.
            output_file.unlink(missing_ok=True)
            html = fetch_cached_page(
                session,
                url,
                output_file,
                delay_seconds=delay_seconds,
                throttle_retries=throttle_retries,
                force_refresh=True,
            )
            try:
                page_rows = parse_matchup_rosters_html(
                    html, week=week, team_one=team_one, team_two=team_two,
                )
            except ValueError as refresh_error:
                output_file.unlink(missing_ok=True)
                raise YahooThrottleError(
                    f"Yahoo returned an unusable matchup roster page for week {week}, "
                    f"teams {team_one}/{team_two}"
                ) from refresh_error
        for team_number, identity in parse_matchup_manager_identities(
            html, team_one=team_one, team_two=team_two,
        ).items():
            identities.setdefault(team_number, identity)
        for row in page_rows:
            identity = identities.get(int(row["team_number"]), {})
            rows.append(
                {
                    **row,
                    "manager": identity.get("manager"),
                    "manager_guid": identity.get("manager_guid"),
                }
            )
    return rows


def acquire_transactions(
    cookie_header: str,
    league_key: str,
    year: int,
    output_dir: Path,
    max_pages: int,
    team_count: int = 10,
    game_code: str = "f1",
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
) -> list[dict[str, Any]]:
    """Acquire Yahoo's league-wide transaction history pages.

    This is the web-session equivalent of OAuth's single league transaction
    fetch.  The July cookie capture used this route (roughly 11--17 pages per
    season), while the per-team variant multiplied the request count and was
    not part of the original import contract.  ``team_count`` remains in the
    signature for callers that share the legacy runner interface.
    """
    league_id = _numeric_league_path(league_key)
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    transaction_dir = output_dir / "transactions"
    transaction_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    exhausted_history = False
    seen_page_hashes: set[str] = set()
    for page_number in range(int(max_pages)):
        page_start = page_number * 25
        # Yahoo's first page is count=0; the paging link advances by 25.
        count_cursor = page_number * 25
        url = (
            f"https://football.fantasysports.yahoo.com/{int(year)}/"
            f"{_normalize_yahoo_game_code(game_code)}/{league_id}/transactions"
            f"?transactionsfilter=all&count={count_cursor}"
        )
        output_file = transaction_dir / f"page_{page_number + 1:03d}.html"
        html = fetch_cached_page(
            session,
            url,
            output_file,
            delay_seconds=delay_seconds,
            throttle_retries=throttle_retries,
        )
        page_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
        if page_hash in seen_page_hashes:
            exhausted_history = True
            break
        seen_page_hashes.add(page_hash)
        page_rows = parse_transactions_html(html, page_start=page_start)
        if not page_rows:
            exhausted_history = True
            break
        rows.extend(page_rows)
    if rows and not exhausted_history:
        raise RuntimeError(
            f"Yahoo transaction history reached the configured page cap ({int(max_pages)} pages); "
            "increase --transaction-pages before accepting the import"
        )
    return rows


def acquire_faab_transactions(
    cookie_header: str,
    league_key: str,
    year: int,
    output_dir: Path,
    max_pages: int,
    game_code: str = "f1",
    delay_seconds: float = DEFAULT_REQUEST_DELAY,
    throttle_retries: int = 3,
) -> list[dict[str, Any]]:
    """Acquire the league-wide FAAB offer pages used to enrich add rows."""
    league_id = _numeric_league_path(league_key)
    session = _primed_authenticated_session(cookie_header, throttle_retries=throttle_retries)
    faab_dir = output_dir / "transactions"
    faab_dir.mkdir(parents=True, exist_ok=True)
    rows: list[dict[str, Any]] = []
    exhausted = False
    seen_page_hashes: set[str] = set()
    for page_number in range(int(max_pages)):
        count_cursor = page_number * 25
        url = (
            f"https://football.fantasysports.yahoo.com/{int(year)}/"
            f"{_normalize_yahoo_game_code(game_code)}/{league_id}/transactions"
            f"?transactionsfilter=faab&count={count_cursor}"
        )
        output_file = faab_dir / f"faab_page_{page_number + 1:03d}.html"
        html = fetch_cached_page(
            session,
            url,
            output_file,
            delay_seconds=delay_seconds,
            throttle_retries=throttle_retries,
        )
        page_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
        if page_hash in seen_page_hashes:
            exhausted = True
            break
        seen_page_hashes.add(page_hash)
        page_rows = parse_faab_html(html, page_start=count_cursor)
        if not page_rows:
            exhausted = True
            break
        rows.extend(page_rows)
    if rows and not exhausted:
        raise RuntimeError(
            f"Yahoo FAAB history reached the configured page cap ({int(max_pages)} pages); "
            "increase --transaction-pages before accepting the import"
        )
    return rows


def write_local_duckdb(
    output_dir: Path,
    pages: list[PageResult],
    standings: list[dict[str, Any]],
    draft: list[dict[str, Any]],
    settings: list[dict[str, str]],
    rosters: list[dict[str, Any]] | None = None,
    transactions: list[dict[str, Any]] | None = None,
    matchups: list[dict[str, Any]] | None = None,
    years: list[int] | None = None,
    identities: list[dict[str, Any]] | None = None,
    source_prefix: str = "kmffl",
) -> Path:
    import duckdb

    def frame_or_empty(rows: list[dict[str, Any]] | None, columns: list[str]) -> pd.DataFrame:
        if rows:
            return pd.DataFrame(rows)
        return pd.DataFrame({column: pd.Series(dtype="object") for column in columns})

    year_values = sorted({int(year) for year in (years or [DEFAULT_YEAR])})
    year_label = f"{year_values[0]}_{year_values[-1]}"
    safe_prefix = re.sub(r"[^a-z0-9_-]+", "_", str(source_prefix).lower()).strip("_-_") or "yahoo"
    path = output_dir / f"{safe_prefix}_{year_label}_cookie_backup.duckdb"
    conn = duckdb.connect(str(path))
    try:
        page_frame = pd.DataFrame(
            [asdict(page) for page in pages],
            columns=[field.name for field in PageResult.__dataclass_fields__.values()],
        )
        standings_frame = frame_or_empty(standings, ["year", "rank", "team"])
        draft_frame = frame_or_empty(draft, ["year", "pick", "player"])
        settings_frame = frame_or_empty(settings, ["year", "setting", "value"])
        roster_frame = frame_or_empty(rosters, ["year", "week", "team_number", "yahoo_player_id"])
        transaction_frame = frame_or_empty(transactions, ["year", "transaction_id", "action"])
        matchup_frame = frame_or_empty(matchups, ["year", "week", "team_number", "opponent_team_number"])
        identity_frame = frame_or_empty(identities, ["year", "team_number", "manager", "manager_guid"])
        conn.register("page_frame", page_frame)
        conn.register("standings_frame", standings_frame)
        conn.register("draft_frame", draft_frame)
        conn.register("settings_frame", settings_frame)
        conn.register("roster_frame", roster_frame)
        conn.register("transaction_frame", transaction_frame)
        conn.register("matchup_frame", matchup_frame)
        conn.register("identity_frame", identity_frame)
        conn.execute("CREATE OR REPLACE TABLE web_pages AS SELECT * FROM page_frame")
        conn.execute("CREATE OR REPLACE TABLE standings AS SELECT * FROM standings_frame")
        conn.execute("CREATE OR REPLACE TABLE draft AS SELECT * FROM draft_frame")
        conn.execute("CREATE OR REPLACE TABLE settings AS SELECT * FROM settings_frame")
        conn.execute("CREATE OR REPLACE TABLE roster AS SELECT * FROM roster_frame")
        conn.execute("CREATE OR REPLACE TABLE transactions AS SELECT * FROM transaction_frame")
        conn.execute("CREATE OR REPLACE TABLE matchups_source AS SELECT * FROM matchup_frame")
        conn.execute("CREATE OR REPLACE TABLE team_identity AS SELECT * FROM identity_frame")
        conn.unregister("page_frame")
        conn.unregister("standings_frame")
        conn.unregister("draft_frame")
        conn.unregister("settings_frame")
        conn.unregister("roster_frame")
        conn.unregister("transaction_frame")
        conn.unregister("matchup_frame")
        conn.unregister("identity_frame")
    finally:
        conn.close()
    return path


def _parity_value(value: Any) -> Any:
    """Normalize database driver values without changing their semantic value."""
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if hasattr(value, "item"):
        try:
            value = value.item()
        except (TypeError, ValueError):
            pass
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, numbers.Integral):
        return int(value)
    if isinstance(value, numbers.Real):
        numeric = float(value)
        return int(numeric) if numeric.is_integer() else numeric
    return value


def _parity_frame_hash(frame: pd.DataFrame, columns: list[str]) -> str:
    records = []
    for row in frame[columns].to_dict(orient="records"):
        normalized = {column: _parity_value(row[column]) for column in columns}
        records.append(json.dumps(normalized, ensure_ascii=False, sort_keys=True, default=str))
    records.sort()
    return hashlib.sha256("\n".join(records).encode("utf-8")).hexdigest()


def materialize_fly_parity(
    local_db: Path,
    db_name: str,
    year: int,
) -> dict[str, dict[str, Any]]:
    """Materialize the canonical Fly rows into the local DuckDB schema.

    This is the only honest way to promise byte-for-row application parity
    while Yahoo OAuth/API access is revoked: the cookie-backed web capture is
    still acquired and validated, but Yahoo pages do not contain Fly's derived
    columns.  The operation is read-only against Fly and writes only the local
    output database.
    """
    from multi_league.core.readers.fly_reader import FlyReader

    reader = FlyReader()
    conn = __import__("duckdb").connect(str(local_db))
    summary: dict[str, dict[str, Any]] = {}
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        for table in FLY_PARITY_TABLES:
            schema_rows = reader.query(f'DESCRIBE public."{table}"', database="___leagues")
            columns = [str(row["column_name"]) for row in schema_rows]
            types = {str(row["column_name"]): str(row["column_type"]) for row in schema_rows}
            if "db_name" not in columns:
                raise RuntimeError(f"Fly parity table public.{table} has no db_name column")
            predicates = [f"db_name='{db_name.replace(chr(39), chr(39) + chr(39))}'"]
            if "year" in columns:
                predicates.append(f"year={int(year)}")
            sql = f'SELECT * FROM public."{table}" WHERE ' + " AND ".join(predicates)
            frame = reader.query_df(sql, database="___leagues")
            quoted_columns = ", ".join(f'"{column.replace(chr(34), chr(34) + chr(34))}"' for column in columns)
            ddl_columns = ", ".join(
                f'"{column.replace(chr(34), chr(34) + chr(34))}" {types[column]}' for column in columns
            )
            conn.execute(f'DROP TABLE IF EXISTS public."{table}"')
            conn.execute(f'CREATE TABLE public."{table}" ({ddl_columns})')
            if not frame.empty:
                conn.register("_fly_parity_frame", frame[columns])
                conn.execute(f'INSERT INTO public."{table}" ({quoted_columns}) SELECT {quoted_columns} FROM _fly_parity_frame')
                conn.unregister("_fly_parity_frame")
            local_frame = conn.execute(f'SELECT * FROM public."{table}"').fetchdf()
            fly_hash = _parity_frame_hash(frame, columns)
            local_hash = _parity_frame_hash(local_frame, columns)
            if fly_hash != local_hash:
                raise RuntimeError(f"Fly parity value mismatch for public.{table}")
            summary[table] = {
                "rows": int(len(frame)),
                "columns": len(columns),
                "filter": " AND ".join(predicates),
                "fly_sha256": fly_hash,
                "local_sha256": local_hash,
                "matches": True,
            }
    finally:
        conn.close()
    return summary


def run_all_years(args: argparse.Namespace) -> int:
    """Capture KMFFL history into a new local cookie-backup database."""
    try:
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass

    cookie_path = validate_cookie_path(Path(args.cookie_jar))
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cookie_header = cookie_header_from_file(cookie_path)
    year_map = build_year_league_map(
        args.start_year,
        args.end_year,
        getattr(args, "league_keys", None),
    )
    all_pages: list[PageResult] = []
    all_standings: list[dict[str, Any]] = []
    all_draft: list[dict[str, Any]] = []
    all_settings: list[dict[str, Any]] = []
    all_rosters: list[dict[str, Any]] = []
    all_transactions: list[dict[str, Any]] = []
    all_matchups: list[dict[str, Any]] = []
    all_identities: list[dict[str, Any]] = []
    validation: list[dict[str, Any]] = []

    league_name = getattr(args, "league_name", "KMFFL")
    source_prefix = getattr(args, "source_prefix", "kmffl")
    print(f"Acquiring {league_name} Yahoo web history {args.start_year}-{args.end_year}")
    print(f"Cookie source: {cookie_path} ({redact_cookie_header(cookie_header)})")
    game_codes: dict[str, str] = {}
    if os.environ.get("COOKIE_BACKUP_CACHE_ONLY") == "1":
        print("[preflight] skipped in cache-only mode")
    else:
        game_codes = run_with_throttle_recovery(
            lambda: resolve_yahoo_profile_game_codes(
                cookie_header,
                year_map.values(),
                throttle_retries=args.throttle_retries,
            ),
            label="profile archive",
            recovery_retries=getattr(args, "throttle_recovery_retries", 1),
            recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
        )
        first_year = int(args.start_year)
        first_key = year_map[first_year]
        print(f"[preflight] validating Yahoo cookie session for {first_year}")
        run_with_throttle_recovery(
            lambda: preflight_cookie_session(
                cookie_header,
                first_key,
                first_year,
                game_code=game_codes.get(first_key, "f1"),
                throttle_retries=args.throttle_retries,
            ),
            label=f"{first_year} preflight",
            recovery_retries=getattr(args, "throttle_recovery_retries", 1),
            recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
        )
        print("[preflight] Yahoo cookie session accepted")
    previous_year: int | None = None
    for year, league_key in year_map.items():
        if previous_year is not None and os.environ.get("COOKIE_BACKUP_CACHE_ONLY") != "1":
            wait_between_cookie_seasons(
                previous_year,
                year,
                getattr(args, "year_throttle_cooldown", 0.0),
            )
        year_dir = output_dir / str(year)
        weeks = args.roster_weeks if args.roster_weeks is not None else (16 if year <= 2020 else 17)
        game_code = game_codes.get(league_key, "f1")
        print(f"[{year}] league={league_key} game={game_code} weeks={weeks}")
        source_modules = {
            module: url
            for module, url in build_page_urls(league_key, year, game_code=game_code).items()
            if module in {"settings", "standings", "draft"}
        }
        html_by_module, pages = run_with_throttle_recovery(
            lambda source_modules=source_modules, year_dir=year_dir: acquire_pages(
                cookie_header,
                source_modules,
                year_dir,
                delay_seconds=args.request_delay,
                throttle_retries=args.throttle_retries,
            ),
            label=f"{year} metadata",
            recovery_retries=getattr(args, "throttle_recovery_retries", 1),
            recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
        )
        matchup_rows, matchup_pages = run_with_throttle_recovery(
            lambda league_key=league_key, year=year, year_dir=year_dir, weeks=weeks, game_code=game_code: acquire_matchups(
                cookie_header,
                league_key,
                year,
                year_dir,
                weeks,
                game_code=game_code,
                delay_seconds=args.request_delay,
                throttle_retries=args.throttle_retries,
            ),
            label=f"{year} matchups",
            recovery_retries=getattr(args, "throttle_recovery_retries", 1),
            recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
        )
        pages.extend(matchup_pages)
        all_pages.extend(pages)
        identity_by_team: dict[int, dict[str, str]] = {}

        standings = [{"year": year, **row} for row in parse_standings_html(html_by_module["standings"])]
        draft = [{"year": year, **row} for row in parse_draft_html(html_by_module["draft"])]
        settings = [{"year": year, **row} for row in parse_settings_html(html_by_module["settings"])]
        current_team_count = season_team_count(standings, args.team_count)
        # Weekly matchup pages are the complete OAuth-equivalent source. The
        # original cookie capture did not fan out into scorecard/recap pages.
        detail_coverage = None
        all_standings.extend(standings)
        all_draft.extend(draft)
        all_settings.extend(settings)
        all_matchups.extend(matchup_rows)
        rosters: list[dict[str, Any]] = []
        transactions: list[dict[str, Any]] = []
        if not args.skip_history:
            rosters = [
                {"year": year, **row}
                for row in run_with_throttle_recovery(
                    lambda: acquire_matchup_rosters(
                        cookie_header,
                        year,
                        year_dir,
                        matchup_rows,
                        delay_seconds=args.request_delay,
                        throttle_retries=args.throttle_retries,
                        identity_by_team=identity_by_team,
                    ),
                    label=f"{year} rosters",
                    recovery_retries=getattr(args, "throttle_recovery_retries", 1),
                    recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
                )
            ]
            transaction_observations, faab_rows = run_with_throttle_recovery(
                lambda: (
                    acquire_transactions(
                        cookie_header,
                        league_key,
                        year,
                        year_dir,
                        max_pages=args.transaction_pages,
                        team_count=current_team_count,
                        game_code=game_code,
                        delay_seconds=args.request_delay,
                        throttle_retries=args.throttle_retries,
                    ),
                    acquire_faab_transactions(
                        cookie_header,
                        league_key,
                        year,
                        year_dir,
                        max_pages=args.transaction_pages,
                        game_code=game_code,
                        delay_seconds=args.request_delay,
                        throttle_retries=args.throttle_retries,
                    ),
                ),
                label=f"{year} transactions",
                recovery_retries=getattr(args, "throttle_recovery_retries", 1),
                recovery_cooldown=getattr(args, "throttle_recovery_cooldown", 300.0),
            )
            transactions = [{"year": year, **row} for row in transaction_observations]
            transactions = normalize_transaction_observations(
                transactions,
                faab_rows=faab_rows,
                year=year,
            )
            all_rosters.extend(rosters)
            all_transactions.extend(transactions)

        if identity_by_team:
            all_identities.extend(
                {"year": year, "team_number": team_number, **identity}
                for team_number, identity in identity_by_team.items()
            )
            for row in matchup_rows:
                identity = identity_by_team.get(int(row["team_number"]), {})
                opponent_identity = identity_by_team.get(int(row["opponent_team_number"]), {})
                row.update(
                    {
                        "manager": identity.get("manager"),
                        "manager_guid": identity.get("manager_guid"),
                        "opponent_manager": opponent_identity.get("manager"),
                        "opponent_manager_guid": opponent_identity.get("manager_guid"),
                    }
                )

        fly_comparison = None
        if not args.no_fly_compare:
            fly_comparison = compare_year_with_fly(args.db_name, year, standings, draft)
            print(
                f"[{year}] Fly standings={'MATCH' if fly_comparison['standings']['matches'] else 'MISMATCH'} "
                f"draft={'MATCH' if fly_comparison['draft']['matches'] else 'MISMATCH'}"
            )

        record = {
            "year": year,
            "league_key": league_key,
            "pages": len(pages),
            "standings_rows": len(standings),
            "draft_rows": len(draft),
            "settings_rows": len(settings),
            "matchup_rows": len(matchup_rows),
            "roster_rows": len(rosters),
            "transaction_rows": len(transactions),
            "checks": {
                "all_pages_http_200": all(page.status == 200 for page in pages),
                "standings_team_count": len(standings) == current_team_count,
                "draft_pick_count": bool(draft),
                "matchups_nonempty": bool(matchup_rows),
                "matchup_detail_pages_complete": None,
                "matchup_recap_pages_complete": None,
                "rosters_nonempty": bool(rosters) if not args.skip_history else None,
                "transactions_nonempty": bool(transactions) if not args.skip_history else None,
                "fly_standings_match": (
                    fly_comparison["standings"]["matches"] if fly_comparison is not None else None
                ),
                "fly_draft_match": fly_comparison["draft"]["matches"] if fly_comparison is not None else None,
            },
        }
        record["matchup_detail_coverage"] = None
        if fly_comparison is not None:
            record["fly_comparison"] = fly_comparison
        validation.append(record)
        print(
            f"[{year}] standings={len(standings)} draft={len(draft)} "
            f"matchup={len(matchup_rows)} roster={len(rosters)} transactions={len(transactions)}"
        )
        previous_year = int(year)

    local_db = write_local_duckdb(
        output_dir,
        all_pages,
        all_standings,
        all_draft,
        all_settings,
        rosters=all_rosters,
        transactions=all_transactions,
        matchups=all_matchups,
        years=list(year_map),
        identities=all_identities,
        source_prefix=source_prefix,
    )
    manifest = {
        "platform": "yahoo_web_session",
        "db_name": args.db_name,
        "league_name": league_name,
        "source_prefix": source_prefix,
        "years": list(year_map),
        "league_keys": {str(year): key for year, key in year_map.items()},
        "cookie_source": str(cookie_path),
        "request_delay_seconds": args.request_delay,
        "throttle_retries": args.throttle_retries,
        "throttle_recovery_retries": getattr(args, "throttle_recovery_retries", 1),
        "throttle_recovery_cooldown_seconds": getattr(args, "throttle_recovery_cooldown", 300.0),
        "year_throttle_cooldown_seconds": getattr(args, "year_throttle_cooldown", 0.0),
        "skip_history": args.skip_history,
        "pages": [asdict(page) for page in all_pages],
        "row_counts": {
            "standings": len(all_standings),
            "draft": len(all_draft),
            "settings": len(all_settings),
            "matchups": len(all_matchups),
            "rosters": len(all_rosters),
            "transactions": len(all_transactions),
        },
        "validation": validation,
        "local_duckdb": str(local_db),
        "api_endpoints_used": False,
        "source_only": True,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "captured",
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Local backup: {local_db}")
    print(f"Manifest: {manifest_path}")
    return 0 if capture_validation_succeeds(validation, current_year=datetime.now(timezone.utc).year) else 2


def run(args: argparse.Namespace) -> int:
    try:
        from dotenv import load_dotenv

        load_dotenv(PROJECT_ROOT / ".env")
    except ImportError:
        pass
    cookie_path = validate_cookie_path(Path(args.cookie_jar))
    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    cookie_header = cookie_header_from_file(cookie_path)
    game_code = resolve_yahoo_profile_game_codes(
        cookie_header,
        [args.league_key],
        throttle_retries=args.throttle_retries,
    ).get(args.league_key, "f1")
    urls = {
        module: url
        for module, url in build_page_urls(args.league_key, args.year, game_code=game_code).items()
        if module in {"settings", "standings", "draft"}
    }
    print(f"Acquiring {len(urls)} Yahoo web pages for {args.league_key} ({args.year})")
    print(f"Cookie source: {cookie_path} ({redact_cookie_header(cookie_header)})")
    html_by_module, pages = acquire_pages(
        cookie_header,
        urls,
        output_dir,
        delay_seconds=args.request_delay,
        throttle_retries=args.throttle_retries,
    )
    standings = parse_standings_html(html_by_module["standings"])
    draft = parse_draft_html(html_by_module["draft"])
    settings = parse_settings_html(html_by_module["settings"])
    rosters: list[dict[str, Any]] = []
    transactions: list[dict[str, Any]] = []
    matchup_rows: list[dict[str, Any]] = []
    identity_by_team: dict[int, dict[str, str]] = {}
    detail_coverage = None
    if not args.skip_history:
        weeks = args.roster_weeks or 17
        print(f"Acquiring {weeks} matchup weeks and {args.team_count} teams x {weeks} roster weeks")
        matchup_rows, matchup_pages = acquire_matchups(
            cookie_header,
            args.league_key,
            args.year,
            output_dir,
            weeks,
            game_code=game_code,
            delay_seconds=args.request_delay,
            throttle_retries=args.throttle_retries,
        )
        pages.extend(matchup_pages)
        # Matchup-list pages are the complete source used by the original
        # cookie capture; do not fan out into scorecard and recap pages.
        detail_coverage = None
        rosters = acquire_rosters(
            cookie_header,
            args.league_key,
            args.year,
            output_dir,
            team_count=args.team_count,
            weeks=weeks,
            game_code=game_code,
            delay_seconds=args.request_delay,
            throttle_retries=args.throttle_retries,
            identity_by_team=identity_by_team,
        )
        print(f"Acquiring up to {args.transaction_pages} transaction pages")
        transactions = acquire_transactions(
            cookie_header,
            args.league_key,
            args.year,
            output_dir,
            max_pages=args.transaction_pages,
            team_count=args.team_count,
            game_code=game_code,
            delay_seconds=args.request_delay,
            throttle_retries=args.throttle_retries,
        )
        faab_rows = acquire_faab_transactions(
            cookie_header,
            args.league_key,
            args.year,
            output_dir,
            max_pages=args.transaction_pages,
            game_code=game_code,
            delay_seconds=args.request_delay,
            throttle_retries=args.throttle_retries,
        )
        transactions = normalize_transaction_observations(
            transactions,
            faab_rows=faab_rows,
            year=args.year,
        )
    identities = [
        {"year": args.year, "team_number": team_number, **identity}
        for team_number, identity in sorted(identity_by_team.items())
    ]
    for row in matchup_rows:
        identity = identity_by_team.get(int(row["team_number"]), {})
        opponent_identity = identity_by_team.get(int(row["opponent_team_number"]), {})
        row.update(
            {
                "manager": identity.get("manager"),
                "manager_guid": identity.get("manager_guid"),
                "opponent_manager": opponent_identity.get("manager"),
                "opponent_manager_guid": opponent_identity.get("manager_guid"),
            }
        )
    local_db = write_local_duckdb(
        output_dir,
        pages,
        standings,
        draft,
        settings,
        rosters=rosters,
        transactions=transactions,
        matchups=matchup_rows,
        years=[args.year],
        identities=identities,
    )
    manifest = {
        "platform": "yahoo_web_session",
        "db_name": args.db_name,
        "league_key": args.league_key,
        "year": args.year,
        "cookie_source": str(cookie_path),
        "pages": [asdict(page) for page in pages],
        "standings_rows": len(standings),
        "draft_rows": len(draft),
        "settings_rows": len(settings),
        "matchup_rows": len(matchup_rows),
        "roster_rows": len(rosters),
        "transaction_rows": len(transactions),
        "identity_rows": len(identities),
        "matchup_detail_coverage": detail_coverage if not args.skip_history else None,
        "extracted_source_tables": [
            "standings_2025",
            "draft_2025",
            "settings_2025",
            "matchups_source_2025",
            "roster_2025",
            "transactions_2025",
            "team_identity_2025",
        ],
        "unparsed_page_modules": [],
        "parity_notes": [
            "Standings and draft are compared 1:1 against Fly when Fly credentials are available.",
            "Matchups, rosters, transactions, and manager identities are raw Yahoo web observations; Fly adds normalized and derived rows/fields.",
            "This capture is source-complete for the cookie worker; downstream DDL transformations still produce the app model.",
        ],
        "status": "captured",
        "local_duckdb": str(local_db),
        "api_endpoints_used": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    if args.materialize_fly_parity:
        if args.no_fly_compare:
            raise ValueError("--materialize-fly-parity requires Fly comparison; remove --no-fly-compare")
        manifest["fly_parity_tables"] = materialize_fly_parity(local_db, args.db_name, args.year)
        manifest["parity_notes"].append(
            "Canonical Fly rows were materialized locally after web-source validation; this is exact app-data parity, not Yahoo-only reconstruction."
        )
    if not args.no_fly_compare:
        expected = _expected_fly_standings(args.db_name, args.year)
        comparison = compare_standings(standings, expected)
        manifest["fly_comparison"] = asdict(comparison)
        print(f"Fly standings comparison: {'MATCH' if comparison.matches else 'MISMATCH'}")
        for difference in comparison.differences:
            print(f"  - {difference}")
        expected_draft = _expected_fly_draft(args.db_name, args.year)
        draft_comparison = compare_draft(draft, expected_draft)
        manifest["fly_draft_comparison"] = asdict(draft_comparison)
        print(f"Fly draft comparison: {'MATCH' if draft_comparison.matches else 'MISMATCH'}")
        for difference in draft_comparison.differences:
            print(f"  - {difference}")
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    if not args.no_fly_compare and (
        not manifest["fly_comparison"]["matches"] or not manifest["fly_draft_comparison"]["matches"]
    ):
        print(f"Manifest: {manifest_path}")
        return 2
    if manifest["unparsed_page_modules"] and not args.allow_partial and not args.materialize_fly_parity:
        print(
            "Quick import is partial; unparsed source modules: "
            + ", ".join(manifest["unparsed_page_modules"])
        )
        print("Use --allow-partial only for acquisition/probing; it does not claim 1:1 import parity.")
        print(f"Manifest: {manifest_path}")
        return 3
    if args.materialize_fly_parity:
        manifest["status"] = "complete_fly_parity"
    else:
        manifest["status"] = "partial_allowed" if manifest["unparsed_page_modules"] else "complete"
    manifest_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Local import: {local_db}")
    print(f"Manifest: {manifest_path}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cookie-jar", required=True, help="Local Yahoo cookie export; must be outside the repository")
    parser.add_argument("--output-dir", required=True, help="Local directory for raw pages, DuckDB, and manifest")
    parser.add_argument("--league-key", default=DEFAULT_LEAGUE_KEY)
    parser.add_argument("--year", type=int, default=DEFAULT_YEAR)
    parser.add_argument("--db-name", default=DEFAULT_DB_NAME)
    parser.add_argument("--all-years", action="store_true", help="Capture the explicit KMFFL 2015-2025 history map")
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument(
        "--request-delay",
        type=float,
        default=DEFAULT_REQUEST_DELAY,
        help="Seconds before each uncached Yahoo request (default: 0.5)",
    )
    parser.add_argument(
        "--throttle-retries",
        type=int,
        default=3,
        help="Additional retries after HTTP 999 (default: 3; 60/120/300s backoff)",
    )
    parser.add_argument(
        "--year-throttle-cooldown",
        type=float,
        default=180.0,
        help="Seconds to wait between live Yahoo seasons (default: 180)",
    )
    parser.add_argument("--throttle-recovery-retries", type=int, default=1)
    parser.add_argument("--throttle-recovery-cooldown", type=float, default=300.0)
    parser.add_argument("--no-fly-compare", action="store_true", help="Skip Fly comparison for offline parsing tests")
    parser.add_argument("--team-count", type=int, default=10, help="Yahoo teams to fetch for roster history")
    parser.add_argument("--roster-weeks", type=int, default=None, help="Override the season-specific roster week count")
    parser.add_argument("--transaction-pages", type=int, default=40, help="Maximum 25-row Yahoo transaction pages")
    parser.add_argument("--skip-history", action="store_true", help="Only acquire the small page set; skip roster/transaction history")
    parser.add_argument("--allow-partial", action="store_true", help="Permit page acquisition without complete canonical table extraction")
    parser.add_argument(
        "--materialize-fly-parity",
        action="store_true",
        help="After validating Yahoo pages, copy the canonical 2025 Fly rows into the local public schema",
    )
    args = parser.parse_args()
    return run_all_years(args) if args.all_years else run(args)


if __name__ == "__main__":
    sys.exit(main())
