"""
Discovery: find candidate newspaper pages for each game.

Strategies (tried in order per tier's SEARCH_LEVELS):
  pfa_html        — Pro Football Archives HTML box score URL
  pfr_boxscore    — PFR game page lookup
  loc_home_away   — LOC Chronicling America: home city + away city papers
  loc_national    — LOC: national/AP-wire papers
  loc_score_sweep — LOC: exhaustive date+score full-text sweep
  local_pbp_replay— Check local stathead PBP parquets for tackling data
  flag_manual     — Add to manual review queue, close current level

Each strategy adds rows to source_ledger.  Callers then pass those
rows to fetch.py and assess.py before deciding whether to advance level.
"""

from __future__ import annotations
import hashlib
import json
import socket
import time
import urllib.parse
import urllib.request
from datetime import date, timedelta

import duckdb

# Windows urllib.request timeouts are unreliable for hung sockets.
# Global socket timeout ensures we never wait more than this many seconds.
socket.setdefaulttimeout(15)

from .config import (
    LOC_SEARCH, LOC_FULLTEXT_FMT, LOC_MAX_YEAR,
    LOC_MIN_INTERVAL_S, LOC_TIMEOUT_S, LOC_MAX_RETRIES,
    LOC_SEARCH_PAGE_SIZE,
    CITY_PAPERS, FRANCHISE_CITY,
    get_search_terms, pfa_url_prefix,
    SEARCH_LEVELS, game_tier, source_budget,
)
from .schema import open_db, set_game_state

_last_request_time: float = 0.0


def _loc_get(url: str) -> dict | None:
    """
    Rate-limited GET against LOC API.
    Uses browser User-Agent — required since www.loc.gov otherwise returns 403.
    tile.loc.gov (OCR/tile endpoint) does NOT require this.
    Returns parsed JSON or None on error.
    """
    global _last_request_time
    elapsed = time.time() - _last_request_time
    if elapsed < LOC_MIN_INTERVAL_S:
        time.sleep(LOC_MIN_INTERVAL_S - elapsed)
    for attempt in range(LOC_MAX_RETRIES):
        try:
            req = urllib.request.Request(
                url,
                headers={
                    "User-Agent": (
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36"
                    ),
                    "Accept": "application/json, text/plain, */*",
                    "Referer": "https://www.loc.gov/",
                },
            )
            with urllib.request.urlopen(req, timeout=LOC_TIMEOUT_S) as resp:
                _last_request_time = time.time()
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except Exception as e:
            wait = 2 ** attempt
            print(f"    [retry {attempt+1}] {e}  sleeping {wait}s")
            time.sleep(wait)
    return None


def _parse_loc_item(item: dict, game_key: str, search_level: int,
                    query: str, rank: int, pub_date: str) -> dict | None:
    """
    Parse one result item from the new LOC collections API (?fo=json).
    Returns kwargs dict for _upsert_source, or None if unusable.

    New API structure (different from old chroniclingamerica.loc.gov):
      item["url"]          — page viewer URL (Cloudflare-blocked, used as identifier only)
      item["image_url"][]  — list of URLs; entries contain either:
                              * IIIF tile URLs ending in .jpg → tile images
                              * word-coordinates URLs containing "word-coordinates" → OCR text
      item["number_lccn"]  — list of LCCNs
      item["date"]         — YYYY-MM-DD
      item["title"]        — "Image N of Newspaper Name ..."
    """
    page_url  = item.get("url", "")
    imgs      = item.get("image_url", [])
    lccn_list = item.get("number_lccn", [])
    lccn      = lccn_list[0] if lccn_list else None

    # Separate tile URLs from OCR/word-coordinate URLs
    tile_sm = tile_md = wc_url = ""
    for img in imgs:
        if not img:
            continue
        if "word-coordinates-service" in img:
            wc_url = img.split("#")[0]  # strip hash fragment
        elif "pct:6.25" in img:
            tile_sm = img.split("#")[0]
        elif "pct:12.5" in img:
            tile_md = img.split("#")[0]

    if not (page_url or tile_sm or wc_url):
        return None

    # The OCR fulltext URL uses the segment path embedded in the wc_url
    # e.g. ...?segment=/service/ndnp/dlc/.../0425.xml&...
    # We need this segment path to build LOC_FULLTEXT_FMT
    local_json = None  # populated after fetch
    if wc_url:
        # Store the word-coordinates URL as json_url — fetch.py will use it to get OCR
        pass

    return dict(
        game_key=game_key,
        source_key=_source_key(page_url or tile_sm or wc_url),
        source_type="LOC_TILE",
        search_level=search_level,
        page_url=page_url,
        tile_url_sm=tile_sm or None,
        tile_url_md=tile_md or None,
        json_url=wc_url or None,   # word-coordinates endpoint = our OCR source
        newspaper_lccn=lccn,
        newspaper_name=(item.get("title") or "")[:120],
        publication_date=item.get("date") or pub_date,
        page_seq=None,
        search_query=query,
        search_rank=rank,
        relevance_score=float(item.get("score") or 0),
    )


def _source_key(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _upsert_source(
    conn: duckdb.DuckDBPyConnection,
    game_key: str,
    source_key: str,
    source_type: str,
    search_level: int,
    page_url: str = None,
    tile_url_sm: str = None,
    tile_url_md: str = None,
    tile_url_lg: str = None,
    json_url: str = None,
    newspaper_lccn: str = None,
    newspaper_name: str = None,
    publication_date: str = None,
    page_seq: int = None,
    search_query: str = None,
    search_rank: int = None,
    relevance_score: float = None,
) -> bool:
    # Sanitize publication_date — DuckDB DATE rejects "None", "", "null"
    if isinstance(publication_date, str) and publication_date.lower().strip() in (
        "none", "null", ""
    ):
        publication_date = None
    # Accept bare years like "1920" → None (not a valid date)
    if isinstance(publication_date, str) and len(publication_date) <= 4:
        publication_date = None
    """Insert source_ledger row; returns True if newly inserted."""
    exists = conn.execute(
        "SELECT 1 FROM source_ledger WHERE source_key=?", [source_key]
    ).fetchone()
    if exists:
        return False
    conn.execute("""
        INSERT INTO source_ledger
        (source_key, game_key, source_type, search_level,
         newspaper_lccn, newspaper_name, publication_date, page_seq,
         page_url, tile_url_sm, tile_url_md, tile_url_lg, json_url,
         search_query, search_rank, relevance_score, fetch_state)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'CANDIDATE')
    """, [
        source_key, game_key, source_type, search_level,
        newspaper_lccn, newspaper_name, publication_date, page_seq,
        page_url, tile_url_sm, tile_url_md, tile_url_lg, json_url,
        search_query, search_rank, relevance_score,
    ])
    return True


# ── Strategy implementations ──────────────────────────────────────────────────

def strategy_pfa_html(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    Pro Football Archives: guess the HTML box score URL from year + sequential game index.
    PFA uses URLs like: /nflboxscores1/1921apfa031.html
    We add the "guessed" URL as a candidate; actual validation happens in fetch/assess.
    Returns number of candidates added.
    """
    year = game["year"]
    prefix = pfa_url_prefix(year)
    # We don't know the exact sequence number without crawling; add a range of candidates
    # starting from 1 and let assess filter.  Limit to ~20 per year-game.
    # A better approach uses the PFA game index page but that requires an extra fetch.
    base = f"https://www.profootballarchives.com/nflboxscores1/"
    added = 0
    # For now, add the index page itself as a LOC_PFA_INDEX type so fetch.py
    # can parse it to find the actual game URL.
    index_url = f"{base}"
    key = _source_key(f"pfa_index_{year}")
    gdate = game.get("game_date")
    ok = _upsert_source(
        conn, game_key, key, "PFA_INDEX", game["search_level"],
        page_url=index_url,
        publication_date=str(gdate)[:10] if gdate else None,
        search_query=f"PFA {year} index",
    )
    if ok:
        added += 1
    return added


def strategy_pfr_boxscore(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    PFR: build search URL for the game's boxscore page.
    PFR game URLs follow /boxscores/YYYYMMDD{team}0.htm pattern.
    We generate the most likely candidates for the home team.
    """
    year = game["year"]
    gdate_raw = game.get("game_date")
    gdate = str(gdate_raw)[:10] if gdate_raw else None
    date_str = (gdate or f"{int(year)}-01-01").replace("-", "")

    added = 0
    for team in [game.get("team_a"), game.get("team_b")]:
        if not team:
            continue
        team_lower = team.lower().replace("_", "")
        url = f"https://www.pro-football-reference.com/boxscores/{date_str}{team_lower}0.htm"
        key = _source_key(url)
        ok = _upsert_source(
            conn, game_key, key, "PFR_BOXSCORE", game["search_level"],
            page_url=url, search_query=f"PFR boxscore {team} {gdate}",
        )
        if ok:
            added += 1
    return added


def _game_date_window(game: dict) -> tuple[date, str, str]:
    """Return (base_date, YYYYMMDD_start, YYYYMMDD_end) for a game."""
    year, week = game["year"], game["week"]
    gdate = game.get("game_date")
    if gdate:
        try:
            base = date.fromisoformat(str(gdate)[:10])
        except ValueError:
            base = date(year, 9, 1)
    else:
        base = date(year, 9, 1) + timedelta(days=(6 - date(year, 9, 1).weekday()) % 7)
        base += timedelta(weeks=week - 1)
    # Box scores appear day-of (evening) + day-after (morning)
    d1 = base.strftime("%Y-%m-%d").replace("-", "")
    d2 = (base + timedelta(days=1)).strftime("%Y-%m-%d").replace("-", "")
    return base, d1, d2


def _loc_search_lccn(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict,
    lccn: str, query: str, base_date: date,
    d1: str, d2: str, max_results: int = 6,
) -> int:
    """
    Run one LOC search for a specific LCCN and query, parse new API response,
    upsert results into source_ledger.  Returns count added.
    """
    url = (
        f"{LOC_SEARCH}?q={urllib.parse.quote(query)}"
        f"&fo=json&at=results"
        f"&dates={d1[:4]}%2F{d2[:4]}"  # year filter (YYYY%2FYYYY)
        f"&lccn={lccn}&c={max_results}"
    )
    data = _loc_get(url)
    if not data:
        return 0

    # Date tolerance: ±2 days when game_date is known, ±7 when estimated from week.
    # 1920s game dates are almost never known exactly, so be generous.
    known_date = bool(game.get("game_date"))
    date_tol = 2 if known_date else 7

    results = data.get("results", [])
    added = 0
    for rank, item in enumerate(results[:max_results]):
        item_date = item.get("date", "")
        if item_date:
            try:
                idate = date.fromisoformat(item_date[:10])
                if abs((idate - base_date).days) > date_tol:
                    continue
            except ValueError:
                pass

        parsed = _parse_loc_item(item, game_key, game["search_level"],
                                  query, rank, base_date.isoformat())
        if not parsed:
            continue
        ok = _upsert_source(conn, **parsed)
        if ok:
            added += 1
    return added


def _build_query(terms_a: list[str], terms_b: list[str]) -> str:
    """Build LOC search query from franchise search terms."""
    parts = []
    if terms_a:
        parts.append(terms_a[0])
    if terms_b:
        parts.append(terms_b[0])
    return " ".join(parts) + " football" if parts else ""


def strategy_loc_home_city(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    Search the home team's local newspaper on LOC.
    Only useful for T0 (1920-1931) where local papers covered local teams.
    """
    if game["year"] > LOC_MAX_YEAR:
        return 0
    year = game["year"]
    fran_a = game.get("franchise_a")
    fran_b = game.get("franchise_b")
    terms_a = get_search_terms(fran_a, year) if fran_a else []
    terms_b = get_search_terms(fran_b, year) if fran_b else []
    query = _build_query(terms_a, terms_b)
    if not query:
        return 0

    base, d1, d2 = _game_date_window(game)
    city_a = FRANCHISE_CITY.get(fran_a)
    city_b = FRANCHISE_CITY.get(fran_b)
    lccns_tried = set()
    added = 0
    for city in [city_a, city_b]:
        if not city:
            continue
        for lccn in CITY_PAPERS.get(city, [])[:2]:
            if lccn in lccns_tried:
                continue
            lccns_tried.add(lccn)
            added += _loc_search_lccn(conn, game_key, game, lccn, query, base, d1, d2)
    return added


def strategy_loc_evening_star(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    Washington Evening Star (sn83045462) — the single best NFL source in the
    LOC corpus.  Has national NFL coverage (not just Redskins) through ~1945.
    Clean OCR, dense game recaps.

    Confirmed via reconnaissance (2026-06-17): 40 results for Bears-Giants
    championship 1940, clean readable text, player names and scores present.
    """
    if game["year"] > LOC_MAX_YEAR:
        return 0
    year = game["year"]
    fran_a = game.get("franchise_a")
    fran_b = game.get("franchise_b")
    terms_a = get_search_terms(fran_a, year) if fran_a else []
    terms_b = get_search_terms(fran_b, year) if fran_b else []
    query = _build_query(terms_a, terms_b)
    if not query:
        return 0

    base, d1, d2 = _game_date_window(game)
    EVENING_STAR = "sn83045462"
    return _loc_search_lccn(conn, game_key, game, EVENING_STAR, query,
                             base, d1, d2, max_results=8)


def strategy_loc_milwaukee_leader(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    Milwaukee Leader (sn83045293) — strong coverage of Green Bay Packers and
    Midwest NFL games 1920-1935.  Confirmed via reconnaissance (2026-06-17):
    8 results for Packers-Bears games in 1930, with standings and game scores.
    """
    if game["year"] > LOC_MAX_YEAR:
        return 0
    year = game["year"]
    fran_a = game.get("franchise_a")
    fran_b = game.get("franchise_b")
    terms_a = get_search_terms(fran_a, year) if fran_a else []
    terms_b = get_search_terms(fran_b, year) if fran_b else []
    query = _build_query(terms_a, terms_b)
    if not query:
        return 0

    base, d1, d2 = _game_date_window(game)
    MILWAUKEE = "sn83045293"
    return _loc_search_lccn(conn, game_key, game, MILWAUKEE, query,
                             base, d1, d2, max_results=5)


def strategy_loc_score_sweep(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    Exhaustive sweep: search ALL LOC papers mentioning both team names
    around the game date (no LCCN restriction).
    Highest budget — only run at final level before flag_manual.
    LOC is dead for years > LOC_MAX_YEAR (1945).
    """
    if game["year"] > LOC_MAX_YEAR:
        return 0
    year = game["year"]
    fran_a = game.get("franchise_a")
    fran_b = game.get("franchise_b")
    terms_a = get_search_terms(fran_a, year) if fran_a else []
    terms_b = get_search_terms(fran_b, year) if fran_b else []
    if not terms_a or not terms_b:
        return 0

    base, d1, d2 = _game_date_window(game)

    # Try a 3-day window and multiple query variants
    d2_wide = (base + timedelta(days=2)).strftime("%Y%m%d")
    queries = [
        f"{terms_a[0]} {terms_b[0]} football",
        f"{terms_a[0]} {terms_b[0]}",
    ]
    if len(terms_a) > 1:
        queries.append(f"{terms_a[1]} {terms_b[0]} football")

    known_date = bool(game.get("game_date"))
    date_tol = 3 if known_date else 10  # generous for estimated dates

    added = 0
    for query in queries:
        url = (
            f"{LOC_SEARCH}?q={urllib.parse.quote(query)}"
            f"&fo=json&at=results"
            f"&dates={d1[:4]}%2F{d2[:4]}"
            f"&c=10"
        )
        data = _loc_get(url)
        if not data:
            continue
        for rank, item in enumerate(data.get("results", [])[:10]):
            item_date = item.get("date", "")
            if item_date:
                try:
                    idate = date.fromisoformat(item_date[:10])
                    if abs((idate - base).days) > date_tol:
                        continue
                except ValueError:
                    pass
            parsed = _parse_loc_item(item, game_key, game["search_level"],
                                      query, rank, base.isoformat())
            if not parsed:
                continue
            ok = _upsert_source(conn, **parsed)
            if ok:
                added += 1
    return added


def strategy_local_pbp_replay(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    """
    T3 (1978-1993): check whether the local stathead PBP parquet has
    tackle attribution for this game's players.  Adds a LOCAL_FILE source.
    """
    from .config import V26_GLOB
    from pathlib import Path
    parquets = sorted(Path(r"D:\sports-data\nfl\source\pbp").glob("*.parquet"))
    if not parquets:
        return 0
    # Add each PBP parquet as a LOCAL_FILE candidate — assess.py will query it
    added = 0
    for p in parquets:
        key = _source_key(f"local_pbp_{game_key}_{p.name}")
        ok = _upsert_source(
            conn, game_key, key, "LOCAL_FILE", game["search_level"],
            page_url=str(p), search_query=f"local PBP replay {game_key}",
        )
        if ok:
            added += 1
            break  # one PBP file is enough for this strategy
    return added


def strategy_flag_manual(
    conn: duckdb.DuckDBPyConnection, game_key: str, game: dict
) -> int:
    conn.execute(
        "UPDATE game_manifest SET state='MANUAL_REVIEW', last_updated=CURRENT_TIMESTAMP WHERE game_key=?",
        [game_key]
    )
    return 0


# ── Strategy dispatcher ───────────────────────────────────────────────────────

STRATEGY_FNS = {
    "pfa_html":              strategy_pfa_html,
    "pfr_boxscore":          strategy_pfr_boxscore,
    "loc_home_city":         strategy_loc_home_city,
    "loc_evening_star":      strategy_loc_evening_star,
    "loc_milwaukee_leader":  strategy_loc_milwaukee_leader,
    "loc_score_sweep":       strategy_loc_score_sweep,
    "local_pbp_replay":      strategy_local_pbp_replay,
    "flag_manual":           strategy_flag_manual,
    # Legacy aliases (kept for any old manifest rows)
    "loc_home_away":         strategy_loc_home_city,
    "loc_national":          strategy_loc_evening_star,
    "pfr_gamelog":           strategy_pfr_boxscore,
}


def discover_game(conn: duckdb.DuckDBPyConnection, game_key: str) -> int:
    """
    Run the next undone search level for a game.
    Returns number of new candidates added.
    """
    row = conn.execute("""
        SELECT year, week, season_type, franchise_a, franchise_b,
               team_a, team_b, game_date, tier, state, current_level
        FROM game_manifest WHERE game_key=?
    """, [game_key]).fetchone()
    if not row:
        raise KeyError(game_key)

    (year, week, stype, fa, fb, ta, tb, gdate, tier, state, cur_level) = row

    if state in ("CLOSED", "CLOSED_NEGATIVE", "SKIP", "MANUAL_REVIEW"):
        return 0

    levels = SEARCH_LEVELS.get(tier, [])
    budget  = source_budget(tier)

    # Find the next level to run
    next_level_def = None
    for ldef in levels:
        if ldef["level"] > cur_level:
            next_level_def = ldef
            break

    if next_level_def is None:
        # All levels exhausted → close as negative
        conn.execute(
            "UPDATE game_manifest SET state='CLOSED_NEGATIVE', last_updated=CURRENT_TIMESTAMP WHERE game_key=?",
            [game_key]
        )
        return 0

    level_num = next_level_def["level"]
    strategy  = next_level_def["strategy"]
    label     = next_level_def["label"]
    print(f"  {game_key}  level={level_num} [{label}]")

    game = dict(
        year=year, week=week, season_type=stype,
        franchise_a=fa, franchise_b=fb,
        team_a=ta, team_b=tb, game_date=gdate,
        tier=tier, search_level=level_num,
    )

    fn = STRATEGY_FNS.get(strategy)
    added = fn(conn, game_key, game) if fn else 0

    # If nothing was found at this level, immediately advance (nothing to fetch/assess).
    # Only stay DISCOVERED if we actually queued something for fetch.py.
    new_state = "DISCOVERED" if added > 0 else "LEVEL_UP"

    conn.execute("""
        UPDATE game_manifest
        SET current_level=?, sources_found=sources_found+?,
            state=?, last_updated=CURRENT_TIMESTAMP
        WHERE game_key=?
    """, [level_num, added, new_state, game_key])

    return added


def discover_batch(
    conn: duckdb.DuckDBPyConnection,
    tier: str | None = None,
    limit: int = 50,
) -> dict[str, int]:
    """
    Run discovery for up to `limit` games in NEW or LEVEL_UP state.
    Returns {game_key: candidates_added}.
    """
    tier_clause = f"AND tier='{tier}'" if tier else ""
    rows = conn.execute(f"""
        SELECT game_key FROM game_manifest
        WHERE state IN ('NEW', 'LEVEL_UP') {tier_clause}
        ORDER BY tier, year, week
        LIMIT {limit}
    """).fetchall()

    results = {}
    for (gk,) in rows:
        n = discover_game(conn, gk)
        results[gk] = n
        print(f"    → {n} candidates")
    return results
