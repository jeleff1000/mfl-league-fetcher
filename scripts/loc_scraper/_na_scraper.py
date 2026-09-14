"""
NewspaperArchive.com tag scraper — fills CLOSED_NEGATIVE gaps for T1/T2 games
where LOC coverage is thin or dead (1932-1977).

Three phases:
  --login       Open browser, user logs in manually, save session cookies.
                Run once per trial account.

  --discover    Fan team tag pages for each CLOSED_NEGATIVE game.
                Collects article URLs → source_ledger (NA_HTML CANDIDATE).
                Reopens CLOSED_NEGATIVE games that get ≥1 hit.

  --fetch       Download article HTML for CANDIDATE NA_HTML sources.

  --probe SLUG YEAR
                Hit one tag URL and print all extracted links.
                Run this FIRST to calibrate selectors before full discovery.

Usage:
  python scripts\\loc_scraper\\_na_scraper.py --login
  python scripts\\loc_scraper\\_na_scraper.py --probe chicago-bears 1951
  python scripts\\loc_scraper\\_na_scraper.py --discover --tier T2
  python scripts\\loc_scraper\\_na_scraper.py --fetch --limit 300
  python scripts\\loc_scraper\\_na_scraper.py --status
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from playwright.sync_api import sync_playwright

from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.config import get_search_terms

# ── Storage ────────────────────────────────────────────────────────────────────

NA_DIR         = Path(r"D:\league-history-data\nfl\curated\na_scraper")
NA_SESSION     = NA_DIR / "na_session.json"
NA_HTML_DIR    = NA_DIR / "html"
NA_BASE        = "https://newspaperarchive.com"
NA_MIN_WAIT    = 3.5    # seconds between page requests
NA_MAX_PER_GAME = 20    # max articles to queue per game (preserve trial quota)

# ── Slug builder ───────────────────────────────────────────────────────────────

def slugify(name: str) -> str:
    """'Chicago Bears' → 'chicago-bears', 'San Francisco 49ers' → 'san-francisco-49ers'"""
    name = name.lower()
    name = re.sub(r"[.''']", "", name)
    name = re.sub(r"[^a-z0-9\s-]", "", name)
    name = re.sub(r"\s+", "-", name.strip())
    return name


def franchise_slug(franchise_num: int, year: int) -> str | None:
    terms = get_search_terms(franchise_num, year)
    return slugify(terms[0]) if terms else None


def tag_url(slug: str, year: int, page: int = 1) -> str:
    return f"{NA_BASE}/tags/{slug}/?pci={page}&ndt=by&py={year}&pey={year}"


# ── Session management ─────────────────────────────────────────────────────────

def save_session(context, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cookies = context.cookies()
    path.write_text(json.dumps(cookies, indent=2))
    print(f"Session saved → {path}  ({len(cookies)} cookies)")


def load_session(context, path: Path) -> bool:
    if not path.exists():
        return False
    cookies = json.loads(path.read_text())
    context.add_cookies(cookies)
    print(f"Session loaded: {len(cookies)} cookies")
    return True


# ── Article link extraction ────────────────────────────────────────────────────
# NA search results show article cards — each card links to an individual
# article viewer page. URLs look like:
#   /some-paper-name-YYYY-MM-DD-p3/       (ISO date in path)
#   /some-paper-name-month-DD-YYYY-p3/    (text month variant)
# Both contain a 4-digit year; neither contains "tags", "search", "pricing", etc.

_SKIP_HREF_RE = re.compile(
    r"/(tags|search|pricing|login|register|help|about|contact|sitemap|"
    r"privacy|terms|account|cart|checkout)/",
    re.I,
)
_ARTICLE_HREF_RE = re.compile(
    r"^/[a-z0-9][a-z0-9-]{5,}-\d{4}[^#?\"]*/?$",
    re.I,
)

# CSS selectors tried in order for article card links
_LINK_SELECTORS = [
    "a.result-title",
    "a.article-result-title",
    ".result-item a[href]",
    ".search-result a[href]",
    ".article-card a[href]",
    "article a[href]",
    ".clipping-result a[href]",
    "h2 > a[href]",
    "h3 > a[href]",
]


def extract_article_links(page) -> list[str]:
    """Return list of absolute article URLs from a tag search results page."""
    links: set[str] = set()

    # Try typed selectors first (more precise)
    for sel in _LINK_SELECTORS:
        try:
            for el in page.query_selector_all(sel):
                href = (el.get_attribute("href") or "").strip()
                if _is_article_href(href):
                    links.add(href if href.startswith("http") else NA_BASE + href)
        except Exception:
            pass
        if links:
            break  # stop at first selector that yields results

    # Broad fallback: all <a> tags with matching hrefs
    if not links:
        try:
            for el in page.query_selector_all("a[href]"):
                href = (el.get_attribute("href") or "").strip()
                if _is_article_href(href):
                    links.add(href if href.startswith("http") else NA_BASE + href)
        except Exception:
            pass

    # Last resort: regex on raw HTML
    if not links:
        html = page.content()
        for m in re.finditer(r'href="(/[a-z0-9][a-z0-9-]+-\d{4}[^"]*/?)"', html, re.I):
            href = m.group(1)
            if _is_article_href(href):
                links.add(NA_BASE + href)

    return sorted(links)


def _is_article_href(href: str) -> bool:
    if not href or not href.startswith("/"):
        return False
    if _SKIP_HREF_RE.search(href):
        return False
    # Must contain a year-like number (prevents matching /tags/x/ etc.)
    if not re.search(r"\d{4}", href):
        return False
    # Must look like a slugged newspaper URL (hyphens, alphanumeric)
    return bool(_ARTICLE_HREF_RE.match(href))


def result_count(page) -> int:
    """Parse total result count from page text (e.g. '672 records')."""
    try:
        text = page.inner_text("body")[:3000]
        m = re.search(r"([\d,]+)\s+(?:records?|results?|clippings?)", text, re.I)
        if m:
            return int(m.group(1).replace(",", ""))
    except Exception:
        pass
    return -1


# ── Paywall detection ─────────────────────────────────────────────────────────

def is_paywalled(page) -> bool:
    url = page.url.lower()
    if any(kw in url for kw in ("/login", "/subscribe", "/pricing", "/register")):
        return True
    try:
        text = page.inner_text("body")[:2000].lower()
        return any(kw in text for kw in (
            "subscribe to read",
            "start your free trial",
            "sign in to view",
            "create a free account to view",
        ))
    except Exception:
        return False


# ── DB helpers ─────────────────────────────────────────────────────────────────

def source_key_for_url(url: str) -> str:
    h = hashlib.sha256(url.encode()).hexdigest()[:16]
    return f"na_{h}"


def upsert_na_source(conn, game_key: str, url: str, level: int = 5) -> bool:
    """Insert NA_HTML CANDIDATE into source_ledger. Returns True if new."""
    sk = source_key_for_url(url)
    if conn.execute("SELECT 1 FROM source_ledger WHERE source_key=?", [sk]).fetchone():
        return False
    conn.execute("""
        INSERT INTO source_ledger
            (source_key, game_key, source_type, search_level, page_url, fetch_state)
        VALUES (?, ?, 'NA_HTML', ?, ?, 'CANDIDATE')
    """, [sk, game_key, level, url])
    return True


def reopen_game(conn, game_key: str) -> None:
    """Reopen a CLOSED_NEGATIVE game that now has NA_HTML sources."""
    conn.execute("""
        UPDATE game_manifest
        SET state='DISCOVERED', last_updated=CURRENT_TIMESTAMP
        WHERE game_key=? AND state='CLOSED_NEGATIVE'
    """, [game_key])


# ── Browser factory ───────────────────────────────────────────────────────────

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)


def _make_context(pw, headless: bool = True):
    browser = pw.chromium.launch(headless=headless, slow_mo=100 if not headless else 0)
    ctx = browser.new_context(user_agent=_UA)
    return browser, ctx


# ── Phase: login ──────────────────────────────────────────────────────────────

def phase_login():
    """Open browser → user logs in manually → save session."""
    NA_DIR.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        browser, ctx = _make_context(pw, headless=False)
        page = ctx.new_page()
        page.goto(f"{NA_BASE}/login/")
        print("\nBrowser open. Log in to NewspaperArchive.com, then press ENTER here.")
        input()
        save_session(ctx, NA_SESSION)
        browser.close()
    print("Login complete. Use --discover to start scraping.")


# ── Phase: probe ──────────────────────────────────────────────────────────────

def phase_probe(slug: str, year: int):
    """
    Hit one tag URL and print extracted links + page stats.
    Run before --discover to verify selectors work.
    """
    url = tag_url(slug, year)
    print(f"Probing: {url}")

    with sync_playwright() as pw:
        browser, ctx = _make_context(pw, headless=True)
        if load_session(ctx, NA_SESSION):
            print("  (using saved session)")
        page = ctx.new_page()
        page.goto(url, timeout=30_000, wait_until="domcontentloaded")

        count = result_count(page)
        links = extract_article_links(page)
        paywalled = is_paywalled(page)

        print(f"\n  Final URL:    {page.url}")
        print(f"  Result count: {count}")
        print(f"  Links found:  {len(links)}")
        print(f"  Paywalled:    {paywalled}")

        if links:
            print(f"\n  First 10 article links:")
            for lnk in links[:10]:
                print(f"    {lnk}")
        else:
            # Dump first 2KB of HTML for debugging
            html = page.content()
            print(f"\n  No links extracted. Page HTML (first 2KB):")
            print(html[:2000])

        browser.close()


# ── Phase: discover ───────────────────────────────────────────────────────────

def phase_discover(tier_filter: str | None = None, year_filter: int | None = None):
    """Fan tag pages for CLOSED_NEGATIVE games, collect NA_HTML candidates."""
    NA_DIR.mkdir(parents=True, exist_ok=True)
    conn = open_db()

    tier_clause = f"AND tier='{tier_filter}'" if tier_filter else ""
    year_clause = f"AND year={year_filter}" if year_filter else ""

    games = conn.execute(f"""
        SELECT game_key, year, week, franchise_a, franchise_b, tier
        FROM game_manifest
        WHERE state='CLOSED_NEGATIVE'
          {tier_clause} {year_clause}
        ORDER BY tier, year, week
    """).fetchall()

    print(f"NA discover: {len(games)} CLOSED_NEGATIVE games  "
          f"tier={tier_filter or 'ALL'}  year={year_filter or 'ALL'}")
    if not games:
        print("Nothing to do.")
        return

    last_req = [0.0]

    def polite_goto(page, url: str) -> bool:
        elapsed = time.time() - last_req[0]
        if elapsed < NA_MIN_WAIT:
            time.sleep(NA_MIN_WAIT - elapsed)
        try:
            page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            last_req[0] = time.time()
            return True
        except Exception as e:
            last_req[0] = time.time()
            print(f"    [nav err] {e}")
            return False

    total_added = 0
    games_hit = 0

    with sync_playwright() as pw:
        browser, ctx = _make_context(pw)
        if not load_session(ctx, NA_SESSION):
            print("WARNING: no saved session — results may be limited. Run --login first.")
        page = ctx.new_page()

        for i, (gk, year, week, fa, fb, tier) in enumerate(games, 1):
            slug_a = franchise_slug(fa, year)
            slug_b = franchise_slug(fb, year)
            slugs = [s for s in [slug_a, slug_b] if s]

            if not slugs:
                print(f"  [{i}/{len(games)}] {gk} — no slug, skip")
                continue

            game_added = 0
            for slug in slugs:
                if game_added >= NA_MAX_PER_GAME:
                    break
                url = tag_url(slug, year)
                if not polite_goto(page, url):
                    continue
                if is_paywalled(page):
                    print(f"  [{i}/{len(games)}] PAYWALL — session expired? Re-run --login")
                    browser.close()
                    return

                count = result_count(page)
                links = extract_article_links(page)
                new = 0
                for lnk in links[:NA_MAX_PER_GAME - game_added]:
                    if upsert_na_source(conn, gk, lnk):
                        game_added += 1
                        new += 1
                        total_added += 1

                print(f"  [{i}/{len(games)}] {gk}  yr={year} wk={week:2d}  "
                      f"slug={slug:<30}  count={count:>4}  links={len(links):>3}  new={new}")

            if game_added > 0:
                reopen_game(conn, gk)
                conn.execute("""
                    UPDATE game_manifest
                    SET sources_found = sources_found + ?
                    WHERE game_key = ?
                """, [game_added, gk])
                games_hit += 1

            if i % 50 == 0:
                print(f"\n  === pass {i}/{len(games)}  total_added={total_added} ===\n")

        browser.close()

    print(f"\nDiscover done: {total_added} NA_HTML sources across {games_hit} games")
    print(f"Run --fetch to download article HTML.")


# ── Phase: fetch ──────────────────────────────────────────────────────────────

def phase_fetch(limit: int = 200):
    """Download article HTML for CANDIDATE NA_HTML sources."""
    NA_HTML_DIR.mkdir(parents=True, exist_ok=True)
    conn = open_db()

    sources = conn.execute("""
        SELECT sl.source_key, sl.page_url, gm.game_key, gm.year, gm.week
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key = sl.game_key
        WHERE sl.source_type = 'NA_HTML'
          AND sl.fetch_state = 'CANDIDATE'
        ORDER BY gm.year, gm.week, sl.created_at
        LIMIT ?
    """, [limit]).fetchall()

    print(f"NA fetch: {len(sources)} articles to download (limit={limit})")
    if not sources:
        print("Nothing to fetch. Run --discover first.")
        return

    last_req = [0.0]
    ok = fail = skipped = 0

    with sync_playwright() as pw:
        browser, ctx = _make_context(pw)
        if not load_session(ctx, NA_SESSION):
            print("WARNING: no saved session — most articles will be paywalled.")
        page = ctx.new_page()

        for i, (sk, url, gk, year, week) in enumerate(sources, 1):
            # Idempotent: already on disk
            out_path = NA_HTML_DIR / f"{sk}.html"
            if out_path.exists():
                conn.execute("""
                    UPDATE source_ledger
                    SET local_tile_sm=?, fetch_state='FETCHED',
                        fetch_at=CURRENT_TIMESTAMP
                    WHERE source_key=?
                """, [str(out_path), sk])
                skipped += 1
                continue

            elapsed = time.time() - last_req[0]
            if elapsed < NA_MIN_WAIT:
                time.sleep(NA_MIN_WAIT - elapsed)

            try:
                resp = page.goto(url, timeout=30_000, wait_until="domcontentloaded")
                last_req[0] = time.time()
                status = resp.status if resp else 0

                if is_paywalled(page):
                    conn.execute("""
                        UPDATE source_ledger
                        SET fetch_state='FETCH_FAILED', fetch_http_status=402,
                            fetch_error='paywall', fetch_at=CURRENT_TIMESTAMP
                        WHERE source_key=?
                    """, [sk])
                    fail += 1
                    print(f"  [{i}/{len(sources)}] PAYWALL {gk} {sk[:20]}")
                    # If we're hitting paywall on every article, session is dead
                    if fail >= 3 and ok == 0:
                        print("\n  3 consecutive paywalls with 0 successes.")
                        print("  Session expired — re-run --login with new trial account.")
                        break
                elif status in (200, 0):
                    html = page.content()
                    out_path.write_text(html, encoding="utf-8", errors="replace")
                    conn.execute("""
                        UPDATE source_ledger
                        SET local_tile_sm=?, fetch_state='FETCHED',
                            fetch_http_status=?, fetch_at=CURRENT_TIMESTAMP
                        WHERE source_key=?
                    """, [str(out_path), status, sk])
                    ok += 1
                    print(f"  [{i}/{len(sources)}] OK  {gk}  {sk[:20]}  ({len(html)//1024}KB)")
                else:
                    conn.execute("""
                        UPDATE source_ledger
                        SET fetch_state='FETCH_FAILED', fetch_http_status=?,
                            fetch_at=CURRENT_TIMESTAMP
                        WHERE source_key=?
                    """, [status, sk])
                    fail += 1
                    print(f"  [{i}/{len(sources)}] HTTP {status}  {sk[:20]}")

            except Exception as e:
                last_req[0] = time.time()
                conn.execute("""
                    UPDATE source_ledger
                    SET fetch_state='FETCH_FAILED', fetch_error=?,
                        fetch_at=CURRENT_TIMESTAMP
                    WHERE source_key=?
                """, [str(e)[:200], sk])
                fail += 1
                print(f"  [{i}/{len(sources)}] ERR  {sk[:20]}: {e}")

            if i % 50 == 0:
                size_mb = sum(f.stat().st_size for f in NA_HTML_DIR.glob("*.html")) / 1_048_576
                print(f"\n  === {i}/{len(sources)}: {ok} OK, {fail} fail, {skipped} skip | "
                      f"disk={size_mb:.1f}MB ===\n")

        browser.close()

    size_mb = sum(f.stat().st_size for f in NA_HTML_DIR.glob("*.html")) / 1_048_576
    print(f"\nFetch done: {ok} OK, {fail} failed, {skipped} already on disk")
    print(f"HTML dir:   {NA_HTML_DIR}  ({size_mb:.1f} MB)")


# ── Phase: status ─────────────────────────────────────────────────────────────

def phase_status():
    conn = open_db()

    print("=== NA source_ledger status ===")
    rows = conn.execute("""
        SELECT sl.fetch_state, gm.tier, COUNT(*) as n
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key = sl.game_key
        WHERE sl.source_type = 'NA_HTML'
        GROUP BY sl.fetch_state, gm.tier
        ORDER BY gm.tier, sl.fetch_state
    """).fetchall()
    if rows:
        for state, tier, n in rows:
            print(f"  {tier}  {state:<15}  {n:>5}")
    else:
        print("  (no NA_HTML sources yet)")

    total_size = (
        sum(f.stat().st_size for f in NA_HTML_DIR.glob("*.html")) / 1_048_576
        if NA_HTML_DIR.exists() else 0
    )
    print(f"\nHTML files on disk: {total_size:.1f} MB  ({NA_HTML_DIR})")

    closed_neg = conn.execute("""
        SELECT tier, COUNT(*) FROM game_manifest
        WHERE state='CLOSED_NEGATIVE'
        GROUP BY tier ORDER BY tier
    """).fetchall()
    print("\nRemaining CLOSED_NEGATIVE (no NA sources yet):")
    for tier, n in closed_neg:
        print(f"  {tier}  {n}")


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="NewspaperArchive.com scraper for NFL game articles"
    )
    ap.add_argument("--login",    action="store_true",
                    help="Open browser for manual login + save session")
    ap.add_argument("--probe",    nargs=2, metavar=("SLUG", "YEAR"),
                    help="Test link extraction on one tag page (e.g. chicago-bears 1951)")
    ap.add_argument("--discover", action="store_true",
                    help="Discover article URLs for CLOSED_NEGATIVE games")
    ap.add_argument("--fetch",    action="store_true",
                    help="Download article HTML for CANDIDATE NA_HTML sources")
    ap.add_argument("--status",   action="store_true",
                    help="Print current NA scraping status")
    ap.add_argument("--tier",  default=None,
                    help="Tier filter for --discover (T0, T1, T2, ...)")
    ap.add_argument("--year",  type=int, default=None,
                    help="Year filter for --discover")
    ap.add_argument("--limit", type=int, default=200,
                    help="Max articles for --fetch (default 200)")
    args = ap.parse_args()

    if args.login:
        phase_login()
    elif args.probe:
        phase_probe(args.probe[0], int(args.probe[1]))
    elif args.discover:
        phase_discover(tier_filter=args.tier, year_filter=args.year)
    elif args.fetch:
        phase_fetch(limit=args.limit)
    elif args.status:
        phase_status()
    else:
        ap.print_help()
