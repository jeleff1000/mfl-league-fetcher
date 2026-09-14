"""
sota_recon/nflcom_harvest.py -- comprehensive NFL.com WITNESS harvester.

NFL.com stat pages are SERVER-RENDERED HTML (a <table> per stat block, one <tr> per row); no JS,
no API token, no GraphQL. This harvests the FULL NFL.com witness slate -- offense, defense, special
teams, team, league -- at every grain, mirroring what we hold for PFR, so it can be introspected into
witness_contracts and used for gap backfills. NFL.com uniquely carries **fumbles_lost** (and FF / OWN
FR / OPP FR) at weekly, season, and career grain for every position -- the atom PFR entirely lacks.

TAXONOMY (all page types are plain-HTML tables):
  player_season  /stats/player-stats/category/{cat}/{year}/{st}/all/{sort}/desc   (ALL players/year, 25/page,
                 cursor-paginated). 11 categories: passing rushing receiving fumbles tackles interceptions
                 field-goals kickoffs kickoff-returns punts punt-returns. Year floor >= 1932.
  team_stats     /stats/team-stats/{side}/{cat}/{year}/{st}/all   (team + league grain; side in
                 offense/defense/special-teams; cat in passing rushing receiving scoring downs)
  player_logs    /players/{slug}/stats/logs/{year}/     (GAME grain; trailing FUM/LOST)
  player_career  /players/{slug}/stats/                 (per-season rows + career totals, per-position table)
  player_splits  /players/{slug}/stats/splits/{year}/   (by day/opponent/stadium; carries fumble breakdown)
  player_situational /players/{slug}/stats/situational/{year}/  (home/road, margin, surface)

Player SLUGS are discovered from the bulk category pages (every row links to /players/{slug}/), so the
category crawl enumerates the full stat-appearing player universe + their active years, which then drives
the per-player views crawl. Everything is cached on disk (resumable) with a URL-level state manifest.

    python -m scripts.sota_recon.nflcom_harvest --selftest              # parse cached sample pages, no network
    python -m scripts.sota_recon.nflcom_harvest --layer categories --years 2000:2025
    python -m scripts.sota_recon.nflcom_harvest --layer teams --years 1970:2025
    python -m scripts.sota_recon.nflcom_harvest --layer players --views logs,career --limit 500
    python -m scripts.sota_recon.nflcom_harvest --status
"""
from __future__ import annotations

import argparse
import hashlib
import html as _html
import json
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

BASE = Path("D:/league-history-data/nfl/raw/nflcom")
CACHE = BASE / "cache"
TABLES = BASE / "tables"
STATE = BASE / "harvest_state.json"
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120.0 Safari/537.36")
POLITE_SECONDS = 1.2

# category -> default sort statistic (from the NFL.com player-stats landing dropdown; required or 404)
CATEGORIES = {
    "passing": "passingyards", "rushing": "rushingyards", "receiving": "receivingreceptions",
    "fumbles": "defensiveforcedfumble", "tackles": "defensivecombinetackles",
    "interceptions": "defensiveinterceptions", "field-goals": "kickingfgmade",
    "kickoffs": "kickofftotal", "kickoff-returns": "kickreturnsaverageyards",
    "punts": "puntingaverageyards", "punt-returns": "puntreturnsaverageyards",
}
TEAM_SIDES = {
    "offense": ["passing", "rushing", "receiving", "scoring", "downs"],
    "defense": ["passing", "rushing", "receiving", "scoring", "downs"],
    "special-teams": ["scoring", "kicking", "punting", "kickoff-returns", "punt-returns", "field-goals"],
}
SEASON_TYPES = ["reg", "post"]
YEAR_FLOOR = 1932


# ---------------------------------------------------------------------------------------------
# HTTP with disk cache + polite throttle + state manifest (resumable)
# ---------------------------------------------------------------------------------------------
def _load_state() -> dict:
    if STATE.exists():
        return json.loads(STATE.read_text())
    return {"done": {}, "empty": {}, "fetches": 0}


def _save_state(st: dict) -> None:
    STATE.parent.mkdir(parents=True, exist_ok=True)
    STATE.write_text(json.dumps(st))


def _cache_file(url: str) -> Path:
    return CACHE / (hashlib.sha1(url.encode()).hexdigest() + ".html")


def http_get(url: str, use_cache: bool = True) -> str | None:
    cf = _cache_file(url)
    if use_cache and cf.exists():
        txt = cf.read_text(encoding="utf-8", errors="replace")
        return txt if txt else None
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    for attempt in range(2):
        try:
            with urllib.request.urlopen(req, timeout=45) as r:
                html = r.read().decode("utf-8", errors="replace")
            cf.parent.mkdir(parents=True, exist_ok=True)
            cf.write_text(html, encoding="utf-8")
            time.sleep(POLITE_SECONDS)
            return html
        except urllib.error.HTTPError as e:
            time.sleep(POLITE_SECONDS)
            if e.code == 404:
                cf.parent.mkdir(parents=True, exist_ok=True)
                cf.write_text("", encoding="utf-8")  # cache the 404 as empty
                return None
            if e.code >= 500 and attempt == 0:
                time.sleep(2.0)
                continue
            return None
        except (urllib.error.URLError, TimeoutError):
            time.sleep(2.0)
            if attempt == 1:
                return None
    return None


# ---------------------------------------------------------------------------------------------
# generic server-rendered-table parser
# ---------------------------------------------------------------------------------------------
def _clean(raw: str) -> str:
    raw = re.sub(r"<svg.*?</svg>", " ", raw, flags=re.S)
    raw = re.sub(r"<[^>]+>", " ", raw)
    return _html.unescape(re.sub(r"\s+", " ", raw)).strip()


def _dedup(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out = []
    for h in headers:
        key = re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_") or "col"
        seen[key] = seen.get(key, 0) + 1
        out.append(key if seen[key] == 1 else f"{key}_{seen[key]}")
    return out


def parse_all_tables(html: str) -> list[dict]:
    """Every <table> on the page -> {caption, headers, rows}. Rows carry deduped header keys plus
    _player_slug / _team_slug captured from cell hrefs (lost when tags are stripped)."""
    if not html:
        return []
    out = []
    for m in re.finditer(r"<table.*?</table>", html, re.S):
        seg = m.group(0)
        pre = html[max(0, m.start() - 500):m.start()]
        pre = re.sub(r"<svg.*?</svg>", " ", pre, flags=re.S)
        heads = re.findall(r"<(?:h[1-6]|caption)[^>]*>(.*?)</(?:h[1-6]|caption)>", pre, re.S)
        caption = _clean(heads[-1]) if heads else ""
        thead = re.search(r"<thead.*?</thead>", seg, re.S)
        if thead:
            hdr_cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", thead.group(0), re.S)
        else:  # first tr as header
            first_tr = re.search(r"<tr.*?</tr>", seg, re.S)
            hdr_cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", first_tr.group(0), re.S) if first_tr else []
        headers = [_clean(c) for c in hdr_cells]
        # COLUMN-SHIFT FIX (O.9.0, 2026-07-26). Splits/situational tables lead with a BLANK
        # header over the split-label column ("Sundays", "Home Games", "Chicago Bears").
        # Dropping the blank NAME while its CELL survives made dict(zip(keys, cells)) pair
        # keys[0] with cells[0], storing every value one column LEFT of its true name and
        # truncating the last column away -- 7.78M rows landed mislabelled at rest before
        # this was caught. Name blank headers positionally instead of dropping them, so the
        # name/cell arrays stay aligned. Tables with no blank header parse identically.
        headers = [h if h != "" else f"unnamed_{i}" for i, h in enumerate(headers)]
        if len([h for h in headers if not h.startswith("unnamed_")]) < 2:
            continue
        keys = _dedup(headers)
        tbody = re.search(r"<tbody.*?</tbody>", seg, re.S)
        body = tbody.group(0) if tbody else seg
        rows = []
        for tr in re.findall(r"<tr.*?</tr>", body, re.S):
            tds = re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S)
            if not tds:
                continue
            cells = [_clean(c) for c in tds]
            if len([c for c in cells if c]) == 0:
                continue
            row = dict(zip(keys, cells[:len(keys)]))
            ps = re.search(r"/players/([a-z0-9\-]+)/", tr)
            ts = re.search(r"/teams/([a-z0-9\-]+)/", tr)
            if ps:
                row["_player_slug"] = ps.group(1)
            if ts:
                row["_team_slug"] = ts.group(1)
            rows.append(row)
        if rows:
            out.append({"caption": caption, "headers": headers, "keys": keys, "rows": rows})
    return out


def _next_cursor_url(html: str, category: str) -> str | None:
    """Find the pagination 'next' href (?aftercursor=...) for a category page."""
    hrefs = re.findall(rf'href="([^"]*/category/{re.escape(category)}/[^"]*aftercursor=[^"]+)"', html)
    if not hrefs:
        return None
    href = _html.unescape(hrefs[-1])
    if href.startswith("/"):
        href = "https://www.nfl.com" + href
    return href


# ---------------------------------------------------------------------------------------------
# JSON-rows -> parquet (schema = union of keys), append-safe per output file
# ---------------------------------------------------------------------------------------------
def _write_parquet(rows: list[dict], out_path: Path) -> int:
    if not rows:
        return 0
    import duckdb
    cols = sorted({k for r in rows for k in r})
    con = duckdb.connect()
    con.execute("CREATE TABLE w (j JSON)")
    con.executemany("INSERT INTO w VALUES (?)", [[json.dumps(r)] for r in rows])
    sel = ", ".join(f"json_extract_string(j, '$.\"{c}\"') AS \"{c}\"" for c in cols)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT {sel} FROM w) TO '{out_path.as_posix()}' (FORMAT PARQUET)")
    con.close()
    return len(rows)


# ---------------------------------------------------------------------------------------------
# LAYER 1: player-season bulk categories (all players/year) + slug discovery
# ---------------------------------------------------------------------------------------------
def harvest_categories(years, season_types, categories, st) -> dict:
    # RESUME FAST: if every category parquet + the player universe already exist, this layer is done --
    # skip the ~5,000-page cache re-parse so a restart jumps straight to the per-player crawl.
    uni = BASE / "player_universe.json"
    done_all = uni.exists() and all((TABLES / "player_season" / f"{c}.parquet").exists() for c in categories)
    if done_all and not st.get("force_categories"):
        n = len(json.loads(uni.read_text()))
        print(f"  [categories] already complete ({n} slugs in universe) -> skipping re-parse")
        return {"skipped": True, "universe": n}
    slugs: dict[str, list[int]] = {}
    for cat in categories:
        sort = CATEGORIES[cat]
        rows_all = []
        for year in years:
            for stype in season_types:
                url = f"https://www.nfl.com/stats/player-stats/category/{cat}/{year}/{stype}/all/{sort}/desc"
                pages = 0
                seen_cursors = set()   # NFL.com's aftercursor pagination STALLS/self-loops past ~page 2;
                seen_slugs_year = set()  # stop when the cursor repeats OR a page adds no new players.
                while url and pages < 60:
                    html = http_get(url)
                    st["fetches"] = st.get("fetches", 0) + 1
                    if not html:
                        break
                    tabs = parse_all_tables(html)
                    if not tabs:
                        break
                    new_this_page = 0
                    for r in tabs[0]["rows"]:
                        sl = r.get("_player_slug")
                        if sl and sl in seen_slugs_year:
                            continue  # duplicate from a stalled cursor -> skip
                        if sl:
                            seen_slugs_year.add(sl); new_this_page += 1
                            slugs.setdefault(sl, [])
                            if year not in slugs[sl]:
                                slugs[sl].append(year)
                        r["_category"] = cat; r["season"] = year; r["season_type"] = stype
                        rows_all.append(r)
                    pages += 1
                    if new_this_page == 0:  # stalled: page contributed no new players
                        break
                    nxt = _next_cursor_url(html, cat)
                    cur = nxt.split("aftercursor=")[-1][:20] if nxt else None
                    if not nxt or cur in seen_cursors:  # cursor self-loop -> stop
                        break
                    seen_cursors.add(cur)
                    url = nxt
        n = _write_parquet(rows_all, TABLES / "player_season" / f"{cat}.parquet")
        print(f"  [categories] {cat}: {n} rows -> {cat}.parquet")
    # persist discovered slug->years universe (drives the player-views crawl)
    su = BASE / "player_universe.json"
    prev = json.loads(su.read_text()) if su.exists() else {}
    for sl, yrs in slugs.items():
        prev[sl] = sorted(set(prev.get(sl, []) + yrs))
    su.parent.mkdir(parents=True, exist_ok=True)
    su.write_text(json.dumps(prev))
    print(f"  [categories] player universe now {len(prev)} slugs")
    return {"slugs": len(slugs), "universe": len(prev)}


# ---------------------------------------------------------------------------------------------
# LAYER 2: team + league stats
# ---------------------------------------------------------------------------------------------
def harvest_teams(years, season_types, st) -> dict:
    # RESUME FAST: skip if every team-stat parquet already exists.
    expected = sum(len(cats) for cats in TEAM_SIDES.values())
    have = len(list((TABLES / "team_stats").glob("*.parquet"))) if (TABLES / "team_stats").exists() else 0
    if have >= expected and not st.get("force_teams"):
        print(f"  [teams] already complete ({have} files) -> skipping")
        return {"skipped": True, "files": have}
    total = 0
    for side, cats in TEAM_SIDES.items():
        for cat in cats:
            rows_all = []
            for year in years:
                for stype in season_types:
                    url = f"https://www.nfl.com/stats/team-stats/{side}/{cat}/{year}/{stype}/all"
                    html = http_get(url)
                    st["fetches"] = st.get("fetches", 0) + 1
                    if not html:
                        continue
                    tabs = parse_all_tables(html)
                    if not tabs:
                        continue
                    for r in tabs[0]["rows"]:
                        r["_side"] = side
                        r["_category"] = cat
                        r["season"] = year
                        r["season_type"] = stype
                        rows_all.append(r)
            n = _write_parquet(rows_all, TABLES / "team_stats" / f"{side}_{cat}.parquet")
            total += n
            print(f"  [teams] {side}/{cat}: {n} rows")
    return {"rows": total}


# ---------------------------------------------------------------------------------------------
# LAYER 3: per-player views (game/season/career grain)
# ---------------------------------------------------------------------------------------------
SHARD_BATCH = 50  # flush a shard (per view) every N players -> progress persists across teardowns/pauses


def _crawl_one_player_view(slug: str, view: str, yrs, st) -> list[dict]:
    rows = []
    if view == "career":  # single page, per-season rows
        html = http_get(f"https://www.nfl.com/players/{slug}/stats/")
        st["fetches"] = st.get("fetches", 0) + 1
        for t in parse_all_tables(html or ""):
            for r in t["rows"]:
                r["_view"] = "career"; r["_table"] = t["caption"]; r["nflcom_slug"] = slug
                rows.append(r)
    else:  # logs / splits / situational are per-year
        for year in yrs:
            html = http_get(f"https://www.nfl.com/players/{slug}/stats/{view}/{year}/")
            st["fetches"] = st.get("fetches", 0) + 1
            for t in parse_all_tables(html or ""):
                for r in t["rows"]:
                    r["_view"] = view; r["_table"] = t["caption"]
                    r["nflcom_slug"] = slug; r["season"] = year
                    if view == "logs" and "fum" in t["keys"] and "lost" in t["keys"]:
                        r["fumbles"] = r.get("fum"); r["fumbles_lost"] = r.get("lost")
                    rows.append(r)
    return rows


def harvest_player_views(slug_years: dict, views, st, limit=None) -> dict:
    """EXHAUSTIVE, RESUMABLE: crawl every player's views. Writes SHARD parquets every SHARD_BATCH players
    (so progress persists if the process dies) and records completed (slug,view) pairs in state so a restart
    SKIPS them. Shards land in tables/player_{view}/shard_NNNN.parquet; readers glob the dir."""
    done = st.setdefault("done_player_views", {})
    for v in views:
        done.setdefault(v, [])
    done_sets = {v: set(done[v]) for v in views}
    shardn = st.setdefault("shard_counters", {})
    slugs = list(slug_years.items())
    if limit:
        slugs = slugs[:limit]
    buckets = {v: [] for v in views}
    pending = {v: [] for v in views}   # slugs whose rows are buffered but not yet flushed

    def flush(view):
        if pending[view]:
            n = shardn.get(view, 0) + 1
            shardn[view] = n
            _write_parquet(buckets[view], TABLES / f"player_{view}" / f"shard_{n:04d}.parquet")
            done[view].extend(pending[view])
            print(f"  [players] {view} shard {n:04d}: {len(buckets[view])} rows ({len(done[view])} players done)")
            buckets[view] = []; pending[view] = []
            _save_state(st)

    processed = 0
    for slug, years in slugs:
        yrs = sorted(set(years))
        any_new = False
        for view in views:
            if slug in done_sets[view]:
                continue
            any_new = True
            buckets[view].extend(_crawl_one_player_view(slug, view, yrs, st))
            pending[view].append(slug); done_sets[view].add(slug)
        if any_new:
            processed += 1
        if processed and processed % SHARD_BATCH == 0:
            for v in views:
                flush(v)
    for v in views:
        flush(v)
    return {v: len(done[v]) for v in views}


# ---------------------------------------------------------------------------------------------
def _parse_years(s: str) -> list[int]:
    if ":" in s:
        a, b = s.split(":")
        return list(range(int(a), int(b) + 1))
    return [int(x) for x in s.split(",")]


SELFTEST_PAGES = {  # cached sample files in the scratchpad (verify parser w/o network)
    "logs(RB)": "jimbrown1963.html", "career(multi)": "jb_career.html",
    "splits(15tab)": "jb_splits.html", "situational": "jb_situ.html",
    "bulk_passing": "bulk_passing.html", "team_defense": "bulk_teamdef.html",
}


def selftest(sample_dir: str):
    d = Path(sample_dir)
    for label, fn in SELFTEST_PAGES.items():
        p = d / fn
        if not p.exists():
            print(f"  [{label}] sample missing: {fn}"); continue
        tabs = parse_all_tables(p.read_text(encoding="utf-8", errors="replace"))
        nrows = sum(len(t["rows"]) for t in tabs)
        caps = [t["caption"][:22] for t in tabs][:6]
        fum = any(("fumbles_lost" in t["keys"] or "lost" in t["keys"] or "fum" in t["keys"]) for t in tabs)
        print(f"  [{label:16}] {len(tabs)} tables, {nrows} rows, fumbles_seen={fum}, captions={caps}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", choices=["categories", "teams", "players", "all"])
    ap.add_argument("--years", default="1932:2025")
    ap.add_argument("--season-types", default="reg,post")
    ap.add_argument("--categories", default=",".join(CATEGORIES))
    ap.add_argument("--views", default="logs,career,splits,situational")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--selftest-dir", default=".")
    ap.add_argument("--status", action="store_true")
    a = ap.parse_args()
    if a.selftest:
        selftest(a.selftest_dir); raise SystemExit
    if a.status:
        st = _load_state()
        print(f"fetches so far: {st.get('fetches', 0)}")
        for p in sorted(TABLES.glob("**/*.parquet")):
            print(f"  {p.relative_to(TABLES)}: {p.stat().st_size:,} bytes")
        raise SystemExit
    years = [y for y in _parse_years(a.years) if y >= YEAR_FLOOR]
    sts = a.season_types.split(",")
    st = _load_state()
    if a.layer in ("categories", "all"):
        harvest_categories(years, sts, a.categories.split(","), st); _save_state(st)
    if a.layer in ("teams", "all"):
        harvest_teams(years, sts, st); _save_state(st)
    if a.layer in ("players", "all"):
        su = BASE / "player_universe.json"
        universe = json.loads(su.read_text()) if su.exists() else {}
        harvest_player_views(universe, a.views.split(","), st, limit=a.limit); _save_state(st)
    _save_state(st)
    print(f"done. total fetches this session: {st.get('fetches', 0)}")
