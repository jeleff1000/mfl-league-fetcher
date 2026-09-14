"""
sota_recon/derive_source_toc.py  --  O.8e: DERIVE the source table-of-contents from
retained bytes instead of asserting it (Joe, 2026-07-26: "you keep being lazy when the
whole point is we're trying to harvest the unknown unknowns").

THE FAILURE THIS CLOSES. Law B (capture_contracts.py) counts
`toc_entries - (captured + excluded) = 0`. That counter is only as honest as the TOC,
and the TOC was HAND-AUTHORED from general knowledge. Measured 2026-07-26 against the
one source whose own index was recoverable: the PFA list missed four real nav sections
(Coaches, Awards, Leaderboards, Seasons) and invented one that does not exist
(transactions). Wrong in BOTH directions -- while the counter read zero.

A hand-authored TOC can only ever contain known knowns. The program exists to find
what nobody listed. So the TOC must be DERIVED from evidence we already hold: ~200,000
retained raw pages across three captures, every one of which carries the site's own
link graph.

METHOD (no crawling -- every byte is already on disk):
  1. sample retained pages per source
  2. extract every internal href
  3. normalize to URL PATTERNS ({year}/{n}/{id}/{slug} placeholders)
  4. emit the observed pattern inventory with hit counts
  5. diff against the declared capture-contract TOC -> unaccounted patterns

Counter (scoreboard): `source_toc_patterns_unaccounted` -- observed URL-pattern
families that no capture-contract TOC entry claims. Nonzero means the site publishes
something our contract does not mention: an unknown unknown, made countable.

Output: docs/source-toc-derived.json

Run:  python -m scripts.sota_recon.derive_source_toc [--sample N]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import re
from collections import Counter
from pathlib import Path

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "source-toc-derived.json")

# Retained-page caches. Every entry is bytes we ALREADY hold -- this lane never fetches.
PAGE_CACHES: dict[str, dict] = {
    "profootballarchives": {
        "root": "D:/league-history-data/nfl/ff_assets/profootballarchives",
        "glob": "**/*.html.gz",
        "host_hint": "profootballarchives",
    },
    "statscrew": {
        "root": "D:/league-history-data/nfl/ff_assets/statscrew",
        "glob": "**/*.html.gz",
        "host_hint": "statscrew",
    },
    "nflcom": {
        "root": "D:/league-history-data/nfl/raw/nflcom/cache",
        "glob": "*.html",
        "host_hint": "nfl.com",
    },
}

# Pattern families a TOC entry is allowed to claim, per source. Each maps an observed
# URL-pattern regex to the capture-contract TOC entry that accounts for it. Anything
# observed and unmatched lands in `unaccounted` -- the unknown-unknown queue.
ACCOUNTED: dict[str, list[tuple[str, str]]] = {
    "profootballarchives": [
        (r"^/?\{year\}(nfl|apfa|aafc|aac)[a-z]*-?boxscores?\.html$", "boxscore index pages (census seeds)"),
        (r"^/?nflboxscores", "per-game player participation"),
        (r"^/?\{year\}nfl[a-z]{2,4}\.html$", "team season pages / season index"),
        (r"^/?\{year\}nfl\.html$", "team season pages / season index"),
        (r"^/?players/", "player season pages"),
        (r"^/?nfl\.html$", "team season pages / season index"),
        (r"^/?players\.html$", "player season pages"),
        (r"^/?teams\.html$", "team season pages / season index"),
        (r"^/?leagues\.html$", "defunct leagues (AAFC, AAFL, ...) [site nav: Leagues]"),
        (r"^/?seasons\.html$", "season index pages [site nav: Seasons]"),
        (r"^/?coaches", "coaches [site nav: Coaches]"),
        (r"^/?awards", "awards [site nav: Awards]"),
        (r"^/?leaderboards", "leaderboards [site nav: Leaderboards]"),
        (r"^/?drafts", "drafts"),
        (r"^/?boxscores\.html$", "boxscore index pages (census seeds)"),
        (r"^/?index\.html$", "site index (navigation only)"),
    ],
    "statscrew": [
        (r"^/football/roster/", "team season rosters (rows)"),
        (r"^/football/stats/p-", "player season stats (ALL families on one page)"),
        (r"^/football/l-", "league indexes /football/l-{LEAGUE}"),
        (r"^/football/stats/t-", "team season pages"),
        (r"^/football/results/t-", "schedules / game-by-game results"),
        (r"^/football/t-", "franchise history /football/t-{TEAM}"),
        (r"^/football/(teams|standings|schedule|leaders|players)", "team season pages"),
        (r"^/football/?$", "site index (navigation only)"),
        (r"^/football/search", "site search (navigation only)"),
    ],
    "nflcom": [
        (r"^/players/\{slug\}/stats/logs", "player game logs"),
        (r"^/players/\{slug\}/stats/career", "player career pages"),
        (r"^/players/\{slug\}/stats/splits", "player splits"),
        (r"^/players/\{slug\}/stats", "player career pages"),
        (r"^/players/", "player career pages"),
        (r"^/stats/.*/category/", "season stat leaderboards"),
        (r"^/stats/.*/(offense|defense|special-teams)/", "team season stats"),
        (r"^/sitemap/html/rosters/", "team season rosters"),
        (r"^/teams/", "team season stats"),
    ],
}

# Observed patterns that are navigation/chrome, not data surfaces. Excluded WITH a
# reason -- never silently dropped (this lane's whole point is the opposite).
CHROME = [
    (r"^/?(index|home)\.html?$", "site index"),
    (r"^/?$", "site root"),
    (r"^/(privacy|terms|about|contact|advertis|faq|help|login|signup)", "site chrome"),
    (r"search", "site search"),
    # StatsCrew is a MULTI-SPORT host (discovered by this lane 2026-07-26 -- it was not
    # in the hand-authored TOC at all). Non-football sports are out of the NFL domain;
    # excluded WITH this reason so the counter reflects football gaps, not lacrosse.
    (r"^/(?!football)(world|aussie|other|pro|mens|womens|minor|college|youth|intl)?"
     r"(baseball|basketball|hockey|soccer|football|lacrosse|volleyball|tennis|golf|"
     r"softball|wrestling|boxing|racing|olympic|sports)",
     "OTHER SPORT on a multi-sport host -- out of the NFL domain"),
    (r"^/(StatsCrew|about|sitemap|rss|blog|store|forum)", "site chrome"),
]


def _normalize(href: str) -> str | None:
    if href.startswith(("mailto:", "javascript:", "#", "tel:")):
        return None
    u = re.sub(r"https?://[^/]+", "", href).split("#")[0].split("?")[0]
    if not u or u.startswith("http"):
        return None
    u = re.sub(r"\d{4,}", "{year}", u)
    u = re.sub(r"/\d+", "/{n}", u)
    # Collapse ENTITY slugs only. A bare word segment ("football", "players") is a
    # SECTION and must stay literal, or whole site sections vanish into {slug} and the
    # unaccounted counter lies in BOTH directions. Caught 2026-07-26: "football" is 8
    # characters, so a /[a-z0-9-]{8,}/ rule collapsed it and hid every StatsCrew sport
    # section behind /{slug}/ -- the exact class of error this lane exists to catch,
    # committed by this lane's own normalizer.
    # Entity slug := contains a hyphen, or is implausibly long for a section word.
    u = re.sub(r"/(?=[a-z0-9]*-)[a-z0-9-]{6,}/", "/{slug}/", u)
    u = re.sub(r"/[a-z0-9]{14,}/", "/{slug}/", u)
    # case-INSENSITIVE: team codes are uppercase (t-CHI), player ids lowercase (p-smit01)
    u = re.sub(r"\b(p|t|l|in|pro)-[A-Za-z0-9-]{3,}", r"\1-{id}", u)
    return u


def _read(p: Path) -> str:
    if p.suffix == ".gz":
        with gzip.open(p, "rt", errors="replace") as fh:
            return fh.read()
    return p.read_text(errors="replace")


def _classify(source: str, pat: str) -> tuple[str, str | None]:
    for rx, reason in CHROME:
        if re.search(rx, pat, re.I):
            return "CHROME", reason
    for rx, entry in ACCOUNTED.get(source, []):
        if re.search(rx, pat, re.I):
            return "ACCOUNTED", entry
    return "UNACCOUNTED", None


def mine(source: str, sample: int = 60, seed: int = 11) -> dict:
    cfg = PAGE_CACHES[source]
    root = Path(cfg["root"])
    if not root.exists():
        return {"source": source, "retained_pages": 0, "error": "cache root missing"}
    files = list(root.glob(cfg["glob"])) if "**" not in cfg["glob"] \
        else list(root.rglob(cfg["glob"].replace("**/", "")))
    if not files:
        return {"source": source, "retained_pages": 0, "error": "no retained pages"}
    rnd = random.Random(seed)
    smp = rnd.sample(files, min(sample, len(files)))
    pats: Counter = Counter()
    for f in smp:
        html = _read(f)
        for href in re.findall(r'<a[^>]+href=["\']([^"\']+)["\']', html, re.I):
            p = _normalize(href)
            if p:
                pats[p] += 1

    accounted, chrome, unaccounted = [], [], []
    for p, n in pats.most_common():
        kind, why = _classify(source, p)
        row = {"pattern": p, "hits": n}
        if kind == "ACCOUNTED":
            row["toc_entry"] = why
            accounted.append(row)
        elif kind == "CHROME":
            row["reason"] = why
            chrome.append(row)
        else:
            unaccounted.append(row)
    return {
        "source": source,
        "retained_pages": len(files),
        "sampled_pages": len(smp),
        "distinct_patterns": len(pats),
        "counters": {
            "accounted": len(accounted),
            "chrome_excluded": len(chrome),
            "unaccounted": len(unaccounted),
        },
        "unaccounted": unaccounted[:120],
        "accounted": accounted[:60],
        "chrome_excluded": chrome[:40],
    }


def build(sample: int = 60) -> dict:
    sources = [mine(s, sample=sample) for s in PAGE_CACHES]
    total_unaccounted = sum(s.get("counters", {}).get("unaccounted", 0) for s in sources)
    return {
        "law": "Law B's TOC must be DERIVED from retained bytes, never asserted from "
               "memory. A hand-authored TOC can only contain known knowns; this lane "
               "reads the site's own link graph out of pages we already hold and "
               "counts what the contract fails to mention.",
        "method": "sample retained pages -> extract internal hrefs -> normalize to URL "
                  "patterns -> classify ACCOUNTED / CHROME(reason) / UNACCOUNTED. "
                  "NEVER fetches: every byte is already on disk.",
        "counters": {
            "sources_with_retained_pages": sum(1 for s in sources if s.get("retained_pages")),
            "source_toc_patterns_unaccounted": total_unaccounted,
        },
        "sources": sources,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=int, default=60)
    args = ap.parse_args()
    doc = build(sample=args.sample)
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    for s in doc["sources"]:
        if not s.get("retained_pages"):
            print(f"{s['source']:22s} {s.get('error')}")
            continue
        c = s["counters"]
        print(f"{s['source']:22s} pages={s['retained_pages']:>7,} "
              f"patterns={s['distinct_patterns']:>5} accounted={c['accounted']:>4} "
              f"chrome={c['chrome_excluded']:>4} UNACCOUNTED={c['unaccounted']:>5}")
        for u in s["unaccounted"][:8]:
            print(f"      !! {u['hits']:>5}  {u['pattern'][:70]}")
    print(f"\nTOTAL UNACCOUNTED PATTERNS: "
          f"{doc['counters']['source_toc_patterns_unaccounted']}")
    print(f"derived toc -> {os.path.abspath(SUMMARY_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
