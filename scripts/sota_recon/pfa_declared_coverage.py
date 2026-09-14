"""
sota_recon/pfa_declared_coverage.py -- turn PFA's OWN coverage declaration into something
a gate can read.

WHY THIS EXISTS. The re-parse lane recovers RUSHING / PASSING / RECEIVING / INTERCEPTIONS
/ SACKS tables from boxscore pages that go back to 1920, and it would be easy -- and
wrong -- to read that as "PFA gives us rushing gamelogs from 1920". The site publishes a
per-statistic coverage table saying otherwise, and it is blunt:

    Touchdowns (all categories)      1920-2024
    Rushing / Passing / Receiving    1960-2024   <- volume stats start in 1960
    Rushing Yards                    1960-2021   <- and several STOP in 2021
    Long Gain (most categories)      1981-2024
    Sacks                            1981-2024
    Receiving Targets, Kickoffs      1999-2024

So a 1935 rushing row on a PFA page is a REAL observation of a touchdown scorer, not a
complete rushing line, and the absence of a 1935 rushing-yards row is NOT evidence the
player did not rush. That distinction is the difference between a witness and a fabricated
zero, and [[feedback-sota-absolute-truth-no-laziness]] is explicit that a gap defaults to
INGESTION, never to a fact.

THE RULE THIS ARTIFACT ENFORCES: PFA POSITIVE claims are admissible everywhere -- a row on
the page is a row on the page. PFA ABSENCE claims are admissible ONLY inside the declared
range for that statistic. Everything outside is `present_not_complete`.

Source bytes: nfl/ff_assets/profootballarchives/site_metadata/nflgamelogcoverage.html.gz
(captured 2026-07-27, "Last Updated: July 1, 2025" -- so 2025 is undeclared by the site
itself, which is also recorded here rather than assumed).

Run:  python -m scripts.sota_recon.pfa_declared_coverage
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path

SOURCE_PAGE = Path(
    "D:/league-history-data/nfl/ff_assets/profootballarchives/site_metadata/nflgamelogcoverage.html.gz"
)
OUT_PATH = Path(__file__).resolve().parents[2] / "docs" / "pfa-declared-coverage.json"

_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
_TABLE = re.compile(r"<table.*?</table>", re.S | re.I)
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELL = re.compile(r"<(th|td)[^>]*>(.*?)</\1>", re.S | re.I)
_UPDATED = re.compile(r"Last Updated:\s*([A-Za-z]+ \d{1,2}, \d{4})", re.I)

# category -> the re-parse lane's table tag, so the declaration can be joined to rows.
CATEGORY_TO_TABLE = {
    "SCORING": None,                      # spread across every table
    "RUSHING": "rushing",
    "PASSING": "passing",
    "RECEIVING": "receiving",
    "INTERCEPTIONS": "interceptions",
    "PUNTING": "punting",
    "PUNT RETURNS": "punt_returns",
    "KICKOFFS": "kickoffs",
    "KICKOFF RETURNS": "kickoff_returns",
    "SACKS": "sacks",
}


def text_of(fragment: str) -> str:
    return " ".join(re.sub(r"<[^>]+>", " ", fragment).split())


def parse_year_spec(value: str) -> tuple[list[list[int]], int | None, int | None]:
    """`1962, 1964-1966, 1968-1969, 1996-2024` -> ranges, first, last.

    Kept as RANGES, not a first/last pair: two-point conversion attempts are declared for
    1962 and 1964-1966 but NOT 1963, and flattening that to 1962-2024 would invent
    coverage for years the source explicitly omits.
    """
    ranges: list[list[int]] = []
    for chunk in value.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        match = re.fullmatch(r"(\d{4})\s*-\s*(\d{4})", chunk)
        if match:
            ranges.append([int(match.group(1)), int(match.group(2))])
            continue
        if re.fullmatch(r"\d{4}", chunk):
            ranges.append([int(chunk), int(chunk)])
    if not ranges:
        return [], None, None
    return ranges, min(r[0] for r in ranges), max(r[1] for r in ranges)


def run() -> dict:
    html = gzip.open(SOURCE_PAGE, "rt", errors="replace").read()
    body = _SCRIPT.sub("", html)
    updated = _UPDATED.search(text_of(body))
    entries: list[dict] = []
    category = None
    undeclared: list[str] = []
    for table in _TABLE.findall(body):
        for row in _ROW.findall(table):
            cells = [text_of(cell) for _, cell in _CELL.findall(row)]
            if len(cells) < 3:
                continue
            if cells[0].upper() == "CATEGORY":
                continue
            if cells[0]:
                category = cells[0]
            statistic, years = cells[1], cells[2]
            if not statistic or category is None:
                continue
            ranges, first, last = parse_year_spec(years)
            if not ranges:
                # The site leaves KICKOFF RETURNS / Long Return blank. An undeclared cell
                # is recorded as undeclared; it is not the same as "no coverage", and it
                # is certainly not "all years".
                undeclared.append(f"{category}|{statistic}")
            entries.append(
                {
                    "category": category,
                    "statistic": statistic,
                    "table_tag": CATEGORY_TO_TABLE.get(category.upper()),
                    "declared_text": years or None,
                    "declared_ranges": ranges,
                    "first_year": first,
                    "last_year": last,
                    "absence_admissible": bool(ranges),
                }
            )

    by_first = {}
    for entry in entries:
        if entry["first_year"]:
            by_first.setdefault(entry["first_year"], []).append(
                f"{entry['category']}/{entry['statistic']}"
            )
    return {
        "law": "PFA POSITIVE claims are admissible everywhere; PFA ABSENCE claims are "
               "admissible ONLY inside the declared range for that statistic. A boxscore "
               "row outside the range is a real observation, not a complete line, and its "
               "absence is not evidence of zero",
        "source_page": SOURCE_PAGE.as_posix(),
        "source_last_updated": updated.group(1) if updated else None,
        "counters": {
            "statistics_declared": len(entries),
            "statistics_without_a_declared_range": len(undeclared),
            "earliest_declared_year": min(
                (e["first_year"] for e in entries if e["first_year"]), default=None
            ),
            "latest_declared_year": max(
                (e["last_year"] for e in entries if e["last_year"]), default=None
            ),
        },
        "undeclared_cells": undeclared,
        "statistics_by_first_declared_year": {
            str(year): sorted(names) for year, names in sorted(by_first.items())
        },
        "statistics": entries,
    }


def main() -> int:
    doc = run()
    from .recon_common import utc_stamp

    doc["generated_utc"] = utc_stamp()
    OUT_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    counters = doc["counters"]
    print(f"statistics declared      : {counters['statistics_declared']}")
    print(f"without a declared range : {counters['statistics_without_a_declared_range']} "
          f"{doc['undeclared_cells']}")
    print(f"declared span            : {counters['earliest_declared_year']}-"
          f"{counters['latest_declared_year']}  (site last updated "
          f"{doc['source_last_updated']})")
    print("\nfirst declared year -> statistics:")
    for year, names in doc["statistics_by_first_declared_year"].items():
        print(f"  {year}: {len(names):>2}  {', '.join(names[:4])}"
              f"{' ...' if len(names) > 4 else ''}")
    stops_early = [
        f"{e['category']}/{e['statistic']} ({e['last_year']})"
        for e in doc["statistics"]
        if e["last_year"] and e["last_year"] < counters["latest_declared_year"]
    ]
    print(f"\nstatistics whose coverage STOPS before {counters['latest_declared_year']}: "
          f"{len(stops_early)}")
    for item in stops_early:
        print(f"    {item}")
    print(f"\nartifact -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
