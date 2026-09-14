"""
sota_recon/reparse_retained_captures.py  --  O.8f PHASE 0: recover dropped columns from
bytes we ALREADY HOLD. No crawling, no rate limits, no permission needed.

WHY THIS EXISTS (findings 8 + 11 of the O.9 handoff). Our capture parsers were scoped
narrowly and threw away most of each page at parse time -- and Law C only ever
enumerated the columns that SURVIVED, so the dropped fields existed in no denominator
anywhere in the program.

  StatsCrew rosters   page publishes 10 columns; the parser kept 2 (Player, Pos.).
                      DROPPED: jersey number, BIRTH DATE, height, weight, college,
                      hometown, GP, GS.

BIRTH DATE is the one that matters most. Measured 2026-07-26: bio holds 1,777 colliding
names over 4,035 players, of which 1,117 names are NOT separable by date of birth, and
DOB coverage collapses precisely where external witnesses matter most (pre-1933 0.0%,
1933-49 36.1%, 1950-77 66.4%). The same missing field also causes the O.7
duplicate-credit doom rows, where one person splits across a GSIS-keyed row (97.5% DOB)
and a PFR-keyed row (20.6% DOB) that cannot be merged.

So this is IDENTITY-SPINE REPAIR, not a column top-up -- and it is a prerequisite for
the nflcom slug->pfr_id crosswalk, which would otherwise have to join on name alone.

GIFT FROM THE SOURCE: StatsCrew emits `sorttable_customkey` attributes carrying an ISO
birth date (1940-04-20) and height in inches -- no date parsing, no unit inference.

Output: D:/league-history-data/nfl/derived/reparsed_captures/
          statscrew_team_season_roster_full.parquet
        docs/reparse-retained-captures.json  (receipt + recovery counters)

Run:  python -m scripts.sota_recon.reparse_retained_captures [--limit N]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
from pathlib import Path

OUT_DIR = Path("D:/league-history-data/nfl/derived/reparsed_captures")
SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "reparse-retained-captures.json")

STATSCREW_ROOT = Path("D:/league-history-data/nfl/ff_assets/statscrew/team_season_roster")

# the source page's own header order (verified 2026-07-26 against a retained page)
STATSCREW_ROSTER_COLUMNS = ["#", "Player", "Pos.", "Birth Date", "Height", "Weight",
                            "College", "Hometown", "GP", "GS"]
# what the ORIGINAL parser kept -- everything else was dropped at capture time
ORIGINAL_KEPT = {"Player", "Pos."}

_CELL = re.compile(r"<td([^>]*)>(.*?)</td>", re.S | re.I)
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_TABLE = re.compile(r"<table.*?</table>", re.S | re.I)
_SORTKEY = re.compile(r'sorttable_customkey="([^"]*)"', re.I)
_PLAYER_HREF = re.compile(r'href="[^"]*?/football/stats/(p-[A-Za-z0-9]+)"', re.I)


def _text(fragment: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", fragment)).strip()


def _int(v: str):
    v = (v or "").strip()
    return int(v) if re.fullmatch(r"-?\d+", v) else None


# header label -> our field name. HEADER-DRIVEN, never positional: StatsCrew serves
# 10-column rosters on most pages and 7-column rosters (no jersey/GP/GS) on others, so a
# positional parser silently drops whole pages the moment the width changes. That is the
# same class of silent loss this whole lane exists to undo -- it is not allowed here.
HEADER_MAP = {
    "#": "jersey_number", "no.": "jersey_number", "no": "jersey_number",
    "player": "player", "pos.": "position", "pos": "position",
    "birth date": "birth_date_text", "height": "height_text", "weight": "weight_lbs",
    "college": "college", "hometown": "hometown",
    "gp": "games_played", "gs": "games_started",
}
INT_FIELDS = {"jersey_number", "weight_lbs", "games_played", "games_started"}


def parse_statscrew_roster(html: str) -> tuple[list[dict], dict]:
    """Parse the FULL roster table, driven by the page's OWN header labels.

    Returns (rows, diagnostics). Diagnostics record unmapped headers and skipped rows
    so a shape we have never seen surfaces as a COUNT instead of vanishing."""
    diag = {"tables": 0, "header_labels": [], "unmapped_headers": [],
            "data_rows_seen": 0, "rows_emitted": 0, "rows_width_mismatch": 0}
    m = _TABLE.search(html)
    if not m:
        return [], diag
    table = m.group(0)
    diag["tables"] = 1
    labels = [_text(x).lower() for x in re.findall(r"<th[^>]*>(.*?)</th>", table,
                                                   re.S | re.I)]
    diag["header_labels"] = labels
    if not labels:
        return [], diag
    fields = [HEADER_MAP.get(l) for l in labels]
    diag["unmapped_headers"] = [l for l, f in zip(labels, fields) if f is None and l]

    rows = []
    for row_html in _ROW.findall(table):
        cells = _CELL.findall(row_html)
        if not cells:
            continue
        diag["data_rows_seen"] += 1
        if len(cells) != len(fields):
            diag["rows_width_mismatch"] += 1
            continue
        rec = {"source": "statscrew", "dataset": "team_season_roster",
               "source_player_id": None, "player": None, "player_sort_name": None,
               "jersey_number": None, "position": None, "birth_date": None,
               "birth_date_text": None, "height_inches": None, "height_text": None,
               "weight_lbs": None, "college": None, "hometown": None,
               "games_played": None, "games_started": None}
        for (attrs, frag), field, label in zip(cells, fields, labels):
            val = _text(frag)
            if field is None:
                continue
            if field == "player":
                rec["player"] = val or None
                pid = _PLAYER_HREF.search(frag)
                if pid:
                    rec["source_player_id"] = pid.group(1)[2:]
                k = _SORTKEY.search(attrs)
                if k:
                    rec["player_sort_name"] = k.group(1)
            elif field == "birth_date_text":
                rec["birth_date_text"] = val or None
                k = _SORTKEY.search(attrs)   # ISO date, gifted by the source
                if k and re.fullmatch(r"\d{4}-\d{2}-\d{2}", k.group(1)):
                    rec["birth_date"] = k.group(1)
            elif field == "height_text":
                rec["height_text"] = val or None
                k = _SORTKEY.search(attrs)   # inches, gifted by the source
                if k:
                    rec["height_inches"] = _int(k.group(1))
            elif field in INT_FIELDS:
                rec[field] = _int(val)
            else:
                rec[field] = val or None
        if rec["player"]:
            rows.append(rec)
    diag["rows_emitted"] = len(rows)
    return rows, diag


def _season_team_index() -> dict[str, tuple]:
    """content_sha256 -> (season, team, source_url) from the ORIGINAL capture parquet.
    The raw page filename IS the sha256, so this rejoins page bytes to their work item
    without re-deriving season/team from page text."""
    import duckdb
    con = duckdb.connect()
    pqs = [str(p).replace("\\", "/") for p in STATSCREW_ROOT.rglob("records.parquet")]
    if not pqs:
        return {}
    q = "['" + "','".join(pqs) + "']"
    rows = con.execute(
        f"SELECT DISTINCT content_sha256, season, team, source_url "
        f"FROM read_parquet({q})").fetchall()
    con.close()
    return {r[0]: (r[1], r[2], r[3]) for r in rows}


def run(limit: int | None = None) -> dict:
    pages = sorted(STATSCREW_ROOT.rglob("*.html.gz"))
    if limit:
        pages = pages[:limit]
    idx = _season_team_index()
    out, unmatched_pages = [], 0
    unmapped: dict[str, int] = {}
    width_mismatch = 0
    empty_pages = 0
    for p in pages:
        sha = p.stem.replace(".html", "")
        ctx = idx.get(sha)
        if ctx is None:
            unmatched_pages += 1
        with gzip.open(p, "rt", errors="replace") as fh:
            html = fh.read()
        page_rows, diag = parse_statscrew_roster(html)
        for lbl in diag["unmapped_headers"]:
            unmapped[lbl] = unmapped.get(lbl, 0) + 1
        width_mismatch += diag["rows_width_mismatch"]
        if not page_rows:
            empty_pages += 1
        for r in page_rows:
            r["season"] = ctx[0] if ctx else None
            r["team"] = ctx[1] if ctx else None
            r["source_url"] = ctx[2] if ctx else None
            r["content_sha256"] = sha
            out.append(r)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    dest = OUT_DIR / "statscrew_team_season_roster_full.parquet"
    if out:
        import duckdb
        import pandas as pd
        df = pd.DataFrame(out)
        con = duckdb.connect()
        con.register("df", df)
        con.execute(f"COPY df TO '{dest.as_posix()}' (FORMAT PARQUET)")
        con.close()

    n = len(out)
    def _has(col):
        return sum(1 for r in out if r.get(col) not in (None, ""))
    recovered = {c: _has(c) for c in ("birth_date", "height_inches", "weight_lbs",
                                      "college", "hometown", "games_played",
                                      "games_started", "jersey_number")}
    return {
        "law": "recover columns dropped at CAPTURE time from bytes already on disk; "
               "Law C can only ever see the columns that survived the parser, so a "
               "field dropped here exists in no denominator anywhere in the program",
        "source_page_columns": STATSCREW_ROSTER_COLUMNS,
        "originally_kept": sorted(ORIGINAL_KEPT),
        "originally_dropped": [c for c in STATSCREW_ROSTER_COLUMNS
                               if c not in ORIGINAL_KEPT],
        "counters": {
            "pages_reparsed": len(pages),
            "pages_without_capture_context": unmatched_pages,
            "rows": n,
            "rows_with_birth_date": recovered["birth_date"],
            "birth_date_pct": round(100.0 * recovered["birth_date"] / n, 1) if n else None,
            "distinct_players": len({r["source_player_id"] for r in out
                                     if r.get("source_player_id")}),
            "distinct_players_with_dob": len({r["source_player_id"] for r in out
                                              if r.get("source_player_id")
                                              and r.get("birth_date")}),
            "pages_yielding_no_rows": empty_pages,
            "rows_skipped_width_mismatch": width_mismatch,
            "unmapped_header_labels": len(unmapped),
        },
        "unmapped_header_labels": unmapped,
        "recovered_non_null_by_column": recovered,
        "output_parquet": dest.as_posix(),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    doc = run(limit=args.limit)
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counters"]
    print(f"pages re-parsed : {c['pages_reparsed']:,}  (no context: "
          f"{c['pages_without_capture_context']})")
    print(f"rows            : {c['rows']:,}")
    print(f"BIRTH DATES     : {c['rows_with_birth_date']:,} ({c['birth_date_pct']}%)")
    print(f"distinct players: {c['distinct_players']:,}  with DOB: "
          f"{c['distinct_players_with_dob']:,}")
    print("recovered non-null by column:")
    for k, v in doc["recovered_non_null_by_column"].items():
        print(f"    {k:16s} {v:>8,}")
    print(f"parquet -> {doc['output_parquet']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
