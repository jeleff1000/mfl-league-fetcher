"""
sota_recon/pfa_boxscore_census.py -- MEASURE the shape of PFA boxscore tables before
writing a parser for them. Phase 0b, step 1.

WHY A CENSUS FIRST. The O.9 handoff names eleven unparsed tables per page from reading
one modern page. A parser built on that reading would be a parser built on an assumption:
the very first page opened in this session also carried a DEFENSE table (TKL/TFL/QH/PD/
FF/BL) that the list does not mention, and 1920s boxscores cannot possibly carry the same
surface as 2003 ones. Every silent-loss defect this program has paid for -- the nflcom
column shift, the StatsCrew missing <tr>, the colspan group header -- came from a parser
that assumed a shape.

So: enumerate every distinct table SIGNATURE across a stratified sample of the 151,623
retained pages, per era, and let the measured inventory drive the grammar.

Run:  python -m scripts.sota_recon.pfa_boxscore_census [--per-shard 200]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
from collections import Counter, defaultdict
from pathlib import Path

PFA_ROOT = Path("D:/league-history-data/nfl/ff_assets/profootballarchives/player_game_participation")
SUMMARY_PATH = Path(__file__).resolve().parents[2] / "docs" / "pfa-boxscore-signature-census.json"

_TABLE = re.compile(r"<table.*?</table>", re.S | re.I)
_ROW = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S | re.I)
_CELL = re.compile(r"<(th|td)([^>]*)>(.*?)</\1>", re.S | re.I)
_TITLE = re.compile(r'title="([^"]*)"', re.I)
_SCRIPT = re.compile(r"<(script|style)[^>]*>.*?</\1>", re.S | re.I)
_GAME_ID = re.compile(r"/nflboxscores\d*/([a-z0-9]+)\.html", re.I)
_YEAR = re.compile(r"(\d{4})")


def text_of(fragment: str) -> str:
    return " ".join(re.sub(r"<[^>]+>", " ", fragment).split())


def table_signatures(html: str) -> list[dict]:
    """One record per table: its header labels, their published titles, and the row mix.

    Header-driven and non-destructive: nothing is classified here, only counted.
    """
    body = _SCRIPT.sub("", html)
    out = []
    for index, table in enumerate(_TABLE.findall(body)):
        rows = _ROW.findall(table)
        header_labels: list[str] | None = None
        header_titles: list[str] = []
        th_rows = 0
        td_rows = 0
        for row in rows:
            cells = _CELL.findall(row)
            if not cells:
                continue
            kinds = {kind.lower() for kind, _, _ in cells}
            if kinds == {"th"}:
                th_rows += 1
                if header_labels is None:
                    header_labels = [text_of(body_) for _, _, body_ in cells]
                    header_titles = [
                        (_TITLE.search(attrs).group(1) if _TITLE.search(attrs) else "")
                        for _, attrs, _ in cells
                    ]
            else:
                td_rows += 1
        if header_labels is None:
            continue
        out.append(
            {
                "table_index": index,
                "header_labels": header_labels,
                "header_titles": header_titles,
                "th_rows": th_rows,
                "td_rows": td_rows,
            }
        )
    return out


def _pages(per_shard: int) -> list[Path]:
    """Stratified: the first N pages of every shard. Shards are sha-partitioned, so a
    per-shard slice spans the whole 1919-2025 range rather than one era."""
    picked: list[Path] = []
    for shard in sorted(PFA_ROOT.glob("*/shards/shard-*")):
        raw = shard / "raw"
        if not raw.is_dir():
            continue
        for count, path in enumerate(sorted(raw.glob("*.html.gz"))):
            if count >= per_shard:
                break
            picked.append(path)
    return picked


def _season_index() -> dict[str, tuple]:
    import duckdb

    parquets = [str(p).replace("\\", "/") for p in PFA_ROOT.rglob("records.parquet")]
    if not parquets:
        return {}
    listing = "['" + "','".join(parquets) + "']"
    connection = duckdb.connect()
    rows = connection.execute(
        f"SELECT DISTINCT content_sha256, season, game_id, source_url FROM read_parquet({listing})"
    ).fetchall()
    connection.close()
    return {row[0]: (row[1], row[2], row[3]) for row in rows}


def run(per_shard: int) -> dict:
    pages = _pages(per_shard)
    index = _season_index()
    by_first_label: Counter = Counter()
    signatures: dict[tuple, dict] = {}
    tables_per_page: Counter = Counter()
    titles: dict[tuple, str] = {}
    eras_for_label: dict[str, set] = defaultdict(set)
    no_context = 0

    for path in pages:
        sha = path.name.replace(".html.gz", "")
        context = index.get(sha)
        if context is None:
            no_context += 1
        season = context[0] if context else None
        era = "unknown" if season is None else f"{(int(season) // 10) * 10}s"
        with gzip.open(path, "rt", errors="replace") as handle:
            html = handle.read()
        found = table_signatures(html)
        tables_per_page[len(found)] += 1
        for table in found:
            labels = tuple(table["header_labels"])
            first = labels[0] if labels else ""
            by_first_label[first] += 1
            eras_for_label[first].add(era)
            key = (first, labels)
            entry = signatures.get(key)
            if entry is None:
                signatures[key] = {
                    "first_label": first,
                    "header_labels": list(labels),
                    "header_titles": table["header_titles"],
                    "pages": 1,
                    "table_index_positions": [table["table_index"]],
                    "eras": [era],
                }
            else:
                entry["pages"] += 1
                if table["table_index"] not in entry["table_index_positions"]:
                    entry["table_index_positions"].append(table["table_index"])
                if era not in entry["eras"]:
                    entry["eras"].append(era)
            for label, title in zip(labels, table["header_titles"]):
                if title:
                    titles[(first, label)] = title

    ordered = sorted(signatures.values(), key=lambda row: (-row["pages"], row["first_label"]))
    return {
        "law": "measure the surface before parsing it: a parser built on one page's "
               "reading is a parser built on an assumption, and every silent-loss defect "
               "this program has paid for came from exactly that",
        "counters": {
            "pages_sampled": len(pages),
            "pages_without_capture_context": no_context,
            "distinct_signatures": len(signatures),
            "distinct_first_labels": len(by_first_label),
        },
        "tables_per_page": dict(sorted(tables_per_page.items())),
        "first_label_frequency": dict(by_first_label.most_common()),
        "first_label_eras": {k: sorted(v) for k, v in sorted(eras_for_label.items())},
        "published_titles": {f"{k[0]}|{k[1]}": v for k, v in sorted(titles.items())},
        "signatures": ordered,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--per-shard", type=int, default=200)
    args = parser.parse_args()
    doc = run(args.per_shard)
    from .recon_common import utc_stamp

    doc["generated_utc"] = utc_stamp()
    SUMMARY_PATH.parent.mkdir(parents=True, exist_ok=True)
    SUMMARY_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    counters = doc["counters"]
    print(f"pages sampled      : {counters['pages_sampled']:,}")
    print(f"distinct signatures: {counters['distinct_signatures']:,}")
    print(f"distinct table names: {counters['distinct_first_labels']:,}")
    print("tables per page:", doc["tables_per_page"])
    print("\ntable name frequency:")
    for label, count in list(doc["first_label_frequency"].items())[:40]:
        print(f"  {count:>7,}  {label!r}  eras={doc['first_label_eras'][label]}")
    print(f"\ncensus -> {SUMMARY_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
