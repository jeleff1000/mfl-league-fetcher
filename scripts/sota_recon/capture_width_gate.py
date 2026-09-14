"""Did the PARSER drop fields the page actually published?

THE HOLE THIS CLOSES, and why no existing counter can see it. Every column counter in the
program -- the physical-field snapshot, the column dossier, the exposure ledgers -- reads
STORED columns. If a parser never wrote a field, that field is absent from the snapshot,
therefore absent from the dossier, therefore absent from every denominator downstream. The
coverage number then reads CLEAN precisely because the data is missing. It is the purest
form of the recurring defect: a counter measuring itself instead of the layer beneath it.

It is not hypothetical. Measured previously: a StatsCrew player page renders 10 columns
and we kept 2; a PFA page renders 16 tables and we parsed 1. And the O.9.0b column-shift
defect -- 7,782,148 rows mislabelled at rest -- was exactly this shape: the parser dropped
blank header NAMES but not their CELLS.

WHAT THE GATE MEASURES. For each source with a declared retained-page root, it samples
pages, counts the header cells the SITE published in the widest table on the page, and
compares that to the number of stat columns we actually store. A page wider than our
schema is a DROPPED-FIELD finding.

THE SAMPLE IS DECLARED, NOT CONVENIENT. Fixed seed, fixed size, recorded in the receipt.
D: is a USB SSD at roughly 663 ms per cold file, so a full sweep is not viable and
pretending otherwise would produce a number nobody re-runs.

THE DENOMINATOR DECLARES ITS OWN HOLE. A source with no retained-page root is listed in
`sources_without_a_page_root` and counted -- never silently skipped. "0 dropped fields"
across sources we never opened is the exact failure this gate exists to prevent, so the
gate reports how much of the registry it could actually see.

Run:  python -m scripts.sota_recon.capture_width_gate [--sample N]
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RECEIPT = ROOT / "docs" / "capture-width.json"
SEED = 20260729
DEFAULT_SAMPLE = 40

# source_id -> (retained page root, why this root is the right evidence for that source).
# Declared, never discovered: pointing a width check at the wrong directory produces a
# confident number about the wrong pages.
PAGE_ROOTS: dict[str, tuple[str, str]] = {
    "nflcom_player_career": (
        "D:/league-history-data/nfl/raw/nflcom/cache",
        "the harvester's own retained cache, keyed by sha1(url); the O.9.0 layout census "
        "was rebuilt from exactly these files"),
    "nflcom_player_logs": (
        "D:/league-history-data/nfl/raw/nflcom/cache",
        "same retained cache; player_logs and player_career are served from it"),
}

_TABLE = re.compile(rb"<table\b.*?</table>", re.I | re.S)
_HEADER_ROW = re.compile(rb"<tr\b[^>]*>(.*?)</tr>", re.I | re.S)
_TH = re.compile(rb"<th\b", re.I)


_SEMANTICS = ROOT / "docs" / "nflcom-column-semantics.json"


def _widest_censused_layout(source_id: str) -> int | None:
    """Widest layout the O.9.0 census recorded for this family.

    This is the honest stored-side number: one page renders ONE layout, so it can only
    be compared against a single layout's width. Measured 2026-07-29: across all 164
    censused signatures, len(headers) == len(keys) everywhere -- the parser drops nothing
    WITHIN a layout it saw. What this gate still catches is a page WIDER than every
    layout the census knows, which is the case the census itself would be blind to.
    """
    if not _SEMANTICS.exists():
        return None
    family = source_id.replace("nflcom_", "", 1)
    families = json.loads(_SEMANTICS.read_text(encoding="utf-8")).get("families", {})
    blob = families.get("player_logs" if family == "player_logs_targeted" else family)
    if not blob:
        return None
    widths = [len(s.get("keys") or ()) for s in blob.get("signatures") or ()]
    return max(widths) if widths else None


def _read(path: str) -> bytes:
    if path.endswith(".gz"):
        with gzip.open(path, "rb") as handle:
            return handle.read()
    with open(path, "rb") as handle:
        return handle.read()


def widest_header(page: bytes) -> int:
    """Header cells in the widest table on the page, counting BLANK <th> too.

    Blank headers are counted deliberately: the column-shift defect existed because a
    blank header was dropped while its cells were kept, so a width check that skips
    blank <th> would be blind to the very defect that motivated this gate.
    """
    widest = 0
    for table in _TABLE.findall(page):
        for row in _HEADER_ROW.findall(table):
            widest = max(widest, len(_TH.findall(row)))
    return widest


def build(sample_size: int = DEFAULT_SAMPLE) -> dict:
    from .sources import registry

    import duckdb
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")

    sources = registry(include_subject=True)
    no_root = [s for s in sorted(sources) if s not in PAGE_ROOTS]
    per_source: dict[str, dict] = {}

    for source_id, (root, why) in sorted(PAGE_ROOTS.items()):
        if source_id not in sources:
            per_source[source_id] = {"error": "declared here but not a registered source"}
            continue
        if not os.path.isdir(root):
            per_source[source_id] = {"error": f"page root missing: {root}"}
            continue
        path = str(sources[source_id].path)
        scan = (f"read_parquet('{os.path.join(path, '**', '*.parquet')}', union_by_name=true)"
                .replace("\\", "/") if os.path.isdir(path)
                else "'" + path.replace("\\", "/") + "'")
        try:
            columns = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()]
        except Exception as exc:                        # noqa: BLE001
            per_source[source_id] = {"error": f"{type(exc).__name__}: {exc}"[:160]}
            continue
        # THE DENOMINATOR HAS TO BE PER-LAYOUT, NOT PER-SOURCE. The first version of this
        # gate compared a page's widest table (19 headers) against the source's whole
        # stored column list (53) -- the UNION across 15 layouts -- and reported 0
        # suspects. That is a FALSE CLEAN produced by the gate built to catch false
        # cleans: 19 < 53 can never trip, whatever the parser dropped. The right
        # comparison is the page's widest table against the WIDEST LAYOUT WE KNOW.
        stored_union = [c for c in columns if not c.startswith("_")
                        and c not in {"nflcom_slug", "source_url", "page_key"}]
        widest_known = _widest_censused_layout(source_id)
        if widest_known is None:
            per_source[source_id] = {"error": "no layout census for this family, so there "
                                              "is no per-layout width to compare against"}
            continue
        stored = list(range(widest_known))

        files = sorted(os.listdir(root))
        files = [os.path.join(root, f) for f in files if f.endswith((".html", ".html.gz", ".gz"))]
        rng = random.Random(SEED)
        chosen = files if len(files) <= sample_size else rng.sample(files, sample_size)
        widths, unreadable = [], 0
        for candidate in chosen:
            try:
                widths.append(widest_header(_read(candidate)))
            except Exception:                           # noqa: BLE001
                unreadable += 1
        widest = max(widths) if widths else 0
        per_source[source_id] = {
            "page_root": root, "why_this_root": why,
            "pages_available": len(files), "pages_sampled": len(chosen),
            "pages_unreadable": unreadable, "seed": SEED,
            "widest_published_header": widest,
            "widest_censused_layout": widest_known,
            "stored_columns_union_across_layouts": len(stored_union),
            "dropped_field_suspects": max(0, widest - widest_known),
        }

    suspects = sum(v.get("dropped_field_suspects", 0) for v in per_source.values())
    errored = [k for k, v in per_source.items() if "error" in v]
    return {
        "generated": "site-published header width vs stored column count, per source",
        "law": "a counter that reads STORED columns cannot see a field the parser never "
               "wrote; this gate reads the page instead",
        "counters": {
            "sources_checked": len(per_source) - len(errored),
            "sources_without_a_page_root": len(no_root),
            "sources_errored": len(errored),
            "dropped_field_suspects": suspects,
        },
        "sources_without_a_page_root": no_root,
        "per_source": per_source,
    }


def write(sample_size: int = DEFAULT_SAMPLE) -> dict:
    document = build(sample_size)
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", type=int, default=DEFAULT_SAMPLE)
    args = parser.parse_args()
    doc = write(args.sample)
    for name, value in doc["counters"].items():
        print(f"  {name:38s} {value:,}")
    print()
    for source, info in doc["per_source"].items():
        if "error" in info:
            print(f"  {source:32s} ERROR {info['error']}")
            continue
        print(f"  {source:32s} widest published {info['widest_published_header']:3d} | "
              f"widest known layout {info['widest_censused_layout']:3d} | suspects "
              f"{info['dropped_field_suspects']:3d}  "
              f"({info['pages_sampled']}/{info['pages_available']:,} pages, seed {info['seed']})")
    print(f"\n  {doc['counters']['sources_without_a_page_root']} registered sources have NO "
          f"declared page root -- counted, so this gate never claims coverage it lacks")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
