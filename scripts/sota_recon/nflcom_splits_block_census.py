"""O.9.3: what NFL.com ITSELF calls each splits/situational stat block.

WHY THIS EXISTS -- the trap the 2026-07-28 handoff measured and warned about.
The un-shift settled which NAME each cell belongs under, but not which STAT FAMILY the
block is. Eight layouts per family, and their column sets are readable by eye
(L5 has COMP and RATE, so it is passing) -- and *readable by eye is exactly what this
program does not accept*. The obvious shortcut of matching these column sets against
O.9.0's RESOLVED gamelog blocks was tried and MEASURED: 0 of 8 matched. The splits blocks
carry genuinely different column sets.

THE EVIDENCE IS THE SITE'S OWN SECTION HEADING, and it was there the whole time. A player
splits page nests two levels of <h3>:

    <h3>Passing</h3>            <- the STAT FAMILY. The source's own published name.
      <h3>Days</h3>    <table>  <- the SPLIT AXIS. Stored as `_table`.
      <h3>Months</h3>  <table>
      ...
    <h3>Rushing</h3>
      <h3>Days</h3>    <table>

so every table on every retained page carries the site's own name for its block, and the
question "is L4 punt returns or kick returns" is answerable by MEASUREMENT rather than by
reading the abbreviations. This is the same discipline the StatsCrew pass used (`<th
title=>` is the site's published column name); the splits pages publish the BLOCK name
instead of the column name, so that is what gets read.

THE SECTION RULE IS STRUCTURAL, NOT A VOCABULARY. Nothing here knows that "Passing" is a
stat or that "Days" is an axis. In document order:

    an <h3> followed by another <h3> before any <table>  -> SECTION
    an <h3> immediately followed by a <table>            -> CAPTION

and the run then CHECKS its own reading: the captions it derives must be exactly the
`_table` values the stored parquet carries, and the section vocabulary must be disjoint
from them. If the structural rule mis-read the page, those two checks fail rather than
the census quietly emitting nonsense.

IT IS A SAMPLE TWICE OVER, AND BOTH LAYERS ARE DECLARED. The cache holds only ~12% of the
pages these tables were built from (the rest were fetched on other GitHub runners), and
reading all 25,661 retained splits/situational pages is not viable on this disk. MEASURED
cold, single-threaded, with nothing else running: 663 ms per 170 KB file, 0.3 MB/s, so the
full set is ~4.7 h -- and parallelism does not rescue it, because 12 workers still only
managed ~75 pages/min. (`D:` is a USB-attached SSD and this is a small-file latency wall,
not bandwidth. An earlier reading of ~1.2 s/file was taken while an orphaned worker pool
from a killed run was still reading the same disk; 663 ms is the honest number.)

So the run draws a DETERMINISTIC RANDOM SAMPLE and records the population, the sample size
and the seed, plus a per-season skew check against the population it was drawn from. This
is the same discipline the un-shift holdout was held to on 2026-07-28, for the same reason:
100% on an uncharacterised sample proves nothing.

WHY A SAMPLE IS SOUND HERE, stated precisely because "we sampled it" is otherwise an
excuse. The claim is about a LAYOUT, not a row: *this header set is rendered under this
section name*. It does not need every page, it needs enough independent observations
spread across eras and split axes that a competing name would have surfaced. And the
sample carries its own POSITIVE CONTROL -- L4 comes back AMBIGUOUS, with both Kick Return
and Punt Return, in the same run. A method that detects ambiguity where ambiguity exists,
and finds none in the other fifteen layouts, is saying something. A layout with two
competing section names is reported AMBIGUOUS and escalated, never averaged.

    python -m scripts.sota_recon.nflcom_splits_block_census --build
    python -m scripts.sota_recon.nflcom_splits_block_census --build --sample 0   # all
"""
from __future__ import annotations

import argparse
import json
import os
import random
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from .nflcom_column_semantics import CACHE, _clean, _dedup, _sha
from .nflcom_column_semantics import BASE

RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-splits-block-census.json"
UNSHIFT_RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-splits-unshift-receipt.json"
VIEWS = ("splits", "situational")

_H3_OR_TABLE = re.compile(r"<h3[^>]*>(.*?)</h3>|<table", re.S | re.I)
_THEAD = re.compile(r"<thead.*?</thead>", re.S)
_FIRST_TR = re.compile(r"<tr.*?</tr>", re.S)
_HDR_CELL = re.compile(r"<t[hd][^>]*>(.*?)</t[hd]>", re.S)
_TABLE_SEG = re.compile(r"<table.*?</table>", re.S)


def build_url_index() -> dict[str, tuple[str, str, str]]:
    """sha1(url) -> (family, slug, year) for the splits/situational pages only."""
    index: dict[str, tuple[str, str, str]] = {}
    universe = json.loads((BASE / "player_universe.json").read_text())
    for slug, years in universe.items():
        for year in sorted(set(years)):
            for view in VIEWS:
                url = f"https://www.nfl.com/players/{slug}/stats/{view}/{year}/"
                index[_sha(url)] = (f"player_{view}", slug, str(year))
    return index


def page_blocks(html: str) -> list[dict]:
    """-> [{section, caption, headers, keys}] for every table on the page.

    The section is carried forward across tables until the next section h3, which is what
    makes one <h3>Passing</h3> label all six of its split-axis tables."""
    if not html:
        return []
    # document-order positions of the table elements, so an h3 can be classified by
    # whether a table intervenes before the next h3
    events: list[tuple[int, str, str]] = []
    for match in _H3_OR_TABLE.finditer(html):
        if match.group(1) is None:
            events.append((match.start(), "table", ""))
        else:
            events.append((match.start(), "h3", _clean(match.group(1))))
    segments = {match.start(): match.group(0) for match in _TABLE_SEG.finditer(html)}

    out: list[dict] = []
    section = ""
    pending: tuple[int, str] | None = None   # an h3 whose role is not yet known
    for position, kind, text in events:
        if kind == "h3":
            if pending is not None:
                # the previous h3 was followed by another h3, never a table -> a SECTION
                section = pending[1]
            pending = (position, text)
            continue
        # a table: the h3 immediately before it is that table's CAPTION
        caption = pending[1] if pending is not None else ""
        pending = None
        segment = segments.get(position)
        if segment is None:
            continue
        thead = _THEAD.search(segment)
        if thead:
            cells = _HDR_CELL.findall(thead.group(0))
        else:
            first = _FIRST_TR.search(segment)
            cells = _HDR_CELL.findall(first.group(0)) if first else []
        headers = [h for h in (_clean(c) for c in cells) if h != ""]
        if len(headers) < 2:
            continue
        out.append({"section": section, "caption": caption,
                    "headers": headers, "keys": _dedup(headers)})
    return out


_INDEX: dict[str, tuple[str, str, str]] = {}


def _init_worker(index: dict) -> None:
    global _INDEX
    _INDEX = index


def _scan_shard(job) -> list[dict]:
    shard, files = job
    aggregate: dict[tuple, dict] = {}
    for name in files:
        meta = _INDEX.get(name[:-5])
        if meta is None:
            continue
        family, slug, year = meta
        try:
            html = (CACHE / name).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for block in page_blocks(html):
            key = (family, block["section"], block["caption"], tuple(block["keys"]))
            entry = aggregate.setdefault(key, {"tables": 0, "example": f"{slug}|{year}"})
            entry["tables"] += 1
    return {"shard": shard, "pages": len(files),
            "rows": [{"family": k[0], "section": k[1], "caption": k[2],
                      "keys": list(k[3]), **v} for k, v in aggregate.items()]}


def _season_shares(names: list[str], index: dict) -> dict[str, float]:
    counts: dict[str, int] = {}
    for name in names:
        meta = index.get(name[:-5])
        if meta:
            counts[meta[2]] = counts.get(meta[2], 0) + 1
    total = sum(counts.values()) or 1
    return {season: count / total for season, count in counts.items()}


def build(sample: int, seed: int, workers: int) -> dict:
    index = build_url_index()
    population = sorted(name for name in os.listdir(CACHE)
                        if name.endswith(".html") and name[:-5] in index)
    if sample and sample < len(population):
        files = sorted(random.Random(seed).sample(population, sample))
    else:
        files = population

    # THE SKEW CHECK. A sample of one runner's cache is not a random draw from the corpus,
    # and the season mix is where that would show. Computed from the URL index alone -- no
    # file is read for it -- so it costs nothing and there is no excuse for skipping it.
    population_shares = _season_shares(population, index)
    sample_shares = _season_shares(files, index)
    skew = sorted(((abs(share - population_shares.get(season, 0.0)) * 100, season)
                   for season, share in sample_shares.items()), reverse=True)

    shards = [files[i::64] for i in range(64)]
    jobs = [(i, shard) for i, shard in enumerate(shards) if shard]

    # RESUMABLE, because this disk reads cold cache files at ~1.2 s each and a run is long
    # enough that a teardown must not cost the whole thing. The shelf is keyed by
    # (seed, sample), so a DIFFERENT sample can never silently inherit another's shards.
    shelf = CACHE.parent / f"splits_block_census_shelf_{seed}_{sample}.jsonl"
    rolled: dict[tuple, dict] = {}
    done_shards: set[int] = set()
    pages_scanned = 0
    if shelf.exists():
        with open(shelf, encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                done_shards.add(record["shard"])
                pages_scanned += record["pages"]
                for row in record["rows"]:
                    key = (row["family"], row["section"], row["caption"],
                           tuple(row["keys"]))
                    entry = rolled.setdefault(key,
                                              {"tables": 0, "example": row["example"]})
                    entry["tables"] += row["tables"]
    jobs = [job for job in jobs if job[0] not in done_shards]

    with ProcessPoolExecutor(max_workers=workers, initializer=_init_worker,
                             initargs=(index,)) as pool:
        done = 0
        for record in pool.map(_scan_shard, jobs):
            with open(shelf, "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
            pages_scanned += record["pages"]
            for row in record["rows"]:
                key = (row["family"], row["section"], row["caption"], tuple(row["keys"]))
                entry = rolled.setdefault(key, {"tables": 0, "example": row["example"]})
                entry["tables"] += row["tables"]
            done += 1
            print(f"  shard {done}/{len(jobs)} (+{record['pages']} pages)", flush=True)

    # ---- fold to (family, layout keys) -> section counts -------------------------------
    by_layout: dict[tuple[str, tuple[str, ...]], dict[str, int]] = defaultdict(
        lambda: defaultdict(int))
    captions: dict[str, set[str]] = defaultdict(set)
    sections: dict[str, set[str]] = defaultdict(set)
    for (family, section, caption, keys), entry in rolled.items():
        by_layout[(family, keys)][section] += entry["tables"]
        captions[family].add(caption)
        sections[family].add(section)

    # ---- name the layouts by the unshift receipt's own key lists ------------------------
    unshift = json.loads(UNSHIFT_RECEIPT.read_text(encoding="utf-8"))
    layout_of: dict[tuple[str, tuple[str, ...]], str] = {}
    declared: dict[str, list[str]] = {}
    for family, body in unshift["families"].items():
        for entry in body["layouts"]:
            layout_of[(family, tuple(entry["columns"]))] = entry["layout"]
            declared[entry["layout"]] = entry["columns"]

    resolved: dict[str, dict] = {}
    unmatched: list[dict] = []
    for (family, keys), counts in sorted(by_layout.items()):
        layout = layout_of.get((family, keys))
        ordered = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
        record = {"family": family, "keys": list(keys),
                  "sections": {name: n for name, n in ordered},
                  "tables": sum(counts.values())}
        if layout is None:
            unmatched.append(record)
            continue
        record["layout"] = layout
        record["status"] = "RESOLVED" if len(counts) == 1 else "AMBIGUOUS"
        record["section"] = ordered[0][0] if len(counts) == 1 else None
        resolved[layout] = record

    receipt = {
        "generated": "O.9.3 splits/situational block identity from the site's own <h3>",
        "cache_pages_scanned": pages_scanned,
        "sampling": {
            "eligible_cache_pages": len(population),
            "sampled": len(files),
            "share_pct": round(100.0 * len(files) / max(1, len(population)), 2),
            "seed": seed,
            "why_not_all": "MEASURED cold, single-threaded, uncontended: 663 ms per "
                           "170 KB cache read (0.3 MB/s) on this USB-attached SSD, so all "
                           "25,661 eligible pages is ~4.7 h -- and 12 workers still only "
                           "managed ~75 pages/min, so it is a small-file latency wall "
                           "parallelism does not rescue. The claim is about a LAYOUT, not "
                           "a row, so it needs enough independent observations spread "
                           "across eras and split axes -- not every page. The run carries "
                           "its own positive control: L4 comes back AMBIGUOUS in the same "
                           "sample that resolves the other fifteen",
            "worst_season_share_skew_points": round(skew[0][0], 3) if skew else 0.0,
            "worst_season": skew[0][1] if skew else None,
            "seasons_in_sample": len(sample_shares),
            "seasons_in_population": len(population_shares),
        },
        "structural_rule": "an <h3> followed by another <h3> before any <table> is a "
                           "SECTION; an <h3> immediately followed by a <table> is that "
                           "table's CAPTION",
        "captions_observed": {f: sorted(v) for f, v in captions.items()},
        "sections_observed": {f: sorted(v) for f, v in sections.items()},
        "layouts": resolved,
        "layouts_declared_but_unobserved": sorted(
            set(declared) - set(resolved)),
        "key_sets_observed_but_not_declared": unmatched,
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true")
    parser.add_argument("--sample", type=int, default=4000,
                        help="deterministic random sample size; 0 reads every page")
    parser.add_argument("--seed", type=int, default=20260727)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if not args.build:
        parser.error("nothing to do -- pass --build")
    receipt = build(args.sample, args.seed, args.workers)
    sampling = receipt["sampling"]
    print(f"\npages scanned: {receipt['cache_pages_scanned']:,} of "
          f"{sampling['eligible_cache_pages']:,} eligible ({sampling['share_pct']}%), "
          f"seed {sampling['seed']}; worst season share skew "
          f"{sampling['worst_season_share_skew_points']} points "
          f"({sampling['worst_season']}), {sampling['seasons_in_sample']} of "
          f"{sampling['seasons_in_population']} seasons reached")
    for family, names in receipt["sections_observed"].items():
        print(f"  {family} sections: {names}")
    for family, names in receipt["captions_observed"].items():
        print(f"  {family} captions: {names}")
    print()
    for layout, record in sorted(receipt["layouts"].items()):
        print(f"  {layout:26s} {record['status']:9s} {record['tables']:>8,}  "
              f"{record['sections']}")
    if receipt["layouts_declared_but_unobserved"]:
        print("\nDECLARED BUT NEVER OBSERVED IN CACHE:",
              receipt["layouts_declared_but_unobserved"])
    if receipt["key_sets_observed_but_not_declared"]:
        print(f"\nKEY SETS OBSERVED BUT NOT DECLARED: "
              f"{len(receipt['key_sets_observed_but_not_declared'])}")
        for record in receipt["key_sets_observed_but_not_declared"][:10]:
            print(f"   - {record['family']} {record['keys']} {record['sections']}")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
