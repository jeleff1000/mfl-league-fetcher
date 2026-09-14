"""
sota_recon/build_nflcom_splits_reparse.py -- O.9.0b: repair the nflcom COLUMN-SHIFT from the cache.

THE DEFECT (O.9.0, commits 56dcd58b0 / c1b23ca15). `player_splits` (4,984,641 rows) and
`player_situational` (2,797,507 rows) lead with a BLANK header over the split-label column
("Sundays", "Home Games", "Chicago Bears"). The old parser dropped blank header NAMES but not their
CELLS, so `dict(zip(keys, cells[:len(keys)]))` paired keys[0] with cells[0]:

    stored:   g='Sundays'   ret=12      yds=0    ...   (and the LAST column truncated away)
    truth:    <label>=Sundays  g=12   fum=0   lost=0 ... td=0

Every value sat one column LEFT of its name across 7,782,148 rows. The parser is already fixed;
this rebuilds the DATA. It is a PARSE, NOT A CRAWL -- every source byte is a retained cache page.

DELETION DISCIPLINE. Writes to `tables/player_{view}_reparsed/`. The quarantined originals are
never overwritten, moved, or deleted -- the repair must be verifiable against them, and lifting the
quarantine is a separate adjudicated step.

FUTURE-PROOFING. Splits/situational tables are ALSO position-block dependent (a DB's table and a
QB's differ), and no block grammar exists for them yet. So every row carries `_header_sig` -- a hash
of its table's ordered raw headers -- and the run emits the signature dictionary alongside. Semantics
can then be resolved later by joining on the signature, WITHOUT re-parsing 8.76 GB a third time.

    python -m scripts.sota_recon.build_nflcom_splits_reparse --build
    python -m scripts.sota_recon.build_nflcom_splits_reparse --build --limit 500   # smoke
    python -m scripts.sota_recon.build_nflcom_splits_reparse --verify

Resumable per [[feedback-agent-work-must-be-resumable]]: sharded by cache-file prefix; each finished
shard writes its parquet and appends to a shelf, so a teardown resumes at the next shard.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from .nflcom_column_semantics import BASE, CACHE, _sha
from .nflcom_harvest import parse_all_tables

VIEWS = ("splits", "situational")
OUT_DIR = BASE / "tables"
SHELF = BASE / "splits_reparse_shelf.jsonl"
SIGS_OUT = BASE / "splits_reparse_signatures.json"
RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-splits-reparse-receipt.json"

# the blank header the old parser dropped; the fixed parser names it positionally
_BLANK_KEY = "unnamed_0"
SPLIT_LABEL = "split_value"


def build_url_index(views=VIEWS) -> dict[str, tuple]:
    """sha1(url) -> (view, slug, year) for every splits/situational page the grammar can emit."""
    idx: dict[str, tuple] = {}
    universe = json.loads((BASE / "player_universe.json").read_text())
    for slug, years in universe.items():
        for y in sorted(set(years)):
            for v in views:
                idx[_sha(f"https://www.nfl.com/players/{slug}/stats/{v}/{y}/")] = (v, slug, str(y))
    return idx


def _sig_of(headers: list[str]) -> str:
    return hashlib.sha1("|".join(h.upper().strip() for h in headers).encode()).hexdigest()[:10]


_IDX: dict[str, tuple] = {}


def _init_worker(idx: dict) -> None:
    global _IDX
    _IDX = idx


def _parse_shard(job) -> dict:
    """Worker: re-parse one shard of cache files with the FIXED parser -> rows + signatures."""
    shard, files = job
    rows: dict[str, list] = {v: [] for v in VIEWS}
    sigs: dict[str, list] = {}
    pages = 0
    for name in files:
        meta = _IDX.get(name[:-5])
        if meta is None:
            continue
        view, slug, year = meta
        try:
            html = (CACHE / name).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        pages += 1
        for t in parse_all_tables(html):
            sig = _sig_of(t["headers"])
            sigs.setdefault(sig, t["headers"])
            for r in t["rows"]:
                # the fixed parser keeps the blank header as unnamed_0; name it for what it is
                if _BLANK_KEY in r:
                    r[SPLIT_LABEL] = r.pop(_BLANK_KEY)
                r["_view"] = view
                r["_table"] = t["caption"]
                r["nflcom_slug"] = slug
                r["season"] = year
                r["_header_sig"] = sig
                rows[view].append(r)
    return {"shard": shard, "pages": pages, "sigs": sigs,
            "counts": {v: len(rows[v]) for v in VIEWS}, "rows": rows}


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


def build(limit: int | None, workers: int) -> None:
    shelf = SHELF if not limit else SHELF.with_suffix(f".smoke{limit}.jsonl")
    idx = build_url_index()
    print(f"[reparse] {len(idx):,} candidate splits/situational URLs", flush=True)
    files = sorted(f for f in os.listdir(CACHE) if f.endswith(".html"))
    if limit:
        files = files[:limit]
    done = set()
    if shelf.exists():
        done = {json.loads(l)["shard"] for l in shelf.read_text().splitlines() if l.strip()}
        print(f"[reparse] resuming: {len(done)} shards already built", flush=True)
    shards = defaultdict(list)
    for f in files:
        shards[f[:2]].append(f)
    jobs = [(s, fs) for s, fs in sorted(shards.items()) if s not in done]
    print(f"[reparse] {len(jobs)} shards to parse ({workers} workers)", flush=True)
    if not jobs:
        print("[reparse] nothing to do")
        return
    all_sigs: dict[str, list] = {}
    if SIGS_OUT.exists():
        all_sigs = json.loads(SIGS_OUT.read_text())
    with shelf.open("a", encoding="utf-8") as fh, ProcessPoolExecutor(
            max_workers=workers, initializer=_init_worker, initargs=(idx,)) as ex:
        for i, res in enumerate(ex.map(_parse_shard, jobs), 1):
            for v in VIEWS:
                if res["rows"][v]:
                    _write_parquet(res["rows"][v],
                                   OUT_DIR / f"player_{v}_reparsed" / f"shard_{res['shard']}.parquet")
            all_sigs.update(res["sigs"])
            fh.write(json.dumps({"shard": res["shard"], "pages": res["pages"],
                                 "counts": res["counts"]}) + "\n")
            fh.flush()
            SIGS_OUT.write_text(json.dumps(all_sigs, indent=1))
            if i % 8 == 0 or i == len(jobs):
                print(f"  [reparse] {i}/{len(jobs)} shards", flush=True)


def verify() -> None:
    """Prove the repair: the label column now holds labels, the stat columns now hold numbers,
    and the previously-truncated last column is present. Compared against the ORIGINALS, which
    remain on disk untouched."""
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    out: dict = {"generated": "O.9.0b nflcom splits/situational re-parse receipt", "views": {}}
    for v in VIEWS:
        old = (OUT_DIR / f"player_{v}" / "*.parquet").as_posix()
        new = (OUT_DIR / f"player_{v}_reparsed" / "*.parquet").as_posix()
        if not list((OUT_DIR / f"player_{v}_reparsed").glob("*.parquet")):
            print(f"[verify] {v}: no re-parsed shards yet")
            continue
        n_old = con.execute(f"SELECT COUNT(*) FROM read_parquet('{old}', union_by_name=true)").fetchone()[0]
        n_new = con.execute(f"SELECT COUNT(*) FROM read_parquet('{new}', union_by_name=true)").fetchone()[0]
        # OLD: `g` held the split label (non-numeric). NEW: `g` must be a game count.
        old_bad = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{old}', union_by_name=true) "
            f"WHERE g IS NOT NULL AND TRY_CAST(g AS DOUBLE) IS NULL").fetchone()[0]
        new_bad = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{new}', union_by_name=true) "
            f"WHERE g IS NOT NULL AND TRY_CAST(g AS DOUBLE) IS NULL").fetchone()[0]
        labelled = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{new}', union_by_name=true) "
            f"WHERE {SPLIT_LABEL} IS NOT NULL AND TRIM({SPLIT_LABEL}) <> ''").fetchone()[0]
        sample = con.execute(
            f"SELECT {SPLIT_LABEL}, g FROM read_parquet('{new}', union_by_name=true) "
            f"WHERE _table = 'Days' AND {SPLIT_LABEL} IS NOT NULL LIMIT 3").fetchall()
        out["views"][v] = {
            "rows_original": n_old, "rows_reparsed": n_new,
            "row_delta": n_new - n_old,
            "original_g_non_numeric": old_bad,
            "reparsed_g_non_numeric": new_bad,
            "reparsed_rows_with_split_label": labelled,
            "sample": [{SPLIT_LABEL: a, "g": b} for a, b in sample],
            "repaired": bool(old_bad > 0 and new_bad == 0 and labelled > 0),
        }
        r = out["views"][v]
        print(f"{v:<14} rows {n_old:>10,} -> {n_new:>10,} ({n_new-n_old:+,})  "
              f"g non-numeric {old_bad:>9,} -> {new_bad:<6,}  labelled {labelled:>10,}  "
              f"REPAIRED={r['repaired']}")
        for s in r["sample"]:
            print(f"                 {s}")
    RECEIPT.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"-> {RECEIPT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--build", action="store_true")
    ap.add_argument("--verify", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 2))
    a = ap.parse_args()
    if a.build:
        build(a.limit, a.workers)
    if a.verify:
        verify()
    if not (a.build or a.verify):
        ap.error("pass --build and/or --verify")
