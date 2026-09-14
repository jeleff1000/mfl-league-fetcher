"""
sota_recon/nflcom_column_semantics.py -- O.9.0: recover NFL.com column SEMANTICS from the retained cache.

THE PROBLEM (measured 2026-07-26). The nflcom harvester flattened every stat table with a positional
de-duplicating header cleaner (`nflcom_harvest._dedup`), so the parquets carry names like `att_2`/`yds_2`
that are PARSE ARTIFACTS, not semantics. Worse than a naming nuisance: NFL.com renders a DIFFERENT
column block per player position under the SAME table caption, so one physical column is several stats.

    tom-brady    logs 2007  caption='Regular Season'  -> ... COMP ATT YDS ...   (`yds` = PASSING yards)
    hall-haynes  logs 1950  caption='Regular Season'  -> ... INT  YDS AVG ...   (`yds` = INT RETURN yards)

The stored parquet retains `_table` (the caption) but NOT the header sequence, so for the game-log,
splits and situational families the caption does not disambiguate anything. A naive "their columns vs
ours" diff over these 395 columns would be a guess.

THE FIX IS A PARSE, NOT A CRAWL. `raw/nflcom/cache` retains 47,060 fetched pages (8.76 GB). Cache keys
are sha1(url), so every URL the harvester could have fetched is reconstructable from
`player_universe.json` + the harvester's own URL grammar -- which attributes each cached page to a
(family, slug, year) with certainty rather than sniffing page content.

PHASE 1 (this module, `census`): re-parse every attributable cached page and emit one row per distinct
(family, caption, ORDERED header signature) with page/row counts and example pages. That signature is
the semantic unit the parquets threw away.

    python -m scripts.sota_recon.nflcom_column_semantics --census
    python -m scripts.sota_recon.nflcom_column_semantics --census --limit 2000   # smoke run
    python -m scripts.sota_recon.nflcom_column_semantics --report

Resumable per [[feedback-agent-work-must-be-resumable]]: work is sharded by cache-file prefix and each
completed shard is appended to the shelf, so a teardown resumes at the next shard.
"""
from __future__ import annotations

import argparse
import hashlib
import html as _html
import json
import os
import re
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

BASE = Path("D:/league-history-data/nfl/raw/nflcom")
CACHE = BASE / "cache"
SHELF = BASE / "column_semantics_shelf.jsonl"
OUT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-column-signature-census.json"

# The harvester's URL grammar (nflcom_harvest.py) -- the source of family attribution.
PLAYER_VIEWS = ("logs", "splits", "situational")
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
YEAR_FLOOR, YEAR_CEIL = 1932, 2025


def _sha(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()


def build_url_index() -> dict[str, tuple]:
    """sha1 -> (family, key1, key2). Reconstructs every URL the harvester's grammar can emit."""
    idx: dict[str, tuple] = {}
    universe = json.loads((BASE / "player_universe.json").read_text())
    for slug, years in universe.items():
        idx[_sha(f"https://www.nfl.com/players/{slug}/stats/")] = ("player_career", slug, "")
        for y in sorted(set(years)):
            for v in PLAYER_VIEWS:
                u = f"https://www.nfl.com/players/{slug}/stats/{v}/{y}/"
                idx[_sha(u)] = (f"player_{v}", slug, str(y))
    for cat, sort in CATEGORIES.items():
        for y in range(YEAR_FLOOR, YEAR_CEIL + 1):
            for stype in ("reg", "post"):
                u = f"https://www.nfl.com/stats/player-stats/category/{cat}/{y}/{stype}/all/{sort}/desc"
                idx[_sha(u)] = ("player_season", cat, f"{y}/{stype}")
    for side, cats in TEAM_SIDES.items():
        for cat in cats:
            for y in range(YEAR_FLOOR, YEAR_CEIL + 1):
                for stype in ("reg", "post"):
                    u = f"https://www.nfl.com/stats/team-stats/{side}/{cat}/{y}/{stype}/all"
                    idx[_sha(u)] = ("team_stats", f"{side}/{cat}", f"{y}/{stype}")
    return idx


# --- parsing: mirrors nflcom_harvest.parse_all_tables exactly, but keeps the ORDERED raw headers ---
def _clean(raw: str) -> str:
    raw = re.sub(r"<svg.*?</svg>", " ", raw, flags=re.S)
    raw = re.sub(r"<[^>]+>", " ", raw)
    return _html.unescape(re.sub(r"\s+", " ", raw)).strip()


def _dedup(headers: list[str]) -> list[str]:
    """Byte-identical to nflcom_harvest._dedup -- this is what produced the stored column names."""
    seen: dict[str, int] = {}
    out = []
    for h in headers:
        key = re.sub(r"[^a-z0-9]+", "_", h.lower()).strip("_") or "col"
        seen[key] = seen.get(key, 0) + 1
        out.append(key if seen[key] == 1 else f"{key}_{seen[key]}")
    return out


def page_signatures(html: str) -> list[dict]:
    """-> [{caption, headers(ordered raw), keys(stored names), n_rows}] for every table on the page."""
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
        else:
            first_tr = re.search(r"<tr.*?</tr>", seg, re.S)
            hdr_cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", first_tr.group(0), re.S) if first_tr else []
        headers = [h for h in (_clean(c) for c in hdr_cells) if h != ""]
        if len(headers) < 2:
            continue
        tbody = re.search(r"<tbody.*?</tbody>", seg, re.S)
        body = tbody.group(0) if tbody else seg
        n_rows = sum(1 for tr in re.findall(r"<tr.*?</tr>", body, re.S)
                     if re.findall(r"<td[^>]*>(.*?)</td>", tr, re.S))
        if n_rows:
            out.append({"caption": caption, "headers": headers, "keys": _dedup(headers), "n_rows": n_rows})
    return out


_IDX: dict[str, tuple] = {}


def _init_worker(idx: dict) -> None:
    """Ship the ~355k-entry URL index to each worker ONCE. Putting it in the job tuple
    instead makes the parent materialize one copy per shard (256x) and thrash."""
    global _IDX
    _IDX = idx


def _scan_shard(job) -> list[dict]:
    """Worker: parse one shard of cache files -> aggregated signature counts."""
    shard, files = job
    idx = _IDX
    agg: dict[tuple, dict] = {}
    unattributed = 0
    for name in files:
        sha = name[:-5]
        meta = idx.get(sha)
        if meta is None:
            unattributed += 1
            continue
        family, k1, k2 = meta
        try:
            html = (CACHE / name).read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for t in page_signatures(html):
            gk = (family, t["caption"], tuple(t["headers"]))
            e = agg.setdefault(gk, {"pages": 0, "rows": 0, "example": f"{k1}|{k2}"})
            e["pages"] += 1
            e["rows"] += t["n_rows"]
    return [{"shard": shard, "unattributed": unattributed,
             "sigs": [{"family": k[0], "caption": k[1], "headers": list(k[2]),
                       "keys": _dedup(list(k[2])), **v} for k, v in agg.items()]}]


def _shelf_for(limit: int | None) -> Path:
    """A --limit run truncates mid-shard, so it must NEVER share the full run's shelf: the partial
    shard would be marked done and silently under-count the real census."""
    return SHELF if not limit else SHELF.with_suffix(f".smoke{limit}.jsonl")


def census(limit: int | None, workers: int) -> None:
    global SHELF
    SHELF = _shelf_for(limit)
    idx = build_url_index()
    print(f"[census] URL grammar reconstructs {len(idx):,} candidate URLs")
    files = sorted(os.listdir(CACHE))
    files = [f for f in files if f.endswith(".html")]
    if limit:
        files = files[:limit]
    print(f"[census] {len(files):,} cached pages on disk -> shelf {SHELF.name}")

    done = set()
    if SHELF.exists():
        for line in SHELF.read_text(encoding="utf-8").splitlines():
            if line.strip():
                done.add(json.loads(line)["shard"])
        print(f"[census] resuming: {len(done)} shards already shelved")

    shards = defaultdict(list)
    for f in files:
        shards[f[:2]].append(f)
    jobs = [(s, fs) for s, fs in sorted(shards.items()) if s not in done]
    print(f"[census] {len(jobs)} shards to scan ({workers} workers)", flush=True)
    if not jobs:
        print("[census] nothing to do")
        return
    with SHELF.open("a", encoding="utf-8") as fh, ProcessPoolExecutor(
            max_workers=workers, initializer=_init_worker, initargs=(idx,)) as ex:
        for i, res in enumerate(ex.map(_scan_shard, jobs), 1):
            for r in res:
                fh.write(json.dumps(r) + "\n")
            fh.flush()
            if i % 8 == 0 or i == len(jobs):
                print(f"  [census] {i}/{len(jobs)} shards", flush=True)


def _load_shelf() -> tuple[dict, int]:
    agg: dict[tuple, dict] = {}
    unattributed = 0
    for line in SHELF.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        unattributed += rec["unattributed"]
        for s in rec["sigs"]:
            gk = (s["family"], s["caption"], tuple(s["headers"]))
            e = agg.setdefault(gk, {"pages": 0, "rows": 0, "example": s["example"], "keys": s["keys"]})
            e["pages"] += s["pages"]
            e["rows"] += s["rows"]
    return agg, unattributed


def report() -> None:
    if not SHELF.exists():
        raise SystemExit("no shelf -- run --census first")
    agg, unattributed = _load_shelf()
    by_family: dict[str, list] = defaultdict(list)
    for (family, caption, headers), v in agg.items():
        by_family[family].append({"caption": caption, "headers": list(headers), "keys": v["keys"],
                                  "pages": v["pages"], "rows": v["rows"], "example": v["example"]})
    payload = {
        "generated": "O.9.0 nflcom column signature census",
        # NB: a page contributes once PER TABLE on it (career pages carry 7-9), so this is
        # table instances, NOT pages. Attributed pages = 47,060 on disk - unattributed.
        "table_instances_parsed": sum(x["pages"] for x in agg.values()),
        "cache_files_on_disk": 47060,
        "unattributed_cache_files": unattributed,
        "distinct_signatures": len(agg),
        "families": {},
    }
    print(f"{'family':<24} {'sigs':>6} {'captions':>9} {'rows':>14}")
    for fam, sigs in sorted(by_family.items()):
        sigs.sort(key=lambda s: -s["rows"])
        caps = len({s["caption"] for s in sigs})
        print(f"{fam:<24} {len(sigs):>6} {caps:>9} {sum(s['rows'] for s in sigs):>14,}")
        # ambiguity witness: stored key -> how many DISTINCT raw headers it stands for in this family
        amb: dict[str, Counter] = defaultdict(Counter)
        for s in sigs:
            for raw, key in zip(s["headers"], s["keys"]):
                amb[key][raw.upper()] += s["rows"]
        payload["families"][fam] = {
            "signatures": sigs,
            "ambiguous_keys": {k: dict(c) for k, c in sorted(amb.items()) if len(c) > 1},
        }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nunattributed cache files: {unattributed:,}")
    print(f"-> {OUT}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--census", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 2))
    a = ap.parse_args()
    if a.census:
        census(a.limit, a.workers)
    if a.report:
        SHELF = _shelf_for(a.limit)
        report()
    if not (a.census or a.report):
        ap.error("pass --census and/or --report")
