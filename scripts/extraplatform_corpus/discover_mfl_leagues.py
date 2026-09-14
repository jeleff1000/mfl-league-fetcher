"""discover_mfl_leagues.py -- expand the MFL crawl seed via MULTI-YEAR keyword search.

MFL's leagueSearch endpoint IS a registry query, and it works per HISTORICAL year:
searching 2016 surfaces leagues alive in 2016 -- including ones dead by 2024 that a
2024-only search can never see. That is exactly the pre-2017 inventory the coarsening
ladder starves for (Sleeper starts 2017; only MFL/Fleaflicker/ESPN reach further back).

The 2026-07-17 seed used 21 keywords x 2024 only, hard-capped at 250 requests (228
leagues, 91% validation hit rate -- the registry is dense and was barely tapped).

This crawler: KEYWORDS x YEARS search plus optional stratified numeric-ID probing ->
candidate season/id pairs -> one validation fetch each (league exists, is fantasy NFL,
records size/name) -> MERGED into the existing seed. MFL IDs are season-scoped, so
dedupe is by (season, league id); this preserves every historical observation. The
importer's lineage walk then follows each seed's own history links. Resumable,
budget-capped, and single-threaded at MFL's throttle.

    py -3 scripts/extraplatform_corpus/discover_mfl_leagues.py
    MFL_REQ_CAP=1500 MFL_TARGET=800 py -3 scripts/extraplatform_corpus/discover_mfl_leagues.py
    MFL_IDS_PER_BAND=100 MFL_YEARS=2016,2017,2018,2019,2020,2021,2022,2023,2024,2025 \
      py -3 scripts/extraplatform_corpus/discover_mfl_leagues.py
"""
from __future__ import annotations

import datetime
import json
import os
import random
import socket
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

OUT = Path(os.environ.get(
    "MFL_SEED_OUT",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/discovery/mfl_crawl_seed.json"))
PROG = Path(os.environ.get("MFL_DISCOVERY_PROGRESS", "D:/tmp/mfl_discovery_progress.txt"))
# Phase-1 keyword-search results held only in memory cost ~35 min to rebuild every
# restart; the DNS wedge (getaddrinfo loops forever inside a long-lived process while
# fresh processes resolve fine) forced exactly that twice. Checkpoint per completed
# year and exit for a supervisor restart instead of retrying in-process.
CKPT = OUT.with_name("mfl_phase1_ckpt.json")
NETERR_EXIT = int(os.environ.get("MFL_NETERR_EXIT", "18"))   # consecutive, ~3 min
UA = "LeagueHistoryImport/1.0 (MFL client)"

TARGET = int(os.environ.get("MFL_TARGET", "800"))          # total leagues incl. existing
REQ_CAP = int(os.environ.get("MFL_REQ_CAP", "1500"))       # this run's request budget
SLEEP = float(os.environ.get("MFL_SLEEP", "4.2"))          # ~14 req/min
HTTP_TIMEOUT = float(os.environ.get("MFL_HTTP_TIMEOUT", "30"))
YEARS = [int(y) for y in os.environ.get(
    "MFL_YEARS", "2015,2016,2017,2018,2019,2020,2022,2024").split(",")]
ID_BANDS = os.environ.get("MFL_ID_BANDS", "1-9999,10000-29999,30000-59999,60000-80000")
IDS_PER_BAND = int(os.environ.get("MFL_IDS_PER_BAND", "0"))
ID_SEED = int(os.environ.get("MFL_ID_SEED", "20260808"))

# Broad, boring vocabulary beats clever: league names are made of common words.
KEYWORDS = [
    "dynasty", "keeper", "family", "league", "football", "redraft", "superflex",
    "IDP", "fantasy", "championship", "gridiron", "touchdown", "ppr", "empire",
    "legacy", "friends", "office", "work", "college", "alumni", "brotherhood",
    "the", "old", "boys", "girls", "club", "money", "beer", "draft", "auction",
    "bowl", "cup", "classic", "premier", "elite", "degenerate", "commish",
    "north", "south", "east", "west", "town", "city", "state", "america",
    "brothers", "cousins", "crew", "squad", "gang", "guys", "dudes", "homies",
    "church", "school", "high", "reunion", "vets", "originals", "est",
]

reqs = 0
neterrs = 0
tls_context: ssl.SSLContext | None = None
allow_expired_cert = os.environ.get("MFL_ALLOW_EXPIRED_CERT", "0") == "1"


def parse_id_bands(spec: str) -> list[tuple[int, int]]:
    """Parse inclusive numeric ID bands used for controlled enumeration."""
    bands: list[tuple[int, int]] = []
    for raw in spec.split(","):
        raw = raw.strip()
        if not raw:
            continue
        parts = raw.split("-", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid MFL ID band: {raw!r}")
        lo, hi = (int(part.strip()) for part in parts)
        if lo < 0 or hi < lo:
            raise ValueError(f"Invalid MFL ID band: {raw!r}")
        bands.append((lo, hi))
    return bands


def stratified_ids(
    bands: list[tuple[int, int]], *, per_band: int, seed: int
) -> list[int]:
    """Return a deterministic, deduplicated sample from each inclusive ID band."""
    if per_band <= 0:
        return []
    rng = random.Random(seed)
    values: list[int] = []
    seen: set[int] = set()
    for lo, hi in bands:
        width = hi - lo + 1
        count = min(per_band, width)
        for value in sorted(rng.sample(range(lo, hi + 1), count)):
            if value not in seen:
                seen.add(value)
                values.append(value)
    return values


def dedupe_season_records(records: list[dict]) -> list[dict]:
    """Deduplicate using MFL's per-season namespace: (year, league id)."""
    by_key: dict[tuple[int, str], dict] = {}
    for record in records:
        key = (int(record["year"]), str(record["id"]))
        by_key[key] = record
    return [by_key[key] for key in sorted(by_key)]


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(PROG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def get(url: str):
    global reqs, neterrs, tls_context
    while True:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        reqs += 1
        try:
            with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT, context=tls_context) as r:
                body = r.read().decode("utf-8", "replace")
            neterrs = 0
            try:
                return r.status, json.loads(body)
            except Exception:
                return r.status, None
        except urllib.error.HTTPError as e:
            neterrs = 0
            if e.code == 429:
                log(f"429 at req {reqs}; sleeping 180s")
                time.sleep(180)
                reqs -= 1
                continue
            return e.code, None
        except Exception as e:
            if allow_expired_cert and "CERTIFICATE_VERIFY_FAILED" in str(e):
                # MFL's legacy historical endpoint currently presents an expired
                # certificate.  Retry once with verification disabled only when
                # the workflow explicitly opts into this public-API workaround.
                if tls_context is None:
                    tls_context = ssl._create_unverified_context()
                    log(f"expired MFL certificate; retrying with explicit insecure TLS fallback at req {reqs}")
                    reqs -= 1
                    continue
            if "timed out" in str(e).lower():
                # Historical search results can point at decommissioned
                # footballNN hosts.  A validation timeout is a dead candidate,
                # not a reason to retry the same host 18 times.
                log(f"validation timeout for {url.split('/')[2]}; skipping candidate at req {reqs}")
                neterrs = 0
                return None, None
            # A name-resolution failure is almost always a DEAD MFL SHARD:
            # api.myfantasyleague.com redirects old league-years to their
            # owning wwwNN host, and decommissioned shards drop out of DNS.
            # Retrying the same URL can never succeed -- fail it so the
            # caller skips the candidate. (The 2026-07-17 "DNS wedge" was
            # exactly this loop pinned on one dead-shard league.)
            reason = getattr(e, "reason", None)
            if isinstance(reason, socket.gaierror) or "getaddrinfo failed" in str(e):
                log(f"dead host for {url.split('/')[2]} at req {reqs}; skipping candidate")
                neterrs = 0
                return None, None
            neterrs += 1
            if neterrs >= NETERR_EXIT:
                log(f"net wedge: {neterrs} consecutive failures at req {reqs} "
                    f"({e}); exiting rc=2 for supervisor restart")
                sys.exit(2)
            log(f"neterr {e} at req {reqs}; sleep 10s")
            time.sleep(10)
            reqs -= 1
            continue


def main() -> None:
    existing = {"platform": "mfl", "generated_probes": 0, "leagues": []}
    if OUT.exists():
        existing = json.loads(OUT.read_text(encoding="utf-8"))
    leagues = {}   # (season, league id) -> record; MFL IDs are per-season
    for rec in existing.get("leagues", []):
        key = (int(rec["year"]), str(rec["id"]))
        leagues[key] = rec
    known_pairs = set(leagues)
    base_probes = int(existing.get("generated_probes", 0))
    log(f"=== MFL multi-year discovery: start with {len(leagues)} known season-leagues, "
        f"target {TARGET}, cap {REQ_CAP}, years {YEARS} ===")

    def write_out() -> None:
        recs = sorted(leagues.values(), key=lambda r: (r["year"], r["id"]))
        candidate_rows = [
            {"id": lid, "year": year, "name": name}
            for lid, year, name in sorted(candidates, key=lambda row: (row[1], row[0]))
        ]
        obj = {
            "platform": "mfl",
            "generated_probes": base_probes + reqs,
            "leagues": recs,
            "candidates": candidate_rows,
        }
        tmp = OUT.with_suffix(".tmp")
        tmp.write_text(json.dumps(obj, indent=1), encoding="utf-8")
        os.replace(tmp, OUT)

    # Phase 1: keyword x year searches. Newest-first within a year list that leads with
    # the OLD years -- pre-2017 is the scarce inventory, spend budget there first.
    candidates: list[tuple[str, int, str]] = []   # (id, year, name)
    cand_seen: set[tuple[str, int]] = set()
    years_done: set[int] = set()
    if CKPT.exists():
        ck = json.loads(CKPT.read_text(encoding="utf-8"))
        years_done = set(ck.get("years_done", []))
        candidates = [tuple(c) for c in ck.get("candidates", [])]
        cand_seen = {(lid, year) for lid, year, _ in candidates}
        log(f"phase-1 checkpoint: {len(candidates)} candidates, "
            f"years done {sorted(years_done)}")

    def write_ckpt() -> None:
        tmp = CKPT.with_suffix(".tmp")
        tmp.write_text(json.dumps(
            {"years_done": sorted(years_done), "candidates": candidates}),
            encoding="utf-8")
        os.replace(tmp, CKPT)

    for year in YEARS:
        if year in years_done:
            continue
        for kw in KEYWORDS:
            if reqs >= REQ_CAP * 0.6:   # keep >=40% of budget for validation
                break
            s, j = get(f"https://api.myfantasyleague.com/{year}/export"
                       f"?TYPE=leagueSearch&SEARCH={urllib.parse.quote(kw)}&JSON=1")
            time.sleep(SLEEP)
            if not j:
                continue
            found = j.get("leagues", {}).get("league")
            found = [found] if isinstance(found, dict) else (found or [])
            for lg in found:
                lid = str(lg.get("id") or "").strip()
                if lid and (year, lid) not in known_pairs and (lid, year) not in cand_seen:
                    cand_seen.add((lid, year))
                    candidates.append((lid, year, str(lg.get("name") or "")))
        log(f"year {year}: cumulative candidates {len(candidates)} (reqs {reqs})")
        years_done.add(year)
        write_ckpt()
        if reqs >= REQ_CAP * 0.6:
            break

    # MFL IDs are season-scoped. Keep every (year, id) candidate; the importer can
    # then follow each record's own history links instead of losing older seasons.
    candidate_pairs = {(year, lid): (year, name) for lid, year, name in candidates}
    log(f"phase 1 done: {len(candidate_pairs)} unique new season-league candidates (reqs {reqs})")

    # Phase 1b: controlled numeric enumeration. This complements registry keyword
    # search and is intentionally opt-in: a zero sample keeps the historical default
    # request budget unchanged. Each band is sampled independently so old and new ID
    # eras cannot be crowded out by the densest range.
    bands = parse_id_bands(ID_BANDS)
    enum_ids = stratified_ids(bands, per_band=IDS_PER_BAND, seed=ID_SEED)
    if enum_ids:
        for year in YEARS:
            for lid_int in enum_ids:
                lid = str(lid_int)
                if (year, lid) not in known_pairs and (lid, year) not in cand_seen:
                    cand_seen.add((lid, year))
                    candidates.append((lid, year, ""))
        candidate_pairs = {(year, lid): (year, name) for lid, year, name in candidates}
        log(f"numeric enumeration added {len(enum_ids) * len(YEARS)} season probes; "
            f"candidates now {len(candidate_pairs)}")

    # Phase 2: validate. One TYPE=league call at the found year.
    validated = 0
    for (year, lid), (_, name) in sorted(candidate_pairs.items(), key=lambda kv: kv[0]):
        if len(leagues) >= TARGET or reqs >= REQ_CAP:
            break
        if (year, lid) in leagues:   # validated on a previous run of this budget
            continue
        s, j = get(f"https://api.myfantasyleague.com/{year}/export"
                   f"?TYPE=league&L={lid}&JSON=1")
        time.sleep(SLEEP)
        lg = (j or {}).get("league") if isinstance(j, dict) else None
        if not lg or not lg.get("franchises"):
            continue
        fr = lg["franchises"].get("franchise")
        size = len(fr) if isinstance(fr, list) else (1 if fr else 0)
        if size < 4:
            continue
        leagues[(year, lid)] = {"seed": f"{year}:{lid}", "id": lid, "year": year,
                                "name": lg.get("name") or name, "size": size}
        known_pairs.add((year, lid))
        validated += 1
        if validated % 25 == 0:
            write_out()
            log(f"validated +{validated} (total {len(leagues)}, reqs {reqs})")

    write_out()
    CKPT.unlink(missing_ok=True)   # a stale checkpoint would skip years on the NEXT run
    log(f"DONE: {len(leagues)} leagues total (+{validated} new), reqs this run {reqs}")


if __name__ == "__main__":
    main()
