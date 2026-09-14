"""discover_fleaflicker_leagues.py -- expand the Fleaflicker crawl seed by ID probing.

Fleaflicker has no search registry, but league ids are dense small integers, so banded
random probing works (measured 2026-07-17: 7.5% hit rate, 189/275 finds pre-2017). The
prior run stopped at 275 leagues / 3,681 probes when the session ended (target was 600).
This is the same crawler, made RESUMABLE (existing seed preloaded; fresh RNG so re-runs
explore new ids) with the probe budget raised. Probe bands stay weighted to the
60k-140k pre-2017 prize band -- old leagues are the whole point of this platform.

Per candidate: FetchLeagueStandings (exists? NFL? size) -> epoch-witnessed season list
(FetchLeagueScoreboard per year; the season PARAM silently falls back, so each year is
verified by its own schedule epoch -- 2026-07-17 trap). 403s are a soft throttle: sleep
60s and retry, never burn budget on them.

    py -3 scripts/extraplatform_corpus/discover_fleaflicker_leagues.py
    FF_TARGET=800 FF_PROBE_CAP=15000 py -3 scripts/extraplatform_corpus/discover_fleaflicker_leagues.py
"""
from __future__ import annotations

import datetime
import json
import os
import random
import time
import urllib.error
import urllib.request
from pathlib import Path

OUT = Path(os.environ.get(
    "FF_SEED_OUT",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/discovery/fleaflicker_crawl_seed.json"))
PROG = Path("D:/tmp/ff_discovery_progress.txt")
UA = "LeagueHistoryImport/1.0 (Fleaflicker client)"

TARGET = int(os.environ.get("FF_TARGET", "700"))
PROBE_CAP = int(os.environ.get("FF_PROBE_CAP", "12000"))
PACE = float(os.environ.get("FF_PACE", "0.85"))
USER_GRAPH_CAP = int(os.environ.get("FF_USER_GRAPH_CAP", "1000"))

random.seed()  # fresh entropy: a resumed run must explore NEW ids, not replay the old walk

probes = 0
leagues: dict[int, dict] = {}
seen_ids: set[int] = set()
owner_ids_seen: set[str] = set()
owner_graph_probes = 0


def log(msg: str) -> None:
    line = f"[{datetime.datetime.now().strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(PROG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def yr(ms):
    try:
        return datetime.datetime.fromtimestamp(int(ms) / 1000, datetime.timezone.utc).year
    except Exception:
        return None


def get(url: str):
    global probes
    while True:
        req = urllib.request.Request(url, headers={"User-Agent": UA})
        probes += 1
        try:
            with urllib.request.urlopen(req, timeout=25) as r:
                body = r.read().decode("utf-8", "replace")
            try:
                return r.status, json.loads(body)
            except Exception:
                return r.status, None
        except urllib.error.HTTPError as e:
            if e.code == 403:
                log(f"403 soft-block at probe {probes}; sleeping 60s")
                time.sleep(60)
                probes -= 1
                continue
            if e.code == 404:
                return 404, None
            return e.code, None
        except Exception as e:
            log(f"neterr {e} at probe {probes}; sleep 5s")
            time.sleep(5)
            probes -= 1
            continue


def write_out(base_probes: int) -> None:
    recs = sorted(leagues.values(), key=lambda r: r["league_id"])
    obj = {"platform": "fleaflicker", "generated_probes": base_probes + probes, "leagues": recs}
    tmp = OUT.with_suffix(".tmp")
    tmp.write_text(json.dumps(obj, indent=1), encoding="utf-8")
    os.replace(tmp, OUT)


def candidate_stream():
    while True:
        r = random.random()
        if r < 0.45:
            lid = random.randint(60000, 140000)    # pre-2017 prize band
        elif r < 0.65:
            lid = random.randint(140000, 260000)
        else:
            lid = random.randint(20000, 357000)
        if lid in seen_ids:
            continue
        seen_ids.add(lid)
        yield lid


def witness_seasons(lid: int) -> list[int]:
    s, j = get(f"https://www.fleaflicker.com/api/FetchLeagueScoreboard?sport=NFL&league_id={lid}")
    time.sleep(PACE)
    anchor = None
    if j:
        anchor = yr(j.get("schedulePeriod", {}).get("low", {}).get("startEpochMilli"))
    top = anchor if (anchor and anchor <= 2025) else 2025
    seasons, found, checked = [], False, 0
    for y in range(top, 2010, -1):
        s, j = get(f"https://www.fleaflicker.com/api/FetchLeagueScoreboard"
                   f"?sport=NFL&league_id={lid}&season={y}&scoring_period=1")
        time.sleep(PACE)
        checked += 1
        ok = False
        if j:
            ms = j.get("schedulePeriod", {}).get("low", {}).get("startEpochMilli")
            if ms and yr(ms) == y and j.get("games", []):
                ok = True
        if ok:
            seasons.append(y)
            found = True
        elif found:
            break
        if checked >= 18:
            break
    return sorted(seasons)


def owner_ids_from_standings(payload: dict) -> set[str]:
    """Extract stable owner ids without treating team ids as user ids."""
    result: set[str] = set()
    for division in (payload or {}).get("divisions") or []:
        for team in (division or {}).get("teams") or []:
            if not isinstance(team, dict):
                continue
            candidates = []
            for key in ("owner", "owners", "user", "users", "manager"):
                value = team.get(key)
                candidates.extend(value if isinstance(value, list) else [value])
            for value in candidates:
                if isinstance(value, dict):
                    value = value.get("id") or value.get("userId") or value.get("user_id")
                if value is not None and str(value).strip():
                    result.add(str(value).strip())
    return result


def user_league_ids(payload: dict) -> list[tuple[int, int | None]]:
    """Normalize FetchUserLeagues responses across its historical shapes."""
    found: list[tuple[int, int | None]] = []
    values = (payload or {}).get("leagues") or (payload or {}).get("league") or []
    values = [values] if isinstance(values, dict) else values
    for league in values:
        if not isinstance(league, dict):
            continue
        raw_id = league.get("league_id") or league.get("leagueId") or league.get("id")
        try:
            lid = int(raw_id)
        except (TypeError, ValueError):
            continue
        try:
            season = int(league.get("season")) if league.get("season") is not None else None
        except (TypeError, ValueError):
            season = None
        found.append((lid, season))
    return found


def main() -> None:
    base_probes = 0
    if OUT.exists():
        existing = json.loads(OUT.read_text(encoding="utf-8"))
        base_probes = int(existing.get("generated_probes", 0))
        for rec in existing.get("leagues", []):
            leagues[int(rec["league_id"])] = rec
            seen_ids.add(int(rec["league_id"]))
    log(f"=== FF discovery resume: {len(leagues)} known, target {TARGET}, cap {PROBE_CAP} ===")

    gen = candidate_stream()
    last_ckpt = 0
    while len(leagues) < TARGET and probes < PROBE_CAP:
        lid = next(gen)
        s, j = get(f"https://www.fleaflicker.com/api/FetchLeagueStandings"
                   f"?sport=NFL&league_id={lid}&season=2024")
        time.sleep(PACE)
        if s == 404 or not j:
            continue
        lg = j.get("league", {}) if isinstance(j, dict) else {}
        divisions = j.get("divisions") or []
        size = sum(len(d.get("teams") or []) for d in divisions) or lg.get("size") or 0
        if not lg.get("id") or size < 4:
            continue
        seasons = witness_seasons(lid)
        if not seasons:
            continue
        leagues[lid] = {"league_id": lid, "seasons": seasons, "size": size,
                        "name": lg.get("name") or f"League {lid}"}
        # Owner graph expansion is additive to ID probing. It is bounded because
        # FetchUserLeagues is an enumeration surface, not a reason to crawl forever.
        global owner_graph_probes
        for owner_id in owner_ids_from_standings(j):
            if owner_id in owner_ids_seen or owner_graph_probes >= USER_GRAPH_CAP:
                continue
            owner_ids_seen.add(owner_id)
            for season in seasons[-3:]:
                if owner_graph_probes >= USER_GRAPH_CAP:
                    break
                s_user, user_payload = get(
                    f"https://www.fleaflicker.com/api/FetchUserLeagues?user_id={urllib.parse.quote(owner_id)}"
                    f"&season={season}"
                )
                time.sleep(PACE)
                owner_graph_probes += 1
                for linked_id, linked_season in user_league_ids(user_payload or {}):
                    if linked_id not in seen_ids and len(leagues) < TARGET:
                        linked_seasons = witness_seasons(linked_id)
                        if linked_seasons:
                            seen_ids.add(linked_id)
                            leagues[linked_id] = {
                                "league_id": linked_id, "seasons": linked_seasons,
                                "size": 0, "name": f"User-linked league {linked_id}",
                                "discovery": "owner_graph",
                            }
        if len(leagues) - last_ckpt >= 25:
            last_ckpt = len(leagues)
            write_out(base_probes)
            n_pre = sum(1 for r in leagues.values() if min(r["seasons"]) < 2017)
            log(f"leagues={len(leagues)} probes={probes} pre2017={n_pre}")

    write_out(base_probes)
    n_pre = sum(1 for r in leagues.values() if min(r["seasons"]) < 2017)
    log(f"DONE leagues={len(leagues)} probes(this run)={probes} pre2017={n_pre}")


if __name__ == "__main__":
    main()
