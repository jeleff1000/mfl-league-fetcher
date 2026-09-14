"""discover_fleaflicker_pilot.py -- pick a pilot cohort of public Fleaflicker leagues.

Fleaflicker league IDs are a dense sequential space (1..~357k, no auth), so discovery is
enumeration, not BFS. This samples two buckets:
  - modern:     last-played 2024/2025, for parity with the Sleeper-dominated cohort cells
  - historical: leagues that GENUINELY played 2015/2016 -- the years Sleeper can never fill

TRAP this script exists to respect: the API silently serves the last-played season when
asked for one the league didn't play. The only honest witness is the scoreboard's
schedule-period epoch year (see FleaflickerAPIClient.season_was_played).

    py -3 scripts/fleaflicker_corpus/discover_fleaflicker_pilot.py --modern 10 --historical 8
"""
from __future__ import annotations

import argparse
import datetime
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.fleaflicker.fleaflicker_api_client import (  # noqa: E402
    FleaflickerAPIClient,
    FleaflickerAPIConfig,
    FleaflickerAPIError,
)

CORPUS = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
OUT = CORPUS / "discovery" / "fleaflicker_pilot.json"

# ID bands: leagues are created in ID order, so the band bounds the creation year.
MODERN_BAND = (330_000, 356_000)      # created ~2023-2025
HISTORICAL_BAND = (60_000, 260_000)   # created before 2016 -> could have played 2015/2016


def default_scoreboard(client: FleaflickerAPIClient, league_id: int) -> dict | None:
    """Scoreboard with NO season param: serves the league's default (last-played) season."""
    try:
        return client._get("FetchLeagueScoreboard", {"league_id": league_id})
    except FleaflickerAPIError:
        return None


def epoch_year(scoreboard: dict | None) -> int | None:
    period = (scoreboard or {}).get("schedulePeriod") or {}
    low = period.get("low") or {}
    ms = low.get("startEpochMilli")
    if not ms:
        return None
    try:
        return datetime.datetime.fromtimestamp(int(ms) / 1000, datetime.timezone.utc).year
    except (TypeError, ValueError, OSError):
        return None


def league_meta(client: FleaflickerAPIClient, league_id: int) -> dict:
    standings = client.fetch_standings(league_id) or {}
    league = standings.get("league") or {}
    size = league.get("size")
    if size is None:
        size = sum(len(d.get("teams") or []) for d in standings.get("divisions") or [])
    return {"name": league.get("name"), "size": size, "max_keepers": league.get("maxKeepers")}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--modern", type=int, default=10)
    ap.add_argument("--historical", type=int, default=8)
    ap.add_argument("--rate", type=int, default=90, help="client req/min budget")
    ap.add_argument("--seed", type=int, default=20260717)
    ap.add_argument("--max-probes", type=int, default=600)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    client = FleaflickerAPIClient(FleaflickerAPIConfig(rate_limit_per_min=args.rate))
    picked: list[dict] = []
    probes = 0

    def note(row: dict) -> None:
        picked.append(row)
        print(f"  [pick {len(picked)}] {row['bucket']} league={row['league_id']} "
              f"'{row.get('name')}' size={row.get('size')} last={row.get('last_played')}", flush=True)

    # -- modern bucket: default scoreboard tells liveness + last-played in ONE call
    modern_ids = rng.sample(range(*MODERN_BAND), 400)
    for lid in modern_ids:
        if sum(1 for r in picked if r["bucket"] == "modern") >= args.modern or probes >= args.max_probes:
            break
        probes += 1
        sb = default_scoreboard(client, lid)
        yr = epoch_year(sb)
        games = len((sb or {}).get("games") or [])
        if yr in (2024, 2025) and games >= 4:  # >=8 teams playing
            meta = league_meta(client, lid)
            if (meta.get("size") or 0) >= 8 and meta.get("name"):
                note({"league_id": lid, "bucket": "modern", "last_played": yr, **meta})

    # -- historical bucket: direct epoch-witness probe for 2015 (falls back to 2016)
    hist_ids = rng.sample(range(*HISTORICAL_BAND), 1200)
    for lid in hist_ids:
        if sum(1 for r in picked if r["bucket"] == "historical") >= args.historical or probes >= args.max_probes:
            break
        probes += 1
        target = None
        if client.season_was_played(lid, 2015):
            target = 2015
        elif client.season_was_played(lid, 2016):
            probes += 1
            target = 2016
        if target:
            meta = league_meta(client, lid)
            if (meta.get("size") or 0) >= 8 and meta.get("name"):
                note({"league_id": lid, "bucket": "historical", "played_target": target,
                      "last_played": None, **meta})

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps({"picked": picked, "probes": probes}, indent=1))
    print(f"\n[done] {len(picked)} leagues picked in {probes} probes -> {OUT}", flush=True)
    if not picked:
        raise SystemExit("[fatal] discovery picked 0 leagues")


if __name__ == "__main__":
    main()
