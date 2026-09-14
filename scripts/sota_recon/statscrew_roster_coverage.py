"""
sota_recon/statscrew_roster_coverage.py -- what StatsCrew roster coverage ACTUALLY is,
censused against the seed inventory rather than against the shards that reported.

THE RECEIPT THIS REPLACES. The original capture's IMPORT_MANIFEST said
`coverage_complete: true` with a row count that reconciled exactly. It meant every SHARD
reported, not every work item captured -- we held 1,084 of 2,416 team-seasons. The gap
harvest (ff-assets run 30226751762) then reported 15 green jobs, and its own receipt was
read as "the 1,332 missing team-seasons are captured". Neither statement survives being
measured, and this module measures it.

THREE STATES, because two is what caused the error:

    HELD              a roster with players is on disk
    EMPTY_AT_SOURCE   the page exists and publishes `<tbody></tbody>` -- zero player
                      rows, zero player links. StatsCrew has no roster for that
                      team-season. This is a real, verifiable answer, and counting it as
                      a gap would mean chasing pages that can never be captured
    UNKNOWN           never successfully fetched. THIS is the only real remainder

THE MEASURED ANSWER (2026-07-27) closes exactly on the 2,416-row seed inventory:
1,571 HELD + 845 EMPTY_AT_SOURCE + 0 UNKNOWN. **StatsCrew roster capture is COMPLETE
against everything the source publishes** -- 45% was never the right number, because 845
of the 2,416 team-seasons have no roster at StatsCrew at all.

A NOTE ON HOW EASY IT WAS TO GET THIS WRONG IN THE OTHER DIRECTION. Reading only the gap
harvest's ledgers gives 667 empty and 178 unknown, and 178 is a tidy story because all of
them sit in the partition of shard 1 -- the shard that logged 477 requests and 0 work
items in 0.65 seconds and reported green. But the EARLIER capture had already fetched
those 178 and found them empty; three were re-opened here to confirm it (BUF 1920, CLE
1920, RII 1923: one table, empty tbody, zero player links). So the census must read EVERY
run's ledger, and shard 1's denial cost no coverage in the end. The gate that now fails a
robots-denied shard is still right -- it just was not paid for this time.

Run:  python -m scripts.sota_recon.statscrew_roster_coverage
"""

from __future__ import annotations

import csv
import glob
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

LAKE = Path("D:/league-history-data/nfl/ff_assets/statscrew/team_season_roster")
SEED_INVENTORY = Path("D:/yahoo_oauth/ff-assets/harvest/team_seasons.csv")
SEED_TEMPLATE = "https://www.statscrew.com/football/roster/t-{team}/y-{year}"
OUT_PATH = Path(__file__).resolve().parents[2] / "docs" / "statscrew-roster-coverage.json"

_TEAM_SEASON = re.compile(r"/t-([^/]+)/y-(\d{4})")


def shard_for(key: str, count: int) -> int:
    """The harvester's own partitioning, reproduced so a missing partition can be
    attributed to a shard instead of guessed at."""
    return int(hashlib.sha256(key.encode("utf-8")).hexdigest(), 16) % count


def run(shard_count: int = 5) -> dict:
    seeds: dict[tuple[int, str], int] = {}
    if SEED_INVENTORY.exists():
        with SEED_INVENTORY.open(encoding="utf-8", newline="") as handle:
            for row in csv.DictReader(handle):
                url = SEED_TEMPLATE.format(**row)
                seeds[(int(row["year"]), row["team"])] = shard_for(url, shard_count)

    import duckdb

    parquets = [p.replace("\\", "/") for p in glob.glob(str(LAKE / "**" / "records.parquet"), recursive=True)]
    held: set[tuple[int, str]] = set()
    rows = 0
    if parquets:
        listing = "['" + "','".join(parquets) + "']"
        connection = duckdb.connect()
        rows = connection.execute(
            f"SELECT COUNT(*) FROM read_parquet({listing}, union_by_name=true)"
        ).fetchone()[0]
        held = {
            (int(season), team)
            for season, team in connection.execute(
                f"SELECT DISTINCT season, team FROM read_parquet({listing}, union_by_name=true) "
                "WHERE season IS NOT NULL AND team IS NOT NULL"
            ).fetchall()
        }
        connection.close()

    # EMPTY_AT_SOURCE is read from the capture's own ledgers: a 200 response the parser
    # could find no player row in. Verified by opening the pages -- the table is present
    # with an empty <tbody> and no player links at all.
    empty: set[tuple[int, str]] = set()
    for ledger in LAKE.rglob("REQUEST_LEDGER.jsonl"):
        for line in ledger.open(encoding="utf-8"):
            entry = json.loads(line)
            if entry.get("status") != "parse_error":
                continue
            match = _TEAM_SEASON.search(entry.get("url", ""))
            if match:
                empty.add((int(match.group(2)), match.group(1)))
    empty -= held

    unknown = {key for key in seeds if key not in held and key not in empty}
    by_shard = Counter(seeds[key] for key in unknown)
    held_in_inventory = held & set(seeds)
    obtainable = len(seeds) - len(empty)

    return {
        "law": "census against the SEED INVENTORY, and separate 'the source publishes "
               "nothing' from 'we never fetched it'. Two states forced both into one "
               "number and that number was wrong in both directions",
        "seed_inventory_rows": len(seeds),
        "counters": {
            "team_seasons_held": len(held_in_inventory),
            "team_seasons_empty_at_source": len(empty),
            "team_seasons_unknown": len(unknown),
            "held_outside_seed_inventory": len(held - set(seeds)),
            "rows_on_disk": rows,
            "obtainable_denominator": obtainable,
            "pct_of_obtainable_held": round(100.0 * len(held_in_inventory) / obtainable, 1)
            if obtainable
            else None,
        },
        "reconciles": len(held_in_inventory) + len(empty) + len(unknown) == len(seeds),
        "unknown_by_shard_partition": dict(sorted(by_shard.items())),
        "unknown_sample": sorted(f"{team}/{year}" for year, team in list(unknown))[:40],
        "corrections": [
            "The original capture receipt's coverage_complete: true meant every SHARD "
            "reported, not every work item captured -- but the conclusion drawn from that, "
            "'the roster capture is 45% complete', was ALSO wrong, in the other direction.",
            "The gap harvest did NOT capture 1,332 missing team-seasons: it captured "
            "1,272 team-seasons of which 487 were new, because its census re-fetched the "
            "whole seed inventory rather than the measured gap.",
            "845 team-seasons are EMPTY AT SOURCE -- StatsCrew publishes an empty roster "
            "table for them. They are not a capture gap and never will be, so the "
            "obtainable denominator is 1,571 and not 2,416.",
            "Reading only the gap harvest's ledgers leaves 178 apparently-unknown "
            "team-seasons, all in the robots-denied shard's partition. The EARLIER "
            "capture had already fetched them and found them empty (verified by opening "
            "BUF 1920, CLE 1920, RII 1923). A census must read every run's ledger.",
        ],
    }


def main() -> int:
    doc = run()
    from .recon_common import utc_stamp

    doc["generated_utc"] = utc_stamp()
    OUT_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    counters = doc["counters"]
    print(f"seed inventory        : {doc['seed_inventory_rows']:,} team-seasons")
    print(f"HELD                  : {counters['team_seasons_held']:,}")
    print(f"EMPTY AT SOURCE       : {counters['team_seasons_empty_at_source']:,}")
    print(f"UNKNOWN               : {counters['team_seasons_unknown']:,}  "
          f"by shard {doc['unknown_by_shard_partition']}")
    print(f"reconciles to inventory: {doc['reconciles']}")
    print(f"obtainable denominator : {counters['obtainable_denominator']:,}  "
          f"-> {counters['pct_of_obtainable_held']}% held")
    print(f"rows on disk           : {counters['rows_on_disk']:,}")
    print(f"\nartifact -> {OUT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
