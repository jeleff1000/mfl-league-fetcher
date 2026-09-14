"""Backfill player_fantasy.NFL_player_id from player_bio + cache maps.

Cross-DB UPDATE-FROM 500s on Fly's read-write endpoint, so we issue one small
UPDATE per (platform, platform_id, NFL_player_id) tuple instead.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def collect_mappings(w: FlyWriter) -> dict[tuple[str, str], str]:
    """Returns {(platform, platform_id_str): NFL_player_id} from bio + cache."""
    out: dict[tuple[str, str], str] = {}

    # bio yahoo
    rows = w.execute(
        "SELECT NFL_player_id, CAST(CAST(yahoo_player_id AS BIGINT) AS VARCHAR) AS pid "
        "FROM nfl_historical.player_bio WHERE yahoo_player_id IS NOT NULL",
        database="___ops",
    )
    for r in rows:
        out[("yahoo", r["pid"])] = r["NFL_player_id"]

    # bio sleeper
    rows = w.execute(
        "SELECT NFL_player_id, CAST(CAST(sleeper_player_id AS BIGINT) AS VARCHAR) AS pid "
        "FROM nfl_historical.player_bio WHERE sleeper_player_id IS NOT NULL",
        database="___ops",
    )
    for r in rows:
        out[("sleeper", r["pid"])] = r["NFL_player_id"]

    # bio espn
    rows = w.execute(
        "SELECT NFL_player_id, espn_id AS pid " "FROM nfl_historical.player_bio WHERE espn_id IS NOT NULL",
        database="___ops",
    )
    for r in rows:
        out[("espn", r["pid"])] = r["NFL_player_id"]

    # cache aliases (yahoo, sleeper, espn) — covers Travis Hunter etc.
    for platform in ("yahoo", "sleeper", "espn"):
        rows = w.execute(
            f"SELECT NFL_player_id, {platform}_player_id AS pid "
            f"FROM public.{platform}_nfl_player_map WHERE {platform}_player_id IS NOT NULL",
            database="___ops",
        )
        for r in rows:
            key = (platform, r["pid"])
            # Bio takes precedence; only fill if not already set.
            if key not in out:
                out[key] = r["NFL_player_id"]
    return out


def collect_unjoined_keys(w: FlyWriter) -> list[tuple[str, str]]:
    """List of (platform, platform_id_str) for currently-unjoined player_fantasy rows."""
    rows = w.execute(
        """
        SELECT DISTINCT platform,
          CASE platform
            WHEN 'yahoo' THEN yahoo_player_id
            WHEN 'sleeper' THEN sleeper_player_id
            WHEN 'espn' THEN espn_player_id
          END AS pid
        FROM public.player_fantasy
        WHERE NFL_player_id IS NULL
          AND platform IN ('yahoo','sleeper','espn')
          AND CASE platform
            WHEN 'yahoo' THEN yahoo_player_id
            WHEN 'sleeper' THEN sleeper_player_id
            WHEN 'espn' THEN espn_player_id
          END IS NOT NULL
        """,
        database="___leagues",
    )
    return [(r["platform"], r["pid"]) for r in rows]


def main() -> None:
    load_env()
    w = FlyWriter()

    print("[before]")
    before = w.execute(
        "SELECT COUNT(*) AS c FROM public.player_fantasy WHERE NFL_player_id IS NULL",
        database="___leagues",
    )
    print(f"  unjoined rows: {before[0]['c']}")

    print("[collect mappings]")
    mappings = collect_mappings(w)
    print(f"  bio+cache mappings: {len(mappings)}")

    print("[collect unjoined keys]")
    unjoined_keys = collect_unjoined_keys(w)
    print(f"  unjoined keys: {len(unjoined_keys)}")

    resolvable = [(p, pid, mappings[(p, pid)]) for (p, pid) in unjoined_keys if (p, pid) in mappings]
    print(f"  resolvable now: {len(resolvable)}")

    total_updated = 0
    for i, (platform, pid, nfl_id) in enumerate(resolvable, 1):
        col = f"{platform}_player_id"
        # Escape pid (may contain '-' for ESPN)
        pid_esc = str(pid).replace("'", "''")
        sql = (
            f"UPDATE public.player_fantasy SET NFL_player_id = '{nfl_id}' "
            f"WHERE NFL_player_id IS NULL AND platform = '{platform}' "
            f"AND {col} = '{pid_esc}'"
        )
        try:
            res = w.execute(sql, database="___leagues")
            cnt = res[0]["Count"] if res and "Count" in res[0] else 0
            total_updated += cnt
            if i % 25 == 0 or cnt > 50:
                print(f"  [{i}/{len(resolvable)}] {platform} {pid} -> {nfl_id}: {cnt} rows")
        except Exception as e:
            print(f"  [{i}/{len(resolvable)}] FAIL {platform} {pid} -> {nfl_id}: {e}")

    print(f"\n[total updated]: {total_updated}")

    print("\n[after]")
    after = w.execute(
        "SELECT COUNT(*) AS c FROM public.player_fantasy WHERE NFL_player_id IS NULL",
        database="___leagues",
    )
    print(f"  unjoined rows: {after[0]['c']}")


if __name__ == "__main__":
    main()
