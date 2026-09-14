"""Repoint yahoo_nfl_player_map for Charles Johnson WR (yahoo_player_id 26839).

The cache currently has yahoo 26839 -> NFL 00-0008454 (Buffalo WR retired
2002), which is wrong. Yahoo 26839 is Charles D. Johnson, Vikings WR
2013-2016, NFL_player_id 00-0030113. Both share the display name
"Charles Johnson"; Layer-1 exact-name matching gave 100% confidence
without disambiguating, so the resolver picked the older (alphabetically
or insertion-order earlier) bio row.

player_bio is already correct (26839 lives on 00-0030113); only the
cache row needs to be repointed. Same pattern as the Conklin/Izzo and
Janikowski/Schaub/etc. cache repairs from session 5 (2026-04-28).

Concrete impact: Ross 2015 wk1 (mohoney_moproblems) had Charles Johnson
WR in his FLEX with 4.70 pts per Yahoo's authoritative XML, but our
player_fantasy row pointed at the retired Buffalo WR and stored 0 pts,
producing a +4.7 system_team_points_vs_player_sum gap. Fleet-wide,
many leagues' 2014-2015 imports inherit the same misroute.

Procedure
---------
1. Pre-check: confirm cache row currently has the wrong NFL_player_id.
2. Pre-check: confirm bio is already correct.
3. UPDATE the cache row to point at 00-0030113.
4. Post-verify: re-read the cache row.

After this, the next reimport for any affected league will pick up the
corrected mapping. To clear stale player_fantasy rows already pointed
at 00-0008454, those leagues need to be reimported.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402


def load_env():
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def main():
    load_env()
    writer = FlyWriter()

    # Pre-check 1: cache currently misroutes 26839
    pre = writer.execute(
        "SELECT yahoo_player_id, NFL_player_id, nfl_name, match_layer "
        "FROM public.yahoo_nfl_player_map WHERE yahoo_player_id = '26839'",
        database="___ops",
    )
    if not pre:
        print("FAIL: cache row for yahoo_player_id 26839 not found; nothing to fix")
        sys.exit(1)
    row = pre[0]
    print(
        f"Pre-fix cache row: NFL={row.get('NFL_player_id')} name={row.get('nfl_name')} layer={row.get('match_layer')}"
    )
    if row.get("NFL_player_id") == "00-0030113":
        print("Already correct (00-0030113); nothing to do")
        return
    if row.get("NFL_player_id") != "00-0008454":
        print(f"UNEXPECTED current NFL_player_id ({row.get('NFL_player_id')}); aborting for safety")
        sys.exit(1)

    # Pre-check 2: bio is already correct
    bio = writer.execute(
        "SELECT NFL_player_id, player, latest_team, draft_year, last_year, yahoo_player_id "
        "FROM nfl_historical.player_bio "
        "WHERE NFL_player_id IN ('00-0008454', '00-0030113') "
        "ORDER BY draft_year",
        database="___ops",
    )
    by_id = {r["NFL_player_id"]: r for r in bio}
    target = by_id.get("00-0030113")
    old = by_id.get("00-0008454")
    print(f"Bio 00-0030113 (target): yahoo_player_id={target.get('yahoo_player_id') if target else 'MISSING'}")
    print(f"Bio 00-0008454 (old):    yahoo_player_id={old.get('yahoo_player_id') if old else 'MISSING'}")
    if not target or int(target.get("yahoo_player_id") or 0) != 26839:
        print("UNEXPECTED bio state for 00-0030113; aborting for safety")
        sys.exit(1)
    if old and old.get("yahoo_player_id") not in (None, 0, "0"):
        print("UNEXPECTED bio state for 00-0008454 (still has yahoo_player_id); aborting for safety")
        sys.exit(1)

    # Apply the cache repair. Layer 0 = manual override convention.
    writer.execute(
        """
        UPDATE public.yahoo_nfl_player_map
        SET NFL_player_id = '00-0030113',
            nfl_name = 'Charles D. Johnson',
            match_layer = 0,
            match_confidence = 100,
            updated_at = CURRENT_TIMESTAMP
        WHERE yahoo_player_id = '26839'
        """,
        database="___ops",
    )

    # Post-verify
    post = writer.execute(
        "SELECT yahoo_player_id, NFL_player_id, nfl_name, match_layer, match_confidence "
        "FROM public.yahoo_nfl_player_map WHERE yahoo_player_id = '26839'",
        database="___ops",
    )
    if not post or post[0].get("NFL_player_id") != "00-0030113":
        print("FAIL: post-verify did not see 00-0030113")
        sys.exit(1)
    print(f"Post-fix cache row: {post[0]}")
    print("Done. ops_cache_fixups.sql already mirrors this update for local-cache restores.")


if __name__ == "__main__":
    main()
