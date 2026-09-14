"""Repair 4 player_bio rows where fix_player_bio_yahoo_id.py wrongly assigned
a newer player's yahoo_id to an older player's NFL_id.

Cause: yahoo_nfl_player_map has corrupt rows for these 4 NFL_ids where TWO
different players (with different yahoo_ids) point to the same NFL_id. Both
rows are match_layer=1, both have name-matched yahoo_name=nfl_name, but one
is a glitch (the newer player erroneously routed to the older player's NFL_id).

Affected:
- 00-0019646 (Sebastian Janikowski K, drafted 2000): correct yahoo=118, my
  fix wrongly set it to 5046 (Royce Freeman RB).
- 00-0021206 (Josh McCown QB, retired 2020): correct yahoo=208, my fix wrongly
  set it to 5967 (Tony Pollard RB).
- 00-0022787 (Matt Schaub QB, retired 2017): correct yahoo=178, my fix wrongly
  set it to 6849 (Denzel Mims WR).
- 00-0022924 (Ben Roethlisberger QB, retired 2021): correct yahoo=138, my fix
  wrongly set it to 6770 (Joe Burrow QB).

For Jordan Williams-Lambert (00-0032876) and Quinnen Williams (00-0035718)
the cache also had collisions but the *correct* row won the dedup tiebreak,
so bio wasn't broken there.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402

CORRECTIONS = [
    ("00-0019646", 118, "Sebastian Janikowski"),
    ("00-0021206", 208, "Josh McCown"),
    ("00-0022787", 178, "Matt Schaub"),
    ("00-0022924", 138, "Ben Roethlisberger"),
]


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
    for nfl_id, yahoo_id, name in CORRECTIONS:
        sql = f"""
        UPDATE ___ops.nfl_historical.player_bio
        SET yahoo_player_id = {yahoo_id}
        WHERE NFL_player_id = '{nfl_id}'
        """
        result = writer.execute(sql, database="___ops")
        print(f"[fix] {name} ({nfl_id}) -> yahoo {yahoo_id}: {result}")
    print("Done.")


if __name__ == "__main__":
    main()
