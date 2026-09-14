"""Delete corrupt rows from ___ops.public.yahoo_nfl_player_map.

The cache has Layer 0/1 rows where multiple yahoo_ids point to the same
NFL_player_id with DIFFERENT yahoo_names. The non-bio-matching row is the
corrupt one (a newer player wrongly routed to an older player's NFL_id).

This script keeps only the row whose yahoo_name matches bio.player for that
NFL_player_id and deletes the rest.

Affected NFL_ids (from earlier diagnosis):
- 00-0019646 Janikowski: keep yahoo=118, delete yahoo=5046 (Royce Freeman bogus)
- 00-0021206 Josh McCown: keep yahoo=208, delete yahoo=5967 (Tony Pollard bogus)
- 00-0022787 Matt Schaub: keep yahoo=178, delete yahoo=6849 (Denzel Mims bogus)
- 00-0022924 Ben Roethlisberger: keep yahoo=138, delete yahoo=6770 (Joe Burrow bogus)
- 00-0032876 Jordan Williams-Lambert: keep yahoo=29865, delete yahoo=900000 (Duplicate placeholder)
- 00-0035718 Quinnen Williams: keep yahoo=31835, delete yahoo=6118 (Miles Sanders bogus)
                                                 and yahoo=32486 (Isaiah Searight bogus)
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402

# (yahoo_player_id, NFL_player_id) pairs to delete
CORRUPT_ROWS = [
    ("5046", "00-0019646"),  # Royce Freeman bogus -> Janikowski's NFL_id
    ("5967", "00-0021206"),  # Tony Pollard bogus -> McCown's NFL_id
    ("6849", "00-0022787"),  # Denzel Mims bogus -> Schaub's NFL_id
    ("6770", "00-0022924"),  # Joe Burrow bogus -> Roethlisberger's NFL_id
    ("900000", "00-0032876"),  # Duplicate placeholder
    ("6118", "00-0035718"),  # Miles Sanders bogus -> Quinnen Williams' NFL_id
    ("32486", "00-0035718"),  # Isaiah Searight bogus -> Quinnen Williams' NFL_id
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

    # Backup
    print("[backup] Snapshotting affected cache rows")
    writer.execute(
        """
        CREATE TABLE IF NOT EXISTS ___ops.public.yahoo_nfl_player_map_collision_backup_20260428 AS
        SELECT * FROM ___ops.public.yahoo_nfl_player_map WHERE 1 = 0
        """,
        database="___ops",
    )
    for yid, nfl_id in CORRUPT_ROWS:
        writer.execute(
            f"""
            INSERT INTO ___ops.public.yahoo_nfl_player_map_collision_backup_20260428
            SELECT * FROM ___ops.public.yahoo_nfl_player_map
            WHERE (yahoo_player_id = '{yid}' OR yahoo_player_id = '{yid}.0')
              AND NFL_player_id = '{nfl_id}'
            """,
            database="___ops",
        )

    backup_count = writer.execute(
        "SELECT COUNT(*) AS n FROM ___ops.public.yahoo_nfl_player_map_collision_backup_20260428",
        database="___ops",
    )
    print(f"[backup] Backed up {backup_count[0]['n']} rows")

    # Delete corrupt rows
    for yid, nfl_id in CORRUPT_ROWS:
        result = writer.execute(
            f"""
            DELETE FROM ___ops.public.yahoo_nfl_player_map
            WHERE (yahoo_player_id = '{yid}' OR yahoo_player_id = '{yid}.0')
              AND NFL_player_id = '{nfl_id}'
            """,
            database="___ops",
        )
        print(f"[delete] yahoo={yid} on NFL={nfl_id}: {result}")

    print("Done.")


if __name__ == "__main__":
    main()
