"""Restore ___ops.nfl_historical.player_bio.yahoo_player_id from the
2026-04-28 backup, then apply ONLY the verified-safe corrections.

Why
---
The 859-row "correction" batch (scripts/fix_player_bio_yahoo_id.py) trusted
yahoo_nfl_player_map as the authoritative source. But the cache had its own
corruption pattern: dozens of NEWER players' yahoo_ids were wrongly attached
to OLDER players' NFL_ids (the same pattern as Royce Freeman -> Janikowski's
NFL_id, but at scale). My batch correction propagated that corruption into
bio.

Concrete example: Chris Johnson RB (NFL 00-0026164) had yahoo_id=8801 in the
backup (the correct Yahoo ID for the Titans RB). The cache had a row
"yahoo 272 -> NFL 00-0026164" which my fix used to set bio.yahoo_player_id=272.
But yahoo 272 is some unrelated older player; 8801 was right.

After this incorrect "correction," mohoney's 2013 imports can't resolve
yahoo=8801 (Chris Johnson) to any NFL_player_id, so his fantasy_points are 0
and 100s of starter rows undercount.

Safe corrections
----------------
Only 4 verified-safe NFL_id corrections, mirroring scripts/
fix_player_bio_collision_repair.py + the Conklin/Izzo swap:
  Tyler Conklin    NFL 00-0034270 -> yahoo 31127  (was 31220, swap with Izzo)
  Ryan Izzo        NFL 00-0034439 -> yahoo 31220  (was 31127, swap with Conklin)
  Sebastian Janikowski (00-0019646)  -> yahoo 118
  Josh McCown          (00-0021206)  -> yahoo 208
  Matt Schaub          (00-0022787)  -> yahoo 178
  Ben Roethlisberger   (00-0022924)  -> yahoo 138

Procedure
---------
1. RESTORE: bulk UPDATE player_bio.yahoo_player_id from backup_20260428.
2. APPLY 6 specific UPDATEs.
3. Verify Chris Johnson RB is back to yahoo 8801 + Conklin is at yahoo 31127.

Then regenerate ops_cache_fixups.sql with only the 6 specific UPDATEs.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402

SAFE_CORRECTIONS = [
    # (NFL_player_id, yahoo_player_id, label)
    ("00-0034270", 31127, "Tyler Conklin"),
    ("00-0034439", 31220, "Ryan Izzo"),
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

    # Phase 1: restore yahoo_player_id from backup for all 38,430 rows
    print("[restore] Restoring player_bio.yahoo_player_id from backup_20260428")
    res = writer.execute(
        """
        UPDATE ___ops.nfl_historical.player_bio b
        SET yahoo_player_id = bk.yahoo_player_id
        FROM ___ops.public.player_bio_yahoo_id_backup_20260428 bk
        WHERE b.NFL_player_id = bk.NFL_player_id
        """,
        database="___ops",
    )
    print(f"  {res}")

    # Phase 2: apply ONLY the 6 verified-safe corrections.
    # Two-step swap to avoid uniqueness violations during partial state:
    #   Phase 2a: NULL out yahoo_id wherever it currently sits (for the 6 yahoo_ids)
    #   Phase 2b: SET yahoo_id on the correct NFL_player_id row
    safe_yahoo_ids = ", ".join(str(c[1]) for c in SAFE_CORRECTIONS)
    print(f"[clear] Clearing yahoo_player_id from any rows currently holding {{{safe_yahoo_ids}}}")
    res = writer.execute(
        f"""
        UPDATE ___ops.nfl_historical.player_bio
        SET yahoo_player_id = NULL
        WHERE CAST(yahoo_player_id AS BIGINT) IN ({safe_yahoo_ids})
        """,
        database="___ops",
    )
    print(f"  {res}")

    print("[set] Applying 6 safe corrections")
    for nfl_id, yahoo_id, label in SAFE_CORRECTIONS:
        res = writer.execute(
            f"""
            UPDATE ___ops.nfl_historical.player_bio
            SET yahoo_player_id = {yahoo_id}
            WHERE NFL_player_id = '{nfl_id}'
            """,
            database="___ops",
        )
        print(f"  {label} ({nfl_id}) -> yahoo {yahoo_id}: {res}")

    # Verify
    print("\n[verify] Spot-check Conklin/Izzo + 4 collision repairs + Chris Johnson RB")
    rows = writer.execute(
        """
        SELECT NFL_player_id, player, CAST(yahoo_player_id AS BIGINT) AS yahoo_id
        FROM ___ops.nfl_historical.player_bio
        WHERE NFL_player_id IN (
          '00-0034270', '00-0034439', '00-0019646', '00-0021206',
          '00-0022787', '00-0022924', '00-0026164'
        )
        ORDER BY NFL_player_id
        """,
        database="___ops",
    )
    for r in rows:
        print(f"  {r}")


if __name__ == "__main__":
    main()
