"""T1 (1932-1945) discovery runner — Evening Star first, then PFR, then LOC locals."""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.manifest import print_summary
from scripts.loc_scraper.discover import discover_game

TIER = "T1"
BATCH = 25

def main():
    conn = open_db()
    print(f"\n{'='*60}")
    print(f"T1 Discovery started  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print_summary(conn)

    total_processed = 0
    total_added = 0
    run_pass = 0

    while True:
        rows = conn.execute("""
            SELECT game_key FROM game_manifest
            WHERE tier=? AND state IN ('NEW','LEVEL_UP')
            ORDER BY year, week
            LIMIT ?
        """, [TIER, BATCH]).fetchall()

        if not rows:
            break

        run_pass += 1
        batch_added = 0
        for (gk,) in rows:
            n = discover_game(conn, gk)
            batch_added += n
            total_added += n
            total_processed += 1

        summary = conn.execute("""
            SELECT state, COUNT(*) FROM game_manifest
            WHERE tier=? GROUP BY state ORDER BY state
        """, [TIER]).fetchall()
        status = "  ".join(f"{s}={n}" for s, n in summary)
        print(f"[pass {run_pass:3d}] processed={total_processed:4d}  "
              f"candidates_added={total_added:5d}  {status}")

    print(f"\n{'='*60}")
    print(f"T1 Discovery COMPLETE  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Total games processed: {total_processed}")
    print(f"Total candidates added: {total_added}")
    print_summary(conn)

    counts = conn.execute("""
        SELECT sl.source_type, sl.fetch_state, COUNT(*)
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key=sl.game_key
        WHERE gm.tier=?
        GROUP BY sl.source_type, sl.fetch_state
        ORDER BY sl.source_type, sl.fetch_state
    """, [TIER]).fetchall()
    print(f"\nT1 source ledger:")
    for stype, fstate, cnt in counts:
        print(f"  {stype:20s}  {fstate:15s}  {cnt:,}")

if __name__ == "__main__":
    main()
