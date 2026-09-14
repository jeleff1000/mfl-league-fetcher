"""
Run T0 + T1 discovery sequentially, then report totals.
T0 handles any remaining games (resumes if already partially done).
T1 starts fresh after T0 completes.
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.manifest import print_summary
from scripts.loc_scraper.discover import discover_game

BATCH = 25


def run_tier(conn, tier: str):
    total_processed = 0
    total_added = 0
    run_pass = 0
    t0 = datetime.now()
    print(f"\n{'─'*50}")
    print(f"{tier} Discovery started  {t0:%H:%M:%S}")

    while True:
        rows = conn.execute("""
            SELECT game_key FROM game_manifest
            WHERE tier=? AND state IN ('NEW','LEVEL_UP')
            ORDER BY year, week LIMIT ?
        """, [tier, BATCH]).fetchall()
        if not rows:
            break

        run_pass += 1
        for (gk,) in rows:
            n = discover_game(conn, gk)
            total_added += n
            total_processed += 1

        summary = conn.execute("""
            SELECT state, COUNT(*) FROM game_manifest
            WHERE tier=? GROUP BY state ORDER BY state
        """, [tier]).fetchall()
        status = "  ".join(f"{s}={n}" for s, n in summary)
        elapsed = (datetime.now() - t0).total_seconds()
        print(f"[{tier} pass {run_pass:3d}] "
              f"proc={total_processed:4d}  cand={total_added:4d}  "
              f"t={elapsed:.0f}s  {status}")

    elapsed = (datetime.now() - t0).total_seconds()
    print(f"{tier} DONE  {total_processed} games  {total_added} candidates  {elapsed:.0f}s")

    # Source ledger for this tier
    counts = conn.execute("""
        SELECT sl.source_type, sl.fetch_state, COUNT(*)
        FROM source_ledger sl JOIN game_manifest gm ON gm.game_key=sl.game_key
        WHERE gm.tier=? GROUP BY sl.source_type, sl.fetch_state
    """, [tier]).fetchall()
    for stype, fstate, cnt in counts:
        print(f"  {stype:20s}  {fstate:12s}  {cnt:,}")

    return total_added


def main():
    conn = open_db()
    print(f"\n{'='*60}")
    print(f"Discover All  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print_summary(conn)

    t0_added = run_tier(conn, "T0")
    t1_added = run_tier(conn, "T1")

    print(f"\n{'='*60}")
    print(f"All discovery COMPLETE  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"T0 candidates: {t0_added}   T1 candidates: {t1_added}")
    print(f"Total: {t0_added + t1_added}")
    print_summary(conn)


if __name__ == "__main__":
    main()
