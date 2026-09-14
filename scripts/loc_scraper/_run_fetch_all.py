"""
Bulk fetch runner — downloads all CANDIDATE sources to D: drive.
Run after discover completes. Idempotent, safe to re-run.

Fetches tiles and OCR JSON for all LOC_TILE sources.
Fetches HTML for PFA_INDEX / PFR_BOXSCORE sources.
Skips LOCAL_FILE (already on disk).
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.manifest import print_summary
from scripts.loc_scraper.fetch import fetch_batch
from scripts.loc_scraper.config import TILES_DIR, OCR_DIR

BATCH = 50


def main():
    conn = open_db()
    print(f"\n{'='*60}")
    print(f"Bulk Fetch started  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print_summary(conn)

    # Summary of what we're about to fetch
    pending_by_type = conn.execute("""
        SELECT sl.source_type, COUNT(*)
        FROM source_ledger sl
        WHERE sl.fetch_state='CANDIDATE'
        GROUP BY sl.source_type ORDER BY sl.source_type
    """).fetchall()
    total_pending = sum(n for _, n in pending_by_type)
    print(f"\nCandidates to fetch: {total_pending:,}")
    for stype, cnt in pending_by_type:
        print(f"  {stype:25s}  {cnt:,}")

    fetched_total = 0
    failed_total = 0
    pass_num = 0

    while True:
        remaining = conn.execute(
            "SELECT COUNT(*) FROM source_ledger WHERE fetch_state='CANDIDATE'"
        ).fetchone()[0]
        if remaining == 0:
            break

        pass_num += 1
        results = fetch_batch(conn, tier=None, limit=BATCH)
        ok = sum(results.values())
        fetched_total += ok
        failed_total += len(results) - ok

        print(f"[pass {pass_num:3d}] "
              f"fetched={fetched_total:5d}  failed={failed_total:3d}  "
              f"remaining={remaining - len(results):5d}")

    print(f"\n{'='*60}")
    print(f"Bulk Fetch COMPLETE  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Total fetched: {fetched_total:,}  failed: {failed_total:,}")
    print_summary(conn)

    # D: drive storage summary
    tile_bytes = sum(f.stat().st_size for f in TILES_DIR.rglob("*") if f.is_file())
    ocr_bytes  = sum(f.stat().st_size for f in OCR_DIR.rglob("*")   if f.is_file())
    tile_files = sum(1 for _ in TILES_DIR.rglob("*") if _.is_file())
    ocr_files  = sum(1 for _ in OCR_DIR.rglob("*")   if _.is_file())
    print(f"\nD: drive usage:")
    print(f"  tiles: {tile_bytes/1e6:8.1f} MB  ({tile_files:,} files)")
    print(f"  ocr:   {ocr_bytes/1e6:8.1f} MB  ({ocr_files:,} files)")
    print(f"  total: {(tile_bytes+ocr_bytes)/1e6:8.1f} MB")

    # Source ledger final state
    final = conn.execute("""
        SELECT gm.tier, sl.source_type, sl.fetch_state, COUNT(*)
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key=sl.game_key
        GROUP BY gm.tier, sl.source_type, sl.fetch_state
        ORDER BY gm.tier, sl.source_type, sl.fetch_state
    """).fetchall()
    print("\nSource ledger by tier/type/state:")
    for tier, stype, fstate, cnt in final:
        print(f"  {tier}  {stype:25s}  {fstate:15s}  {cnt:,}")


if __name__ == "__main__":
    main()
