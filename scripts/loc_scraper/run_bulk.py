"""
Bulk LOC fan-out runner.
Discovers every candidate URL for every T0/T1 game, then downloads everything.

Run:
    python -m scripts.loc_scraper.run_bulk [--tier T0] [--skip-discover] [--skip-fetch]

Designed to run overnight. All downloads are idempotent — safe to interrupt and resume.
Progress is printed live. Everything lands on D: drive.
"""

from __future__ import annotations
import argparse
import sys
import time
from datetime import datetime

from .schema import open_db, game_status_summary
from .manifest import build_manifest, print_summary


def run_bulk(tier_filter: str | None, skip_discover: bool, skip_fetch: bool) -> None:
    conn = open_db()

    # Ensure manifest is populated
    total = conn.execute("SELECT COUNT(*) FROM game_manifest").fetchone()[0]
    if total == 0:
        print("Manifest is empty — building from v26...")
        build_manifest(conn)

    tiers = ["T0", "T1"] if not tier_filter else [tier_filter]

    print(f"\n{'='*60}")
    print(f"LOC Bulk Fan-Out  {datetime.now():%Y-%m-%d %H:%M}")
    print(f"Tiers: {tiers}  skip_discover={skip_discover}  skip_fetch={skip_fetch}")
    print_summary(conn)

    # ── PHASE 1: DISCOVER ────────────────────────────────────────────────────
    if not skip_discover:
        from .discover import discover_game, STRATEGY_FNS
        from .config import SEARCH_LEVELS

        for tier in tiers:
            print(f"\n{'─'*50}")
            print(f"DISCOVER {tier}")
            print(f"{'─'*50}")

            passes = 0
            while True:
                # Find games still needing discovery in this tier
                pending = conn.execute("""
                    SELECT game_key FROM game_manifest
                    WHERE tier=? AND state IN ('NEW','LEVEL_UP')
                    ORDER BY year, week
                    LIMIT 50
                """, [tier]).fetchall()

                if not pending:
                    print(f"  {tier}: all games discovered")
                    break

                for (gk,) in pending:
                    n = discover_game(conn, gk)
                    passes += 1

                # Print rolling status every 50 games
                summary = conn.execute("""
                    SELECT state, COUNT(*) FROM game_manifest
                    WHERE tier=? GROUP BY state ORDER BY state
                """, [tier]).fetchall()
                status_str = "  ".join(f"{s}={n}" for s, n in summary)
                print(f"  [{tier} pass {passes}]  {status_str}")

                # Brief pause to avoid hammering LOC
                time.sleep(0.1)

    # ── PHASE 2: FETCH ───────────────────────────────────────────────────────
    if not skip_fetch:
        from .fetch import fetch_batch

        for tier in tiers:
            print(f"\n{'─'*50}")
            print(f"FETCH {tier}")
            print(f"{'─'*50}")

            fetched_total = 0
            while True:
                remaining = conn.execute("""
                    SELECT COUNT(*) FROM source_ledger sl
                    JOIN game_manifest gm ON gm.game_key=sl.game_key
                    WHERE sl.fetch_state='CANDIDATE' AND gm.tier=?
                """, [tier]).fetchone()[0]

                if remaining == 0:
                    print(f"  {tier}: all candidates fetched")
                    break

                results = fetch_batch(conn, tier=tier, limit=100)
                fetched_total += len(results)
                ok = sum(results.values())
                print(f"  [{tier}] fetched {fetched_total} total  "
                      f"({ok} OK this batch, {remaining-len(results)} remaining)")

    # ── FINAL STATUS ─────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"Bulk fan-out complete  {datetime.now():%Y-%m-%d %H:%M}")
    print_summary(conn)

    src_counts = conn.execute("""
        SELECT gm.tier, sl.fetch_state, COUNT(*)
        FROM source_ledger sl
        JOIN game_manifest gm ON gm.game_key=sl.game_key
        GROUP BY gm.tier, sl.fetch_state
        ORDER BY gm.tier, sl.fetch_state
    """).fetchall()
    print("\nSource ledger by tier:")
    for tier, fstate, cnt in src_counts:
        print(f"  {tier}  {fstate:15s}  {cnt:,}")

    # Storage estimate
    from .config import TILES_DIR, OCR_DIR
    import os
    tile_bytes = sum(f.stat().st_size for f in TILES_DIR.rglob("*") if f.is_file())
    ocr_bytes  = sum(f.stat().st_size for f in OCR_DIR.rglob("*")   if f.is_file())
    print(f"\nD: drive usage:")
    print(f"  tiles: {tile_bytes/1e6:.1f} MB  ({sum(1 for _ in TILES_DIR.rglob('*') if _.is_file()):,} files)")
    print(f"  ocr:   {ocr_bytes/1e6:.1f} MB  ({sum(1 for _ in OCR_DIR.rglob('*') if _.is_file()):,} files)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--tier", default=None)
    ap.add_argument("--skip-discover", action="store_true")
    ap.add_argument("--skip-fetch",    action="store_true")
    args = ap.parse_args()
    run_bulk(args.tier, args.skip_discover, args.skip_fetch)
