"""
Bulk assess runner — processes all FETCHED sources through the assess pipeline.
Idempotent: skips sources already assessed.

Output:
  - stat_extracts table (player stats found)
  - review_queue.csv  (D-tier sources with tile images for human/Claude review)
  - Pass-by-pass summary printed to stdout
"""
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.assess import assess_batch
from scripts.loc_scraper.config import SCRAPER_ROOT

BATCH = 100


def main():
    conn = open_db()
    print(f"\n{'='*60}")
    print(f"Bulk Assess started  {datetime.now():%Y-%m-%d %H:%M:%S}")

    # Pre-run counts
    pending = conn.execute("""
        SELECT sl.source_type, COUNT(*) n
        FROM source_ledger sl
        WHERE sl.fetch_state='FETCHED' AND sl.assess_state='PENDING'
        GROUP BY sl.source_type ORDER BY sl.source_type
    """).fetchall()
    total = sum(n for _, n in pending)
    print(f"\nPending assessment: {total:,}")
    for stype, n in pending:
        print(f"  {stype:<20} {n:,}")

    totals: dict[str, int] = {}
    pass_num = 0

    while True:
        remaining = conn.execute("""
            SELECT COUNT(*) FROM source_ledger
            WHERE fetch_state='FETCHED' AND assess_state='PENDING'
        """).fetchone()[0]
        if remaining == 0:
            break

        pass_num += 1
        counts = assess_batch(conn, tier=None, limit=BATCH)
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v

        assessed = totals.get("ASSESSED", 0)
        needs_img = totals.get("NEEDS_IMAGE", 0)
        irrelevant = totals.get("IRRELEVANT", 0)
        print(f"[pass {pass_num:3d}]  "
              f"ASSESSED={assessed:4d}  NEEDS_IMAGE={needs_img:4d}  "
              f"IRRELEVANT={irrelevant:4d}  remaining={remaining - BATCH:4d}")

    print(f"\n{'='*60}")
    print(f"Bulk Assess COMPLETE  {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"Final totals: {totals}")

    # Stat extract summary
    rows = conn.execute("""
        SELECT stat_column, confidence, COUNT(*) n, COUNT(DISTINCT player_name_raw) players
        FROM stat_extracts
        WHERE player_name_raw != ''
        GROUP BY stat_column, confidence
        ORDER BY n DESC
    """).fetchall()
    if rows:
        print(f"\nStat extracts with player names:")
        for col, conf, n, players in rows:
            print(f"  {col:<25} {conf:<8} {n:4d} extracts  {players:3d} distinct players")

    # Review queue summary
    review_csv = SCRAPER_ROOT / "review_queue.csv"
    if review_csv.exists():
        import csv
        with open(review_csv, encoding="utf-8") as f:
            review_rows = list(csv.DictReader(f))
        print(f"\nImage review queue: {len(review_rows)} sources → {review_csv}")
        from collections import Counter
        by_tier = Counter(r["tier"] for r in review_rows)
        for tier, n in sorted(by_tier.items()):
            print(f"  {tier}  {n}")

    # Game-level summary
    print(f"\nGames with useful sources (A/B tier):")
    game_rows = conn.execute("""
        SELECT gm.tier, COUNT(DISTINCT gm.game_key) games,
               SUM(gm.sources_useful) useful
        FROM game_manifest gm
        WHERE gm.sources_useful > 0
        GROUP BY gm.tier ORDER BY gm.tier
    """).fetchall()
    for tier, games, useful in game_rows:
        print(f"  {tier}  {games} games  {useful} useful sources")


if __name__ == "__main__":
    main()
