"""Quick manifest status check."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
from scripts.loc_scraper.manifest import print_summary, build_manifest

conn = open_db()
total = conn.execute("SELECT COUNT(*) FROM game_manifest").fetchone()[0]
print(f"Manifest rows: {total}")

if total == 0:
    print("Building manifest from v26...")
    build_manifest(conn)

print_summary(conn)

rows = conn.execute(
    "SELECT game_key, tier, state, year, week FROM game_manifest ORDER BY tier, year, week LIMIT 8"
).fetchall()
print("\nSample rows:")
for r in rows:
    print(" ", r)

src_count = conn.execute("SELECT COUNT(*) FROM source_ledger").fetchone()[0]
print(f"\nSource ledger rows: {src_count}")
