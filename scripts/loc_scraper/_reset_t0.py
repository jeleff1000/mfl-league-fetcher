"""Reset T0 games back to NEW so discovery can re-run with the fixed state machine."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db

conn = open_db()

# Count before
before = conn.execute(
    "SELECT state, COUNT(*) FROM game_manifest WHERE tier='T0' GROUP BY state"
).fetchall()
print("Before reset:")
for row in before: print(" ", row)

# Reset T0 games — keep current_level=0 and state=NEW so discovery restarts
# Delete any T0 source_ledger entries from the bad run (PFA_INDEX only had 12)
src_deleted = conn.execute("""
    DELETE FROM source_ledger
    WHERE game_key IN (SELECT game_key FROM game_manifest WHERE tier='T0')
""").rowcount
print(f"\nDeleted {src_deleted} source_ledger rows")

conn.execute("""
    UPDATE game_manifest
    SET state='NEW', current_level=0, sources_found=0,
        last_updated=CURRENT_TIMESTAMP
    WHERE tier='T0'
""")

after = conn.execute(
    "SELECT state, COUNT(*) FROM game_manifest WHERE tier='T0' GROUP BY state"
).fetchall()
print("After reset:")
for row in after: print(" ", row)
print("\nReady for re-run.")
