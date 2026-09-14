import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scripts.loc_scraper.schema import open_db
conn = open_db()
sl = conn.execute("SELECT COUNT(*) FROM source_ledger").fetchone()[0]
gm = conn.execute("SELECT state, COUNT(*) FROM game_manifest WHERE tier='T0' GROUP BY state").fetchall()
print(f"source_ledger rows: {sl}")
print("T0 game_manifest:")
for r in gm: print(" ", r)
