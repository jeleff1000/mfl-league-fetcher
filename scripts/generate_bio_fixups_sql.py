"""Regenerate ops_cache_fixups.sql player_bio section from Fly's authoritative bio.

The local ops_cache predates the 2026-04-28 player_bio yahoo_player_id corrections
(859 + collision repairs). The resolver in core/db_utils.py:resolve_nfl_ids reads
bio from the local cache, so without a fixup the stale bio overrides Yahoo's
correct yahoo->NFL mapping (e.g. Conklin yahoo 31127 stays attached to Izzo's
NFL_id 00-0034439 instead of 00-0034270).

This script pulls every NFL_player_id where Fly's current bio.yahoo_player_id
differs from the backup snapshot, then writes idempotent UPDATE statements
into ops_cache_fixups.sql so workers can replay them post-cache-restore.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.db_reader import get_reader  # noqa: E402


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
    if not os.environ.get("DATABASE_BACKEND"):
        os.environ["DATABASE_BACKEND"] = "fly"

    reader = get_reader()
    df = reader.query_df(
        """
        SELECT cur.NFL_player_id,
               CAST(cur.yahoo_player_id AS BIGINT) AS yahoo_id
        FROM public.player_bio_yahoo_id_backup_20260428 bk
        JOIN nfl_historical.player_bio cur ON cur.NFL_player_id = bk.NFL_player_id
        WHERE bk.yahoo_player_id IS DISTINCT FROM cur.yahoo_player_id
        ORDER BY cur.NFL_player_id
        """,
        database="___ops",
    )
    print(f"[fly] {len(df):,} rows differ between bio backup and current")

    # Split into:
    #   fixes_set    : current yahoo_id is non-null -> SET on this NFL_player_id
    #   fixes_clear  : current yahoo_id IS NULL -> ensure cache row has NULL too
    fixes_set = []
    fixes_clear = []
    for _, row in df.iterrows():
        nfl_id = str(row["NFL_player_id"]).replace("'", "''")
        yid_raw = row["yahoo_id"]
        if yid_raw is None or (isinstance(yid_raw, float) and yid_raw != yid_raw):  # NaN check
            fixes_clear.append(nfl_id)
        else:
            fixes_set.append((nfl_id, int(yid_raw)))

    print(f"  -> {len(fixes_set):,} NFL_ids need yahoo_id set, {len(fixes_clear):,} need yahoo_id cleared")

    out_path = ROOT / "fantasy_football_data_scripts" / "ops_cache_fixups.sql"
    existing = out_path.read_text(encoding="utf-8")

    # Strip the previous bio section if present (idempotent regenerate)
    marker = "\n-- ---------------------------------------------------------------\n-- 2026-04-28: Backfill player_bio.yahoo_player_id corrections."
    if marker in existing:
        existing = existing.split(marker)[0].rstrip() + "\n"

    lines: list[str] = []
    lines.append("\n-- ---------------------------------------------------------------")
    lines.append("-- 2026-04-28: Backfill player_bio.yahoo_player_id corrections.")
    lines.append(f"-- {len(df):,} NFL_ids: {len(fixes_set):,} SET + {len(fixes_clear):,} CLEAR.")
    lines.append("-- Already applied to Fly via scripts/fix_player_bio_yahoo_id.py +")
    lines.append("-- scripts/fix_player_bio_collision_repair.py. Local ops_cache predates")
    lines.append("-- these corrections; resolver in core/db_utils.py:resolve_nfl_ids reads")
    lines.append("-- bio from the local cache, so without these fixups the stale bio")
    lines.append("-- overrides correct yahoo->NFL mapping (e.g. Conklin yahoo 31127).")
    lines.append("-- Fully idempotent: re-running on already-fixed rows is a no-op.")
    lines.append("--")
    lines.append("-- Strategy:")
    lines.append("--   Phase A: clear yahoo_id from rows where it's now misplaced.")
    lines.append("--   Phase B: set yahoo_id on the NFL_player_id where Fly says it belongs.")
    lines.append("-- Both phases driven by a temp staging table to avoid 1k+ statements.\n")

    lines.append("CREATE OR REPLACE TEMP TABLE __bio_target (yahoo_id BIGINT, nfl_id VARCHAR);")
    lines.append("DELETE FROM __bio_target;")

    CHUNK = 200
    if fixes_set:
        for i in range(0, len(fixes_set), CHUNK):
            batch = fixes_set[i : i + CHUNK]
            values = ", ".join(f"({y}, '{n}')" for n, y in batch)
            lines.append(f"INSERT INTO __bio_target (yahoo_id, nfl_id) VALUES {values};")

    # Phase A: NULL out misplaced yahoo_ids
    lines.append(
        """
-- Phase A: clear yahoo_player_id where it's currently on the WRONG NFL row.
UPDATE nfl_historical.player_bio
SET yahoo_player_id = NULL
WHERE NFL_player_id IN (
  SELECT b.NFL_player_id
  FROM nfl_historical.player_bio b
  JOIN __bio_target t ON CAST(b.yahoo_player_id AS BIGINT) = t.yahoo_id
  WHERE b.NFL_player_id != t.nfl_id
);
""".strip()
    )

    # Phase B: SET yahoo_id on the right NFL row
    lines.append(
        """
-- Phase B: set yahoo_player_id on the NFL_player_id where Fly says it belongs.
UPDATE nfl_historical.player_bio b
SET yahoo_player_id = CAST(t.yahoo_id AS DOUBLE)
FROM __bio_target t
WHERE b.NFL_player_id = t.nfl_id
  AND (b.yahoo_player_id IS NULL OR CAST(b.yahoo_player_id AS BIGINT) != t.yahoo_id);
""".strip()
    )

    # Phase C: explicit clears (rows where Fly's current yahoo_id is NULL)
    if fixes_clear:
        lines.append("\n-- Phase C: explicitly clear yahoo_id from NFL_ids where Fly says it should be NULL.")
        # Chunk the IN list
        for i in range(0, len(fixes_clear), CHUNK):
            batch = fixes_clear[i : i + CHUNK]
            in_list = ", ".join(f"'{n}'" for n in batch)
            lines.append(
                "UPDATE nfl_historical.player_bio "
                f"SET yahoo_player_id = NULL "
                f"WHERE NFL_player_id IN ({in_list}) AND yahoo_player_id IS NOT NULL;"
            )

    lines.append("\nDROP TABLE IF EXISTS __bio_target;")

    out_path.write_text(existing + "\n" + "\n".join(lines) + "\n", encoding="utf-8")
    print(f"[ok] Rewrote {out_path} with {len(fixes_set):,} SET + {len(fixes_clear):,} CLEAR fixups")


if __name__ == "__main__":
    main()
