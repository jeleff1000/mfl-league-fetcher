"""Fix ___ops.nfl_historical.player_bio.yahoo_player_id from yahoo_nfl_player_map cache.

Background
----------
The cache (___ops.public.yahoo_nfl_player_map) was re-synced from Yahoo's API
at some point and reflects current Yahoo (yahoo_id, NFL_player_id) pairs.
player_bio.yahoo_player_id was never re-synced after the 2026-04-28 1:1
invariant fix, so ~659 rows assign yahoo_ids to retired pre-internet players
(e.g. yahoo 31127 maps to Ryan Izzo in bio but to Tyler Conklin in cache and
in Yahoo's API today).

Random sampling (20 rows) of cache-vs-bio conflicts confirmed the cache is
correct in every case. The import resolver (multi_league/data_fetchers/
shared/nfl_player_mapping.py) reads player_bio as authoritative, so every
Yahoo reimport reproduces the wrong mapping. Fixing player_bio is the
prerequisite for idempotent reimports.

Procedure
---------
1. Snapshot player_bio.yahoo_player_id pre-fix (audit trail CSV + a SQL
   backup table on Fly).
2. Identify clean cache rows (yahoo_name == nfl_name, no .0 suffix).
3. UPDATE phase A: clear bio.yahoo_player_id from rows where the current
   yahoo_id is wrong per cache.
4. UPDATE phase B: set bio.yahoo_player_id from cache where bio currently
   has NULL or a different value than cache.
5. Re-run the diff to verify zero conflicts remain.

Read path: fly_query (MCP) — used here via the same MotherDuck fallback in
db_reader. Write path: FlyWriter via /query-rw.

This script is a one-shot. After it ships, future reimports will read the
corrected player_bio and resolve correctly.
"""

from __future__ import annotations

import csv
import os
import sys
from datetime import date
from pathlib import Path

# Ensure repo root + fantasy_football_data_scripts on path
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.db_reader import get_reader  # noqa: E402
from multi_league.core.fly_writer import FlyWriter  # noqa: E402

DRY_RUN = "--dry-run" in sys.argv
APPLY = "--apply" in sys.argv

if not DRY_RUN and not APPLY:
    print("Usage: fix_player_bio_yahoo_id.py [--dry-run | --apply]")
    sys.exit(2)


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


def fetch_diff(reader):
    """Return (corrections_df, summary_dict).

    corrections_df has columns:
        yahoo_player_id : str (clean integer string)
        correct_nfl_id  : str (per cache)
        bio_current_nfl_id_with_this_yahoo : str | None
        bio_current_yahoo_on_correct_row : str | None
        action : 'set_only' | 'swap_clear_then_set' | 'set_with_clear'
    """
    sql = """
    WITH cache AS (
      -- Clean cache rows: yahoo_name matches nfl_name, no .0 float-string suffix,
      -- match_layer in (0, 1) to exclude fuzzy Layer 2/3 fallbacks. Also
      -- cross-validate cache_yahoo_name against bio.player for the cache_nfl_id
      -- so we don't trust a cache row where yahoo_name disagrees with bio's
      -- player name on the target NFL_id (catches Royce Freeman -> Janikowski-NFL_id
      -- style corruption: a newer player wrongly routed to an older player's NFL_id).
      SELECT ynm.yahoo_player_id, ynm.NFL_player_id, ynm.match_layer, ynm.last_verified_year
      FROM ___ops.public.yahoo_nfl_player_map ynm
      JOIN ___ops.nfl_historical.player_bio bio ON bio.NFL_player_id = ynm.NFL_player_id
      WHERE ynm.yahoo_player_id NOT LIKE '%.0'
        AND ynm.match_layer IN (0, 1)
        AND ynm.yahoo_name IS NOT NULL AND ynm.nfl_name IS NOT NULL AND bio.player IS NOT NULL
        AND LOWER(TRIM(ynm.yahoo_name)) = LOWER(TRIM(ynm.nfl_name))
        AND LOWER(TRIM(ynm.yahoo_name)) = LOWER(TRIM(bio.player))
    ),
    -- Pick exactly ONE yahoo_id per NFL_player_id (most-recently-verified, lowest match_layer)
    cache_per_nfl AS (
      SELECT yahoo_player_id, NFL_player_id AS correct_nfl_id FROM (
        SELECT yahoo_player_id, NFL_player_id, match_layer, last_verified_year,
               ROW_NUMBER() OVER (
                 PARTITION BY NFL_player_id
                 ORDER BY match_layer ASC, last_verified_year DESC NULLS LAST, yahoo_player_id ASC
               ) AS rk
        FROM cache
      ) WHERE rk = 1
    ),
    cache_unique AS (
      -- Each yahoo_id appears at most once with one NFL_id
      SELECT yahoo_player_id, ANY_VALUE(correct_nfl_id) AS correct_nfl_id
      FROM cache_per_nfl
      GROUP BY yahoo_player_id
      HAVING COUNT(DISTINCT correct_nfl_id) = 1
    ),
    bio AS (
      SELECT
        NFL_player_id,
        CAST(CAST(yahoo_player_id AS BIGINT) AS VARCHAR) AS yahoo_player_id_str,
        yahoo_player_id AS yahoo_player_id_raw
      FROM ___ops.nfl_historical.player_bio
    ),
    diff AS (
      SELECT
        c.yahoo_player_id,
        c.correct_nfl_id,
        -- Where (if anywhere) does bio currently have this yahoo_id?
        (SELECT bio.NFL_player_id FROM bio
         WHERE bio.yahoo_player_id_str = c.yahoo_player_id LIMIT 1)
            AS bio_current_nfl_id_with_this_yahoo,
        -- What does bio currently have on the correct row?
        (SELECT bio.yahoo_player_id_str FROM bio
         WHERE bio.NFL_player_id = c.correct_nfl_id LIMIT 1)
            AS bio_current_yahoo_on_correct_row
      FROM cache_unique c
    )
    SELECT *
    FROM diff
    WHERE
      -- Only include rows where bio is wrong:
      -- a) bio's correct row has wrong/missing yahoo_id, OR
      -- b) this yahoo_id is currently on the wrong row in bio
      bio_current_yahoo_on_correct_row IS DISTINCT FROM yahoo_player_id
      OR (bio_current_nfl_id_with_this_yahoo IS NOT NULL
          AND bio_current_nfl_id_with_this_yahoo != correct_nfl_id)
    """
    df = reader.query_df(sql, database="___ops")

    # Skip rows where the correct NFL_player_id doesn't exist in bio (orphan)
    if df.empty:
        skipped_no_bio_row = 0
    else:
        bio_nfl_ids = set(
            reader.query_df(
                "SELECT NFL_player_id FROM ___ops.nfl_historical.player_bio",
                database="___ops",
            )["NFL_player_id"].astype(str)
        )
        before = len(df)
        df = df[df["correct_nfl_id"].astype(str).isin(bio_nfl_ids)].reset_index(drop=True)
        skipped_no_bio_row = before - len(df)

    summary = {
        "total_corrections": len(df),
        "skipped_no_bio_row_for_correct_nfl_id": skipped_no_bio_row,
    }
    return df, summary


def write_csv(df, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, quoting=csv.QUOTE_MINIMAL)
    print(f"[audit] Wrote {len(df):,} corrections to {path}")


def apply_corrections(df, writer: FlyWriter):
    """Apply in two phases: clear then set."""
    if df.empty:
        print("[apply] No corrections to apply.")
        return

    # Backup table
    backup_table = f"___ops.public.player_bio_yahoo_id_backup_{date.today().isoformat().replace('-', '')}"
    print(f"[backup] Creating backup table {backup_table}")
    writer.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {backup_table} AS
        SELECT NFL_player_id, yahoo_player_id, player, nfl_position
        FROM ___ops.nfl_historical.player_bio
        """,
        database="___ops",
    )
    backup_count = writer.execute(f"SELECT COUNT(*) AS n FROM {backup_table}", database="___ops")
    print(f"[backup] Backed up {backup_count[0]['n']:,} rows")

    # Build the corrections set as a temp staging table
    # Cache by inserting via VALUES — DuckDB on Fly should accept big VALUES
    yids = [str(y).strip() for y in df["yahoo_player_id"].tolist()]
    nflids = [str(n).strip() for n in df["correct_nfl_id"].tolist()]
    values_rows = ", ".join(
        f"('{y}', '{n}')"
        for y, n in zip(yids, nflids)
        if "'" not in y and "'" not in n  # defensive
    )
    if not values_rows:
        print("[apply] No valid VALUES rows — abort")
        return

    print("[apply] Creating staging table __corrections")
    writer.execute("DROP TABLE IF EXISTS __corrections", database="___ops")
    writer.execute(
        """
        CREATE TABLE __corrections (yahoo_player_id VARCHAR, correct_nfl_id VARCHAR);
        """,
        database="___ops",
    )
    # Chunk inserts to avoid request size limits
    CHUNK = 200
    pairs = list(zip(yids, nflids))
    for i in range(0, len(pairs), CHUNK):
        chunk = pairs[i : i + CHUNK]
        rows_sql = ", ".join(f"('{y}', '{n}')" for y, n in chunk)
        writer.execute(
            f"INSERT INTO __corrections (yahoo_player_id, correct_nfl_id) VALUES {rows_sql}",
            database="___ops",
        )
    inserted = writer.execute("SELECT COUNT(*) AS n FROM __corrections", database="___ops")
    print(f"[apply] Staged {inserted[0]['n']:,} corrections")

    # Phase A: NULL out wrong yahoo_player_ids
    # A "wrong" row is one whose current yahoo_id appears in __corrections
    # but whose NFL_player_id is NOT the correct_nfl_id for that yahoo_id.
    print("[apply] Phase A: clear wrong yahoo_player_ids")
    res_a = writer.execute(
        """
        UPDATE ___ops.nfl_historical.player_bio
        SET yahoo_player_id = NULL
        WHERE NFL_player_id IN (
          SELECT b.NFL_player_id
          FROM ___ops.nfl_historical.player_bio b
          JOIN __corrections c
            ON CAST(CAST(b.yahoo_player_id AS BIGINT) AS VARCHAR) = c.yahoo_player_id
          WHERE b.NFL_player_id != c.correct_nfl_id
        )
        """,
        database="___ops",
    )
    print(f"[apply] Phase A result: {res_a}")

    # Phase B: SET correct yahoo_player_ids on the right NFL rows
    print("[apply] Phase B: set correct yahoo_player_ids")
    res_b = writer.execute(
        """
        UPDATE ___ops.nfl_historical.player_bio AS b
        SET yahoo_player_id = CAST(c.yahoo_player_id AS DOUBLE)
        FROM __corrections c
        WHERE b.NFL_player_id = c.correct_nfl_id
          AND (b.yahoo_player_id IS NULL
               OR CAST(CAST(b.yahoo_player_id AS BIGINT) AS VARCHAR) != c.yahoo_player_id)
        """,
        database="___ops",
    )
    print(f"[apply] Phase B result: {res_b}")

    # Cleanup
    writer.execute("DROP TABLE IF EXISTS __corrections", database="___ops")
    print("[apply] Done.")


def verify(reader):
    """Re-run the diff after the fix; should be ~0."""
    sql = """
    WITH cache AS (
      SELECT yahoo_player_id, NFL_player_id AS correct_nfl_id
      FROM ___ops.public.yahoo_nfl_player_map
      WHERE yahoo_player_id NOT LIKE '%.0'
        AND yahoo_name IS NOT NULL AND nfl_name IS NOT NULL
        AND LOWER(TRIM(yahoo_name)) = LOWER(TRIM(nfl_name))
    ),
    cache_unique AS (
      SELECT yahoo_player_id, ANY_VALUE(correct_nfl_id) AS correct_nfl_id
      FROM cache GROUP BY 1 HAVING COUNT(DISTINCT correct_nfl_id) = 1
    ),
    bio AS (
      SELECT NFL_player_id, CAST(CAST(yahoo_player_id AS BIGINT) AS VARCHAR) AS yahoo_player_id_str
      FROM ___ops.nfl_historical.player_bio
    )
    SELECT
      COUNT(*) AS total,
      COUNT(*) FILTER (WHERE bio_current = c.yahoo_player_id) AS matches,
      COUNT(*) FILTER (WHERE bio_current IS DISTINCT FROM c.yahoo_player_id) AS still_wrong
    FROM cache_unique c
    LEFT JOIN (
      SELECT NFL_player_id, yahoo_player_id_str AS bio_current FROM bio
    ) b ON b.NFL_player_id = c.correct_nfl_id
    """
    df = reader.query_df(sql, database="___ops")
    print(f"[verify] {df.to_dict(orient='records')}")


def main():
    load_env()
    if not os.environ.get("DATABASE_BACKEND"):
        os.environ["DATABASE_BACKEND"] = "fly"

    reader = get_reader()
    print("[diff] Computing cache-vs-bio diff...")
    df, summary = fetch_diff(reader)
    print(f"[diff] Summary: {summary}")

    audit_path = ROOT / "scripts" / "_artifacts" / f"player_bio_yahoo_id_corrections_{date.today().isoformat()}.csv"
    write_csv(df, audit_path)

    if df.empty:
        print("[diff] No corrections needed.")
        return

    print(f"[diff] First 10 rows:\n{df.head(10).to_string()}")

    if DRY_RUN:
        print("[dry-run] Not applying. Re-run with --apply to commit.")
        return

    writer = FlyWriter()
    apply_corrections(df, writer)

    print("[verify] Running post-fix diff...")
    verify(reader)


if __name__ == "__main__":
    main()
