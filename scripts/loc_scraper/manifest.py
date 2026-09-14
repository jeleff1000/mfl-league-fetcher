"""
Build the game_manifest from v26 and the legacy completeness queue.

For every franchise-pair × year × week that exists in v26, evaluate coverage
and write a work-order row.  Already-existing rows are never overwritten —
run with --reset to rebuild from scratch.

Usage:
    python -m loc_scraper manifest [--reset] [--tier T0]
"""

from __future__ import annotations
import hashlib
import sys
from pathlib import Path

import duckdb

from .config import (
    V26_GLOB, OLD_COMPLETENESS_DB, OLD_QUEUE_CSV,
    game_tier, source_budget, SEARCH_LEVELS,
)
from .schema import open_db, GAME_TRANSITIONS


# ── Coverage thresholds ───────────────────────────────────────────────────────

# A team-game counts as "has passing" if any QB on that side had passing_yards > 0
PASS_COL  = "passing_yards"
RUSH_COL  = "rushing_yards"
RECV_COL  = "receiving_yards"
TACK_COL  = "def_tackles_solo"


def _latest_v26() -> Path:
    candidates = sorted(Path(r"D:\league-history-data\nfl\releases").glob(
        "*_v26/tables/nfl_player_stats_all.parquet"
    ))
    if not candidates:
        raise FileNotFoundError("No v26 parquet found")
    return candidates[-1]


def build_manifest(conn: duckdb.DuckDBPyConnection, reset: bool = False,
                   tier_filter: str | None = None) -> int:
    """
    Populate game_manifest from v26.  Returns number of rows inserted.
    """
    parquet = _latest_v26()
    print(f"Reading: {parquet}")

    tmp = duckdb.connect()
    tmp.execute("SET progress_bar_time=99999")
    tmp.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{parquet}')")

    if reset:
        if tier_filter:
            conn.execute("DELETE FROM game_manifest WHERE tier=?", [tier_filter])
        else:
            conn.execute("DELETE FROM game_manifest")

    # One row per franchise_a × franchise_b × year × week (player rows only)
    # Use nfl_franchise_number as the stable ID; fall back to nfl_team for display
    game_rows = tmp.execute("""
        WITH pairs AS (
            SELECT
                LEAST(nfl_franchise_number, opponent_nfl_franchise_number)  AS fran_a,
                GREATEST(nfl_franchise_number, opponent_nfl_franchise_number) AS fran_b,
                nfl_franchise_number,
                opponent_nfl_franchise_number,
                nfl_team,
                opponent_nfl_team,
                year,
                week,
                CASE WHEN week > 17 THEN 'POST' ELSE 'REG' END AS season_type,
                NULL::VARCHAR AS game_date,
                -- Coverage flags
                MAX(CASE WHEN position='QB' AND COALESCE(passing_yards,0)>0 THEN 1 ELSE 0 END)    AS has_pass,
                MAX(CASE WHEN position IN ('RB','FB') AND COALESCE(rushing_yards,0)>0 THEN 1 ELSE 0 END) AS has_rush,
                MAX(CASE WHEN position IN ('WR','TE','RB','FB') AND COALESCE(receiving_yards,0)>0 THEN 1 ELSE 0 END) AS has_recv,
                MAX(CASE WHEN COALESCE(def_tackles_solo,0)>0 THEN 1 ELSE 0 END)                   AS has_tack
            FROM st
            WHERE position != 'DEF'
              AND nfl_franchise_number IS NOT NULL
              AND opponent_nfl_franchise_number IS NOT NULL
              AND year IS NOT NULL AND week IS NOT NULL
            GROUP BY fran_a, fran_b, nfl_franchise_number, opponent_nfl_franchise_number,
                     nfl_team, opponent_nfl_team, year, week, season_type
        ),
        game_level AS (
            SELECT
                fran_a, fran_b, year, week, season_type,
                NULL::VARCHAR AS game_date,
                -- side-A coverage (team with lower franchise number)
                MAX(CASE WHEN nfl_franchise_number=fran_a THEN has_pass ELSE 0 END) AS has_pass_a,
                MAX(CASE WHEN nfl_franchise_number=fran_a THEN has_rush ELSE 0 END) AS has_rush_a,
                MAX(CASE WHEN nfl_franchise_number=fran_a THEN has_recv ELSE 0 END) AS has_recv_a,
                -- side-B coverage
                MAX(CASE WHEN nfl_franchise_number=fran_b THEN has_pass ELSE 0 END) AS has_pass_b,
                MAX(CASE WHEN nfl_franchise_number=fran_b THEN has_rush ELSE 0 END) AS has_rush_b,
                MAX(CASE WHEN nfl_franchise_number=fran_b THEN has_recv ELSE 0 END) AS has_recv_b,
                -- tackles (either side)
                MAX(has_tack) AS has_tackles,
                -- team codes for display
                FIRST(CASE WHEN nfl_franchise_number=fran_a THEN nfl_team END) AS team_a,
                FIRST(CASE WHEN nfl_franchise_number=fran_b THEN nfl_team END) AS team_b
            FROM pairs
            GROUP BY fran_a, fran_b, year, week, season_type
        )
        SELECT * FROM game_level
        ORDER BY year, week, fran_a, fran_b
    """).fetchall()

    print(f"  Found {len(game_rows):,} franchise-pair game slots in v26")

    inserted = 0
    skipped  = 0
    for row in game_rows:
        (fran_a, fran_b, year, week, season_type, game_date,
         has_pass_a, has_rush_a, has_recv_a,
         has_pass_b, has_rush_b, has_recv_b, has_tackles,
         team_a, team_b) = row

        tier = game_tier(year)
        if tier_filter and tier != tier_filter:
            continue

        # Only include games that are actually incomplete in some way
        # T0-T2: skill position gaps; T3: tackle gaps; T4: anything odd
        if tier in ("T0", "T1", "T2"):
            incomplete = not all([has_pass_a, has_rush_a, has_recv_a,
                                  has_pass_b, has_rush_b, has_recv_b])
        elif tier == "T3":
            incomplete = not has_tackles
        else:  # T4 – include only if any skill position gap
            incomplete = not all([has_pass_a, has_rush_a, has_recv_a,
                                  has_pass_b, has_rush_b, has_recv_b])

        if not incomplete:
            continue

        # Build a stable game_key
        fa = int(fran_a) if fran_a else 0
        fb = int(fran_b) if fran_b else 0
        game_key = f"{year}_w{int(week):02d}_{season_type}_{fa}_{fb}"

        # Check if already exists
        exists = conn.execute(
            "SELECT 1 FROM game_manifest WHERE game_key=?", [game_key]
        ).fetchone()
        if exists:
            skipped += 1
            continue

        # Build missing_csv
        missing = []
        if not has_pass_a or not has_pass_b: missing.append("passing")
        if not has_rush_a or not has_rush_b: missing.append("rushing")
        if not has_recv_a or not has_recv_b: missing.append("receiving")
        if not has_tackles and tier == "T3":  missing.append("tackles")

        conn.execute("""
            INSERT INTO game_manifest
            (game_key, year, week, season_type, franchise_a, franchise_b,
             team_a, team_b, game_date, tier,
             has_pass_a, has_rush_a, has_recv_a,
             has_pass_b, has_rush_b, has_recv_b, has_tackles,
             missing_csv, state, current_level)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'NEW',0)
        """, [
            game_key, int(year), int(week), season_type, fa, fb,
            team_a, team_b, game_date, tier,
            bool(has_pass_a), bool(has_rush_a), bool(has_recv_a),
            bool(has_pass_b), bool(has_rush_b), bool(has_recv_b), bool(has_tackles),
            ",".join(missing),
        ])
        inserted += 1

    print(f"  Inserted: {inserted:,}  Skipped (already exist): {skipped:,}")

    # ── Migrate useful entries from old completeness.duckdb ──────────────────
    if OLD_COMPLETENESS_DB.exists():
        _migrate_old_queue(conn)

    return inserted


def _migrate_old_queue(conn: duckdb.DuckDBPyConnection) -> None:
    """
    Pull LOC tile URLs from the old source_evidence_queue into source_ledger
    for any games that overlap with our new manifest.
    """
    try:
        old = duckdb.connect(str(OLD_COMPLETENESS_DB), read_only=True)
        old.execute("SET progress_bar_time=99999")
        # Detect column names (old schema varied)
        cols = [r[0] for r in old.execute(
            "DESCRIBE source_evidence_queue"
        ).fetchall()]
        tile_col = "loc_tile_url" if "loc_tile_url" in cols else "loc_url"
        json_col  = "loc_json_url" if "loc_json_url" in cols else (
                    "loc_json_url" if "loc_json_url" in cols else "NULL")
        rows = old.execute(f"""
            SELECT game_pair_key, game_date, year, week, team, opponent,
                   loc_page_url, {tile_col}, {json_col}, local_tile_path,
                   local_ocr_path
            FROM source_evidence_queue
            WHERE {tile_col} IS NOT NULL AND {tile_col} != ''
        """).fetchall()
        old.close()
    except Exception as e:
        print(f"  [warn] Could not read old completeness.duckdb: {e}")
        return

    migrated = 0
    for r in rows:
        (gpk, gdate, year, week, team, opp,
         page_url, tile_url, json_url, local_tile, local_ocr) = r
        # Find the matching game_manifest entry
        matches = conn.execute("""
            SELECT game_key FROM game_manifest
            WHERE year=? AND week=? AND (team_a=? OR team_b=? OR team_a=? OR team_b=?)
        """, [year, week, team, team, opp, opp]).fetchall()
        if not matches:
            continue
        game_key = matches[0][0]

        # Build source_key from LOC tile URL or fallback
        raw = (tile_url or page_url or f"{game_key}_migrated_{migrated}").encode()
        source_key = "migrated_" + hashlib.sha256(raw).hexdigest()[:12]

        exists = conn.execute(
            "SELECT 1 FROM source_ledger WHERE source_key=?", [source_key]
        ).fetchone()
        if exists:
            continue

        conn.execute("""
            INSERT INTO source_ledger
            (source_key, game_key, source_type, search_level,
             page_url, tile_url_sm, json_url,
             local_tile_sm, local_json, fetch_state)
            VALUES (?,?,'LOC_TILE',0,?,?,?,?,?,?)
        """, [
            source_key, game_key, page_url, tile_url, json_url,
            local_tile, local_ocr,
            "FETCHED" if local_tile else "CANDIDATE",
        ])
        migrated += 1

    if migrated:
        print(f"  Migrated {migrated} LOC source entries from old completeness.duckdb")


def print_summary(conn: duckdb.DuckDBPyConnection) -> None:
    from .schema import game_status_summary
    summary = game_status_summary(conn)
    total = summary.pop("_total", 0)
    print(f"\nGame manifest: {total:,} total incomplete games")
    for tier in sorted(summary):
        states = summary[tier]
        counts = "  ".join(f"{s}={n}" for s, n in sorted(states.items()))
        print(f"  {tier}: {counts}")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset",  action="store_true")
    ap.add_argument("--tier",   default=None)
    ap.add_argument("--summary",action="store_true")
    args = ap.parse_args()

    conn = open_db()
    if args.summary:
        print_summary(conn)
    else:
        build_manifest(conn, reset=args.reset, tier_filter=args.tier)
        print_summary(conn)
