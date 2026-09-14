#!/usr/bin/env python3
"""
Yahoo-NFL Player ID Mapping Cache

Persistent cache that maps yahoo_player_id -> NFL_player_id.
Stored in MotherDuck ___ops.public.yahoo_nfl_player_map.

This cache eliminates repeated 4-layer name matching for known players.
~99% cache hit rate after initial import.

Author: Fantasy Football Analytics Pipeline
"""

from __future__ import annotations

import os
from datetime import datetime

from multi_league.core.date_utils import get_current_nfl_season_year

import pandas as pd


# MotherDuck configuration
# Table is in ops.public schema - we connect to ops database directly
MOTHERDUCK_TABLE = "public.yahoo_nfl_player_map"


def get_motherduck_connection(token: str = None):
    """Get a connection to ___ops for player ID cache operations.

    Returns a DuckDB connection (MotherDuck) or None if unavailable.
    For Fly.io reads, callers should use get_reader() from db_reader module.
    This function is primarily used for write operations (cache persistence).
    """
    import duckdb

    token = token or os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        return None

    try:
        # Connect to the ops database directly
        conn = duckdb.connect(f"md:___ops?motherduck_token={token}")
        return conn
    except Exception as e:
        print(f"[CACHE] Warning: Could not connect to MotherDuck: {e}")
        return None


def ensure_table_exists(conn) -> bool:
    """Create the mapping table if it doesn't exist, and add any missing columns."""
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {MOTHERDUCK_TABLE} (
                yahoo_player_id VARCHAR NOT NULL PRIMARY KEY,
                NFL_player_id VARCHAR NOT NULL,
                match_layer INTEGER NOT NULL,
                match_confidence DOUBLE,
                yahoo_name VARCHAR,
                nfl_name VARCHAR,
                position VARCHAR,
                headshot_url VARCHAR,
                gsis_id VARCHAR,
                created_at TIMESTAMP DEFAULT now(),
                updated_at TIMESTAMP DEFAULT now(),
                last_verified_year INTEGER,
                is_manual_override BOOLEAN DEFAULT false
            )
        """)

        # Add gsis_id column if missing (for tables created before this change)
        try:
            conn.execute(f"""
                ALTER TABLE {MOTHERDUCK_TABLE}
                ADD COLUMN IF NOT EXISTS gsis_id VARCHAR
            """)
        except Exception:
            # Column may already exist, ignore
            pass

        return True
    except Exception as e:
        print(f"[CACHE] Warning: Could not create table: {e}")
        return False


def load_yahoo_nfl_mapping(token: str = None) -> dict[str, str]:
    """
    Load all yahoo_player_id -> NFL_player_id mappings.

    Uses the shared unified mapping (player_bio as primary source,
    legacy yahoo_nfl_player_map as fallback).

    Returns:
        Dict mapping yahoo_player_id (str) to NFL_player_id (str)
    """
    try:
        from multi_league.data_fetchers.shared.nfl_player_mapping import get_yahoo_to_nfl_map

        result = get_yahoo_to_nfl_map()
        if result:
            return result
    except Exception as e:
        print(f"[CACHE] Warning: Could not load unified mapping: {e}")

    # Fallback to direct read if unified mapping fails
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        try:
            from multi_league.core.db_reader import get_reader

            reader = get_reader()
            df = reader.query_df(
                f"""
                SELECT yahoo_player_id, NFL_player_id
                FROM {MOTHERDUCK_TABLE}
                """,
                database="___ops",
            )
            if not df.empty:
                return dict(zip(df["yahoo_player_id"].astype(str), df["NFL_player_id"]))
        except Exception as e:
            print(f"[CACHE] Warning: Fly.io fallback failed: {e}")
        return {}

    conn = get_motherduck_connection(token)
    if not conn:
        return {}

    try:
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'yahoo_nfl_player_map'
        """).fetchall()

        if not tables:
            conn.close()
            return {}

        df = conn.execute(f"""
            SELECT yahoo_player_id, NFL_player_id
            FROM {MOTHERDUCK_TABLE}
        """).fetchdf()

        conn.close()

        return dict(zip(df["yahoo_player_id"].astype(str), df["NFL_player_id"]))

    except Exception as e:
        print(f"[CACHE] Warning: Could not load mapping cache: {e}")
        try:
            conn.close()
        except Exception:
            pass
        return {}


def save_yahoo_nfl_mapping(mappings: list[tuple[str, str, int, float, str, str, str]], token: str = None) -> int:
    """
    Save new yahoo_player_id -> NFL_player_id mappings to cache.

    Does NOT overwrite rows where is_manual_override = true.

    Args:
        mappings: List of (yahoo_player_id, NFL_player_id, match_layer,
                          confidence, yahoo_name, nfl_name, position)
        token: MotherDuck token

    Returns:
        Number of mappings saved
    """
    if not mappings:
        return 0

    conn = get_motherduck_connection(token)
    if not conn:
        return 0

    try:
        # Ensure table exists
        if not ensure_table_exists(conn):
            conn.close()
            return 0

        # Prepare data
        df = pd.DataFrame(
            mappings,
            columns=[
                "yahoo_player_id",
                "NFL_player_id",
                "match_layer",
                "match_confidence",
                "yahoo_name",
                "nfl_name",
                "position",
            ],
        )
        df["yahoo_player_id"] = df["yahoo_player_id"].astype(str)
        df["updated_at"] = datetime.now()
        df["last_verified_year"] = get_current_nfl_season_year()

        conn.register("new_mappings", df)

        # Insert ONLY new mappings - never overwrite existing mappings
        # Once a player is mapped, that mapping should be permanent unless manually overridden
        # This prevents name collisions (e.g., Brandon Marshall WR vs LB) from corrupting data
        conn.execute(f"""
            INSERT INTO {MOTHERDUCK_TABLE}
                (yahoo_player_id, NFL_player_id, match_layer, match_confidence,
                 yahoo_name, nfl_name, position, updated_at, last_verified_year)
            SELECT yahoo_player_id, NFL_player_id, match_layer, match_confidence,
                   yahoo_name, nfl_name, position, updated_at, last_verified_year
            FROM new_mappings
            ON CONFLICT (yahoo_player_id) DO NOTHING
        """)

        conn.close()
        return len(df)

    except Exception as e:
        print(f"[CACHE] Warning: Could not save mappings: {e}")
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return 0


def get_unmapped_yahoo_ids(yahoo_ids: list[str], token: str = None) -> set:
    """
    Return the subset of yahoo_ids that are NOT in the cache.
    Useful for determining which players need matching.
    """
    cache = load_yahoo_nfl_mapping(token)
    yahoo_ids_str = {str(yid) for yid in yahoo_ids}
    return yahoo_ids_str - set(cache.keys())


def manually_override_mapping(yahoo_player_id: str, NFL_player_id: str, reason: str = None, token: str = None) -> bool:
    """
    Manually set a mapping that won't be overwritten by automated matching.

    Use this to correct known bad matches.
    """
    conn = get_motherduck_connection(token)
    if not conn:
        return False

    try:
        # Ensure table exists
        if not ensure_table_exists(conn):
            conn.close()
            return False

        conn.execute(
            f"""
            INSERT INTO {MOTHERDUCK_TABLE}
                (yahoo_player_id, NFL_player_id, match_layer, is_manual_override, updated_at)
            VALUES (?, ?, 0, true, now())
            ON CONFLICT (yahoo_player_id) DO UPDATE SET
                NFL_player_id = EXCLUDED.NFL_player_id,
                is_manual_override = true,
                updated_at = now()
        """,
            [str(yahoo_player_id), NFL_player_id],
        )

        conn.close()
        return True

    except Exception as e:
        print(f"[CACHE] Error setting manual override: {e}")
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return False


def backfill_from_player_fantasy(
    league_tables: list[str] = None, token: str = None, verbose: bool = True
) -> tuple[int, pd.DataFrame]:
    """
    Backfill the mapping cache from existing player_fantasy tables.

    Args:
        league_tables: List of fully-qualified table names (e.g., ["kmffl.public.player_fantasy"])
                      If None, will auto-discover leagues
        token: MotherDuck token
        verbose: Print progress

    Returns:
        Tuple of (count of mappings inserted, DataFrame of name mismatches for review)
    """
    conn = get_motherduck_connection(token)
    if not conn:
        return 0, pd.DataFrame()

    try:
        # Ensure table exists
        if not ensure_table_exists(conn):
            conn.close()
            return 0, pd.DataFrame()

        # Auto-discover league databases if not provided
        if league_tables is None:
            # MotherDuck: query md_databases() to list all databases
            try:
                databases = conn.execute("""
                    SELECT name as database_name FROM md_databases()
                    WHERE name NOT IN ('___ops', 'sample_data', 'system', 'temp', 'memory', 'my_db')
                """).fetchdf()
            except Exception:
                # Fallback: query information_schema for tables with player_fantasy
                databases = pd.DataFrame({"database_name": []})

            league_tables = []
            for db in databases["database_name"].tolist():
                try:
                    # Check if player_fantasy table exists by attempting a no-op query
                    conn.execute(f"SELECT 1 FROM {db}.public.player_fantasy LIMIT 0")
                    league_tables.append(f"{db}.public.player_fantasy")
                except Exception:  # noqa: broad-except
                    pass
            if verbose:
                print(f"[CACHE] Auto-discovered {len(league_tables)} league tables: {league_tables}")

        if not league_tables:
            print("[CACHE] No league tables found for backfill")
            conn.close()
            return 0, pd.DataFrame()

        # Build UNION query for all leagues (skip tables missing required columns)
        required_cols = {"yahoo_player_id", "NFL_player_id", "player", "position", "year"}
        union_parts = []
        skipped = []
        for table in league_tables:
            try:
                cols = {r[0] for r in conn.execute(f"SELECT column_name FROM (DESCRIBE {table})").fetchall()}
                if not required_cols.issubset(cols):
                    missing = required_cols - cols
                    skipped.append((table, missing))
                    continue
            except Exception:
                skipped.append((table, {"unknown"}))
                continue

            union_parts.append(f"""
                SELECT
                    CAST(yahoo_player_id AS VARCHAR) as yahoo_player_id,
                    NFL_player_id,
                    player as yahoo_name,
                    position,
                    year
                FROM {table}
                WHERE yahoo_player_id IS NOT NULL
                  AND NFL_player_id IS NOT NULL
                  AND NFL_player_id NOT LIKE 'YAHOO-%'
                  AND NFL_player_id NOT LIKE 'UNK-%'
            """)

        if skipped and verbose:
            print(f"[CACHE] Skipped {len(skipped)} tables missing required columns: {[s[0] for s in skipped]}")

        if not union_parts:
            print("[CACHE] No valid league tables found for backfill")
            conn.close()
            return 0, pd.DataFrame()

        union_query = " UNION ALL ".join(union_parts)

        # Get all existing mappings with name info for comparison
        if verbose:
            print("[CACHE] Extracting mappings from player_fantasy tables...")

        # First, get the aggregated data
        agg_query = f"""
            SELECT
                yahoo_player_id,
                NFL_player_id,
                yahoo_name,
                position,
                MAX(year) as last_verified_year,
                COUNT(*) as occurrence_count
            FROM ({union_query})
            GROUP BY yahoo_player_id, NFL_player_id, yahoo_name, position
        """

        mappings_df = conn.execute(agg_query).fetchdf()

        if verbose:
            print(f"[CACHE] Found {len(mappings_df):,} unique yahoo_player_id -> NFL_player_id mappings")

        # Check for conflicting mappings (same yahoo_id -> different NFL_id)
        conflict_check = mappings_df.groupby("yahoo_player_id")["NFL_player_id"].nunique()
        conflicts = conflict_check[conflict_check > 1]

        if len(conflicts) > 0 and verbose:
            print(f"[CACHE] Warning: {len(conflicts)} yahoo_player_ids map to multiple NFL_player_ids")
            # Get details on conflicts
            conflict_details = mappings_df[mappings_df["yahoo_player_id"].isin(conflicts.index)]
            print(conflict_details.head(10).to_string())

        # For conflicts, keep the one with highest occurrence count
        mappings_df = mappings_df.sort_values(["yahoo_player_id", "occurrence_count"], ascending=[True, False])
        mappings_df = mappings_df.drop_duplicates(subset=["yahoo_player_id"], keep="first")

        # Now we need to get NFL names to compare for mismatches
        # Query super table for NFL names (nfl_historical is in same ops database)
        nfl_names_query = """
            SELECT DISTINCT NFL_player_id, player as nfl_name
            FROM nfl_historical.nfl_player_stats_all
            WHERE NFL_player_id IS NOT NULL
        """

        try:
            nfl_names_df = conn.execute(nfl_names_query).fetchdf()
            mappings_df = mappings_df.merge(nfl_names_df, on="NFL_player_id", how="left")
        except Exception as e:
            if verbose:
                print(f"[CACHE] Could not fetch NFL names for comparison: {e}")
            mappings_df["nfl_name"] = None

        # Find name mismatches (where yahoo_name != nfl_name after normalization)
        def normalize_for_compare(name):
            """Simple normalization for comparison."""
            if pd.isna(name):
                return ""
            return str(name).lower().strip().replace(".", "").replace("'", "").replace("-", " ")

        mappings_df["yahoo_name_norm"] = mappings_df["yahoo_name"].apply(normalize_for_compare)
        mappings_df["nfl_name_norm"] = mappings_df["nfl_name"].apply(normalize_for_compare)

        # Identify mismatches (names don't match after normalization)
        mismatches = mappings_df[
            (mappings_df["yahoo_name_norm"] != mappings_df["nfl_name_norm"])
            & (mappings_df["nfl_name_norm"] != "")  # Exclude rows where we couldn't get NFL name
        ][["yahoo_player_id", "NFL_player_id", "yahoo_name", "nfl_name", "position"]].copy()

        if verbose and len(mismatches) > 0:
            print(f"\n[CACHE] Found {len(mismatches)} name mismatches to review:")
            print(mismatches.to_string())

        # Snapshot layer distribution before backfill for comparison
        before_layers = {}
        try:
            before_df = conn.execute(f"""
                SELECT match_layer, COUNT(*) as cnt
                FROM {MOTHERDUCK_TABLE}
                GROUP BY match_layer
            """).fetchdf()
            before_layers = dict(zip(before_df["match_layer"], before_df["cnt"]))
            before_total = sum(before_layers.values())
        except Exception:
            before_total = 0

        # Prepare data for insert
        insert_df = mappings_df[
            ["yahoo_player_id", "NFL_player_id", "yahoo_name", "nfl_name", "position", "last_verified_year"]
        ].copy()
        insert_df["match_layer"] = 1  # Verified via player_fantasy = Layer 1
        insert_df["match_confidence"] = 100.0
        insert_df["updated_at"] = datetime.now()

        conn.register("backfill_mappings", insert_df)

        # Insert new mappings, and promote existing Layer 2/3 entries to Layer 1
        # since they're now verified in actual league data (player_fantasy).
        # Also refresh yahoo_name/nfl_name/position with latest data.
        conn.execute(f"""
            INSERT INTO {MOTHERDUCK_TABLE}
                (yahoo_player_id, NFL_player_id, match_layer, match_confidence,
                 yahoo_name, nfl_name, position, updated_at, last_verified_year)
            SELECT yahoo_player_id, NFL_player_id, match_layer, match_confidence,
                   yahoo_name, nfl_name, position, updated_at, last_verified_year
            FROM backfill_mappings
            ON CONFLICT (yahoo_player_id) DO UPDATE SET
                last_verified_year = EXCLUDED.last_verified_year,
                updated_at = EXCLUDED.updated_at,
                match_layer = CASE
                    WHEN {MOTHERDUCK_TABLE}.match_layer > EXCLUDED.match_layer
                    THEN EXCLUDED.match_layer
                    ELSE {MOTHERDUCK_TABLE}.match_layer
                END,
                match_confidence = CASE
                    WHEN {MOTHERDUCK_TABLE}.match_layer > EXCLUDED.match_layer
                    THEN EXCLUDED.match_confidence
                    ELSE {MOTHERDUCK_TABLE}.match_confidence
                END,
                yahoo_name = COALESCE(EXCLUDED.yahoo_name, {MOTHERDUCK_TABLE}.yahoo_name),
                nfl_name = COALESCE(EXCLUDED.nfl_name, {MOTHERDUCK_TABLE}.nfl_name),
                position = COALESCE(EXCLUDED.position, {MOTHERDUCK_TABLE}.position)
            WHERE NOT {MOTHERDUCK_TABLE}.is_manual_override
        """)

        count = len(insert_df)

        if verbose:
            # Show layer distribution after backfill
            try:
                after_df = conn.execute(f"""
                    SELECT match_layer, COUNT(*) as cnt
                    FROM {MOTHERDUCK_TABLE}
                    GROUP BY match_layer
                """).fetchdf()
                after_layers = dict(zip(after_df["match_layer"], after_df["cnt"]))
                after_total = sum(after_layers.values())

                new_added = after_total - before_total
                promoted = 0
                for layer in [2, 3, 4]:
                    before_cnt = before_layers.get(layer, 0)
                    after_cnt = after_layers.get(layer, 0)
                    promoted += max(0, before_cnt - after_cnt)

                print(f"[CACHE] Backfilled {count:,} mappings to {MOTHERDUCK_TABLE}")
                print(f"[CACHE]   New mappings added: {new_added:,}")
                if promoted > 0:
                    print(f"[CACHE]   Promoted to Layer 1: {promoted:,} (were Layer 2/3)")
                print(f"[CACHE]   Layer distribution: {dict(sorted(after_layers.items()))}")
            except Exception:
                print(f"[CACHE] Backfilled {count:,} mappings to {MOTHERDUCK_TABLE}")

        conn.close()
        return count, mismatches

    except Exception as e:
        print(f"[CACHE] Error during backfill: {e}")
        import traceback

        traceback.print_exc()
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return 0, pd.DataFrame()


def get_cache_stats(token: str = None) -> dict:
    """Get statistics about the mapping cache."""
    conn = get_motherduck_connection(token)
    if not conn:
        return {}

    try:
        stats = conn.execute(f"""
            SELECT
                COUNT(*) as total_mappings,
                COUNT(CASE WHEN gsis_id IS NOT NULL THEN 1 END) as with_gsis_id,
                COUNT(CASE WHEN is_manual_override THEN 1 END) as manual_overrides,
                COUNT(CASE WHEN match_layer = 1 THEN 1 END) as layer1_exact,
                COUNT(CASE WHEN match_layer = 2 THEN 1 END) as layer2_cross_pos,
                COUNT(CASE WHEN match_layer = 3 THEN 1 END) as layer3_last_name,
                COUNT(CASE WHEN match_layer = 4 THEN 1 END) as layer4_fuzzy,
                MIN(last_verified_year) as oldest_year,
                MAX(last_verified_year) as newest_year,
                ROUND(100.0 * COUNT(CASE WHEN gsis_id IS NOT NULL THEN 1 END) / COUNT(*), 1) as gsis_pct
            FROM {MOTHERDUCK_TABLE}
        """).fetchdf()

        conn.close()

        if len(stats) > 0:
            return stats.iloc[0].to_dict()
        return {}

    except Exception as e:
        print(f"[CACHE] Error getting stats: {e}")
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return {}


def backfill_yahoo_headshots(token: str = None, verbose: bool = True) -> int:
    """
    Backfill headshot_url from super_table for existing Yahoo mappings.

    Args:
        token: MotherDuck token
        verbose: Print progress

    Returns:
        Number of headshots backfilled
    """
    conn = get_motherduck_connection(token)
    if not conn:
        return 0

    try:
        # Add column if missing (for older tables)
        try:
            conn.execute(f"""
                ALTER TABLE {MOTHERDUCK_TABLE}
                ADD COLUMN IF NOT EXISTS headshot_url VARCHAR
            """)
        except Exception as e:
            # Column may already exist
            if verbose:
                print(f"[CACHE] Note: headshot_url column check: {e}")

        # Count rows that need headshots
        before_count = conn.execute(f"""
            SELECT COUNT(*) FROM {MOTHERDUCK_TABLE}
            WHERE headshot_url IS NULL
        """).fetchone()[0]

        if verbose:
            print(f"[CACHE] Found {before_count:,} Yahoo mappings without headshot_url")

        if before_count == 0:
            conn.close()
            return 0

        # Update from super_table
        conn.execute(f"""
            UPDATE {MOTHERDUCK_TABLE} m
            SET headshot_url = s.headshot_url
            FROM (
                SELECT DISTINCT NFL_player_id, FIRST(headshot_url) as headshot_url
                FROM nfl_historical.nfl_player_stats_all
                WHERE NFL_player_id IS NOT NULL AND headshot_url IS NOT NULL
                GROUP BY NFL_player_id
            ) s
            WHERE m.NFL_player_id = s.NFL_player_id
            AND m.headshot_url IS NULL
        """)

        # Count rows that now have headshots
        after_count = conn.execute(f"""
            SELECT COUNT(*) FROM {MOTHERDUCK_TABLE}
            WHERE headshot_url IS NULL
        """).fetchone()[0]

        updated = before_count - after_count

        if verbose:
            print(f"[CACHE] Backfilled {updated:,} headshots ({after_count:,} still missing)")

        conn.close()
        return updated

    except Exception as e:
        print(f"[CACHE] Error backfilling headshots: {e}")
        import traceback

        traceback.print_exc()
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return 0


def update_gsis_ids(gsis_mappings: list[tuple[str, str]], token: str = None, verbose: bool = True) -> int:
    """
    Update GSIS IDs for existing Yahoo mappings.

    This function adds GSIS IDs to existing yahoo_nfl_player_map entries.
    Only updates rows where gsis_id is currently NULL.

    Args:
        gsis_mappings: List of (yahoo_player_id, gsis_id) tuples
        token: MotherDuck token
        verbose: Print progress

    Returns:
        Number of GSIS IDs updated
    """
    if not gsis_mappings:
        return 0

    conn = get_motherduck_connection(token)
    if not conn:
        return 0

    try:
        # Ensure table has gsis_id column
        if not ensure_table_exists(conn):
            conn.close()
            return 0

        # Count rows that need GSIS IDs before
        before_count = conn.execute(f"""
            SELECT COUNT(*) FROM {MOTHERDUCK_TABLE}
            WHERE gsis_id IS NULL
        """).fetchone()[0]

        if verbose:
            print(f"[CACHE] {before_count:,} mappings currently missing gsis_id")

        # Prepare update data
        df = pd.DataFrame(gsis_mappings, columns=["yahoo_player_id", "gsis_id"])
        df["yahoo_player_id"] = df["yahoo_player_id"].astype(str)
        df["gsis_id"] = df["gsis_id"].astype(str)

        conn.register("gsis_updates", df)

        # Update existing rows with GSIS IDs (only where currently NULL)
        conn.execute(f"""
            UPDATE {MOTHERDUCK_TABLE} m
            SET gsis_id = g.gsis_id,
                updated_at = now()
            FROM gsis_updates g
            WHERE m.yahoo_player_id = g.yahoo_player_id
            AND m.gsis_id IS NULL
        """)

        # Count rows that still need GSIS IDs
        after_count = conn.execute(f"""
            SELECT COUNT(*) FROM {MOTHERDUCK_TABLE}
            WHERE gsis_id IS NULL
        """).fetchone()[0]

        updated = before_count - after_count

        if verbose:
            print(f"[CACHE] Updated {updated:,} GSIS IDs ({after_count:,} still missing)")

        conn.close()
        return updated

    except Exception as e:
        print(f"[CACHE] Error updating GSIS IDs: {e}")
        import traceback

        traceback.print_exc()
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return 0


def load_yahoo_nfl_mapping_full(token: str = None) -> pd.DataFrame:
    """
    Load all mappings with full details (including GSIS ID).

    Returns:
        DataFrame with all mapping columns
    """
    conn = get_motherduck_connection(token)
    if not conn:
        return pd.DataFrame()

    try:
        # Check if table exists
        tables = conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = 'yahoo_nfl_player_map'
        """).fetchall()

        if not tables:
            conn.close()
            return pd.DataFrame()

        df = conn.execute(f"""
            SELECT
                yahoo_player_id,
                NFL_player_id,
                gsis_id,
                yahoo_name,
                nfl_name,
                position,
                headshot_url,
                match_layer,
                match_confidence,
                last_verified_year,
                is_manual_override
            FROM {MOTHERDUCK_TABLE}
        """).fetchdf()

        conn.close()
        return df

    except Exception as e:
        print(f"[CACHE] Warning: Could not load full mapping cache: {e}")
        try:
            conn.close()
        except Exception:  # noqa: broad-except
            pass
        return pd.DataFrame()


# CLI for manual operations
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Yahoo-NFL Player ID Mapping Cache")
    parser.add_argument("--backfill", action="store_true", help="Backfill cache from existing player_fantasy tables")
    parser.add_argument("--backfill-headshots", action="store_true", help="Backfill headshot URLs from super_table")
    parser.add_argument("--backfill-gsis", action="store_true", help="Backfill GSIS IDs from Sleeper bridge")
    parser.add_argument("--stats", action="store_true", help="Show cache statistics")
    parser.add_argument("--override", nargs=2, metavar=("YAHOO_ID", "NFL_ID"), help="Manually override a mapping")
    args = parser.parse_args()

    if args.stats:
        stats = get_cache_stats()
        if stats:
            print("\n=== Cache Statistics ===")
            for k, v in stats.items():
                print(f"  {k}: {v}")
        else:
            print("Could not get cache stats (table may not exist yet)")

    elif args.backfill:
        count, mismatches = backfill_from_player_fantasy(verbose=True)
        print(f"\nBackfill complete: {count} mappings")
        if len(mismatches) > 0:
            print(f"\n{len(mismatches)} name mismatches found - review above")

    elif args.backfill_headshots:
        count = backfill_yahoo_headshots(verbose=True)
        print(f"\nHeadshot backfill complete: {count} headshots added")

    elif args.backfill_gsis:
        print("To backfill GSIS IDs, run the dedicated script:")
        print("  python -m maintenance.backfill_yahoo_gsis")
        print("\nThis will extract yahoo_id + gsis_id pairs from Sleeper API")
        print("and update yahoo_nfl_player_map with the GSIS IDs.")

    elif args.override:
        yahoo_id, nfl_id = args.override
        if manually_override_mapping(yahoo_id, nfl_id):
            print(f"Successfully set manual override: {yahoo_id} -> {nfl_id}")
        else:
            print("Failed to set manual override")

    else:
        # Default: show cache size
        cache = load_yahoo_nfl_mapping()
        print(f"Cache contains {len(cache):,} mappings")
