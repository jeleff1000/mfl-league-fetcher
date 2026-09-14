#!/usr/bin/env python3
"""
SQL-based Player Matching

Matches Yahoo players to NFL players using SQL queries against MotherDuck.
This replaces the pandas-based multi-layer matching with efficient SQL queries.

Instead of loading millions of NFL rows into memory, we:
1. Send unmatched player names to MotherDuck
2. Use SQL JOINs to find matches
3. Return only the matched results

Matching Layers (executed in SQL):
1. Exact normalized name + position + year + week
2. Normalized name only (cross-position) with 1:1 constraint

Author: Fantasy Football Analytics Pipeline
"""

from __future__ import annotations

import os
import re
import unicodedata
from datetime import datetime

import pandas as pd


def log(msg: str):
    """Print timestamped log message."""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] [SQL-MATCH] {msg}")


def normalize_name_sql(name: str) -> str:
    """
    Normalize player name for SQL matching.
    Must match the normalization used in the super_table.
    """
    if not name:
        return ""

    # Lowercase
    name = name.lower()

    # Remove accents
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")

    # Remove suffixes
    suffixes = [" iii", " iv", " ii", " jr", " sr", " v", ".", ","]
    for suffix in suffixes:
        name = name.replace(suffix, "")

    # Remove non-alphanumeric except spaces
    name = re.sub(r"[^a-z\s]", "", name)

    # Collapse whitespace
    name = " ".join(name.split())

    return name.strip()


def get_motherduck_connection():
    """Get connection to ___ops database (MotherDuck or Fly.io-compatible)."""
    import duckdb

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    if backend == "fly":
        # For Fly.io, return None — callers should use get_reader() instead
        # This function is only called for write operations (cache saves)
        token = os.environ.get("MOTHERDUCK_TOKEN")
        if not token:
            raise RuntimeError("MOTHERDUCK_TOKEN not set (needed for cache writes)")
        return duckdb.connect(f"md:___ops?motherduck_token={token}")

    token = os.environ.get("MOTHERDUCK_TOKEN")
    if not token:
        raise RuntimeError("MOTHERDUCK_TOKEN not set")

    return duckdb.connect(f"md:___ops?motherduck_token={token}")


def match_players_sql(unmatched_players: pd.DataFrame, verbose: bool = True) -> dict[str, tuple[str, int, str]]:
    """
    Match unmatched Yahoo players to NFL players using SQL.

    Args:
        unmatched_players: DataFrame with columns:
            - yahoo_player_id
            - player (name)
            - position (or yahoo_position)
            - year
            - week
        verbose: Print progress

    Returns:
        Dict mapping yahoo_player_id -> (NFL_player_id, match_layer, nfl_name)
    """
    if unmatched_players.empty:
        return {}

    if verbose:
        log(f"Matching {len(unmatched_players):,} unmatched players via SQL...")

    # Determine column names
    name_col = "player" if "player" in unmatched_players.columns else "player_name"
    pos_col = "yahoo_position" if "yahoo_position" in unmatched_players.columns else "position"

    # Prepare player data for SQL
    players_data = []
    for _, row in unmatched_players.iterrows():
        yahoo_id = str(row["yahoo_player_id"])
        name = str(row.get(name_col, ""))
        position = str(row.get(pos_col, ""))
        year = int(row.get("year", 0))
        week = int(row.get("week", 0))

        # Normalize name for matching
        norm_name = normalize_name_sql(name)

        if norm_name and year > 0 and week > 0:
            players_data.append(
                {
                    "yahoo_player_id": yahoo_id,
                    "player_name": name,
                    "norm_name": norm_name,
                    "position": position,
                    "year": year,
                    "week": week,
                }
            )

    if not players_data:
        if verbose:
            log("No valid players to match")
        return {}

    players_df = pd.DataFrame(players_data)

    try:
        conn = get_motherduck_connection()
        conn.register("unmatched_yahoo", players_df)

        # Get unique years for filtering
        years = players_df["year"].unique().tolist()
        year_list = ", ".join(str(y) for y in years)

        # =====================================================================
        # LAYER 1: Exact name + position + year + week
        # =====================================================================
        if verbose:
            log("Layer 1: Exact name + position match...")

        layer1_query = f"""
            WITH nfl_normalized AS (
                SELECT
                    NFL_player_id,
                    player as nfl_name,
                    position as nfl_position,
                    year,
                    week,
                    -- Normalize NFL name to match Yahoo normalization
                    LOWER(REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(player, '[^a-zA-Z ]', '', 'g'),
                            ' (iii|iv|ii|jr|sr|v)$', '', 'i'
                        ),
                        '\\s+', ' ', 'g'
                    )) as norm_name
                FROM nfl_historical.nfl_player_stats_all
                WHERE year IN ({year_list})
                AND NFL_player_id IS NOT NULL
                AND player IS NOT NULL
            )
            SELECT DISTINCT
                y.yahoo_player_id,
                n.NFL_player_id,
                n.nfl_name,
                1 as match_layer
            FROM unmatched_yahoo y
            INNER JOIN nfl_normalized n
                ON y.norm_name = n.norm_name
                AND y.position = n.nfl_position
                AND y.year = n.year
                AND y.week = n.week
        """

        layer1_results = conn.execute(layer1_query).fetchdf()

        if verbose:
            log(f"  Layer 1 matches: {len(layer1_results):,}")

        # Build results dict
        results = {}
        matched_yahoo_ids = set()

        for _, row in layer1_results.iterrows():
            yahoo_id = str(row["yahoo_player_id"])
            if yahoo_id not in matched_yahoo_ids:
                results[yahoo_id] = (row["NFL_player_id"], 1, row["nfl_name"])
                matched_yahoo_ids.add(yahoo_id)

        # =====================================================================
        # LAYER 2: Name only (cross-position) with 1:1 constraint
        # =====================================================================
        # Filter to unmatched players
        remaining = players_df[~players_df["yahoo_player_id"].isin(matched_yahoo_ids)]

        if not remaining.empty:
            if verbose:
                log(f"Layer 2: Cross-position match for {len(remaining):,} remaining...")

            conn.register("remaining_yahoo", remaining)

            layer2_query = f"""
                WITH nfl_normalized AS (
                    SELECT
                        NFL_player_id,
                        player as nfl_name,
                        year,
                        week,
                        LOWER(REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(player, '[^a-zA-Z ]', '', 'g'),
                                ' (iii|iv|ii|jr|sr|v)$', '', 'i'
                            ),
                            '\\s+', ' ', 'g'
                        )) as norm_name
                    FROM nfl_historical.nfl_player_stats_all
                    WHERE year IN ({year_list})
                    AND NFL_player_id IS NOT NULL
                    AND player IS NOT NULL
                ),
                -- Find 1:1 matches (only one NFL player with that name in that year/week)
                name_counts AS (
                    SELECT norm_name, year, week, COUNT(DISTINCT NFL_player_id) as nfl_count
                    FROM nfl_normalized
                    GROUP BY norm_name, year, week
                    HAVING COUNT(DISTINCT NFL_player_id) = 1
                ),
                yahoo_counts AS (
                    SELECT norm_name, year, week, COUNT(DISTINCT yahoo_player_id) as yahoo_count
                    FROM remaining_yahoo
                    GROUP BY norm_name, year, week
                    HAVING COUNT(DISTINCT yahoo_player_id) = 1
                )
                SELECT DISTINCT
                    y.yahoo_player_id,
                    n.NFL_player_id,
                    n.nfl_name,
                    2 as match_layer
                FROM remaining_yahoo y
                INNER JOIN nfl_normalized n
                    ON y.norm_name = n.norm_name
                    AND y.year = n.year
                    AND y.week = n.week
                INNER JOIN name_counts nc
                    ON n.norm_name = nc.norm_name
                    AND n.year = nc.year
                    AND n.week = nc.week
                INNER JOIN yahoo_counts yc
                    ON y.norm_name = yc.norm_name
                    AND y.year = yc.year
                    AND y.week = yc.week
            """

            layer2_results = conn.execute(layer2_query).fetchdf()

            if verbose:
                log(f"  Layer 2 matches: {len(layer2_results):,}")

            for _, row in layer2_results.iterrows():
                yahoo_id = str(row["yahoo_player_id"])
                if yahoo_id not in results:
                    results[yahoo_id] = (row["NFL_player_id"], 2, row["nfl_name"])

        conn.close()

        if verbose:
            total_matched = len(results)
            total_unmatched = len(unmatched_players) - total_matched
            log(f"SQL matching complete: {total_matched:,} matched, {total_unmatched:,} unmatched")

        return results

    except Exception as e:
        log(f"SQL matching error: {e}")
        import traceback

        traceback.print_exc()
        return {}


def match_and_save_to_cache(unmatched_players: pd.DataFrame, token: str = None, verbose: bool = True) -> dict[str, str]:
    """
    Match unmatched players via SQL and save results to cache.

    Args:
        unmatched_players: DataFrame with yahoo_player_id, player, position, year, week
        token: MotherDuck token
        verbose: Print progress

    Returns:
        Dict mapping yahoo_player_id -> NFL_player_id
    """
    # Match via SQL
    matches = match_players_sql(unmatched_players, verbose=verbose)

    if not matches:
        return {}

    # Prepare mappings for cache
    from .player_id_cache import save_yahoo_nfl_mapping

    name_col = "player" if "player" in unmatched_players.columns else "player_name"
    pos_col = "yahoo_position" if "yahoo_position" in unmatched_players.columns else "position"

    new_mappings = []
    for yahoo_id, (nfl_id, layer, nfl_name) in matches.items():
        # Get Yahoo name from original data
        yahoo_row = unmatched_players[unmatched_players["yahoo_player_id"].astype(str) == yahoo_id]
        if not yahoo_row.empty:
            yahoo_name = str(yahoo_row.iloc[0].get(name_col, ""))
            position = str(yahoo_row.iloc[0].get(pos_col, ""))
        else:
            yahoo_name = ""
            position = ""

        new_mappings.append(
            (
                yahoo_id,
                nfl_id,
                layer,  # match_layer
                95.0 if layer == 1 else 85.0,  # confidence
                yahoo_name,
                nfl_name,
                position,
            )
        )

    # Save to MotherDuck cache
    if new_mappings:
        try:
            saved = save_yahoo_nfl_mapping(new_mappings, token)
            if verbose:
                log(f"Saved {saved} new mappings to cache")
        except Exception as e:
            if verbose:
                log(f"Could not save to cache: {e}")

    # Return simple mapping
    return {yahoo_id: nfl_id for yahoo_id, (nfl_id, _, _) in matches.items()}


# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="SQL-based Player Matching")
    parser.add_argument("--test", action="store_true", help="Run test matching")
    args = parser.parse_args()

    if args.test:
        # Test with sample data
        test_data = pd.DataFrame(
            [
                {"yahoo_player_id": "99999", "player": "Patrick Mahomes", "position": "QB", "year": 2024, "week": 1},
                {"yahoo_player_id": "99998", "player": "Travis Kelce", "position": "TE", "year": 2024, "week": 1},
                {"yahoo_player_id": "99997", "player": "Nonexistent Player", "position": "WR", "year": 2024, "week": 1},
            ]
        )

        results = match_players_sql(test_data, verbose=True)

        print("\nResults:")
        for yahoo_id, (nfl_id, layer, nfl_name) in results.items():
            print(f"  {yahoo_id} -> {nfl_id} (Layer {layer}, {nfl_name})")
