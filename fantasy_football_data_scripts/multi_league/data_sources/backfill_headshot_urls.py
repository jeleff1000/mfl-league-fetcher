"""
Backfill headshot_url in the super_table (___ops.nfl_historical.nfl_player_stats_all).

This is a one-time operation that propagates headshot URLs across all rows
for the same NFL_player_id. After running this, league imports don't need
to backfill headshot_url anymore.

Usage:
    python backfill_headshot_urls.py
"""

import os
import sys

import duckdb

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()


def backfill_headshot_urls_in_super_table():
    """
    Backfill headshot_url in super_table by propagating non-null values
    across rows with the same NFL_player_id.
    """
    print("=" * 60)
    print("BACKFILL HEADSHOT URLS IN SUPER TABLE")
    print("=" * 60)

    # Connect to MotherDuck
    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        print("[ERROR] No MotherDuck token found in environment")
        return 1

    conn = duckdb.connect(f"md:___ops?motherduck_token={token}")

    try:
        # Check current state
        print("\nChecking current headshot_url coverage...")
        result = conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT NFL_player_id) as total_players,
                COUNT(headshot_url) as rows_with_headshot,
                COUNT(DISTINCT CASE WHEN headshot_url IS NOT NULL THEN NFL_player_id END) as players_with_headshot
            FROM nfl_historical.nfl_player_stats_all
        """).fetchone()

        total_rows, total_players, rows_with_headshot, players_with_headshot = result
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total players: {total_players:,}")
        print(f"  Rows with headshot_url: {rows_with_headshot:,} ({100*rows_with_headshot/total_rows:.1f}%)")
        print(
            f"  Players with headshot_url: {players_with_headshot:,} ({100*players_with_headshot/total_players:.1f}%)"
        )

        # Backfill missing headshot_url values
        # For each NFL_player_id, propagate the first non-null headshot_url to all rows
        print("\nBackfilling headshot_url values...")
        print("  Creating temp table with canonical headshot URLs per player...")

        update_sql = """
            UPDATE nfl_historical.nfl_player_stats_all s
            SET headshot_url = canonical.headshot_url
            FROM (
                SELECT
                    NFL_player_id,
                    FIRST(headshot_url) FILTER (WHERE headshot_url IS NOT NULL) as headshot_url
                FROM nfl_historical.nfl_player_stats_all
                WHERE NFL_player_id IS NOT NULL
                GROUP BY NFL_player_id
                HAVING FIRST(headshot_url) FILTER (WHERE headshot_url IS NOT NULL) IS NOT NULL
            ) canonical
            WHERE s.NFL_player_id = canonical.NFL_player_id
              AND s.headshot_url IS NULL
              AND canonical.headshot_url IS NOT NULL
        """

        print("  Executing UPDATE...")
        conn.execute(update_sql)
        print("  Done!")

        # Check updated state
        print("\nChecking updated headshot_url coverage...")
        result = conn.execute("""
            SELECT
                COUNT(*) as total_rows,
                COUNT(DISTINCT NFL_player_id) as total_players,
                COUNT(headshot_url) as rows_with_headshot,
                COUNT(DISTINCT CASE WHEN headshot_url IS NOT NULL THEN NFL_player_id END) as players_with_headshot
            FROM nfl_historical.nfl_player_stats_all
        """).fetchone()

        total_rows, total_players, rows_with_headshot, players_with_headshot = result
        print(f"  Total rows: {total_rows:,}")
        print(f"  Total players: {total_players:,}")
        print(f"  Rows with headshot_url: {rows_with_headshot:,} ({100*rows_with_headshot/total_rows:.1f}%)")
        print(
            f"  Players with headshot_url: {players_with_headshot:,} ({100*players_with_headshot/total_players:.1f}%)"
        )

        print("\n" + "=" * 60)
        print("BACKFILL COMPLETE")
        print("=" * 60)

    except Exception as e:
        print(f"\n[ERROR] Backfill failed: {e}")
        import traceback

        traceback.print_exc()
        return 1
    finally:
        conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(backfill_headshot_urls_in_super_table())
