"""Verify populate_fantasy_points leaves no NULL fantasy_points on starter rows.

Used as a read-only regression harness for the DST/scoring Phase 3 work.

Usage:
    python scripts/verify_fantasy_points_not_null.py
    python scripts/verify_fantasy_points_not_null.py the_league_formerly_the_cffl
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS_ROOT = PROJECT_ROOT / "fantasy_football_data_scripts"
if str(DATA_SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(DATA_SCRIPTS_ROOT))

from multi_league.core.db_reader import get_reader


DEFAULT_LEAGUES = [
    "the_league_formerly_the_cffl",
    "flesh_for_fantasy",
    "tfl_of_extraordinary_gentleman",
    "the_pigskin_platoon",
    "tpg_fantasy_football_league",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only fantasy_points null verification harness")
    parser.add_argument("leagues", nargs="*", help="League database names to inspect")
    parser.add_argument(
        "--breakdown",
        action="store_true",
        help="Also print year/position breakdown for starter rows with NULL fantasy_points",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    reader = get_reader()

    leagues = args.leagues or DEFAULT_LEAGUES

    print(f"{'league':<35} {'null_starter_pts':>18} {'total_starter':>15} {'pct':>6}")
    print("-" * 78)
    for db_name in leagues:
        row = reader.query(
            f"""
            SELECT
                COUNT(*) FILTER (
                    WHERE fantasy_points IS NULL
                      AND CAST(is_started AS INTEGER) = 1
                ) AS null_starters,
                COUNT(*) FILTER (
                    WHERE CAST(is_started AS INTEGER) = 1
                ) AS total_starters
            FROM public.player_fantasy
            WHERE db_name = '{db_name}'
            """,
            database="___leagues",
        )
        null_starters = row[0]["null_starters"] if row else 0
        total_starters = row[0]["total_starters"] if row else 0
        pct = (100.0 * null_starters / total_starters) if total_starters else 0.0
        print(f"{db_name:<35} {null_starters:>18,} {total_starters:>15,} {pct:>5.1f}%")

        if args.breakdown and null_starters:
            rows = reader.query(
                f"""
                SELECT
                    year,
                    COALESCE(position, '<NULL>') AS position,
                    COUNT(*) FILTER (
                        WHERE fantasy_points IS NULL
                          AND CAST(is_started AS INTEGER) = 1
                    ) AS null_starters,
                    COUNT(*) FILTER (
                        WHERE CAST(is_started AS INTEGER) = 1
                    ) AS total_starters
                FROM public.player_fantasy
                WHERE db_name = '{db_name}'
                GROUP BY year, COALESCE(position, '<NULL>')
                HAVING COUNT(*) FILTER (
                    WHERE fantasy_points IS NULL
                      AND CAST(is_started AS INTEGER) = 1
                ) > 0
                ORDER BY null_starters DESC, year, position
                LIMIT 20
                """,
                database="___leagues",
            )
            print(f"\nBreakdown for {db_name}:")
            print(f"{'year':<8} {'position':<12} {'null_starter':>14} {'total_starter':>15}")
            for r in rows:
                print(f"{str(r['year']):<8} {r['position']:<12} {r['null_starters']:>14,} {r['total_starters']:>15,}")
            print()

    print("\nExpected after fix: null_starters = 0 for all leagues")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
