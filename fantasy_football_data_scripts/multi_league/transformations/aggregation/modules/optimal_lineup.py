"""
Optimal lineup module -- pure functions extracted from AggregationEnrichmentsMixin.

Contains:
- league_wide_optimal(): Mark top-N NFL players per position per week (league-wide best)
- manager_optimal(): Mark each manager's best possible lineup per week
- position_rank(): Populate position_rank from scoring-variant rank columns

All functions take (conn, player_table, ...) instead of using self.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any
from collections.abc import Callable

from multi_league.core.sql_utils import validate_db_name
from multi_league.core.roster_slots import FRONT_7, FRONT_7_SLOTS, IDP_FAMILIES, NON_STARTER_SLOTS
from multi_league.shared.filters import rostered_filter_sql  # noqa: F401 - used in f-strings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers dataclass
# ---------------------------------------------------------------------------


@dataclass
class RosterHelpers:
    """Roster/position helper functions extracted from sql_base for standalone use."""

    available_settings_years: Callable
    resolve_settings_year: Callable
    get_dedicated_slots: Callable
    identify_flex_positions: Callable
    position_eligibility_sql: Callable
    flex_eligibility_sql: Callable
    preferred_flex_rank_column: Callable
    front7_eligibility_sql: Callable
    primary_position_sql: Callable
    group_years_by_scoring: Callable


def _db_filter(db_name: str | None, alias: str = "") -> str:
    """Return the centralized db_name filter or a no-op for local mode."""
    if not db_name:
        return "1=1"

    validate_db_name(db_name)
    prefix = f"{alias}." if alias else ""
    return f"{prefix}db_name = '{db_name}'"


def _flex_slot_sort_key(item: tuple[str, set[str], int] | tuple[str, list[str], int]) -> tuple[int, int, str]:
    """Fill defensive flex pools before generic offensive flex pools."""
    flex_name, eligible_positions, _ = item
    slot_name = flex_name.upper()
    eligible = {str(pos).upper() for pos in eligible_positions}
    is_defensive_flex = slot_name in {"IDP", "DL_LB", "DB_LB"} or eligible.issubset({"DL", "LB", "DB"})
    return (0 if is_defensive_flex else 1, len(eligible), slot_name)


def _table_has_column(conn, table_name: str, column_name: str) -> bool:
    """Best-effort column existence check for temp test tables and local DBs."""
    try:
        rows = conn.execute(f"DESCRIBE {table_name}").fetchall()
    except Exception:
        return False
    return any(row and row[0] == column_name for row in rows)


# ---------------------------------------------------------------------------
# League-wide optimal
# ---------------------------------------------------------------------------


def league_wide_optimal(
    conn,
    player_table: str,
    roster_by_year: dict[Any, dict[str, int]],
    helpers: RosterHelpers,
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Set league-wide optimal flags via staged temp tables.

    Marks league_wide_optimal_player = 1 for the top N players at each position
    per week, where N is the number of roster slots for that position.  This enables
    "what was the best possible NFL lineup?" analysis.

    Args:
        conn: DuckDB connection.
        player_table: Fully qualified table name (e.g. ``db.public.player_fantasy``).
        roster_by_year: Dict mapping year -> roster_settings dict.
        helpers: RosterHelpers with position/eligibility callables.
        dry_run: If True, log what would happen without writing.

    Returns:
        Total number of years processed.
    """
    if not roster_by_year:
        raise ValueError("roster_by_year is required. Ensure settings are loaded before calling this method.")

    years = conn.execute(
        f"SELECT DISTINCT year FROM {player_table} WHERE year IS NOT NULL AND {_db_filter(db_name)} ORDER BY year"
    ).fetchall()
    if not years:
        logger.warning("[league_wide_optimal] No years found in player_fantasy")
        return 0

    year_list = [int(row[0]) for row in years]
    logger.info(f"[league_wide_optimal] Processing {len(year_list)} years: {year_list}")

    available_roster_years = helpers.available_settings_years(roster_by_year)
    if not available_roster_years:
        raise ValueError("roster_by_year has no valid year keys")

    if not dry_run:
        conn.execute(f"""
            UPDATE {player_table}
            SET league_wide_optimal_player = 0,
                league_wide_optimal_position = NULL
            WHERE {_db_filter(db_name)}
        """)
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _league_optimal_pool AS
            SELECT DISTINCT
                player_week,
                year,
                week,
                position,
                fantasy_points,
                position_rank,
                flex_week_rank,
                sflex_week_rank
            FROM {player_table}
            WHERE year IS NOT NULL
              AND week IS NOT NULL
              AND fantasy_points IS NOT NULL
              AND {_db_filter(db_name)}
        """)
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _league_optimal_selected (
                player_week VARCHAR,
                year INTEGER,
                week INTEGER,
                slot_group VARCHAR
            )
        """)
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _league_slot_counts (
                year INTEGER,
                slot_group VARCHAR,
                slot_count INTEGER
            )
        """)

    total_updated = 0

    for year_int in year_list:
        resolved_year, roster_settings = helpers.resolve_settings_year(year_int, roster_by_year)
        settings_source = resolved_year if resolved_year is not None else year_int
        if resolved_year is not None and resolved_year != year_int:
            if year_int < available_roster_years[0]:
                logger.info(f"[league_wide_optimal] Year {year_int}: using first league year {resolved_year} settings")
            elif year_int > available_roster_years[-1]:
                logger.info(f"[league_wide_optimal] Year {year_int}: using last league year {resolved_year} settings")
            else:
                logger.info(f"[league_wide_optimal] Year {year_int}: using nearest available {resolved_year} settings")

        if not roster_settings:
            logger.warning(f"[league_wide_optimal] No roster settings for year {year_int}, skipping")
            continue

        dedicated_slots = helpers.get_dedicated_slots(roster_settings)
        flex_positions = sorted(helpers.identify_flex_positions(roster_settings), key=_flex_slot_sort_key)
        total_starters = sum(int(slots) for slots in dedicated_slots.values()) + sum(
            int(flex_count) for _, _, flex_count in flex_positions
        )

        if not dry_run:
            slot_rows = []
            for position, slots in dedicated_slots.items():
                slot_count = int(slots)
                if slot_count <= 0:
                    continue
                slot_group = position.upper().replace("'", "''")
                slot_rows.append(f"({year_int}, '{slot_group}', {slot_count})")
            for flex_name, _, flex_count in flex_positions:
                slot_count = int(flex_count)
                if slot_count <= 0:
                    continue
                slot_group = flex_name.upper().replace("'", "''")
                slot_rows.append(f"({year_int}, '{slot_group}', {slot_count})")
            if slot_rows:
                conn.execute(f"INSERT INTO _league_slot_counts VALUES {', '.join(slot_rows)}")

            for position, slots in dedicated_slots.items():
                slot_count = int(slots)
                if slot_count <= 0:
                    continue
                slot_group = position.upper().replace("'", "''")
                eligible_sql = helpers.position_eligibility_sql("p.position", position)
                conn.execute(f"""
                    INSERT INTO _league_optimal_selected
                    SELECT player_week, year, week, '{slot_group}'
                    FROM (
                        SELECT
                            p.player_week,
                            p.year,
                            p.week,
                            ROW_NUMBER() OVER (
                                PARTITION BY p.year, p.week
                                ORDER BY
                                    CASE WHEN p.position_rank IS NULL THEN 1 ELSE 0 END,
                                    p.position_rank ASC NULLS LAST,
                                    p.fantasy_points DESC NULLS LAST,
                                    p.player_week
                            ) AS slot_rank
                        FROM _league_optimal_pool p
                        LEFT JOIN _league_optimal_selected s
                          ON p.player_week = s.player_week
                         AND p.year = s.year
                         AND p.week = s.week
                        WHERE p.year = {year_int}
                          AND {eligible_sql}
                          AND s.player_week IS NULL
                    ) ranked
                    WHERE slot_rank <= {slot_count}
                """)

            for flex_name, eligible_positions, flex_count in flex_positions:
                slot_count = int(flex_count)
                if slot_count <= 0:
                    continue
                slot_group = flex_name.upper().replace("'", "''")
                eligible_sql = helpers.flex_eligibility_sql("p.position", eligible_positions)
                preferred_rank_col = helpers.preferred_flex_rank_column(eligible_positions)
                if preferred_rank_col:
                    order_sql = (
                        f"CASE WHEN p.{preferred_rank_col} IS NULL THEN 1 ELSE 0 END, "
                        f"p.{preferred_rank_col} ASC NULLS LAST, "
                        "p.fantasy_points DESC NULLS LAST, "
                        "p.player_week"
                    )
                else:
                    order_sql = (
                        "p.fantasy_points DESC NULLS LAST, "
                        "CASE WHEN p.position_rank IS NULL THEN 1 ELSE 0 END, "
                        "p.position_rank ASC NULLS LAST, "
                        "p.player_week"
                    )

                conn.execute(f"""
                    INSERT INTO _league_optimal_selected
                    SELECT player_week, year, week, '{slot_group}'
                    FROM (
                        SELECT
                            p.player_week,
                            p.year,
                            p.week,
                            ROW_NUMBER() OVER (
                                PARTITION BY p.year, p.week
                                ORDER BY {order_sql}
                            ) AS slot_rank
                        FROM _league_optimal_pool p
                        LEFT JOIN _league_optimal_selected s
                          ON p.player_week = s.player_week
                         AND p.year = s.year
                         AND p.week = s.week
                        WHERE p.year = {year_int}
                          AND {eligible_sql}
                          AND s.player_week IS NULL
                    ) ranked
                    WHERE slot_rank <= {slot_count}
                """)
                logger.debug(
                    f"[league_wide_optimal] {year_int} {flex_name}: selected {slot_count} slots from {eligible_positions}"
                )

            sample_rows = conn.execute(f"""
                SELECT week, COUNT(*) AS optimal_count
                FROM _league_optimal_selected
                WHERE year = {year_int}
                GROUP BY week
                ORDER BY week
                LIMIT 3
            """).fetchall()
            sample_counts = [int(row[1]) for row in sample_rows]
            logger.info(
                f"[league_wide_optimal] Year {year_int} (from {settings_source}): "
                f"expected {total_starters} starters, got {sample_counts[:3]}... per week"
            )
        else:
            logger.info(f"[DRY RUN] Year {year_int}: {total_starters} starters (settings from {settings_source})")

        total_updated += 1

    if not dry_run:
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _league_optimal_labels AS
            WITH ranked AS (
                SELECT
                    s.player_week,
                    s.year,
                    s.week,
                    s.slot_group,
                    c.slot_count,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.year, s.week, s.slot_group
                        ORDER BY p.fantasy_points DESC NULLS LAST, s.player_week
                    ) AS slot_rank
                FROM _league_optimal_selected s
                JOIN _league_slot_counts c
                  ON s.year = c.year
                 AND s.slot_group = c.slot_group
                JOIN _league_optimal_pool p
                  ON s.player_week = p.player_week
                 AND s.year = p.year
                 AND s.week = p.week
            )
            SELECT
                player_week,
                year,
                week,
                CASE
                    WHEN slot_count > 1 THEN slot_group || CAST(slot_rank AS VARCHAR)
                    ELSE slot_group
                END AS slot_label
            FROM ranked
        """)
        conn.execute(f"""
            UPDATE {player_table} p
            SET league_wide_optimal_player = 1,
                league_wide_optimal_position = labels.slot_label
            FROM _league_optimal_labels labels
            WHERE p.player_week = labels.player_week
              AND p.year = labels.year
              AND p.week = labels.week
              AND {_db_filter(db_name, 'p')}
        """)
        conn.execute("DROP TABLE IF EXISTS _league_optimal_labels")
        conn.execute("DROP TABLE IF EXISTS _league_optimal_selected")
        conn.execute("DROP TABLE IF EXISTS _league_slot_counts")
        conn.execute("DROP TABLE IF EXISTS _league_optimal_pool")

    logger.info(f"[league_wide_optimal] Processed {total_updated} years")
    return total_updated


# ---------------------------------------------------------------------------
# Manager optimal
# ---------------------------------------------------------------------------


def _refine_manager_optimal_with_swaps(
    conn,
    year_int: int,
    dedicated_slots: dict[str, int],
    flex_positions: list[tuple[str, set[str], int]],
) -> None:
    """Local-search post-processing pass that fixes greedy-assignment artifacts.

    Greedy filling in scarcity order (K, DEF, TE, QB, RB, WR, ...) leaves points
    on the bench whenever a multi-slot-eligible player gets consumed for the
    wrong slot. Three improvement patterns:

    1. Simple 1-for-1: a bench player scores higher than someone currently in
       a slot that bench player is eligible for.
    2. Two-step: selected M with multi-slot eligibility moves to alt_slot Y
       (displacing the worst Y occupant), and a bench player fills M's old
       slot.
    3. Three-step cascade: dual-eligible A moves to alt_slot Y (displaces the
       worst Y occupant B); a current FLEX occupant C — whose NFL position
       fits A's old slot X — cascades into X; bench D fills the freed FLEX.
       Net delta = D.pts - B.pts. (Concrete fleet bug: the_tfl 2021 W2
       Cordarrelle Patterson — RB started at WR, FLEX held an RB, bench TE
       was wasted.)

    Iterates until no improvement; capped at 20 rounds per manager-week to
    bound worst-case cost on pathological inputs.
    """
    dedicated_keys_upper = {k.upper(): k for k in dedicated_slots}
    flex_by_name = {flex_name.upper(): set(eligible_positions) for flex_name, eligible_positions, _ in flex_positions}
    flex_slot_names = set(flex_by_name.keys())
    front_7_upper = {p.upper() for p in FRONT_7}
    idp_family_upper = {fam.upper(): {p.upper() for p in members} for fam, members in IDP_FAMILIES.items()}

    def is_eligible(pos_tokens: set[str], slot: str) -> bool:
        slot_upper = slot.upper()
        if slot_upper == "_FRONT7":
            return bool(pos_tokens & front_7_upper)
        if slot_upper in flex_by_name:
            allowed = {p.upper() for p in flex_by_name[slot_upper]} | {slot_upper}
            return bool(pos_tokens & allowed)
        if slot_upper in idp_family_upper:
            allowed = idp_family_upper[slot_upper] | {slot_upper}
            return bool(pos_tokens & allowed)
        if slot_upper in dedicated_keys_upper:
            return slot_upper in pos_tokens
        return False

    def all_slot_options(pos_tokens: set[str]) -> set[str]:
        options: set[str] = set()
        for slot in dedicated_keys_upper:
            if is_eligible(pos_tokens, slot):
                options.add(slot)
        for slot in flex_slot_names:
            if is_eligible(pos_tokens, slot):
                options.add(slot)
        return options

    manager_weeks = conn.execute(
        """
        SELECT DISTINCT s.year, s.week, s.franchise_id
        FROM _manager_optimal_selected s
        WHERE s.year = ?
        """,
        [year_int],
    ).fetchall()

    for yr, wk, fid in manager_weeks:
        pool: dict[str, dict] = {}
        for pw, position, fantasy_position, pts in conn.execute(
            """
            SELECT player_week, position, fantasy_position, fantasy_points
            FROM _manager_optimal_pool
            WHERE year=? AND week=? AND franchise_id=?
            """,
            [yr, wk, fid],
        ).fetchall():
            tokens: set[str] = set()
            if position:
                tokens.update(p.strip().upper() for p in str(position).split(",") if p.strip())
            if fantasy_position:
                fp = str(fantasy_position).strip().upper()
                if fp:
                    tokens.add(fp)
            pool[pw] = {"tokens": tokens, "pts": float(pts or 0.0)}

        selection: dict[str, str] = {}
        for pw, slot in conn.execute(
            """
            SELECT player_week, slot_group
            FROM _manager_optimal_selected
            WHERE year=? AND week=? AND franchise_id=?
            """,
            [yr, wk, fid],
        ).fetchall():
            selection[pw] = slot

        for _ in range(20):
            slot_occupants: dict[str, list[tuple[str, float]]] = {}
            for pw, slot in selection.items():
                slot_occupants.setdefault(slot.upper(), []).append((pw, pool[pw]["pts"]))
            bench = [(pw, p) for pw, p in pool.items() if pw not in selection and p["pts"] > 0]

            best_delta = 0.0
            best_action: tuple | None = None

            # Type 1 — simple 1-for-1: a bench player replaces a lower-scoring
            # selected player at any slot the bench player is eligible for.
            for B_pw, B in bench:
                for L_pw, L_slot in selection.items():
                    if not is_eligible(B["tokens"], L_slot):
                        continue
                    delta = B["pts"] - pool[L_pw]["pts"]
                    if delta > best_delta:
                        best_delta = delta
                        best_action = ("simple", L_pw, B_pw, L_slot)

            # Type 2 — two-step: selected M with multi-slot eligibility moves
            # to alt_slot Y (displacing Y's worst occupant); bench D fills M's
            # old slot. Subsumes the prior comma-position swap pass.
            for M_pw, M_slot in selection.items():
                M_slot_upper = M_slot.upper()
                M_options = all_slot_options(pool[M_pw]["tokens"])
                for alt_slot in M_options:
                    if alt_slot == M_slot_upper:
                        continue
                    if alt_slot not in slot_occupants:
                        continue
                    D_pw, D_pts = min(slot_occupants[alt_slot], key=lambda x: x[1])
                    if D_pw == M_pw:
                        continue
                    eligible_bench = [
                        (pw, pool[pw]["pts"])
                        for pw, p in pool.items()
                        if pw not in selection and p["pts"] > 0 and is_eligible(p["tokens"], M_slot_upper)
                    ]
                    if not eligible_bench:
                        continue
                    B_pw, B_pts = max(eligible_bench, key=lambda x: x[1])
                    delta = B_pts - D_pts
                    if delta > best_delta:
                        best_delta = delta
                        best_action = ("twostep", M_pw, alt_slot, D_pw, B_pw, M_slot_upper)

            # Type 3 — three-step cascade: A (selected at X) moves to Y
            # (displaces worst occupant B); FLEX occupant C (eligible for X)
            # cascades into X; bench D fills the now-empty FLEX. The Patterson
            # case: A=Patterson, X=RB, Y=WR, C=Mitchell (FLEX→RB), D=Kittle
            # (bench→FLEX), B=Beckham (0 pts).
            for A_pw, A_slot in selection.items():
                X = A_slot.upper()
                A_options = all_slot_options(pool[A_pw]["tokens"])
                for Y in A_options:
                    if Y == X:
                        continue
                    if Y not in slot_occupants:
                        continue
                    B_pw, B_pts = min(slot_occupants[Y], key=lambda x: x[1])
                    if B_pw == A_pw:
                        continue
                    for Z in flex_slot_names:
                        if Z not in slot_occupants:
                            continue
                        for C_pw, _C_pts in slot_occupants[Z]:
                            if C_pw in (A_pw, B_pw):
                                continue
                            if not is_eligible(pool[C_pw]["tokens"], X):
                                continue
                            eligible_bench_z = [
                                (pw, pool[pw]["pts"])
                                for pw, p in pool.items()
                                if pw not in selection
                                and p["pts"] > 0
                                and pw not in (A_pw, B_pw, C_pw)
                                and is_eligible(p["tokens"], Z)
                            ]
                            if not eligible_bench_z:
                                continue
                            D_pw, D_pts = max(eligible_bench_z, key=lambda x: x[1])
                            delta = D_pts - B_pts
                            if delta > best_delta:
                                best_delta = delta
                                best_action = ("threestep", A_pw, Y, B_pw, C_pw, X, D_pw, Z)

            if best_action is None or best_delta <= 0:
                break

            kind = best_action[0]
            if kind == "simple":
                _, L_pw, B_pw, L_slot = best_action
                del selection[L_pw]
                selection[B_pw] = L_slot
            elif kind == "twostep":
                _, M_pw, alt_slot, D_pw, B_pw, M_slot = best_action
                del selection[D_pw]
                selection[M_pw] = alt_slot
                selection[B_pw] = M_slot
            elif kind == "threestep":
                _, A_pw, Y, B_pw, C_pw, X, D_pw, Z = best_action
                del selection[B_pw]
                selection[A_pw] = Y
                selection[C_pw] = X
                selection[D_pw] = Z

        conn.execute(
            "DELETE FROM _manager_optimal_selected WHERE year=? AND week=? AND franchise_id=?",
            [yr, wk, fid],
        )
        for pw, slot in selection.items():
            conn.execute(
                "INSERT INTO _manager_optimal_selected VALUES (?, ?, ?, ?, ?)",
                [pw, yr, wk, fid, slot],
            )


def manager_optimal(
    conn,
    player_table: str,
    roster_by_year: dict[Any, dict[str, int]],
    helpers: RosterHelpers,
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Set per-manager optimal flags via staged temp tables.

    For each (manager, year, week), selects the best lineup from that manager's
    rostered players using the league's roster slot configuration.  Sets
    optimal_player=1 for the players that would form the highest-scoring valid
    lineup.

    Args:
        conn: DuckDB connection.
        player_table: Fully qualified table name.
        roster_by_year: Dict mapping year -> roster_settings dict.
        helpers: RosterHelpers with position/eligibility callables.
        dry_run: If True, log what would happen without writing.

    Returns:
        Total number of years processed.
    """
    if not roster_by_year:
        raise ValueError("roster_by_year is required.")

    years = conn.execute(
        f"SELECT DISTINCT year FROM {player_table} WHERE year IS NOT NULL AND {_db_filter(db_name)} ORDER BY year"
    ).fetchall()
    if not years:
        return 0

    year_list = [int(row[0]) for row in years]
    available_roster_years = helpers.available_settings_years(roster_by_year)
    if not available_roster_years:
        raise ValueError("roster_by_year has no valid year keys")

    bye_filter = "AND COALESCE(p.is_bye_week, 0) = 0" if _table_has_column(conn, player_table, "is_bye_week") else ""

    if not dry_run:
        conn.execute(f"UPDATE {player_table} SET optimal_player = 0 WHERE {_db_filter(db_name)}")
        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _manager_optimal_pool AS
            SELECT DISTINCT
                p.player_week,
                p.year,
                p.week,
                p.manager,
                p.franchise_id,
                p.position,
                p.fantasy_position,
                p.fantasy_points,
                p.position_rank,
                p.flex_week_rank,
                p.sflex_week_rank
            FROM {player_table} p
            WHERE {rostered_filter_sql("p")}
              AND {_db_filter(db_name, 'p')}
              {bye_filter}
              AND p.year IS NOT NULL
              AND p.week IS NOT NULL
              AND p.fantasy_points IS NOT NULL
        """)
        conn.execute("""
            CREATE OR REPLACE TEMP TABLE _manager_optimal_selected (
                player_week VARCHAR,
                year INTEGER,
                week INTEGER,
                franchise_id VARCHAR,
                slot_group VARCHAR
            )
        """)

    total = 0

    for year_int in year_list:
        resolved_year, roster_settings = helpers.resolve_settings_year(year_int, roster_by_year)
        if resolved_year is not None and resolved_year != year_int:
            if year_int < available_roster_years[0]:
                logger.info(f"[manager_optimal] Year {year_int}: using first league year {resolved_year} settings")
            elif year_int > available_roster_years[-1]:
                logger.info(f"[manager_optimal] Year {year_int}: using last league year {resolved_year} settings")
            else:
                logger.info(f"[manager_optimal] Year {year_int}: using nearest available {resolved_year} settings")

        if not roster_settings:
            continue

        dedicated_slots = helpers.get_dedicated_slots(roster_settings)
        flex_positions = sorted(helpers.identify_flex_positions(roster_settings), key=_flex_slot_sort_key)

        # Merge DL + LB dedicated slots into a single front-7 pool for
        # manager optimal.  Platforms classify edge rushers inconsistently
        # (e.g. Will Anderson = DL on Sleeper, LB elsewhere), so forcing
        # exact DL-to-DL matching leaves points on the bench.
        front7_total = sum(dedicated_slots.get(s, 0) for s in FRONT_7_SLOTS)
        if front7_total > 0:
            for s in FRONT_7_SLOTS:
                dedicated_slots.pop(s, None)
            dedicated_slots["_FRONT7"] = front7_total

        # Fill scarcer positions first so dual-eligible players (e.g. Taysom Hill
        # QB/TE) are assigned to their rarer position.  This prevents greedy
        # assignment from putting a QB/TE in the QB slot when TE has no backup,
        # leaving the actual QB on the bench.
        #
        # DB is placed BEFORE _FRONT7 because a DB slot only accepts
        # DB-family players (CB/S/FS/SS/NB), while _FRONT7 accepts any LB/DL
        # player — so DB is the scarcer pool. Filling _FRONT7 first allows a
        # greedy LB-with-DB-eligibility (e.g. Keanu Neal, Foye Oluokun
        # played in DB) to get swallowed by _FRONT7, leaving the DB slot
        # empty. Filling DB first assigns those players to their only
        # eligible dedicated slot and keeps _FRONT7 for pure LB/DL.
        _SCARCITY_ORDER = {"K": 0, "DEF": 1, "TE": 2, "QB": 3, "RB": 4, "WR": 5, "DB": 6, "_FRONT7": 7}
        sorted_slots = sorted(
            dedicated_slots.items(),
            key=lambda x: _SCARCITY_ORDER.get(x[0].upper(), 10),
        )

        if not dry_run:
            # Dual-eligibility expression: NFL position OR the fantasy_position
            # slot the manager actually played the player in this week.
            # This lets the optimal algorithm see players like Taysom Hill
            # (NFL QB, but eligible at TE when the platform exposes dual
            # eligibility) as TE-eligible. The player can still only be
            # picked ONCE per week because the `LEFT JOIN
            # _manager_optimal_selected s ON p.player_week = s.player_week`
            # predicate + `s.player_week IS NULL` excludes already-selected
            # players from subsequent slot passes. For bench players
            # (fantasy_position='BN'/'IR'/'TAXI'), the extra token doesn't
            # match any canonical slot so the expression is a no-op.
            eligibility_col = "(COALESCE(p.position, '') || ',' || COALESCE(p.fantasy_position, ''))"
            for position, slots in sorted_slots:
                slot_count = int(slots)
                if slot_count <= 0:
                    continue
                slot_group = position.upper().replace("'", "''")
                # Front-7 pool: use combined LB+DL eligibility
                if position == "_FRONT7":
                    eligible_sql = helpers.front7_eligibility_sql(eligibility_col)
                else:
                    eligible_sql = helpers.position_eligibility_sql(eligibility_col, position)
                # Never fill a slot with a negative-point player. If the only
                # eligible option scores below zero, the real manager could
                # (and often did) leave the slot empty — e.g. tfl DEF slots
                # where the only rostered defense scored -4.10. Including a
                # negative-point pick makes optimal < actual, breaking the
                # optimal_gte_team_points invariant.
                conn.execute(f"""
                    INSERT INTO _manager_optimal_selected
                    SELECT player_week, year, week, franchise_id, '{slot_group}'
                    FROM (
                        SELECT
                            p.player_week,
                            p.year,
                            p.week,
                            p.franchise_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY p.year, p.week, p.franchise_id
                                ORDER BY
                                    p.fantasy_points DESC NULLS LAST,
                                    p.player_week
                            ) AS slot_rank
                        FROM _manager_optimal_pool p
                        LEFT JOIN _manager_optimal_selected s
                          ON p.player_week = s.player_week
                         AND p.year = s.year
                         AND p.week = s.week
                         AND p.franchise_id = s.franchise_id
                        WHERE p.year = {year_int}
                          AND {eligible_sql}
                          AND s.player_week IS NULL
                          AND p.fantasy_points >= 0
                    ) ranked
                    WHERE slot_rank <= {slot_count}
                """)

            for flex_name, eligible_positions, flex_count in flex_positions:
                slot_count = int(flex_count)
                if slot_count <= 0:
                    continue
                slot_group = flex_name.upper().replace("'", "''")
                # Same dual-eligibility expression as the dedicated slot
                # loop above — Taysom Hill should be SUPER_FLEX eligible
                # too, not just QB/TE.
                #
                # Also extend the flex's eligible_positions with the flex
                # slot NAME itself (e.g. add "IDP" to {DL, LB, DB}). That
                # way a player whose fantasy_position is literally the
                # flex slot name — like Travis Hunter being played at an
                # "IDP" slot despite his NFL position=WR — matches on the
                # slot-name token. If the league allowed the manager to
                # play him there, the optimal algorithm should be allowed
                # to consider him for that slot too.
                flex_eligible_tokens = list(eligible_positions) + [flex_name]
                eligible_sql = helpers.flex_eligibility_sql(eligibility_col, flex_eligible_tokens)
                # Order flex by ACTUAL fantasy_points for this league, not by
                # the super_table's precomputed flex_week_rank/sflex_week_rank.
                # Those precomputed ranks are weekly league-wide orderings
                # under a DEFAULT scoring variant and disagree with this
                # league's per-player fantasy_points whenever the scoring
                # isn't the canonical half-PPR/4pt config (e.g. Diggs 5.3 in
                # this league's scoring vs. Ridley 5.2 → real points say
                # Diggs, rank column says Ridley). Manager optimal's
                # objective is maximize SUM(fantasy_points) for THIS league,
                # so fantasy_points DESC must be the primary order. rank
                # columns stay as a tiebreaker only.
                preferred_rank_col = helpers.preferred_flex_rank_column(eligible_positions)
                if preferred_rank_col:
                    order_sql = (
                        "p.fantasy_points DESC NULLS LAST, "
                        f"CASE WHEN p.{preferred_rank_col} IS NULL THEN 1 ELSE 0 END, "
                        f"p.{preferred_rank_col} ASC NULLS LAST, "
                        "p.player_week"
                    )
                else:
                    order_sql = (
                        "p.fantasy_points DESC NULLS LAST, "
                        "CASE WHEN p.position_rank IS NULL THEN 1 ELSE 0 END, "
                        "p.position_rank ASC NULLS LAST, "
                        "p.player_week"
                    )

                conn.execute(f"""
                    INSERT INTO _manager_optimal_selected
                    SELECT player_week, year, week, franchise_id, '{slot_group}'
                    FROM (
                        SELECT
                            p.player_week,
                            p.year,
                            p.week,
                            p.franchise_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY p.year, p.week, p.franchise_id
                                ORDER BY {order_sql}
                            ) AS slot_rank
                        FROM _manager_optimal_pool p
                        LEFT JOIN _manager_optimal_selected s
                          ON p.player_week = s.player_week
                         AND p.year = s.year
                         AND p.week = s.week
                         AND p.franchise_id = s.franchise_id
                        WHERE p.year = {year_int}
                          AND {eligible_sql}
                          AND s.player_week IS NULL
                          AND p.fantasy_points >= 0
                    ) ranked
                    WHERE slot_rank <= {slot_count}
                """)

            # ---------------------------------------------------------
            # POST-GREEDY LOCAL SEARCH: fixes greedy artifacts like
            # dual-eligible players (NFL position vs. fantasy_position
            # mismatch) consuming the wrong slot, including 3-step
            # cascades through FLEX. See _refine_manager_optimal_with_swaps
            # docstring for the full taxonomy.
            # ---------------------------------------------------------
            _refine_manager_optimal_with_swaps(conn, year_int, dedicated_slots, flex_positions)

            # ---------------------------------------------------------
            # BACKSTOP: handle non-standard roster slots (Punter "P",
            # Head Coach "HC", custom Yahoo IDP variants). The flat
            # `league_settings` DDL has columns for QB/RB/WR/TE/K/DEF/
            # LB/DL/DB/FLX/SUPER_FLEX/etc. but none for P or HC, so
            # `roster_settings` for leagues like njfl never includes
            # those slots — the dedicated/flex passes above never fill
            # them — and starters in those slots end up with
            # optimal_player=0. Result: matchup.team_points (sum of
            # started fantasy_points) > matchup.optimal_points (sum of
            # selected) by the contribution of the unrecognized slot.
            #
            # Backstop: any started player whose fantasy_position slot
            # the standard pass didn't try to fill is treated as
            # already-optimal at that slot. There's no bench
            # alternative the algorithm considered (since the slot
            # wasn't enumerated), so the started player IS the optimal
            # choice. Bench-replacement logic continues to work
            # correctly for known slots — this only catches slots the
            # main algorithm ignored entirely.
            processed_slot_names = (
                {p.upper() for p in dedicated_slots if p}
                | {n.upper() for n, _, _ in flex_positions}
                | {p.upper() for p in FRONT_7_SLOTS}
                | {"_FRONT7"}
                | {s.upper() for s in NON_STARTER_SLOTS}
            )
            processed_in = ", ".join(f"'{s}'" for s in sorted(processed_slot_names))
            conn.execute(f"""
                INSERT INTO _manager_optimal_selected
                SELECT
                    p.player_week,
                    p.year,
                    p.week,
                    p.franchise_id,
                    UPPER(TRIM(p.fantasy_position)) AS slot_group
                FROM _manager_optimal_pool p
                LEFT JOIN _manager_optimal_selected s
                  ON p.player_week = s.player_week
                 AND p.year = s.year
                 AND p.week = s.week
                 AND p.franchise_id = s.franchise_id
                WHERE p.year = {year_int}
                  AND s.player_week IS NULL
                  AND p.fantasy_points >= 0
                  AND p.fantasy_position IS NOT NULL
                  AND TRIM(p.fantasy_position) <> ''
                  AND UPPER(TRIM(p.fantasy_position)) NOT IN ({processed_in})
            """)

        total += 1

    if not dry_run:
        conn.execute(f"""
            UPDATE {player_table} p
            SET optimal_player = 1,
                optimal_position = s.slot_group
            FROM _manager_optimal_selected s
            WHERE p.player_week = s.player_week
              AND p.year = s.year
              AND p.week = s.week
              AND p.franchise_id = s.franchise_id
              AND {_db_filter(db_name, 'p')}
        """)
        opt_count = conn.execute(
            f"SELECT COUNT(*) FROM {player_table} WHERE optimal_player = 1 AND {_db_filter(db_name)}"
        ).fetchone()[0]
        logger.info(f"[manager_optimal] {opt_count:,} optimal_player flags set across {total} years")
        conn.execute("DROP TABLE IF EXISTS _manager_optimal_selected")
        conn.execute("DROP TABLE IF EXISTS _manager_optimal_pool")

    return total


# ---------------------------------------------------------------------------
# Position rank
# ---------------------------------------------------------------------------


def position_rank(
    conn,
    player_table: str,
    roster_by_year: dict,
    helpers: RosterHelpers,
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Populate generic position_rank column from scoring-variant rank columns.

    Creates a league-specific position_rank that adapts to rule changes.
    If a league switches from 4pt to 6pt pass TD between years, each year
    uses the correct scoring variant:
    - Year 2024 (4pt): position_rank = rank_qb_4pt
    - Year 2025 (6pt): position_rank = rank_qb_6pt

    The UI just sees ``position_rank`` and doesn't care about the variant.

    Args:
        conn: DuckDB connection.
        player_table: Fully qualified table name.
        roster_by_year: Dict mapping year -> roster_settings dict.
        helpers: RosterHelpers with position/eligibility callables.
        dry_run: If True, log what would happen without writing.

    Returns:
        Total number of player-weeks updated.
    """
    if not roster_by_year:
        logger.error("[position_rank] No roster settings available")
        return 0

    # Get years in player_fantasy
    years_result = conn.execute(f"""
        SELECT DISTINCT year FROM {player_table}
        WHERE year IS NOT NULL
          AND {_db_filter(db_name)}
        ORDER BY year
    """).fetchall()
    years = [row[0] for row in years_result]

    logger.info(f"[position_rank] Processing {len(years)} years")

    total_updated = 0
    primary_pos_sql = helpers.primary_position_sql("p.position")

    # Group years by scoring variant to reduce query count
    year_groups = helpers.group_years_by_scoring(years, roster_by_year)
    logger.info(f"[position_rank] {len(year_groups)} scoring variant group(s) for {len(years)} years")

    if not dry_run:
        conn.execute(f"""
            UPDATE {player_table}
            SET position_rank = NULL,
                position_week_rank = NULL,
                position_season_rank = NULL,
                position_alltime_rank = NULL,
                flex_week_rank = NULL,
                flex_season_rank = NULL,
                flex_alltime_rank = NULL,
                sflex_week_rank = NULL,
                sflex_season_rank = NULL,
                sflex_alltime_rank = NULL,
                season_ppg = NULL,
                alltime_ppg = NULL
            WHERE {_db_filter(db_name)}
        """)

    for year_scoring, year_group in year_groups:
        rank_cols_for_year = year_scoring["rank_cols"]
        years_csv = ", ".join(str(y) for y in year_group)

        position_rank_parts = []
        # Use player_bio.nfl_position as canonical position, fall back to
        # s.position then p.position for rank lookup.
        for position in ("QB", "RB", "WR", "TE", "K", "DEF", "LB", "DL", "DB"):
            rank_col = rank_cols_for_year.get(position)
            if rank_col:
                position_rank_parts.append(
                    f"CASE WHEN {helpers.position_eligibility_sql('COALESCE(pb.nfl_position, s.position, p.position)', position)} THEN s.{rank_col} END"
                )

        position_rank_expr = f"COALESCE({', '.join(position_rank_parts)})" if position_rank_parts else "NULL"
        _pos_col = "COALESCE(pb.nfl_position, s.position, p.position)"
        flex_week_expr = (
            f"CASE WHEN {helpers.flex_eligibility_sql(_pos_col, ['RB', 'WR', 'TE'])}"
            f" THEN s.{rank_cols_for_year['FLEX']} ELSE NULL END"
            if rank_cols_for_year.get("FLEX")
            else "NULL"
        )
        flex_season_expr = (
            f"CASE WHEN {helpers.flex_eligibility_sql(_pos_col, ['RB', 'WR', 'TE'])}"
            f" THEN s.{rank_cols_for_year['season_FLEX']} ELSE NULL END"
            if rank_cols_for_year.get("season_FLEX")
            else "NULL"
        )
        flex_alltime_expr = (
            f"CASE WHEN {helpers.flex_eligibility_sql(_pos_col, ['RB', 'WR', 'TE'])}"
            f" THEN s.{rank_cols_for_year['alltime_FLEX']} ELSE NULL END"
            if rank_cols_for_year.get("alltime_FLEX")
            else "NULL"
        )
        sflex_week_expr = (
            f"CASE WHEN {helpers.flex_eligibility_sql(_pos_col, ['QB', 'RB', 'WR', 'TE'])}"
            f" THEN s.{rank_cols_for_year['SUPER_FLEX']} ELSE NULL END"
            if rank_cols_for_year.get("SUPER_FLEX")
            else "NULL"
        )
        sflex_season_expr = (
            f"CASE WHEN {helpers.flex_eligibility_sql(_pos_col, ['QB', 'RB', 'WR', 'TE'])}"
            f" THEN s.{rank_cols_for_year['season_SUPER_FLEX']} ELSE NULL END"
            if rank_cols_for_year.get("season_SUPER_FLEX")
            else "NULL"
        )
        sflex_alltime_expr = (
            f"CASE WHEN {helpers.flex_eligibility_sql(_pos_col, ['QB', 'RB', 'WR', 'TE'])}"
            f" THEN s.{rank_cols_for_year['alltime_SUPER_FLEX']} ELSE NULL END"
            if rank_cols_for_year.get("alltime_SUPER_FLEX")
            else "NULL"
        )

        if not dry_run:
            conn.execute(f"""
                CREATE OR REPLACE TEMP TABLE _position_rank_stage AS
                SELECT DISTINCT
                    p.player_week,
                    {position_rank_expr} AS position_rank,
                    {position_rank_expr} AS position_week_rank,
                    {flex_week_expr} AS flex_week_rank,
                    {flex_season_expr} AS flex_season_rank,
                    {flex_alltime_expr} AS flex_alltime_rank,
                    {sflex_week_expr} AS sflex_week_rank,
                    {sflex_season_expr} AS sflex_season_rank,
                    {sflex_alltime_expr} AS sflex_alltime_rank
                FROM {player_table} p
                JOIN ___ops.nfl_historical.player_bio pb
                  ON CAST(p.NFL_player_id AS VARCHAR) = CAST(pb.NFL_player_id AS VARCHAR)
                LEFT JOIN ___ops.nfl_historical.nfl_player_stats_all s
                  ON p.player_week = s.player_week
                WHERE p.year IN ({years_csv})
                  AND p.NFL_player_id IS NOT NULL
                  AND {_db_filter(db_name, 'p')}
            """)
            conn.execute(f"""
                UPDATE {player_table} p
                SET position_rank = st.position_rank,
                    position_week_rank = st.position_week_rank,
                    flex_week_rank = st.flex_week_rank,
                    flex_season_rank = st.flex_season_rank,
                    flex_alltime_rank = st.flex_alltime_rank,
                    sflex_week_rank = st.sflex_week_rank,
                    sflex_season_rank = st.sflex_season_rank,
                    sflex_alltime_rank = st.sflex_alltime_rank
                FROM _position_rank_stage st
                WHERE p.player_week = st.player_week
                  AND p.year IN ({years_csv})
                  AND {_db_filter(db_name, 'p')}
            """)
            total_updated += conn.execute("SELECT COUNT(*) FROM _position_rank_stage").fetchone()[0]
            conn.execute("DROP TABLE IF EXISTS _position_rank_stage")
        else:
            total_updated += len(year_group)

    logger.info(f"[position_rank] Updated weekly/flex rank aliases for {total_updated:,} player-weeks")

    if not dry_run:
        deduped_primary_pos_sql = helpers.primary_position_sql("d.position")

        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _position_rank_metrics AS
            WITH deduped AS (
                SELECT DISTINCT player_week, year, week, position, fantasy_points
                FROM {player_table}
                WHERE fantasy_points IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(position, '')), '') IS NOT NULL
                  AND {_db_filter(db_name)}
            )
            SELECT
                d.player_week,
                RANK() OVER (
                    PARTITION BY {deduped_primary_pos_sql}, d.year
                    ORDER BY d.fantasy_points DESC NULLS LAST, d.player_week
                ) AS position_season_rank,
                RANK() OVER (
                    PARTITION BY {deduped_primary_pos_sql}
                    ORDER BY d.fantasy_points DESC NULLS LAST, d.year, d.week, d.player_week
                ) AS position_alltime_rank
            FROM deduped d
        """)
        conn.execute(f"""
            UPDATE {player_table} p
            SET position_season_rank = m.position_season_rank,
                position_alltime_rank = m.position_alltime_rank
            FROM _position_rank_metrics m
            WHERE p.player_week = m.player_week
              AND {_db_filter(db_name, 'p')}
        """)
        conn.execute("DROP TABLE IF EXISTS _position_rank_metrics")

        conn.execute(f"""
            CREATE OR REPLACE TEMP TABLE _ppg_stats AS
            WITH deduped AS (
                SELECT DISTINCT player_week, NFL_player_id, year, fantasy_points
                FROM {player_table}
                WHERE NFL_player_id IS NOT NULL
                  AND fantasy_points IS NOT NULL
                  AND {_db_filter(db_name)}
            )
            SELECT DISTINCT
                NFL_player_id,
                year,
                AVG(CASE WHEN fantasy_points > 0 THEN fantasy_points END)
                    OVER (PARTITION BY NFL_player_id, year) AS season_ppg,
                AVG(CASE WHEN fantasy_points > 0 THEN fantasy_points END)
                    OVER (PARTITION BY NFL_player_id) AS alltime_ppg
            FROM deduped
        """)
        conn.execute(f"""
            UPDATE {player_table} p
            SET season_ppg = s.season_ppg,
                alltime_ppg = s.alltime_ppg
            FROM _ppg_stats s
            WHERE p.NFL_player_id = s.NFL_player_id
              AND p.year = s.year
              AND {_db_filter(db_name, 'p')}
        """)
        conn.execute("DROP TABLE IF EXISTS _ppg_stats")

        summary = conn.execute(f"""
            SELECT
                COUNT(position_rank),
                COUNT(position_season_rank),
                COUNT(position_alltime_rank),
                COUNT(flex_week_rank),
                COUNT(sflex_week_rank),
                COUNT(season_ppg),
                COUNT(alltime_ppg)
            FROM {player_table}
            WHERE {_db_filter(db_name)}
        """).fetchone()
        logger.info(
            "[position_rank] counts: "
            f"weekly={summary[0]:,}, season={summary[1]:,}, alltime={summary[2]:,}, "
            f"flex={summary[3]:,}, sflex={summary[4]:,}, "
            f"season_ppg={summary[5]:,}, alltime_ppg={summary[6]:,}"
        )

    return total_updated
