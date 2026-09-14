"""
Draft Enrichments Mixin

Draft dedup, cross-joins, and all draft analytics methods.
"""

import json
import logging
import os
from multi_league.shared.filters import rostered_filter_sql  # noqa: F401 - used in f-strings

logger = logging.getLogger(__name__)


def _detect_draft_type_for_year(settings_row: dict):
    """Detect auction vs snake from a single league_settings row.

    Returns 'auction', 'snake', or None (needs data-based fallback).

    Note: Yahoo's draft_type describes the draft *medium*, not the format:
    - "live" = live online draft (could be snake OR auction)
    - "self" = offline/commissioner-entered (could be snake OR auction)
    - "offline" = offline draft
    Only "auction" and "snake" are unambiguous. Ambiguous types return None
    so the caller falls back to cost-based detection.
    """
    raw_type = settings_row.get("draft_type")
    if raw_type is not None:
        draft_type = str(raw_type).lower().strip()
        if draft_type == "auction":
            return "auction"
        if draft_type in ("snake", "offline_snake", "linear"):
            return "snake"

    metadata = settings_row.get("metadata", "{}")
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except (json.JSONDecodeError, TypeError):
            metadata = {}

    draft_type = metadata.get("draft_type", "").lower()
    # Unambiguous types (Sleeper/ESPN always use these; Yahoo sometimes)
    if draft_type == "auction":
        return "auction"
    if draft_type in ("snake", "offline_snake", "linear"):
        return "snake"
    # Yahoo ambiguous types: "live", "self", "offline" â€” can't determine
    # format from settings alone, caller should check draft cost data
    return None


class DraftEnrichmentsMixin:
    """Mixin providing draft-related SQL enrichments.

    Requires SQLEnrichmentsBase infrastructure (self._execute, self._table_exists, etc.)
    """

    def _keeper_filters(self, draft_cols: set) -> tuple:
        """Build SQL keeper exclusion/inclusion filter fragments.

        Returns (exclude, include) tuple of SQL AND-clause strings.
        Empty strings when no keeper column exists.
        Uses TRY_CAST for safety in case column is still VARCHAR.
        """
        if "is_keeper" in draft_cols:
            return (
                "AND COALESCE(TRY_CAST(is_keeper AS INTEGER), 0) = 0",
                "AND COALESCE(TRY_CAST(is_keeper AS INTEGER), 0) = 1",
            )
        elif "is_keeper_status" in draft_cols:
            return (
                "AND COALESCE(TRY_CAST(is_keeper_status AS INTEGER), 0) = 0",
                "AND COALESCE(TRY_CAST(is_keeper_status AS INTEGER), 0) != 0",
            )
        elif "is_keeper_cost" in draft_cols:
            return (
                "AND COALESCE(TRY_CAST(is_keeper_cost AS INTEGER), 0) = 0",
                "AND COALESCE(TRY_CAST(is_keeper_cost AS INTEGER), 0) > 0",
            )
        return ("", "")

    def _keeper_cohort_expr(self, draft_cols: set, alias: str = "") -> str:
        """Return SQL that buckets rows into keeper-vs-draft grading cohorts."""
        prefix = alias
        if "is_keeper" in draft_cols:
            return f"CASE WHEN COALESCE(TRY_CAST({prefix}is_keeper AS INTEGER), 0) = 1 THEN 'keeper' ELSE 'draft' END"
        if "is_keeper_status" in draft_cols:
            return (
                f"CASE WHEN COALESCE(TRY_CAST({prefix}is_keeper_status AS INTEGER), 0) != 0 "
                "THEN 'keeper' ELSE 'draft' END"
            )
        if "is_keeper_cost" in draft_cols:
            return (
                f"CASE WHEN COALESCE(TRY_CAST({prefix}is_keeper_cost AS INTEGER), 0) > 0 "
                "THEN 'keeper' ELSE 'draft' END"
            )
        return "'draft'"

    @staticmethod
    def _sql_literal(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return int(os.environ.get(name, str(default)))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _find_column(columns: set, *candidates: str) -> str | None:
        by_lower = {str(col).lower(): str(col) for col in columns}
        for candidate in candidates:
            found = by_lower.get(candidate.lower())
            if found:
                return found
        return None

    @classmethod
    def _num_col_expr(cls, columns: set, *candidates: str, default: float = 0.0) -> str:
        col = cls._find_column(columns, *candidates)
        return f"COALESCE(TRY_CAST({col} AS DOUBLE), {float(default)})" if col else str(float(default))

    @classmethod
    def _bool_col_expr(cls, columns: set, *candidates: str) -> str:
        col = cls._find_column(columns, *candidates)
        if not col:
            return "0"
        return f"CASE WHEN COALESCE(TRY_CAST({col} AS BOOLEAN), false) THEN 1 ELSE 0 END"

    @staticmethod
    def _position_norm_expr(column: str) -> str:
        raw = f"UPPER(TRIM(SPLIT_PART(COALESCE({column}, 'UNK'), ',', 1)))"
        return f"CASE WHEN {raw} IN ('D/ST', 'DST', 'D') THEN 'DEF' WHEN {raw} = '' THEN 'UNK' ELSE {raw} END"

    def _draft_market_expr(self, draft_cols: set, alias: str = "") -> str:
        prefix = alias
        keeper_cohort = self._keeper_cohort_expr(draft_cols, prefix)
        if "draft_category" not in draft_cols:
            return f"CASE WHEN ({keeper_cohort}) = 'keeper' THEN 'keeper' ELSE 'redraft' END"
        raw = f"LOWER(TRIM(COALESCE(CAST({prefix}draft_category AS VARCHAR), '')))"
        return (
            f"CASE WHEN ({keeper_cohort}) = 'keeper' THEN 'keeper' "
            f"WHEN {raw} IN ('startup', 'rookie', 'veteran') "
            f"THEN {raw} ELSE 'redraft' END"
        )

    def _draft_kind_expr(self, draft_cols: set, alias: str = "") -> str:
        prefix = alias
        cost_expr = f"COALESCE(TRY_CAST({prefix}cost AS DOUBLE), 0)" if "cost" in draft_cols else "0"
        type_expr = (
            f"LOWER(TRIM(COALESCE(CAST({prefix}draft_type AS VARCHAR), '')))" if "draft_type" in draft_cols else "''"
        )
        return f"CASE WHEN {type_expr} = 'auction' OR {cost_expr} > 0 " "THEN 'auction' ELSE 'snake' END"

    def _assign_pick_grade_percentiles(
        self,
        draft_table: str,
        draft_cols: set,
        lamar_col: str,
        cat_expr: str,
        cat_expr_d: str,
    ) -> int:
        """Assign individual draft grades, partitioned by category and keeper cohort."""
        conn = self._get_connection()

        pool_size = conn.execute(
            f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND (pick_score IS NOT NULL OR {lamar_col} IS NOT NULL)"
        ).fetchone()[0]

        if pool_size < 30:
            try:
                cohort_expr = self._keeper_cohort_expr(draft_cols)
                cat_sizes = conn.execute(
                    f"""SELECT {cat_expr} as cat, {cohort_expr} as cohort, COUNT(*) as cnt
                        FROM {draft_table}
                        WHERE {self._db_filter()} AND (pick_score IS NOT NULL OR {lamar_col} IS NOT NULL)
                        GROUP BY {cat_expr}, {cohort_expr}
                        ORDER BY {cat_expr}, {cohort_expr}"""
                ).fetchall()
                logger.warning(
                    f"[draft_grade] Only {pool_size} total graded picks (need 30), category/cohort sizes: {cat_sizes}"
                )
            except Exception:
                logger.warning(f"[draft_grade] Only {pool_size} graded picks (need 30), skipping grade assignment")
            return 0

        cohort_expr = self._keeper_cohort_expr(draft_cols)
        # Two reasons a partition may have no usable signal for grading:
        #   1. The draft year never produced a played season (e.g. dynasty
        #      startup draft held in year N with first played season in year
        #      N+1; long_live_tbin 2023). All picks have manager_lamar=0 and
        #      pick_score=0, so PERCENT_RANK collapses every pick to the same
        #      bucket â€” grades come out all 'F' which is meaningless.
        #   2. A small partition where every pick happened to score the same
        #      (rare but possible).
        # In either case we'd rather report NULL than "all picks failed."
        # The partition's distinct_scores window count makes this a single-
        # statement gate without a separate query.
        sql_pick_grade = f"""
            WITH ranked AS (
                SELECT year, round, pick,
                       PERCENT_RANK() OVER (
                           PARTITION BY {cat_expr}, {cohort_expr}
                           ORDER BY COALESCE(pick_score, -999) ASC
                       ) * 100 as pctile,
                       COUNT(DISTINCT COALESCE(pick_score, -999)) OVER (
                           PARTITION BY {cat_expr}, {cohort_expr}
                       ) as partition_distinct_scores
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND (pick_score IS NOT NULL
                   OR {lamar_col} IS NOT NULL)
            )
            UPDATE {draft_table} d
            SET draft_grade = CASE
                WHEN r.partition_distinct_scores < 2 THEN NULL
                WHEN r.pctile >= 95 THEN 'A+'
                WHEN r.pctile >= 85 THEN 'A'
                WHEN r.pctile >= 75 THEN 'A-'
                WHEN r.pctile >= 65 THEN 'B+'
                WHEN r.pctile >= 50 THEN 'B'
                WHEN r.pctile >= 35 THEN 'B-'
                WHEN r.pctile >= 20 THEN 'C'
                WHEN r.pctile >= 10 THEN 'D'
                ELSE 'F'
            END
            FROM ranked r
            WHERE d.year = r.year
              AND d.round = r.round
              AND d.pick = r.pick
              AND {self._db_filter('d')}
        """
        return self._execute(sql_pick_grade, "draft_value_zscore: draft_grade (percentile by category + cohort)")

    def draft_to_player(self) -> int:
        """Join draft data to player_fantasy table.

        Adds draft context (round, pick, cost) to each player row.
        This enrichment adds draft columns to player_fantasy if they don't exist,
        then populates them from the draft table.

        Join key: NFL_player_id + year (preferred), or platform-specific IDs

        Replaces: draft_to_player_v2.py
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[draft_to_player] player_fantasy table not found")
            return 0

        if not self._table_exists("draft"):
            logger.warning("[draft_to_player] draft table not found")
            return 0

        player_table = self._qualified_name("player_fantasy")
        draft_table = self._qualified_name("draft")

        # Get available columns
        player_cols = self._get_table_columns("player_fantasy")
        draft_cols = self._get_table_columns("draft")

        # Draft columns to join into player_fantasy (pre-created by canonical_player DDL)
        desired_cols_with_types = {
            "round": "INTEGER",
            "pick": "INTEGER",
            "cost": "DOUBLE",
            "is_keeper": "BOOLEAN",
            "kept_next_year": "BOOLEAN",
            "draft_roi": "DOUBLE",
            "cost_bucket": "VARCHAR",
        }

        # Build SET clause for columns that exist in BOTH player_fantasy AND draft
        # Use TRY_CAST for keeper columns in case draft types are still VARCHAR
        set_clauses = []
        cast_map = {"is_keeper": "TRY_CAST(d.is_keeper AS INTEGER)"}
        for col in desired_cols_with_types.keys():
            if col in draft_cols and col in player_cols:
                set_clauses.append(f"{col} = {cast_map.get(col, f'd.{col}')}")

        if not set_clauses:
            logger.warning("[draft_to_player] No matching columns found in draft table")
            return 0

        join_key = "NFL_player_id"

        logger.info(
            f"[draft_to_player] Using join key: {join_key}, updating columns: {[c.split(' = ')[0] for c in set_clauses]}"
        )

        sql = f"""
            UPDATE {player_table} p
            SET {", ".join(set_clauses)}
            FROM {draft_table} d
            WHERE p.NFL_player_id = d.NFL_player_id
              AND p.year = d.year
              AND {self._db_filter('p')}
              AND {self._db_filter('d')}
        """

        return self._execute(sql, f"draft_to_player: Join draft context (key={join_key})")

    def backfill_draft_positions(self) -> int:
        """Backfill missing draft.position from player_bio.

        ESPN (and some Sleeper) drafts arrive without position. Once
        resolve_all_nfl_player_ids has populated NFL_player_id, we can
        look up position from player_bio.

        Two passes:
          1. Join by NFL_player_id (preferred â€” stable identity)
          2. Name fallback for rows where NFL_player_id is NULL but the
             player name resolves to exactly one player_bio row (handles
             legacy Yahoo picks like 'Shaun Hill' 2009 where the fetcher
             didn't write an ID). Skips ambiguous names and the 'Unknown'
             placeholder.
        """
        if not self._table_exists("draft"):
            return 0

        draft_table = self._qualified_name("draft")
        draft_cols = self._get_table_columns("draft")

        if "position" not in draft_cols or "NFL_player_id" not in draft_cols:
            return 0

        sql_id = f"""
            UPDATE {draft_table} d
            SET position = bio.nfl_position
            FROM ___ops.nfl_historical.player_bio bio
            WHERE d.NFL_player_id = bio.NFL_player_id
              AND bio.nfl_position IS NOT NULL AND TRIM(bio.nfl_position) != ''
              AND (d.position IS NULL OR TRIM(d.position) = '')
              AND {self._db_filter('d')}
        """
        total = self._execute(sql_id, "backfill_draft_positions: NFL_player_id join")

        if "player" not in draft_cols:
            return total

        sql_name = f"""
            UPDATE {draft_table} d
            SET position = bio.nfl_position
            FROM (
                SELECT LOWER(TRIM(player)) AS norm_name,
                       ANY_VALUE(nfl_position) AS nfl_position
                FROM ___ops.nfl_historical.player_bio
                WHERE player IS NOT NULL AND TRIM(player) != ''
                  AND nfl_position IS NOT NULL AND TRIM(nfl_position) != ''
                GROUP BY LOWER(TRIM(player))
                HAVING COUNT(DISTINCT nfl_position) = 1
            ) bio
            WHERE LOWER(TRIM(d.player)) = bio.norm_name
              AND d.NFL_player_id IS NULL
              AND d.player IS NOT NULL
              AND LOWER(TRIM(d.player)) NOT IN ('unknown', '', 'n/a')
              AND (d.position IS NULL OR TRIM(d.position) = '')
              AND {self._db_filter('d')}
        """
        total += self._execute(sql_name, "backfill_draft_positions: name fallback")

        # Pass 3: Name-pattern fallback for non-player roster slots that don't
        # appear in player_bio. njfl drafts NFL head coaches into a custom HC
        # slot (e.g., "Bills Coach", "Buccaneers Coach") â€” bio lookup misses
        # these because head-coach-only entries aren't in the player table.
        # Same shape applies to a Punter slot if a league ever drafts the
        # team's punter as "Bills Punter".
        sql_pattern = f"""
            UPDATE {draft_table} d
            SET position = CASE
                WHEN LOWER(TRIM(d.player)) LIKE '% coach' THEN 'HC'
                WHEN LOWER(TRIM(d.player)) LIKE '% punter' THEN 'P'
            END
            WHERE d.player IS NOT NULL
              AND (
                LOWER(TRIM(d.player)) LIKE '% coach'
                OR LOWER(TRIM(d.player)) LIKE '% punter'
              )
              AND (d.position IS NULL OR TRIM(d.position) = '')
              AND {self._db_filter('d')}
        """
        total += self._execute(sql_pattern, "backfill_draft_positions: HC/P name pattern")
        return total

    def backfill_draft_managers(self) -> int:
        """Backfill N/A or NULL draft managers from player_fantasy via team_key.

        When Yahoo API rate-limits the /teams endpoint during draft fetch,
        picks arrive with team_key (e.g. '399.l.3642.t.2') but manager,
        manager_guid, franchise_id, and team_name are all NULL/N/A.
        player_fantasy has the same team_key format with full identity info.
        """
        if not self._table_exists("draft") or not self._table_exists("player_fantasy"):
            return 0

        draft_table = self._qualified_name("draft")
        draft_cols = self._get_table_columns("draft")
        pf_cols = self._get_table_columns("player_fantasy")

        if "team_key" not in draft_cols or "team_key" not in pf_cols:
            return 0

        # Build SET clause for all available identity columns
        set_parts = ["manager = lkp.manager"]
        join_cols = ["manager"]
        if "manager_guid" in draft_cols and "manager_guid" in pf_cols:
            set_parts.append("manager_guid = lkp.manager_guid")
            join_cols.append("manager_guid")
        if "franchise_id" in draft_cols and "franchise_id" in pf_cols:
            set_parts.append("franchise_id = lkp.franchise_id")
            join_cols.append("franchise_id")
        if "team_name" in draft_cols and "team_name" in pf_cols:
            set_parts.append("team_name = lkp.team_name")
            join_cols.append("team_name")

        pf_table = self._qualified_name("player_fantasy")

        # Pass 1: Backfill manager/franchise_id/manager_guid from player_fantasy
        # Join on team_key + year: values can change year-to-year, never cross years
        sql = f"""
            UPDATE {draft_table} d
            SET {', '.join(set_parts)}
            FROM (
                SELECT DISTINCT year, team_key, {', '.join(join_cols)}
                FROM {pf_table}
                WHERE manager IS NOT NULL
                  AND TRIM(manager) != ''
                  AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                  AND team_key IS NOT NULL
                  AND {self._db_filter()}
            ) lkp
            WHERE d.team_key = lkp.team_key
              AND d.year = lkp.year
              AND (d.manager IS NULL OR TRIM(d.manager) = '' OR d.manager = 'N/A')
              AND {self._db_filter('d')}
        """
        rows = self._execute(sql, "backfill_draft_managers (pass 1: identity from player_fantasy)")

        # Pass 2: Backfill team_name from matchup using franchise_id + year
        # (player_fantasy doesn't carry team_name, but matchup does)
        if "team_name" in draft_cols and "franchise_id" in draft_cols and self._table_exists("matchup"):
            matchup_cols = self._get_table_columns("matchup")
            if "franchise_id" in matchup_cols and "team_name" in matchup_cols:
                matchup_table = self._qualified_name("matchup")
                sql2 = f"""
                UPDATE {draft_table} d
                SET team_name = lkp.team_name
                FROM (
                    SELECT DISTINCT year, franchise_id, team_name
                    FROM {matchup_table}
                        WHERE franchise_id IS NOT NULL
                          AND team_name IS NOT NULL AND TRIM(team_name) != ''
                          AND {self._db_filter()}
                ) lkp
                WHERE d.franchise_id = lkp.franchise_id
                  AND d.year = lkp.year
                  AND (d.team_name IS NULL OR TRIM(d.team_name) = '')
                  AND {self._db_filter('d')}
                """
                rows2 = self._execute(sql2, "backfill_draft_managers (pass 2: team_name from matchup)")
                rows = (rows or 0) + (rows2 or 0)

        return rows

    def player_to_draft(self) -> int:
        """Aggregate season stats to draft table.

        Adds total_fantasy_points, games_played, season_ppg, rankings.
        Join key: (player_id, year)

        Replaces: player_to_draft_v2.py
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[player_to_draft] player_fantasy table not found")
            return 0

        if not self._table_exists("draft"):
            logger.warning("[player_to_draft] draft table not found")
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        draft_cols = self._get_table_columns("draft")

        join_key = "NFL_player_id"

        # Use schema-qualified table names
        player_table = self._qualified_name("player_fantasy")
        draft_table = self._qualified_name("draft")

        # Columns pre-created by canonical_draft DDL

        # Build aggregation based on available columns
        agg_parts = [f"{join_key}", "year"]
        set_parts = []

        if "fantasy_points" in player_cols:
            if "total_fantasy_points" in draft_cols:
                # Floating-point reduction order can vary between DuckDB
                # executions. Round the persisted derived value so identical
                # imports produce an identical draft table.
                agg_parts.append("ROUND(SUM(COALESCE(fantasy_points, 0)), 6) as total_fantasy_points")
                set_parts.append("total_fantasy_points = s.total_fantasy_points")
            if "season_ppg" in draft_cols:
                agg_parts.append("AVG(fantasy_points) as season_ppg")
                set_parts.append("season_ppg = ROUND(s.season_ppg, 2)")

        if "week" in player_cols and ("games_played" in draft_cols or "weeks_rostered" in draft_cols):
            # games_played and weeks_rostered are identical (both COUNT(DISTINCT week))
            # Compute once, assign to both columns
            agg_parts.append("COUNT(DISTINCT week) as games_played")
            if "games_played" in draft_cols:
                set_parts.append("games_played = s.games_played")
            if "weeks_rostered" in draft_cols:
                set_parts.append("weeks_rostered = s.games_played")

        if "is_started" in player_cols and ("games_started" in draft_cols or "weeks_started" in draft_cols):
            # games_started and weeks_started are identical (both SUM(is_started))
            # Compute once, assign to both columns
            agg_parts.append("SUM(CASE WHEN CAST(is_started AS INTEGER) = 1 THEN 1 ELSE 0 END) as games_started")
            if "games_started" in draft_cols:
                set_parts.append("games_started = s.games_started")
            if "weeks_started" in draft_cols:
                set_parts.append("weeks_started = s.games_started")

        if "manager_lamar" in player_cols and "manager_lamar" in draft_cols:
            agg_parts.append("SUM(COALESCE(manager_lamar, 0)) as manager_lamar")
            set_parts.append("manager_lamar = s.manager_lamar")

        if "player_lamar" in player_cols and "player_lamar" in draft_cols:
            agg_parts.append("SUM(COALESCE(player_lamar, 0)) as player_lamar")
            set_parts.append("player_lamar = s.player_lamar")

        if not set_parts:
            logger.warning("[player_to_draft] No columns to aggregate")
            return 0

        sql = f"""
            WITH season_stats AS (
                SELECT
                    {", ".join(agg_parts)}
                FROM {player_table}
                WHERE NFL_player_id IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY NFL_player_id, year
            )
            UPDATE {draft_table} d
            SET {", ".join(set_parts)}
            FROM season_stats s
            WHERE d.NFL_player_id = s.NFL_player_id
              AND d.year = s.year
              AND {self._db_filter('d')}
        """

        rows = self._execute(sql, f"player_to_draft: Aggregate season stats (key={join_key})")

        # Calculate season_position_rank: rank within position-year by total points
        draft_cols = self._get_table_columns("draft")
        pos_col = (
            "yahoo_position" if "yahoo_position" in draft_cols else ("position" if "position" in draft_cols else None)
        )

        if pos_col and "season_position_rank" in draft_cols and "total_fantasy_points" in draft_cols:
            sql_rank = f"""
                UPDATE {draft_table}
                SET season_position_rank = ranked.pos_rank
                FROM (
                SELECT rowid AS rid,
                        RANK() OVER (
                            PARTITION BY year, {pos_col}
                            ORDER BY COALESCE(total_fantasy_points, 0) DESC
                        ) AS pos_rank
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND {pos_col} IS NOT NULL
                      AND total_fantasy_points IS NOT NULL
                ) ranked
                WHERE {draft_table}.rowid = ranked.rid
                  AND {self._db_filter()}
            """
            rows += self._execute(sql_rank, "player_to_draft: season_position_rank")

        return rows

    def draft_manager_aggregates(self) -> int:
        """Calculate manager-level draft aggregates for report cards.

        Adds to draft table:
        - manager_total_lamar: Sum of manager_lamar for each manager-year
        - manager_hit_rate: Percentage of picks with positive LAMAR
        - manager_draft_percentile_alltime: Percentile rank across all manager-years
        - total_fantasy_points: Sum of fantasy points (if player_fantasy has data)

        These are broadcast back to each pick row for easy access in report cards.
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_manager_aggregates] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")

        # Columns pre-created by canonical_draft DDL

        # Check if we have manager_lamar to work with
        lamar_col = None
        if "manager_lamar" in draft_cols:
            lamar_col = "manager_lamar"
        elif "lamar" in draft_cols:
            lamar_col = "lamar"

        if not lamar_col:
            logger.warning("[draft_manager_aggregates] No LAMAR column found - skipping aggregates")
            return 0

        rows_updated = 0

        # Step 1: Calculate manager_total_lamar, manager_avg_lamar, manager_picks_count, manager_hit_rate
        sql_manager_stats = f"""
            WITH manager_stats AS (
                SELECT
                    franchise_id,
                    year,
                    SUM(COALESCE({lamar_col}, 0)) as total_lamar,
                    ROUND(AVG(COALESCE({lamar_col}, 0)), 2) as avg_lamar,
                    COUNT(*) as pick_count,
                    SUM(CASE WHEN {lamar_col} > 0 THEN 1 ELSE 0 END) as hit_count,
                    ROUND(SUM(CASE WHEN {lamar_col} > 0 THEN 1 ELSE 0 END) * 100.0 / NULLIF(COUNT(*), 0), 1) as hit_rate
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL
                GROUP BY franchise_id, year
            )
            UPDATE {draft_table} d
            SET
                manager_total_lamar = ms.total_lamar,
                manager_avg_lamar = ms.avg_lamar,
                manager_picks_count = ms.pick_count,
                manager_hit_rate = ms.hit_rate
            FROM manager_stats ms
            WHERE d.franchise_id = ms.franchise_id AND d.year = ms.year
              AND {self._db_filter('d')}
        """
        rows_updated += self._execute(sql_manager_stats, "draft_manager_aggregates: manager stats")

        # Step 2: Calculate manager_draft_percentile_alltime (percentile across all manager-years)
        sql_percentile = f"""
            WITH manager_yearly_lamar AS (
                SELECT
                    franchise_id,
                    year,
                    SUM(COALESCE({lamar_col}, 0)) as total_lamar
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL
                GROUP BY franchise_id, year
            ),
            manager_percentiles AS (
                SELECT
                    franchise_id,
                    year,
                    ROUND(PERCENT_RANK() OVER (ORDER BY total_lamar) * 100, 1) as percentile
                FROM manager_yearly_lamar
            )
            UPDATE {draft_table} d
            SET manager_draft_percentile_alltime = mp.percentile
            FROM manager_percentiles mp
            WHERE d.franchise_id = mp.franchise_id AND d.year = mp.year
              AND {self._db_filter('d')}
        """
        rows_updated += self._execute(sql_percentile, "draft_manager_aggregates: manager_draft_percentile_alltime")

        # Step 3: Populate total_fantasy_points from player_fantasy if available
        if self._table_exists("player_fantasy"):
            player_cols = self._get_table_columns("player_fantasy")
            player_table = self._qualified_name("player_fantasy")

            if "fantasy_points" in player_cols and "NFL_player_id" in player_cols and "NFL_player_id" in draft_cols:
                sql_points = f"""
                    WITH season_points AS (
                    SELECT
                        NFL_player_id,
                        year,
                        ROUND(SUM(COALESCE(fantasy_points, 0)), 6) as total_pts
                    FROM {player_table}
                        WHERE NFL_player_id IS NOT NULL
                          AND {self._db_filter()}
                        GROUP BY NFL_player_id, year
                    )
                    UPDATE {draft_table} d
                    SET total_fantasy_points = sp.total_pts
                    FROM season_points sp
                    WHERE d.NFL_player_id = sp.NFL_player_id
                      AND d.year = sp.year
                      AND {self._db_filter('d')}
                """
                rows_updated += self._execute(
                    sql_points, "draft_manager_aggregates: total_fantasy_points (key=NFL_player_id)"
                )

        return rows_updated

    def _get_draft_type_per_year(self) -> dict:
        """Resolve {year: 'auction'|'snake'} independently for each league-year.

        Detection order:
        1. Explicit draft.draft_type on that year's draft rows.
        2. Explicit draft_type from that year's league_settings row.
        3. Year-scoped cost fallback when no explicit type is available.
        4. Default: snake.

        External draft uploads can carry draft type per row/year. Those values
        are treated as per-year settings, not as a league-wide majority signal.
        """
        draft_table = self._qualified_name("draft")
        settings_table = self._qualified_name("league_settings")
        conn = self._get_connection()

        # Get all years from draft
        try:
            years_result = conn.execute(
                f"SELECT DISTINCT year FROM {draft_table} WHERE {self._db_filter()} AND year IS NOT NULL ORDER BY year"
            ).fetchall()
        except Exception:
            return {}

        all_years = [int(r[0]) for r in years_result]
        if not all_years:
            return {}

        draft_cols = self._get_table_columns("draft")
        result = {}
        settings_cols: set[str] = set()
        try:
            settings_cols = set(self._get_table_columns("league_settings"))
        except Exception:
            settings_cols = set()

        # Draft rows can carry explicit external draft_type values after staging
        # merge. Resolve them year-by-year before consulting league_settings.
        try:
            draft_type_rows = conn.execute(f"""
                SELECT year, LOWER(TRIM(COALESCE(CAST(draft_type AS VARCHAR), ''))) AS draft_type
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND year IS NOT NULL
                  AND draft_type IS NOT NULL
            """).fetchall()
            draft_types_by_year: dict[int, set[str]] = {}
            for year, draft_type in draft_type_rows:
                detected = _detect_draft_type_for_year({"draft_type": draft_type})
                if detected:
                    draft_types_by_year.setdefault(int(year), set()).add(detected)
            for year, detected_types in draft_types_by_year.items():
                if len(detected_types) == 1:
                    result[year] = next(iter(detected_types))
        except Exception:  # noqa: BLE001
            pass

        # Try league_settings draft_type/metadata (if available)
        if "draft_type" in settings_cols:
            try:
                settings_df = conn.execute(
                    f"SELECT year, draft_type FROM {settings_table} WHERE {self._db_filter()} AND year IS NOT NULL"
                ).fetchdf()
                for _, row in settings_df.iterrows():
                    year = int(row["year"])
                    detected = _detect_draft_type_for_year(row.to_dict())
                    if detected and year not in result:
                        result[year] = detected
            except Exception:
                pass

        if "metadata" in settings_cols:
            try:
                settings_df = conn.execute(
                    f"SELECT year, metadata FROM {settings_table} WHERE {self._db_filter()} AND year IS NOT NULL"
                ).fetchdf()
                for _, row in settings_df.iterrows():
                    year = int(row["year"])
                    detected = _detect_draft_type_for_year(row.to_dict())
                    if detected and year not in result:
                        result[year] = detected
            except Exception:
                pass  # league_settings may not exist or lack metadata

        # Cost fallback only fills years where neither draft rows nor settings
        # provided an explicit type. It is still scoped to one year at a time.
        missing_years = [y for y in all_years if y not in result]
        if missing_years:
            years_list = ", ".join(str(y) for y in missing_years)
            non_keeper_filter = (
                "COALESCE(TRY_CAST(is_keeper AS INTEGER), 0) = 0 AND " if "is_keeper" in draft_cols else ""
            )
            try:
                fb_rows = conn.execute(f"""
                    SELECT year,
                           COUNT(*) FILTER (
                               WHERE {non_keeper_filter}COALESCE(cost, 0) > 0
                           ) AS priced_non_keeper_picks
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND year IN ({years_list})
                    GROUP BY year
                """).fetchall()
                for year, priced_non_keeper_picks in fb_rows:
                    if priced_non_keeper_picks > 0:
                        result[int(year)] = "auction"
            except Exception:  # noqa: BLE001
                pass

        # Fill any remaining years as snake
        for y in all_years:
            if y not in result:
                result[y] = "snake"

        logger.info(f"[draft_type_per_year] {result}")
        return result

    def normalize_draft_type_per_year(self) -> int:
        """Normalize draft.draft_type to a single value per league-year.

        Uses canonical per-year detection and overwrites draft.draft_type plus
        league_settings.draft_type for that same year so downstream code sees
        one format per league-year.
        """
        if not self._table_exists("draft"):
            return 0

        draft_cols = self._get_table_columns("draft")
        if "draft_type" not in draft_cols:
            return 0

        draft_table = self._qualified_name("draft")
        draft_types = self._get_draft_type_per_year()
        if not draft_types:
            return 0

        total = 0
        for year, dtype in draft_types.items():
            dtype = str(dtype).lower().strip()
            if dtype not in ("snake", "auction"):
                continue
            sql = f"""
                UPDATE {draft_table}
                SET draft_type = '{dtype}'
                WHERE year = {year}
                  AND {self._db_filter()}
                  AND COALESCE(TRIM(CAST(draft_type AS VARCHAR)), '') != '{dtype}'
            """
            total += self._execute(sql, f"normalize_draft_type_per_year: year={year} -> {dtype}")

        if self._table_exists("league_settings"):
            settings_cols = self._get_table_columns("league_settings")
            if "draft_type" in settings_cols:
                settings_table = self._qualified_name("league_settings")
                for year, dtype in draft_types.items():
                    dtype = str(dtype).lower().strip()
                    if dtype not in ("snake", "auction"):
                        continue
                    sql = f"""
                        UPDATE {settings_table}
                        SET draft_type = '{dtype}'
                        WHERE year = {year}
                          AND {self._db_filter()}
                          AND COALESCE(TRIM(CAST(draft_type AS VARCHAR)), '') != '{dtype}'
                    """
                    total += self._execute(sql, f"normalize_league_settings_draft_type: year={year} -> {dtype}")

        return total

    def _league_settings_profile_cte(self) -> str:
        """Return a settings CTE for draft environment similarity."""
        empty = """
            SELECT CAST(NULL AS INTEGER) AS year, 0.0 AS num_teams, 0.0 AS draft_rounds,
                   0.0 AS scoring_rec, 4.0 AS scoring_pass_td,
                   0.0 AS roster_qb, 0.0 AS roster_rb, 0.0 AS roster_wr, 0.0 AS roster_te,
                   0.0 AS roster_k, 0.0 AS roster_def, 0.0 AS roster_flex,
                   0.0 AS roster_super_flex, 0.0 AS roster_rec_flex, 0.0 AS roster_bn,
                   0.0 AS idp_slots, 0.0 AS max_keepers, 0 AS is_dynasty,
                   0.0 AS taxi_slots, 0 AS pick_trading, 0 AS uses_median
            WHERE 1 = 0
        """
        if not self._table_exists("league_settings"):
            return f"settings AS ({empty})"

        cols = self._get_table_columns("league_settings")
        settings_table = self._qualified_name("league_settings")
        roster_qb = self._num_col_expr(cols, "roster_QB", "roster_qb")
        roster_rb = self._num_col_expr(cols, "roster_RB", "roster_rb")
        roster_wr = self._num_col_expr(cols, "roster_WR", "roster_wr")
        roster_te = self._num_col_expr(cols, "roster_TE", "roster_te")
        roster_k = self._num_col_expr(cols, "roster_K", "roster_k")
        roster_def = self._num_col_expr(cols, "roster_DEF", "roster_def")
        roster_flex = self._num_col_expr(cols, "roster_FLX", "roster_flex", "roster_WRRB_FLEX")
        roster_super_flex = self._num_col_expr(cols, "roster_SUPER_FLEX", "roster_Q/W/R/T", "roster_QB_RB_WR_TE")
        roster_rec_flex = self._num_col_expr(cols, "roster_REC_FLEX", "roster_REC")
        idp_slots = " + ".join(
            [
                self._num_col_expr(cols, "roster_LB", "roster_lb"),
                self._num_col_expr(cols, "roster_DL", "roster_dl"),
                self._num_col_expr(cols, "roster_DB", "roster_db"),
                self._num_col_expr(cols, "roster_IDP", "roster_idp"),
                self._num_col_expr(cols, "roster_DB_LB", "roster_db_lb"),
                self._num_col_expr(cols, "roster_DL_LB", "roster_dl_lb"),
            ]
        )
        return f"""
            settings AS (
                SELECT
                    year,
                    MAX({self._num_col_expr(cols, "num_teams")}) AS num_teams,
                    MAX({self._num_col_expr(cols, "draft_rounds")}) AS draft_rounds,
                    MAX({self._num_col_expr(cols, "scoring_rec")}) AS scoring_rec,
                    MAX({self._num_col_expr(cols, "scoring_pass_td", default=4)}) AS scoring_pass_td,
                    MAX({roster_qb}) AS roster_qb,
                    MAX({roster_rb}) AS roster_rb,
                    MAX({roster_wr}) AS roster_wr,
                    MAX({roster_te}) AS roster_te,
                    MAX({roster_k}) AS roster_k,
                    MAX({roster_def}) AS roster_def,
                    MAX({roster_flex}) AS roster_flex,
                    MAX({roster_super_flex}) AS roster_super_flex,
                    MAX({roster_rec_flex}) AS roster_rec_flex,
                    MAX({self._num_col_expr(cols, "roster_BN", "roster_bn", "bench_count")}) AS roster_bn,
                    MAX({idp_slots}) AS idp_slots,
                    MAX({self._num_col_expr(cols, "max_keepers")}) AS max_keepers,
                    MAX({self._bool_col_expr(cols, "is_dynasty")}) AS is_dynasty,
                    MAX({self._num_col_expr(cols, "sleeper_taxi_slots", "taxi_slots")}) AS taxi_slots,
                    MAX({self._bool_col_expr(cols, "sleeper_pick_trading", "pick_trading")}) AS pick_trading,
                    MAX({self._bool_col_expr(cols, "uses_median")}) AS uses_median
                FROM {settings_table}
                WHERE {self._db_filter()}
                GROUP BY year
            )
        """

    def _create_draft_score_rows_temp(
        self,
        draft_table: str,
        draft_cols: set,
        lamar_col: str,
        pos_col: str,
    ) -> int:
        """Materialize draft score features for local picks."""
        conn = self._get_connection()
        cohort_expr = self._keeper_cohort_expr(draft_cols, "d.")
        market_expr = self._draft_market_expr(draft_cols, "d.")
        kind_expr = self._draft_kind_expr(draft_cols, "d.")
        position_expr = self._position_norm_expr(f"d.{pos_col}")
        cost_expr = "COALESCE(TRY_CAST(d.cost AS DOUBLE), 0)" if "cost" in draft_cols else "0"
        round_expr = "TRY_CAST(d.round AS INTEGER)" if "round" in draft_cols else "NULL"
        pick_expr = "TRY_CAST(d.pick AS INTEGER)" if "pick" in draft_cols else "NULL"
        manager_expr = "d.manager" if "manager" in draft_cols else "NULL"
        franchise_expr = "d.franchise_id" if "franchise_id" in draft_cols else "NULL"
        player_expr = "d.player" if "player" in draft_cols else "NULL"
        draft_id_expr = "d.draft_id" if "draft_id" in draft_cols else "NULL"
        settings_cte = self._league_settings_profile_cte()

        conn.execute("DROP TABLE IF EXISTS _draft_score_rows")
        conn.execute(
            f"""
            CREATE TEMP TABLE _draft_score_rows AS
            WITH raw AS (
                SELECT d.year, {round_expr} AS round, {pick_expr} AS pick,
                       {manager_expr} AS manager, {franchise_expr} AS franchise_id,
                       {player_expr} AS player, {draft_id_expr} AS draft_id,
                       {position_expr} AS position,
                       {cohort_expr} AS cohort, {market_expr} AS draft_market,
                       {kind_expr} AS draft_kind, {cost_expr} AS cost,
                       TRY_CAST(d.{lamar_col} AS DOUBLE) AS manager_lamar
                FROM {draft_table} d
                WHERE {self._db_filter('d')}
                  AND d.year IS NOT NULL
                  AND d.{lamar_col} IS NOT NULL
            ),
            valid_years AS (
                SELECT year
                FROM raw
                GROUP BY year
                HAVING SUM(ABS(COALESCE(manager_lamar, 0))) > 0
            ),
            eligible_raw AS (
                SELECT r.*
                FROM raw r
                JOIN valid_years v ON r.year = v.year
            ),
            year_stats AS (
                SELECT year, draft_market, draft_kind, cohort,
                       COUNT(*) AS picks_in_market,
                       COUNT(DISTINCT manager) AS managers_in_market,
                       MAX(COALESCE(round, 0)) AS max_round,
                       MAX(COALESCE(pick, 0)) AS max_pick,
                       SUM(COALESCE(cost, 0)) AS total_cost
                FROM eligible_raw
                GROUP BY year, draft_market, draft_kind, cohort
            ),
            {settings_cte},
            profiled AS (
                SELECT r.*,
                       COALESCE(NULLIF(s.num_teams, 0), NULLIF(ys.managers_in_market, 0), 12) AS teams,
                       COALESCE(NULLIF(s.draft_rounds, 0), NULLIF(ys.max_round, 0), 16) AS draft_rounds,
                       COALESCE(s.scoring_rec, 0) AS scoring_rec,
                       COALESCE(s.scoring_pass_td, 4) AS pass_td_pts,
                       COALESCE(s.roster_qb, 0) AS roster_qb,
                       COALESCE(s.roster_rb, 0) AS roster_rb,
                       COALESCE(s.roster_wr, 0) AS roster_wr,
                       COALESCE(s.roster_te, 0) AS roster_te,
                       COALESCE(s.roster_k, 0) AS roster_k,
                       COALESCE(s.roster_def, 0) AS roster_def,
                       COALESCE(s.roster_flex, 0) AS roster_flex,
                       COALESCE(s.roster_super_flex, 0) AS roster_super_flex,
                       COALESCE(s.roster_rec_flex, 0) AS roster_rec_flex,
                       COALESCE(s.roster_bn, 0) AS bench_count,
                       COALESCE(s.idp_slots, 0) AS idp_slots,
                       COALESCE(s.max_keepers, 0) AS max_keepers,
                       COALESCE(s.is_dynasty, 0) AS is_dynasty,
                       COALESCE(s.taxi_slots, 0) AS taxi_slots,
                       COALESCE(s.pick_trading, 0) AS pick_trading,
                       COALESCE(s.uses_median, 0) AS uses_median,
                       ys.picks_in_market, ys.managers_in_market, ys.max_round,
                       ys.max_pick, ys.total_cost
                FROM eligible_raw r
                JOIN year_stats ys
                  ON r.year = ys.year
                 AND r.draft_market = ys.draft_market
                 AND r.draft_kind = ys.draft_kind
                 AND r.cohort = ys.cohort
                LEFT JOIN settings s ON r.year = s.year
            )
            SELECT ROW_NUMBER() OVER () AS score_row_id, *,
                   CASE
                       WHEN draft_kind = 'auction' THEN
                           CASE
                               WHEN cost <= 0 THEN 'a_free'
                               WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.30 THEN 'a_30p'
                               WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.20 THEN 'a_20_29p'
                               WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.10 THEN 'a_10_19p'
                               WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.05 THEN 'a_05_09p'
                               ELSE 'a_01_04p'
                           END
                       ELSE
                           's_' || LPAD(CAST(LEAST(20, GREATEST(1,
                               CEIL(((COALESCE(pick, round, picks_in_market) - 1) * 20.0)
                               / GREATEST(picks_in_market, 1)))) AS VARCHAR), 2, '0')
                   END AS capital_bucket,
                   CASE WHEN roster_super_flex > 0 OR roster_qb >= 2 THEN 1 ELSE 0 END AS superflex,
                   CASE WHEN idp_slots > 0 THEN 1 ELSE 0 END AS idp,
                   roster_qb + roster_rb + roster_wr + roster_te + roster_k + roster_def
                       + roster_flex + roster_super_flex + roster_rec_flex + idp_slots + bench_count
                       AS total_roster_slots,
                   roster_flex + roster_super_flex + roster_rec_flex AS flex_count,
                   CASE
                       WHEN is_dynasty = 1 OR taxi_slots > 0 OR pick_trading = 1
                         OR draft_market IN ('startup', 'rookie', 'veteran')
                       THEN 1 ELSE 0
                   END AS dynasty_like,
                   CASE WHEN max_keepers > 0 OR cohort = 'keeper' THEN 1 ELSE 0 END AS keeper_like
            FROM profiled
            """
        )
        return int(conn.execute("SELECT COUNT(*) FROM _draft_score_rows").fetchone()[0] or 0)

    def _fetch_global_draft_score_baseline(self) -> int:
        """Fetch weighted global draft baselines from Fly into a temp table."""
        conn = self._get_connection()
        conn.execute("DROP TABLE IF EXISTS _draft_global_baseline")
        conn.execute(
            """
            CREATE TEMP TABLE _draft_global_baseline (
                profile_year INTEGER,
                draft_market VARCHAR,
                draft_kind VARCHAR,
                cohort VARCHAR,
                position VARCHAR,
                capital_bucket VARCHAR,
                mean_lamar DOUBLE,
                std_lamar DOUBLE,
                sample_size BIGINT,
                effective_n DOUBLE
            )
            """
        )
        if os.environ.get("DRAFT_SCORE_GLOBAL_BASELINE", "1").lower() in {"0", "false", "no"}:
            return 0
        profiles = conn.execute(
            """
            SELECT DISTINCT year AS profile_year, draft_market, draft_kind, cohort,
                   teams, draft_rounds, total_roster_slots, bench_count, flex_count,
                   superflex, idp, scoring_rec, pass_td_pts, dynasty_like, keeper_like, uses_median
            FROM _draft_score_rows
            ORDER BY profile_year, draft_market, draft_kind, cohort
            """
        ).fetchall()
        if not profiles:
            return 0

        def num(value: object, default: float = 0.0) -> float:
            try:
                return float(value)
            except (TypeError, ValueError):
                return default

        target_values = ",\n".join(
            "("
            + ", ".join(
                [
                    str(int(row[0])),
                    self._sql_literal(row[1]),
                    self._sql_literal(row[2]),
                    self._sql_literal(row[3]),
                    str(num(row[4], 12)),
                    str(num(row[5], 16)),
                    str(num(row[6], 0)),
                    str(num(row[7], 0)),
                    str(num(row[8], 0)),
                    str(int(num(row[9], 0))),
                    str(int(num(row[10], 0))),
                    str(num(row[11], 0)),
                    str(num(row[12], 4)),
                    str(int(num(row[13], 0))),
                    str(int(num(row[14], 0))),
                    str(int(num(row[15], 0))),
                ]
            )
            + ")"
            for row in profiles
        )
        db_lit = self._sql_literal(self.db_name)

        local_count = self._fetch_global_draft_score_baseline_from_local_source(target_values, db_lit)
        if local_count is not None:
            return local_count

        from multi_league.core.runtime_mode import is_corpus_mode

        if is_corpus_mode():
            raise RuntimeError(
                "CORPUS_MODE requires a valid DRAFT_GLOBAL_SOURCE_PATH; "
                "production Fly fallback is disabled"
            )
        if not os.environ.get("DATABASE_SERVER_URL") or not os.environ.get("DATABASE_READ_TOKEN"):
            logger.info("[draft_score] Fly credentials missing; using local baseline only")
            return 0

        sql = f"""
            WITH targets(
                profile_year, draft_market, draft_kind, cohort, teams, draft_rounds,
                total_roster_slots, bench_count, flex_count, superflex, idp,
                scoring_rec, pass_td_pts, dynasty_like, keeper_like, uses_median
            ) AS (VALUES {target_values}),
            raw AS (
                SELECT d.db_name, d.year, TRY_CAST(d.round AS INTEGER) AS round,
                       TRY_CAST(d.pick AS INTEGER) AS pick, d.manager,
                       CASE
                         WHEN UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1))) IN ('D/ST', 'DST', 'D')
                           THEN 'DEF'
                         WHEN UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1))) = ''
                           THEN 'UNK'
                         ELSE UPPER(TRIM(SPLIT_PART(COALESCE(d.position, 'UNK'), ',', 1)))
                       END AS position,
                       CASE WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1 THEN 'keeper' ELSE 'draft' END AS cohort,
                       CASE
                         WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1
                           THEN 'keeper'
                         WHEN LOWER(TRIM(COALESCE(CAST(d.draft_category AS VARCHAR), ''))) IN ('startup', 'rookie', 'veteran')
                           THEN LOWER(TRIM(COALESCE(CAST(d.draft_category AS VARCHAR), '')))
                         ELSE 'redraft'
                       END AS draft_market,
                       CASE
                         WHEN LOWER(TRIM(COALESCE(CAST(d.draft_type AS VARCHAR), ''))) = 'auction'
                           OR COALESCE(TRY_CAST(d.cost AS DOUBLE), 0) > 0
                         THEN 'auction' ELSE 'snake'
                       END AS draft_kind,
                       COALESCE(TRY_CAST(d.cost AS DOUBLE), 0) AS cost,
                       TRY_CAST(d.manager_lamar AS DOUBLE) AS manager_lamar
                FROM public.draft d
                WHERE d.db_name IS NOT NULL
                  AND d.db_name != {db_lit}
                  AND d.year IS NOT NULL
                  AND d.manager_lamar IS NOT NULL
            ),
            year_stats AS (
                SELECT db_name, year, draft_market, draft_kind, cohort,
                       COUNT(*) AS picks_in_market,
                       COUNT(DISTINCT manager) AS managers_in_market,
                       MAX(COALESCE(round, 0)) AS max_round,
                       SUM(COALESCE(cost, 0)) AS total_cost
                FROM raw
                GROUP BY db_name, year, draft_market, draft_kind, cohort
            ),
            settings AS (
                SELECT db_name, year,
                       MAX(COALESCE(num_teams, 0)) AS num_teams,
                       MAX(COALESCE(draft_rounds, 0)) AS draft_rounds,
                       MAX(COALESCE(scoring_rec, 0)) AS scoring_rec,
                       MAX(COALESCE(scoring_pass_td, 4)) AS scoring_pass_td,
                       MAX(COALESCE(roster_QB, 0)) AS roster_qb,
                       MAX(COALESCE(roster_RB, 0)) AS roster_rb,
                       MAX(COALESCE(roster_WR, 0)) AS roster_wr,
                       MAX(COALESCE(roster_TE, 0)) AS roster_te,
                       MAX(COALESCE(roster_K, 0)) AS roster_k,
                       MAX(COALESCE(roster_DEF, 0)) AS roster_def,
                       MAX(COALESCE(roster_FLX, 0)) AS roster_flex,
                       MAX(COALESCE(roster_SUPER_FLEX, 0)) AS roster_super_flex,
                       MAX(COALESCE(roster_REC_FLEX, 0)) AS roster_rec_flex,
                       MAX(COALESCE(roster_BN, 0)) AS roster_bn,
                       MAX(COALESCE(roster_LB, 0) + COALESCE(roster_DL, 0) + COALESCE(roster_DB, 0)
                           + COALESCE(roster_IDP, 0) + COALESCE(roster_DB_LB, 0) + COALESCE(roster_DL_LB, 0)) AS idp_slots,
                       MAX(COALESCE(max_keepers, 0)) AS max_keepers,
                       MAX(CASE WHEN COALESCE(is_dynasty, false) THEN 1 ELSE 0 END) AS is_dynasty,
                       MAX(COALESCE(sleeper_taxi_slots, 0)) AS taxi_slots,
                       MAX(CASE WHEN COALESCE(sleeper_pick_trading, false) THEN 1 ELSE 0 END) AS pick_trading,
                       MAX(CASE WHEN COALESCE(uses_median, false) THEN 1 ELSE 0 END) AS uses_median
                FROM public.league_settings
                GROUP BY db_name, year
            ),
            profiled AS (
                SELECT r.*,
                       COALESCE(NULLIF(s.num_teams, 0), NULLIF(ys.managers_in_market, 0), 12) AS teams,
                       COALESCE(NULLIF(s.draft_rounds, 0), NULLIF(ys.max_round, 0), 16) AS draft_rounds,
                       COALESCE(s.scoring_rec, 0) AS scoring_rec,
                       COALESCE(s.scoring_pass_td, 4) AS pass_td_pts,
                       CASE WHEN COALESCE(s.roster_super_flex, 0) > 0 OR COALESCE(s.roster_qb, 0) >= 2 THEN 1 ELSE 0 END AS superflex,
                       CASE WHEN COALESCE(s.idp_slots, 0) > 0 THEN 1 ELSE 0 END AS idp,
                       COALESCE(s.roster_bn, 0) AS bench_count,
                       COALESCE(s.roster_flex, 0) + COALESCE(s.roster_super_flex, 0) + COALESCE(s.roster_rec_flex, 0) AS flex_count,
                       COALESCE(s.roster_qb, 0) + COALESCE(s.roster_rb, 0) + COALESCE(s.roster_wr, 0)
                         + COALESCE(s.roster_te, 0) + COALESCE(s.roster_k, 0) + COALESCE(s.roster_def, 0)
                         + COALESCE(s.roster_flex, 0) + COALESCE(s.roster_super_flex, 0)
                         + COALESCE(s.roster_rec_flex, 0) + COALESCE(s.idp_slots, 0) + COALESCE(s.roster_bn, 0)
                         AS total_roster_slots,
                       CASE
                         WHEN COALESCE(s.is_dynasty, 0) = 1 OR COALESCE(s.taxi_slots, 0) > 0
                           OR COALESCE(s.pick_trading, 0) = 1 OR r.draft_market IN ('startup', 'rookie', 'veteran')
                         THEN 1 ELSE 0
                       END AS dynasty_like,
                       CASE WHEN COALESCE(s.max_keepers, 0) > 0 OR r.cohort = 'keeper' THEN 1 ELSE 0 END AS keeper_like,
                       COALESCE(s.uses_median, 0) AS uses_median,
                       ys.picks_in_market, ys.managers_in_market, ys.total_cost
                FROM raw r
                JOIN year_stats ys
                  ON r.db_name = ys.db_name AND r.year = ys.year
                 AND r.draft_market = ys.draft_market AND r.draft_kind = ys.draft_kind AND r.cohort = ys.cohort
                LEFT JOIN settings s ON r.db_name = s.db_name AND r.year = s.year
            ),
            bucketed AS (
                SELECT p.*,
                       CASE
                         WHEN draft_kind = 'auction' THEN
                           CASE
                             WHEN cost <= 0 THEN 'a_free'
                             WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.30 THEN 'a_30p'
                             WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.20 THEN 'a_20_29p'
                             WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.10 THEN 'a_10_19p'
                             WHEN cost / GREATEST(total_cost / GREATEST(managers_in_market, 1), 1) >= 0.05 THEN 'a_05_09p'
                             ELSE 'a_01_04p'
                           END
                         ELSE
                           's_' || LPAD(CAST(LEAST(20, GREATEST(1,
                             CEIL(((COALESCE(pick, round, picks_in_market) - 1) * 20.0)
                             / GREATEST(picks_in_market, 1)))) AS VARCHAR), 2, '0')
                       END AS capital_bucket
                FROM profiled p
            ),
            weighted AS (
                SELECT t.profile_year, t.draft_market, t.draft_kind, t.cohort,
                       b.position, b.capital_bucket, b.manager_lamar,
                       GREATEST(0.03,
                           (1.0 - LEAST(ABS(b.teams - t.teams) / 10.0, 0.55))
                           * (1.0 - LEAST(ABS(b.draft_rounds - t.draft_rounds) / 12.0, 0.35))
                           * (1.0 - LEAST(ABS(COALESCE(b.total_roster_slots, 0) - COALESCE(t.total_roster_slots, 0)) / 16.0, 0.35))
                           * (1.0 - LEAST(ABS(b.bench_count - t.bench_count) / 8.0, 0.25))
                           * (1.0 - LEAST(ABS(b.flex_count - t.flex_count) / 3.0, 0.20))
                           * CASE WHEN b.superflex = t.superflex THEN 1.0 ELSE 0.65 END
                           * CASE WHEN b.idp = t.idp THEN 1.0 ELSE 0.72 END
                           * (1.0 - LEAST(ABS(b.scoring_rec - t.scoring_rec), 0.35))
                           * CASE WHEN b.pass_td_pts = t.pass_td_pts THEN 1.0 ELSE 0.90 END
                           * CASE WHEN b.dynasty_like = t.dynasty_like THEN 1.0 ELSE 0.70 END
                           * CASE WHEN b.keeper_like = t.keeper_like THEN 1.0 ELSE 0.82 END
                           * CASE WHEN b.uses_median = t.uses_median THEN 1.0 ELSE 0.95 END
                           * CASE
                               WHEN ABS(b.year - t.profile_year) = 0 THEN 1.0
                               WHEN ABS(b.year - t.profile_year) <= 2 THEN 0.90
                               WHEN ABS(b.year - t.profile_year) <= 5 THEN 0.75
                               ELSE 0.55
                             END
                       ) AS weight
                FROM bucketed b
                JOIN targets t
                  ON b.draft_market = t.draft_market
                 AND b.draft_kind = t.draft_kind
                 AND b.cohort = t.cohort
            ),
            agg AS (
                SELECT profile_year, draft_market, draft_kind, cohort, position, capital_bucket,
                       COUNT(*) AS sample_size,
                       SUM(weight) AS effective_n,
                       SUM(manager_lamar * weight) / NULLIF(SUM(weight), 0) AS mean_lamar,
                       SUM(manager_lamar * manager_lamar * weight) / NULLIF(SUM(weight), 0) AS mean_sq_lamar
                FROM weighted
                GROUP BY profile_year, draft_market, draft_kind, cohort, position, capital_bucket
                HAVING SUM(weight) >= 2
            )
            SELECT profile_year, draft_market, draft_kind, cohort, position, capital_bucket,
                   mean_lamar,
                   SQRT(GREATEST(mean_sq_lamar - mean_lamar * mean_lamar, 0.0)) AS std_lamar,
                   sample_size, effective_n
            FROM agg
        """

        try:
            from multi_league.core.readers.fly_reader import (
                FlyReader,
                FlyReaderError,
                FlyReaderNetworkError,
                FlyReaderTableNotFound,
            )

            reader = FlyReader()
            reader.MAX_RETRIES = self._env_int("DRAFT_SCORE_BASELINE_MAX_RETRIES", 2)
            reader.TIMEOUT_SECONDS = self._env_int("DRAFT_SCORE_BASELINE_TIMEOUT_SECONDS", 45)
            rows = reader.query_df(sql, database="___leagues")
        except (FlyReaderTableNotFound, FlyReaderNetworkError, FlyReaderError, RuntimeError) as e:
            logger.info("[draft_score] Fly global baseline unavailable; using local baseline only: %s", e)
            return 0

        if rows.empty:
            return 0
        rows = rows.reindex(
            columns=[
                "profile_year",
                "draft_market",
                "draft_kind",
                "cohort",
                "position",
                "capital_bucket",
                "mean_lamar",
                "std_lamar",
                "sample_size",
                "effective_n",
            ]
        )
        conn.register("_draft_global_baseline_upload", rows)
        try:
            conn.execute(
                """
                INSERT INTO _draft_global_baseline
                SELECT TRY_CAST(profile_year AS INTEGER), draft_market, draft_kind, cohort,
                       position, capital_bucket, TRY_CAST(mean_lamar AS DOUBLE),
                       TRY_CAST(std_lamar AS DOUBLE), TRY_CAST(sample_size AS BIGINT),
                       TRY_CAST(effective_n AS DOUBLE)
                FROM _draft_global_baseline_upload
                """
            )
        finally:
            conn.unregister("_draft_global_baseline_upload")
        count = int(conn.execute("SELECT COUNT(*) FROM _draft_global_baseline").fetchone()[0] or 0)
        logger.info("[draft_score] Loaded %s weighted global baseline rows", count)
        return count

    def _fetch_global_draft_score_baseline_from_local_source(self, target_values: str, db_lit: str) -> int | None:
        """Build global draft baselines from a local source cache when available.

        Fleet backfills can materialize all source draft rows once in the local
        worker process. This keeps the live update narrow while avoiding one
        expensive Fly baseline query per league.
        """
        conn = self._get_connection()
        source_path = os.environ.get("DRAFT_GLOBAL_SOURCE_PATH", "").strip()
        if source_path:
            from pathlib import Path

            resolved = Path(source_path).expanduser()
            if not resolved.is_file():
                from multi_league.core.runtime_mode import is_corpus_mode

                if is_corpus_mode():
                    raise RuntimeError(
                        f"DRAFT_GLOBAL_SOURCE_PATH does not exist or is not a file: {resolved}"
                    )
                logger.warning("[draft_score] Local global source is missing: %s", resolved)
                return None
            conn.execute(
                "CREATE OR REPLACE TEMP VIEW _draft_global_source AS "
                f"SELECT * FROM read_parquet({self._sql_literal(resolved.as_posix())})"
            )

        try:
            conn.execute("SELECT 1 FROM _draft_global_source LIMIT 1").fetchone()
        except Exception:  # noqa: BLE001
            return None

        sql = f"""
            WITH targets(
                profile_year, draft_market, draft_kind, cohort, teams, draft_rounds,
                total_roster_slots, bench_count, flex_count, superflex, idp,
                scoring_rec, pass_td_pts, dynasty_like, keeper_like, uses_median
            ) AS (VALUES {target_values}),
            weighted AS (
                SELECT t.profile_year, t.draft_market, t.draft_kind, t.cohort,
                       b.position, b.capital_bucket, b.manager_lamar,
                       GREATEST(0.03,
                           (1.0 - LEAST(ABS(b.teams - t.teams) / 10.0, 0.55))
                           * (1.0 - LEAST(ABS(b.draft_rounds - t.draft_rounds) / 12.0, 0.35))
                           * (1.0 - LEAST(ABS(COALESCE(b.total_roster_slots, 0) - COALESCE(t.total_roster_slots, 0)) / 16.0, 0.35))
                           * (1.0 - LEAST(ABS(b.bench_count - t.bench_count) / 8.0, 0.25))
                           * (1.0 - LEAST(ABS(b.flex_count - t.flex_count) / 3.0, 0.20))
                           * CASE WHEN b.superflex = t.superflex THEN 1.0 ELSE 0.65 END
                           * CASE WHEN b.idp = t.idp THEN 1.0 ELSE 0.72 END
                           * (1.0 - LEAST(ABS(b.scoring_rec - t.scoring_rec), 0.35))
                           * CASE WHEN b.pass_td_pts = t.pass_td_pts THEN 1.0 ELSE 0.90 END
                           * CASE WHEN b.dynasty_like = t.dynasty_like THEN 1.0 ELSE 0.70 END
                           * CASE WHEN b.keeper_like = t.keeper_like THEN 1.0 ELSE 0.82 END
                           * CASE WHEN b.uses_median = t.uses_median THEN 1.0 ELSE 0.95 END
                           * CASE
                               WHEN ABS(b.year - t.profile_year) = 0 THEN 1.0
                               WHEN ABS(b.year - t.profile_year) <= 2 THEN 0.90
                               WHEN ABS(b.year - t.profile_year) <= 5 THEN 0.75
                               ELSE 0.55
                             END
                       ) AS weight
                FROM _draft_global_source b
                JOIN targets t
                  ON b.draft_market = t.draft_market
                 AND b.draft_kind = t.draft_kind
                 AND b.cohort = t.cohort
                WHERE b.db_name != {db_lit}
            ),
            agg AS (
                SELECT profile_year, draft_market, draft_kind, cohort, position, capital_bucket,
                       COUNT(*) AS sample_size,
                       SUM(weight) AS effective_n,
                       SUM(manager_lamar * weight) / NULLIF(SUM(weight), 0) AS mean_lamar,
                       SUM(manager_lamar * manager_lamar * weight) / NULLIF(SUM(weight), 0) AS mean_sq_lamar
                FROM weighted
                GROUP BY profile_year, draft_market, draft_kind, cohort, position, capital_bucket
                HAVING SUM(weight) >= 2
            )
            INSERT INTO _draft_global_baseline
            SELECT profile_year, draft_market, draft_kind, cohort, position, capital_bucket,
                   mean_lamar,
                   SQRT(GREATEST(mean_sq_lamar - mean_lamar * mean_lamar, 0.0)) AS std_lamar,
                   sample_size, effective_n
            FROM agg
        """
        try:
            conn.execute(sql)
        except Exception as exc:  # noqa: BLE001
            logger.info("[draft_score] Local global baseline cache unavailable; falling back to Fly: %s", exc)
            conn.execute("DELETE FROM _draft_global_baseline")
            return None

        count = int(conn.execute("SELECT COUNT(*) FROM _draft_global_baseline").fetchone()[0] or 0)
        logger.info("[draft_score] Loaded %s weighted global baseline rows from local cache", count)
        return count

    def _compute_blended_draft_value_zscore(
        self,
        draft_table: str,
        draft_cols: set,
        lamar_col: str,
        pos_col: str,
    ) -> int:
        """Populate expected LAMAR and z-scores from global + local evidence."""
        if self.dry_run:
            return 0

        conn = self._get_connection()
        if self._create_draft_score_rows_temp(draft_table, draft_cols, lamar_col, pos_col) == 0:
            return 0
        self._fetch_global_draft_score_baseline()
        update_matchers = [
            "d.year IS NOT DISTINCT FROM f.year",
            "d.round IS NOT DISTINCT FROM f.round",
            "d.pick IS NOT DISTINCT FROM f.pick",
            f"{self._draft_market_expr(draft_cols, 'd.')} IS NOT DISTINCT FROM f.draft_market",
            f"{self._draft_kind_expr(draft_cols, 'd.')} IS NOT DISTINCT FROM f.draft_kind",
            f"{self._keeper_cohort_expr(draft_cols, 'd.')} IS NOT DISTINCT FROM f.cohort",
            f"{self._position_norm_expr(f'd.{pos_col}')} IS NOT DISTINCT FROM f.position",
        ]
        if "draft_id" in draft_cols:
            update_matchers.append("d.draft_id IS NOT DISTINCT FROM f.draft_id")
        if "franchise_id" in draft_cols:
            update_matchers.append("d.franchise_id IS NOT DISTINCT FROM f.franchise_id")
        if "manager" in draft_cols:
            update_matchers.append("d.manager IS NOT DISTINCT FROM f.manager")
        if "player" in draft_cols:
            update_matchers.append("d.player IS NOT DISTINCT FROM f.player")
        update_match_sql = "\n              AND ".join(update_matchers)

        sql = f"""
            WITH local_exact_stats AS (
                SELECT draft_market, draft_kind, cohort, position, capital_bucket,
                       COUNT(*) AS sample_size,
                       SUM(manager_lamar) AS sum_lamar,
                       SUM(manager_lamar * manager_lamar) AS sum_lamar_sq
                FROM _draft_score_rows
                GROUP BY draft_market, draft_kind, cohort, position, capital_bucket
            ),
            local_exact AS (
                SELECT r.score_row_id,
                       CASE WHEN les.sample_size > 1 THEN (les.sum_lamar - r.manager_lamar) / (les.sample_size - 1) END AS mean_lamar,
                       CASE WHEN les.sample_size > 2 THEN
                           SQRT(GREATEST(((les.sum_lamar_sq - r.manager_lamar * r.manager_lamar)
                               - POWER(les.sum_lamar - r.manager_lamar, 2) / (les.sample_size - 1))
                               / (les.sample_size - 2), 0.0))
                       END AS std_lamar,
                       les.sample_size - 1 AS effective_n
                FROM _draft_score_rows r
                JOIN local_exact_stats les
                  ON r.draft_market = les.draft_market
                 AND r.draft_kind = les.draft_kind
                 AND r.cohort = les.cohort
                 AND r.position = les.position
                 AND r.capital_bucket = les.capital_bucket
                WHERE les.sample_size >= 3
            ),
            local_pos_stats AS (
                SELECT draft_market, draft_kind, cohort, position,
                       COUNT(*) AS sample_size,
                       SUM(manager_lamar) AS sum_lamar,
                       SUM(manager_lamar * manager_lamar) AS sum_lamar_sq
                FROM _draft_score_rows
                GROUP BY draft_market, draft_kind, cohort, position
            ),
            local_pos AS (
                SELECT r.score_row_id,
                       CASE WHEN lps.sample_size > 1 THEN (lps.sum_lamar - r.manager_lamar) / (lps.sample_size - 1) END AS mean_lamar,
                       CASE WHEN lps.sample_size > 2 THEN
                           SQRT(GREATEST(((lps.sum_lamar_sq - r.manager_lamar * r.manager_lamar)
                               - POWER(lps.sum_lamar - r.manager_lamar, 2) / (lps.sample_size - 1))
                               / (lps.sample_size - 2), 0.0))
                       END AS std_lamar,
                       lps.sample_size - 1 AS effective_n
                FROM _draft_score_rows r
                JOIN local_pos_stats lps
                  ON r.draft_market = lps.draft_market
                 AND r.draft_kind = lps.draft_kind
                 AND r.cohort = lps.cohort
                 AND r.position = lps.position
                WHERE lps.sample_size >= 3
            ),
            global_pos AS (
                SELECT profile_year, draft_market, draft_kind, cohort, position,
                       SUM(mean_lamar * effective_n) / NULLIF(SUM(effective_n), 0) AS mean_lamar,
                       SQRT(GREATEST(
                           SUM((std_lamar * std_lamar + mean_lamar * mean_lamar) * effective_n)
                           / NULLIF(SUM(effective_n), 0)
                           - POWER(SUM(mean_lamar * effective_n) / NULLIF(SUM(effective_n), 0), 2),
                           0.0
                       )) AS std_lamar,
                       SUM(effective_n) AS effective_n
                FROM _draft_global_baseline
                GROUP BY profile_year, draft_market, draft_kind, cohort, position
            ),
            blended AS (
                SELECT r.score_row_id, r.year, r.round, r.pick, r.manager, r.franchise_id, r.player, r.draft_id,
                       r.draft_market, r.draft_kind, r.cohort, r.position, r.manager_lamar,
                       COALESCE(le.mean_lamar, lp.mean_lamar) AS local_mean,
                       GREATEST(COALESCE(le.std_lamar, lp.std_lamar, 1.0), 1.0) AS local_std,
                       GREATEST(COALESCE(le.effective_n, lp.effective_n, 0), 0) AS local_n,
                       COALESCE(ge.mean_lamar, gp.mean_lamar) AS global_mean,
                       GREATEST(COALESCE(ge.std_lamar, gp.std_lamar, 1.0), 1.0) AS global_std,
                       GREATEST(COALESCE(ge.effective_n, gp.effective_n, 0), 0) AS global_n
                FROM _draft_score_rows r
                LEFT JOIN local_exact le
                  ON r.score_row_id IS NOT DISTINCT FROM le.score_row_id
                LEFT JOIN local_pos lp
                  ON r.score_row_id IS NOT DISTINCT FROM lp.score_row_id
                LEFT JOIN _draft_global_baseline ge
                  ON r.year = ge.profile_year
                 AND r.draft_market = ge.draft_market
                 AND r.draft_kind = ge.draft_kind
                 AND r.cohort = ge.cohort
                 AND r.position = ge.position
                 AND r.capital_bucket = ge.capital_bucket
                 AND ge.effective_n >= 2
                LEFT JOIN global_pos gp
                  ON r.year = gp.profile_year
                 AND r.draft_market = gp.draft_market
                 AND r.draft_kind = gp.draft_kind
                 AND r.cohort = gp.cohort
                 AND r.position = gp.position
                 AND gp.effective_n >= 2
            ),
            scored AS (
                SELECT *,
                       CASE
                           WHEN global_mean IS NOT NULL AND local_mean IS NOT NULL THEN local_n / (local_n + 24.0)
                           WHEN local_mean IS NOT NULL THEN 1.0
                           ELSE 0.0
                       END AS local_alpha
                FROM blended
                WHERE global_mean IS NOT NULL OR local_mean IS NOT NULL
            ),
            final AS (
                SELECT score_row_id, year, round, pick, manager, franchise_id, player, draft_id,
                       draft_market, draft_kind, cohort, position, manager_lamar,
                       CASE
                           WHEN global_mean IS NOT NULL AND local_mean IS NOT NULL
                               THEN global_mean * (1.0 - local_alpha) + local_mean * local_alpha
                           WHEN local_mean IS NOT NULL THEN local_mean
                           ELSE global_mean
                       END AS expected_lamar,
                       GREATEST(
                           CASE
                               WHEN global_mean IS NOT NULL AND local_mean IS NOT NULL
                                   THEN global_std * (1.0 - local_alpha) + local_std * local_alpha
                               WHEN local_mean IS NOT NULL THEN local_std
                               ELSE global_std
                           END,
                           1.0
                       ) AS blended_std
                FROM scored
            )
            UPDATE {draft_table} d
            SET expected_lamar = ROUND(CAST(f.expected_lamar AS DOUBLE), 3),
                draft_value_zscore = ROUND(
                    CAST((f.manager_lamar - f.expected_lamar) / GREATEST(f.blended_std, 1.0) AS DOUBLE),
                    3
                )
            FROM final f
            WHERE {update_match_sql}
              AND {self._db_filter('d')}
        """
        rows_updated = self._execute(sql, "draft_value_zscore: blended global/local expected LAMAR z-scores")
        populated = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND draft_value_zscore IS NOT NULL"
            ).fetchone()[0]
            or 0
        )
        logger.info("[draft_score] Blended model populated %s draft z-scores", populated)
        return rows_updated if rows_updated and rows_updated > 0 else populated

    def _compute_pick_score(self) -> int:
        """Normalize the primary draft z-score to a 100-centered pick_score."""
        draft_table = self._qualified_name("draft")
        conn = self._get_connection()
        sql = f"""
            UPDATE {draft_table}
            SET pick_score = CASE
                WHEN draft_value_zscore IS NULL THEN NULL
                ELSE ROUND(100.0 + 15.0 * CAST(draft_value_zscore AS DOUBLE), 3)
            END
            WHERE {self._db_filter()}
        """
        try:
            conn.execute(sql)
        except Exception as e:
            logger.error(f"[pick_score] 100-index update failed: {e}")
            return 0

        # Count how many rows got pick_score
        try:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND pick_score IS NOT NULL"
            ).fetchone()[0]
            logger.info(f"[pick_score] {count} rows populated as 100-indexed draft scores")
            return count
        except Exception:  # noqa: BLE001
            return 0

    def draft_value_zscore(self) -> int:
        """Calculate z-score metrics for draft picks.

        Each year is treated independently (leagues can switch auction/snake).
        - Auction years (cost > 0): peer group is (year, position, cost_bucket)
        - Snake years (cost = 0): peer group is (year, position, round)
        - Fallback: (year, position) when primary group has < 3 picks

        Keepers are excluded from peer group baselines and manager grade calculations.
        Keeper picks still receive z-scores (vs non-keeper baselines) but get separate
        keeper_draft_score and keeper_draft_grade columns.

        Adds to draft table:
        - draft_value_zscore: z-score of LAMAR vs peer group expectations
        - pick_quality_zscore: same as draft_value_zscore (for compatibility)
        - pick_score: 100-centered index from draft_value_zscore (100 = expected return)
        - expected_lamar: mean LAMAR for peer group
        - draft_grade: individual pick letter grade (A+ to F)
        - manager_draft_score: average pick_score across non-keeper picks
        - manager_draft_grade: letter grade (A+, A, B+, B, C, D, F) excluding keepers
        - keeper_draft_score: average pick_score across keeper picks only (if keepers exist)
        - keeper_draft_grade: letter grade for keeper retention skill (if keepers exist)

        Requires: draft_cost_buckets must run first (provides cost_bucket column).

        Z-score = (actual_lamar - expected_lamar) / std_lamar
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_value_zscore] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")
        conn = self._get_connection()

        # Normalize draft_type per league-year before scoring (collapse linear/snake drift)
        self.normalize_draft_type_per_year()

        # Need manager_lamar for calculations
        lamar_col = "manager_lamar" if "manager_lamar" in draft_cols else "player_lamar"
        if lamar_col not in draft_cols:
            logger.warning("[draft_value_zscore] No LAMAR column found - skipping")
            return 0

        # Columns pre-created by canonical_draft DDL

        rows_updated = 0

        # Determine position column (yahoo_position or position)
        pos_col = "yahoo_position" if "yahoo_position" in draft_cols else "position"
        if pos_col not in draft_cols:
            logger.warning("[draft_value_zscore] No position column found")
            return 0

        # Build keeper exclusion/inclusion filters
        keeper_exclude, keeper_include = self._keeper_filters(draft_cols)

        has_keeper_col = bool(keeper_exclude)

        # Check if there are actually any keepers
        has_keepers = False
        if has_keeper_col:
            try:
                keeper_count = conn.execute(
                    f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND {lamar_col} IS NOT NULL {keeper_include}"
                ).fetchone()[0]
                has_keepers = keeper_count > 0
                if has_keepers:
                    non_keeper_count = conn.execute(
                        f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND {lamar_col} IS NOT NULL {keeper_exclude}"
                    ).fetchone()[0]
                    logger.info(
                        f"[draft_value_zscore] {keeper_count} keeper picks, {non_keeper_count} draft picks â€” grading separately"
                    )
            except Exception:  # noqa: BLE001
                pass
        # Step 1: Calculate z-scores per year
        # Each year is treated independently (leagues can switch auction/snake).
        # Auction years (cost > 0): peer group is (year, position, cost_bucket)
        # Snake years (cost = 0): peer group is (year, position, round)
        # Fallback for both: (year, position) when primary group has < 3 picks.
        #
        # For keepers: always group by (year, position) since keeper costs/rounds
        # are artificial.

        # Build draft_category expressions: partition grading by category when column exists
        # (dynasty leagues have startup/rookie/veteran; others have no column â†’ single pool)
        has_draft_category = "draft_category" in draft_cols
        if has_draft_category:
            cat_expr = "COALESCE(CAST(draft_category AS VARCHAR), 'standard')"
            cat_expr_d = "COALESCE(CAST(d.draft_category AS VARCHAR), 'standard')"
        else:
            cat_expr = "'standard'"
            cat_expr_d = "'standard'"

        # Build peer_key expression: cost_bucket for auction, round for snake
        # Two versions: unqualified (for CTE GROUP BY) and d-qualified (for UPDATE WHERE)
        has_cost_bucket = "cost_bucket" in draft_cols
        if has_cost_bucket:
            peer_key_expr = """CASE WHEN COALESCE(cost, 0) > 0 AND cost_bucket IS NOT NULL
                 THEN CAST(cost_bucket AS BIGINT) ELSE round END"""
            peer_key_expr_d = """CASE WHEN COALESCE(d.cost, 0) > 0 AND d.cost_bucket IS NOT NULL
                 THEN CAST(d.cost_bucket AS BIGINT) ELSE d.round END"""
        else:
            peer_key_expr = "round"
            peer_key_expr_d = "d.round"

        def _build_zscore_sql(extra_filter: str, label: str) -> list[str]:
            """Build zscore UPDATE SQL with per-year peer groups.

            Returns two SQL statements:
            1. Primary: match on exact peer group (year, position, round/cost_bucket)
            2. Fallback: for picks still NULL, match on broader (year, position)
            """
            # Pass 1: exact peer group match (partitioned by draft_category)
            primary = f"""
                WITH peer_stats AS (
                    SELECT
                        year,
                        {pos_col} as position,
                        {peer_key_expr} as peer_key,
                        {cat_expr} as cat,
                        AVG({lamar_col}) as mean_lamar,
                        STDDEV_SAMP({lamar_col}) as std_lamar,
                        SUM(CAST({lamar_col} AS DOUBLE)) as sum_lamar,
                        SUM(POWER(CAST({lamar_col} AS DOUBLE), 2)) as sum_lamar_sq,
                        COUNT(*) as sample_size
                    FROM {draft_table}
                    WHERE {lamar_col} IS NOT NULL
                      AND {pos_col} IS NOT NULL
                      AND {self._db_filter()}
                      {extra_filter}
                    GROUP BY year, {pos_col}, {peer_key_expr}, {cat_expr}
                    HAVING COUNT(*) >= 3
                )
                UPDATE {draft_table} d
                SET
                    expected_lamar = CASE
                        WHEN ps.sample_size > 1 THEN
                            (ps.sum_lamar - CAST(d.{lamar_col} AS DOUBLE)) / (ps.sample_size - 1)
                        ELSE ps.mean_lamar
                    END,
                    draft_value_zscore = CASE
                        WHEN ps.sample_size > 2 THEN
                            ROUND(
                                (
                                    CAST(d.{lamar_col} AS DOUBLE)
                                    - ((ps.sum_lamar - CAST(d.{lamar_col} AS DOUBLE)) / (ps.sample_size - 1))
                                )
                                / GREATEST(
                                    SQRT(GREATEST(
                                        (
                                            (ps.sum_lamar_sq - POWER(CAST(d.{lamar_col} AS DOUBLE), 2))
                                            - POWER(ps.sum_lamar - CAST(d.{lamar_col} AS DOUBLE), 2) / (ps.sample_size - 1)
                                        ) / (ps.sample_size - 2),
                                        0.0
                                    )),
                                    1.0
                                ),
                                3
                            )
                        WHEN COALESCE(ps.std_lamar, 1) > 0 THEN
                            ROUND(
                                (
                                    CAST(d.{lamar_col} AS DOUBLE)
                                    - ((ps.sum_lamar - CAST(d.{lamar_col} AS DOUBLE)) / (ps.sample_size - 1))
                                )
                                / GREATEST(COALESCE(ps.std_lamar, 1.0), 1.0),
                                3
                            )
                        ELSE 0
                    END
                FROM peer_stats ps
                WHERE d.year = ps.year
                  AND d.{pos_col} = ps.position
                  AND {peer_key_expr_d} = ps.peer_key
                  AND {cat_expr_d} = ps.cat
                  AND d.{lamar_col} IS NOT NULL
                  AND {self._db_filter('d')}
                  {extra_filter}
            """
            # Pass 2: fallback for picks whose peer group was too small
            fallback = f"""
                WITH year_position_fallback AS (
                    SELECT
                        year,
                        {pos_col} as position,
                        {cat_expr} as cat,
                        AVG({lamar_col}) as mean_lamar,
                        STDDEV_SAMP({lamar_col}) as std_lamar,
                        SUM(CAST({lamar_col} AS DOUBLE)) as sum_lamar,
                        SUM(POWER(CAST({lamar_col} AS DOUBLE), 2)) as sum_lamar_sq,
                        COUNT(*) as sample_size
                    FROM {draft_table}
                    WHERE {lamar_col} IS NOT NULL
                      AND {pos_col} IS NOT NULL
                      AND {self._db_filter()}
                      {extra_filter}
                    GROUP BY year, {pos_col}, {cat_expr}
                    HAVING COUNT(*) >= 3
                )
                UPDATE {draft_table} d
                SET
                    expected_lamar = CASE
                        WHEN ypf.sample_size > 1 THEN
                            (ypf.sum_lamar - CAST(d.{lamar_col} AS DOUBLE)) / (ypf.sample_size - 1)
                        ELSE ypf.mean_lamar
                    END,
                    draft_value_zscore = CASE
                        WHEN ypf.sample_size > 2 THEN
                            ROUND(
                                (
                                    CAST(d.{lamar_col} AS DOUBLE)
                                    - ((ypf.sum_lamar - CAST(d.{lamar_col} AS DOUBLE)) / (ypf.sample_size - 1))
                                )
                                / GREATEST(
                                    SQRT(GREATEST(
                                        (
                                            (ypf.sum_lamar_sq - POWER(CAST(d.{lamar_col} AS DOUBLE), 2))
                                            - POWER(ypf.sum_lamar - CAST(d.{lamar_col} AS DOUBLE), 2) / (ypf.sample_size - 1)
                                        ) / (ypf.sample_size - 2),
                                        0.0
                                    )),
                                    1.0
                                ),
                                3
                            )
                        WHEN COALESCE(ypf.std_lamar, 1) > 0 THEN
                            ROUND(
                                (
                                    CAST(d.{lamar_col} AS DOUBLE)
                                    - ((ypf.sum_lamar - CAST(d.{lamar_col} AS DOUBLE)) / (ypf.sample_size - 1))
                                )
                                / GREATEST(COALESCE(ypf.std_lamar, 1.0), 1.0),
                                3
                            )
                        ELSE 0
                    END
                FROM year_position_fallback ypf
                WHERE d.year = ypf.year
                  AND d.{pos_col} = ypf.position
                  AND {cat_expr_d} = ypf.cat
                  AND d.{lamar_col} IS NOT NULL
                  AND d.draft_value_zscore IS NULL
                  AND {self._db_filter('d')}
                  {extra_filter}
            """
            return [primary, fallback]

        blended_rows = self._compute_blended_draft_value_zscore(draft_table, draft_cols, lamar_col, pos_col)
        if blended_rows > 0:
            rows_updated += blended_rows

        if blended_rows <= 0 and has_keepers:
            # Step 1a: Non-keeper z-scores â€” per-year peer groups
            for sql in _build_zscore_sql(keeper_exclude, "non-keeper"):
                rows_updated += self._execute(
                    sql, "draft_value_zscore: non-keeper z-scores (per-year, auction=cost_bucket/snake=round)"
                )

            # Step 1b: Keeper z-scores â€” (year, position) baseline among keepers
            # Keeper costs/rounds are artificial, so only group by position per year.
            # Falls back to all-keepers pooled for sparse positions (DEF, K).
            sql_zscore_keeper = f"""
                WITH keeper_position_stats AS (
                    SELECT
                        year,
                        {pos_col} as position,
                        {cat_expr} as cat,
                        AVG({lamar_col}) as mean_lamar,
                        STDDEV_SAMP({lamar_col}) as std_lamar,
                        COUNT(*) as sample_size
                    FROM {draft_table}
                    WHERE {lamar_col} IS NOT NULL
                      AND {pos_col} IS NOT NULL
                      AND {self._db_filter()}
                      {keeper_include}
                    GROUP BY year, {pos_col}, {cat_expr}
                    HAVING COUNT(*) >= 3
                ),
                keeper_global_fallback AS (
                    SELECT
                        {cat_expr} as cat,
                        AVG({lamar_col}) as mean_lamar,
                        STDDEV_SAMP({lamar_col}) as std_lamar
                    FROM {draft_table}
                    WHERE {lamar_col} IS NOT NULL
                      AND {self._db_filter()}
                      {keeper_include}
                    GROUP BY {cat_expr}
                ),
                keeper_baseline AS (
                    SELECT
                        d_pos.year,
                        d_pos.position,
                        d_pos.cat,
                        COALESCE(kps.mean_lamar, kgf.mean_lamar) as mean_lamar,
                        COALESCE(kps.std_lamar, kgf.std_lamar) as std_lamar
                    FROM (SELECT DISTINCT year, {pos_col} as position,
                            {cat_expr} as cat
                          FROM {draft_table}
                          WHERE {self._db_filter()}
                            AND {pos_col} IS NOT NULL {keeper_include}) d_pos
                    LEFT JOIN keeper_position_stats kps
                        ON d_pos.year = kps.year AND d_pos.position = kps.position AND d_pos.cat = kps.cat
                    LEFT JOIN keeper_global_fallback kgf
                        ON d_pos.cat = kgf.cat
                )
                UPDATE {draft_table} d
                SET
                    expected_lamar = kb.mean_lamar,
                    draft_value_zscore = CASE
                        WHEN kb.std_lamar > 0 THEN
                            ROUND((d.{lamar_col} - kb.mean_lamar)
                                  / GREATEST(kb.std_lamar, 1.0), 3)
                        ELSE 0
                    END
                FROM keeper_baseline kb
                WHERE d.year = kb.year
                  AND d.{pos_col} = kb.position
                  AND {cat_expr_d} = kb.cat
                  AND d.{lamar_col} IS NOT NULL
                  AND {self._db_filter('d')}
                  {keeper_include}
            """
            rows_updated += self._execute(
                sql_zscore_keeper, "draft_value_zscore: keeper z-scores (per-year position baseline)"
            )
        elif blended_rows <= 0:
            # No keepers â€” two-pass with per-year peer groups + fallback
            for sql in _build_zscore_sql("", "all-picks"):
                rows_updated += self._execute(
                    sql, "draft_value_zscore: z-scores (per-year, auction=cost_bucket/snake=round)"
                )

        # Step 2: Store a plain 100-indexed pick score. Explicit z-score
        # columns stay z-scores for statistical consumers.
        self._compute_pick_score()

        # Step 3: Populate individual pick draft_grade from PERCENT_RANK(pick_score)
        # Pool size check per draft_category â€” PERCENT_RANK handles small partitions fine
        # Include drafted players who never played (NULL LAMAR = 0 production = still gradeable)
        rows_updated += self._assign_pick_grade_percentiles(draft_table, draft_cols, lamar_col, cat_expr, cat_expr_d)

        # Step 4: Sync pick_quality_zscore alias (column pre-created by canonical_draft DDL).
        if "pick_quality_zscore" in self._get_table_columns("draft"):
            sql_alias = f"""
                UPDATE {draft_table} SET pick_quality_zscore = draft_value_zscore
                WHERE draft_value_zscore IS NOT NULL
                  AND {self._db_filter()}
            """
            rows_updated += self._execute(sql_alias, "draft_value_zscore: pick_quality_zscore alias")

        # Step 5: Calculate manager_draft_score (avg 100-index pick_score per franchise-year-category)
        # Keepers excluded â€” manager grade reflects drafting skill, not retention
        # Build keeper condition for FILTER clause (strip leading AND)
        keeper_filter_condition = keeper_exclude.strip().removeprefix("AND").strip() if keeper_exclude else "TRUE"

        # Stable franchise_id is required; do not fall back to display-name joins.
        has_franchise_id = "franchise_id" in draft_cols
        if not has_franchise_id:
            logger.warning(
                "[draft_value_zscore] franchise_id missing - skipping manager-level draft score/grade updates"
            )
            return rows_updated
        mgr_group_col = "franchise_id"
        mgr_select = "franchise_id, MAX(manager) AS manager,"
        mgr_join_cond = "d.franchise_id = ma.franchise_id"

        sql_manager_score = f"""
            WITH manager_avg AS (
                SELECT
                    {mgr_select}
                    year,
                    {cat_expr} as cat,
                    AVG(pick_score) FILTER (WHERE {keeper_filter_condition}) as avg_score
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND {mgr_group_col} IS NOT NULL
                  AND pick_score IS NOT NULL
                GROUP BY {mgr_group_col}, year, {cat_expr}
            )
            UPDATE {draft_table} d
            SET manager_draft_score = ROUND(ma.avg_score, 3)
            FROM manager_avg ma
            WHERE {mgr_join_cond} AND d.year = ma.year
              AND {cat_expr_d} = ma.cat
              AND {self._db_filter('d')}
        """
        rows_updated += self._execute(
            sql_manager_score,
            "draft_value_zscore: manager_draft_score (avg 100-index pick_score, per category, keepers excluded)",
        )

        if "manager_draft_percentile_alltime" in self._get_table_columns("draft"):
            sql_mgr_percentile = f"""
                WITH mgr_scores AS (
                    SELECT DISTINCT franchise_id, year, manager_draft_score,
                        {cat_expr} as cat
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND franchise_id IS NOT NULL AND manager_draft_score IS NOT NULL
                ),
                mgr_ranked AS (
                    SELECT franchise_id, year, cat,
                        ROUND(PERCENT_RANK() OVER (
                            PARTITION BY cat
                            ORDER BY manager_draft_score ASC
                        ) * 100, 1) as pctile
                    FROM mgr_scores
                )
                UPDATE {draft_table} d
                SET manager_draft_percentile_alltime = mr.pctile
                FROM mgr_ranked mr
                WHERE d.franchise_id = mr.franchise_id AND d.year = mr.year
                  AND {cat_expr_d} = mr.cat
                  AND {self._db_filter('d')}
            """
            rows_updated += self._execute(
                sql_mgr_percentile,
                "draft_value_zscore: manager_draft_percentile_alltime (z-score percentile, per category)",
            )

        # Step 6: Derive manager_draft_grade from percentile of manager_draft_score, per category
        pool_size = conn.execute(
            f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND (pick_score IS NOT NULL OR {lamar_col} IS NOT NULL)"
        ).fetchone()[0]
        if pool_size >= 30:
            sql_mgr_grade = f"""
                WITH mgr_scores AS (
                    SELECT DISTINCT franchise_id, year, manager_draft_score,
                        {cat_expr} as cat
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND franchise_id IS NOT NULL AND manager_draft_score IS NOT NULL
                ),
                mgr_ranked AS (
                    SELECT franchise_id, year, cat,
                        PERCENT_RANK() OVER (
                            PARTITION BY cat
                            ORDER BY manager_draft_score ASC
                        ) * 100 as pctile
                    FROM mgr_scores
                )
                UPDATE {draft_table} d
                SET manager_draft_grade = CASE
                    WHEN mr.pctile >= 95 THEN 'A+'
                    WHEN mr.pctile >= 85 THEN 'A'
                    WHEN mr.pctile >= 75 THEN 'A-'
                    WHEN mr.pctile >= 65 THEN 'B+'
                    WHEN mr.pctile >= 50 THEN 'B'
                    WHEN mr.pctile >= 35 THEN 'B-'
                    WHEN mr.pctile >= 20 THEN 'C'
                    WHEN mr.pctile >= 10 THEN 'D'
                    ELSE 'F'
                END
                FROM mgr_ranked mr
                WHERE d.franchise_id = mr.franchise_id AND d.year = mr.year
                  AND {cat_expr_d} = mr.cat
                  AND {self._db_filter('d')}
            """
            rows_updated += self._execute(
                sql_mgr_grade, "draft_value_zscore: manager_draft_grade (9-tier percentile, per category)"
            )

        # Step 7: Keeper-specific manager score + grade (mirrors manager score/grade for keeper picks only)
        if has_keepers and "keeper_draft_score" in self._get_table_columns("draft"):
            keeper_only_condition = keeper_include.strip().removeprefix("AND").strip() if keeper_include else "FALSE"

            sql_keeper_mgr_score = f"""
                WITH keeper_mgr_avg AS (
                    SELECT
                        {mgr_select}
                        year,
                        {cat_expr} as cat,
                        AVG(pick_score) FILTER (WHERE {keeper_only_condition}) as avg_score
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND {mgr_group_col} IS NOT NULL
                      AND pick_score IS NOT NULL
                    GROUP BY {mgr_group_col}, year, {cat_expr}
                )
                UPDATE {draft_table} d
                SET keeper_draft_score = ROUND(kma.avg_score, 3)
                FROM keeper_mgr_avg kma
                WHERE {mgr_join_cond.replace('ma.', 'kma.')} AND d.year = kma.year
                  AND {cat_expr_d} = kma.cat
                  AND kma.avg_score IS NOT NULL
                  AND {self._db_filter('d')}
            """
            rows_updated += self._execute(
                sql_keeper_mgr_score,
                "draft_value_zscore: keeper_draft_score (avg 100-index pick_score for keeper picks only)",
            )

            # Grade keeper retention skill on its own percentile curve
            keeper_pool = conn.execute(
                f"SELECT COUNT(DISTINCT {mgr_group_col} || '|' || CAST(year AS VARCHAR)) "
                f"FROM {draft_table} WHERE {self._db_filter()} AND keeper_draft_score IS NOT NULL"
            ).fetchone()[0]
            if keeper_pool >= 5:
                sql_keeper_mgr_grade = f"""
                    WITH kscores AS (
                        SELECT DISTINCT {mgr_group_col}, year, keeper_draft_score,
                            {cat_expr} as cat
                        FROM {draft_table}
                        WHERE {self._db_filter()}
                          AND {mgr_group_col} IS NOT NULL AND keeper_draft_score IS NOT NULL
                    ),
                    kranked AS (
                        SELECT {mgr_group_col}, year, cat,
                            PERCENT_RANK() OVER (
                                PARTITION BY cat
                                ORDER BY keeper_draft_score ASC
                            ) * 100 as pctile
                        FROM kscores
                    )
                    UPDATE {draft_table} d
                    SET keeper_draft_grade = CASE
                        WHEN kr.pctile >= 95 THEN 'A+'
                        WHEN kr.pctile >= 85 THEN 'A'
                        WHEN kr.pctile >= 75 THEN 'A-'
                        WHEN kr.pctile >= 65 THEN 'B+'
                        WHEN kr.pctile >= 50 THEN 'B'
                        WHEN kr.pctile >= 35 THEN 'B-'
                        WHEN kr.pctile >= 20 THEN 'C'
                        WHEN kr.pctile >= 10 THEN 'D'
                        ELSE 'F'
                    END
                    FROM kranked kr
                    WHERE d.{mgr_group_col} = kr.{mgr_group_col} AND d.year = kr.year
                      AND {cat_expr_d} = kr.cat
                      AND {self._db_filter('d')}
                """
                rows_updated += self._execute(
                    sql_keeper_mgr_grade, "draft_value_zscore: keeper_draft_grade (9-tier percentile, keepers only)"
                )

        return rows_updated

    # draft_value_tiers() and draft_efficiency_ranks() removed â€”
    # replaced by z-score aliases + percentile-based draft_grade.

    # =========================================================================
    # DRAFT AGE Z-SCORE: Age preference metric weighted by draft capital
    # =========================================================================

    def draft_age_zscore(self) -> int:
        """Calculate age z-scores for draft picks using player_bio birth_date.

        Each year is treated independently. Peer groups mirror draft_value_zscore:
        - Auction years (cost > 0): peer group is (year, position, cost_bucket)
        - Snake years (cost = 0): peer group is (year, position, round)
        - Fallback: (year, position) when primary group has < 3 picks

        Keepers are stratified separately (keeper costs/rounds are artificial).

        Adds to draft table:
        - draft_age: player age at time of draft (year - birth_year)
        - draft_age_zscore: z-score of age vs peer group (negative = younger than peers)
        - expected_age: mean age for peer group
        - manager_weighted_age: capital-weighted avg draft age per manager-year
          (auction: weighted by cost, snake: weighted by inverse pick number)

        Requires: player_bio table with birth_date, draft_cost_buckets (for cost_bucket).
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_age_zscore] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")
        conn = self._get_connection()

        if "NFL_player_id" not in draft_cols:
            logger.warning("[draft_age_zscore] No NFL_player_id column â€” skipping")
            return 0

        # Check player_bio availability
        try:
            bio_count = conn.execute(
                "SELECT COUNT(*) FROM ___ops.nfl_historical.player_bio WHERE birth_date IS NOT NULL"
            ).fetchone()[0]
            if bio_count == 0:
                logger.warning("[draft_age_zscore] player_bio has no birth_date data â€” skipping")
                return 0
        except Exception as e:
            logger.warning(f"[draft_age_zscore] Cannot access player_bio: {e}")
            return 0

        # Columns pre-created by canonical_draft DDL

        rows_updated = 0

        # Step 1: Populate draft_age from player_bio
        sql_age = f"""
            UPDATE {draft_table} d
            SET draft_age = (
                d.year - EXTRACT(YEAR FROM TRY_CAST(pb.birth_date AS DATE))
            )::INT
            FROM ___ops.nfl_historical.player_bio pb
            WHERE d.NFL_player_id = pb.NFL_player_id
              AND TRY_CAST(pb.birth_date AS DATE) IS NOT NULL
              AND {self._db_filter('d')}
        """
        rows_updated += self._execute(sql_age, "draft_age_zscore: populate draft_age from player_bio")

        # Check coverage
        age_count = conn.execute(
            f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND draft_age IS NOT NULL AND draft_age > 0"
        ).fetchone()[0]
        total_count = conn.execute(
            f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND NFL_player_id IS NOT NULL"
        ).fetchone()[0]
        logger.info(
            f"[draft_age_zscore] Age coverage: {age_count}/{total_count} picks ({round(age_count / max(total_count, 1) * 100, 1)}%)"
        )

        if age_count < 30:
            logger.warning(f"[draft_age_zscore] Only {age_count} picks with age data â€” skipping z-scores")
            return rows_updated

        # Determine position and peer key columns (same as draft_value_zscore)
        pos_col = "yahoo_position" if "yahoo_position" in draft_cols else "position"
        if pos_col not in draft_cols:
            logger.warning("[draft_age_zscore] No position column found")
            return rows_updated

        has_cost_bucket = "cost_bucket" in draft_cols
        if has_cost_bucket:
            peer_key_expr = """CASE WHEN COALESCE(cost, 0) > 0 AND cost_bucket IS NOT NULL
                 THEN CAST(cost_bucket AS BIGINT) ELSE round END"""
            peer_key_expr_d = """CASE WHEN COALESCE(d.cost, 0) > 0 AND d.cost_bucket IS NOT NULL
                 THEN CAST(d.cost_bucket AS BIGINT) ELSE d.round END"""
        else:
            peer_key_expr = "round"
            peer_key_expr_d = "d.round"

        # Build keeper filters
        keeper_exclude, keeper_include = self._keeper_filters(draft_cols)
        has_keeper_col = bool(keeper_exclude)

        has_keepers = False
        if has_keeper_col:
            try:
                keeper_count = conn.execute(
                    f"SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND draft_age IS NOT NULL {keeper_include}"
                ).fetchone()[0]
                has_keepers = keeper_count > 0
            except Exception:  # noqa: BLE001
                pass

        # Step 2: Calculate age z-scores per peer group
        def _build_age_zscore_sql(extra_filter: str) -> list[str]:
            """Build age z-score UPDATE SQL with per-year peer groups + fallback."""
            primary = f"""
                WITH peer_stats AS (
                    SELECT
                        year,
                        {pos_col} as position,
                        {peer_key_expr} as peer_key,
                        AVG(draft_age) as mean_age,
                        STDDEV_SAMP(draft_age) as std_age,
                        COUNT(*) as sample_size
                    FROM {draft_table}
                    WHERE draft_age IS NOT NULL AND draft_age > 0
                      AND {pos_col} IS NOT NULL
                      AND {self._db_filter()}
                      {extra_filter}
                    GROUP BY year, {pos_col}, {peer_key_expr}
                    HAVING COUNT(*) >= 3
                )
                UPDATE {draft_table} d
                SET
                    expected_age = ps.mean_age,
                    draft_age_zscore = CASE
                        WHEN COALESCE(ps.std_age, 0) > 0 THEN
                            ROUND((d.draft_age - ps.mean_age)
                                  / GREATEST(ps.std_age, 0.5), 3)
                        ELSE 0
                    END
                FROM peer_stats ps
                WHERE d.year = ps.year
                  AND d.{pos_col} = ps.position
                  AND {peer_key_expr_d} = ps.peer_key
                  AND d.draft_age IS NOT NULL AND d.draft_age > 0
                  AND {self._db_filter('d')}
                  {extra_filter}
            """
            fallback = f"""
                WITH year_position_fallback AS (
                    SELECT
                        year,
                        {pos_col} as position,
                        AVG(draft_age) as mean_age,
                        STDDEV_SAMP(draft_age) as std_age
                    FROM {draft_table}
                    WHERE draft_age IS NOT NULL AND draft_age > 0
                      AND {pos_col} IS NOT NULL
                      AND {self._db_filter()}
                      {extra_filter}
                    GROUP BY year, {pos_col}
                    HAVING COUNT(*) >= 3
                )
                UPDATE {draft_table} d
                SET
                    expected_age = ypf.mean_age,
                    draft_age_zscore = CASE
                        WHEN COALESCE(ypf.std_age, 0) > 0 THEN
                            ROUND((d.draft_age - ypf.mean_age)
                                  / GREATEST(ypf.std_age, 0.5), 3)
                        ELSE 0
                    END
                FROM year_position_fallback ypf
                WHERE d.year = ypf.year
                  AND d.{pos_col} = ypf.position
                  AND d.draft_age IS NOT NULL AND d.draft_age > 0
                  AND d.draft_age_zscore IS NULL
                  AND {self._db_filter('d')}
                  {extra_filter}
            """
            return [primary, fallback]

        if has_keepers:
            for sql in _build_age_zscore_sql(keeper_exclude):
                rows_updated += self._execute(sql, "draft_age_zscore: non-keeper age z-scores")
            # Keeper age z-scores: (year, position) baseline only
            sql_keeper_age = f"""
                WITH keeper_age_stats AS (
                    SELECT year, {pos_col} as position,
                        AVG(draft_age) as mean_age,
                        STDDEV_SAMP(draft_age) as std_age
                    FROM {draft_table}
                    WHERE draft_age IS NOT NULL AND draft_age > 0
                      AND {pos_col} IS NOT NULL
                      AND {self._db_filter()}
                      {keeper_include}
                    GROUP BY year, {pos_col}
                    HAVING COUNT(*) >= 3
                ),
                keeper_global AS (
                    SELECT AVG(draft_age) as mean_age, STDDEV_SAMP(draft_age) as std_age
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND draft_age IS NOT NULL AND draft_age > 0 {keeper_include}
                ),
                keeper_baseline AS (
                    SELECT dp.year, dp.position,
                        COALESCE(kas.mean_age, kg.mean_age) as mean_age,
                        COALESCE(kas.std_age, kg.std_age) as std_age
                    FROM (SELECT DISTINCT year, {pos_col} as position FROM {draft_table}
                          WHERE {self._db_filter()}
                            AND {pos_col} IS NOT NULL {keeper_include}) dp
                    LEFT JOIN keeper_age_stats kas ON dp.year = kas.year AND dp.position = kas.position
                    CROSS JOIN keeper_global kg
                )
                UPDATE {draft_table} d
                SET expected_age = kb.mean_age,
                    draft_age_zscore = CASE
                        WHEN COALESCE(kb.std_age, 0) > 0 THEN
                            ROUND((d.draft_age - kb.mean_age) / GREATEST(kb.std_age, 0.5), 3)
                        ELSE 0
                    END
                FROM keeper_baseline kb
                WHERE d.year = kb.year AND d.{pos_col} = kb.position
                  AND d.draft_age IS NOT NULL AND d.draft_age > 0
                  AND {self._db_filter('d')}
                  {keeper_include}
            """
            rows_updated += self._execute(sql_keeper_age, "draft_age_zscore: keeper age z-scores")
        else:
            for sql in _build_age_zscore_sql(""):
                rows_updated += self._execute(sql, "draft_age_zscore: age z-scores")

        # Step 3: Capital-weighted manager_weighted_age per manager-year
        # Auction: weight by cost. Snake: weight by inverse overall pick so early picks weigh more.
        keeper_filter_condition = keeper_exclude.strip().removeprefix("AND").strip() if keeper_exclude else "TRUE"

        sql_mgr_age = f"""
            WITH pick_weights AS (
                SELECT franchise_id, year, draft_age,
                    CASE
                        WHEN COALESCE(cost, 0) > 0 THEN GREATEST(cost, 1)
                        ELSE GREATEST(
                            (MAX(COALESCE(pick, round, 1)) OVER (PARTITION BY year))
                            + 1 - COALESCE(pick, round, 1),
                            1
                        )
                    END as capital_weight
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL
                  AND draft_age IS NOT NULL AND draft_age > 0
                  AND {keeper_filter_condition}
            ),
            mgr_weighted AS (
                SELECT franchise_id, year,
                    ROUND(SUM(draft_age * capital_weight) / NULLIF(SUM(capital_weight), 0), 1) as wtd_age
                FROM pick_weights
                GROUP BY franchise_id, year
            )
            UPDATE {draft_table} d
            SET manager_weighted_age = mw.wtd_age
            FROM mgr_weighted mw
            WHERE d.franchise_id = mw.franchise_id AND d.year = mw.year
              AND {self._db_filter('d')}
        """
        rows_updated += self._execute(sql_mgr_age, "draft_age_zscore: manager_weighted_age (capital-weighted)")

        # Log summary
        try:
            summary = conn.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE draft_age_zscore IS NOT NULL) as zscored,
                    ROUND(AVG(draft_age) FILTER (WHERE draft_age IS NOT NULL), 1) as avg_age,
                    ROUND(AVG(manager_weighted_age) FILTER (WHERE manager_weighted_age IS NOT NULL), 1) as avg_wtd_age
                FROM {draft_table}
                WHERE {self._db_filter()}
            """).fetchone()
            logger.info(
                f"[draft_age_zscore] Done: {summary[0]} z-scored, avg_age={summary[1]}, avg_wtd_age={summary[2]}"
            )
        except Exception:  # noqa: BLE001
            pass
        return rows_updated

    # =========================================================================
    # DRAFT COST BUCKETS: Position-tier bucketing for Draft Optimizer
    # =========================================================================

    def draft_cost_buckets(self) -> int:
        """Create cost_bucket column on draft table using NTILE position tiers.

        Replaces the Python assign_cost_buckets() from draft_value_metrics_v3.py.
        Groups auction picks into position-specific cost tiers using NTILE(5).
        Keepers are excluded from tier calculation (artificial prices).

        Adds to draft table:
        - cost_bucket: Integer tier (1=cheapest, 5=most expensive) within position-year
        - position_percentile: Percentile rank (0-100) within position-year

        Required by: Draft Optimizer (load_draft_optimizer_data aggregates by cost_bucket)
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_cost_buckets] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")
        conn = self._get_connection()

        # Determine position column
        if "primary_position" in draft_cols:
            pos_col = "primary_position"
        elif "yahoo_position" in draft_cols:
            pos_col = "yahoo_position"
        elif "position" in draft_cols:
            pos_col = "position"
        else:
            logger.warning("[draft_cost_buckets] No position column found")
            return 0

        # Columns pre-created by canonical_draft DDL

        # Build keeper exclusion filter
        keeper_filter, _ = self._keeper_filters(draft_cols)

        # Detect if this is an auction league (any pick with cost > 0)
        try:
            has_cost = conn.execute(f"""
                SELECT COUNT(*) FROM {draft_table} WHERE {self._db_filter()} AND cost > 0
            """).fetchone()[0]
        except Exception:
            has_cost = 0

        if has_cost == 0:
            # Snake draft - bucket by round instead of cost
            if "round" not in draft_cols:
                logger.warning("[draft_cost_buckets] Snake draft but no round column - skipping")
                return 0

            sql = f"""
                WITH pos_counts AS (
                    SELECT {pos_col}, year, COUNT(*) as cnt
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND {pos_col} IS NOT NULL
                      AND round IS NOT NULL
                    GROUP BY {pos_col}, year
                )
                UPDATE {draft_table}
                SET cost_bucket = bucket.tier,
                    position_percentile = bucket.pctile
                FROM (
                    SELECT
                        d.rowid as rid,
                        NTILE(LEAST(5, GREATEST(2, FLOOR(pc.cnt / 3)))) OVER (
                            PARTITION BY d.{pos_col}, d.year
                            ORDER BY d.round ASC, d.pick ASC
                        ) as tier,
                        ROUND(PERCENT_RANK() OVER (
                            PARTITION BY d.{pos_col}, d.year
                            ORDER BY d.round ASC, d.pick ASC
                        ) * 100, 1) as pctile
                    FROM {draft_table} d
                    JOIN pos_counts pc ON d.{pos_col} = pc.{pos_col} AND d.year = pc.year
                    WHERE d.{pos_col} IS NOT NULL
                      AND d.round IS NOT NULL
                      AND {self._db_filter('d')}
                ) bucket
                WHERE {draft_table}.rowid = bucket.rid
                  AND {self._db_filter()}
            """
            return self._execute(sql, "draft_cost_buckets: snake draft (adaptive tiers by round)")

        # Auction draft - bucket by cost within position-year (excluding keepers)
        # Adaptive tier count: NTILE(min(5, max(2, floor(cnt/3))))
        # Ensures at least 3 picks per tier, at least 2 tiers, at most 5 tiers
        # Step 1: Set buckets for non-keeper picks with cost > 0
        sql_auction = f"""
            WITH pos_counts AS (
                SELECT {pos_col}, year, COUNT(*) as cnt
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND {pos_col} IS NOT NULL
                  AND cost > 0
                  {keeper_filter}
                GROUP BY {pos_col}, year
            )
            UPDATE {draft_table}
            SET cost_bucket = bucket.tier,
                position_percentile = bucket.pctile
            FROM (
                SELECT
                    d.rowid as rid,
                    NTILE(LEAST(5, GREATEST(2, FLOOR(pc.cnt / 3)))) OVER (
                        PARTITION BY d.{pos_col}, d.year
                        ORDER BY d.cost DESC
                    ) as tier,
                    ROUND(PERCENT_RANK() OVER (
                        PARTITION BY d.{pos_col}, d.year
                        ORDER BY d.cost DESC
                    ) * 100, 1) as pctile
                FROM {draft_table} d
                JOIN pos_counts pc ON d.{pos_col} = pc.{pos_col} AND d.year = pc.year
                WHERE d.{pos_col} IS NOT NULL
                  AND d.cost > 0
                  {keeper_filter}
                  AND {self._db_filter('d')}
            ) bucket
            WHERE {draft_table}.rowid = bucket.rid
              AND {self._db_filter()}
        """
        rows = self._execute(sql_auction, "draft_cost_buckets: auction (adaptive tiers, non-keeper picks)")

        # Step 2: Assign keepers to bucket based on their cost relative to non-keepers
        # Keepers get the bucket whose avg cost is closest to their keeper cost
        if keeper_filter:
            keeper_cond = keeper_filter.replace("AND ", "", 1).replace("= 0", "= 1")
            sql_keepers = f"""
                UPDATE {draft_table}
                SET cost_bucket = COALESCE(nearest.tier, 1),
                    position_percentile = COALESCE(nearest.pctile, 50.0)
                FROM (
                    SELECT
                        k.rowid as rid,
                        (SELECT b.cost_bucket FROM {draft_table} b
                         WHERE b.{pos_col} = k.{pos_col}
                           AND b.year = k.year
                           AND b.cost_bucket IS NOT NULL
                           AND {self._db_filter('b')}
                         ORDER BY ABS(b.cost - k.cost) ASC
                         LIMIT 1
                        ) as tier,
                        (SELECT b.position_percentile FROM {draft_table} b
                         WHERE b.{pos_col} = k.{pos_col}
                           AND b.year = k.year
                           AND b.position_percentile IS NOT NULL
                           AND {self._db_filter('b')}
                         ORDER BY ABS(b.cost - k.cost) ASC
                         LIMIT 1
                        ) as pctile
                    FROM {draft_table} k
                    WHERE k.{pos_col} IS NOT NULL
                      AND k.cost > 0
                      AND {keeper_cond}
                      AND {self._db_filter('k')}
                ) nearest
                WHERE {draft_table}.rowid = nearest.rid
                  AND {self._db_filter()}
            """
            rows += self._execute(sql_keepers, "draft_cost_buckets: assign keepers to nearest bucket")

        return rows

    # =========================================================================
    # DRAFT BENCH INSURANCE: Position-specific bench value discounts
    # =========================================================================

    def draft_bench_insurance(self) -> int:
        """Calculate bench_insurance_discount from player_fantasy starter/bench data.

        Replaces the Python bench_insurance_calculator from draft_value_metrics_v3.py.
        Computes position-specific bench value as ratio of bench-to-starter performance.

        Adds to draft table:
        - bench_insurance_discount: Float 0.0-1.0, position-specific bench value factor
        - bench_lamar: Pre-computed bench LAMAR = max(0, manager_lamar) * discount

        Required by: Draft Optimizer bench valuation analysis
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_bench_insurance] draft table not found")
            return 0

        if not self._table_exists("player_fantasy"):
            logger.warning("[draft_bench_insurance] player_fantasy not found - using defaults")
            return self._apply_default_bench_discounts()

        draft_cols = self._get_table_columns("draft")
        player_cols = self._get_table_columns("player_fantasy")
        draft_table = self._qualified_name("draft")
        player_table = self._qualified_name("player_fantasy")

        # Determine position column in draft
        if "primary_position" in draft_cols:
            draft_pos = "primary_position"
        elif "yahoo_position" in draft_cols:
            draft_pos = "yahoo_position"
        elif "position" in draft_cols:
            draft_pos = "position"
        else:
            logger.warning("[draft_bench_insurance] No position column in draft")
            return self._apply_default_bench_discounts()

        # Determine position column in player_fantasy
        if "yahoo_position" in player_cols:
            player_pos = "yahoo_position"
        elif "position" in player_cols:
            player_pos = "position"
        else:
            logger.warning("[draft_bench_insurance] No position column in player_fantasy")
            return self._apply_default_bench_discounts()

        # Need fantasy_points and is_started columns
        pts_col = "fantasy_points" if "fantasy_points" in player_cols else "points"
        if pts_col not in player_cols:
            logger.warning("[draft_bench_insurance] No points column in player_fantasy")
            return self._apply_default_bench_discounts()

        if "is_started" not in player_cols:
            logger.warning("[draft_bench_insurance] No is_started column - using defaults")
            return self._apply_default_bench_discounts()

        has_eligible_player_rows = self.conn.execute(
            f"""
            SELECT EXISTS (
                SELECT 1
                FROM {player_table} p
                WHERE p.{player_pos} IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
                  AND p.{pts_col} IS NOT NULL
                  AND {self._db_filter('p')}
                  AND {rostered_filter_sql("p")}
            )
            """
        ).fetchone()[0]
        if not has_eligible_player_rows:
            # A completed offseason draft can precede Week 1.  This is a
            # normal model state, not an absent enrichment: use the same
            # documented position defaults until player-week evidence exists.
            logger.info("[draft_bench_insurance] No eligible player weeks - using defaults")
            return self._apply_default_bench_discounts()

        # Columns pre-created by canonical_draft DDL

        # Calculate position-specific bench insurance discount:
        # discount = failure_rate Ã— activation_rate (clamped to 0.01-0.50)
        # Where:
        #   failure_rate = fraction of started player-weeks with below-average points (need backup)
        #   activation_rate = fraction of bench player-weeks where bench player got started later
        # This matches the Python bench_insurance_calculator formula
        sql = f"""
            WITH position_rates AS (
                SELECT
                    {player_pos} as pos,
                    -- failure_rate: % of starter-weeks where player scored below position avg
                    -- (indicates when a backup would be needed)
                    SUM(CASE WHEN is_started = 1 AND {pts_col} < pos_avg.avg_pts * 0.5 THEN 1 ELSE 0 END)::DOUBLE
                        / GREATEST(SUM(CASE WHEN is_started = 1 THEN 1 ELSE 0 END), 1) as failure_rate,
                    -- activation_rate: % of bench players who also got starts in the same season
                    -- (indicates bench players actually see the field)
                    COUNT(DISTINCT CASE WHEN is_started = 0 AND player_season_starts.started_any = 1
                        THEN p.player_week END)::DOUBLE
                        / GREATEST(COUNT(DISTINCT CASE WHEN is_started = 0 THEN p.player_week END), 1) as activation_rate
                FROM {player_table} p
                LEFT JOIN (
                    SELECT {player_pos} as pos, AVG({pts_col}) as avg_pts
                    FROM {player_table}
                    WHERE is_started = 1 AND {pts_col} IS NOT NULL
                      AND {self._db_filter()}
                      AND {player_pos} IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
                    GROUP BY {player_pos}
                ) pos_avg ON p.{player_pos} = pos_avg.pos
                LEFT JOIN (
                    SELECT NFL_player_id, year,
                        MAX(CASE WHEN is_started = 1 THEN 1 ELSE 0 END) as started_any
                    FROM {player_table}
                    WHERE NFL_player_id IS NOT NULL
                      AND {self._db_filter()}
                    GROUP BY NFL_player_id, year
                ) player_season_starts ON p.NFL_player_id = player_season_starts.NFL_player_id
                    AND p.year = player_season_starts.year
                WHERE p.{player_pos} IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
                  AND p.{pts_col} IS NOT NULL
                  AND {self._db_filter('p')}
                  AND {rostered_filter_sql("p")}
                GROUP BY p.{player_pos}
            ),
            discounts AS (
                SELECT
                    pos,
                    ROUND(LEAST(GREATEST(failure_rate * activation_rate, 0.01), 0.50), 3) as discount
                FROM position_rates
            )
            UPDATE {draft_table}
            SET bench_insurance_discount = COALESCE(d.discount, 0.10)
            FROM discounts d
            WHERE {draft_table}.{draft_pos} = d.pos
              AND {self._db_filter()}
        """
        rows = self._execute(sql, "draft_bench_insurance: calculate position discounts")

        # Calculate bench_lamar = max(0, manager_lamar) * discount
        lamar_col = "manager_lamar" if "manager_lamar" in draft_cols else None
        if lamar_col:
            sql_bench_lamar = f"""
                UPDATE {draft_table}
                SET bench_lamar = GREATEST(COALESCE({lamar_col}, 0), 0)
                    * COALESCE(bench_insurance_discount, 0)
                WHERE {self._db_filter()}
            """
            rows += self._execute(sql_bench_lamar, "draft_bench_insurance: bench_lamar")

        return rows

    def _apply_default_bench_discounts(self) -> int:
        """Apply hardcoded bench insurance defaults when player data unavailable.

        Default values based on typical league averages.
        """
        if not self._table_exists("draft"):
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")

        if "primary_position" in draft_cols:
            pos_col = "primary_position"
        elif "yahoo_position" in draft_cols:
            pos_col = "yahoo_position"
        elif "position" in draft_cols:
            pos_col = "position"
        else:
            return 0

        # Columns pre-created by canonical_draft DDL

        # Default position discounts from historical analysis
        sql = f"""
            UPDATE {draft_table}
            SET bench_insurance_discount = CASE {pos_col}
                WHEN 'QB' THEN 0.15
                WHEN 'RB' THEN 0.30
                WHEN 'WR' THEN 0.25
                WHEN 'TE' THEN 0.12
                WHEN 'K' THEN 0.05
                WHEN 'DEF' THEN 0.05
                ELSE 0.10
            END
            WHERE {self._db_filter()}
        """
        rows = self._execute(sql, "draft_bench_insurance: apply defaults")

        lamar_col = "manager_lamar" if "manager_lamar" in draft_cols else None
        if lamar_col:
            sql_bench = f"""
                UPDATE {draft_table}
                SET bench_lamar = GREATEST(COALESCE({lamar_col}, 0), 0)
                    * COALESCE(bench_insurance_discount, 0)
                WHERE {self._db_filter()}
            """
            rows += self._execute(sql_bench, "draft_bench_insurance: bench_lamar (defaults)")

        return rows

    # =========================================================================
    # DRAFT STARTER DESIGNATION: Position rank, starter/backup flags
    # =========================================================================

    def draft_starter_designation(self) -> int:
        """Calculate starter/backup designation for draft picks.

        Uses league roster settings (from MotherDuck league_settings table)
        to determine which draft picks were intended as starters vs backups.

        Multi-pass approach:
        1. Assign dedicated position starters (QB1, RB1-2, WR1-3, etc.)
        2. Pool remaining FLEX-eligible (RB/WR/TE) and assign FLEX slots by draft capital
        3. Pool remaining SUPERFLEX-eligible (QB/RB/WR/TE) and assign SUPERFLEX slots

        Adds to draft table:
        - position_draft_rank: Rank within position per manager-year (1, 2, 3, ...)
        - position_draft_label: Label like "QB1", "RB2", "WR3"
        - starter_slots_available: Dedicated starter slots for this position (excl FLEX)
        - drafted_as_starter: 1 if drafted to be a starter, 0 if backup
        - drafted_as_backup: 1 if drafted to be a backup, 0 if starter

        Required by: Draft Optimizer bench valuation, starter/backup split analysis
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_starter] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")
        conn = self._get_connection()

        # Detect position column
        pos_col = None
        for c in ("primary_position", "yahoo_position", "position"):
            if c in draft_cols:
                pos_col = c
                break
        if not pos_col:
            logger.warning("[draft_starter] No position column in draft table")
            return 0

        # Columns pre-created by canonical_draft DDL

        # Detect auction vs snake (global)
        try:
            counts = conn.execute(f"""
                SELECT
                    COUNT(CASE WHEN cost > 0 THEN 1 END) as with_cost,
                    COUNT(*) as total
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND pick IS NOT NULL
            """).fetchone()
            has_cost, total = counts[0], counts[1]
        except Exception:
            has_cost, total = 0, 1

        is_auction = has_cost >= max(1, int(total * 0.25))
        rank_order = "cost DESC, pick ASC" if is_auction else "round ASC, pick ASC"
        flex_order = rank_order
        logger.info(f"[draft_starter] {'Auction' if is_auction else 'Snake'} draft detected")

        rows = 0

        # Step 1: Compute position_draft_rank using window function
        sql_rank = f"""
            UPDATE {draft_table}
            SET position_draft_rank = ranked.rnk
            FROM (
                SELECT rowid AS rid,
                    ROW_NUMBER() OVER (
                        PARTITION BY year, franchise_id, {pos_col}
                        ORDER BY {rank_order}
                    ) AS rnk
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND pick IS NOT NULL AND {pos_col} IS NOT NULL
            ) ranked
            WHERE {draft_table}.rowid = ranked.rid
              AND {self._db_filter()}
        """
        rows += self._execute(sql_rank, "draft_starter: position_draft_rank")

        # Step 2: Compute position_draft_label (e.g., "QB1", "RB2")
        sql_label = f"""
            UPDATE {draft_table}
            SET position_draft_label = {pos_col} || CAST(position_draft_rank AS VARCHAR)
            WHERE position_draft_rank IS NOT NULL AND {pos_col} IS NOT NULL
              AND {self._db_filter()}
        """
        rows += self._execute(sql_label, "draft_starter: position_draft_label")

        # Step 3: Load and normalize roster settings per year
        if not self.roster_by_year:
            self.load_settings_from_db()

        DEFAULT_SLOTS = {"QB": 1, "RB": 2, "WR": 3, "TE": 1, "K": 1, "DEF": 1}
        DEFAULT_FLEX = 1

        # Get all years from draft
        try:
            year_rows = conn.execute(
                f"SELECT DISTINCT year FROM {draft_table} WHERE {self._db_filter()} AND pick IS NOT NULL ORDER BY year"
            ).fetchall()
            all_years = [int(r[0]) for r in year_rows]
        except Exception:
            all_years = []

        if not all_years:
            logger.warning("[draft_starter] No years found in draft table")
            return rows

        # Build per-year config: {year: (dedicated_dict, flex_count, superflex_count)}
        year_configs = {}
        for year in all_years:
            _resolved_year, raw = self._resolve_settings_year(year, self.roster_by_year)
            dedicated = self._get_dedicated_slots(raw)
            flex_count = 0
            superflex_count = 0

            for pos, _eligible, count in self._identify_flex_positions(raw):
                if pos == "FLX":
                    flex_count += int(count)
                elif pos == "SUPER_FLEX":
                    superflex_count += int(count)

            if not dedicated:
                dedicated = DEFAULT_SLOTS.copy()
                flex_count = max(flex_count, DEFAULT_FLEX)

            year_configs[year] = (dedicated, flex_count, superflex_count)

        # Step 4: Set starter_slots_available via VALUES join
        slots_values = []
        for year, (dedicated, _, _) in year_configs.items():
            for pos, count in dedicated.items():
                slots_values.append(f"({int(year)}, '{pos}', {count})")

        if slots_values:
            values_str = ", ".join(slots_values)
            sql_slots = f"""
                UPDATE {draft_table}
                SET starter_slots_available = rs.slots
                FROM (SELECT * FROM (VALUES {values_str}) AS t(yr, pos, slots)) rs
                WHERE {draft_table}.year = rs.yr AND {draft_table}.{pos_col} = rs.pos
                  AND {self._db_filter()}
            """
            rows += self._execute(sql_slots, "draft_starter: starter_slots_available")

        # Default positions without explicit settings to 0
        sql_default = f"""
            UPDATE {draft_table}
            SET starter_slots_available = 0
            WHERE starter_slots_available IS NULL
              AND {self._db_filter()}
        """
        rows += self._execute(sql_default, "draft_starter: default slots = 0")

        # Step 5: Initialize starter/backup flags
        sql_init = f"""
            UPDATE {draft_table}
            SET drafted_as_starter = 0, drafted_as_backup = 0
            WHERE pick IS NOT NULL
              AND {self._db_filter()}
        """
        rows += self._execute(sql_init, "draft_starter: initialize flags")

        # Step 6: Mark dedicated starters (rank <= slots for position)
        sql_ded = f"""
            UPDATE {draft_table}
            SET drafted_as_starter = 1
            WHERE position_draft_rank IS NOT NULL
              AND starter_slots_available > 0
              AND position_draft_rank <= starter_slots_available
              AND {self._db_filter()}
        """
        rows += self._execute(sql_ded, "draft_starter: dedicated position starters")

        # Step 7: Mark FLEX starters (RB/WR/TE pool, sorted by draft capital)
        flex_values = [(int(y), fc) for y, (_, fc, _) in year_configs.items() if fc > 0]
        if flex_values:
            flex_str = ", ".join(f"({y}, {fc})" for y, fc in flex_values)
            sql_flex = f"""
                UPDATE {draft_table}
                SET drafted_as_starter = 1
                FROM (
                    SELECT d_inner.rid
                    FROM (
                        SELECT rowid AS rid, year, franchise_id AS _mgr_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY year, franchise_id ORDER BY {flex_order}
                            ) AS flex_rank
                        FROM {draft_table}
                        WHERE {self._db_filter()}
                          AND drafted_as_starter = 0
                          AND {pos_col} IN ('RB', 'WR', 'TE')
                          AND pick IS NOT NULL
                    ) d_inner
                    JOIN (SELECT * FROM (VALUES {flex_str}) AS t(yr, flex_count)) fc
                        ON d_inner.year = fc.yr
                    WHERE d_inner.flex_rank <= fc.flex_count
                    ) flex_picks
                WHERE {draft_table}.rowid = flex_picks.rid
                  AND {self._db_filter()}
            """
            rows += self._execute(sql_flex, "draft_starter: FLEX starters")

        # Step 8: Mark SUPERFLEX starters (QB/RB/WR/TE pool)
        sf_values = [(int(y), sc) for y, (_, _, sc) in year_configs.items() if sc > 0]
        if sf_values:
            sf_str = ", ".join(f"({y}, {sc})" for y, sc in sf_values)
            sql_sf = f"""
                UPDATE {draft_table}
                SET drafted_as_starter = 1
                FROM (
                    SELECT d_inner.rid
                    FROM (
                        SELECT rowid AS rid, year, franchise_id AS _mgr_id,
                            ROW_NUMBER() OVER (
                                PARTITION BY year, franchise_id ORDER BY {flex_order}
                            ) AS sf_rank
                        FROM {draft_table}
                        WHERE {self._db_filter()}
                          AND drafted_as_starter = 0
                          AND {pos_col} IN ('QB', 'RB', 'WR', 'TE')
                          AND pick IS NOT NULL
                    ) d_inner
                    JOIN (SELECT * FROM (VALUES {sf_str}) AS t(yr, sf_count)) sc
                        ON d_inner.year = sc.yr
                    WHERE d_inner.sf_rank <= sc.sf_count
                    ) sf_picks
                WHERE {draft_table}.rowid = sf_picks.rid
                  AND {self._db_filter()}
            """
            rows += self._execute(sql_sf, "draft_starter: SUPERFLEX starters")

        # Step 9: Mark remaining as backups
        sql_backup = f"""
            UPDATE {draft_table}
            SET drafted_as_backup = 1
            WHERE drafted_as_starter = 0 AND pick IS NOT NULL
              AND {self._db_filter()}
        """
        rows += self._execute(sql_backup, "draft_starter: mark backups")

        logger.info("[draft_starter] Starter designation complete")
        return rows

    # =========================================================================
    # DRAFT FAILURE RATES: Position-specific failure + activation rates
    # =========================================================================

    def draft_failure_rates(self) -> int:
        """Calculate position-specific starter failure and bench activation rates.

        Failure rate: fraction of starters that busted (negative LAMAR or injury).
        Activation rate: fraction of backup players that got meaningful starts.

        Adds to draft table:
        - position_failure_rate: P(starter needs replacement) per position
        - position_activation_rate: P(backup gets activated) per position

        Required by: Draft Optimizer risk modeling and insurance calculations
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_failure_rates] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")

        pos_col = None
        for c in ("primary_position", "yahoo_position", "position"):
            if c in draft_cols:
                pos_col = c
                break
        if not pos_col:
            logger.warning("[draft_failure_rates] No position column")
            return 0

        # Columns pre-created by canonical_draft DDL

        lamar_col = "manager_lamar" if "manager_lamar" in draft_cols else ("lamar" if "lamar" in draft_cols else None)
        has_starter = "drafted_as_starter" in draft_cols
        has_weeks = "weeks_started" in draft_cols

        if not lamar_col:
            logger.warning("[draft_failure_rates] No LAMAR column - applying defaults")
            return self._apply_default_failure_rates()

        # Build keeper exclusion â€” keepers under different constraints than draft picks
        fr_keeper_exclude, _ = self._keeper_filters(draft_cols)

        rows = 0

        # Failure rate: starters with negative LAMAR or too few weeks started
        # Keepers excluded from baseline â€” rates still applied to all rows (position-level)
        starter_filter = "AND drafted_as_starter = 1" if has_starter else ""
        injury_clause = "OR (weeks_started IS NOT NULL AND weeks_started < 8)" if has_weeks else ""

        sql_failure = f"""
            UPDATE {draft_table}
            SET position_failure_rate = rates.failure_rate
            FROM (
                SELECT {pos_col} AS pos,
                    ROUND(
                        SUM(CASE WHEN {lamar_col} < 0 {injury_clause} THEN 1 ELSE 0 END)::DOUBLE
                        / GREATEST(COUNT(*), 1),
                    3) AS failure_rate
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND {pos_col} IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
                  AND pick IS NOT NULL
                  {starter_filter}
                  {fr_keeper_exclude}
                GROUP BY {pos_col}
            ) rates
            WHERE {draft_table}.{pos_col} = rates.pos
              AND {self._db_filter()}
        """
        rows += self._execute(
            sql_failure, "draft_failure_rates: failure_rate by position (keepers excluded from baseline)"
        )

        # Activation rate: backups who got starts
        if has_starter and has_weeks:
            sql_activation = f"""
                UPDATE {draft_table}
                SET position_activation_rate = rates.act_rate
                FROM (
                    SELECT {pos_col} AS pos,
                        ROUND(
                            SUM(CASE WHEN weeks_started > 0 THEN 1 ELSE 0 END)::DOUBLE
                            / GREATEST(COUNT(*), 1),
                        3) AS act_rate
                    FROM {draft_table}
                    WHERE {self._db_filter()}
                      AND {pos_col} IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
                      AND pick IS NOT NULL
                      AND drafted_as_starter = 0
                      {fr_keeper_exclude}
                    GROUP BY {pos_col}
                ) rates
                WHERE {draft_table}.{pos_col} = rates.pos
                  AND {self._db_filter()}
            """
            rows += self._execute(
                sql_activation, "draft_failure_rates: activation_rate by position (keepers excluded from baseline)"
            )
        elif self._table_exists("player_fantasy"):
            # Estimate activation from player_fantasy (bench players who got starts)
            player_cols = self._get_table_columns("player_fantasy")
            player_table = self._qualified_name("player_fantasy")
            player_pos = next((c for c in ("yahoo_position", "position") if c in player_cols), None)

            if player_pos and "is_started" in player_cols:
                sql_activation = f"""
                    UPDATE {draft_table}
                    SET position_activation_rate = rates.act_rate
                    FROM (
                        SELECT p.{player_pos} AS pos,
                            ROUND(
                                COUNT(DISTINCT CASE WHEN p.is_started = 1 THEN
                                    p.NFL_player_id || '_' || CAST(p.year AS VARCHAR) END)::DOUBLE
                                / GREATEST(COUNT(DISTINCT
                                    p.NFL_player_id || '_' || CAST(p.year AS VARCHAR)), 1),
                            3) AS act_rate
                        FROM {player_table} p
                        WHERE p.{player_pos} IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
                          AND {self._db_filter('p')}
                          AND {rostered_filter_sql("p")}
                        GROUP BY p.{player_pos}
                    ) rates
                    WHERE {draft_table}.{pos_col} = rates.pos
                      AND {self._db_filter()}
                """
                rows += self._execute(sql_activation, "draft_failure_rates: activation from player_fantasy")

        # Fill any remaining NULLs with sensible defaults
        sql_defaults = f"""
            UPDATE {draft_table}
            SET position_failure_rate = COALESCE(position_failure_rate, 0.30),
                position_activation_rate = COALESCE(position_activation_rate, 0.40)
            WHERE pick IS NOT NULL
              AND {self._db_filter()}
        """
        rows += self._execute(sql_defaults, "draft_failure_rates: fill defaults")

        return rows

    def _apply_default_failure_rates(self) -> int:
        """Apply hardcoded failure/activation rate defaults when LAMAR unavailable."""
        if not self._table_exists("draft"):
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")

        pos_col = next((c for c in ("primary_position", "yahoo_position", "position") if c in draft_cols), None)
        if not pos_col:
            return 0

        # Columns pre-created by canonical_draft DDL

        sql = f"""
            UPDATE {draft_table}
            SET position_failure_rate = CASE {pos_col}
                    WHEN 'QB' THEN 0.25  WHEN 'RB' THEN 0.40
                    WHEN 'WR' THEN 0.30  WHEN 'TE' THEN 0.35
                    WHEN 'K' THEN 0.15   WHEN 'DEF' THEN 0.20
                    ELSE 0.30 END,
                position_activation_rate = CASE {pos_col}
                    WHEN 'QB' THEN 0.45  WHEN 'RB' THEN 0.55
                    WHEN 'WR' THEN 0.50  WHEN 'TE' THEN 0.40
                    WHEN 'K' THEN 0.10   WHEN 'DEF' THEN 0.15
                    ELSE 0.40 END
            WHERE {self._db_filter()}
        """
        return self._execute(sql, "draft_failure_rates: apply defaults")

    # =========================================================================
    # DRAFT BENCH VALUE BY RANK: Historical median LAMAR per position-rank
    # =========================================================================

    def draft_bench_value_by_rank(self) -> int:
        """Calculate historical median LAMAR by position_draft_label.

        Computes expected LAMAR for each position draft rank (QB1, QB2, RB1, etc.)
        across all historical years. Excludes keepers (which inflate values).
        Bench value is clipped at 0 (negative = replacement level, don't draft).

        Adds to draft table:
        - bench_value_by_rank: Median manager_lamar for this position draft rank

        Required by: Draft Optimizer bench position caps and value estimation
        """
        if not self._table_exists("draft"):
            logger.warning("[draft_bench_value] draft table not found")
            return 0

        draft_cols = self._get_table_columns("draft")
        draft_table = self._qualified_name("draft")

        if "position_draft_label" not in draft_cols:
            logger.warning("[draft_bench_value] position_draft_label not found - run draft_starter_designation first")
            return 0

        lamar_col = "manager_lamar" if "manager_lamar" in draft_cols else ("lamar" if "lamar" in draft_cols else None)
        if not lamar_col:
            logger.warning("[draft_bench_value] No LAMAR column found")
            return 0

        # Columns pre-created by canonical_draft DDL

        # Keeper exclusion
        keeper_filter, _ = self._keeper_filters(draft_cols)

        # Calculate median LAMAR by position_draft_label (across all years)
        # Clipped at 0: negative median = replacement level = not worth drafting
        sql = f"""
            UPDATE {draft_table}
            SET bench_value_by_rank = vals.med_lamar
            FROM (
                SELECT position_draft_label AS label,
                    ROUND(GREATEST(MEDIAN({lamar_col}), 0), 2) AS med_lamar
                FROM {draft_table}
                WHERE {self._db_filter()}
                  AND position_draft_label IS NOT NULL
                  AND {lamar_col} IS NOT NULL
                  AND pick IS NOT NULL
                  {keeper_filter}
                GROUP BY position_draft_label
                HAVING COUNT(*) >= 3
            ) vals
            WHERE {draft_table}.position_draft_label = vals.label
              AND {self._db_filter()}
        """
        rows = self._execute(sql, "draft_bench_value: median LAMAR by rank")

        # Default: labels without enough data get 0
        sql_default = f"""
            UPDATE {draft_table}
            SET bench_value_by_rank = COALESCE(bench_value_by_rank, 0)
            WHERE position_draft_label IS NOT NULL
              AND {self._db_filter()}
        """
        rows += self._execute(sql_default, "draft_bench_value: fill defaults")

        return rows
