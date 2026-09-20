"""
Player Enrichments Mixin

Player data normalization, scoring fixes, and deduplication methods.
"""

import logging
import os
import re

from multi_league.core.join_keys import franchise_identity_join_sql
from multi_league.core.readers.fly_reader import (  # noqa: F401 - imported for patch target
    FlyReader,
    FlyReaderError,
    FlyReaderTableNotFound,
    FlyReaderNetworkError,
)
from multi_league.core.player_week_identity import player_week_publish_dedup_statements
from multi_league.shared.filters import rostered_filter_sql  # noqa: F401 - used in f-strings
from multi_league.transformations.player.modules.scoring_calculator import (
    build_components_fantasy_points_sql,
    get_scoring_columns,
)

logger = logging.getLogger(__name__)


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except (TypeError, ValueError):
        return default


def _sql_string_literal(value: object) -> str:
    """Return a quoted SQL string literal for read-only Fly queries."""
    return "'" + str(value).replace("'", "''") + "'"


_NUMERIC_DUCKDB_TYPE_PREFIXES = (
    "TINYINT",
    "SMALLINT",
    "INTEGER",
    "BIGINT",
    "HUGEINT",
    "UTINYINT",
    "USMALLINT",
    "UINTEGER",
    "UBIGINT",
    "UHUGEINT",
    "DECIMAL",
    "DOUBLE",
    "REAL",
    "FLOAT",
)


def _is_numeric_duckdb_type(type_name: str | None) -> bool:
    """Return True when a DuckDB type name is numeric enough for COALESCE(..., 0)."""
    if not type_name:
        return False
    return str(type_name).upper().startswith(_NUMERIC_DUCKDB_TYPE_PREFIXES)


def _filter_multiplier_targets_to_available(
    multiplier_targets: set[str],
    super_cols: set[str],
    league: str,
    year: int,
    super_col_types: dict[str, str] | None = None,
) -> set[str]:
    """Filter multiplier targets to those actually present in super_table.

    Previously this raised ValueError on any missing column (spec §4.5(b),
    "catches backfill mistakes immediately"). That hard-fail was propagating
    out of populate_fantasy_points and leaving entire leagues un-scored —
    notably pre-2019 ESPN leagues whose historical scoring configs reference
    columns that were never backfilled in super_table. The function would
    abort mid-loop, leaving kickers nulled (from the pre-reset) and pre-2019
    rows with NULL fantasy_points across every position.

    New behavior: log a warning naming the missing columns, drop them from
    the contributing multiplier set, and return the filtered set. The caller
    is expected to also filter its own dict of {col: mult} down to this
    subset before building the dynamic expression.

    The missing-column signal is still visible (WARNING-level log) but it
    no longer corrupts the data for every other league-year on the same run.
    """
    missing = sorted(c for c in multiplier_targets if c not in super_cols)
    if missing:
        logger.warning(
            f"[populate_fantasy_points] {league}/{year}: dropping multipliers "
            f"for missing super_table columns: {missing}. "
            f"Add via ALTER TABLE + nflverse backfill, or remove from *_COL_MAP."
        )
    available = {c for c in multiplier_targets if c in super_cols}
    if not super_col_types:
        return available

    non_numeric = sorted(c for c in available if not _is_numeric_duckdb_type(super_col_types.get(c)))
    if non_numeric:
        logger.warning(
            f"[populate_fantasy_points] {league}/{year}: dropping multipliers "
            f"for non-numeric super_table columns: {non_numeric}. "
            f"These columns cannot be safely used in COALESCE(s.col, 0) scoring expressions."
        )
    return {c for c in available if c not in non_numeric}


class PlayerEnrichmentsMixin:
    """Mixin providing player-related SQL enrichments.

    Requires SQLEnrichmentsBase infrastructure (self._execute, self._table_exists, etc.)
    """

    def dedup_player_fantasy(self) -> int:
        """Keep one roster owner per NFL player/week.

        Sleeper can emit the same player in multiple roster matchup payloads
        for a week (usually post-lock roster churn). Downstream optimal lineup
        logic treats every rostered row as eligible, so resolve those duplicates
        before scoring/optimal waves. Preference order: started row, draft owner,
        non-bench slot, then deterministic identity.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_table = self._qualified_name("player_fantasy")
        player_cols = self._get_table_columns("player_fantasy")
        required = {"year", "week", "manager"}
        if not required.issubset(player_cols):
            return 0

        player_identity_candidates = []
        if "NFL_player_id" in player_cols:
            player_identity_candidates.append("NULLIF(TRIM(CAST(p.NFL_player_id AS VARCHAR)), '')")
        for col, prefix in [
            ("yahoo_player_id", "yahoo"),
            ("sleeper_player_id", "sleeper"),
            ("espn_player_id", "espn"),
            ("fleaflicker_player_id", "fleaflicker"),
        ]:
            if col in player_cols:
                player_identity_candidates.append(
                    f"CASE WHEN NULLIF(TRIM(CAST(p.{col} AS VARCHAR)), '') IS NOT NULL "
                    f"THEN '{prefix}:' || TRIM(CAST(p.{col} AS VARCHAR)) ELSE NULL END"
                )
        if not player_identity_candidates:
            return 0
        player_identity_expr = f"COALESCE({', '.join(player_identity_candidates)})"

        draft_join = ""
        draft_select = "0 AS draft_owner_priority"
        if self._table_exists("draft"):
            draft_cols = self._get_table_columns("draft")
            if {"NFL_player_id", "year", "franchise_id"}.issubset(draft_cols) and "franchise_id" in player_cols:
                draft_table = self._qualified_name("draft")
                draft_join = f"""
                    LEFT JOIN (
                        SELECT year, NFL_player_id, MIN(franchise_id) AS drafted_franchise_id
                        FROM {draft_table}
                        WHERE {self._db_filter()}
                          AND NFL_player_id IS NOT NULL
                          AND NULLIF(TRIM(COALESCE(franchise_id, '')), '') IS NOT NULL
                        GROUP BY year, NFL_player_id
                        HAVING COUNT(DISTINCT franchise_id) = 1
                    ) d
                      ON p.year = d.year
                     AND p.NFL_player_id = d.NFL_player_id
                """
                draft_select = (
                    "CASE WHEN d.drafted_franchise_id IS NOT NULL "
                    "AND p.franchise_id = d.drafted_franchise_id THEN 1 ELSE 0 END AS draft_owner_priority"
                )

        started_expr = "COALESCE(CAST(p.is_started AS INTEGER), 0)" if "is_started" in player_cols else "0"
        slot_expr = "UPPER(TRIM(COALESCE(p.fantasy_position, '')))" if "fantasy_position" in player_cols else "''"
        franchise_expr = "COALESCE(CAST(p.franchise_id AS VARCHAR), '')" if "franchise_id" in player_cols else "''"
        team_key_expr = "COALESCE(CAST(p.team_key AS VARCHAR), '')" if "team_key" in player_cols else "''"
        team_name_present_expr = (
            "CASE WHEN NULLIF(TRIM(COALESCE(p.team_name, '')), '') IS NOT NULL THEN 1 ELSE 0 END"
            if "team_name" in player_cols
            else "0"
        )

        sql = f"""
            CREATE OR REPLACE TEMP TABLE _player_fantasy_dedup AS
            SELECT row_id
            FROM (
                SELECT
                    p.row_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY p.year, p.week, p._dedup_player_identity
                        ORDER BY
                            {started_expr} DESC,
                            draft_owner_priority DESC,
                            CASE WHEN {slot_expr} NOT IN ('', 'BN', 'IR', 'TAXI', 'RES') THEN 1 ELSE 0 END DESC,
                            {team_name_present_expr} DESC,
                            {franchise_expr},
                            {team_key_expr},
                            COALESCE(p.manager, ''),
                            p.row_id
                    ) AS keep_rank
                FROM (
                    SELECT p.rowid AS row_id, p.*, {draft_select}, {player_identity_expr} AS _dedup_player_identity
                    FROM {player_table} p
                    {draft_join}
                    WHERE {self._db_filter("p")}
                      AND p.year IS NOT NULL
                      AND p.week IS NOT NULL
                      AND {rostered_filter_sql("p")}
                ) p
                WHERE p._dedup_player_identity IS NOT NULL
            ) ranked
            WHERE keep_rank > 1;

            DELETE FROM {player_table} p
            USING _player_fantasy_dedup d
            WHERE p.rowid = d.row_id
              AND {self._db_filter("p")};
        """
        result = self._execute(sql, "dedup_player_fantasy: one roster owner per NFL player/week")
        try:
            self._get_connection().execute("DROP TABLE IF EXISTS _player_fantasy_dedup")
        except Exception:
            pass
        return result

    def dedup_player_fantasy_publish_identity(self) -> int:
        """Collapse late player_week duplicates before analytics and upload."""
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        if "player_week" not in player_cols:
            return 0

        player_table = self._qualified_name("player_fantasy")
        statements = player_week_publish_dedup_statements(
            player_table,
            player_cols,
            db_filter=self._db_filter("p"),
        )
        if not statements:
            return 0

        conn = self._get_connection()
        temp_table = "_player_fantasy_publish_dedup"
        try:
            self._execute(statements[0][1], "dedup_player_fantasy_publish_identity: stage duplicate player_week rows")
            duplicate_count = int(conn.execute(f'SELECT COUNT(*) FROM "{temp_table}"').fetchone()[0] or 0)
            if duplicate_count:
                self._execute(
                    statements[1][1],
                    f"dedup_player_fantasy_publish_identity: delete {duplicate_count:,} duplicate player_week rows",
                )
                logger.info(
                    "[dedup_player_fantasy_publish_identity] removed " f"{duplicate_count:,} duplicate player_week rows"
                )
            return duplicate_count
        finally:
            try:
                conn.execute(statements[2][1])
            except Exception:
                pass

    # =========================================================================
    # EXPAND + SCORE: Add all NFL players, then calculate fantasy_points
    # =========================================================================

    def expand_to_all_nfl(self) -> int:
        """INSERT unrostered NFL players from super_table into player_fantasy.

        For quick imports: only the imported year(s) already present in player_fantasy.
        For full imports: the full year range available in the NFL super table.

        Players already in player_fantasy (by player_week) are skipped.
        New rows get manager='Unrostered', is_started=FALSE.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        conn = self._get_connection()
        player_table = self._qualified_name("player_fantasy")
        settings_table = self._qualified_name("league_settings")

        year_filter_sql = ""
        year_label = ""

        # Quick imports stay scoped to the imported league year. Full imports prefer
        # the active-year bounds persisted in league_settings so we do not materialize
        # out-of-scope history into player_fantasy.
        if getattr(self, "quick", False):
            years = conn.execute(
                f"SELECT DISTINCT year FROM {player_table} "
                f"WHERE year IS NOT NULL AND {self._db_filter()} ORDER BY year"
            ).fetchall()
            if not years:
                return 0
            year_list = [int(r[0]) for r in years]
            years_csv = ", ".join(str(y) for y in year_list)
            year_filter_sql = f"AND s.year IN ({years_csv})"
            year_label = str(year_list)
            logger.info(f"[expand_to_all_nfl] Quick mode: limiting expansion to years {year_list}")
        else:
            try:
                played_years = conn.execute(
                    f"""
                    SELECT DISTINCT year
                    FROM {settings_table}
                    WHERE year IS NOT NULL
                      AND {self._db_filter()}
                    ORDER BY year
                    """
                ).fetchall()
                if played_years:
                    year_list = [int(r[0]) for r in played_years]
                    years_csv = ", ".join(str(y) for y in year_list)
                    year_filter_sql = f"AND s.year IN ({years_csv})"
                    year_label = f"{year_list[0]}-{year_list[-1]} (n={len(year_list)})"
                    logger.info(
                        f"[expand_to_all_nfl] Full mode: limiting expansion to played league years " f"{year_label}"
                    )
                else:
                    years = conn.execute(
                        """
                        SELECT DISTINCT year
                        FROM ___ops.nfl_historical.nfl_player_stats_all
                        WHERE year IS NOT NULL
                        ORDER BY year
                        """
                    ).fetchall()
                    if not years:
                        return 0
                    year_list = [int(r[0]) for r in years]
                    years_csv = ", ".join(str(y) for y in year_list)
                    year_filter_sql = f"AND s.year IN ({years_csv})"
                    year_label = f"{year_list[0]}-{year_list[-1]}"
                    logger.info(
                        f"[expand_to_all_nfl] Full mode: expanding across all NFL years "
                        f"{year_label} ({len(year_list)} years)"
                    )
            except Exception as e:
                logger.warning(
                    f"[expand_to_all_nfl] Could not read active-year bounds; "
                    f"falling back to player_fantasy years: {e}"
                )
                years = conn.execute(
                    f"SELECT DISTINCT year FROM {player_table} "
                    f"WHERE year IS NOT NULL AND {self._db_filter()} ORDER BY year"
                ).fetchall()
                if not years:
                    return 0
                year_list = [int(r[0]) for r in years]
                years_csv = ", ".join(str(y) for y in year_list)
                year_filter_sql = f"AND s.year IN ({years_csv})"
                year_label = str(year_list)

        # Detect positions from roster settings
        roster_by_year = getattr(self, "roster_by_year", {}) or {}
        if not roster_by_year:
            self.load_settings_from_db()
            roster_by_year = getattr(self, "roster_by_year", {}) or {}

        # Collect all positions used across all years
        positions = set()
        for yr_settings in roster_by_year.values():
            if isinstance(yr_settings, dict):
                for pos in yr_settings:
                    if pos not in ("scoring_settings", "def_multipliers") and isinstance(yr_settings[pos], int | float):
                        positions.add(pos.upper())

        # Supplement with observed positions from player_fantasy and draft.
        # Some leagues have incomplete roster_* columns in league_settings.
        extra_positions = set()
        ignored_positions = {
            "",
            "BN",
            "BENCH",
            "IR",
            "PUP",
            "NA",
            "N/A",
            "TAXI",
            "FA",
            "FREE AGENT",
            "RES",
            "RESERVE",
            "UNROSTERED",
            "W/R",
            "W/T",
            "W/R/T",
            "FLEX",
            "REC_FLEX",
            "SUPER_FLEX",
            "OP",
        }
        known_positions = {
            "QB",
            "RB",
            "WR",
            "TE",
            "K",
            "DEF",
            "LB",
            "DL",
            "DB",
            "IDP",
            "FB",
            "P",
            "C",
            "G",
            "OT",
            "OL",
            "LS",
            "KR",
            "PR",
        }
        pos_aliases = {
            "DST": "DEF",
            "DEFENSE": "DEF",
            "D": "IDP",
            "DE": "DL",
            "DT": "DL",
            "NT": "DL",
            "ED": "DL",
            "EDGE": "DL",
            "CB": "DB",
            "S": "DB",
            "FS": "DB",
            "SS": "DB",
            "SAF": "DB",
            "OLB": "LB",
            "ILB": "LB",
            "MLB": "LB",
        }

        def _ingest_positions(rows):
            for (raw_pos,) in rows:
                if raw_pos is None:
                    continue
                pos = str(raw_pos).strip().upper()
                if not pos or pos in ignored_positions:
                    continue
                # Split composite tokens (e.g., "DL/LB") and normalize aliases
                tokens = re.split(r"[^A-Z0-9]+", pos)
                for token in tokens:
                    if not token or token in ignored_positions:
                        continue
                    norm = pos_aliases.get(token, token)
                    if norm in known_positions:
                        extra_positions.add(norm)

        try:
            pf_rows = conn.execute(
                f"""
                SELECT DISTINCT UPPER(position) AS position
                FROM {player_table}
                WHERE position IS NOT NULL AND {self._db_filter()}
                """
            ).fetchall()
            _ingest_positions(pf_rows)
        except Exception:
            pass

        if self._table_exists("draft"):
            try:
                draft_table = self._qualified_name("draft")
                draft_rows = conn.execute(
                    f"""
                    SELECT DISTINCT UPPER(position) AS position
                    FROM {draft_table}
                    WHERE position IS NOT NULL AND {self._db_filter()}
                    """
                ).fetchall()
                _ingest_positions(draft_rows)
            except Exception:
                pass

        if extra_positions:
            positions.update(extra_positions)
        # IDP slots imply LB/DL/DB pools in super_table (not an "IDP" position)
        if any("IDP" in p for p in positions):
            positions = {p for p in positions if "IDP" not in p}
            positions.update({"LB", "DL", "DB"})
        # Standard positions if none detected
        if not positions:
            positions = {"QB", "RB", "WR", "TE", "K", "DEF"}

        # Map roster positions → NFL positions for super_table filter
        nfl_pos_map = {
            "LB": ["LB", "OLB", "ILB", "MLB"],
            "DL": ["DL", "DE", "DT", "NT", "ED", "EDGE"],
            "DB": ["DB", "CB", "S", "SAF", "FS", "SS", "WR,DB"],
            "QB": ["QB", "QB,K", "TE,QB"],
            "RB": ["RB", "RB,K"],
            "WR": ["WR", "WR,DB", "WR,K"],
            "TE": ["TE", "TE,QB"],
            "K": ["K", "QB,K", "RB,K", "WR,K", "P,K"],
            "DEF": ["DEF"],
            "FB": ["FB", "RB"],
            "P": ["P", "P,K"],
            "C": ["C"],
            "G": ["G"],
            "OT": ["OT"],
            "OL": ["OL"],
            "LS": ["LS"],
            "KR": ["KR"],
            "PR": ["PR"],
        }
        nfl_positions = []
        for p in positions:
            nfl_positions.extend(nfl_pos_map.get(p, [p]))
        nfl_pos_sql = ", ".join(f"'{p}'" for p in set(nfl_positions))

        # Position normalization for IDP families
        position_expr = """CASE
            WHEN s.nfl_position IN ('LB', 'OLB', 'ILB', 'MLB') THEN 'LB'
            WHEN s.nfl_position IN ('DL', 'DE', 'DT', 'NT', 'ED') THEN 'DL'
            WHEN s.nfl_position IN ('DB', 'CB', 'S', 'SAF', 'FS', 'SS') THEN 'DB'
            ELSE s.nfl_position
        END"""

        deduped_super_source_sql = f"""
            SELECT player_week, NFL_player_id, player, year, week, nfl_position
            FROM (
                SELECT
                    s.player_week,
                    s.NFL_player_id,
                    s.player,
                    s.year,
                    s.week,
                    s.nfl_position,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.player_week
                        ORDER BY s.NFL_player_id NULLS LAST, s.player NULLS LAST, s.year, s.week
                    ) AS row_num
                FROM ___ops.nfl_historical.nfl_player_stats_all s
                WHERE s.player_week IS NOT NULL
                  AND s.nfl_position IN ({nfl_pos_sql})
                  {year_filter_sql}
            ) s
            WHERE row_num = 1
        """

        # Count new records
        count_sql = f"""
            SELECT COUNT(*)
            FROM ({deduped_super_source_sql}) s
            WHERE s.player_week IS NOT NULL
              AND s.player_week NOT IN (
                  SELECT player_week
                  FROM {player_table}
                  WHERE player_week IS NOT NULL
                    AND {self._db_filter()}
              )
        """
        new_count = conn.execute(count_sql).fetchone()[0]
        logger.info(f"[expand_to_all_nfl] {new_count:,} unrostered rows to add for years {year_label}")

        if new_count == 0:
            return 0

        # INSERT unrostered players with canonical join keys populated up front
        insert_sql = f"""
            INSERT INTO {player_table}
                (db_name, player_week, NFL_player_id, player, year, week, cumulative_week,
                 position, manager, is_started)
            SELECT
                '{self.db_name}', s.player_week, s.NFL_player_id, s.player, s.year, s.week,
                (CAST(s.year AS BIGINT) * 100 + CAST(s.week AS BIGINT)),
                {position_expr}, 'Unrostered', FALSE
            FROM ({deduped_super_source_sql}) s
            WHERE s.player_week IS NOT NULL
              AND s.player_week NOT IN (
                  SELECT player_week
                  FROM {player_table}
                  WHERE player_week IS NOT NULL
                    AND {self._db_filter()}
              )
        """
        self._execute(insert_sql, f"expand_to_all_nfl: INSERT {new_count:,} unrostered rows")

        # Verify
        total = conn.execute(f"SELECT COUNT(*) FROM {player_table} WHERE {self._db_filter()}").fetchone()[0]
        unrostered = conn.execute(
            f"SELECT COUNT(*) FROM {player_table} WHERE manager = 'Unrostered' AND {self._db_filter()}"
        ).fetchone()[0]
        logger.info(f"[expand_to_all_nfl] Total: {total:,} rows ({unrostered:,} unrostered)")

        # Invalidate column cache (table may have changed)
        self._invalidate_column_cache("player_fantasy")
        return new_count

    def populate_fantasy_points(self) -> int:
        """Calculate fantasy_points for all rows using league scoring rules.

        Consolidated method that replaces:
        - backfill_fantasy_points_from_super_table (offense)
        - fix_def_fantasy_points (DEF with custom multipliers)
        - fix_idp_fantasy_points (IDP with custom multipliers)
        - apply_bonus_scoring (milestone bonuses)
        - apply_te_premium (TE extra PPR)

        Runs AFTER expand_to_all_nfl so all rows (rostered + unrostered) get scored.
        Uses per-year scoring from flat league_settings columns.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        if "player_week" not in player_cols or "position" not in player_cols:
            return 0

        conn = self._get_connection()
        player_table = self._qualified_name("player_fantasy")
        super_cols = self._get_super_table_columns()
        super_col_types = self._get_super_table_column_types()
        numeric_super_cols = {col for col in super_cols if _is_numeric_duckdb_type(super_col_types.get(col))}

        # Load settings if not already loaded
        roster_by_year = getattr(self, "roster_by_year", {}) or {}
        if not roster_by_year:
            self.load_settings_from_db()
            roster_by_year = getattr(self, "roster_by_year", {}) or {}

        # Get years
        years = conn.execute(
            f"SELECT DISTINCT year FROM {player_table} " f"WHERE year IS NOT NULL AND {self._db_filter()} ORDER BY year"
        ).fetchall()
        if not years:
            return 0

        total = 0
        primary_pos_sql = self._primary_position_sql("COALESCE(pb.nfl_position, p.position)")

        # Scope filter: trust platform-provided points where the API actually
        # supplied them, and only recompute where we have to. Overwriting
        # API-provided rows on Sleeper/modern ESPN produces a persistent
        # delta vs matchup.team_points (which is also API-sourced), since
        # our DDL-derived scoring config will not exactly match the
        # platform's scoring function in every edge case (bonuses, rounding,
        # per-reception tiers, etc.). The canonical recompute is ground
        # truth ONLY for rows the API cannot speak to:
        #   - Unrostered players (all platforms, all years) — no API points
        #   - Yahoo rostered rows with missing points only — yahoo_rosters +
        #     yahoo_nfl_merge already preserve platform points and seed the
        #     historical archive gaps before this SQL pass runs
        #   - ESPN rostered year < 2019 — API historical coverage gap
        # Everything else (Sleeper/Fleaflicker rostered, ESPN rostered year >=
        # 2019) keeps the platform-stored value ONLY when that value is
        # non-NULL.
        # cffl on 2026-04-15 exposed the gap: modern ESPN kicker rows can
        # still have NULL platform fantasy_points, and those must be
        # backfilled by the canonical recompute rather than preserved.
        #
        # Jupiter Fantasy League on 2026-04-24 exposed the same failure mode
        # on Yahoo historical imports: we were overwriting good API-sourced
        # roster points with a best-effort scoring rebuild, which drifted away
        # from matchup.team_points for non-standard historical seasons.
        platform = self._detect_platform()
        unrostered_clause = (
            "(p.manager IS NULL "
            "OR TRIM(COALESCE(p.manager, '')) = '' "
            "OR LOWER(TRIM(p.manager)) IN ('unrostered', 'fa', 'free agent', 'waivers'))"
        )
        null_points_clause = "p.fantasy_points IS NULL"
        if platform in {"sleeper", "fleaflicker"}:
            recompute_points = f"({unrostered_clause} OR {null_points_clause})"
        elif platform == "espn":
            recompute_points = f"(p.year < 2019 OR {unrostered_clause} OR {null_points_clause})"
        elif platform == "yahoo":
            # Yahoo imports are DDL-scored end-to-end. The matchup table keeps
            # Yahoo's official team score for reconciliation, while every
            # player row is recomputed from super_table x league_settings so
            # reimports are idempotent and scoring-rule gaps are visible.
            recompute_points = "TRUE"
        else:
            # Unknown fallback: keep the historical behavior and recompute.
            recompute_points = "TRUE"

        # Note: the unconditional kicker pre-reset that used to live here
        # (UPDATE ... SET fantasy_points = NULL WHERE position = 'K') was
        # deleted because (a) the per-group UPDATE below uses a COALESCE
        # fallback so a successful recompute always overwrites the stored
        # value, (b) the CASE branch at primary_pos_sql = 'K' already routes
        # kickers to kick_expr so the "fell through to offense_expr" bug the
        # reset was patching is no longer live, and (c) the reset combined
        # with a mid-loop ValueError left kickers at NULL fleet-wide whenever
        # any group raised — the exact failure mode that made every rostered
        # kicker show 0.00 points in the UI.

        year_groups: dict[tuple, dict] = {}

        for (year_val,) in years:
            year = int(year_val)
            _resolved_year, yr_settings = self._resolve_scoring_year(year, roster_by_year)
            scoring = dict(yr_settings.get("scoring_settings", {}) if isinstance(yr_settings, dict) else {})
            # Last-resort: only fires if no year has populated scoring at all.
            if "rec" not in scoring:
                scoring["rec"] = getattr(self, "ppr", 0.0)
            if "pass_td" not in scoring:
                scoring["pass_td"] = getattr(self, "pass_td_pts", 4)
            scoring_columns = get_scoring_columns({"scoring_settings": scoring})
            rb_premium = float(scoring_columns.get("rb_premium", 0.0) or 0.0)
            wr_premium = float(scoring_columns.get("wr_premium", 0.0) or 0.0)

            ppr_val = scoring.get("rec", getattr(self, "ppr", 0.0))
            td_val = scoring.get("pass_td", getattr(self, "pass_td_pts", 4))
            ppr_key = {0: "0ppr", 0.5: "half", 1.0: "ppr"}.get(float(ppr_val), "half")
            td_key = f"{int(td_val)}pt"
            fpts_col = f"fpts_{td_key}_{ppr_key}"
            if fpts_col not in super_cols:
                fpts_col = "fpts_4pt_half"

            offense_expr = build_components_fantasy_points_sql(
                {"scoring_settings": scoring},
                table_alias="s",
                position_sql=primary_pos_sql,
                include_bonus_flags=False,
                include_position_bonuses=False,
                available_columns=numeric_super_cols,
            )
            te_component_col = scoring_columns.get("rec_tep")

            def_mults = (
                yr_settings.get("def_multipliers", getattr(self, "def_multipliers", {}))
                if isinstance(yr_settings, dict)
                else getattr(self, "def_multipliers", {})
            )
            idp_mults = (
                yr_settings.get("idp_multipliers", getattr(self, "idp_multipliers", {}))
                if isinstance(yr_settings, dict)
                else getattr(self, "idp_multipliers", {})
            )
            idp_variant = (
                yr_settings.get("idp_scoring", getattr(self, "idp_scoring", "std"))
                if isinstance(yr_settings, dict)
                else getattr(self, "idp_scoring", "std")
            )
            kick_col = (
                yr_settings.get("kick_col", getattr(self, "kick_col", "pts_k_std"))
                if isinstance(yr_settings, dict)
                else getattr(self, "kick_col", "pts_k_std")
            )
            bonus_mults = (
                yr_settings.get("bonus_multipliers", getattr(self, "bonus_multipliers", {}))
                if isinstance(yr_settings, dict)
                else getattr(self, "bonus_multipliers", {})
            )
            te_premium = (
                float(yr_settings.get("te_premium", getattr(self, "te_premium", 0.0)))
                if isinstance(yr_settings, dict)
                else float(getattr(self, "te_premium", 0.0))
            )
            if te_premium:
                te_component_col = None
            kick_mults = (
                yr_settings.get("kick_multipliers", getattr(self, "kick_multipliers", {}))
                if isinstance(yr_settings, dict)
                else getattr(self, "kick_multipliers", {})
            )

            key = (
                offense_expr,
                te_component_col,
                fpts_col,
                kick_col,
                idp_variant,
                tuple(sorted((col, float(mult)) for col, mult in def_mults.items())),
                tuple(sorted((col, float(mult)) for col, mult in idp_mults.items())),
                tuple(sorted((col, float(mult)) for col, mult in bonus_mults.items())),
                tuple(sorted((col, float(mult)) for col, mult in kick_mults.items())),
                te_premium,
                rb_premium,
                wr_premium,
            )

            group = year_groups.setdefault(
                key,
                {
                    "config": {
                        "fpts_col": fpts_col,
                        "offense_expr": offense_expr,
                        "te_component_col": te_component_col,
                        "kick_col": kick_col,
                        "idp_variant": idp_variant,
                        "def_mults": def_mults,
                        "idp_mults": idp_mults,
                        "bonus_mults": bonus_mults,
                        "kick_mults": kick_mults,
                        "te_premium": te_premium,
                        "rb_premium": rb_premium,
                        "wr_premium": wr_premium,
                    },
                    "years": [],
                },
            )
            group["years"].append(year)

        super_cols_set = set(super_cols)

        for group in year_groups.values():
            years_sorted = sorted(group["years"])
            try:
                years_csv = ", ".join(str(year) for year in years_sorted)
                cfg = group["config"]
                fpts_col = cfg["fpts_col"]
                kick_col = cfg["kick_col"]

                # Filter out any multipliers targeting columns that don't exist in
                # super_table. Previously this raised and killed the whole function;
                # now it logs a WARNING and drops the missing contributions so the
                # remaining multipliers still build a valid expression.
                all_targets = (
                    set(cfg["def_mults"].keys())
                    | set(cfg["idp_mults"].keys())
                    | set(cfg["kick_mults"].keys())
                    | set(cfg["bonus_mults"].keys())
                )
                if cfg["te_component_col"]:
                    all_targets.add(cfg["te_component_col"])
                available_targets = _filter_multiplier_targets_to_available(
                    all_targets,
                    super_cols_set,
                    league=str(self.db_name),
                    year=years_sorted[0],
                    super_col_types=super_col_types,
                )
                def_mults_f = {k: v for k, v in cfg["def_mults"].items() if k in available_targets}
                idp_mults_f = {k: v for k, v in cfg["idp_mults"].items() if k in available_targets}
                kick_mults_f = {k: v for k, v in cfg["kick_mults"].items() if k in available_targets}
                bonus_mults_f = {k: v for k, v in cfg["bonus_mults"].items() if k in available_targets}

                # cffl 2023 exposed that existence-only filtering is not enough:
                # a configured kicker column can exist in super_table but still be
                # non-numeric, which makes DuckDB reject COALESCE(s.col, 0).
                if kick_col not in super_cols_set or not _is_numeric_duckdb_type(super_col_types.get(kick_col)):
                    if kick_col != "pts_k_std":
                        logger.warning(
                            f"[populate_fantasy_points] {self.db_name}/{years_sorted[0]}: "
                            f"kick_col {kick_col!r} is missing or non-numeric in super_table, "
                            f"falling back to pts_k_std"
                        )
                    kick_col = "pts_k_std"

                offense_expr = cfg["offense_expr"]

                # DEF TD scoring reads the canonical precomputed component.
                # Reconstructing it as def_tds + fum_ret_td is unsafe because
                # team-DST def_tds can already include fumble-return TDs.
                def_parts: list[str] = []
                from multi_league.transformations.common.dst_brackets import (
                    BRACKET_BOUNDS,
                    bracket_scored_sql,
                )

                for col, mult in def_mults_f.items():
                    if mult == 0:
                        continue
                    bracket_spec = BRACKET_BOUNDS.get(col)
                    bracket_expr = (
                        bracket_scored_sql(col, mult, alias="s")
                        if bracket_spec and bracket_spec[0] in super_cols_set
                        else None
                    )
                    if bracket_expr:
                        def_parts.append(bracket_expr)
                    else:
                        def_parts.append(f"COALESCE(s.{col}, 0) * {mult}")
                def_expr = " + ".join(def_parts) if def_parts else "COALESCE(s.pts_def_std, 0)"

                idp_parts: list[str] = []
                for col, mult in idp_mults_f.items():
                    if mult == 0 or col.startswith("_"):
                        continue
                    if col == "pts_idp_fr":
                        idp_parts.append(f"COALESCE(s.fum_rec, 0) * {mult}")
                    elif col == "pts_idp_td":
                        idp_parts.append(f"(COALESCE(s.def_tds, 0) + COALESCE(s.fum_ret_td, 0)) * {mult}")
                    elif col == "pts_idp_fum_ret_td":
                        idp_parts.append(f"COALESCE(s.fum_ret_td, 0) * {mult}")
                    elif col == "pts_idp_pass_def_3p":
                        idp_parts.append(f"LEAST(COALESCE(s.pts_idp_pd, 0) * {mult}, 3.0)")
                    else:
                        idp_parts.append(f"COALESCE(s.{col}, 0) * {mult}")
                if idp_parts:
                    idp_expr = " + ".join(idp_parts)
                else:
                    idp_expr = f"COALESCE(s.pts_idp_{cfg['idp_variant']}, 0)"

                bonus_parts = [f"COALESCE(s.{col}, 0) * {mult}" for col, mult in bonus_mults_f.items() if mult != 0]
                bonus_expr = " + ".join(bonus_parts) if bonus_parts else "0.0"
                if cfg["te_component_col"] and cfg["te_component_col"] in available_targets:
                    te_expr = (
                        f"CASE WHEN {primary_pos_sql} = 'TE' "
                        f"THEN COALESCE(s.{cfg['te_component_col']}, 0) ELSE 0.0 END"
                    )
                elif cfg["te_premium"]:
                    te_expr = (
                        f"CASE WHEN {primary_pos_sql} = 'TE' "
                        f"THEN COALESCE(s.receptions, 0) * {cfg['te_premium']} ELSE 0.0 END"
                    )
                else:
                    te_expr = "0.0"
                rb_expr = (
                    f"CASE WHEN {primary_pos_sql} = 'RB' "
                    f"THEN COALESCE(s.receptions, 0) * {cfg['rb_premium']} ELSE 0.0 END"
                    if cfg["rb_premium"]
                    else "0.0"
                )
                wr_expr = (
                    f"CASE WHEN {primary_pos_sql} = 'WR' "
                    f"THEN COALESCE(s.receptions, 0) * {cfg['wr_premium']} ELSE 0.0 END"
                    if cfg["wr_premium"]
                    else "0.0"
                )
                position_bonus_expr = f"({te_expr}) + ({rb_expr}) + ({wr_expr})"

                kick_parts = [f"COALESCE(s.{col}, 0) * {mult}" for col, mult in kick_mults_f.items() if mult != 0]
                kick_expr = " + ".join(kick_parts) if kick_parts else f"COALESCE(s.{kick_col}, 0)"
                base_expr = f"""CASE
                    WHEN {primary_pos_sql} IN ('DEF', 'DST', 'D/ST') THEN {def_expr}
                    WHEN {primary_pos_sql} IN ('LB','DL','DB','ILB','OLB','MLB','DE','DT','ED','CB','S','SS','FS','D')
                        THEN {idp_expr}
                    WHEN {primary_pos_sql} = 'K' THEN {kick_expr}
                    ELSE {offense_expr}
                END"""
                adjusted_bonus_expr = (
                    f"CASE WHEN {primary_pos_sql} IN ('DEF', 'DST', 'D/ST', 'K') THEN 0.0 ELSE {bonus_expr} END"
                    if bonus_parts
                    else "0.0"
                )
                if not self.dry_run:
                    conn.execute(f"""
                        CREATE OR REPLACE TEMP TABLE _points_stage AS
                        SELECT DISTINCT
                            p.player_week,
                            CAST(({base_expr}) AS DOUBLE) AS base_points,
                            CAST(({adjusted_bonus_expr}) AS DOUBLE) AS bonus_points,
                            CAST(({position_bonus_expr}) AS DOUBLE) AS te_premium_points,
                            CAST(({base_expr}) + ({adjusted_bonus_expr}) + ({position_bonus_expr}) AS DOUBLE) AS fantasy_points
                        FROM {player_table} p
                        LEFT JOIN ___ops.nfl_historical.player_bio pb
                          ON CAST(p.NFL_player_id AS VARCHAR) = CAST(pb.NFL_player_id AS VARCHAR)
                        LEFT JOIN ___ops.nfl_historical.nfl_player_stats_all s
                          ON p.player_week = s.player_week
                        WHERE p.year IN ({years_csv})
                          AND p.NFL_player_id IS NOT NULL
                          AND {self._db_filter('p')}
                    """)
                    # COALESCE fallback: if the recompute produces NULL for a row
                    # (e.g. super_table has no stats for that player_week), preserve
                    # the platform-stored value rather than clobbering it with NULL.
                    # This is the null-safe half of 7a5265a7 — canonical recompute
                    # still wins where it has data; stored values survive the gaps.
                    conn.execute(f"""
                        UPDATE {player_table} p
                        SET fantasy_points = CASE WHEN {recompute_points}
                                THEN COALESCE(st.fantasy_points, p.fantasy_points)
                                ELSE p.fantasy_points END,
                            bonus_points = COALESCE(st.bonus_points, p.bonus_points),
                            te_premium_points = COALESCE(st.te_premium_points, p.te_premium_points)
                        FROM _points_stage st
                        WHERE p.player_week = st.player_week
                          AND p.year IN ({years_csv})
                          AND {self._db_filter('p')}
                    """)
                    total += conn.execute("SELECT COUNT(*) FROM _points_stage").fetchone()[0]
                    conn.execute("DROP TABLE IF EXISTS _points_stage")
                else:
                    total += len(group["years"])

                logger.info(
                    f"[populate_fantasy_points] years {years_sorted}: "
                    f"{fpts_col}, offense=component_dispatch, "
                    f"kick={kick_col}, def_rules={len(def_parts)}, idp_rules={len(idp_parts)}, "
                    f"bonus_rules={len(bonus_parts)}, te_component={bool(cfg['te_component_col'])}"
                )
            except Exception as e:
                # Belt-and-suspenders: one malformed group never kills other
                # groups' updates. The log surfaces the skip for follow-up.
                logger.error(
                    f"[populate_fantasy_points] {self.db_name} years {years_sorted}: "
                    f"skipping group after error: {type(e).__name__}: {e}"
                )
                continue

        return total

    def backfill_fantasy_position_from_position(self) -> int:
        """Backfill fantasy_position from position where the fetcher left it NULL.

        fantasy_position is supposed to hold the lineup slot the manager started
        the player in (QB, RB, WR, TE, FLEX, BN, IR, DEF, K). The UI reads this
        field to render the per-player badge on the lineups page.

        The ESPN historical API does NOT return lineup slot data for older
        seasons (observed: 2014-2018 on tfl_of_extraordinary_gentleman,
        the_pigskin_platoon, potatoes, the_league_formerly_the_cffl). When the
        fetcher has no slot info it stores NULL, and the UI falls back to "BN"
        even though most of those rows are is_started=1. Result: pages like
        /home/lineups render every player as bench with 0.00 points.

        Resolution for the display gap: copy the NFL position from p.position
        when fantasy_position is NULL. Tom Brady gets a "QB" badge, Tyreek Hill
        gets "WR", etc. It's not the true lineup slot (FLEX/BN/IR info is
        permanently lost for those years) but it's the best we can do and it's
        the same thing the Sleeper/Yahoo paths effectively produce when slot
        data is available.

        This does NOT touch is_started. That flag is separately garbage for
        pre-2019 ESPN (fetcher defaults to 1 when slot info is missing) — the
        ESPN-fetcher investigation is a separate workstream.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        if "fantasy_position" not in player_cols or "position" not in player_cols:
            return 0

        player_table = self._qualified_name("player_fantasy")
        sql = f"""
            UPDATE {player_table}
            SET fantasy_position = position
            WHERE fantasy_position IS NULL
              AND position IS NOT NULL
              AND TRIM(position) != ''
              AND {self._db_filter()}
        """
        count = self._execute(sql, "backfill_fantasy_position_from_position")
        if count and count > 0:
            logger.info(f"[backfill_fantasy_position] filled {count:,} NULL fantasy_position rows from position")
        return count or 0

    # =========================================================================
    # LEGACY WRAPPERS (delegate to populate_fantasy_points)
    # =========================================================================

    def backfill_fantasy_points_from_super_table(self) -> int:
        """Legacy wrapper — use populate_fantasy_points instead."""
        return self.populate_fantasy_points()

    # =========================================================================

    def fix_zero_point_starters(self) -> int:
        """Fix players marked as started who didn't actually play.

        ESPN (especially 2020 COVID season) marks IR/inactive players as started
        because they're in active lineup slots. If a player has is_started=1 and
        fantasy_points=0 and their player_week doesn't exist in the super_table
        (they didn't play that week), mark them as bench (is_started=0).

        Excludes DEF/K since those can legitimately score 0.
        Only applies when the manager's team_points > 0 (avoid un-starting
        entire rosters for legitimate 0-point games).
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        if "is_started" not in player_cols or "player_week" not in player_cols:
            return 0

        player_table = self._qualified_name("player_fantasy")
        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")
        identity_join_sql = franchise_identity_join_sql(
            "m",
            "pf",
            left_has_franchise_id="franchise_id" in matchup_cols,
            right_has_franchise_id="franchise_id" in player_cols,
        )

        # Only un-start players whose manager scored > 0 in that week
        # (if team_points=0, the whole roster legitimately scored 0 — don't touch)
        sql = f"""
            UPDATE {player_table} pf
            SET is_started = 0,
                fantasy_position = 'BN'
            WHERE pf.is_started = 1
              AND COALESCE(pf.fantasy_points, 0) = 0
              AND COALESCE(pf.position, '') NOT IN ('DEF', 'K')
              AND pf.player_week IS NOT NULL
              AND {self._db_filter('pf')}
              AND pf.player_week NOT IN (
                  SELECT player_week FROM ___ops.nfl_historical.nfl_player_stats_all
                  WHERE player_week IS NOT NULL
              )
              AND EXISTS (
                  SELECT 1 FROM {matchup_table} m
                  WHERE m.year = pf.year AND m.week = pf.week
                    AND {self._db_filter('m')}
                    AND {identity_join_sql} AND m.team_points > 0
              )
        """
        count = self._execute(sql, "fix_zero_point_starters: unstart players who didn't play")
        if count and count > 0:
            logger.info(f"[fix_zero_point_starters] Fixed {count:,} zero-point starters (didn't play)")
        return count or 0

    def apply_player_bio_positions(self) -> int:
        """Override player_fantasy.position from player_bio.nfl_position for a
        narrow whitelist of canonical-slot transitions.

        Why: super_table position has known misclassifications that break the
        optimal lineup computation. Examples on pilot leagues:
          - Devin Funchess: super says 'TE', bio says 'WR'. Manager started
            him at WR. Optimal greedily put him in TE slot, displacing
            Kittle, costing 2.9 pts on nyu 2018 w6.
          - Keanu Neal: super says 'LB', bio says 'DB'. Manager started him
            in the DB slot. Optimal put him in _FRONT7, leaving DB empty.

        player_bio has 99.6% nfl_position coverage and is sourced from PFR
        which is more authoritative on player classification than the
        per-week super_table rows (which can flip based on the game's
        snap-count data).

        Why a whitelist, not a blanket override: bio is noisy for fullbacks
        (tags 76+ players as 'FB' that fantasy plays as 'RB'), edge rushers
        (DE ↔ DL is semantically equivalent for front-7 eligibility), and
        a handful of outright wrong entries (TE→OT, WR→G). Only transitions
        that CHANGE canonical roster eligibility are applied here:
          - WR/TE/RB skill-position relabels (Funchess TE→WR)
          - LB → DB-family reclassification (Neal LB→DB)

        This does NOT touch:
          - DE/DT/NT ↔ DL (all resolve to front-7 family already)
          - CB/S/SS/FS ↔ DB (all resolve to DB family already)
          - OLB/ILB/MLB ↔ LB (all resolve to LB family already)
          - FB (fantasy plays FB as RB; bio would regress 76 players)
          - multi-position bio values like 'TE,QB' (preserved as-is on
            player_fantasy rather than rewriting via this enrichment)
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        if "position" not in player_cols or "NFL_player_id" not in player_cols:
            return 0

        player_table = self._qualified_name("player_fantasy")

        sql = f"""
            UPDATE {player_table} p
            SET position = pb.nfl_position
            FROM ___ops.nfl_historical.player_bio pb
            WHERE CAST(p.NFL_player_id AS VARCHAR) = CAST(pb.NFL_player_id AS VARCHAR)
              AND {self._db_filter('p')}
              AND pb.nfl_position IS NOT NULL
              AND pb.nfl_position != ''
              AND pb.nfl_position NOT LIKE '%,%'
              AND UPPER(p.position) != UPPER(pb.nfl_position)
              AND (
                (UPPER(p.position) IN ('WR','TE','RB')
                  AND UPPER(pb.nfl_position) IN ('WR','TE','RB'))
                OR (UPPER(p.position) = 'LB'
                  AND UPPER(pb.nfl_position) IN ('DB','CB','S','SS','FS','NB'))
              )
        """
        count = self._execute(sql, "apply_player_bio_positions: override from player_bio canonical")
        if count and count > 0:
            logger.info(
                f"[apply_player_bio_positions] Overrode {count:,} player_fantasy rows "
                f"from player_bio.nfl_position (canonical skill/DB transitions only)"
            )
        return count or 0

    def apply_super_table_skill_positions(self) -> int:
        """Override player_fantasy.position FB->RB using super_table as the source.

        Why a separate method (vs. extending apply_player_bio_positions):
        player_bio also classifies fullbacks as 'FB' (would regress 76+ players),
        so apply_player_bio_positions deliberately excludes the FB->RB transition.
        super_table classifies the same players (Ricard, Juszczyk, Luepke, Ham,
        Blasingame, Ingold) as 'RB' across all weeks they played, so it is the
        right source for this specific transition.

        Why this matters (concrete fleet-wide bug fixed here):
        Sleeper API's player cache reports depth-chart position 'FB' for these
        players, and that label flows through normalize_roster_df() into
        player_fantasy.position unchanged. The LAMAR replacement-pool grouping
        buckets by UPPER(SPLIT_PART(position, ',', 1)); with position='FB' there
        are zero FB starters with positive fantasy_points, so manager_lamar and
        clutch_equity stay NULL on every rostered/started row for these players.
        Verified on the_tfl 2024 W12 (Patrick Ricard, manager MagicMawlz).

        Narrow whitelist (FB->RB only): IDP labels (LB, DL, DB, CB, DT, etc.)
        are intentionally raw in super_table for IDP leagues — do not blanket
        normalize them here.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        if "position" not in player_cols or "NFL_player_id" not in player_cols:
            return 0

        player_table = self._qualified_name("player_fantasy")

        sql = f"""
            UPDATE {player_table} p
            SET position = 'RB'
            FROM (
                SELECT DISTINCT NFL_player_id
                FROM ___ops.nfl_historical.nfl_player_stats_all
                WHERE UPPER(position) = 'RB'
                  AND NFL_player_id IS NOT NULL
            ) s
            WHERE CAST(p.NFL_player_id AS VARCHAR) = CAST(s.NFL_player_id AS VARCHAR)
              AND {self._db_filter('p')}
              AND UPPER(p.position) = 'FB'
        """
        count = self._execute(sql, "apply_super_table_skill_positions: override FB->RB from super_table")
        if count and count > 0:
            logger.info(
                f"[apply_super_table_skill_positions] Overrode {count:,} FB->RB rows "
                f"from super_table (Sleeper depth-chart leak fix)"
            )
        return count or 0

    def fix_idp_flex_positions(self) -> int:
        """Map Yahoo fantasy slot positions to actual NFL positions from super_table.

        Yahoo uses non-standard position values for flex slots:
        - 'D' = IDP flex (any defensive player) -> map to LB, DL, or DB
        - 'W/R' = offensive flex (WR or RB) -> map to WR or RB
        - 'W/T' = offensive flex (WR or TE) -> map to WR or TE

        These can't get LAMAR calculated because there's no replacement level
        for 'D', 'W/R', or 'W/T'. This resolves them to the player's actual
        NFL position using the super_table.
        """
        if not self._table_exists("player_fantasy"):
            return 0

        player_cols = self._get_table_columns("player_fantasy")

        if "player_week" not in player_cols or "position" not in player_cols:
            return 0

        player_table = self._qualified_name("player_fantasy")

        # Map 'D', 'W/R', 'W/T' positions to actual NFL position from super_table
        sql = f"""
            UPDATE {player_table} p
            SET position = s.position
            FROM ___ops.nfl_historical.nfl_player_stats_all s
            WHERE p.player_week = s.player_week
              AND {self._db_filter('p')}
              AND UPPER(TRIM(p.position)) IN ('D', 'W/R', 'W/T')
              AND s.position IS NOT NULL
              AND s.position != ''
        """

        count = self._execute(sql, "fix_idp_flex_positions: Map D/W/R/W/T to actual NFL position")
        if count and count > 0:
            logger.info(f"[fix_idp_flex_positions] Mapped {count:,} IDP flex/combo positions to actual NFL positions")

        # Fallback: default remaining 'D' positions to 'LB' (most common IDP position)
        # These are old/obscure IDP players with no matching player_week in super_table
        fallback_sql = f"""
            UPDATE {player_table}
            SET position = 'LB'
            WHERE UPPER(TRIM(position)) = 'D'
              AND {self._db_filter()}
        """
        fallback_count = self._execute(fallback_sql, "fix_idp_flex_positions: Default remaining D to LB")
        if fallback_count and fallback_count > 0:
            logger.info(f"[fix_idp_flex_positions] Defaulted {fallback_count:,} remaining 'D' positions to 'LB'")
            count = (count or 0) + fallback_count

        return count if count else 0

    def populate_keeper_economics(self) -> int:
        """Compute consecutive keeper years + base acquisition cost per (NFL_player_id, franchise_id, year).

        Reads end-of-season roster snapshots, detects keeper streaks (consecutive years
        a player was kept by the same franchise), records initial acquisition cost.
        Writes keeper_year and base_keeper_cost back to player_fantasy. No keeper rules,
        no keeper_price — projected prices are the frontend's job (see /keeper-config/apply).
        """
        if not self._table_exists("player_fantasy"):
            return 0
        cols = self._get_table_columns("player_fantasy")
        required = {
            "is_keeper",
            "franchise_id",
            "NFL_player_id",
            "year",
            "week",
            "manager",
            "cost",
            "max_faab_bid_to_date",
            "keeper_year",
            "base_keeper_cost",
        }
        missing = required - cols
        if missing:
            logger.info(f"  [keeper_economics] skipped — missing columns: {sorted(missing)}")
            return -1

        conn = self._get_connection()
        pt = self._qualified_name("player_fantasy")
        db_filter = self._db_filter()  # "db_name = 'X'" or "1=1" in local mode
        db_filter_s = self._db_filter("s")  # alias-safe: "s.db_name = 'X'" or "1=1"
        db_filter_p = self._db_filter("p")  # alias-safe: "p.db_name = 'X'" or "1=1"

        # 1. Reset (clean slate so dropped keepers go back to 0 on rerun)
        # Routed through _execute so dry_run mode is honored — bare conn.execute()
        # would zero the data even when self.dry_run=True.
        self._execute(
            f"UPDATE {pt} SET keeper_year = 0, base_keeper_cost = 0 WHERE {db_filter}",
            "populate_keeper_economics: reset keeper_year + base_keeper_cost",
        )

        # 2. Streak detection + write-back in one statement
        sql = f"""
        UPDATE {pt} AS p
        SET keeper_year = k.keeper_year,
            base_keeper_cost = k.base_keeper_cost
        FROM (
            WITH rostered_eos AS (
                SELECT NFL_player_id, franchise_id, year, MAX(week) AS last_week
                FROM {pt}
                WHERE {db_filter}
                  AND manager IS NOT NULL AND TRIM(manager) <> ''
                  AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                  AND franchise_id IS NOT NULL AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
                  AND NFL_player_id IS NOT NULL
                GROUP BY NFL_player_id, franchise_id, year
            ),
            season_snap AS (
                SELECT s.NFL_player_id,
                       s.franchise_id,
                       s.year,
                       COALESCE(s.is_keeper, 0)::INT AS is_keeper_int,
                       GREATEST(COALESCE(s.cost, 0), COALESCE(s.max_faab_bid_to_date, 0))::DOUBLE AS initial_cost
                FROM {pt} s
                JOIN rostered_eos eos
                  ON s.NFL_player_id = eos.NFL_player_id
                 AND CAST(s.franchise_id AS VARCHAR) = CAST(eos.franchise_id AS VARCHAR)
                 AND s.year = eos.year
                 AND s.week = eos.last_week
                WHERE {db_filter_s}
            ),
            with_boundaries AS (
                SELECT NFL_player_id, franchise_id, year, is_keeper_int, initial_cost,
                       LAG(year) OVER w AS prev_year,
                       LAG(is_keeper_int) OVER w AS prev_is_keeper
                FROM season_snap
                WINDOW w AS (PARTITION BY NFL_player_id, franchise_id ORDER BY year)
            ),
            with_streak AS (
                SELECT NFL_player_id, franchise_id, year, is_keeper_int, initial_cost,
                       SUM(
                           CASE WHEN is_keeper_int = 1
                                AND (prev_year IS NULL
                                     OR year - prev_year > 1
                                     OR COALESCE(prev_is_keeper, 0) = 0)
                                THEN 1 ELSE 0 END
                       ) OVER (PARTITION BY NFL_player_id, franchise_id ORDER BY year) AS streak_id
                FROM with_boundaries
            )
            SELECT NFL_player_id, franchise_id, year,
                   CASE WHEN is_keeper_int = 1
                        THEN ROW_NUMBER() OVER (PARTITION BY NFL_player_id, franchise_id, streak_id ORDER BY year)
                        ELSE 0 END AS keeper_year,
                   CASE WHEN is_keeper_int = 1
                        THEN FIRST_VALUE(initial_cost) OVER (PARTITION BY NFL_player_id, franchise_id, streak_id ORDER BY year)
                        ELSE 0.0 END AS base_keeper_cost
            FROM with_streak
        ) AS k
        WHERE p.NFL_player_id = k.NFL_player_id
          AND CAST(p.franchise_id AS VARCHAR) = CAST(k.franchise_id AS VARCHAR)
          AND p.year = k.year
          AND {db_filter_p}
        """
        self._execute(sql, "populate_keeper_economics: write keeper_year + base_keeper_cost")

        # 3. Return count of keeper rows for log readability (DuckDB UPDATE doesn't expose rowcount)
        verify = conn.execute(f"SELECT COUNT(*) FROM {pt} WHERE {db_filter} AND keeper_year > 0").fetchone()[0]
        return verify

    def populate_max_keepers(self) -> int:
        """Forward-only fill of league_settings.max_keepers from draft keeper picks.

        Replaces the dead Phase 5.6 path (run_settings_enrichment) for the one
        column with a real consumer (keeper validator gates on max_keepers > 0).
        Years where the platform settings API already populated max_keepers are
        preserved (no overwrite). Years with zero keeper picks stay NULL —
        writing 0 would lie about the league's keeper config.
        """
        if not self._table_exists("draft") or not self._table_exists("league_settings"):
            return -1

        draft_cols = self._get_table_columns("draft")
        keeper_signal_cols = [c for c in ("is_keeper", "is_keeper_status", "is_keeper_cost") if c in draft_cols]
        if not keeper_signal_cols:
            return -1

        # Build cross-platform keeper-detection. Sleeper uses is_keeper=1; Yahoo
        # uses is_keeper_status=1 OR is_keeper_cost>0. Any positive signal counts.
        keeper_signal = " OR ".join(f'COALESCE("{c}", 0) > 0' for c in keeper_signal_cols)

        conn = self._get_connection()
        draft_table = self._qualified_name("draft")
        settings_table = self._qualified_name("league_settings")
        db_filter_d = self._db_filter("d")
        db_filter_s = self._db_filter("s")
        db_filter = self._db_filter()

        sql = f"""
        UPDATE {settings_table} AS s
        SET max_keepers = c.max_keepers
        FROM (
            SELECT year, MAX(per_mgr.keepers_count)::INTEGER AS max_keepers
            FROM (
                SELECT year, manager, COUNT(*) AS keepers_count
                FROM {draft_table} AS d
                WHERE {db_filter_d}
                  AND ({keeper_signal})
                  AND manager IS NOT NULL
                  AND TRIM(manager) <> ''
                GROUP BY year, manager
            ) AS per_mgr
            GROUP BY year
        ) AS c
        WHERE s.year = c.year
          AND {db_filter_s}
          AND s.max_keepers IS NULL
        """
        self._execute(sql, "populate_max_keepers: fill from draft keeper picks (NULLs only)")

        verify = conn.execute(
            f"SELECT COUNT(*) FROM {settings_table} WHERE {db_filter} AND max_keepers IS NOT NULL"
        ).fetchone()[0]
        return verify

    # ────────────────────────────────────────────────────────────────────────
    # apply_keeper_rules — Wave 4 (overrides populate_keeper_economics defaults
    # when user-configured rules exist in keeper_config).
    # Mirrors frontend buildBaseCostSql / buildKeeperPriceSql in
    # /api/league/[db]/keeper-config/apply/route.ts.
    # ────────────────────────────────────────────────────────────────────────

    #: Rostered-player filter matching the frontend ROSTERED_FILTER constant.
    _KEEPER_ROSTERED_FILTER = (
        "manager IS NOT NULL "
        "AND TRIM(COALESCE(manager, '')) != '' "
        "AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')"
    )

    def _keeper_config_table_ref(self) -> str:
        """Prefer canonical public.keeper_config when present."""
        conn = self._get_connection()
        rows = conn.execute(
            """
            SELECT table_schema
            FROM information_schema.tables
            WHERE table_name = 'keeper_config'
            """
        ).fetchall()
        schemas = {row[0] for row in rows}
        if "public" in schemas:
            return "public.keeper_config"
        if "main" in schemas:
            return "keeper_config"

        schema_rows = conn.execute(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'public'"
        ).fetchall()
        if schema_rows:
            return "public.keeper_config"
        return "keeper_config"

    def _read_keeper_config_default(self) -> dict | None:
        """Read year=0 row from local keeper_config; returns None if missing."""
        from multi_league.core.keeper_config_schema import KEEPER_CONFIG_FIELD_COLUMNS

        col_list = ", ".join(KEEPER_CONFIG_FIELD_COLUMNS)
        table_ref = self._keeper_config_table_ref()
        db_name_literal = _sql_string_literal(self.db_name)
        try:
            row = (
                self._get_connection()
                .execute(
                    f"SELECT {col_list} FROM {table_ref} "
                    f"WHERE db_name = {db_name_literal} AND year = 0 LIMIT 1"
                )
                .fetchone()
            )
        except Exception:
            return None
        if row is None:
            return None
        return dict(zip(KEEPER_CONFIG_FIELD_COLUMNS, row))

    @staticmethod
    def _keeper_finite_int(v, default, name, *, lo=None, hi=None) -> int:
        if v is None:
            v = default
        v = int(v)
        if lo is not None and v < lo:
            raise ValueError(f"{name} must be >= {lo}")
        if hi is not None and v > hi:
            raise ValueError(f"{name} must be <= {hi}")
        return v

    @staticmethod
    def _keeper_finite_float(v, default, name, *, lo=None, hi=None) -> float:
        if v is None:
            v = default
        v = float(v)
        if lo is not None and v < lo:
            raise ValueError(f"{name} must be >= {lo}")
        if hi is not None and v > hi:
            raise ValueError(f"{name} must be <= {hi}")
        return v

    def _build_base_cost_sql(self, cfg: dict) -> str:
        """Build UPDATE SQL that writes base_keeper_cost onto player_fantasy."""
        rostered = self._KEEPER_ROSTERED_FILTER
        db_filter = f"db_name = {_sql_string_literal(self.db_name)}"
        player_table = self._qualified_name("player_fantasy")

        if cfg.get("draft_type") == "snake":
            num_rounds = self._keeper_finite_int(cfg.get("num_rounds"), 15, "num_rounds", lo=1, hi=60)
            min_round = self._keeper_finite_int(cfg.get("min_round"), 1, "min_round", lo=0, hi=60)
            if min_round == 0:
                return f"UPDATE {player_table} SET base_keeper_cost = 0 " f"WHERE {db_filter} AND {rostered}"

            offset = self._keeper_finite_int(
                cfg.get("snake_drafted_round_offset"), -1, "snake_drafted_round_offset", lo=-60, hi=60
            )
            # FA round: fixed value or last round
            if cfg.get("snake_fa_pickup_source") == "fixed" and cfg.get("snake_fa_pickup_round") is not None:
                fa_round = self._keeper_finite_int(
                    cfg.get("snake_fa_pickup_round"), num_rounds, "snake_fa_pickup_round", lo=1, hi=60
                )
            else:
                fa_round = num_rounds

            return (
                f"UPDATE {player_table} SET base_keeper_cost = "
                "CASE "
                f"  WHEN COALESCE(round, 0) > 0 "
                f"    THEN LEAST(GREATEST(COALESCE(round, 0) + ({offset}), 1), {num_rounds}) "
                f"  ELSE {fa_round} "
                "END "
                f"WHERE {db_filter} AND {rostered}"
            )

        # Auction
        d_mult = self._keeper_finite_float(cfg.get("auction_drafted_mult"), 1.0, "auction_drafted_mult", lo=0, hi=100)
        d_flat = self._keeper_finite_int(
            cfg.get("auction_drafted_flat"), 0, "auction_drafted_flat", lo=-10000, hi=10000
        )
        f_mult = self._keeper_finite_float(cfg.get("auction_faab_mult"), 1.0, "auction_faab_mult", lo=0, hi=100)
        f_flat = self._keeper_finite_int(cfg.get("auction_faab_flat"), 0, "auction_faab_flat", lo=-10000, hi=10000)
        min_p = self._keeper_finite_int(cfg.get("min_price"), 1, "min_price", lo=0, hi=10000)
        fa_val = self._keeper_finite_int(cfg.get("auction_fa_value"), min_p, "auction_fa_value", lo=0, hi=10000)

        return (
            f"UPDATE {player_table} SET base_keeper_cost = "
            "CASE "
            f"  WHEN COALESCE(cost, 0) > 0 AND COALESCE(cost, 0) >= COALESCE(max_faab_bid_to_date, 0) "
            f"    THEN GREATEST(ROUND(COALESCE(cost, 0) * {d_mult} + {d_flat}), {min_p}) "
            f"  WHEN COALESCE(max_faab_bid_to_date, 0) > 0 "
            f"    THEN GREATEST(ROUND(COALESCE(max_faab_bid_to_date, 0) * {f_mult} + {f_flat}), {min_p}) "
            f"  ELSE {fa_val} "
            "END "
            f"WHERE {db_filter} AND {rostered}"
        )

    def _build_keeper_price_sql(self, cfg: dict) -> str:
        """Build UPDATE SQL that writes keeper_price onto player_fantasy."""
        rostered = self._KEEPER_ROSTERED_FILTER
        db_filter = f"db_name = {_sql_string_literal(self.db_name)}"
        player_table = self._qualified_name("player_fantasy")

        esc_type = cfg.get("escalation_type") or "from_base"
        mult = self._keeper_finite_float(cfg.get("escalation_mult"), 1.0, "escalation_mult", lo=0, hi=100)
        flat = self._keeper_finite_int(cfg.get("escalation_flat_add"), 0, "escalation_flat_add", lo=-10000, hi=10000)
        flat_py = self._keeper_finite_int(
            cfg.get("escalation_flat_per_year"), 5, "escalation_flat_per_year", lo=-10000, hi=10000
        )
        rounds_py = self._keeper_finite_int(
            cfg.get("escalation_rounds_per_year"), 1, "escalation_rounds_per_year", lo=0, hi=60
        )

        is_snake = cfg.get("draft_type") == "snake"
        if is_snake:
            min_clamp = self._keeper_finite_int(cfg.get("min_round"), 1, "min_round", lo=0, hi=60)
            max_clamp: int | None = self._keeper_finite_int(cfg.get("num_rounds"), 15, "num_rounds", lo=1, hi=60)
        else:
            min_clamp = self._keeper_finite_int(cfg.get("min_price"), 1, "min_price", lo=0, hi=10000)
            max_clamp = (
                self._keeper_finite_int(cfg.get("max_price"), 0, "max_price", lo=0, hi=10000)
                if cfg.get("max_price") is not None
                else None
            )

        def _clamp(expr: str) -> str:
            out = f"GREATEST({expr}, {min_clamp})"
            if max_clamp is not None:
                out = f"LEAST({out}, {max_clamp})"
            return out

        # Build CASE branches: year=0 or negative → NULL, year≥1 → escalated price
        cases = ["WHEN COALESCE(keeper_year, 0) <= 0 THEN NULL"]
        cases.append(f"WHEN keeper_year = 1 THEN {_clamp('base_keeper_cost')}")

        for y in range(2, 16):
            if esc_type == "compounding":
                expr = "base_keeper_cost"
                for _ in range(1, y):
                    expr = f"ROUND({expr} * {mult} + {flat})"
            elif esc_type == "from_base":
                expr = f"base_keeper_cost + {flat_py} * {y - 1}"
            elif esc_type == "round_escalation":
                expr = f"base_keeper_cost - {rounds_py} * {y - 1}"
            else:
                # "none" or unknown: keeper_price == base_keeper_cost
                expr = "base_keeper_cost"
            cases.append(f"WHEN keeper_year = {y} THEN {_clamp(f'ROUND({expr})')}")

        cases_sql = "\n        ".join(cases)
        return (
            f"UPDATE {player_table} SET keeper_price = "
            f"CASE {cases_sql} ELSE NULL END "
            f"WHERE {db_filter} AND {rostered}"
        )

    def apply_keeper_rules(self) -> int:
        """Apply keeper config rules to player_fantasy.

        Reads year=0 row from keeper_config (the league-wide default config),
        then runs two UPDATE passes:
          1. base_keeper_cost: round-based (snake) or cost-based (auction) acquisition cost
          2. keeper_price: escalated price based on keeper_year + escalation rules

        Returns total rows touched (base_cost + price), or 0 if config is
        absent or disabled.

        Idempotent: re-running produces the same result.
        """
        cfg = self._read_keeper_config_default()
        if cfg is None:
            logger.info(f"[APPLY_KEEPER_RULES] {self.db_name}: no config found — skip")
            return 0
        if not cfg.get("enabled"):
            logger.info(f"[APPLY_KEEPER_RULES] {self.db_name}: config disabled — skip")
            return 0

        conn = self._get_connection()
        player_table = self._qualified_name("player_fantasy")

        # Execute base_cost pass
        base_sql = self._build_base_cost_sql(cfg)
        self._execute(base_sql, "apply_keeper_rules: base_cost")
        base_rows = conn.execute(
            f"SELECT COUNT(*) FROM {player_table} "
            f"WHERE db_name = {_sql_string_literal(self.db_name)} AND base_keeper_cost IS NOT NULL"
        ).fetchone()[0]

        # Execute keeper_price pass
        price_sql = self._build_keeper_price_sql(cfg)
        self._execute(price_sql, "apply_keeper_rules: keeper_price")
        price_rows = conn.execute(
            f"SELECT COUNT(*) FROM {player_table} "
            f"WHERE db_name = {_sql_string_literal(self.db_name)} AND keeper_price IS NOT NULL"
        ).fetchone()[0]

        logger.info(
            f"[APPLY_KEEPER_RULES] {self.db_name}: applied — "
            f"base_cost {base_rows} row(s), keeper_price {price_rows} row(s)"
        )
        return base_rows + price_rows

    def _sync_keeper_config_from_fly(self) -> int:
        """Materialize keeper_config from Fly into local DuckDB.

        Best-effort for table-not-found and network errors; raises loudly
        on schema mismatch (binder errors) per spec.

        Returns the number of rows synced, or 0 on best-effort skip.
        """
        from multi_league.core.runtime_mode import is_corpus_mode

        if is_corpus_mode():
            logger.info(
                "[KEEPER_CONFIG_SYNC] %s: corpus mode uses source-platform settings; skipping Fly read",
                self.db_name,
            )
            return 0

        from multi_league.core.keeper_config_schema import (
            KEEPER_CONFIG_COLUMNS,
            KEEPER_CONFIG_DDL,
        )

        conn = self._get_connection()
        table_ref = self._keeper_config_table_ref()
        db_name_literal = _sql_string_literal(self.db_name)
        conn.execute(KEEPER_CONFIG_DDL.replace("keeper_config", table_ref, 1))  # ensure local table exists

        # A weekly worker includes the complete keeper_config partition in its
        # scoped Fly snapshot.  That snapshot is authoritative even when it has
        # no row for this league, so do not repeat the same remote read.
        if self.keeper_config_hydrated:
            logger.info(
                "[KEEPER_CONFIG_SYNC] %s: using hydrated keeper_config snapshot; skipping Fly read",
                self.db_name,
            )
            return 0

        existing_cols = {row[1] for row in conn.execute(f"PRAGMA table_info('{table_ref}')").fetchall()}
        if set(KEEPER_CONFIG_COLUMNS).issubset(existing_cols):
            local_rows = conn.execute(
                f"SELECT COUNT(*) FROM {table_ref} "
                f"WHERE db_name = {db_name_literal} AND year = 0"
            ).fetchone()[0]
            if local_rows:
                logger.info(f"[KEEPER_CONFIG_SYNC] {self.db_name}: using local keeper_config row; skipping Fly read")
                return 0

        col_list = ", ".join(KEEPER_CONFIG_COLUMNS)
        try:
            reader = FlyReader()
            reader.MAX_RETRIES = _env_int("KEEPER_CONFIG_SYNC_MAX_RETRIES", 1)
            reader.TIMEOUT_SECONDS = _env_int("KEEPER_CONFIG_SYNC_TIMEOUT_SECONDS", 5)
            rows = reader.query_df(
                f"SELECT {col_list} FROM public.keeper_config "
                f"WHERE db_name = {db_name_literal}",
                database="___leagues",
            )
        except FlyReaderTableNotFound:
            logger.info(
                f"[KEEPER_CONFIG_SYNC] {self.db_name}: no keeper_config table on Fly "
                "(expected for non-keeper leagues)"
            )
            return 0
        except FlyReaderNetworkError as e:
            logger.info(f"[KEEPER_CONFIG_SYNC] {self.db_name}: Fly read failed (skip): {e}")
            return 0
        except FlyReaderError as e:
            msg = str(e)
            if "Binder Error" in msg or "Referenced column" in msg:
                raise
            logger.info(f"[KEEPER_CONFIG_SYNC] {self.db_name}: Fly read failed (skip): {e}")
            return 0
        # Other errors (binder / schema mismatch) propagate loudly.

        if rows.empty:
            logger.info(f"[KEEPER_CONFIG_SYNC] {self.db_name}: keeper_config exists but empty")
            return 0

        conn.execute(f"DELETE FROM {table_ref} WHERE db_name = {db_name_literal}")
        placeholders = ", ".join(["?"] * len(KEEPER_CONFIG_COLUMNS))
        conn.executemany(
            f"INSERT INTO {table_ref} ({col_list}) VALUES ({placeholders})",
            rows[KEEPER_CONFIG_COLUMNS].itertuples(index=False, name=None),
        )
        logger.info(f"[KEEPER_CONFIG_SYNC] {self.db_name}: synced {len(rows)} row(s) from Fly")
        return len(rows)
