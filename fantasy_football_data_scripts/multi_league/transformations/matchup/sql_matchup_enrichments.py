"""
Matchup Enrichments Mixin

Franchise management and matchup<->player cross-join methods.
"""

import logging
import os

import pandas as pd

from multi_league.core.franchise_identity_schema import (
    FRANCHISE_IDENTITY_AUDIT_COLUMNS,
    FRANCHISE_IDENTITY_AUDIT_DDL,
    FRANCHISE_IDENTITY_REGISTRY_COLUMNS,
    FRANCHISE_IDENTITY_REGISTRY_DDL,
)
from multi_league.core.identity import get_manager_col
from multi_league.core.readers.fly_reader import (
    FlyReader,
    FlyReaderError,
    FlyReaderNetworkError,
    FlyReaderTableNotFound,
)
from multi_league.core.player_week_identity import matchup_publish_dedup_statements
from multi_league.shared.filters import rostered_filter_sql  # noqa: F401 - used in f-strings
from multi_league.transformations.matchup.modules.playoff_helpers import (
    POINTS_FIRST_SEEDING,
    normalize_playoff_seeding_rule,
)

logger = logging.getLogger(__name__)


class MatchupEnrichmentsMixin:
    """Mixin providing matchup-related SQL enrichments.

    Requires SQLEnrichmentsBase infrastructure (self._execute, self._table_exists, etc.)
    """

    @staticmethod
    def _truthy_setting(value) -> bool:
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "t", "yes", "y"}
        try:
            if pd.isna(value):
                return False
        except (TypeError, ValueError):
            pass
        return bool(value)

    @classmethod
    def _resolve_playoff_round_type(cls, settings_row: dict, playoff_teams: int | None = None) -> int:
        """Return canonical round type: 0=single, 1=all two-week, 2=championship-only two-week."""
        prt_raw = settings_row.get("playoff_round_type")
        if prt_raw is None:
            prt_raw = settings_row.get("sleeper_playoff_type")
        try:
            prt = int(prt_raw) if prt_raw is not None and not pd.isna(prt_raw) else 0
        except (TypeError, ValueError):
            prt = 0

        has_multiweek = cls._truthy_setting(settings_row.get("has_multiweek_championship")) or prt in (1, 2)
        if not has_multiweek:
            return 0

        try:
            pt = int(playoff_teams if playoff_teams is not None else settings_row.get("playoff_teams"))
            if pt <= 1:
                return 0
        except (TypeError, ValueError):
            pt = 0

        try:
            import math

            round_count = int(math.ceil(math.log2(max(pt, 2)))) if pt else 0
            playoff_start = int(settings_row.get("playoff_start_week"))
            end_week = int(settings_row.get("end_week"))
            playoff_weeks = end_week - playoff_start + 1
        except (TypeError, ValueError):
            round_count = 0
            playoff_weeks = 0

        # Prefer the observed season span when available. This keeps canonical
        # all-round vs championship-only behavior stable even when a platform's
        # raw enum differs from our internal enum.
        if round_count > 0 and playoff_weeks > 0:
            if playoff_weeks >= 2 * round_count:
                return 1
            if playoff_weeks >= round_count + 1:
                return 2

        if prt in (1, 2):
            return prt
        return 2

    def dedup_matchup_publish_identity(self) -> int:
        """Collapse duplicate matchup manager_week rows before analytics/upload."""
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        if self._get_connection().execute(
            f"SELECT 1 FROM {matchup_table} WHERE {self._db_filter()} LIMIT 1"
        ).fetchone() is None:
            return 0

        matchup_cols = self._get_table_columns("matchup")
        if "manager_week" not in matchup_cols:
            return 0

        statements = matchup_publish_dedup_statements(
            matchup_table,
            matchup_cols,
            db_filter=self._db_filter("m"),
        )
        if not statements:
            return 0

        conn = self._get_connection()
        temp_table = "_matchup_publish_dedup"
        try:
            self._execute(statements[0][1], "dedup_matchup_publish_identity: stage duplicate manager_week rows")
            duplicate_count = int(conn.execute(f'SELECT COUNT(*) FROM "{temp_table}"').fetchone()[0] or 0)
            if duplicate_count:
                self._execute(
                    statements[1][1],
                    f"dedup_matchup_publish_identity: delete {duplicate_count:,} duplicate manager_week rows",
                )
                logger.info(f"[dedup_matchup_publish_identity] removed {duplicate_count:,} duplicate manager_week rows")
            return duplicate_count
        finally:
            try:
                conn.execute(statements[2][1])
            except Exception:
                pass

    def _disambiguate_shared_franchise_ids(self, matchup_t: str) -> int:
        """Assign synthetic franchise_ids to ALL hidden managers, then rebuild opponent_franchise_id.

        Yahoo returns franchise_id='--' for hidden managers. This step:
        1. Assigns 'hidden_{team_name_slug}' to every row with franchise_id in ('--','','None')
        2. Falls back to team_name only when the manager label itself is hidden/blank
        3. Rebuilds opponent_franchise_id using a score-based self-join (not name matching,
           which fails when two managers share the same display name)
        """
        conn = self._get_connection()

        # Step 1: Assign synthetic franchise_ids to ALL hidden managers
        hidden_teams = conn.execute(f"""
            SELECT DISTINCT team_name
            FROM {matchup_t}
            WHERE {self._db_filter()}
              AND (franchise_id IS NULL OR franchise_id IN ('--', '', 'None'))
              AND team_name IS NOT NULL AND TRIM(team_name) != ''
        """).fetchall()

        if not hidden_teams:
            return 0

        total = 0
        matchup_cols = self._get_table_columns("matchup")
        if "opponent_franchise_id" in matchup_cols:
            no_opponent_predicate = (
                "m.opponent_franchise_id IS NULL OR TRIM(COALESCE(m.opponent_franchise_id, '')) = ''"
            )
            has_opponent_predicate = (
                "m.opponent_franchise_id IS NOT NULL AND TRIM(COALESCE(m.opponent_franchise_id, '')) <> ''"
            )
        else:
            no_opponent_predicate = "m.opponent IS NULL OR TRIM(COALESCE(m.opponent, '')) = ''"
            has_opponent_predicate = "m.opponent IS NOT NULL AND TRIM(COALESCE(m.opponent, '')) <> ''"
        for (team_name,) in hidden_teams:
            slug = team_name.lower().replace(" ", "_").replace("'", "")[:30]
            synthetic_fid = f"hidden_{slug}"

            conn.execute(f"""
                UPDATE {matchup_t}
                SET franchise_id = '{synthetic_fid}',
                    manager = CASE
                        WHEN manager IS NULL
                          OR TRIM(COALESCE(CAST(manager AS VARCHAR), '')) = ''
                          OR LOWER(TRIM(CAST(manager AS VARCHAR))) IN (
                              '--', '--hidden--', 'hidden', 'unknown', 'none', 'nan', '<na>', 'n/a', 'null'
                          )
                        THEN '{team_name.replace("'", "''")}'
                        ELSE manager
                    END
                WHERE {self._db_filter()}
                  AND (franchise_id IS NULL OR franchise_id IN ('--', '', 'None'))
                  AND team_name = '{team_name.replace("'", "''")}'
            """)
            total += 1
            logger.info(f"[disambiguate] hidden team '{team_name}' -> '{synthetic_fid}'")

        # Step 2: Rebuild opponent_franchise_id using score-based self-join.
        # For each row, find the row in the same (year, week) whose team_points
        # matches our opponent_points (and vice versa) — that's our actual opponent.
        if self._column_exists(matchup_t, "opponent_franchise_id"):
            conn.execute(f"""
                UPDATE {matchup_t} m
                SET opponent_franchise_id = opp.franchise_id
                FROM {matchup_t} opp
                WHERE m.year = opp.year AND m.week = opp.week
                  AND {self._db_filter("m")}
                  AND {self._db_filter("opp")}
                  AND m.franchise_id != opp.franchise_id
                  AND ROUND(CAST(m.team_points AS DOUBLE), 2) = ROUND(CAST(opp.opponent_points AS DOUBLE), 2)
                  AND ROUND(CAST(m.opponent_points AS DOUBLE), 2) = ROUND(CAST(opp.team_points AS DOUBLE), 2)
                  AND m.team_points IS NOT NULL AND m.team_points != 0
                  AND (m.opponent_franchise_id IS NULL
                       OR m.opponent_franchise_id IN ('--', '', 'None')
                       OR m.opponent_franchise_id NOT IN (
                           SELECT DISTINCT franchise_id
                           FROM {matchup_t}
                           WHERE {self._db_filter()}
                             AND franchise_id IS NOT NULL
                       ))
            """)
            logger.info("[disambiguate] Rebuilt opponent_franchise_id via score matching")

        return total

    @staticmethod
    def _sql_literal(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        try:
            return int(os.environ.get(name, str(default)))
        except (TypeError, ValueError):
            return default

    def _ensure_franchise_identity_tables(self) -> None:
        """Create local identity continuity tables if they do not exist."""
        conn = self._get_connection()
        registry_ref = self._qualified_name("franchise_identity_registry")
        audit_ref = self._qualified_name("franchise_identity_audit")
        conn.execute(FRANCHISE_IDENTITY_REGISTRY_DDL.replace("franchise_identity_registry", registry_ref, 1))
        conn.execute(FRANCHISE_IDENTITY_AUDIT_DDL.replace("franchise_identity_audit", audit_ref, 1))
        self._table_cache["franchise_identity_registry"] = True
        self._table_cache["franchise_identity_audit"] = True
        self._invalidate_column_cache("franchise_identity_registry")
        self._invalidate_column_cache("franchise_identity_audit")

    def _sync_franchise_identity_registry_from_fly(self) -> int:
        """Materialize prior branch assignments from Fly into the local import DB.

        This makes fresh Yahoo reimports idempotent: if a GUID has already been
        split into `{guid}_N` branches, the next import reuses those resolved
        ids before assigning any new branch indexes.
        """
        self._ensure_franchise_identity_tables()

        conn = self._get_connection()
        registry_ref = self._qualified_name("franchise_identity_registry")
        db_lit = self._sql_literal(self.db_name)

        local_rows = int(
            conn.execute(f"SELECT COUNT(*) FROM {registry_ref} WHERE db_name = {db_lit}").fetchone()[0] or 0
        )
        if local_rows:
            logger.info(
                "[FRANCHISE_IDENTITY_SYNC] %s: using %s local registry row(s); skipping Fly read",
                self.db_name,
                local_rows,
            )
            return 0

        if not os.environ.get("DATABASE_SERVER_URL") or not os.environ.get("DATABASE_READ_TOKEN"):
            logger.info(
                "[FRANCHISE_IDENTITY_SYNC] %s: DATABASE_SERVER_URL/DATABASE_READ_TOKEN not set; skipping Fly read",
                self.db_name,
            )
            return 0

        col_list = ", ".join(FRANCHISE_IDENTITY_REGISTRY_COLUMNS)
        try:
            reader = FlyReader()
            reader.MAX_RETRIES = self._env_int("FRANCHISE_IDENTITY_SYNC_MAX_RETRIES", 2)
            reader.TIMEOUT_SECONDS = self._env_int("FRANCHISE_IDENTITY_SYNC_TIMEOUT_SECONDS", 10)
            rows = reader.query_df(
                f"""
                SELECT {col_list}
                FROM public.franchise_identity_registry
                WHERE db_name = {db_lit}
                """,
                database="___leagues",
            )
        except FlyReaderTableNotFound:
            logger.info(
                "[FRANCHISE_IDENTITY_SYNC] %s: no franchise_identity_registry table on Fly yet",
                self.db_name,
            )
            return 0
        except FlyReaderNetworkError as e:
            logger.info("[FRANCHISE_IDENTITY_SYNC] %s: Fly read failed (skip): %s", self.db_name, e)
            return 0
        except FlyReaderError as e:
            msg = str(e)
            if "Binder Error" in msg or "Referenced column" in msg:
                raise
            logger.info("[FRANCHISE_IDENTITY_SYNC] %s: Fly read failed (skip): %s", self.db_name, e)
            return 0
        except RuntimeError as e:
            msg = str(e)
            if "Binder Error" in msg or "Referenced column" in msg:
                raise
            logger.info("[FRANCHISE_IDENTITY_SYNC] %s: Fly read failed (skip): %s", self.db_name, e)
            return 0

        if rows.empty:
            logger.info("[FRANCHISE_IDENTITY_SYNC] %s: no prior registry rows", self.db_name)
            return 0

        rows = rows.reindex(columns=FRANCHISE_IDENTITY_REGISTRY_COLUMNS)
        placeholders = ", ".join(["?"] * len(FRANCHISE_IDENTITY_REGISTRY_COLUMNS))
        conn.executemany(
            f"INSERT INTO {registry_ref} ({col_list}) VALUES ({placeholders})",
            rows[FRANCHISE_IDENTITY_REGISTRY_COLUMNS].itertuples(index=False, name=None),
        )
        logger.info(
            "[FRANCHISE_IDENTITY_SYNC] %s: synced %s registry row(s) from Fly",
            self.db_name,
            len(rows),
        )
        return len(rows)

    def _disambiguate_simultaneous_multi_team_franchise_ids(self, matchup_t: str) -> int:
        """Split one real owner GUID into per-team franchise_ids when needed.

        Yahoo can return the same manager GUID for multiple teams owned by the
        same account. A single franchise_id is correct for a renamed team, but
        wrong when distinct teams appear in the same league-week. In that case,
        derive stable `{guid}_{team_index}` ids from team_key when available,
        otherwise from team_name.
        """
        matchup_cols = self._get_table_columns("matchup")
        required_cols = {"franchise_id", "team_name", "year", "week"}
        if not required_cols.issubset(matchup_cols):
            return 0

        self._sync_franchise_identity_registry_from_fly()

        has_team_key = "team_key" in matchup_cols
        has_manager = "manager" in matchup_cols
        has_manager_guid = "manager_guid" in matchup_cols
        has_platform = "platform" in matchup_cols

        conn = self._get_connection()
        registry_t = self._qualified_name("franchise_identity_registry")
        audit_t = self._qualified_name("franchise_identity_audit")
        db_lit = self._sql_literal(self.db_name)

        def team_slot_expr(alias: str) -> str:
            if not has_team_key:
                return "CAST(NULL AS VARCHAR)"
            raw_team_key = f"TRIM(CAST({alias}.team_key AS VARCHAR))"
            extracted = f"REGEXP_EXTRACT({raw_team_key}, '\\.t\\.([0-9]+)$', 1)"
            return (
                "CASE "
                f"WHEN NULLIF({extracted}, '') IS NOT NULL THEN {extracted} "
                f"WHEN REGEXP_MATCHES({raw_team_key}, '^[0-9]+$') THEN {raw_team_key} "
                "ELSE CAST(NULL AS VARCHAR) "
                "END"
            )

        def identity_expr(alias: str) -> str:
            slot = team_slot_expr(alias)
            if has_team_key:
                return (
                    "CASE "
                    f"WHEN NULLIF({slot}, '') IS NOT NULL "
                    f"THEN 'team_slot:' || {slot} "
                    f"WHEN NULLIF(TRIM(CAST({alias}.team_key AS VARCHAR)), '') IS NOT NULL "
                    f" AND TRIM(CAST({alias}.team_key AS VARCHAR)) != TRIM(CAST({alias}.franchise_id AS VARCHAR)) "
                    f"THEN 'team_key:' || TRIM(CAST({alias}.team_key AS VARCHAR)) "
                    f"ELSE 'team_name:' || LOWER(TRIM(CAST({alias}.team_name AS VARCHAR))) "
                    "END"
                )
            return f"'team_name:' || LOWER(TRIM(CAST({alias}.team_name AS VARCHAR)))"

        def norm_expr(alias: str, column: str) -> str:
            return (
                f"LOWER(REGEXP_REPLACE(TRIM(COALESCE(CAST({alias}.{column} AS VARCHAR), '')), "
                "'[^A-Za-z0-9]+', '', 'g'))"
            )

        def base_franchise_expr(alias: str) -> str:
            fid_norm = f"LOWER(TRIM(COALESCE(CAST({alias}.franchise_id AS VARCHAR), '')))"
            if not has_manager_guid:
                return f"CAST({alias}.franchise_id AS VARCHAR)"

            guid_norm = f"LOWER(TRIM(COALESCE(CAST({alias}.manager_guid AS VARCHAR), '')))"
            hidden_guid = f"{guid_norm} IN ('', '--', '--hidden--', 'hidden', 'none', 'nan', '<na>', 'n/a', 'null')"
            yahoo_checks = []
            if has_platform:
                yahoo_checks.append(f"LOWER(TRIM(COALESCE(CAST({alias}.platform AS VARCHAR), ''))) = 'yahoo'")
            if has_team_key:
                yahoo_checks.append(
                    f"REGEXP_MATCHES(TRIM(COALESCE(CAST({alias}.team_key AS VARCHAR), '')), "
                    "'^[0-9]+\\.l\\.[0-9]+\\.t\\.[0-9]+$')"
                )
            if not yahoo_checks:
                return f"CAST({alias}.franchise_id AS VARCHAR)"

            yahoo_row = "(" + " OR ".join(yahoo_checks) + ")"
            synthetic_or_invalid_fid = (
                f"({fid_norm} IN ('', '--', '--hidden--', 'none', 'nan', '<na>', 'n/a', 'null') "
                f"OR LEFT({fid_norm}, 7) = 'hidden_')"
            )
            return (
                "CASE "
                f"WHEN {yahoo_row} AND {hidden_guid} AND {synthetic_or_invalid_fid} "
                "THEN '--hidden--' "
                f"ELSE CAST({alias}.franchise_id AS VARCHAR) "
                "END"
            )

        manager_select = "m.manager" if has_manager else "CAST(NULL AS VARCHAR)"
        manager_guid_select = "m.manager_guid" if has_manager_guid else "CAST(NULL AS VARCHAR)"
        platform_select = "m.platform" if has_platform else "CAST(NULL AS VARCHAR)"
        team_key_select = "m.team_key" if has_team_key else "CAST(NULL AS VARCHAR)"
        manager_match = "AND m.manager IS NOT DISTINCT FROM c.manager" if has_manager else ""
        team_key_match = "AND m.team_key IS NOT DISTINCT FROM c.team_key" if has_team_key else ""
        team_slot_order_expr = "TRY_CAST(NULLIF(team_slot, '') AS INTEGER)"

        conn.execute("DROP TABLE IF EXISTS _franchise_branch_assignments")
        stage_sql = f"""
            CREATE OR REPLACE TEMP TABLE _franchise_branch_assignments AS
            WITH simultaneous_weeks AS (
                SELECT franchise_id, year, week
                FROM {matchup_t}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL
                  AND LOWER(TRIM(CAST(franchise_id AS VARCHAR))) NOT IN ('', '--', '--hidden--', 'none', 'nan', '<na>')
                  AND team_name IS NOT NULL
                  AND TRIM(CAST(team_name AS VARCHAR)) != ''
                GROUP BY franchise_id, year, week
                HAVING COUNT(DISTINCT TRIM(CAST(team_name AS VARCHAR))) > 1
            ),
            simultaneous_base AS (
                SELECT DISTINCT franchise_id
                FROM simultaneous_weeks
            ),
            registry_all AS (
                SELECT
                    base_franchise_id,
                    identity_key,
                    branch_key,
                    resolved_franchise_id,
                    team_index,
                    manager_guid,
                    platform,
                    anchor_year,
                    anchor_week,
                    anchor_team_name,
                    anchor_manager,
                    anchor_team_key,
                    anchor_team_slot
                FROM {registry_t}
                WHERE db_name = {db_lit}
            ),
            split_base AS (
                SELECT franchise_id AS base_franchise_id
                FROM simultaneous_base
                UNION
                SELECT base_franchise_id
                FROM registry_all
            ),
            anchor_rows AS (
                SELECT
                    {base_franchise_expr("m")} AS base_franchise_id,
                    {identity_expr("m")} AS identity_key,
                    {team_slot_expr("m")} AS team_slot,
                    {team_key_select} AS team_key,
                    m.team_name,
                    {manager_select} AS manager,
                    {manager_guid_select} AS manager_guid,
                    {platform_select} AS platform,
                    m.year,
                    m.week
                FROM {matchup_t} m
                LEFT JOIN simultaneous_weeks sw
                  ON sw.franchise_id = m.franchise_id
                 AND sw.year = m.year
                 AND sw.week = m.week
                WHERE {self._db_filter("m")}
                  AND (
                      sw.franchise_id IS NOT NULL
                      OR {base_franchise_expr("m")} IN (SELECT base_franchise_id FROM registry_all)
                  )
                  AND m.team_name IS NOT NULL
                  AND TRIM(CAST(m.team_name AS VARCHAR)) != ''
            ),
            anchor_identities AS (
                SELECT
                    base_franchise_id,
                    identity_key,
                    COALESCE(NULLIF(MIN(TRIM(CAST(manager_guid AS VARCHAR))), ''), base_franchise_id) AS manager_guid,
                    COALESCE(NULLIF(MIN(TRIM(CAST(platform AS VARCHAR))), ''), 'unknown') AS platform,
                    MIN(TRIM(CAST(team_name AS VARCHAR))) AS stable_team_name,
                    MIN(TRIM(CAST(manager AS VARCHAR))) AS stable_manager,
                    MIN(TRIM(CAST(team_key AS VARCHAR))) AS stable_team_key,
                    MIN(CAST(year AS INTEGER)) AS first_year,
                    MIN(CAST(week AS INTEGER)) AS first_week,
                    MIN({team_slot_order_expr}) AS team_slot_order,
                    MIN(NULLIF(team_slot, '')) AS team_slot
                FROM anchor_rows ar
                GROUP BY base_franchise_id, identity_key
            ),
            registry_identities AS (
                SELECT
                    base_franchise_id,
                    identity_key,
                    COALESCE(NULLIF(TRIM(CAST(manager_guid AS VARCHAR)), ''), base_franchise_id) AS manager_guid,
                    COALESCE(NULLIF(TRIM(CAST(platform AS VARCHAR)), ''), 'unknown') AS platform,
                    anchor_team_name AS stable_team_name,
                    anchor_manager AS stable_manager,
                    anchor_team_key AS stable_team_key,
                    anchor_year AS first_year,
                    anchor_week AS first_week,
                    TRY_CAST(NULLIF(anchor_team_slot, '') AS INTEGER) AS team_slot_order,
                    anchor_team_slot AS team_slot
                FROM registry_all
                WHERE base_franchise_id IN (SELECT base_franchise_id FROM split_base)
            ),
            team_identities AS (
                SELECT *
                FROM anchor_identities
                UNION ALL
                SELECT ri.*
                FROM registry_identities ri
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM anchor_identities ai
                    WHERE ai.base_franchise_id = ri.base_franchise_id
                      AND ai.identity_key = ri.identity_key
                )
            ),
            keyed_branch_year_names AS (
                SELECT DISTINCT
                    base_franchise_id,
                    identity_key,
                    year,
                    {norm_expr("ar", "team_name")} AS team_norm
                FROM anchor_rows ar
                WHERE identity_key NOT LIKE 'team_name:%'
                  AND {norm_expr("ar", "team_name")} != ''
            ),
            registry AS (
                SELECT *
                FROM registry_all
                WHERE base_franchise_id IN (SELECT base_franchise_id FROM split_base)
            ),
            existing_index AS (
                SELECT
                    ti.*,
                    r.branch_key AS existing_branch_key,
                    r.resolved_franchise_id AS existing_resolved_franchise_id,
                    r.team_index AS existing_team_index
                FROM team_identities ti
                LEFT JOIN registry r
                  ON r.base_franchise_id = ti.base_franchise_id
                 AND r.identity_key = ti.identity_key
            ),
            max_existing AS (
                SELECT base_franchise_id, COALESCE(MAX(team_index), 0) AS max_team_index
                FROM registry
                GROUP BY base_franchise_id
            ),
            unmatched AS (
                SELECT
                    base_franchise_id,
                    identity_key,
                    ROW_NUMBER() OVER (
                        PARTITION BY base_franchise_id
                        ORDER BY
                            first_year,
                            first_week,
                            COALESCE(team_slot_order, 2147483647),
                            stable_team_name,
                            identity_key
                    ) AS new_ordinal
                FROM existing_index
                WHERE existing_team_index IS NULL
            ),
            indexed_branches AS (
                SELECT
                    ei.base_franchise_id,
                    ei.identity_key,
                    COALESCE(ei.existing_branch_key, ei.identity_key) AS branch_key,
                    COALESCE(
                        ei.existing_team_index,
                        COALESCE(me.max_team_index, 0) + COALESCE(u.new_ordinal, 0)
                    ) AS team_index,
                    COALESCE(
                        ei.existing_resolved_franchise_id,
                        ei.base_franchise_id || '_' || CAST(
                            COALESCE(me.max_team_index, 0) + COALESCE(u.new_ordinal, 0)
                            AS VARCHAR
                        )
                    ) AS resolved_franchise_id,
                    ei.manager_guid,
                    ei.platform,
                    team_slot,
                    stable_team_name AS anchor_team_name,
                    stable_manager AS anchor_manager,
                    stable_team_key AS anchor_team_key,
                    team_slot AS anchor_team_slot,
                    first_year AS anchor_year,
                    first_week AS anchor_week
                FROM existing_index ei
                LEFT JOIN max_existing me
                  ON me.base_franchise_id = ei.base_franchise_id
                LEFT JOIN unmatched u
                  ON u.base_franchise_id = ei.base_franchise_id
                 AND u.identity_key = ei.identity_key
            ),
            branch_names AS (
                SELECT DISTINCT
                    base_franchise_id,
                    identity_key,
                    {norm_expr("ti", "stable_team_name")} AS team_norm,
                    {norm_expr("ti", "stable_manager") if has_manager else "''"} AS manager_norm
                FROM team_identities ti
            ),
            target_rows AS (
                SELECT
                    {base_franchise_expr("m")} AS base_franchise_id,
                    {identity_expr("m")} AS target_identity_key,
                    m.year,
                    m.week,
                    m.team_name,
                    {manager_select} AS manager,
                    {manager_guid_select} AS manager_guid,
                    {platform_select} AS platform,
                    {team_key_select} AS team_key,
                    {team_slot_expr("m")} AS team_slot,
                    {norm_expr("m", "team_name")} AS team_norm,
                    {norm_expr("m", "manager") if has_manager else "''"} AS manager_norm
                FROM {matchup_t} m
                WHERE {self._db_filter("m")}
                  AND {base_franchise_expr("m")} IN (SELECT base_franchise_id FROM split_base)
                  AND m.team_name IS NOT NULL
                  AND TRIM(CAST(m.team_name AS VARCHAR)) != ''
            ),
            scored AS (
                SELECT
                    tr.base_franchise_id,
                    tr.year,
                    tr.week,
                    tr.team_name,
                    tr.manager,
                    tr.manager_guid,
                    tr.platform,
                    tr.team_key,
                    tr.team_slot,
                    tr.target_identity_key,
                    b.identity_key AS branch_identity_key,
                    b.branch_key,
                    b.resolved_franchise_id,
                    b.team_index,
                    b.manager_guid AS branch_manager_guid,
                    b.platform AS branch_platform,
                    b.anchor_year,
                    b.anchor_week,
                    b.anchor_team_name,
                    b.anchor_manager,
                    b.anchor_team_key,
                    b.anchor_team_slot,
                    CASE
                        WHEN tr.target_identity_key = b.identity_key
                         AND tr.target_identity_key NOT LIKE 'team_slot:%'
                        THEN 1000
                        ELSE 0
                    END AS identity_score,
                    MAX(
                        CASE
                            WHEN tr.team_norm != '' AND bn.team_norm != '' AND tr.team_norm = bn.team_norm THEN 100
                            WHEN tr.team_norm != '' AND bn.team_norm != ''
                              AND (CONTAINS(tr.team_norm, bn.team_norm) OR CONTAINS(bn.team_norm, tr.team_norm))
                            THEN 70
                            ELSE 0
                        END
                        +
                        CASE
                            WHEN tr.manager_norm != '' AND bn.manager_norm != '' AND tr.manager_norm = bn.manager_norm THEN 40
                            WHEN tr.manager_norm != '' AND bn.manager_norm != ''
                              AND (CONTAINS(tr.manager_norm, bn.manager_norm) OR CONTAINS(bn.manager_norm, tr.manager_norm))
                            THEN 20
                            ELSE 0
                        END
                        +
                        CASE
                            WHEN kbyn.identity_key IS NOT NULL THEN 2000
                            ELSE 0
                        END
                    ) AS name_score,
                    CASE
                        WHEN tr.team_slot IS NOT NULL
                         AND tr.team_slot != ''
                         AND b.team_slot IS NOT NULL
                         AND tr.team_slot = b.team_slot
                        THEN 30
                        ELSE 0
                    END AS slot_score
                FROM target_rows tr
                JOIN indexed_branches b
                  ON b.base_franchise_id = tr.base_franchise_id
                LEFT JOIN branch_names bn
                  ON bn.base_franchise_id = b.base_franchise_id
                 AND bn.identity_key = b.identity_key
                LEFT JOIN keyed_branch_year_names kbyn
                  ON kbyn.base_franchise_id = b.base_franchise_id
                 AND kbyn.identity_key = b.identity_key
                 AND kbyn.year = tr.year
                 AND kbyn.team_norm = tr.team_norm
                GROUP BY
                    tr.base_franchise_id,
                    tr.year,
                    tr.week,
                    tr.team_name,
                    tr.manager,
                    tr.manager_guid,
                    tr.platform,
                    tr.team_key,
                    tr.team_slot,
                    tr.target_identity_key,
                    b.identity_key,
                    b.branch_key,
                    b.resolved_franchise_id,
                    b.team_index,
                    b.manager_guid,
                    b.platform,
                    b.anchor_year,
                    b.anchor_week,
                    b.anchor_team_name,
                    b.anchor_manager,
                    b.anchor_team_key,
                    b.anchor_team_slot,
                    b.team_slot
            ),
            chosen AS (
                SELECT *
                FROM (
                    SELECT
                        scored.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY base_franchise_id, year, week, team_name, manager, team_key
                            ORDER BY (COALESCE(identity_score, 0) + COALESCE(name_score, 0) + COALESCE(slot_score, 0)) DESC,
                                     COALESCE(identity_score, 0) DESC,
                                     COALESCE(name_score, 0) DESC,
                                     COALESCE(slot_score, 0) DESC,
                                     team_index
                        ) AS rn
                    FROM scored
                ) ranked
                WHERE rn = 1
            )
            SELECT
                {db_lit} AS db_name,
                COALESCE(NULLIF(platform, ''), NULLIF(branch_platform, ''), 'unknown') AS platform,
                year,
                week,
                COALESCE(NULLIF(manager_guid, ''), NULLIF(branch_manager_guid, ''), base_franchise_id) AS manager_guid,
                base_franchise_id,
                resolved_franchise_id,
                sha256(concat_ws(chr(31),
                    {db_lit},
                    COALESCE(CAST(year AS VARCHAR), '<NULL>'),
                    COALESCE(CAST(week AS VARCHAR), '<NULL>'),
                    COALESCE(CAST(team_key AS VARCHAR), '<NULL>'),
                    COALESCE(CAST(team_name AS VARCHAR), '<NULL>'),
                    COALESCE(CAST(manager AS VARCHAR), '<NULL>'),
                    COALESCE(resolved_franchise_id, '<NULL>')
                )) AS assignment_key,
                target_identity_key AS identity_key,
                branch_identity_key,
                branch_key,
                team_index,
                team_key,
                team_slot,
                team_name,
                manager,
                CASE
                    WHEN COALESCE(identity_score, 0) > 0 THEN 'identity_key'
                    WHEN COALESCE(name_score, 0) > 0 THEN 'name'
                    WHEN COALESCE(slot_score, 0) > 0 THEN 'slot'
                    ELSE 'fallback_index'
                END AS reason,
                CAST(COALESCE(identity_score, 0) AS INTEGER) AS identity_score,
                CAST(COALESCE(name_score, 0) AS INTEGER) AS name_score,
                CAST(COALESCE(slot_score, 0) AS INTEGER) AS slot_score,
                CAST(COALESCE(identity_score, 0) + COALESCE(name_score, 0) + COALESCE(slot_score, 0) AS INTEGER)
                    AS total_score,
                CURRENT_TIMESTAMP AS created_at,
                anchor_year,
                anchor_week,
                anchor_team_name,
                anchor_manager,
                anchor_team_key,
                anchor_team_slot
            FROM chosen
        """
        conn.execute(stage_sql)

        assignment_count = int(conn.execute("SELECT COUNT(*) FROM _franchise_branch_assignments").fetchone()[0] or 0)
        if assignment_count == 0:
            return 0

        registry_cols = ", ".join(FRANCHISE_IDENTITY_REGISTRY_COLUMNS)
        audit_cols = ", ".join(FRANCHISE_IDENTITY_AUDIT_COLUMNS)

        conn.execute(
            """
            CREATE OR REPLACE TEMP TABLE _franchise_identity_registry_stage AS
            SELECT
                db_name,
                COALESCE(NULLIF(MIN(platform), ''), 'unknown') AS platform,
                base_franchise_id,
                COALESCE(NULLIF(MIN(manager_guid), ''), base_franchise_id) AS manager_guid,
                branch_identity_key AS identity_key,
                branch_key,
                resolved_franchise_id,
                team_index,
                MIN(anchor_year) AS anchor_year,
                MIN(anchor_week) AS anchor_week,
                MIN(anchor_team_name) AS anchor_team_name,
                MIN(anchor_manager) AS anchor_manager,
                MIN(anchor_team_key) AS anchor_team_key,
                MIN(anchor_team_slot) AS anchor_team_slot,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(team_name AS VARCHAR)), ''), ' | ' ORDER BY NULLIF(TRIM(CAST(team_name AS VARCHAR)), ''))
                    AS known_team_names,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(manager AS VARCHAR)), ''), ' | ' ORDER BY NULLIF(TRIM(CAST(manager AS VARCHAR)), ''))
                    AS known_manager_names,
                STRING_AGG(DISTINCT NULLIF(TRIM(CAST(team_slot AS VARCHAR)), ''), ' | ' ORDER BY NULLIF(TRIM(CAST(team_slot AS VARCHAR)), ''))
                    AS known_team_slots,
                STRING_AGG(DISTINCT CAST(year AS VARCHAR), ',' ORDER BY CAST(year AS VARCHAR)) AS active_years,
                CURRENT_TIMESTAMP AS created_at,
                CURRENT_TIMESTAMP AS updated_at
            FROM _franchise_branch_assignments
            GROUP BY
                db_name,
                base_franchise_id,
                branch_identity_key,
                branch_key,
                resolved_franchise_id,
                team_index
            """
        )

        conn.execute(
            f"""
            UPDATE {registry_t} r
            SET
                platform = s.platform,
                base_franchise_id = s.base_franchise_id,
                manager_guid = s.manager_guid,
                identity_key = s.identity_key,
                branch_key = s.branch_key,
                team_index = s.team_index,
                anchor_year = s.anchor_year,
                anchor_week = s.anchor_week,
                anchor_team_name = s.anchor_team_name,
                anchor_manager = s.anchor_manager,
                anchor_team_key = s.anchor_team_key,
                anchor_team_slot = s.anchor_team_slot,
                known_team_names = s.known_team_names,
                known_manager_names = s.known_manager_names,
                known_team_slots = s.known_team_slots,
                active_years = s.active_years,
                updated_at = CURRENT_TIMESTAMP
            FROM _franchise_identity_registry_stage s
            WHERE r.db_name = s.db_name
              AND r.resolved_franchise_id = s.resolved_franchise_id
            """
        )

        conn.execute(
            f"""
            INSERT INTO {registry_t} ({registry_cols})
            SELECT {registry_cols}
            FROM _franchise_identity_registry_stage s
            WHERE NOT EXISTS (
                SELECT 1
                FROM {registry_t} r
                WHERE r.db_name = s.db_name
                  AND r.resolved_franchise_id = s.resolved_franchise_id
            )
            """
        )

        conn.execute(
            f"""
            DELETE FROM {audit_t}
            WHERE db_name = {db_lit}
              AND base_franchise_id IN (
                  SELECT DISTINCT base_franchise_id
                  FROM _franchise_branch_assignments
              )
            """
        )
        conn.execute(
            f"""
            INSERT INTO {audit_t} ({audit_cols})
            SELECT {audit_cols}
            FROM _franchise_branch_assignments
            """
        )

        update_sql = f"""
            UPDATE {matchup_t} m
            SET franchise_id = c.resolved_franchise_id
            FROM _franchise_branch_assignments c
            WHERE {self._db_filter("m")}
              AND {base_franchise_expr("m")} = c.base_franchise_id
              AND m.year IS NOT DISTINCT FROM c.year
              AND m.week IS NOT DISTINCT FROM c.week
              AND m.team_name IS NOT DISTINCT FROM c.team_name
              {manager_match}
              {team_key_match}
        """
        self._execute(update_sql, "resolve_managers: split simultaneous multi-team owner GUIDs")

        # The branch split is authoritative for every table carrying the same
        # provider team key.  Imports can arrive with a concrete but pre-split
        # franchise_id (for example, a cross-platform merge target ID), so this
        # must correct mismatches rather than only fill NULL values.  Doing it
        # here also lets resolve_hidden_managers Step 1 propagate each branch's
        # disambiguated display name across the related tables.
        for table_name in ("player_fantasy", "draft", "transactions", "schedule"):
            if not self._table_exists(table_name):
                continue
            table_cols = self._get_table_columns(table_name)
            if not {"franchise_id", "team_key", "year"}.issubset(table_cols):
                continue
            table_ref = self._qualified_name(table_name)
            propagate_sql = f"""
                UPDATE {table_ref} t
                SET franchise_id = branch.resolved_franchise_id
                FROM (
                    SELECT
                        year,
                        team_key,
                        MIN(resolved_franchise_id) AS resolved_franchise_id
                    FROM _franchise_branch_assignments
                    WHERE NULLIF(TRIM(COALESCE(CAST(team_key AS VARCHAR), '')), '') IS NOT NULL
                    GROUP BY year, team_key
                    HAVING COUNT(DISTINCT resolved_franchise_id) = 1
                ) branch
                WHERE t.year = branch.year
                  AND NULLIF(TRIM(COALESCE(CAST(t.team_key AS VARCHAR), '')), '') IS NOT NULL
                  AND TRIM(CAST(t.team_key AS VARCHAR)) = TRIM(CAST(branch.team_key AS VARCHAR))
                  AND t.franchise_id IS DISTINCT FROM branch.resolved_franchise_id
                  AND {self._db_filter("t")}
            """
            self._execute(
                propagate_sql,
                f"resolve_managers: propagate multi-team branch ids to {table_name}",
            )

        logger.info(
            "[FRANCHISE_IDENTITY] %s: assigned %s matchup row(s) across %s branch(es)",
            self.db_name,
            assignment_count,
            conn.execute("SELECT COUNT(DISTINCT resolved_franchise_id) FROM _franchise_branch_assignments").fetchone()[
                0
            ],
        )
        return assignment_count

    def _disambiguate_duplicate_display_names(self, matchup_t: str) -> int:
        """Rewrite manager to disambiguate same-display-name / different-franchise_id collisions.

        Detects cases where two or more distinct franchise_ids share a canonical
        manager display name (e.g. two "Ryan"s on the same league) and rewrites
        the manager column to a disambiguated form.

        Rewrite rule:
          - Case A (canonical + team_name collision persists):
                "{canonical} - {team_name} ({RIGHT(franchise_id, 4)})"
          - Case B (canonical + team_name unique):
                "{canonical} - {team_name}"
          - Case C (canonical empty, team_name present):
                "{team_name}"
          - Case D (both empty AND colliding):
                "({RIGHT(franchise_id, 4)})"

        Join key: franchise_id throughout. `manager` is used only as an
        aggregated value for collision detection and as a target column for
        writes — never as a join key. Idempotent on re-run: the
        `colliding_franchise_ids` CTE empties after the first pass, and the
        `m.manager != d.disambiguated_name` guard skips no-op writes.

        Args:
            matchup_t: Qualified matchup table name (e.g. 'public.matchup').

        Returns:
            Number of rows affected by the final UPDATE.
        """
        # Spec Deliverables §1: emit a logger.warning for the pathological
        # Case D (both canonical manager empty AND team_name NULL). This is
        # defensive-only observability — Case D still resolves the collision
        # by emitting "(xxxx)" alone, but an operator should know when it
        # fires so upstream data gaps get investigated.
        try:
            case_d_sql = f"""
                WITH base AS (
                    SELECT franchise_id, manager, team_name, year, week
                    FROM {matchup_t}
                    WHERE {self._db_filter()} AND franchise_id IS NOT NULL
                ),
                canonical_one AS (
                    SELECT franchise_id, manager AS canonical_manager
                    FROM (
                        SELECT franchise_id, manager,
                               ROW_NUMBER() OVER (PARTITION BY franchise_id
                                                  ORDER BY year DESC, week DESC) AS rn
                        FROM base
                        WHERE manager IS NOT NULL AND TRIM(manager) != ''
                    ) t WHERE rn = 1
                )
                SELECT b.franchise_id
                FROM base b
                LEFT JOIN canonical_one c USING (franchise_id)
                WHERE (c.canonical_manager IS NULL OR TRIM(c.canonical_manager) = '')
                  AND (b.team_name IS NULL OR TRIM(b.team_name) = '')
                GROUP BY b.franchise_id
                """
            case_d_rows = self._get_connection().execute(case_d_sql).fetchall()
        except Exception:
            case_d_rows = []
        if case_d_rows:
            logger.warning(
                "[disambiguate] Case D pathological: %d franchise_id(s) have "
                "empty canonical manager AND NULL team_name; they will be "
                "labeled '(xxxx)' using franchise_id_short. Investigate upstream. "
                "Affected: %s",
                len(case_d_rows),
                [r[0] for r in case_d_rows[:5]],
            )

        sql = f"""
            WITH base AS (
                SELECT franchise_id, manager, team_name, year, week
                FROM {matchup_t}
                WHERE {self._db_filter()} AND franchise_id IS NOT NULL
            ),
            canonical AS (
                SELECT franchise_id, manager,
                       ROW_NUMBER() OVER (PARTITION BY franchise_id
                                          ORDER BY year DESC, week DESC) AS rn
                FROM base
                WHERE manager IS NOT NULL AND TRIM(manager) != ''
            ),
            canonical_one AS (
                SELECT franchise_id, manager AS canonical_manager
                FROM canonical WHERE rn = 1
            ),
            colliding_franchise_ids AS (
                SELECT franchise_id
                FROM canonical_one
                WHERE canonical_manager IN (
                    SELECT canonical_manager
                    FROM canonical_one
                    GROUP BY canonical_manager
                    HAVING COUNT(*) > 1
                )
            ),
            most_recent_team AS (
                SELECT franchise_id, team_name,
                       ROW_NUMBER() OVER (PARTITION BY franchise_id
                                          ORDER BY year DESC, week DESC) AS rn_t
                FROM base
                WHERE franchise_id IN (SELECT franchise_id FROM colliding_franchise_ids)
                  AND team_name IS NOT NULL AND TRIM(team_name) != ''
            ),
            collision_pairs AS (
                SELECT c.canonical_manager,
                       COALESCE(mrt.team_name, '__NULL__') AS team_name_key
                FROM canonical_one c
                LEFT JOIN most_recent_team mrt
                  ON mrt.franchise_id = c.franchise_id AND mrt.rn_t = 1
                WHERE c.franchise_id IN (SELECT franchise_id FROM colliding_franchise_ids)
                GROUP BY c.canonical_manager, COALESCE(mrt.team_name, '__NULL__')
                HAVING COUNT(*) > 1
            ),
            disambiguated AS (
                SELECT c.franchise_id,
                       CASE
                           WHEN c.canonical_manager IS NOT NULL
                             AND TRIM(c.canonical_manager) != ''
                             AND mrt.team_name IS NOT NULL
                             AND cp.canonical_manager IS NOT NULL
                           THEN c.canonical_manager || ' - ' || mrt.team_name
                                || ' (' || RIGHT(c.franchise_id, 4) || ')'
                           WHEN c.canonical_manager IS NOT NULL
                             AND TRIM(c.canonical_manager) != ''
                             AND mrt.team_name IS NOT NULL
                           THEN c.canonical_manager || ' - ' || mrt.team_name
                           WHEN mrt.team_name IS NOT NULL
                           THEN mrt.team_name
                           WHEN cp.canonical_manager IS NOT NULL
                           THEN '(' || RIGHT(c.franchise_id, 4) || ')'
                           ELSE c.canonical_manager
                       END AS disambiguated_name
                FROM canonical_one c
                LEFT JOIN most_recent_team mrt
                  ON mrt.franchise_id = c.franchise_id AND mrt.rn_t = 1
                LEFT JOIN collision_pairs cp
                  ON cp.canonical_manager = c.canonical_manager
                 AND cp.team_name_key = COALESCE(mrt.team_name, '__NULL__')
                WHERE c.franchise_id IN (SELECT franchise_id FROM colliding_franchise_ids)
            )
            UPDATE {matchup_t} m
            SET manager = d.disambiguated_name
            FROM disambiguated d
            WHERE m.franchise_id = d.franchise_id
              AND {self._db_filter("m")}
              AND d.disambiguated_name IS NOT NULL
        """
        return self._execute(sql, "resolve_managers: disambiguate duplicate display names")

    def _column_exists(self, table: str, col: str) -> bool:
        """Check if a column exists in a table."""
        try:
            cols = self._get_table_columns(table.split(".")[-1])
            return col in cols
        except Exception:
            return False

    @staticmethod
    def _sql_string(value: object) -> str:
        return "'" + str(value).replace("'", "''") + "'"

    def _saved_merge_display_name(self, merge: dict) -> str:
        display_name = str(merge.get("display_name") or merge.get("into_name") or "").strip()
        overrides = {
            str(source).strip().lower(): str(target).strip()
            for source, target in (getattr(self, "manager_name_overrides", None) or {}).items()
            if str(source).strip() and str(target).strip()
        }
        seen: set[str] = set()
        while display_name and display_name.lower() in overrides and display_name.lower() not in seen:
            seen.add(display_name.lower())
            display_name = overrides[display_name.lower()]
        return display_name

    def _resolved_saved_name_overrides(self) -> dict[str, str]:
        """Return case-insensitive saved aliases with chained aliases collapsed."""
        overrides = {
            str(source).strip().lower(): str(target).strip()
            for source, target in (getattr(self, "manager_name_overrides", None) or {}).items()
            if str(source).strip() and str(target).strip()
        }
        resolved: dict[str, str] = {}
        for source, first_target in overrides.items():
            target = first_target
            seen = {source}
            while target.lower() in overrides and target.lower() not in seen:
                seen.add(target.lower())
                target = overrides[target.lower()]
            if target and target.lower() != source:
                resolved[source] = target
        return resolved

    def _identity_scope_filter(self, alias: str = "", *, season_years: set[int] | None = None) -> str:
        scope = self._db_filter(alias)
        if season_years is not None:
            if any(type(year) is not int or year <= 0 for year in season_years):
                raise ValueError("Identity season years must be positive integers")
            if not season_years:
                return "FALSE"
            prefix = f"{alias}." if alias else ""
            scope += f" AND {prefix}year IN ({', '.join(str(year) for year in sorted(season_years))})"
        return scope

    def _apply_saved_manager_name_overrides(self, *, season_years: set[int] | None = None) -> int:
        """Apply persisted display aliases to every source identity surface."""
        overrides = self._resolved_saved_name_overrides()
        if not overrides:
            return 0

        name_targets = {
            "matchup": ("manager", "opponent", "franchise_name"),
            "player_fantasy": ("manager", "opponent", "franchise_name"),
            "draft": ("manager", "franchise_name"),
            "transactions": (
                "manager",
                "source_manager",
                "destination_manager",
                "franchise_name",
            ),
            "schedule": ("manager", "opponent", "franchise_name"),
        }
        values_sql = ", ".join(
            f"({self._sql_string(source)}, {self._sql_string(target)})"
            for source, target in overrides.items()
        )

        total = 0
        for table, candidate_columns in name_targets.items():
            if not self._table_exists(table):
                continue
            cols = self._get_table_columns(table)
            table_ref = self._qualified_name(table)
            for name_col in candidate_columns:
                if name_col not in cols:
                    continue
                sql = f"""
                    UPDATE {table_ref} t
                    SET {name_col} = aliases.target
                    FROM (VALUES {values_sql}) AS aliases(source, target)
                    WHERE {self._identity_scope_filter('t', season_years=season_years)}
                      AND LOWER(TRIM(CAST(t.{name_col} AS VARCHAR))) = aliases.source
                """
                total += self._execute(sql, f"resolve_managers: saved alias {table}.{name_col}")
        return total

    def _link_schedule_opponent_identities(self, *, season_years: set[int] | None = None) -> int:
        """Resolve schedule opponent IDs from its paired manager rows."""
        if not self._table_exists("schedule"):
            return 0
        cols = self._get_table_columns("schedule")
        required = {"year", "week", "manager", "franchise_id", "opponent", "opponent_franchise_id"}
        if not required.issubset(cols):
            return 0

        schedule_table = self._qualified_name("schedule")
        set_parts = ["opponent_franchise_id = lkp.franchise_id"]
        guid_select = ""
        if {"manager_guid", "opponent_guid"}.issubset(cols):
            guid_select = ", MIN(manager_guid) AS manager_guid"
            set_parts.append("opponent_guid = lkp.manager_guid")
        sql = f"""
            UPDATE {schedule_table} s
            SET {', '.join(set_parts)}
            FROM (
                SELECT year,
                       week,
                       LOWER(TRIM(manager)) AS manager_norm,
                       MIN(franchise_id) AS franchise_id
                       {guid_select}
                FROM {schedule_table}
                WHERE {self._identity_scope_filter(season_years=season_years)}
                  AND franchise_id IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(manager, '')), '') IS NOT NULL
                GROUP BY year, week, LOWER(TRIM(manager))
                HAVING COUNT(DISTINCT franchise_id) = 1
            ) lkp
            WHERE s.year = lkp.year
              AND s.week = lkp.week
              AND LOWER(TRIM(COALESCE(s.opponent, ''))) = lkp.manager_norm
              AND {self._identity_scope_filter('s', season_years=season_years)}
        """
        return self._execute(sql, "resolve_managers: schedule opponent identity")

    def reapply_saved_identity_settings(self, *, season_years: set[int] | None = None) -> int:
        """Reconcile saved identities after a provider source table is restored."""
        self._identity_scope_filter(season_years=season_years)
        total = self._apply_saved_franchise_merges(season_years=season_years)
        total += self._apply_saved_manager_name_overrides(season_years=season_years)
        total += self._link_schedule_opponent_identities(season_years=season_years)
        return total

    def _canonicalize_preseason_manager_names(self) -> int:
        """Use the best active source name when no matchup row exists yet."""
        candidates: list[str] = []
        for priority, table in enumerate(("schedule", "player_fantasy", "transactions", "draft")):
            if not self._table_exists(table):
                continue
            cols = self._get_table_columns(table)
            if not {"franchise_id", "manager"}.issubset(cols):
                continue
            table_ref = self._qualified_name(table)
            year_expr = "COALESCE(CAST(year AS BIGINT), 0)" if "year" in cols else "0"
            week_expr = "COALESCE(CAST(week AS BIGINT), 0)" if "week" in cols else "0"
            candidates.append(
                f"""
                SELECT franchise_id, manager, {priority} AS source_priority,
                       {year_expr} AS year_order, {week_expr} AS week_order
                FROM {table_ref}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(manager, '')), '') IS NOT NULL
                  AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
                """
            )
        if not candidates:
            return 0

        canonical_cte = f"""
            WITH candidates AS ({' UNION ALL '.join(candidates)}),
            canonical AS (
                SELECT franchise_id, manager
                FROM candidates
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY franchise_id
                    ORDER BY source_priority, year_order DESC, week_order DESC, manager
                ) = 1
            )
        """
        identity_targets = {
            "matchup": (("franchise_id", "manager"), ("opponent_franchise_id", "opponent")),
            "player_fantasy": (("franchise_id", "manager"), ("opponent_franchise_id", "opponent")),
            "draft": (("franchise_id", "manager"),),
            "transactions": (
                ("franchise_id", "manager"),
                ("source_franchise_id", "source_manager"),
                ("destination_franchise_id", "destination_manager"),
            ),
            "schedule": (("franchise_id", "manager"), ("opponent_franchise_id", "opponent")),
        }
        total = 0
        for table, pairs in identity_targets.items():
            if not self._table_exists(table):
                continue
            cols = self._get_table_columns(table)
            table_ref = self._qualified_name(table)
            for identity_col, name_col in pairs:
                if identity_col not in cols or name_col not in cols:
                    continue
                assignments = [f"{name_col} = c.manager"]
                if identity_col == "franchise_id" and "franchise_name" in cols:
                    assignments.append("franchise_name = c.manager")
                total += self._execute(
                    f"""
                    {canonical_cte}
                    UPDATE {table_ref} t
                    SET {', '.join(assignments)}
                    FROM canonical c
                    WHERE t.{identity_col} = c.franchise_id
                      AND {self._db_filter('t')}
                    """,
                    f"resolve_managers: preseason canonical name {table}.{name_col}",
                )
        return total

    def _apply_saved_franchise_merges(self, *, season_years: set[int] | None = None) -> int:
        """Reapply persisted identity merges before rebuilding derived manager data."""
        merges = getattr(self, "franchise_merges", None) or []
        if not merges:
            return 0

        identity_targets = {
            "matchup": [
                ("franchise_id", "manager"),
                ("opponent_franchise_id", "opponent"),
            ],
            "player_fantasy": [
                ("franchise_id", "manager"),
                ("opponent_franchise_id", "opponent"),
            ],
            "draft": [("franchise_id", "manager")],
            "transactions": [
                ("franchise_id", "manager"),
                ("source_franchise_id", "source_manager"),
                ("destination_franchise_id", "destination_manager"),
            ],
            "schedule": [
                ("franchise_id", "manager"),
                ("opponent_franchise_id", "opponent"),
            ],
        }

        total = 0
        for merge in merges:
            if not isinstance(merge, dict):
                continue
            owner_ids = merge.get("owner_ids") or merge.get("franchise_ids") or []
            members = [str(value).strip() for value in owner_ids if str(value).strip()]
            for key in ("from_franchise_id", "into_franchise_id"):
                value = str(merge.get(key) or "").strip()
                if value and value not in members:
                    members.append(value)
            canonical = str(merge.get("into_franchise_id") or (members[0] if members else "")).strip()
            if not canonical or len(set(members)) < 2:
                continue
            display_name = self._saved_merge_display_name(merge)
            member_sql = ", ".join(self._sql_string(value) for value in dict.fromkeys(members))

            for table, targets in identity_targets.items():
                if not self._table_exists(table):
                    continue
                cols = self._get_table_columns(table)
                table_ref = self._qualified_name(table)
                for identity_col, name_col in targets:
                    if identity_col not in cols:
                        continue
                    assignments = [f"{identity_col} = {self._sql_string(canonical)}"]
                    if display_name and name_col in cols:
                        assignments.append(f"{name_col} = {self._sql_string(display_name)}")
                    if identity_col == "franchise_id" and display_name and "franchise_name" in cols:
                        assignments.append(f"franchise_name = {self._sql_string(display_name)}")
                    sql = f"""
                        UPDATE {table_ref} t
                        SET {', '.join(assignments)}
                        WHERE {self._identity_scope_filter('t', season_years=season_years)}
                          AND t.{identity_col} IN ({member_sql})
                    """
                    total += self._execute(sql, f"resolve_managers: saved merge {table}.{identity_col}")
        return total

    def resolve_hidden_managers(self) -> int:
        """Resolve manager names, franchise identity, and opponent linkage.

        Replaces resolve_hidden_managers.py + discover_franchises.py.
        Pure SQL, cross-platform. 5 batched operations:

        0. Disambiguate hidden franchise_ids: when multiple distinct team_name
           values share the same franchise_id (e.g. '--'), create synthetic IDs
           so they don't get merged by the canonical name resolution.
        1. Canonical names: most-recent-year name per franchise_id → all tables
        2. Opponent franchise_id: reverse matchup self-join
        3. Sync opponents: use opponent_franchise_id to fix opponent display names
        4. Stubs: INSERT minimal rows for hidden managers missing from player_fantasy
        """
        if not self._table_exists("matchup"):
            total = self._canonicalize_preseason_manager_names()
            total += self.reapply_saved_identity_settings()
            return total

        matchup_t = self._qualified_name("matchup")
        # A live week can have roster/player data before the provider posts a
        # single matchup result. Every operation below derives identity from a
        # matchup row, so with no scoped rows it can only issue empty UPDATEs
        # (and, in a fresh local database, unnecessary schema work).
        if self._get_connection().execute(
            f"SELECT 1 FROM {matchup_t} WHERE {self._db_filter()} LIMIT 1"
        ).fetchone() is None:
            total = self._canonicalize_preseason_manager_names()
            total += self.reapply_saved_identity_settings()
            return total

        # Saved Settings identity choices apply to every refreshed source table.
        total = self.reapply_saved_identity_settings()

        # --- Step 0: Disambiguate shared franchise_ids -----------------------
        # When multiple distinct team_name values share the same franchise_id
        # (e.g. two Yahoo --hidden-- managers both have franchise_id = '--'),
        # they are different people. Assign synthetic franchise_ids based on
        # team_name so the canonical name resolution doesn't merge them.
        total += self._disambiguate_shared_franchise_ids(matchup_t)
        total += self._disambiguate_simultaneous_multi_team_franchise_ids(matchup_t)

        # --- Step 0.5: Disambiguate duplicate display names ------------------
        # When two distinct franchise_ids share the same canonical manager
        # display name (e.g. "two Ryans"), rewrite manager to "{name} - {team}"
        # — plus a deterministic franchise_id_short suffix if team_names also
        # collide. Propagation to player_fantasy / draft / transactions /
        # schedule happens automatically via Step 1's canonical CTE below.
        total += self._disambiguate_duplicate_display_names(matchup_t)

        # --- Build canonical name CTE (reused across tables) ---
        canonical_cte = f"""
            WITH canonical AS (
                SELECT franchise_id, manager,
                       ROW_NUMBER() OVER (
                           PARTITION BY franchise_id
                           ORDER BY year DESC, week DESC
                       ) AS rn
                FROM {matchup_t}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL
                  AND manager IS NOT NULL
                  AND TRIM(manager) != ''
            )
        """

        # Step 1: Resolve canonical manager name across all tables.
        # Matchup rows with an explicit paired opponent team key already carry
        # source-truth team-week identity from the fetcher. Keep visible API
        # names intact for those rows so stale franchise/team-name history does
        # not rewrite the current matchup owner.
        for table in ["matchup", "player_fantasy", "draft", "transactions", "schedule"]:
            if not self._table_exists(table):
                continue
            cols = self._get_table_columns(table)
            if "franchise_id" not in cols or "manager" not in cols:
                continue
            t = self._qualified_name(table)
            manager_update_guard = "AND t.manager IS NOT NULL"
            if table == "matchup" and {"team_key", "opponent_team_key"}.issubset(cols):
                manager_update_guard = """
                  AND (
                      NULLIF(TRIM(COALESCE(CAST(t.team_key AS VARCHAR), '')), '') IS NULL
                      OR NULLIF(TRIM(COALESCE(CAST(t.opponent_team_key AS VARCHAR), '')), '') IS NULL
                      OR NULLIF(TRIM(COALESCE(CAST(t.manager AS VARCHAR), '')), '') IS NULL
                      OR LOWER(TRIM(CAST(t.manager AS VARCHAR))) IN ('--', '--hidden--', 'hidden', 'unknown', 'none', 'nan')
                  )
                """

            sql = f"""
                {canonical_cte}
                UPDATE {t} t
                SET manager = c.manager
                FROM canonical c
                WHERE t.franchise_id = c.franchise_id
                  AND {self._db_filter("t")}
                  AND c.rn = 1
                  {manager_update_guard}
            """
            total += self._execute(sql, f"resolve_managers: {table}")

        # Step 2: Populate opponent_franchise_id via reverse matchup self-join
        matchup_cols = self._get_table_columns("matchup")
        self._ensure_columns(
            matchup_t,
            {
                "opponent_franchise_id": "VARCHAR",
                "franchise_name": "VARCHAR",
            },
            matchup_cols,
        )
        self._invalidate_column_cache("matchup")

        # Prefer platform-provided opponent team keys when available. This is
        # exact for paired API rows and avoids score-only collisions.
        # This is robust against name collisions (two "Ryan"s) and case mismatches.
        # Match: same (year, week), my team_points = their opponent_points AND vice versa.
        matchup_cols = self._get_table_columns("matchup")
        if "franchise_id" in matchup_cols and "opponent_franchise_id" in matchup_cols:
            if {"team_key", "opponent_team_key"}.issubset(matchup_cols):
                sql_opp_team_key = f"""
                    UPDATE {matchup_t} m
                    SET opponent_franchise_id = opp.franchise_id
                    FROM {matchup_t} opp
                    WHERE m.year = opp.year AND m.week = opp.week
                      AND {self._db_filter("m")}
                      AND {self._db_filter("opp")}
                      AND m.franchise_id != opp.franchise_id
                      AND NULLIF(TRIM(COALESCE(CAST(m.opponent_team_key AS VARCHAR), '')), '') IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(CAST(opp.team_key AS VARCHAR), '')), '') IS NOT NULL
                      AND TRIM(CAST(m.opponent_team_key AS VARCHAR)) = TRIM(CAST(opp.team_key AS VARCHAR))
                      AND opp.franchise_id IS NOT NULL
                      AND (
                          m.opponent_franchise_id IS NULL
                          OR TRIM(COALESCE(CAST(m.opponent_franchise_id AS VARCHAR), '')) = ''
                          OR TRIM(CAST(m.opponent_franchise_id AS VARCHAR)) != TRIM(CAST(opp.franchise_id AS VARCHAR))
                      )
                """
                total += self._execute(
                    sql_opp_team_key,
                    "resolve_managers: opponent_franchise_id (opponent_team_key)",
                )

            score_guard = ""
            if "opponent_team_key" in matchup_cols:
                score_guard = """
                  AND NULLIF(TRIM(COALESCE(CAST(m.opponent_team_key AS VARCHAR), '')), '') IS NULL
                """
            sql_opp_fid = f"""
                UPDATE {matchup_t} m
                SET opponent_franchise_id = opp.franchise_id
                FROM {matchup_t} opp
                WHERE m.year = opp.year AND m.week = opp.week
                  AND {self._db_filter("m")}
                  AND {self._db_filter("opp")}
                  AND m.franchise_id != opp.franchise_id
                  AND ROUND(CAST(m.team_points AS DOUBLE), 2) = ROUND(CAST(opp.opponent_points AS DOUBLE), 2)
                  AND ROUND(CAST(m.opponent_points AS DOUBLE), 2) = ROUND(CAST(opp.team_points AS DOUBLE), 2)
                  AND m.team_points IS NOT NULL AND CAST(m.team_points AS DOUBLE) != 0
                  AND opp.franchise_id IS NOT NULL
                  {score_guard}
            """
            total += self._execute(sql_opp_fid, "resolve_managers: opponent_franchise_id (score-based)")

            # Step 2a: Repair asymmetric pairings. When two distinct pairs share
            # identical scores (e.g. cffl 2007 wk2: Steve-Thomas 106-96 AND
            # Jeffrey-Alan 106-96), the score-only self-join above can pick
            # the wrong partner for one side. If row R points at S but S
            # already symmetrically points at someone else (T), we trust the
            # symmetric pair (S<->T) and re-derive R's opponent_franchise_id
            # from whichever counter-row points back at R.
            sql_repair = f"""
                UPDATE {matchup_t} m
                SET opponent_franchise_id = s.franchise_id
                FROM {matchup_t} s
                WHERE m.year = s.year AND m.week = s.week
                  AND {self._db_filter("m")}
                  AND {self._db_filter("s")}
                  AND s.opponent_franchise_id = m.franchise_id
                  AND s.franchise_id != m.franchise_id
                  AND m.franchise_id IS NOT NULL
                  AND COALESCE(m.is_bye_week, FALSE) = FALSE
                  AND NOT EXISTS (
                      SELECT 1 FROM {matchup_t} r
                      WHERE r.year = m.year AND r.week = m.week
                        AND {self._db_filter("r")}
                        AND r.franchise_id = m.opponent_franchise_id
                        AND r.opponent_franchise_id = m.franchise_id
                  )
            """
            total += self._execute(sql_repair, "resolve_managers: repair asymmetric opponent_franchise_id")

        # Step 2b: Set franchise_name = canonical manager name
        sql_fname = f"""
            {canonical_cte}
            UPDATE {matchup_t} t
            SET franchise_name = c.manager
            FROM canonical c
            WHERE t.franchise_id = c.franchise_id AND c.rn = 1
              AND {self._db_filter("t")}
              AND (t.franchise_name IS NULL OR t.franchise_name = '')
        """
        total += self._execute(sql_fname, "resolve_managers: franchise_name")

        # Step 3: Sync opponent display names using opponent_franchise_id.
        # Preserve visible opponent names on rows that already have explicit
        # opponent team-key identity from the fetcher.
        opponent_update_guard = ""
        if "opponent_team_key" in matchup_cols:
            opponent_update_guard = """
              AND (
                  NULLIF(TRIM(COALESCE(CAST(m.opponent_team_key AS VARCHAR), '')), '') IS NULL
                  OR NULLIF(TRIM(COALESCE(CAST(m.opponent AS VARCHAR), '')), '') IS NULL
                  OR LOWER(TRIM(CAST(m.opponent AS VARCHAR))) IN ('--', '--hidden--', 'hidden', 'unknown', 'none', 'nan')
              )
            """
        sql_opp = f"""
            UPDATE {matchup_t} m
            SET opponent = opp.manager
            FROM (
                SELECT DISTINCT franchise_id, manager
                FROM {matchup_t}
                WHERE {self._db_filter()}
                  AND franchise_id IS NOT NULL AND manager IS NOT NULL
            ) opp
            WHERE m.opponent_franchise_id = opp.franchise_id
              AND {self._db_filter("m")}
              {opponent_update_guard}
        """
        total += self._execute(sql_opp, "resolve_managers: sync opponent names")

        return total

    def populate_franchise_id(self) -> int:
        """Populate franchise_id in player_fantasy from matchup table.

        This MUST run before matchup_to_player, which joins on franchise_id.
        The matchup table has franchise_id populated from the franchise discovery
        step, but player_fantasy may not have it yet.

        Join key preference:
        1. team_key + year + week
        2. manager_guid + team_name + year + week
        3. unambiguous manager_guid + year + week
        4. unambiguous normalized manager + year + week
        5. team_name + year + week (last resort)
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[populate_franchise_id] player_fantasy table not found")
            return 0

        if not self._table_exists("matchup"):
            logger.warning("[populate_franchise_id] matchup table not found")
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        matchup_cols = self._get_table_columns("matchup")

        if "franchise_id" not in matchup_cols:
            logger.warning("[populate_franchise_id] matchup.franchise_id not found")
            return 0

        # franchise_id column is pre-defined by canonical DDL

        player_table = self._qualified_name("player_fantasy")
        matchup_table = self._qualified_name("matchup")

        invalid_franchise_tokens = "('', '--', '--hidden--', 'none', 'nan', '<na>', 'n/a', 'null')"

        def invalid_franchise_expr(alias: str, column: str = "franchise_id") -> str:
            return (
                f"({alias}.{column} IS NULL OR "
                f"LOWER(TRIM(COALESCE(CAST({alias}.{column} AS VARCHAR), ''))) "
                f"IN {invalid_franchise_tokens})"
            )

        needs_player_backfill = invalid_franchise_expr("p")
        if "manager_guid" in player_cols:
            needs_player_backfill = (
                "("
                f"{invalid_franchise_expr('p')} OR ("
                "NULLIF(TRIM(COALESCE(p.manager_guid, '')), '') IS NOT NULL "
                "AND TRIM(COALESCE(p.franchise_id, '')) = TRIM(COALESCE(p.manager_guid, '')) "
                "AND TRIM(COALESCE(m.franchise_id, '')) != TRIM(COALESCE(p.franchise_id, ''))"
                "))"
            )
        # Manager-name fallback is intentionally stricter than team_key / guid-based
        # passes. If a row already has a concrete guid-derived franchise_id, a bad
        # raw manager name should not be able to collapse it onto another manager.
        needs_player_manager_fallback = invalid_franchise_expr("p")

        total = 0
        if "team_key" in player_cols and "team_key" in matchup_cols:
            logger.info("[populate_franchise_id] Using team_key + year/week join")
            sql = f"""
                UPDATE {player_table} p
                SET franchise_id = m.franchise_id
                FROM {matchup_table} m
                WHERE p.year = m.year
                  AND p.week = m.week
                  AND {self._db_filter("p")}
                  AND {self._db_filter("m")}
                  AND NULLIF(TRIM(COALESCE(p.team_key, '')), '') IS NOT NULL
                  AND NULLIF(TRIM(COALESCE(m.team_key, '')), '') IS NOT NULL
                  AND TRIM(p.team_key) = TRIM(m.team_key)
                  AND {rostered_filter_sql("p")}
                  AND {needs_player_backfill}
            """
            total += self._execute(sql, "populate_franchise_id: player_fantasy pass 1 (team_key)")

            logger.info("[populate_franchise_id] Using unambiguous team_key + year join")
            sql = f"""
                UPDATE {player_table} p
                SET franchise_id = m.franchise_id
                FROM (
                    SELECT team_key, year, MIN(franchise_id) AS franchise_id
                    FROM {matchup_table}
                    WHERE {self._db_filter()}
                      AND franchise_id IS NOT NULL
                      AND NULLIF(TRIM(COALESCE(team_key, '')), '') IS NOT NULL
                    GROUP BY team_key, year
                    HAVING COUNT(DISTINCT franchise_id) = 1
                ) m
                WHERE p.year = m.year
                  AND {self._db_filter("p")}
                  AND NULLIF(TRIM(COALESCE(p.team_key, '')), '') IS NOT NULL
                  AND TRIM(p.team_key) = TRIM(m.team_key)
                  AND {rostered_filter_sql("p")}
                  AND {needs_player_backfill}
            """
            total += self._execute(sql, "populate_franchise_id: player_fantasy pass 1b (team_key year)")

        if (
            "manager_guid" in player_cols
            and "manager_guid" in matchup_cols
            and "team_name" in player_cols
            and "team_name" in matchup_cols
        ):
            join_clauses = [
                "p.year = m.year",
                "p.week = m.week",
                self._db_filter("p"),
                self._db_filter("m"),
                "NULLIF(TRIM(p.manager_guid), '') IS NOT NULL",
                "NULLIF(TRIM(m.manager_guid), '') IS NOT NULL",
                "TRIM(p.manager_guid) = TRIM(m.manager_guid)",
                rostered_filter_sql("p"),
                needs_player_backfill,
            ]
            join_clauses.append(
                "COALESCE(NULLIF(TRIM(p.team_name), ''), '__EMPTY__') = "
                "COALESCE(NULLIF(TRIM(m.team_name), ''), '__EMPTY__')"
            )
            logger.info("[populate_franchise_id] Using manager_guid + team_name + year/week join")

            sql = f"""
                UPDATE {player_table} p
                SET franchise_id = m.franchise_id
                FROM {matchup_table} m
                WHERE {" AND ".join(join_clauses)}
            """
            total += self._execute(sql, "populate_franchise_id: player_fantasy pass 2 (manager_guid + team_name)")

        if "manager_guid" in player_cols and "manager_guid" in matchup_cols:
            logger.info("[populate_franchise_id] Using unambiguous manager_guid + year/week join")
            sql = f"""
                UPDATE {player_table} p
                SET franchise_id = m.franchise_id
                FROM (
                    SELECT manager_guid, year, week, MIN(franchise_id) AS franchise_id
                    FROM {matchup_table}
                    WHERE {self._db_filter()}
                      AND franchise_id IS NOT NULL
                      AND NULLIF(TRIM(manager_guid), '') IS NOT NULL
                    GROUP BY manager_guid, year, week
                    HAVING COUNT(DISTINCT franchise_id) = 1
                ) m
                WHERE p.year = m.year
                  AND p.week = m.week
                  AND {self._db_filter("p")}
                  AND NULLIF(TRIM(COALESCE(p.manager_guid, '')), '') IS NOT NULL
                  AND TRIM(p.manager_guid) = TRIM(m.manager_guid)
                  AND {rostered_filter_sql("p")}
                  AND {needs_player_backfill}
            """
            total += self._execute(sql, "populate_franchise_id: player_fantasy pass 3 (manager_guid unambiguous)")

        if "manager" in player_cols and "manager" in matchup_cols:
            logger.info("[populate_franchise_id] Using unambiguous normalized manager + year/week join")
            sql = f"""
                UPDATE {player_table} p
                SET franchise_id = m.franchise_id
                FROM (
                    SELECT LOWER(TRIM(manager)) AS manager_norm,
                           year,
                           week,
                           MIN(franchise_id) AS franchise_id
                    FROM {matchup_table}
                    WHERE {self._db_filter()}
                      AND franchise_id IS NOT NULL
                      AND NULLIF(TRIM(manager), '') IS NOT NULL
                    GROUP BY LOWER(TRIM(manager)), year, week
                    HAVING COUNT(DISTINCT franchise_id) = 1
                ) m
                WHERE p.year = m.year
                  AND p.week = m.week
                  AND {self._db_filter("p")}
                  AND NULLIF(TRIM(COALESCE(p.manager, '')), '') IS NOT NULL
                  AND LOWER(TRIM(p.manager)) = m.manager_norm
                  AND {rostered_filter_sql("p")}
                  AND {needs_player_manager_fallback}
            """
            total += self._execute(sql, "populate_franchise_id: player_fantasy pass 4 (manager unambiguous)")

        if "team_name" in player_cols and "team_name" in matchup_cols:
            logger.warning("[populate_franchise_id] Using team_name + year/week join as final fallback")
            sql = f"""
                UPDATE {player_table} p
                SET franchise_id = m.franchise_id
                FROM {matchup_table} m
                WHERE LOWER(TRIM(COALESCE(p.team_name, ''))) = LOWER(TRIM(COALESCE(m.team_name, '')))
                  AND p.year = m.year
                  AND p.week = m.week
                  AND {self._db_filter("p")}
                  AND {self._db_filter("m")}
                  AND {rostered_filter_sql("p")}
                  AND {needs_player_manager_fallback}
            """
            total += self._execute(sql, "populate_franchise_id: player_fantasy pass 5 (team_name fallback)")

        # Also populate franchise_id in draft and transactions tables.
        # Multi-pass approach handles deleted Sleeper accounts where:
        #   - matchup has manager="Team 12" (renamed by franchise registry)
        #   - draft/transactions have manager="Unknown" (raw fetcher output)
        #   - manager_guid may be empty in draft/transactions but valid in matchup
        conn = self._get_connection()
        for extra_table in ("draft", "transactions"):
            if not self._table_exists(extra_table):
                continue
            extra_cols = self._get_table_columns(extra_table)
            if "franchise_id" not in extra_cols or "manager" not in extra_cols:
                continue
            extra_qualified = self._qualified_name(extra_table)
            has_guid = "manager_guid" in extra_cols and "manager_guid" in matchup_cols
            has_team_key = "team_key" in extra_cols and "team_key" in matchup_cols
            has_team_name = "team_name" in extra_cols and "team_name" in matchup_cols
            needs_extra_backfill = invalid_franchise_expr("t")
            if "manager_guid" in extra_cols:
                needs_extra_backfill = (
                    "("
                    f"{invalid_franchise_expr('t')} OR ("
                    "NULLIF(TRIM(COALESCE(t.manager_guid, '')), '') IS NOT NULL "
                    "AND TRIM(COALESCE(t.franchise_id, '')) = TRIM(COALESCE(t.manager_guid, '')) "
                    "AND TRIM(COALESCE(lkp.franchise_id, '')) != TRIM(COALESCE(t.franchise_id, ''))"
                    "))"
                )

            # Yahoo publishes the complete weekly schedule before every team
            # necessarily has a player whose NFL game is finalized. That makes
            # schedule the authoritative all-team identity source in preseason.
            if self._table_exists("schedule"):
                schedule_cols = self._get_table_columns("schedule")
                schedule_table = self._qualified_name("schedule")
                if {"year", "franchise_id", "team_name"}.issubset(schedule_cols) and "team_name" in extra_cols:
                    total += self._execute(
                        f"""
                        UPDATE {extra_qualified} t
                        SET franchise_id = lkp.franchise_id
                        FROM (
                            SELECT year,
                                   LOWER(TRIM(team_name)) AS team_name_norm,
                                   MIN(franchise_id) AS franchise_id
                            FROM {schedule_table}
                            WHERE {self._db_filter()}
                              AND franchise_id IS NOT NULL
                              AND NULLIF(TRIM(COALESCE(team_name, '')), '') IS NOT NULL
                            GROUP BY year, LOWER(TRIM(team_name))
                            HAVING COUNT(DISTINCT franchise_id) = 1
                        ) lkp
                        WHERE t.year = lkp.year
                          AND LOWER(TRIM(COALESCE(t.team_name, ''))) = lkp.team_name_norm
                          AND {needs_extra_backfill}
                          AND {self._db_filter('t')}
                        """,
                        f"populate_franchise_id: {extra_table} pass 0s (schedule team_name)",
                    )
                if {"year", "franchise_id", "manager"}.issubset(schedule_cols) and "manager" in extra_cols:
                    total += self._execute(
                        f"""
                        UPDATE {extra_qualified} t
                        SET franchise_id = lkp.franchise_id
                        FROM (
                            SELECT year,
                                   LOWER(TRIM(manager)) AS manager_norm,
                                   MIN(franchise_id) AS franchise_id
                            FROM {schedule_table}
                            WHERE {self._db_filter()}
                              AND franchise_id IS NOT NULL
                              AND NULLIF(TRIM(COALESCE(manager, '')), '') IS NOT NULL
                            GROUP BY year, LOWER(TRIM(manager))
                            HAVING COUNT(DISTINCT franchise_id) = 1
                        ) lkp
                        WHERE t.year = lkp.year
                          AND LOWER(TRIM(COALESCE(t.manager, ''))) = lkp.manager_norm
                          AND {needs_extra_backfill}
                          AND {self._db_filter('t')}
                        """,
                        f"populate_franchise_id: {extra_table} pass 0t (schedule manager)",
                    )

            # Preseason providers can expose complete roster identities before
            # they expose a matchup row. Use the roster table as an equally
            # stable same-season source for draft and transaction ownership.
            if "franchise_id" in player_cols and "year" in player_cols and "year" in extra_cols:
                if "team_key" in extra_cols and "team_key" in player_cols:
                    total += self._execute(
                        f"""
                        UPDATE {extra_qualified} t
                        SET franchise_id = lkp.franchise_id
                        FROM (
                            SELECT year, team_key, MIN(franchise_id) AS franchise_id
                            FROM {player_table}
                            WHERE {self._db_filter()}
                              AND franchise_id IS NOT NULL
                              AND NULLIF(TRIM(COALESCE(team_key, '')), '') IS NOT NULL
                              AND {rostered_filter_sql()}
                            GROUP BY year, team_key
                            HAVING COUNT(DISTINCT franchise_id) = 1
                        ) lkp
                        WHERE t.year = lkp.year
                          AND TRIM(COALESCE(t.team_key, '')) = TRIM(lkp.team_key)
                          AND {needs_extra_backfill}
                          AND {self._db_filter('t')}
                        """,
                        f"populate_franchise_id: {extra_table} pass 0a (roster team_key)",
                    )

                if "manager" in extra_cols and "manager" in player_cols:
                    total += self._execute(
                        f"""
                        UPDATE {extra_qualified} t
                        SET franchise_id = lkp.franchise_id
                        FROM (
                            SELECT year,
                                   LOWER(TRIM(manager)) AS manager_norm,
                                   MIN(franchise_id) AS franchise_id
                            FROM {player_table}
                            WHERE {self._db_filter()}
                              AND franchise_id IS NOT NULL
                              AND NULLIF(TRIM(COALESCE(manager, '')), '') IS NOT NULL
                              AND {rostered_filter_sql()}
                            GROUP BY year, LOWER(TRIM(manager))
                            HAVING COUNT(DISTINCT franchise_id) = 1
                        ) lkp
                        WHERE t.year = lkp.year
                          AND LOWER(TRIM(COALESCE(t.manager, ''))) = lkp.manager_norm
                          AND {needs_extra_backfill}
                          AND {self._db_filter('t')}
                        """,
                        f"populate_franchise_id: {extra_table} pass 0b (roster manager)",
                    )

            # Pass 1: Join on team_key when available (most specific for same-owner multi-team leagues).
            if has_team_key:
                total += self._execute(
                    f"""
                    UPDATE {extra_qualified} t
                    SET franchise_id = lkp.franchise_id
                    FROM (SELECT DISTINCT team_key, year, franchise_id FROM {matchup_table}
                          WHERE {self._db_filter()}
                            AND franchise_id IS NOT NULL
                            AND NULLIF(TRIM(team_key), '') IS NOT NULL) lkp
                    WHERE NULLIF(TRIM(t.team_key), '') IS NOT NULL
                      AND TRIM(t.team_key) = TRIM(lkp.team_key)
                      AND t.year = lkp.year
                      AND {needs_extra_backfill}
                      AND {self._db_filter("t")}
                """,
                    f"populate_franchise_id: {extra_table} pass 1 (team_key)",
                )

            # Pass 2: Join on team_name (stable within-year and avoids duplicate-name collisions)
            if has_team_name:
                total += self._execute(
                    f"""
                    UPDATE {extra_qualified} t
                    SET franchise_id = lkp.franchise_id
                    FROM (SELECT DISTINCT team_name, year, franchise_id FROM {matchup_table}
                          WHERE {self._db_filter()}
                            AND franchise_id IS NOT NULL AND team_name IS NOT NULL) lkp
                    WHERE LOWER(TRIM(COALESCE(t.team_name, ''))) = LOWER(TRIM(COALESCE(lkp.team_name, '')))
                      AND t.year = lkp.year
                      AND {needs_extra_backfill}
                      AND t.team_name IS NOT NULL AND TRIM(t.team_name) != ''
                      AND {self._db_filter("t")}
                """,
                    f"populate_franchise_id: {extra_table} pass 2 (team_name)",
                )

            # Pass 3: Join on manager_guid only when it maps to exactly one franchise that year.
            if has_guid:
                total += self._execute(
                    f"""
                    UPDATE {extra_qualified} t
                    SET franchise_id = lkp.franchise_id
                    FROM (
                          SELECT manager_guid, year, MIN(franchise_id) AS franchise_id
                          FROM {matchup_table}
                          WHERE {self._db_filter()}
                            AND franchise_id IS NOT NULL
                            AND NULLIF(TRIM(manager_guid), '') IS NOT NULL
                          GROUP BY manager_guid, year
                          HAVING COUNT(DISTINCT franchise_id) = 1
                    ) lkp
                    WHERE NULLIF(TRIM(t.manager_guid), '') IS NOT NULL
                      AND TRIM(t.manager_guid) = TRIM(lkp.manager_guid)
                      AND t.year = lkp.year
                   AND {needs_extra_backfill}
                   AND {self._db_filter("t")}
                """,
                    f"populate_franchise_id: {extra_table} pass 3 (manager_guid unambiguous)",
                )

            # Pass 4: Synthetic fallback — any remaining NULLs get a deterministic ID
            # so franchise_id is NEVER NULL on any manager row
            unresolved = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {extra_qualified} t
                WHERE t.franchise_id IS NULL
                  AND t.manager IS NOT NULL AND TRIM(t.manager) != ''
                  AND {self._db_filter("t")}
                """
            ).fetchone()[0]
            if unresolved:
                logger.warning(
                    "[populate_franchise_id] %s %s row(s) still missing franchise_id after stable backfills; "
                    "leaving NULL rather than using manager-name fallback",
                    unresolved,
                    extra_table,
                )

        # ── Normalize source_franchise_id + source_manager on transactions ──
        # source_manager drifts across years (name changes). Resolve via
        # franchise_id lookup, then overwrite source_manager with the canonical name.
        if self._table_exists("transactions"):
            trans_cols = self._get_table_columns("transactions")
            trans_table = self._qualified_name("transactions")

            if "source_franchise_id" in trans_cols and "source_manager" in trans_cols:
                # Step 1: Populate source_franchise_id from stable source identifiers when missing.
                has_src_guid = "source_manager_guid" in trans_cols and "manager_guid" in matchup_cols
                has_src_team_name = "source_team_name" in trans_cols and "team_name" in matchup_cols
                needs_source_backfill = invalid_franchise_expr("t", "source_franchise_id")
                if "source_manager_guid" in trans_cols:
                    needs_source_backfill = (
                        "("
                        f"{invalid_franchise_expr('t', 'source_franchise_id')} OR ("
                        "NULLIF(TRIM(COALESCE(t.source_manager_guid, '')), '') IS NOT NULL "
                        "AND TRIM(COALESCE(t.source_franchise_id, '')) = TRIM(COALESCE(t.source_manager_guid, '')) "
                        "AND TRIM(COALESCE(lkp.franchise_id, '')) != TRIM(COALESCE(t.source_franchise_id, ''))"
                        "))"
                    )

                if has_src_team_name:
                    total += self._execute(
                        f"""
                        UPDATE {trans_table} t
                        SET source_franchise_id = lkp.franchise_id
                        FROM (SELECT DISTINCT team_name, year, franchise_id FROM {matchup_table}
                              WHERE {self._db_filter()}
                                AND franchise_id IS NOT NULL
                                AND NULLIF(TRIM(team_name), '') IS NOT NULL) lkp
                        WHERE LOWER(TRIM(COALESCE(t.source_team_name, ''))) = LOWER(TRIM(COALESCE(lkp.team_name, '')))
                          AND t.year = lkp.year
                          AND {needs_source_backfill}
                          AND t.source_team_name IS NOT NULL AND TRIM(t.source_team_name) != ''
                          AND {self._db_filter("t")}
                    """,
                        "populate_franchise_id: source_franchise_id pass 1 (source_team_name)",
                    )

                if has_src_guid:
                    total += self._execute(
                        f"""
                        UPDATE {trans_table} t
                        SET source_franchise_id = lkp.franchise_id
                        FROM (
                              SELECT manager_guid, year, MIN(franchise_id) AS franchise_id
                              FROM {matchup_table}
                              WHERE {self._db_filter()}
                                AND franchise_id IS NOT NULL
                                AND NULLIF(TRIM(manager_guid), '') IS NOT NULL
                              GROUP BY manager_guid, year
                              HAVING COUNT(DISTINCT franchise_id) = 1
                        ) lkp
                        WHERE NULLIF(TRIM(t.source_manager_guid), '') IS NOT NULL
                          AND TRIM(t.source_manager_guid) = TRIM(lkp.manager_guid)
                          AND t.year = lkp.year
                       AND {needs_source_backfill}
                       AND {self._db_filter("t")}
                 """,
                        "populate_franchise_id: source_franchise_id pass 2 (source_manager_guid unambiguous)",
                    )

                # Step 2: Normalize source_manager to the canonical name for that franchise_id.
                # Use the most recent manager name from matchup for each franchise_id.
                total += self._execute(
                    f"""
                    UPDATE {trans_table} t
                    SET source_manager = canon.manager
                    FROM (
                        SELECT franchise_id,
                               FIRST_VALUE(manager) OVER (
                                   PARTITION BY franchise_id
                                   ORDER BY year DESC, week DESC
                               ) AS manager
                        FROM {matchup_table}
                        WHERE {self._db_filter()}
                          AND franchise_id IS NOT NULL AND manager IS NOT NULL
                        QUALIFY ROW_NUMBER() OVER (PARTITION BY franchise_id ORDER BY year DESC, week DESC) = 1
                    ) canon
                    WHERE t.source_franchise_id IS NOT NULL
                      AND t.source_franchise_id = canon.franchise_id
                      AND LOWER(TRIM(t.source_manager)) != LOWER(TRIM(canon.manager))
                      AND {self._db_filter("t")}
                """,
                    "populate_franchise_id: normalize source_manager to canonical name",
                )

        # Some rows only acquired a usable franchise_id in the roster-backed
        # passes above. Reapply the persisted choices so those new seasonal IDs
        # collapse into the user's canonical franchise before aggregation.
        total += self._apply_saved_franchise_merges()
        total += self._apply_saved_manager_name_overrides()
        return total

    def ensure_missing_player_stubs(self) -> int:
        """Create player_fantasy STUB rows only after real roster identity is resolved.

        Hidden/private ancient leagues can have matchup rows with no roster rows
        for a manager-week. We keep one STUB row for those cases so team-level
        analytics still have a roster-side identity. This must run after
        populate_franchise_id: early stubs can become redundant once real roster
        rows are linked by team_key/franchise_id.
        """
        if not self._table_exists("player_fantasy") or not self._table_exists("matchup"):
            return 0

        pf_cols = self._get_table_columns("player_fantasy")
        matchup_cols = self._get_table_columns("matchup")
        required_pf = {"year", "week", "franchise_id", "position"}
        required_matchup = {"year", "week", "franchise_id"}
        if not required_pf.issubset(pf_cols) or not required_matchup.issubset(matchup_cols):
            return 0

        pf_t = self._qualified_name("player_fantasy")
        matchup_t = self._qualified_name("matchup")
        real_rostered_filter = rostered_filter_sql("real") if "manager" in pf_cols else "1 = 1"

        total = 0
        total += self._execute(
            f"""
            DELETE FROM {pf_t} stub
            WHERE {self._db_filter("stub")}
              AND COALESCE(stub.position, '') = 'STUB'
              AND stub.franchise_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM {pf_t} real
                  WHERE {self._db_filter("real")}
                    AND real.year = stub.year
                    AND real.week = stub.week
                    AND real.franchise_id = stub.franchise_id
                    AND COALESCE(real.position, '') != 'STUB'
                    AND {real_rostered_filter}
              )
            """,
            "ensure_missing_player_stubs: prune redundant stubs",
        )

        column_exprs = {
            "db_name": f"'{self.db_name}'",
            "year": "m.year",
            "week": "m.week",
            "cumulative_week": "(CAST(m.year AS BIGINT) * 100 + CAST(m.week AS BIGINT))",
            "manager_week": "m.franchise_id || '_' || CAST(m.year AS VARCHAR) || '_' || CAST(m.week AS VARCHAR)",
            "manager": "m.manager" if "manager" in matchup_cols else "NULL",
            "manager_guid": "m.manager_guid" if "manager_guid" in matchup_cols else "NULL",
            "franchise_id": "m.franchise_id",
            "team_key": "m.team_key" if "team_key" in matchup_cols else "NULL",
            "team_name": "m.team_name" if "team_name" in matchup_cols else "NULL",
            "platform": "m.platform" if "platform" in matchup_cols else "NULL",
            "league_id": "m.league_id" if "league_id" in matchup_cols else "NULL",
            "fantasy_points": "0.0",
            "points": "0.0",
            "is_started": "0",
            "is_rostered": "1",
            "position": "'STUB'",
            "fantasy_position": "'BN'",
            "team_points": "m.team_points" if "team_points" in matchup_cols else "NULL",
            "opponent": "m.opponent" if "opponent" in matchup_cols else "NULL",
            "opponent_points": "m.opponent_points" if "opponent_points" in matchup_cols else "NULL",
            "matchup_name": "m.matchup_name" if "matchup_name" in matchup_cols else "NULL",
            "win": "m.win" if "win" in matchup_cols else "NULL",
            "loss": "m.loss" if "loss" in matchup_cols else "NULL",
            "margin": "m.margin" if "margin" in matchup_cols else "NULL",
            "is_playoffs": "m.is_playoffs" if "is_playoffs" in matchup_cols else "NULL",
            "is_consolation": "m.is_consolation" if "is_consolation" in matchup_cols else "NULL",
        }
        insert_cols = [col for col in column_exprs if col in pf_cols]
        if not insert_cols:
            return total

        quoted_cols = ", ".join(f'"{col}"' for col in insert_cols)
        select_exprs = ", ".join(f'{column_exprs[col]} AS "{col}"' for col in insert_cols)
        total += self._execute(
            f"""
            INSERT INTO {pf_t} ({quoted_cols})
            SELECT DISTINCT {select_exprs}
            FROM {matchup_t} m
            WHERE {self._db_filter("m")}
              AND m.franchise_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM {pf_t} real
                  WHERE {self._db_filter("real")}
                    AND real.year = m.year
                    AND real.week = m.week
                    AND real.franchise_id = m.franchise_id
                    AND COALESCE(real.position, '') != 'STUB'
                    AND {real_rostered_filter}
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM {pf_t} existing_stub
                  WHERE {self._db_filter("existing_stub")}
                    AND existing_stub.year = m.year
                    AND existing_stub.week = m.week
                    AND existing_stub.franchise_id = m.franchise_id
                    AND COALESCE(existing_stub.position, '') = 'STUB'
              )
            """,
            "ensure_missing_player_stubs: create missing stubs",
        )

        return total

    def repair_matchup_symmetry(self) -> int:
        """Synthesize missing reciprocal matchup rows for departed managers.

        When the Sleeper API returns matchup data for active managers who played
        against departed/inactive managers, the departed manager's row is missing.
        This detects asymmetric pairs (A vs B exists but B vs A doesn't) and
        creates the missing row by swapping franchise_id/opponent_franchise_id
        and team_points/opponent_points.

        Must run AFTER populate_franchise_id (needs franchise_id and
        opponent_franchise_id populated).
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_cols = self._get_table_columns("matchup")
        matchup_t = self._qualified_name("matchup")

        if "franchise_id" not in matchup_cols or "opponent_franchise_id" not in matchup_cols:
            return 0

        # Detect asymmetric pairs: A has a row vs B, but B has no row vs A.
        # Only consider rows where both franchise_id and opponent_franchise_id
        # are populated, and exclude bye weeks.
        # Build column list for INSERT — mirror all available columns with
        # appropriate swaps.
        swap_map = {
            "franchise_id": "a.opponent_franchise_id",
            "opponent_franchise_id": "a.franchise_id",
            "manager": "a.opponent",
            "opponent": "a.manager",
            "manager_guid": "a.opponent_guid",
            "opponent_guid": "a.manager_guid",
            "team_name": "NULL",  # unknown for departed manager
            "team_key": "NULL",
            "opponent_team_key": "a.team_key",
            "team_points": "a.opponent_points",
            "opponent_points": "a.team_points",
            "margin": "CAST(-1 * CAST(a.margin AS DOUBLE) AS DOUBLE)",
            "win": "a.loss",
            "loss": "a.win",
            "tie": "a.tie",
            "franchise_name": "a.opponent",
            # cumulative_week is deterministic from year/week and is required by
            # pre-upload validation. ensure_manager_week (next enrichment) only
            # backfills manager_week, not cumulative_week.
            "cumulative_week": "(CAST(a.year AS BIGINT) * 100 + CAST(a.week AS BIGINT))",
        }
        # Columns to copy as-is from the existing row
        copy_cols = {
            "year",
            "week",
            "matchup_id",
            "is_playoffs",
            "is_consolation",
            "championship",
            "total_matchup_score",
            "close_margin",
            "db_name",
            "platform",
            "league_id",
            "is_bye_week",
        }

        select_parts = []
        insert_cols = []
        for col in matchup_cols:
            if col in swap_map:
                select_parts.append(f"{swap_map[col]} AS {col}")
                insert_cols.append(col)
            elif col in copy_cols:
                select_parts.append(f"a.{col}")
                insert_cols.append(col)
            # Skip other columns — they'll be populated by downstream enrichments

        if not insert_cols:
            return 0

        authoritative_pair_guard = ""
        if "opponent_team_key" in matchup_cols:
            authoritative_pair_guard = """
              AND NULLIF(TRIM(COALESCE(CAST(a.opponent_team_key AS VARCHAR), '')), '') IS NULL
            """

        conn = self._get_connection()
        before_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {matchup_t} WHERE {self._db_filter()}").fetchone()[0] or 0
        )

        sql = f"""
            INSERT INTO {matchup_t} ({", ".join(insert_cols)})
            SELECT {", ".join(select_parts)}
            FROM {matchup_t} a
            WHERE {self._db_filter("a")}
              AND a.franchise_id IS NOT NULL
              AND a.opponent_franchise_id IS NOT NULL
              AND COALESCE(a.is_bye_week, 0) = 0
              AND a.opponent IS NOT NULL AND TRIM(a.opponent) != ''
              {authoritative_pair_guard}
              AND NOT EXISTS (
                  SELECT 1 FROM {matchup_t} b
                  WHERE b.year = a.year AND b.week = a.week
                    AND {self._db_filter("b")}
                    AND b.franchise_id = a.opponent_franchise_id
                    AND b.opponent_franchise_id = a.franchise_id
                  )
        """
        self._execute(sql, "repair_matchup_symmetry: synthesize missing reciprocal rows")
        after_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {matchup_t} WHERE {self._db_filter()}").fetchone()[0] or 0
        )
        total = max(0, after_count - before_count)
        if total:
            logger.info(f"[repair_matchup_symmetry] synthesized {total} missing reciprocal matchup rows")
        return total

    def matchup_to_player(self) -> int:
        """Join matchup data to player_fantasy table.

        Adds matchup context (win/loss, opponent, points) to each player row.
        Join key: (franchise_id, year, week) - preferred
        Fallback: (manager, year, week) if franchise_id not populated

        Replaces: matchup_to_player_v2.py
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[matchup_to_player] player_fantasy table not found")
            return 0

        if not self._table_exists("matchup"):
            logger.warning("[matchup_to_player] matchup table not found")
            return 0

        # Get available columns in both tables
        player_cols = self._get_table_columns("player_fantasy")
        matchup_cols = self._get_table_columns("matchup")

        # Ensure desired columns exist in player_fantasy (DuckDB UPDATE can't add columns)
        columns_to_ensure_player = {
            "win": "INTEGER",
            "loss": "INTEGER",
            "opponent": "VARCHAR",
            "team_points": "DOUBLE",
            "opponent_points": "DOUBLE",
            "margin": "DOUBLE",
            "is_bye_week": "INTEGER",
            "final_playoff_seed": "INTEGER",
            "playoff_seed": "INTEGER",
            "championship": "INTEGER",
            "is_championship": "INTEGER",
            "champion": "INTEGER",
            "sacko": "INTEGER",
            "playoff_round": "VARCHAR",
            "consolation_round": "VARCHAR",
            "is_playoffs": "INTEGER",
            "is_consolation": "INTEGER",
        }
        player_table = self._qualified_name("player_fantasy")
        matchup_table = self._qualified_name("matchup")
        conn = self._get_connection()

        player_cols = self._ensure_columns(player_table, columns_to_ensure_player, player_cols)

        # Ensure key matchup columns exist on the matchup table itself
        columns_to_ensure_matchup = {
            "final_playoff_seed": "INTEGER",
            "playoff_seed": "INTEGER",
            "champion": "INTEGER",
            "sacko": "INTEGER",
            "is_championship": "INTEGER",
            "playoff_round": "VARCHAR",
            "consolation_round": "VARCHAR",
        }

        matchup_cols = self._ensure_columns(matchup_table, columns_to_ensure_matchup, matchup_cols)

        # Core matchup columns to import (only if they exist in BOTH tables)
        desired_cols = [
            "win",
            "loss",
            "team_points",
            "opponent",
            "opponent_points",
            "margin",
            "is_bye_week",
            "is_playoffs",
            "is_consolation",
            "weekly_rank",
            "teams_beat_this_week",
            "above_league_median",
            "final_playoff_seed",
            "playoff_seed",
            "championship",
            "is_championship",
            "champion",
            "sacko",
            "playoff_round",
            "consolation_round",
        ]

        # Build SET clause for columns that exist in BOTH player_fantasy AND matchup
        # DuckDB UPDATE requires the target column to exist (can't add columns via UPDATE)
        set_clauses = []
        for col in desired_cols:
            if col in matchup_cols and col in player_cols:
                set_clauses.append(f"{col} = m.{col}")

        if not set_clauses:
            logger.warning("[matchup_to_player] No matching columns found in matchup table")
            return 0

        # Check if franchise_id is available in both tables
        use_franchise_id = "franchise_id" in player_cols and "franchise_id" in matchup_cols

        if use_franchise_id:
            # Prefer franchise_id - stable identifier across years/name changes
            # Also sync manager name from matchup (source of truth)
            if "manager" in player_cols and "manager" in matchup_cols:
                set_clauses.append("manager = m.manager")

            sql = f"""
                UPDATE {player_table} p
                SET {", ".join(set_clauses)}
                FROM {matchup_table} m
                WHERE p.franchise_id = m.franchise_id
                  AND p.year = m.year
                  AND p.week = m.week
                  AND p.franchise_id IS NOT NULL
                  AND {rostered_filter_sql("p")}
                  AND {self._db_filter("p")}
                  AND {self._db_filter("m")}
            """
            logger.info("[matchup_to_player] Using franchise_id for join (preferred)")
        else:
            logger.warning("[matchup_to_player] franchise_id not available, skipping unsafe manager-name join")
            return 0

        return self._execute(sql, "matchup_to_player: Join matchup context to player")

    def player_to_matchup(self) -> int:
        """Aggregate player stats to matchup table.

        Adds optimal lineup, bench points, lineup efficiency to matchup.
        Join key: manager_week

        Replaces: player_to_matchup_v2.py
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[player_to_matchup] player_fantasy table not found")
            return 0

        if not self._table_exists("matchup"):
            logger.warning("[player_to_matchup] matchup table not found")
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        matchup_cols = self._get_table_columns("matchup")

        # Check for manager_week join key
        if "manager_week" not in player_cols or "manager_week" not in matchup_cols:
            logger.warning("[player_to_matchup] manager_week column not found in both tables")
            return 0

        # Add missing columns to matchup table
        # DuckDB UPDATE requires the target column to exist
        matchup_table = self._qualified_name("matchup")
        conn = self._get_connection()

        columns_to_add = {
            "optimal_points": "DOUBLE",
            "bench_points": "DOUBLE",
            "total_player_points": "DOUBLE",
            "starter_points": "DOUBLE",
            "players_rostered": "INTEGER",
            "players_started": "INTEGER",
            "manager_lamar": "DOUBLE",
            "lineup_efficiency": "DOUBLE",
        }

        matchup_cols = self._ensure_columns(matchup_table, columns_to_add, matchup_cols)

        # Build aggregation based on available columns
        agg_parts = []
        set_parts = []

        # Check which columns we can aggregate from player_fantasy and update in matchup
        if "fantasy_points" in player_cols and "total_player_points" in matchup_cols:
            agg_parts.append("SUM(fantasy_points) as total_player_points")
            set_parts.append("total_player_points = pa.total_player_points")

        if "is_rostered" in player_cols and "players_rostered" in matchup_cols:
            agg_parts.append("SUM(CASE WHEN CAST(is_rostered AS INTEGER) = 1 THEN 1 ELSE 0 END) as players_rostered")
            set_parts.append("players_rostered = pa.players_rostered")

        if "is_started" in player_cols and "players_started" in matchup_cols:
            agg_parts.append("SUM(CASE WHEN CAST(is_started AS INTEGER) = 1 THEN 1 ELSE 0 END) as players_started")
            set_parts.append("players_started = pa.players_started")

        # Optimal points: always compute fresh from optimal_player flag + fantasy_points
        # to avoid stale pre-computed values (e.g., when points were 0 before sync)
        if "optimal_points" in matchup_cols:
            # optimal_player is set by compute_manager_optimal() (per-manager best lineup)
            if "optimal_player" in player_cols:
                agg_parts.append(
                    "SUM(CASE WHEN CAST(COALESCE(optimal_player, 0) AS INTEGER) = 1 THEN fantasy_points ELSE 0.0 END) as optimal_points"
                )
                set_parts.append("optimal_points = pa.optimal_points")
            elif "optimal_points" in player_cols:
                agg_parts.append("MAX(optimal_points) as optimal_points")
                set_parts.append("optimal_points = pa.optimal_points")

        # Starter points: sum of fantasy_points for players with is_started=1.
        # This is the internal-data-consistent version of team_points — it
        # equals team_points when the fetcher returned all starter rows, and
        # diverges when upstream data is incomplete (e.g. ESPN API missing
        # specific player-weeks). optimal_points invariant checks compare
        # against this, not team_points, so an upstream data gap doesn't fire
        # an optimal_lineup validator failure.
        if "is_started" in player_cols and "fantasy_points" in player_cols and "starter_points" in matchup_cols:
            agg_parts.append(
                "SUM(CASE WHEN CAST(is_started AS INTEGER) = 1 THEN fantasy_points ELSE 0.0 END) as starter_points"
            )
            set_parts.append("starter_points = pa.starter_points")

        # Bench points: sum of points from rostered but not started players
        if "is_rostered" in player_cols and "is_started" in player_cols and "bench_points" in matchup_cols:
            agg_parts.append(
                "SUM(CASE WHEN CAST(is_rostered AS INTEGER) = 1 AND (CAST(is_started AS INTEGER) = 0 OR is_started IS NULL) THEN fantasy_points ELSE 0.0 END) as bench_points"
            )
            set_parts.append("bench_points = pa.bench_points")

        # Manager LAMAR: sum of manager_lamar for started players
        if "manager_lamar" in player_cols and "is_started" in player_cols and "manager_lamar" in matchup_cols:
            agg_parts.append(
                "SUM(CASE WHEN CAST(is_started AS INTEGER) = 1 THEN manager_lamar ELSE 0.0 END) as manager_lamar"
            )
            set_parts.append("manager_lamar = pa.manager_lamar")

        if not agg_parts:
            logger.warning("[player_to_matchup] No aggregatable columns found")
            return 0

        # Use schema-qualified table names (matchup_table already defined above)
        player_table = self._qualified_name("player_fantasy")

        sql = f"""
            WITH player_agg AS (
                SELECT
                    manager_week,
                    {", ".join(agg_parts)}
                FROM {player_table} p
                WHERE manager_week IS NOT NULL
                  AND {self._db_filter("p")}
                GROUP BY manager_week
            )
            UPDATE {matchup_table} m
            SET {", ".join(set_parts)}
            FROM player_agg pa
            WHERE m.manager_week = pa.manager_week
              AND {self._db_filter("m")}
        """

        total = self._execute(sql, "player_to_matchup: Aggregate player stats")

        # Multi-week playoffs: if matchup rows only keep the first week but scores
        # are cumulative, recompute matchup aggregates across the full window.
        total += self._apply_multiweek_matchup_aggregates(matchup_cols, player_cols)

        # Lineup efficiency: team_points / optimal_points (computed after optimal_points is populated)
        if "lineup_efficiency" in matchup_cols and "optimal_points" in matchup_cols and "team_points" in matchup_cols:
            efficiency_sql = f"""
                UPDATE {matchup_table}
                SET lineup_efficiency = CASE
                    WHEN optimal_points > 0 THEN ROUND(team_points / optimal_points, 4)
                    ELSE NULL
                END
                WHERE optimal_points IS NOT NULL
                  AND {self._db_filter()}
            """
            total += self._execute(efficiency_sql, "player_to_matchup: Calculate lineup_efficiency")

        return total

    def _apply_multiweek_matchup_aggregates(self, matchup_cols: set[str], player_cols: set[str]) -> int:
        """Adjust matchup aggregates for 2-week playoff rounds when only one week is stored.

        Some platforms (notably ESPN) expose cumulative scores across 2-week
        playoff matchups, and the fetcher de-dupes the continuation week. In
        that case, team_points is already the 2-week total while optimal_points
        (and other aggregates) are still single-week, which makes optimal_points
        < team_points and inflates lineup_efficiency.
        """
        if not self._table_exists("league_settings"):
            return 0

        # Require base columns
        if "year" not in matchup_cols or "week" not in matchup_cols:
            return 0
        if "fantasy_points" not in player_cols:
            return 0

        if "franchise_id" not in matchup_cols or "franchise_id" not in player_cols:
            logger.warning("[multiweek_matchup_aggregates] franchise_id required; skipping")
            return 0
        id_col = "franchise_id"

        # Build aggregate expressions for columns that exist
        agg_parts: dict[str, str] = {}
        if "optimal_points" in matchup_cols:
            if "optimal_player" in player_cols:
                agg_parts["optimal_points"] = (
                    "SUM(CASE WHEN CAST(COALESCE(optimal_player, 0) AS INTEGER) = 1 THEN fantasy_points ELSE 0.0 END)"
                )
            elif "optimal_points" in player_cols:
                agg_parts["optimal_points"] = "SUM(optimal_points)"

        if (
            "bench_points" in matchup_cols
            and "is_rostered" in player_cols
            and "is_started" in player_cols
            and "fantasy_points" in player_cols
        ):
            agg_parts["bench_points"] = (
                "SUM(CASE WHEN CAST(is_rostered AS INTEGER) = 1 "
                "AND (CAST(is_started AS INTEGER) = 0 OR is_started IS NULL) "
                "THEN fantasy_points ELSE 0.0 END)"
            )

        if "total_player_points" in matchup_cols and "fantasy_points" in player_cols:
            agg_parts["total_player_points"] = "SUM(fantasy_points)"

        if "players_rostered" in matchup_cols and "is_rostered" in player_cols:
            agg_parts["players_rostered"] = "SUM(CASE WHEN CAST(is_rostered AS INTEGER) = 1 THEN 1 ELSE 0 END)"

        if "players_started" in matchup_cols and "is_started" in player_cols:
            agg_parts["players_started"] = "SUM(CASE WHEN CAST(is_started AS INTEGER) = 1 THEN 1 ELSE 0 END)"

        if "manager_lamar" in matchup_cols and "manager_lamar" in player_cols and "is_started" in player_cols:
            agg_parts["manager_lamar"] = (
                "SUM(CASE WHEN CAST(is_started AS INTEGER) = 1 THEN manager_lamar ELSE 0.0 END)"
            )

        if not agg_parts:
            return 0

        matchup_table = self._qualified_name("matchup")
        player_table = self._qualified_name("player_fantasy")
        settings_table = self._qualified_name("league_settings")
        conn = self._get_connection()

        # Pull years with multi-week playoff settings. The canonical flat
        # league_settings DDL exposes playoff_teams / has_multiweek_championship /
        # sleeper_playoff_type but NOT a platform-agnostic playoff_round_type —
        # the prior SELECT referenced playoff_round_type and num_playoff_teams,
        # both of which don't exist as columns, so the try/except swallowed the
        # binder error and the whole multiweek aggregation function returned 0
        # for every league. Derive the round-type and team count from the
        # columns that actually exist.
        try:
            rows = conn.execute(
                f"""
                SELECT year,
                       playoff_start_week,
                       has_multiweek_championship,
                       sleeper_playoff_type,
                       playoff_teams,
                       bye_teams
                FROM {settings_table}
                WHERE {self._db_filter()}
                """
            ).fetchall()
            col_names = [d[0] for d in conn.description]
        except Exception:
            return 0

        if not rows:
            return 0

        from multi_league.transformations.matchup.modules.playoff_bracket.utils import (  # noqa: PLC0415
            get_expected_playoff_rounds,
        )

        total = 0
        for row in rows:
            s = dict(zip(col_names, row))
            try:
                year = int(s.get("year"))
            except (TypeError, ValueError):
                continue

            playoff_start = s.get("playoff_start_week")
            if playoff_start is None:
                continue
            try:
                playoff_start = int(playoff_start)
            except (TypeError, ValueError):
                continue

            num_playoff_teams = s.get("playoff_teams")
            if num_playoff_teams is None:
                continue
            try:
                num_playoff_teams = int(num_playoff_teams)
            except (TypeError, ValueError):
                continue

            prt = self._resolve_playoff_round_type(s, playoff_teams=num_playoff_teams)

            if prt not in (1, 2):
                continue

            expected_rounds = get_expected_playoff_rounds({"num_playoff_teams": num_playoff_teams})

            # Build week windows per round
            week_cursor = playoff_start
            for r in range(1, expected_rounds + 1):
                if prt == 1:
                    span = 2
                elif prt == 2 and r == expected_rounds:
                    span = 2
                else:
                    span = 1

                if span == 2:
                    start_week = week_cursor
                    end_week = week_cursor + 1

                    select_parts = [f"{id_col} AS id_key", "year"]
                    select_parts += [f"{expr} AS {col}" for col, expr in agg_parts.items()]
                    set_parts = [f"{col} = mw.{col}" for col in agg_parts.keys()]

                    # Both real playoff rows (is_playoffs=1) AND consolation
                    # rows (is_consolation=1) need multiweek aggregation when
                    # the league runs multi-week rounds. The bracket stores
                    # the 2-week total as team_points on the first week row,
                    # regardless of whether the game was in the main or
                    # consolation bracket.
                    if "is_consolation" in matchup_cols:
                        postseason_filter = (
                            "(COALESCE(CAST(m.is_playoffs AS INT), 0) = 1 "
                            "OR COALESCE(CAST(m.is_consolation AS INT), 0) = 1)"
                        )
                    else:
                        postseason_filter = "COALESCE(CAST(m.is_playoffs AS INT), 0) = 1"

                    sql = f"""
                        WITH multiweek_agg AS (
                            SELECT
                                {", ".join(select_parts)}
                            FROM {player_table} p
                            WHERE p.year = {year}
                              AND p.week IN ({start_week}, {end_week})
                              AND {self._db_filter("p")}
                            GROUP BY {id_col}, year
                        ),
                        missing_pair AS (
                            SELECT m.{id_col} AS id_key, m.year
                            FROM {matchup_table} m
                            WHERE m.year = {year}
                              AND m.week = {start_week}
                              AND {postseason_filter}
                              AND m.team_points IS NOT NULL
                              AND m.{id_col} IS NOT NULL
                              AND {self._db_filter("m")}
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM {matchup_table} m2
                                  WHERE m2.year = m.year
                                    AND m2.week = {end_week}
                                    AND m2.{id_col} = m.{id_col}
                                    AND (COALESCE(CAST(m2.is_playoffs AS INT), 0) = 1
                                         OR COALESCE(CAST(m2.is_consolation AS INT), 0) = 1)
                                    AND COALESCE(CAST(m2.is_bye_week AS INT), 0) = 0
                                    AND m2.team_points IS NOT NULL
                                    AND {self._db_filter("m2")}
                              )
                        )
                        UPDATE {matchup_table} m
                        SET {", ".join(set_parts)}
                        FROM multiweek_agg mw
                        JOIN missing_pair mp
                          ON mp.id_key = mw.id_key AND mp.year = mw.year
                        WHERE m.year = mw.year
                          AND m.week = {start_week}
                          AND m.{id_col} = mw.id_key
                          AND {self._db_filter("m")}
                    """

                    total += self._execute(
                        sql,
                        f"player_to_matchup: multiweek aggregates (year={year}, weeks={start_week}-{end_week})",
                    )

                week_cursor += span

        return total

    def _freeze_final_playoff_seeds(self, matchup_df: pd.DataFrame, settings_df: pd.DataFrame | None) -> pd.DataFrame:
        """Freeze each team's end-of-regular-season seed onto all rows for that year.

        The quick-import path needs `final_playoff_seed` before the playoff odds worker
        runs so bracket shape and champion detection can work from matchup enrichment
        alone. We prefer the already-computed `playoff_seed_to_date` from the last
        regular-season week when available, and fall back to a standings recomputation
        from regular-season rows.
        """
        if matchup_df.empty or "year" not in matchup_df.columns or "manager" not in matchup_df.columns:
            return matchup_df

        df = matchup_df.copy()
        if "final_playoff_seed" not in df.columns:
            df["final_playoff_seed"] = pd.Series([None] * len(df), dtype="Int64")
        else:
            df["final_playoff_seed"] = pd.to_numeric(df["final_playoff_seed"], errors="coerce").astype("Int64")

        if settings_df is None or settings_df.empty or "year" not in settings_df.columns:
            raise ValueError(
                "_freeze_final_playoff_seeds requires canonical league_settings rows with playoff_start_week"
            )

        settings_by_year: dict[int, int] = {}
        for _, row in settings_df.iterrows():
            try:
                year = int(row["year"])
            except (TypeError, ValueError):
                continue
            playoff_start_raw = row.get("playoff_start_week")
            if pd.isna(playoff_start_raw):
                raise ValueError(f"Missing playoff_start_week in league_settings for year {year}")
            try:
                playoff_start = int(playoff_start_raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Invalid playoff_start_week in league_settings for year {year}: {playoff_start_raw!r}"
                ) from exc
            settings_by_year[year] = playoff_start

        valid_opponent = (
            df["opponent"].notna()
            & (df["opponent"].astype(str).str.strip() != "")
            & (df["opponent"].astype(str).str.lower() != "none")
        )

        for year_val in sorted(pd.Series(df["year"]).dropna().unique()):
            try:
                year = int(year_val)
            except (TypeError, ValueError):
                continue

            if year not in settings_by_year:
                raise ValueError(f"Missing league_settings row for year {year} while freezing final playoff seeds")
            playoff_start = settings_by_year[year]
            last_regular_week = playoff_start - 1
            year_mask = df["year"] == year

            # Use franchise_id when available for stable identity tracking
            _id_col = get_manager_col(df)

            seeds_map: dict[str, int] = {}

            if "playoff_seed_to_date" in df.columns:
                seed_cols = [_id_col, "playoff_seed_to_date"]
                if _id_col != "manager":
                    seed_cols.append("manager")
                frozen = (
                    df.loc[
                        year_mask
                        & (pd.to_numeric(df["week"], errors="coerce") == last_regular_week)
                        & df[_id_col].notna()
                        & pd.to_numeric(df["playoff_seed_to_date"], errors="coerce").notna(),
                        seed_cols,
                    ]
                    .copy()
                    .drop_duplicates(subset=[_id_col], keep="last")
                )
                if not frozen.empty:
                    frozen["playoff_seed_to_date"] = pd.to_numeric(
                        frozen["playoff_seed_to_date"], errors="coerce"
                    ).astype("Int64")
                    seeds_map = {
                        str(row[_id_col]): int(row["playoff_seed_to_date"])
                        for _, row in frozen.iterrows()
                        if pd.notna(row[_id_col]) and pd.notna(row["playoff_seed_to_date"])
                    }

            if not seeds_map:
                select_cols = [_id_col, "win", "loss", "tie", "team_points"]
                if _id_col != "manager" and "manager" not in select_cols:
                    select_cols.append("manager")
                if "above_league_median" in df.columns:
                    select_cols.append("above_league_median")
                reg = df.loc[
                    year_mask & (pd.to_numeric(df["week"], errors="coerce") < playoff_start) & valid_opponent,
                    select_cols,
                ].copy()
                if reg.empty:
                    continue

                for col in ["win", "loss", "tie", "team_points"]:
                    if col not in reg.columns:
                        reg[col] = 0
                    reg[col] = pd.to_numeric(reg[col], errors="coerce").fillna(0)

                # H2H+Median: add median wins/losses for years with uses_median
                year_uses_median = False
                if settings_df is not None and "uses_median" in settings_df.columns:
                    yr_row = settings_df.loc[settings_df["year"] == year]
                    if not yr_row.empty:
                        year_uses_median = bool(yr_row.iloc[0].get("uses_median", False))

                if year_uses_median and "above_league_median" in reg.columns:
                    reg["above_league_median"] = pd.to_numeric(reg["above_league_median"], errors="coerce").fillna(0)
                    reg["win"] = reg["win"] + reg["above_league_median"]
                    reg["loss"] = reg["loss"] + (1 - reg["above_league_median"]).clip(lower=0)

                _id_col = get_manager_col(reg)
                standings = (
                    reg.groupby(_id_col, dropna=True)
                    .agg(
                        wins=("win", "sum"),
                        ties=("tie", "sum"),
                        points=("team_points", "sum"),
                    )
                    .reset_index()
                    .sort_values(["wins", "ties", "points", _id_col], ascending=[False, False, False, True])
                    .reset_index(drop=True)
                )
                standings["seed"] = standings.index + 1
                seeds_map = {
                    str(row[_id_col]): int(row["seed"]) for _, row in standings.iterrows() if pd.notna(row[_id_col])
                }

            if not seeds_map:
                continue

            seed_series = df.loc[year_mask, _id_col].map(seeds_map)
            seed_series = pd.to_numeric(seed_series, errors="coerce").astype("Int64")
            # Computed seeds (from wins_to_date which includes median) ALWAYS
            # override API-sourced seeds which may use H2H-only standings.
            existing = pd.to_numeric(df.loc[year_mask, "final_playoff_seed"], errors="coerce").astype("Int64")
            df.loc[year_mask, "final_playoff_seed"] = seed_series.combine_first(existing).astype("Int64")

            if "playoff_seed" in df.columns:
                current_playoff_seed = pd.to_numeric(df.loc[year_mask, "playoff_seed"], errors="coerce").astype("Int64")
                df.loc[year_mask, "playoff_seed"] = seed_series.combine_first(current_playoff_seed).astype("Int64")

        return df

    def _get_playoff_settings_by_year(
        self, matchup_df: pd.DataFrame, settings_df: pd.DataFrame | None
    ) -> dict[int, dict[str, int]]:
        """Build a normalized per-year playoff config from flat league_settings."""
        years = sorted(int(y) for y in pd.Series(matchup_df["year"]).dropna().unique())
        settings_by_year: dict[int, dict[str, int]] = {}

        settings_lookup: dict[int, dict[str, object]] = {}
        if settings_df is not None and not settings_df.empty and "year" in settings_df.columns:
            for _, row in settings_df.iterrows():
                try:
                    settings_lookup[int(row["year"])] = row.to_dict()
                except (TypeError, ValueError):
                    continue

        for year in years:
            row = settings_lookup.get(year)
            if not row:
                raise ValueError(f"Missing league_settings row for year {year}")
            playoff_teams_raw = row.get("num_playoff_teams", row.get("playoff_teams"))
            try:
                if pd.isna(playoff_teams_raw):
                    raise ValueError
                playoff_teams = int(playoff_teams_raw)
            except (TypeError, ValueError) as err:
                raise ValueError(f"Missing or invalid num_playoff_teams in league_settings for year {year}") from err

            playoff_start_raw = row.get("playoff_start_week")
            try:
                if pd.isna(playoff_start_raw):
                    raise ValueError
                playoff_start = int(playoff_start_raw)
            except (TypeError, ValueError) as err:
                raise ValueError(f"Missing or invalid playoff_start_week in league_settings for year {year}") from err

            bye_raw = row.get("bye_teams")
            try:
                if pd.isna(bye_raw):
                    raise ValueError
                bye_teams = int(bye_raw)
            except (TypeError, ValueError) as err:
                raise ValueError(f"Missing or invalid bye_teams in league_settings for year {year}") from err

            round_type = self._resolve_playoff_round_type(row, playoff_teams=playoff_teams)

            end_week_raw = row.get("end_week")
            try:
                end_week = int(end_week_raw) if pd.notna(end_week_raw) else None
            except (TypeError, ValueError):
                end_week = None

            uses_reseeding_raw = row.get("uses_playoff_reseeding")
            uses_reseeding = bool(uses_reseeding_raw) if pd.notna(uses_reseeding_raw) else False

            num_teams_raw = row.get("num_teams")
            try:
                num_teams = int(num_teams_raw) if pd.notna(num_teams_raw) else 12
            except (TypeError, ValueError):
                num_teams = 12

            settings_by_year[year] = {
                "playoff_start_week": playoff_start,
                "num_playoff_teams": playoff_teams,
                "bye_teams": bye_teams,
                "playoff_round_type": round_type,
                "end_week": end_week,
                "uses_playoff_reseeding": uses_reseeding,
                "num_teams": num_teams,
            }

        return settings_by_year

    def _classify_postseason_locally(
        self,
        matchup_df: pd.DataFrame,
        settings_by_year: dict[int, dict[str, int]],
    ) -> pd.DataFrame:
        """Classify championship vs consolation rows from local settings and frozen seeds."""
        from multi_league.transformations.matchup.modules.playoff_flags import (  # noqa: PLC0415
            _repair_alive_pairings,
            _repair_bracket_pairings,
        )

        df = matchup_df.copy()

        def _series_or_default(column_name: str, default_value: int = 0) -> pd.Series:
            if column_name in df.columns:
                series = df[column_name]
                if pd.api.types.is_bool_dtype(series):
                    return series.astype("Int64").fillna(default_value)
                return pd.to_numeric(series, errors="coerce").astype("Float64").fillna(default_value)
            return pd.Series([default_value] * len(df), index=df.index, dtype="float64")

        original_is_playoffs = _series_or_default("is_playoffs").astype(int)
        original_is_consolation = _series_or_default("is_consolation").astype(int)
        df["is_playoffs"] = original_is_playoffs
        df["is_consolation"] = original_is_consolation

        for year, year_settings in settings_by_year.items():
            year_mask = df["year"] == year
            if not year_mask.any():
                continue

            playoff_start = int(year_settings["playoff_start_week"])
            num_playoff = int(year_settings["num_playoff_teams"])
            bye_teams = int(year_settings["bye_teams"])
            postseason_mask = year_mask & (pd.to_numeric(df["week"], errors="coerce") >= playoff_start)
            if not postseason_mask.any():
                continue

            postseason_rows = df.loc[postseason_mask]
            has_playoff_flags = (pd.to_numeric(postseason_rows["is_playoffs"], errors="coerce").fillna(0) == 1).any()
            has_consolation_flags = (
                pd.to_numeric(postseason_rows["is_consolation"], errors="coerce").fillna(0) == 1
            ).any()

            preserve_existing = False
            if has_playoff_flags:
                preserve_existing = True
                if bye_teams > 0 and "final_playoff_seed" in df.columns:
                    for seed in range(1, bye_teams + 1):
                        seed_rows = postseason_rows[
                            pd.to_numeric(postseason_rows["final_playoff_seed"], errors="coerce") == seed
                        ]
                        if seed_rows.empty:
                            continue
                        if (
                            not (pd.to_numeric(seed_rows["is_playoffs"], errors="coerce").fillna(0) == 1).any()
                            and (pd.to_numeric(seed_rows["is_consolation"], errors="coerce").fillna(0) == 1).any()
                        ):
                            preserve_existing = False
                            break
            elif has_consolation_flags and not has_playoff_flags:
                preserve_existing = False

            if preserve_existing:
                continue

            df.loc[postseason_mask, "is_playoffs"] = 0
            df.loc[postseason_mask, "is_consolation"] = 0

            manager_seeds: dict[str, int] = {}
            year_df = df.loc[year_mask].copy()
            if "final_playoff_seed" in year_df.columns:
                for fid in year_df["franchise_id"].dropna().unique():
                    fid_rows = year_df[year_df["franchise_id"] == fid]
                    seed_values = pd.to_numeric(fid_rows["final_playoff_seed"], errors="coerce").dropna()
                    if not seed_values.empty:
                        manager_seeds[str(fid)] = int(seed_values.iloc[0])

            playoff_qualifiers = {mgr for mgr, seed in manager_seeds.items() if seed <= num_playoff}
            alive_for_championship = set(playoff_qualifiers)
            if not alive_for_championship:
                continue

            df = _repair_bracket_pairings(df, year_mask, manager_seeds, num_playoff, bye_teams, playoff_start, year)

            playoff_weeks = sorted(pd.Series(df.loc[postseason_mask, "week"]).dropna().astype(int).unique())
            for week in playoff_weeks:
                week_mask = postseason_mask & (pd.to_numeric(df["week"], errors="coerce") == week)
                week_games = df.loc[week_mask].copy()
                if week_games.empty:
                    continue

                df = _repair_alive_pairings(df, year_mask, week, alive_for_championship, year)
                week_games = df.loc[week_mask].copy()

                championship_games_mask = (
                    week_mask
                    & df["franchise_id"].isin(alive_for_championship)
                    & df["opponent_franchise_id"].isin(alive_for_championship)
                )
                df.loc[championship_games_mask, "is_playoffs"] = 1

                bye_row_mask = (
                    df["opponent"].isna()
                    | (df["opponent"].astype(str).str.strip() == "")
                    | (df["opponent"].astype(str).str.lower() == "none")
                )
                non_championship_mask = week_mask & ~championship_games_mask & ~bye_row_mask
                df.loc[non_championship_mask, "is_consolation"] = 1

                championship_losers = week_games[
                    week_games["franchise_id"].isin(alive_for_championship)
                    & week_games["opponent_franchise_id"].isin(alive_for_championship)
                    & (pd.to_numeric(week_games["loss"], errors="coerce").fillna(0) == 1)
                ]["franchise_id"].dropna()
                alive_for_championship -= set(str(mgr) for mgr in championship_losers.tolist())

        df["postseason"] = (
            (pd.to_numeric(df["is_playoffs"], errors="coerce").fillna(0) == 1)
            | (pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0) == 1)
        ).astype(int)
        return df

    def _classify_via_bracket_tracer(
        self,
        matchup_df: pd.DataFrame,
        settings_by_year: dict[int, dict[str, int]],
        settings_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Classify playoff/consolation using bracket_tracer for champion/bracket detection.

        Yahoo's API already provides correct matchup pairings and scores — those are
        NEVER modified. The bracket_tracer traces the bracket forward from corrected
        seeds (which include median wins) to determine:
        - Which games are championship bracket vs consolation
        - Who the champion is (winner of the traced championship game)

        Opponents, scores, win/loss all come from Yahoo's API and are preserved.
        """
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_bracket,
        )

        df = matchup_df.copy()
        if "franchise_id" not in df.columns or "opponent_franchise_id" not in df.columns:
            logger.warning("[bracket_tracer] franchise_id + opponent_franchise_id required; skipping classification")
            return df

        # Initialize columns — coerce booleans to int first to avoid dtype conflicts
        for col, default in [
            ("is_playoffs", 0),
            ("is_consolation", 0),
            ("postseason", 0),
            ("champion", 0),
            ("championship", 0),
        ]:
            if col in df.columns:
                series = df[col]
                if pd.api.types.is_bool_dtype(series):
                    df[col] = series.astype("Int64").fillna(default).astype(int)
                else:
                    df[col] = pd.to_numeric(series, errors="coerce").fillna(default).astype(int)
            else:
                df[col] = default

        # Platform fetchers can provide an authoritative bracket membership.
        # Keep it separate from the tracer's working flags: Yahoo's matchup HTML
        # explicitly marks Championship and Consolation bracket sections, while
        # the tracer only infers membership from seeds and results.
        explicit_championship_mask = (df["is_playoffs"] == 1) & (df["is_consolation"] == 0)
        explicit_consolation_mask = df["is_consolation"] == 1

        if not df["franchise_id"].notna().any():
            logger.warning("[bracket_tracer] franchise_id values are all NULL; skipping classification")
            return df

        id_col = "franchise_id"

        for year, year_settings in settings_by_year.items():
            year_mask = df["year"] == year
            if not year_mask.any():
                continue

            playoff_start = int(year_settings["playoff_start_week"])
            postseason_mask = year_mask & (pd.to_numeric(df["week"], errors="coerce") >= playoff_start)

            # Reset only classification flags (NOT opponents/scores — those are API data)
            df.loc[postseason_mask, "is_playoffs"] = 0
            df.loc[postseason_mask, "is_consolation"] = 0
            df.loc[postseason_mask, "champion"] = 0
            df.loc[postseason_mask, "championship"] = 0

            bt_settings = {
                "playoff_teams": int(year_settings["num_playoff_teams"]),
                "bye_teams": int(year_settings["bye_teams"]),
                "playoff_start_week": playoff_start,
                "end_week": int(year_settings.get("end_week", 17)),
                "num_teams": int(year_settings.get("num_teams", 12)),
                "uses_playoff_reseeding": bool(year_settings.get("uses_playoff_reseeding", False)),
                "playoff_round_type": int(year_settings.get("playoff_round_type", 0)),
                "has_multiweek_championship": bool(year_settings.get("has_multiweek_championship", False)),
                "uses_median_score": bool(
                    year_settings.get("uses_median_score") or year_settings.get("uses_median", False)
                ),
            }

            try:
                result = trace_bracket(df[year_mask], year, bt_settings, id_col=id_col)
            except Exception as exc:
                logger.warning(f"[bracket_tracer] {year}: {exc}")
                continue

            if not result or not result.get("classifications"):
                continue

            # Apply classifications from bracket tracer
            # Skip bye week rows — they should NOT be marked is_playoffs
            # (they have is_bye_week=True, no opponent, no score)
            has_bye_col = "is_bye_week" in df.columns
            for (team_id, week), label in result["classifications"].items():
                row_mask = year_mask & (df[id_col] == team_id) & (df["week"] == week)
                if has_bye_col:
                    is_bye = df.loc[row_mask, "is_bye_week"].fillna(False).astype(bool)
                    row_mask = row_mask & ~is_bye
                if not row_mask.any():
                    continue
                if label == "playoff":
                    df.loc[row_mask, "is_playoffs"] = 1
                elif label == "consolation":
                    df.loc[row_mask, "is_consolation"] = 1

            # Mark championship game and champion
            # For multi-week rounds, mark ALL weeks of the championship round
            champ_week = result.get("championship_week")
            champ_teams = result.get("championship_teams")
            champion_id = result.get("champion")
            champ_rounds = result.get("rounds", [])

            # Determine championship week range from the rounds log
            champ_weeks = [champ_week] if champ_week else []
            if champ_rounds:
                last_round = champ_rounds[-1]
                rw = last_round.get("week")
                if isinstance(rw, str) and "-" in rw:
                    cr_s, cr_e = (int(x) for x in rw.split("-"))
                    champ_weeks = list(range(cr_s, cr_e + 1))
                elif rw is not None:
                    champ_weeks = [int(rw)]

            if champ_weeks and champ_teams:
                for tid in champ_teams:
                    for cw in champ_weeks:
                        df.loc[year_mask & (df[id_col] == tid) & (df["week"] == cw), "championship"] = 1

            if champion_id and champ_weeks:
                for cw in champ_weeks:
                    df.loc[year_mask & (df[id_col] == champion_id) & (df["week"] == cw), "champion"] = 1

            # Any remaining postseason rows not classified by bracket tracer = consolation
            postseason_unclassified = (
                postseason_mask
                & (df["is_playoffs"] == 0)
                & (df["is_consolation"] == 0)
                & df[id_col].notna()
                & df["opponent"].notna()
                & (df["opponent"].astype(str).str.strip() != "")
                & (df["opponent"].astype(str).str.lower() != "none")
            )
            df.loc[postseason_unclassified, "is_consolation"] = 1

            # Source bracket labels outrank inferred tracer membership.  Raw
            # Yahoo consolation rows carry both flags, but canonical matchup
            # rows represent them as consolation-only.
            explicit_championship = postseason_mask & explicit_championship_mask
            explicit_consolation = postseason_mask & explicit_consolation_mask
            df.loc[explicit_championship, "is_playoffs"] = 1
            df.loc[explicit_championship, "is_consolation"] = 0
            df.loc[explicit_consolation, "is_playoffs"] = 0
            df.loc[explicit_consolation, "is_consolation"] = 1

        df["postseason"] = (
            (pd.to_numeric(df["is_playoffs"], errors="coerce").fillna(0) == 1)
            | (pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0) == 1)
        ).astype(int)
        return df

    def _mark_playoff_rounds_locally(
        self,
        matchup_df: pd.DataFrame,
        settings_by_year: dict[int, dict[str, int]],
    ) -> pd.DataFrame:
        """Label playoff rounds from settings-driven bracket shape."""
        from multi_league.transformations.matchup.modules.playoff_bracket.utils import (  # noqa: PLC0415
            get_expected_championship_week,
            get_expected_playoff_rounds,
        )

        df = matchup_df.copy()
        if "playoff_round" in df.columns:
            df["playoff_round"] = df["playoff_round"].astype("string").fillna("")
        else:
            df["playoff_round"] = pd.Series([""] * len(df), index=df.index, dtype="string")
        if "consolation_round" in df.columns:
            df["consolation_round"] = df["consolation_round"].astype("string").fillna("")
        else:
            df["consolation_round"] = pd.Series([""] * len(df), index=df.index, dtype="string")

        for int_col in [
            "playoff_week_index",
            "playoff_round_num",
            "quarterfinal",
            "semifinal",
            "championship",
            "consolation_semifinal",
            "consolation_final",
            "placement_game",
            "placement_rank",
        ]:
            if int_col in df.columns:
                df[int_col] = pd.to_numeric(df[int_col], errors="coerce").fillna(0).astype("Int64")
            else:
                df[int_col] = pd.Series([0] * len(df), index=df.index, dtype="Int64")

        regular_mask = (pd.to_numeric(df["is_playoffs"], errors="coerce").fillna(0) == 0) & (
            pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0) == 0
        )
        for col, zero_value in [
            ("playoff_round", ""),
            ("consolation_round", ""),
            ("playoff_week_index", 0),
            ("playoff_round_num", 0),
            ("quarterfinal", 0),
            ("semifinal", 0),
            ("championship", 0),
            ("consolation_semifinal", 0),
            ("consolation_final", 0),
            ("placement_game", 0),
            ("placement_rank", 0),
        ]:
            df.loc[regular_mask, col] = zero_value

        def _label_for_index(
            idx_from_start: int, total_rounds: int, actual_week: int, expected_championship_week: int
        ) -> str:
            offset_from_end = total_rounds - idx_from_start
            if offset_from_end == 0:
                return "championship"
            if offset_from_end == 1:
                return "semifinal"
            if offset_from_end == 2:
                return "quarterfinal"
            return f"round_{idx_from_start}"

        for year, settings in settings_by_year.items():
            playoff_start = int(settings["playoff_start_week"])
            expected_rounds = int(get_expected_playoff_rounds(settings))
            expected_championship_week = int(get_expected_championship_week(settings))
            round_type = int(settings.get("playoff_round_type", 0))

            champ_mask = (df["year"] == year) & (pd.to_numeric(df["is_playoffs"], errors="coerce").fillna(0) == 1)
            champ_weeks = sorted(pd.Series(df.loc[champ_mask, "week"]).dropna().astype(int).unique())
            if champ_weeks:
                # Build week→round mapping that respects playoff_round_type.
                # For type 0: each week = one round.
                # For type 1 (all 2-week): pairs of weeks share a round.
                # For type 2 (2-week champ): last 2 weeks share the final round.
                week_to_round: dict[int, int] = {}
                w = playoff_start
                for r in range(1, expected_rounds + 1):
                    if round_type == 1:
                        span = 2
                    elif round_type == 2 and r == expected_rounds:
                        span = 2
                    else:
                        span = 1
                    for offset in range(span):
                        week_to_round[w + offset] = r
                    w += span

                # For any playoff week not covered (extra weeks beyond expected),
                # fall back to the naive index
                for week in champ_weeks:
                    if week not in week_to_round:
                        week_to_round[week] = week - playoff_start + 1

                week_to_idx = week_to_round
                week_labels = {
                    week: _label_for_index(week_to_idx[week], expected_rounds, week, expected_championship_week)
                    for week in champ_weeks
                }
                df.loc[champ_mask, "playoff_week_index"] = df.loc[champ_mask, "week"].map(week_to_idx).astype("Int64")
                df.loc[champ_mask, "playoff_round_num"] = df.loc[champ_mask, "playoff_week_index"].astype("Int64")
                df.loc[champ_mask, "playoff_round"] = df.loc[champ_mask, "week"].map(week_labels).astype("string")
                for week, label in week_labels.items():
                    week_mask = champ_mask & (pd.to_numeric(df["week"], errors="coerce") == week)
                    if label == "quarterfinal":
                        df.loc[week_mask, "quarterfinal"] = 1
                    elif label == "semifinal":
                        df.loc[week_mask, "semifinal"] = 1
                    elif label == "championship":
                        df.loc[week_mask, "championship"] = 1

            cons_mask = (df["year"] == year) & (pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0) == 1)
            cons_weeks = sorted(pd.Series(df.loc[cons_mask, "week"]).dropna().astype(int).unique())
            if cons_weeks:
                for week in cons_weeks:
                    idx = week_to_idx.get(week, week - playoff_start + 1)
                    offset_from_end = expected_rounds - idx
                    week_mask = cons_mask & (pd.to_numeric(df["week"], errors="coerce") == week)
                    if offset_from_end == 0:
                        df.loc[week_mask, "consolation_round"] = "consolation_final"
                        df.loc[week_mask, "consolation_final"] = 1
                    elif offset_from_end == 1:
                        df.loc[week_mask, "consolation_round"] = "consolation_semifinal"
                        df.loc[week_mask, "consolation_semifinal"] = 1
                    else:
                        df.loc[week_mask, "consolation_round"] = f"consolation_round_{idx}"

        playoff_only_mask = pd.to_numeric(df["is_playoffs"], errors="coerce").fillna(0) == 1
        consolation_only_mask = pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0) == 1
        df.loc[playoff_only_mask, "consolation_round"] = ""
        df.loc[playoff_only_mask, "consolation_semifinal"] = 0
        df.loc[playoff_only_mask, "consolation_final"] = 0
        df.loc[playoff_only_mask, "placement_game"] = 0
        df.loc[playoff_only_mask, "placement_rank"] = 0
        df.loc[consolation_only_mask, "playoff_round"] = ""
        df.loc[consolation_only_mask, "playoff_week_index"] = 0
        df.loc[consolation_only_mask, "playoff_round_num"] = 0
        df.loc[consolation_only_mask, "quarterfinal"] = 0
        df.loc[consolation_only_mask, "semifinal"] = 0
        df.loc[consolation_only_mask, "championship"] = 0

        df["is_championship"] = pd.to_numeric(df["championship"], errors="coerce").fillna(0).astype("Int64")
        return df

    def shape_playoff_bracket_local(self) -> int:
        """Shape championship/consolation brackets via SQL-native bracket tracer.

        Reads matchup + league_settings from the DDL, traces brackets per year
        using actual matchup data (data-first, no geometry assumptions), and
        writes is_playoffs/is_consolation/championship/champion back via SQL.

        Supports H2H and H2H+Median seeding automatically.
        """
        from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
            trace_championship_bracket_sql,
        )

        if not self._table_exists("matchup") or not self._table_exists("league_settings"):
            return 0

        matchup_table = self._qualified_name("matchup")
        settings_table = self._qualified_name("league_settings")
        matchup_cols = self._get_table_columns("matchup")
        conn = self._get_connection()

        # Ensure required columns exist
        required_columns = {
            "is_playoffs": "INTEGER",
            "is_consolation": "INTEGER",
            "postseason": "INTEGER",
            "is_championship": "BOOLEAN",
            "champion": "INTEGER",
            "sacko": "INTEGER",
            "placement_rank": "INTEGER",
            "playoff_round": "VARCHAR",
            "consolation_round": "VARCHAR",
            "final_playoff_seed": "INTEGER",
        }
        matchup_cols = self._ensure_columns(matchup_table, required_columns, matchup_cols)
        if "franchise_id" not in matchup_cols or "opponent_franchise_id" not in matchup_cols:
            logger.warning("[shape_playoff_bracket_local] franchise_id + opponent_franchise_id required; skipping")
            return 0
        id_col = "franchise_id"

        # Get per-year settings
        try:
            rows = conn.execute(f"SELECT * FROM {settings_table} WHERE {self._db_filter()} ORDER BY year").fetchall()
            col_names = [d[0] for d in conn.description]
        except Exception as exc:
            logger.warning(f"[shape_playoff_bracket_local] Failed to read settings: {exc}")
            return 0

        total = 0
        for row in rows:
            s = dict(zip(col_names, row))
            year = int(s.get("year", 0))

            pt = s.get("playoff_teams")
            if pt is None:
                pt = s.get("num_playoff_teams")
            ps = s.get("playoff_start_week")
            if pt is not None:
                try:
                    if int(pt) <= 1:
                        if self._shape_no_playoff_season_sql(conn, matchup_table, year, s, matchup_cols):
                            total += 1
                        continue
                except (TypeError, ValueError):
                    pass

            if not pt or not ps:
                continue

            import math

            pt = int(pt)
            bs = 2 ** math.ceil(math.log2(max(pt, 2)))
            bye_raw = s.get("bye_teams")
            bye = int(bye_raw) if bye_raw is not None else bs - pt

            mw = self._truthy_setting(s.get("has_multiweek_championship"))
            prt = self._resolve_playoff_round_type(s, playoff_teams=pt)

            settings = {
                "playoff_teams": pt,
                "bye_teams": bye,
                "playoff_start_week": int(ps),
                "end_week": int(s.get("end_week") or 17),
                "num_teams": int(s.get("num_teams") or 12),
                "uses_playoff_reseeding": bool(s.get("uses_playoff_reseeding")),
                "has_multiweek_championship": mw,
                "playoff_round_type": prt,
                "uses_median": bool(s.get("uses_median")),
                "playoff_seeding_rule": s.get("playoff_seeding_rule"),
                "playoff_seeding_rule_by": s.get("playoff_seeding_rule_by"),
            }

            existing_champion_ids = self._existing_champion_ids_sql(conn, matchup_table, year, int(ps))
            has_existing_tiers = self._has_existing_playoff_tiers_sql(conn, matchup_table, year, int(ps))
            if existing_champion_ids and not has_existing_tiers:
                logger.info(
                    f"[bracket] {year}: champion already exists but DDL bracket tiers are missing; "
                    "preserving champion and skipping generic inference"
                )
                total += 1
                continue

            if has_existing_tiers:
                season_complete = self._season_complete_for_result_inference_sql(
                    conn, matchup_table, year, settings["end_week"], matchup_cols
                )
                if existing_champion_ids or season_complete:
                    if self._shape_existing_playoff_bracket_sql(
                        conn,
                        matchup_table,
                        year,
                        s,
                        matchup_cols,
                        existing_champion_ids=existing_champion_ids,
                        allow_missing_champion_inference=season_complete,
                    ):
                        total += 1
                else:
                    logger.info(
                        f"[bracket] {year}: existing DDL playoff tiers but no champion yet; "
                        "season is not complete, so preserving tiers without inference"
                    )
                continue

            if self._has_existing_playoff_result_sql(conn, matchup_table, year, int(ps)):
                if self._shape_existing_playoff_bracket_sql(conn, matchup_table, year, s, matchup_cols):
                    total += 1
                continue

            if not self._season_complete_for_result_inference_sql(
                conn, matchup_table, year, settings["end_week"], matchup_cols
            ):
                logger.info(f"[bracket] {year}: no champion yet and season is not complete; skipping inference")
                continue

            try:
                result = trace_championship_bracket_sql(
                    conn,
                    year,
                    settings,
                    table=matchup_table,
                    id_col=id_col,
                    write_back=True,
                    db_filter=self._db_filter(),
                )
                if result.get("champion"):
                    total += 1
                    logger.info(
                        f"[bracket] {year}: champion={result['champion']}, "
                        f"classifications={len(result.get('classifications', {}))}, "
                        f"champ_teams={result.get('championship_teams')}"
                    )
                else:
                    # Log diagnostic info for years where bracket tracer found nothing
                    n_cls = len(result.get("classifications", {}))
                    champ_wk = result.get("championship_week")
                    champ_teams = result.get("championship_teams")
                    logger.warning(
                        f"[bracket] {year}: NO CHAMPION — "
                        f"classifications={n_cls}, champ_week={champ_wk}, "
                        f"champ_teams={champ_teams}, is_complete={result.get('is_complete')}"
                    )
            except Exception as exc:
                logger.warning(f"[bracket] {year}: EXCEPTION {exc}")

        # Post-bracket diagnostic: verify is_playoffs per year
        try:
            diag = conn.execute(
                f"SELECT year, SUM(CASE WHEN CAST(is_playoffs AS INTEGER) = 1 THEN 1 ELSE 0 END) as po "
                f"FROM {matchup_table} WHERE {self._db_filter()} GROUP BY year ORDER BY year"
            ).fetchall()
            for yr_row in diag:
                print(f"[POST-BRACKET DIAG] {yr_row[0]}: is_playoffs=1 count = {yr_row[1]}")
        except Exception as e:
            print(f"[POST-BRACKET DIAG] check failed: {e}")

        # Sync: is_championship → championship (legacy column for backward compat)
        if "championship" in matchup_cols:
            conn.execute(
                f"UPDATE {matchup_table} SET championship = CAST(is_championship AS INTEGER) "
                f"WHERE is_championship IS NOT NULL AND {self._db_filter()}"
            )

        return total

    def _shape_no_playoff_season_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        settings_row: dict,
        matchup_cols: set[str],
    ) -> bool:
        """Finalize seasons where canonical settings explicitly disable playoffs.

        There is no bracket to trace in these seasons. The final regular-season
        seed order is the final placement order: seed 1 is champion, last seed
        is sacko, and no rows become playoff/consolation/postseason rows.
        """
        try:
            end_week = int(settings_row.get("end_week") or 0)
        except (TypeError, ValueError):
            end_week = 0
        try:
            playoff_start = int(settings_row.get("playoff_start_week") or 0)
        except (TypeError, ValueError):
            playoff_start = 0

        if end_week <= 0:
            row = conn.execute(
                f"""
                SELECT MAX(TRY_CAST(week AS INTEGER))
                FROM {matchup_table}
                WHERE year = {year}
                  AND {self._db_filter()}
                """
            ).fetchone()
            end_week = int(row[0] or 0) if row else 0

        last_regular_week = end_week
        if playoff_start > 0:
            last_regular_week = min(last_regular_week, playoff_start - 1) if last_regular_week else playoff_start - 1
        if last_regular_week <= 0:
            return False

        real_row_filters = [
            f"year = {year}",
            f"TRY_CAST(week AS INTEGER) <= {last_regular_week}",
            "franchise_id IS NOT NULL",
            "TRIM(CAST(franchise_id AS VARCHAR)) <> ''",
            "team_points IS NOT NULL",
            "opponent_points IS NOT NULL",
            self._db_filter(),
        ]
        if "opponent" in matchup_cols:
            real_row_filters.extend(
                [
                    "opponent IS NOT NULL",
                    "TRIM(CAST(opponent AS VARCHAR)) <> ''",
                    "UPPER(TRIM(CAST(opponent AS VARCHAR))) <> 'BYE'",
                ]
            )
        if "is_bye_week" in matchup_cols:
            real_row_filters.append("COALESCE(TRY_CAST(is_bye_week AS INTEGER), 0) = 0")
        if "is_placeholder" in matchup_cols:
            real_row_filters.append("COALESCE(TRY_CAST(is_placeholder AS INTEGER), 0) = 0")

        uses_median_raw = settings_row.get("uses_median")
        uses_median = bool(uses_median_raw)
        if isinstance(uses_median_raw, str):
            uses_median = uses_median_raw.strip().lower() in {"1", "true", "t", "yes", "y"}

        h2h_wins_expr = "SUM(COALESCE(TRY_CAST(win AS INTEGER), 0))" if "win" in matchup_cols else "0"
        median_wins_expr = (
            "SUM(COALESCE(TRY_CAST(above_league_median AS INTEGER), 0))"
            if uses_median and "above_league_median" in matchup_cols
            else "0"
        )
        existing_seed_expr = (
            "MIN(TRY_CAST(final_playoff_seed AS INTEGER))" if "final_playoff_seed" in matchup_cols else "NULL"
        )
        if (
            normalize_playoff_seeding_rule(
                settings_row.get("playoff_seeding_rule"),
                settings_row.get("playoff_seeding_rule_by"),
            )
            == POINTS_FIRST_SEEDING
        ):
            seed_order_sql = """
                                 existing_seed ASC NULLS LAST,
                                 total_points DESC,
                                 (h2h_wins + median_wins) DESC,
                                 franchise_id
            """
        else:
            seed_order_sql = """
                                 existing_seed ASC NULLS LAST,
                                 (h2h_wins + median_wins) DESC,
                                 total_points DESC,
                                 franchise_id
            """
        where_sql = "\n                  AND ".join(real_row_filters)

        conn.execute("DROP TABLE IF EXISTS _no_playoff_ranked")
        conn.execute(
            f"""
            CREATE TEMP TABLE _no_playoff_ranked AS
            WITH reg AS (
                SELECT
                    CAST(franchise_id AS VARCHAR) AS franchise_id,
                    MAX(TRY_CAST(week AS INTEGER)) AS last_week,
                    {existing_seed_expr} AS existing_seed,
                    {h2h_wins_expr} AS h2h_wins,
                    {median_wins_expr} AS median_wins,
                    SUM(COALESCE(TRY_CAST(team_points AS DOUBLE), 0)) AS total_points
                FROM {matchup_table}
                WHERE {where_sql}
                GROUP BY CAST(franchise_id AS VARCHAR)
            ),
            ranked AS (
                SELECT
                    franchise_id,
                    last_week,
                    ROW_NUMBER() OVER (
                        ORDER BY {seed_order_sql}
                    ) AS seed
                FROM reg
            )
            SELECT
                franchise_id,
                last_week,
                seed,
                MAX(seed) OVER () AS max_seed
            FROM ranked
            """
        )

        ranked_count = conn.execute("SELECT COUNT(*) FROM _no_playoff_ranked").fetchone()[0]
        if not ranked_count:
            conn.execute("DROP TABLE IF EXISTS _no_playoff_ranked")
            logger.warning(f"[bracket] {year}: no-playoff season had no rankable regular-season rows")
            return False

        reset_parts = [
            "is_playoffs = 0",
            "is_consolation = 0",
            "postseason = 0",
            "is_championship = FALSE",
            "champion = 0",
            "sacko = 0",
            "placement_rank = NULL",
            "playoff_round = NULL",
            "consolation_round = NULL",
        ]
        if "championship" in matchup_cols:
            reset_parts.append("championship = 0")
        conn.execute(
            f"""
            UPDATE {matchup_table}
            SET {", ".join(reset_parts)}
            WHERE year = {year}
              AND {self._db_filter()}
            """
        )

        update_parts = [
            "placement_rank = r.seed",
            "champion = CASE WHEN r.seed = 1 AND TRY_CAST(m.week AS INTEGER) = r.last_week THEN 1 ELSE 0 END",
            "sacko = CASE WHEN r.seed = r.max_seed AND TRY_CAST(m.week AS INTEGER) = r.last_week THEN 1 ELSE 0 END",
        ]
        if "final_playoff_seed" in matchup_cols:
            update_parts.append("final_playoff_seed = COALESCE(m.final_playoff_seed, r.seed)")
        if "playoff_seed" in matchup_cols:
            update_parts.append("playoff_seed = COALESCE(m.playoff_seed, r.seed)")

        conn.execute(
            f"""
            UPDATE {matchup_table} AS m
            SET {", ".join(update_parts)}
            FROM _no_playoff_ranked r
            WHERE m.year = {year}
              AND CAST(m.franchise_id AS VARCHAR) = r.franchise_id
              AND {self._db_filter("m")}
            """
        )

        champion_row = conn.execute(
            """
            SELECT franchise_id
            FROM _no_playoff_ranked
            WHERE seed = 1
            LIMIT 1
            """
        ).fetchone()
        sacko_row = conn.execute(
            """
            SELECT franchise_id
            FROM _no_playoff_ranked
            WHERE seed = max_seed
            LIMIT 1
            """
        ).fetchone()
        conn.execute("DROP TABLE IF EXISTS _no_playoff_ranked")

        logger.info(
            "[bracket] %s: no-playoff season finalized champion=%s sacko=%s teams=%s",
            year,
            champion_row[0] if champion_row else None,
            sacko_row[0] if sacko_row else None,
            ranked_count,
        )
        return bool(champion_row)

    def _has_existing_playoff_result_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        playoff_start_week: int,
    ) -> bool:
        """Return true when flattened matchup DDL already carries a playoff result."""
        try:
            row = conn.execute(
                f"""
                SELECT
                    COUNT(DISTINCT CASE
                        WHEN COALESCE(TRY_CAST(champion AS INTEGER), 0) = 1
                        THEN CAST(franchise_id AS VARCHAR)
                    END) AS champion_franchises,
                    SUM(CASE
                        WHEN COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1
                          OR COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 1
                        THEN 1 ELSE 0
                    END) AS bracket_rows
                FROM {matchup_table}
                WHERE year = {year}
                  AND week >= {playoff_start_week}
                  AND {self._db_filter()}
                """
            ).fetchone()
            return bool(row and int(row[0] or 0) > 0 and int(row[1] or 0) > 0)
        except Exception as exc:
            logger.warning(f"[shape_playoff_bracket_local] existing playoff result check failed for {year}: {exc}")
            return False

    def _has_existing_playoff_tiers_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        playoff_start_week: int,
    ) -> bool:
        """Return true when flattened DDL already identifies championship tiers.

        This deliberately does not require ``champion=1``. Once platform data
        has been flattened into championship-side ``is_playoffs`` rows, the
        generic tracer should not reconstruct or rearrange that bracket just
        because the champion flag is missing.

        Consolation-only labels are not enough evidence that the championship
        side was flattened. Yahoo can provide consolation rows while leaving the
        winners bracket blank; in that case the generic tracer must still run.
        """
        try:
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {matchup_table}
                WHERE year = {year}
                  AND week >= {playoff_start_week}
                  AND (
                      COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1
                      OR COALESCE(TRY_CAST(is_championship AS INTEGER), 0) = 1
                      OR COALESCE(TRY_CAST(champion AS INTEGER), 0) = 1
                  )
                  AND {self._db_filter()}
                """
            ).fetchone()
            return bool(row and int(row[0] or 0) > 0)
        except Exception as exc:
            logger.warning(f"[shape_playoff_bracket_local] existing playoff tier check failed for {year}: {exc}")
            return False

    def _has_existing_consolation_flags_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        playoff_start_week: int,
    ) -> bool:
        """Return true when flattened matchup DDL already identifies consolation rows."""
        try:
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {matchup_table}
                WHERE year = {year}
                  AND week >= {playoff_start_week}
                  AND COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 1
                  AND {self._db_filter()}
                """
            ).fetchone()
            return bool(row and int(row[0] or 0) > 0)
        except Exception as exc:
            logger.warning(
                f"[shape_consolation_bracket_local] existing consolation flag check failed for {year}: {exc}"
            )
            return False

    def _shape_existing_playoff_bracket_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        settings_row: dict,
        matchup_cols: set[str],
        existing_champion_ids: set[str] | None = None,
        allow_missing_champion_inference: bool = False,
    ) -> bool:
        """Preserve flattened playoff/consolation rows and add derived labels.

        If a completed season has existing DDL bracket tiers but no champion
        flag, fill the missing champion only from the DDL championship rows.
        Do not fall through to the generic bracket tracer, which can rebuild
        matchups from seeds and contradict the flattened API games.
        """
        ps = int(settings_row.get("playoff_start_week") or 0)
        if not ps:
            return False

        if existing_champion_ids is None:
            existing_champion_ids = self._existing_champion_ids_sql(conn, matchup_table, year, ps)
        has_existing_championship_flags = self._has_existing_championship_flags_sql(
            conn, matchup_table, year, ps, matchup_cols
        )
        if has_existing_championship_flags and "championship" in matchup_cols:
            conn.execute(
                f"""
                UPDATE {matchup_table}
                SET is_championship = TRUE
                WHERE year = {year}
                  AND week >= {ps}
                  AND COALESCE(TRY_CAST(championship AS INTEGER), 0) = 1
                  AND {self._db_filter()}
                """
            )

        reset_parts = [
            "playoff_round = NULL",
            "consolation_round = NULL",
        ]
        if not has_existing_championship_flags:
            reset_parts.append("is_championship = FALSE")
        if "championship" in matchup_cols and not has_existing_championship_flags:
            reset_parts.append("championship = 0")
        conn.execute(
            f"""
            UPDATE {matchup_table}
            SET {", ".join(reset_parts)}
            WHERE year = {year}
              AND week >= {ps}
              AND {self._db_filter()}
            """
        )

        self._label_existing_playoff_rounds_sql(
            conn, matchup_table, year, settings_row, preserve_championship_flags=has_existing_championship_flags
        )
        champion_marked = False
        if existing_champion_ids:
            champion_marked = self._preserve_existing_playoff_champion_sql(
                conn, matchup_table, year, existing_champion_ids
            )
        if not champion_marked and allow_missing_champion_inference:
            champion_marked = self._infer_missing_champion_from_existing_ddl_sql(
                conn, matchup_table, year, ps, matchup_cols
            )

        if "postseason" in matchup_cols:
            conn.execute(
                f"""
                UPDATE {matchup_table}
                SET postseason = CASE
                    WHEN COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1
                      OR COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 1
                    THEN 1 ELSE 0 END
                WHERE year = {year}
                  AND week >= {ps}
                  AND {self._db_filter()}
                """
            )

        logger.info(f"[bracket] {year}: preserved existing DDL playoff tiers; champion_marked={champion_marked}")
        return champion_marked

    def _season_complete_for_result_inference_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        end_week: int,
        matchup_cols: set[str],
    ) -> bool:
        """Return true only when final-week result evidence exists."""
        try:
            end_week = int(end_week)
        except (TypeError, ValueError):
            return False
        if end_week <= 0:
            return False

        evidence_clauses = []
        if {"team_points", "opponent_points"}.issubset(matchup_cols):
            evidence_clauses.append(
                "(team_points IS NOT NULL AND opponent_points IS NOT NULL "
                "AND (TRY_CAST(team_points AS DOUBLE) <> 0 OR TRY_CAST(opponent_points AS DOUBLE) <> 0))"
            )
        for col in ("win", "loss", "tie"):
            if col in matchup_cols:
                evidence_clauses.append(f"COALESCE(TRY_CAST({col} AS INTEGER), 0) = 1")
        if not evidence_clauses:
            return False

        filters = [
            f"year = {year}",
            f"TRY_CAST(week AS INTEGER) >= {end_week}",
            "franchise_id IS NOT NULL",
            "TRIM(CAST(franchise_id AS VARCHAR)) <> ''",
            self._db_filter(),
            f"({' OR '.join(evidence_clauses)})",
        ]
        if "opponent" in matchup_cols:
            filters.extend(
                [
                    "opponent IS NOT NULL",
                    "TRIM(CAST(opponent AS VARCHAR)) <> ''",
                    "UPPER(TRIM(CAST(opponent AS VARCHAR))) <> 'BYE'",
                ]
            )
        if "is_bye_week" in matchup_cols:
            filters.append("COALESCE(TRY_CAST(is_bye_week AS INTEGER), 0) = 0")
        if "is_placeholder" in matchup_cols:
            filters.append("COALESCE(TRY_CAST(is_placeholder AS INTEGER), 0) = 0")

        where_sql = "\n                  AND ".join(filters)
        try:
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {matchup_table}
                WHERE {where_sql}
                """
            ).fetchone()
            if row and int(row[0] or 0) > 0:
                return True
        except Exception as exc:
            logger.warning(f"[shape_playoff_bracket_local] season-complete check failed for {year}: {exc}")

        # Fallback: the check above compares `week >= end_week`, where end_week is
        # in NFL scoring-period units. ESPN leagues with 2-week playoff matchup
        # periods (playoffMatchupPeriodLength >= 2) are imported with `week`
        # collapsed to the matchup-period index, so the real final lands a couple
        # of scoring-periods short of end_week and the check above sees no rows.
        # Detect completion straight from the bracket instead — the played
        # championship — which is unit- and era-agnostic.
        return self._championship_final_played_sql(conn, matchup_table, year, matchup_cols, evidence_clauses)

    def _championship_final_played_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        matchup_cols: set[str],
        evidence_clauses: list[str],
    ) -> bool:
        """Return true when the last winners-bracket week is a finished final.

        Data-driven completeness signal that does not depend on ``end_week``
        alignment. It reads the actual playoff weeks present in the matchup data
        and treats the season as complete only when the highest ``is_playoffs``
        (non-consolation) week is a single decided two-team game — the played
        title game. This behaves identically for single-week and multi-week
        finals (the last week of a 2-week final still has exactly the two
        finalists) and stays false for in-progress brackets, whose last playoff
        week either has more than two live teams or no decided result yet. No
        week numbers are hardcoded; the final week is whatever the data shows.
        """
        if "is_playoffs" not in matchup_cols or not evidence_clauses:
            return False

        decided_expr = "(" + " OR ".join(evidence_clauses) + ")"
        filters = [
            f"year = {year}",
            "COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1",
            "franchise_id IS NOT NULL",
            "TRIM(CAST(franchise_id AS VARCHAR)) <> ''",
            self._db_filter(),
        ]
        if "is_consolation" in matchup_cols:
            filters.append("COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 0")
        if "opponent" in matchup_cols:
            filters.extend(
                [
                    "opponent IS NOT NULL",
                    "TRIM(CAST(opponent AS VARCHAR)) <> ''",
                    "UPPER(TRIM(CAST(opponent AS VARCHAR))) <> 'BYE'",
                ]
            )
        if "is_bye_week" in matchup_cols:
            filters.append("COALESCE(TRY_CAST(is_bye_week AS INTEGER), 0) = 0")
        if "is_placeholder" in matchup_cols:
            filters.append("COALESCE(TRY_CAST(is_placeholder AS INTEGER), 0) = 0")

        where_sql = "\n                  AND ".join(filters)
        try:
            row = conn.execute(
                f"""
                WITH playoff_rows AS (
                    SELECT
                        TRY_CAST(week AS INTEGER) AS wk,
                        CAST(franchise_id AS VARCHAR) AS fid,
                        CASE WHEN {decided_expr} THEN 1 ELSE 0 END AS decided
                    FROM {matchup_table}
                    WHERE {where_sql}
                ),
                final_week AS (
                    SELECT MAX(wk) AS wk FROM playoff_rows WHERE wk IS NOT NULL
                )
                SELECT
                    COUNT(DISTINCT p.fid) AS n_teams,
                    COUNT(*) AS n_rows,
                    COALESCE(SUM(p.decided), 0) AS decided_rows
                FROM playoff_rows p
                JOIN final_week f ON p.wk = f.wk
                """
            ).fetchone()
        except Exception as exc:
            logger.warning(f"[shape_playoff_bracket_local] championship-final check failed for {year}: {exc}")
            return False

        if not row:
            return False
        n_teams = int(row[0] or 0)
        n_rows = int(row[1] or 0)
        decided_rows = int(row[2] or 0)
        # Exactly two finalists, all their final-week rows decided → the title
        # game has been played. More than two teams ⇒ the bracket has not yet
        # reached its final; any undecided row ⇒ the final is still in progress.
        return n_teams == 2 and n_rows >= 2 and decided_rows == n_rows

    def _has_existing_championship_flags_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        playoff_start_week: int,
        matchup_cols: set[str],
    ) -> bool:
        clauses = ["COALESCE(TRY_CAST(is_championship AS INTEGER), 0) = 1"]
        if "championship" in matchup_cols:
            clauses.append("COALESCE(TRY_CAST(championship AS INTEGER), 0) = 1")
        try:
            row = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {matchup_table}
                WHERE year = {year}
                  AND week >= {playoff_start_week}
                  AND ({" OR ".join(clauses)})
                  AND {self._db_filter()}
                """
            ).fetchone()
            return bool(row and int(row[0] or 0) > 0)
        except Exception as exc:
            logger.warning(f"[bracket] {year}: failed to read existing championship flags: {exc}")
            return False

    def _existing_champion_ids_sql(self, conn, matchup_table: str, year: int, playoff_start_week: int) -> set[str]:
        try:
            rows = conn.execute(
                f"""
                SELECT DISTINCT CAST(franchise_id AS VARCHAR)
                FROM {matchup_table}
                WHERE year = {year}
                  AND week >= {playoff_start_week}
                  AND COALESCE(TRY_CAST(champion AS INTEGER), 0) = 1
                  AND franchise_id IS NOT NULL
                  AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
                  AND {self._db_filter()}
                """
            ).fetchall()
            return {str(row[0]) for row in rows if row and row[0] is not None}
        except Exception as exc:
            logger.warning(f"[bracket] {year}: failed to read existing champion ids: {exc}")
            return set()

    def _preserve_existing_playoff_champion_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        existing_champion_ids: set[str] | None = None,
    ) -> bool:
        """Keep already-flattened champion flags intact."""
        existing_champion_ids = set(existing_champion_ids or set())
        if not existing_champion_ids:
            logger.warning(f"[bracket] {year}: existing DDL bracket had no champion flags to preserve")
            return False
        conn.execute(
            f"""
            UPDATE {matchup_table}
            SET champion = 0
            WHERE year = {year}
              AND champion IS NULL
              AND {self._db_filter()}
            """
        )
        if len(existing_champion_ids) > 1:
            logger.warning(f"[bracket] {year}: multiple existing champions found; preserving DDL flags")
        return True

    def _infer_missing_champion_from_existing_ddl_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        playoff_start_week: int,
        matchup_cols: set[str],
    ) -> bool:
        """Fill a missing champion from already-flattened DDL final rows.

        This is intentionally narrow: exactly two real championship finalists
        must be present in the existing DDL bracket. The winner comes from the
        teams' own ``team_points`` totals across those final rows, with win
        flags as the fallback. If the DDL final is ambiguous, we leave champion
        blank instead of making a seed-based guess.
        """
        championship_predicate = (
            "COALESCE(TRY_CAST(is_championship AS INTEGER), 0) = 1 "
            "OR LOWER(COALESCE(CAST(playoff_round AS VARCHAR), '')) = 'championship'"
        )
        real_filters = [
            f"year = {year}",
            f"week >= {playoff_start_week}",
            "COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1",
            "COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 0",
            f"({championship_predicate})",
            "franchise_id IS NOT NULL",
            "TRIM(CAST(franchise_id AS VARCHAR)) <> ''",
            "opponent_franchise_id IS NOT NULL",
            "TRIM(CAST(opponent_franchise_id AS VARCHAR)) <> ''",
            self._db_filter(),
        ]
        if "opponent" in matchup_cols:
            real_filters.extend(
                [
                    "opponent IS NOT NULL",
                    "TRIM(CAST(opponent AS VARCHAR)) <> ''",
                    "UPPER(TRIM(CAST(opponent AS VARCHAR))) <> 'BYE'",
                ]
            )
        if "is_bye_week" in matchup_cols:
            real_filters.append("COALESCE(TRY_CAST(is_bye_week AS INTEGER), 0) = 0")
        if "is_placeholder" in matchup_cols:
            real_filters.append("COALESCE(TRY_CAST(is_placeholder AS INTEGER), 0) = 0")

        where_sql = "\n                  AND ".join(real_filters)
        try:
            winner_row = conn.execute(
                f"""
                WITH finalists AS (
                    SELECT
                        CAST(franchise_id AS VARCHAR) AS franchise_id,
                        SUM(COALESCE(TRY_CAST(team_points AS DOUBLE), 0)) AS total_points,
                        SUM(COALESCE(TRY_CAST(win AS INTEGER), 0)) AS total_wins
                    FROM {matchup_table}
                    WHERE {where_sql}
                    GROUP BY CAST(franchise_id AS VARCHAR)
                ),
                summary AS (
                    SELECT
                        COUNT(*) AS finalist_count,
                        COUNT(DISTINCT total_points) AS distinct_point_totals,
                        COUNT(DISTINCT total_wins) AS distinct_win_totals
                    FROM finalists
                ),
                ranked AS (
                    SELECT
                        f.franchise_id,
                        s.finalist_count,
                        s.distinct_point_totals,
                        s.distinct_win_totals,
                        ROW_NUMBER() OVER (
                            ORDER BY
                                CASE
                                    WHEN s.distinct_point_totals > 1 THEN f.total_points
                                    ELSE CAST(f.total_wins AS DOUBLE)
                                END DESC,
                                f.franchise_id
                        ) AS rn
                    FROM finalists f
                    CROSS JOIN summary s
                )
                SELECT franchise_id
                FROM ranked
                WHERE finalist_count = 2
                  AND rn = 1
                  AND (distinct_point_totals > 1 OR distinct_win_totals > 1)
                """
            ).fetchone()
        except Exception as exc:
            logger.warning(f"[bracket] {year}: failed to infer missing DDL champion from final rows: {exc}")
            return False

        if not winner_row or winner_row[0] is None:
            logger.warning(f"[bracket] {year}: existing DDL final was ambiguous; leaving champion blank")
            return False

        winner_id = str(winner_row[0])
        conn.execute(
            f"""
            UPDATE {matchup_table}
            SET champion = 0
            WHERE year = {year}
              AND champion IS NULL
              AND {self._db_filter()}
            """
        )
        conn.execute(
            f"""
            UPDATE {matchup_table}
            SET champion = 1
            WHERE year = {year}
              AND week >= {playoff_start_week}
              AND CAST(franchise_id AS VARCHAR) = ?
              AND COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1
              AND COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 0
              AND ({championship_predicate})
              AND {self._db_filter()}
            """,
            [winner_id],
        )
        logger.info(f"[bracket] {year}: filled missing champion from existing DDL final rows: {winner_id}")
        return True

    def _label_existing_playoff_rounds_sql(
        self,
        conn,
        matchup_table: str,
        year: int,
        settings_row: dict,
        preserve_championship_flags: bool = False,
    ):
        """Label existing playoff rows from flattened matchup rows."""
        ps = int(settings_row.get("playoff_start_week") or 0)
        if not ps:
            return

        playoff_labels = self._existing_round_labels(conn, matchup_table, year, ps, "is_playoffs")
        for wk, label in playoff_labels.items():
            conn.execute(
                f"""
                UPDATE {matchup_table}
                SET playoff_round = ?,
                    is_championship = CASE
                        WHEN ? THEN is_championship
                        WHEN ? = 'championship' THEN TRUE
                        ELSE FALSE
                    END
                WHERE year = {year}
                  AND week = {wk}
                  AND COALESCE(TRY_CAST(is_playoffs AS INTEGER), 0) = 1
                  AND {self._db_filter()}
                """,
                [label, preserve_championship_flags, label],
            )

        consolation_map = {
            "championship": "consolation_final",
            "semifinal": "consolation_semifinal",
            "quarterfinal": "consolation_quarterfinal",
        }
        consolation_labels = self._existing_round_labels(conn, matchup_table, year, ps, "is_consolation")
        for wk, label in consolation_labels.items():
            conn.execute(
                f"""
                UPDATE {matchup_table}
                SET consolation_round = ?
                WHERE year = {year}
                  AND week = {wk}
                  AND COALESCE(TRY_CAST(is_consolation AS INTEGER), 0) = 1
                  AND {self._db_filter()}
                """,
                [consolation_map.get(label, f"consolation_{label}")],
            )

        logger.info(f"[shape_playoff_bracket_local] {year}: labeled rounds from existing DDL playoff rows")

    def _existing_round_labels(
        self,
        conn,
        matchup_table: str,
        year: int,
        playoff_start_week: int,
        flag_col: str,
    ) -> dict[int, str]:
        rows = conn.execute(
            f"""
            SELECT week, franchise_id
            FROM {matchup_table}
            WHERE year = {year}
              AND week >= {playoff_start_week}
              AND COALESCE(TRY_CAST({flag_col} AS INTEGER), 0) = 1
              AND franchise_id IS NOT NULL
              AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
              AND opponent IS NOT NULL
              AND TRIM(CAST(opponent AS VARCHAR)) <> ''
              AND {self._db_filter()}
            ORDER BY week, franchise_id
            """
        ).fetchall()

        week_to_ids: dict[int, set[str]] = {}
        for wk, fid in rows:
            week_to_ids.setdefault(int(wk), set()).add(str(fid))
        weeks = sorted(week_to_ids)
        labels: dict[int, str] = {}
        round_names = ["championship", "semifinal", "quarterfinal"]
        round_idx = 0

        while weeks:
            end_week = weeks[-1]
            participants = week_to_ids[end_week]
            grouped = [end_week]
            cursor = len(weeks) - 2
            while cursor >= 0:
                prev_week = weeks[cursor]
                if prev_week != grouped[-1] - 1 or week_to_ids[prev_week] != participants:
                    break
                grouped.append(prev_week)
                cursor -= 1

            label = round_names[round_idx] if round_idx < len(round_names) else f"round_{round_idx + 1}"
            for wk in grouped:
                labels[wk] = label
            grouped_set = set(grouped)
            weeks = [wk for wk in weeks if wk not in grouped_set]
            round_idx += 1

        return labels

    def shape_consolation_bracket_local(self) -> int:
        """Shape consolation brackets, placement ranks, and sacko via SQL.

        Reads championship bracket state (is_playoffs, champion, final_playoff_seed)
        written by shape_playoff_bracket_local, then labels consolation games,
        assigns placement_rank to all teams, and flags the sacko.

        Must run AFTER shape_playoff_bracket_local.
        """
        from multi_league.transformations.matchup.modules.playoff_bracket.consolation_tracer_sql import (
            trace_consolation_sql,
        )

        if not self._table_exists("matchup") or not self._table_exists("league_settings"):
            return 0

        matchup_table = self._qualified_name("matchup")
        settings_table = self._qualified_name("league_settings")
        matchup_cols = self._get_table_columns("matchup")
        conn = self._get_connection()

        # Ensure required columns exist
        required_columns = {
            "is_consolation": "INTEGER",
            "consolation_round": "VARCHAR",
            "placement_game": "INTEGER",
            "placement_rank": "INTEGER",
            "sacko": "INTEGER",
            "postseason": "INTEGER",
        }
        matchup_cols = self._ensure_columns(matchup_table, required_columns, matchup_cols)
        if "franchise_id" not in matchup_cols or "opponent_franchise_id" not in matchup_cols:
            logger.warning("[shape_consolation_bracket_local] franchise_id + opponent_franchise_id required; skipping")
            return 0
        id_col = "franchise_id"

        # Get per-year settings
        try:
            rows = conn.execute(f"SELECT * FROM {settings_table} WHERE {self._db_filter()} ORDER BY year").fetchall()
            col_names = [d[0] for d in conn.description]
        except Exception as exc:
            logger.warning(f"[shape_consolation_bracket_local] Failed to read settings: {exc}")
            return 0

        total = 0
        for row in rows:
            s = dict(zip(col_names, row))
            year = int(s.get("year", 0))

            pt = s.get("num_playoff_teams") or s.get("playoff_teams")
            ps = s.get("playoff_start_week")
            if not pt or not ps:
                continue

            import math

            pt = int(pt)
            bs = 2 ** math.ceil(math.log2(max(pt, 2)))
            bye_raw = s.get("bye_teams")
            bye = int(bye_raw) if bye_raw is not None else bs - pt

            mw = self._truthy_setting(s.get("has_multiweek_championship"))
            prt = self._resolve_playoff_round_type(s, playoff_teams=pt)

            settings = {
                "playoff_teams": pt,
                "bye_teams": bye,
                "playoff_start_week": int(ps),
                "end_week": int(s.get("end_week") or 17),
                "num_teams": int(s.get("num_teams") or 12),
                "uses_playoff_reseeding": bool(s.get("uses_playoff_reseeding")),
                "has_multiweek_championship": mw,
                "playoff_round_type": prt,
                "uses_median": bool(s.get("uses_median")),
            }
            if self._has_existing_consolation_flags_sql(conn, matchup_table, year, int(ps)):
                settings["preserve_consolation_flags"] = True

            try:
                result = trace_consolation_sql(
                    conn,
                    year,
                    settings,
                    table=matchup_table,
                    id_col=id_col,
                    db_filter=self._db_filter(),
                )
                if result.get("placements"):
                    total += 1
                    sacko = result.get("sacko")
                    logger.info(f"[consolation] {year}: {len(result['placements'])} placements, sacko={sacko}")
            except Exception as exc:
                logger.warning(f"[consolation] {year}: {exc}")

        return total

    def _label_playoff_rounds_sql(self, conn, matchup_table: str, year: int, settings_row: dict):
        """Label playoff_round for a year where fetcher already set is_playoffs/champion.

        Maps each playoff week to a round label (quarterfinal/semifinal/championship)
        based on league settings. Only updates rows already flagged as is_playoffs=1.
        Does NOT re-derive is_playoffs or champion.
        """
        import math

        pt = settings_row.get("num_playoff_teams") or settings_row.get("playoff_teams")
        ps = settings_row.get("playoff_start_week")
        ew = settings_row.get("end_week") or 17
        if not pt or not ps:
            return

        pt, ps, ew = int(pt), int(ps), int(ew)
        num_rounds = math.ceil(math.log2(max(pt, 2)))
        bye_count = 2**num_rounds - pt

        # Map round index (from end) to label
        label_map = {0: "championship", 1: "semifinal", 2: "quarterfinal"}

        prt = self._resolve_playoff_round_type(settings_row, playoff_teams=pt)

        # Build week → round label mapping
        week_labels = {}
        week = ps
        for r in range(num_rounds):
            offset_from_end = num_rounds - r - 1
            label = label_map.get(offset_from_end, f"round_{r + 1}")
            is_multi = (prt == 1) or (prt == 2 and offset_from_end == 0)
            weeks_in_round = 2 if is_multi else 1
            for w in range(week, week + weeks_in_round):
                if w <= ew:
                    week_labels[w] = label
            week += weeks_in_round

        # Update playoff_round for is_playoffs rows
        for wk, label in week_labels.items():
            try:
                conn.execute(f"""
                    UPDATE {matchup_table}
                    SET playoff_round = '{label}',
                        is_championship = CASE WHEN '{label}' = 'championship' THEN 1 ELSE 0 END
                    WHERE year = {year} AND week = {wk}
                      AND (CAST(is_playoffs AS INTEGER) = 1)
                      AND (playoff_round IS NULL OR playoff_round = '')
                      AND {self._db_filter()}
                """)
            except Exception as exc:
                logger.warning(f"[_label_playoff_rounds_sql] {year} week {wk}: {exc}")

        # Also label consolation_round for is_consolation rows. The consolation bracket
        # mirrors the championship bracket in week structure, so reuse the same week map
        # with "consolation_" prefixed labels. Needed for playoff_result aggregation.
        consolation_label_map = {
            "championship": "consolation_final",
            "semifinal": "consolation_semifinal",
            "quarterfinal": "consolation_quarterfinal",
        }
        for wk, label in week_labels.items():
            clabel = consolation_label_map.get(label, f"consolation_{label}")
            try:
                conn.execute(f"""
                    UPDATE {matchup_table}
                    SET consolation_round = '{clabel}'
                    WHERE year = {year} AND week = {wk}
                      AND (CAST(is_consolation AS INTEGER) = 1)
                      AND (consolation_round IS NULL OR consolation_round = '')
                      AND {self._db_filter()}
                """)
            except Exception as exc:
                logger.warning(f"[_label_playoff_rounds_sql consolation] {year} week {wk}: {exc}")

        logger.info(f"[shape_playoff_bracket_local] {year}: labeled rounds from settings (fetcher-set bracket)")

    def backfill_team_name(self) -> int:
        """Backfill NULL team_name from the same (db_name, year, manager) where it
        exists in other weeks. The bracket tracer creates consolation matchup rows
        without team_name; this fills those in from the manager's other rows in the
        same year (using the most recent populated week).
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_cols = self._get_table_columns("matchup")
        if "team_name" not in matchup_cols or "manager" not in matchup_cols:
            return 0

        matchup_table = self._qualified_name("matchup")
        conn = self._get_connection()
        use_franchise_id = False
        if "franchise_id" in matchup_cols:
            try:
                use_franchise_id = bool(
                    conn.execute(
                        f"""
                        SELECT 1
                        FROM {matchup_table}
                        WHERE franchise_id IS NOT NULL
                          AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
                          AND {self._db_filter()}
                        LIMIT 1
                        """
                    ).fetchone()
                )
            except Exception:
                use_franchise_id = False

        if not use_franchise_id:
            logger.warning("[backfill_team_name] franchise_id required; skipping unsafe manager-name backfill")
            return 0

        join_col = "franchise_id"
        sql = f"""
            UPDATE {matchup_table} m
            SET team_name = kn.team_name_known
            FROM (
                SELECT db_name, year, {join_col},
                       ARG_MAX(team_name, week) AS team_name_known
                FROM {matchup_table}
                WHERE team_name IS NOT NULL AND TRIM(team_name) != ''
                  AND {self._db_filter()}
                GROUP BY db_name, year, {join_col}
            ) kn
            WHERE m.db_name = kn.db_name
              AND m.year = kn.year
              AND m.{join_col} = kn.{join_col}
              AND (m.team_name IS NULL OR TRIM(m.team_name) = '')
              AND {self._db_filter("m")}
        """
        total = self._execute(sql, "backfill_team_name: fill NULL team_name from same franchise_id other weeks")

        # Fallback: if team_name is still NULL after cross-week backfill (i.e. the
        # franchise NEVER had a team name in any week), default to franchise_name.
        if "franchise_name" in matchup_cols:
            sql_fallback = f"""
                UPDATE {matchup_table} m
                SET team_name = m.franchise_name
                WHERE m.team_name IS NULL
                  AND m.franchise_name IS NOT NULL
                  AND TRIM(m.franchise_name) <> ''
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_fallback, "backfill_team_name: NULL team_name -> franchise_name")

        return total

    def detect_champions_and_sackos(self) -> int:
        """Detect champions and sackos from playoff/consolation results.

        Champion = winner of the last playoff (non-consolation) game each year.
        Sacko = loser of the last consolation game each year (lowest finisher).

        Same logic, flipped: champion is the top, sacko is the bottom.
        Runs as SQL directly in MotherDuck — no local files needed.
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_cols = self._get_table_columns("matchup")
        matchup_table = self._qualified_name("matchup")
        conn = self._get_connection()

        # Ensure columns exist
        matchup_cols = self._ensure_columns(matchup_table, {"champion": "INTEGER", "sacko": "INTEGER"}, matchup_cols)

        if "is_playoffs" not in matchup_cols or "win" not in matchup_cols:
            logger.warning("[detect_champions_and_sackos] Required columns missing")
            return 0

        has_consolation = "is_consolation" in matchup_cols
        has_seed = "final_playoff_seed" in matchup_cols
        has_fid = "franchise_id" in matchup_cols and "opponent_franchise_id" in matchup_cols
        if not has_fid:
            logger.warning("[detect_champions_and_sackos] franchise_id + opponent_franchise_id required; skipping")
            return 0
        fid_col = "franchise_id"
        opp_fid_col = "opponent_franchise_id"
        total = 0

        # Champion/Sacko flags already set earlier in the pipeline are preserved.

        # Find years where the pipeline already set a champion — skip these
        years_with_champion = set()
        years_with_sacko = set()
        try:
            rows = conn.execute(f"""
                SELECT DISTINCT year FROM {matchup_table} WHERE champion = 1 AND {self._db_filter()}
            """).fetchall()
            years_with_champion = {int(r[0]) for r in rows}
        except Exception:
            pass

        try:
            rows = conn.execute(
                f"""
                SELECT DISTINCT year
                FROM {matchup_table}
                WHERE sacko = 1
                  AND {self._db_filter()}
                """
            ).fetchall()
            years_with_sacko = {int(r[0]) for r in rows}
        except Exception:
            pass

        if years_with_champion:
            logger.info(
                f"[detect_champions_and_sackos] Years with existing champion flags (protected): {sorted(years_with_champion)}"
            )
        if years_with_sacko:
            logger.info(
                f"[detect_champions_and_sackos] Years with existing sacko flags (protected): {sorted(years_with_sacko)}"
            )

        # Build a SQL filter to exclude years that already have champions
        skip_years_filter = ""
        if years_with_champion:
            skip_years_filter = f"AND year NOT IN ({','.join(str(y) for y in years_with_champion)})"
        skip_sacko_years_filter = ""
        skip_sacko_years_filter_m = ""
        if years_with_sacko:
            sacko_years_csv = ",".join(str(y) for y in years_with_sacko)
            skip_sacko_years_filter = f"AND year NOT IN ({sacko_years_csv})"
            skip_sacko_years_filter_m = f"AND m.year NOT IN ({sacko_years_csv})"

        # Only reset sacko flags for gap years this safety net owns.
        reset_sql = f"""
            UPDATE {matchup_table}
            SET sacko = 0
            WHERE sacko = 1
              {skip_sacko_years_filter}
              AND {self._db_filter()}
        """
        self._execute(reset_sql, "detect_champions_and_sackos: Reset sacko flags for gap years")

        # Load per-year playoff_round_type from league_settings
        # 0 = 1 week per round (standard)
        # 1 = 2 weeks per round (all rounds)
        # 2 = 2 weeks championship only
        settings_table = self._qualified_name("league_settings")
        multi_week_years = set()  # Years with 2-week championship
        try:
            if self._table_exists("league_settings"):
                settings_cols = set(self._get_table_columns("league_settings"))
                # Check all possible column names for multi-week detection
                if "playoff_round_type" in settings_cols:
                    rows = conn.execute(
                        f"SELECT year FROM {settings_table} WHERE COALESCE(playoff_round_type, 0) IN (1, 2) AND {self._db_filter()}"
                    ).fetchall()
                elif "has_multiweek_championship" in settings_cols:
                    rows = conn.execute(
                        f"SELECT year FROM {settings_table} WHERE COALESCE(has_multiweek_championship, FALSE) AND {self._db_filter()}"
                    ).fetchall()
                elif "sleeper_playoff_type" in settings_cols:
                    rows = conn.execute(
                        f"SELECT year FROM {settings_table} WHERE sleeper_playoff_type IN (1, 2) AND {self._db_filter()}"
                    ).fetchall()
                else:
                    rows = []

                multi_week_years = {int(year) for (year,) in rows}
        except Exception:
            pass

        if multi_week_years:
            logger.info(f"[detect_champions_and_sackos] Multi-week championship years: {sorted(multi_week_years)}")

        # --- CHAMPION DETECTION (safety net for years without pipeline-set champions) ---

        # Only detect champions for years that don't already have one.
        # Priority: 1) Platform API, 2) Bracket model (pipeline), 3) SQL safety net (here).
        non_consolation_filter = "AND COALESCE(CAST(is_consolation AS INT), 0) = 0" if has_consolation else ""

        # When the max-week playoff round has multiple winners (e.g. corrupt
        # bracket labeling on legacy leagues where semifinals and the
        # championship share is_playoffs=1 across the same week, or where
        # the actual championship game is mis-labeled is_consolation=1),
        # rank winners within the year by team_points desc and only crown
        # the highest scorer. Conservative tiebreaker: never produces > 1
        # champion per year. Real championship-game labeling fixes belong
        # upstream in shape_playoff_bracket_local / enforce_postseason_flags.
        standard_champion_sql = f"""
            UPDATE {matchup_table}
            SET champion = 1
            WHERE manager_week IN (
                SELECT manager_week FROM (
                    SELECT manager_week,
                           ROW_NUMBER() OVER (
                               PARTITION BY year
                               ORDER BY week DESC, COALESCE(team_points, 0) DESC, manager
                           ) AS rn
                    FROM {matchup_table}
                    WHERE COALESCE(CAST(is_playoffs AS INT), 0) = 1
                      {non_consolation_filter}
                      AND COALESCE(CAST(win AS INT), 0) = 1
                      {skip_years_filter}
                      AND {self._db_filter()}
                ) ranked
                WHERE rn = 1
            )
            AND {self._db_filter()}
        """

        if not multi_week_years:
            # All years are standard
            total += self._execute(
                standard_champion_sql, "detect_champions_and_sackos: Mark champions (standard, gap years only)"
            )
        else:
            # mw_years_list covers ALL multi-week years (used by both champion and sacko sections)
            mw_years_list = ",".join(str(y) for y in multi_week_years)
            # Standard years only (exclude multi-week AND already-set years)
            standard_years = f"AND year NOT IN ({','.join(str(y) for y in multi_week_years)})"
            standard_sql = f"""
                UPDATE {matchup_table}
                SET champion = 1
                WHERE manager_week IN (
                    SELECT manager_week FROM (
                        SELECT manager_week,
                               ROW_NUMBER() OVER (
                                   PARTITION BY year
                                   ORDER BY week DESC, COALESCE(team_points, 0) DESC, manager
                               ) AS rn
                        FROM {matchup_table}
                        WHERE COALESCE(CAST(is_playoffs AS INT), 0) = 1
                          {non_consolation_filter}
                          AND COALESCE(CAST(win AS INT), 0) = 1
                          {standard_years}
                          {skip_years_filter}
                          AND {self._db_filter()}
                    ) ranked
                    WHERE rn = 1
                )
                AND {self._db_filter()}
            """
            total += self._execute(
                standard_sql, "detect_champions_and_sackos: Mark champions (standard, gap years only)"
            )

            # Multi-week championship: find the 2 managers in the last 2 playoff weeks,
            # sum their points across both weeks, the one with higher total is champion.
            # Only for years without a pipeline-set champion.
            mw_gap_years = multi_week_years - years_with_champion
            if mw_gap_years:
                mw_years_list = ",".join(str(y) for y in mw_gap_years)
                multi_week_champion_sql = f"""
                    WITH last_two_playoff_weeks AS (
                        SELECT year, week,
                               ROW_NUMBER() OVER (PARTITION BY year ORDER BY week DESC) as rn
                        FROM (
                            SELECT DISTINCT year, week
                            FROM {matchup_table}
                            WHERE COALESCE(CAST(is_playoffs AS INT), 0) = 1
                              {non_consolation_filter}
                              AND year IN ({mw_years_list})
                              AND {self._db_filter()}
                        )
                    ),
                    championship_weeks AS (
                        SELECT year, week FROM last_two_playoff_weeks WHERE rn <= 2
                    ),
                    championship_totals AS (
                        SELECT m.year, m.{fid_col},
                               MAX(m.manager) AS manager,
                               SUM(m.team_points) as total_pts
                        FROM {matchup_table} m
                        JOIN championship_weeks cw ON m.year = cw.year AND m.week = cw.week
                        WHERE COALESCE(CAST(m.is_playoffs AS INT), 0) = 1
                          {non_consolation_filter}
                          AND {self._db_filter("m")}
                        GROUP BY m.year, m.{fid_col}
                    ),
                    champions AS (
                        SELECT year, {fid_col},
                               ROW_NUMBER() OVER (PARTITION BY year ORDER BY total_pts DESC) as rn
                        FROM championship_totals
                    )
                    UPDATE {matchup_table} m
                    SET champion = 1
                    FROM champions c
                    JOIN championship_weeks cw ON c.year = cw.year
                    WHERE m.year = c.year AND m.week = cw.week AND m.{fid_col} = c.{fid_col}
                      AND c.rn = 1
                      AND COALESCE(CAST(m.is_playoffs AS INT), 0) = 1
                      {non_consolation_filter}
                      AND {self._db_filter("m")}
                """
                total += self._execute(
                    multi_week_champion_sql, "detect_champions_and_sackos: Mark champions (multi-week, gap years only)"
                )

        # --- CHAMPION DEDUP (for SQL-detected years with placement games) ---
        # If the SQL safety net marked multiple winners in the same finals week
        # (championship + placement games), keep only the real championship game
        # identified by the lowest combined playoff seed sum.
        if has_seed:
            dedup_sql = f"""
                WITH multi_champ_years AS (
                    SELECT year FROM {matchup_table}
                    WHERE champion = 1
                      {skip_years_filter}
                      AND {self._db_filter()}
                    GROUP BY year
                    HAVING COUNT(*) > 2
                ),
                -- Get one row per matchup pair, compute combined seed
                matchup_pairs AS (
                    SELECT m.year, m.week,
                           LEAST(m.{fid_col}, m.{opp_fid_col}) as pair_a,
                           GREATEST(m.{fid_col}, m.{opp_fid_col}) as pair_b,
                           MIN(COALESCE(m.final_playoff_seed, 999)) +
                               MIN(COALESCE(
                                   (SELECT opp.final_playoff_seed FROM {matchup_table} opp
                                    WHERE opp.year = m.year AND opp.week = m.week
                                    AND opp.{fid_col} = m.{opp_fid_col}
                                    AND opp.{opp_fid_col} = m.{fid_col}
                                    AND {self._db_filter("opp")}
                                    LIMIT 1), 999
                           )) as seed_sum
                    FROM {matchup_table} m
                    JOIN multi_champ_years mcy ON m.year = mcy.year
                    WHERE m.champion = 1
                      AND {self._db_filter("m")}
                    GROUP BY m.year, m.week,
                             LEAST(m.{fid_col}, m.{opp_fid_col}),
                             GREATEST(m.{fid_col}, m.{opp_fid_col})
                ),
                -- Rank pairs: lowest seed sum = real championship
                best_pair AS (
                    SELECT year, pair_a, pair_b,
                           ROW_NUMBER() OVER (PARTITION BY year ORDER BY seed_sum ASC) as rn
                    FROM matchup_pairs
                )
                UPDATE {matchup_table} m
                SET champion = 0
                FROM best_pair bp
                WHERE m.year = bp.year
                  AND m.champion = 1
                  AND bp.rn = 1
                  AND m.year IN (SELECT year FROM multi_champ_years)
                  AND {self._db_filter("m")}
                  AND NOT (
                      (m.{fid_col} = bp.pair_a AND m.{opp_fid_col} = bp.pair_b) OR
                      (m.{fid_col} = bp.pair_b AND m.{opp_fid_col} = bp.pair_a)
                  )
            """
            dedup_count = self._execute(dedup_sql, "detect_champions_and_sackos: Dedup multiple champions by seed")
            if dedup_count:
                logger.info(
                    f"[detect_champions_and_sackos] Deduped {dedup_count} false champion rows via seed-based tiebreaker"
                )

        # --- FINAL SAFETY NET: clear multi-champion years ---
        # If any year still has 2+ DISTINCT franchises flagged champion=1 after dedup,
        # the bracket is broken (e.g., flesh_for_fantasy 2016 where the tracer
        # mislabelled both semifinal games as the final round, leaving both SF
        # winners flagged as champion). Better to have no champion than two.
        clear_multi_champion_sql = f"""
            UPDATE {matchup_table}
            SET champion = 0
            WHERE year IN (
                SELECT year FROM {matchup_table}
                WHERE champion = 1
                  {skip_years_filter}
                  AND {self._db_filter()}
                GROUP BY year
                HAVING COUNT(DISTINCT COALESCE({fid_col}, 'NA')) > 1
            )
            AND champion = 1
            AND {self._db_filter()}
        """
        cleared = self._execute(
            clear_multi_champion_sql,
            "detect_champions_and_sackos: Clear champion flags in broken-bracket years",
        )
        if cleared:
            logger.warning(
                f"[detect_champions_and_sackos] Cleared {cleared} champion rows in years with multiple distinct champion franchises (broken bracket)"
            )

        # --- SACKO DETECTION ---
        #
        # Rewritten to prefer the authoritative `consolation_round` label
        # populated by shape_playoff_bracket_local. The prior logic
        # (`last consolation week + lowest points`) failed on pigskin
        # 2019-2025: those years have both `consolation_semifinal` and
        # `consolation_final` rounds in distinct weeks, and summing team
        # points across "last two consolation weeks" picked up teams from
        # different bracket tiers. The sacko is unambiguously the team
        # that LOST their final consolation_final game — `consolation_round`
        # already identifies that directly. For multi-week finals, a team
        # has TWO consolation_final rows (one per week of the 2-week round)
        # and we pick the team with the lowest SUM across its
        # consolation_final rows, breaking ties on the row's own loss flag.

        if has_consolation:
            sacko_sql = f"""
                WITH final_totals AS (
                    SELECT m.year, m.{fid_col},
                           SUM(COALESCE(m.team_points, 0)) AS total_pts,
                           SUM(COALESCE(CAST(m.loss AS INT), 0)) AS total_losses
                    FROM {matchup_table} m
                    WHERE m.consolation_round = 'consolation_final'
                      {skip_sacko_years_filter_m}
                      AND {self._db_filter("m")}
                    GROUP BY m.year, m.{fid_col}
                ),
                sackos AS (
                    SELECT year, {fid_col},
                           ROW_NUMBER() OVER (
                               PARTITION BY year
                               ORDER BY total_pts ASC, total_losses DESC
                           ) as rn
                    FROM final_totals
                )
                UPDATE {matchup_table} m
                SET sacko = 1
                FROM sackos s
                WHERE m.year = s.year
                  AND m.{fid_col} = s.{fid_col}
                  AND s.rn = 1
                  AND m.consolation_round = 'consolation_final'
                  {skip_sacko_years_filter_m}
                  AND {self._db_filter("m")}
            """
            total += self._execute(sacko_sql, "detect_champions_and_sackos: Mark sacko via consolation_final")

            # Fallback for years where consolation_round wasn't populated
            # (legacy / older imports). Use the prior "lowest team_points in
            # last consolation week" heuristic scoped to years that have
            # consolation rows but NO consolation_final label.
            fallback_sql = f"""
                WITH years_without_final_label AS (
                    SELECT DISTINCT year
                    FROM {matchup_table}
                    WHERE COALESCE(CAST(is_consolation AS INT), 0) = 1
                      {skip_sacko_years_filter}
                      AND {self._db_filter()}
                      AND year NOT IN (
                          SELECT DISTINCT year FROM {matchup_table}
                          WHERE consolation_round = 'consolation_final'
                            AND {self._db_filter()}
                      )
                ),
                last_consolation_week AS (
                    SELECT m.year, MAX(m.week) as last_week
                    FROM {matchup_table} m
                    INNER JOIN years_without_final_label ywfl ON ywfl.year = m.year
                    WHERE COALESCE(CAST(m.is_consolation AS INT), 0) = 1
                      {skip_sacko_years_filter_m}
                      AND {self._db_filter("m")}
                    GROUP BY m.year
                ),
                sacko_losers AS (
                    SELECT m.year, m.week, m.{fid_col},
                           ROW_NUMBER() OVER (
                               PARTITION BY m.year
                               ORDER BY m.team_points ASC
                           ) as rn
                    FROM {matchup_table} m
                    JOIN last_consolation_week lc ON m.year = lc.year AND m.week = lc.last_week
                    WHERE COALESCE(CAST(m.is_consolation AS INT), 0) = 1
                      AND COALESCE(CAST(m.loss AS INT), 0) = 1
                      {skip_sacko_years_filter_m}
                      AND {self._db_filter("m")}
                )
                UPDATE {matchup_table} m
                SET sacko = 1
                FROM sacko_losers sl
                WHERE m.year = sl.year AND m.week = sl.week AND m.{fid_col} = sl.{fid_col}
                  AND sl.rn = 1
                  {skip_sacko_years_filter_m}
                  AND {self._db_filter("m")}
            """
            total += self._execute(fallback_sql, "detect_champions_and_sackos: Fallback sacko for legacy data")

        return total

    def compute_season_result(self) -> int:
        """Derive ``season_result`` strings for every postseason team.

        Replaces the orphaned Python ``add_season_result()`` /
        ``simulate_playoff_brackets()`` wire-up. Builds the string directly from
        the VARCHAR ``playoff_round`` / ``consolation_round`` labels already
        populated by ``shape_playoff_bracket_local``:

        - Championship winner/loser: ``"won championship"`` / ``"lost championship"``
        - Semifinal/quarterfinal losers that never play again: ``"lost semifinal"``,
          ``"lost quarterfinal"``
        - Consolation bracket: ``"won consolation final"``, ``"lost consolation semifinal"``,
          ``"won consolation round 1"`` …

        Picks each team's highest-priority postseason game per year. The result is
        broadcast to every row for that ``(year, franchise_id)``.
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_cols = self._get_table_columns("matchup")
        required = {"year", "week", "franchise_id", "is_playoffs", "is_consolation", "win", "loss", "playoff_round"}
        missing = required - set(matchup_cols)
        if missing:
            logger.info(f"[compute_season_result] Missing columns {missing}, skipping")
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._ensure_columns(matchup_table, {"season_result": "VARCHAR"}, matchup_cols)

        has_cons_round = "consolation_round" in matchup_cols

        # Priority ordering for selecting each team's most-important postseason game.
        # Higher priority wins. Championship always beats semifinal beats quarterfinal.
        # Plain consolation rounds rank below any championship-bracket labels.
        priority_cases = [
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND playoff_round = 'championship' THEN 100",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND playoff_round = 'semifinal' THEN 80",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND playoff_round = 'quarterfinal' THEN 60",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND COALESCE(playoff_round, '') != '' THEN 70",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 THEN 50",
        ]
        game_type_cases = [
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND playoff_round = 'championship' THEN 'championship'",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND playoff_round = 'semifinal' THEN 'semifinal'",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND playoff_round = 'quarterfinal' THEN 'quarterfinal'",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 AND COALESCE(playoff_round, '') != '' "
            "THEN REPLACE(playoff_round, '_', ' ')",
            "WHEN CAST(is_playoffs AS INTEGER) = 1 THEN 'playoff round'",
        ]

        if has_cons_round:
            priority_cases.extend(
                [
                    "WHEN CAST(is_consolation AS INTEGER) = 1 AND consolation_round = 'consolation_final' THEN 40",
                    "WHEN CAST(is_consolation AS INTEGER) = 1 AND consolation_round = 'consolation_semifinal' THEN 30",
                    "WHEN CAST(is_consolation AS INTEGER) = 1 AND COALESCE(consolation_round, '') != '' THEN 20",
                ]
            )
            game_type_cases.extend(
                [
                    "WHEN CAST(is_consolation AS INTEGER) = 1 AND consolation_round = 'consolation_final' "
                    "THEN 'consolation final'",
                    "WHEN CAST(is_consolation AS INTEGER) = 1 AND consolation_round = 'consolation_semifinal' "
                    "THEN 'consolation semifinal'",
                    "WHEN CAST(is_consolation AS INTEGER) = 1 AND COALESCE(consolation_round, '') != '' "
                    "THEN REPLACE(consolation_round, '_', ' ')",
                ]
            )
        priority_cases.append("WHEN CAST(is_consolation AS INTEGER) = 1 THEN 10")
        game_type_cases.append("WHEN CAST(is_consolation AS INTEGER) = 1 THEN 'consolation round'")

        priority_sql = (
            "CASE\n              " + "\n              ".join(priority_cases) + "\n              ELSE 0\n            END"
        )
        game_type_sql = (
            "CASE\n              "
            + "\n              ".join(game_type_cases)
            + "\n              ELSE NULL\n            END"
        )

        sql = f"""
        WITH postseason_rows AS (
            SELECT year, franchise_id, week,
                   COALESCE(CAST(win AS INTEGER), 0) AS win_int,
                   COALESCE(CAST(loss AS INTEGER), 0) AS loss_int,
                   {priority_sql} AS priority,
                   {game_type_sql} AS game_type
            FROM {matchup_table}
            WHERE {self._db_filter()}
              AND franchise_id IS NOT NULL
              AND (CAST(is_playoffs AS INTEGER) = 1
                   OR CAST(is_consolation AS INTEGER) = 1)
        ),
        ranked AS (
            SELECT year, franchise_id, priority, win_int, loss_int, game_type,
                   ROW_NUMBER() OVER (
                       PARTITION BY year, franchise_id
                       ORDER BY priority DESC, week DESC
                   ) AS rn
            FROM postseason_rows
            WHERE game_type IS NOT NULL
        ),
        final_results AS (
            SELECT year, franchise_id,
                   CASE
                       WHEN win_int = 1 THEN 'won ' || game_type
                       WHEN loss_int = 1 THEN 'lost ' || game_type
                       ELSE 'tied in ' || game_type
                   END AS season_result
            FROM ranked
            WHERE rn = 1
        )
        UPDATE {matchup_table} m
        SET season_result = fr.season_result
        FROM final_results fr
        WHERE m.year = fr.year
          AND m.franchise_id = fr.franchise_id
          AND {self._db_filter("m")}
          AND (m.season_result IS NULL
               OR m.season_result != fr.season_result)
        """

        return self._execute(sql, "compute_season_result: Derive season_result from postseason flags")

    def fix_margin(self) -> int:
        """Recalculate margin = team_points - opponent_points for all matchup rows.

        Fixes stale/corrupted margin values from legacy imports. Safe to run
        on all rows since margin is always a simple derived column.
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_cols = self._get_table_columns("matchup")
        if "margin" not in matchup_cols or "team_points" not in matchup_cols or "opponent_points" not in matchup_cols:
            return 0

        matchup_table = self._qualified_name("matchup")

        sql = f"""
            UPDATE {matchup_table}
            SET margin = team_points - opponent_points
            WHERE team_points IS NOT NULL
              AND opponent_points IS NOT NULL
              AND (margin IS NULL OR ABS(margin - (team_points - opponent_points)) > 0.01)
        """

        return self._execute(sql, "fix_margin: Recalculate margin from team/opponent points")

    def populate_team_projected_points(self) -> int:
        """Roll up player_fantasy.projected_points into matchup.team_projected_points.

        Source of truth:
        - Yahoo: populated by matchup fetcher via XML `team_projected_points/total`
          (canonical `"api"` source). This step is a no-op for Yahoo because of
          the `IS NULL` guard.
        - ESPN 2019+: player_fantasy.projected_points is populated per start (ESPN
          API started returning projections around 2019). This step rolls those up.
        - ESPN pre-2019 + Sleeper: player_fantasy.projected_points is always NULL,
          so the rollup subquery produces no rows and no UPDATE fires.

        This rollup runs as part of matchup enrichments so team_projected_points
        is populated for ESPN 2019+ leagues. Yahoo populates it directly via the
        matchup fetcher. ESPN pre-2019 + Sleeper have no projection data.

        Joins on `franchise_id` / `opponent_franchise_id` (the stable join key)
        rather than `manager_guid`, per the project-wide franchise-id migration.

        Must run BEFORE `compute_win_loss_and_projections` (which reads
        team_projected_points to derive proj_wins / proj_score_error / etc.).
        """
        if not self._table_exists("matchup") or not self._table_exists("player_fantasy"):
            return 0

        matchup_cols = self._get_table_columns("matchup")
        player_cols = self._get_table_columns("player_fantasy")
        required_m = {"franchise_id", "opponent_franchise_id", "team_projected_points", "opponent_projected_points"}
        required_p = {"franchise_id", "projected_points", "is_started"}
        if not required_m.issubset(matchup_cols) or not required_p.issubset(player_cols):
            return 0

        matchup_table = self._qualified_name("matchup")
        player_table = self._qualified_name("player_fantasy")
        total = 0

        # Coverage-aware rollup: require at least 75% of a team's started
        # players to have projected_points before writing the sum. ESPN's
        # player.projected_points attribute is unreliable for certain
        # weeks in certain years (e.g., tfl + pigskin 2023 week 1 where
        # only the kicker had a projection), and partial rollups produce
        # implausibly-tiny team projections (9.32 = "just Justin Tucker")
        # that look populated to downstream consumers but are worse than
        # leaving the column NULL. Require coverage ≥ 75% of starters.
        sql_team = f"""
            WITH starter_counts AS (
                SELECT year, week, franchise_id, COUNT(*) AS n_starters
                FROM {player_table}
                WHERE is_started = 1
                  AND franchise_id IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY year, week, franchise_id
            ),
            projection_totals AS (
                SELECT year, week, franchise_id,
                       ROUND(SUM(projected_points), 2) AS proj,
                       COUNT(*) AS n_projected
                FROM {player_table}
                WHERE is_started = 1
                  AND projected_points IS NOT NULL
                  AND franchise_id IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY year, week, franchise_id
            )
            UPDATE {matchup_table} m
            SET team_projected_points = pt.proj
            FROM projection_totals pt
            JOIN starter_counts sc
              ON sc.year = pt.year AND sc.week = pt.week AND sc.franchise_id = pt.franchise_id
            WHERE m.year = pt.year
              AND m.week = pt.week
              AND m.franchise_id = pt.franchise_id
              AND m.team_projected_points IS NULL
              AND pt.n_projected * 4 >= sc.n_starters * 3
              AND {self._db_filter("m")}
        """
        total += self._execute(sql_team, "populate_team_projected_points: team rollup from player_fantasy")

        sql_opp = f"""
            WITH starter_counts AS (
                SELECT year, week, franchise_id, COUNT(*) AS n_starters
                FROM {player_table}
                WHERE is_started = 1
                  AND franchise_id IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY year, week, franchise_id
            ),
            projection_totals AS (
                SELECT year, week, franchise_id,
                       ROUND(SUM(projected_points), 2) AS proj,
                       COUNT(*) AS n_projected
                FROM {player_table}
                WHERE is_started = 1
                  AND projected_points IS NOT NULL
                  AND franchise_id IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY year, week, franchise_id
            )
            UPDATE {matchup_table} m
            SET opponent_projected_points = pt.proj
            FROM projection_totals pt
            JOIN starter_counts sc
              ON sc.year = pt.year AND sc.week = pt.week AND sc.franchise_id = pt.franchise_id
            WHERE m.year = pt.year
              AND m.week = pt.week
              AND m.opponent_franchise_id = pt.franchise_id
              AND m.opponent_projected_points IS NULL
              AND pt.n_projected * 4 >= sc.n_starters * 3
              AND {self._db_filter("m")}
        """
        total += self._execute(sql_opp, "populate_team_projected_points: opponent rollup from player_fantasy")

        return total

    def enforce_postseason_flags(self) -> int:
        """Enforce that every postseason row has is_playoffs, is_consolation, or is_bye_week.

        Uses per-year playoff_start_week from the flat league_settings table.
        Falls back to NFL standard (week 15 for 2021+, week 14 before) when
        settings are missing for a year.

        Rows in postseason weeks with no flag are assigned:
        - is_consolation=1 if they have an opponent (real consolation game)
        - Phantom bye rows (is_bye_week=1 with no real game after) are cleaned up.
        """
        matchup_table = self._qualified_name("matchup")
        settings_table = self._qualified_name("league_settings")

        matchup_cols = self._get_table_columns("matchup")
        if "is_bye_week" not in matchup_cols:
            logger.info("[enforce_postseason_flags] is_bye_week not in matchup, skipping")
            return 0

        if not self._table_exists("league_settings"):
            raise ValueError("[enforce_postseason_flags] canonical league_settings table is required")

        # Build a strict per-year playoff_start_week CTE from canonical league_settings.
        # Reads flat column directly — no JSON parsing
        missing_years = self.conn.execute(
            f"""
            SELECT DISTINCT m.year
            FROM {matchup_table} m
            LEFT JOIN {settings_table} ls ON m.year = ls.year
            WHERE (ls.year IS NULL OR ls.playoff_start_week IS NULL)
              AND {self._db_filter("m")}
              AND {self._db_filter("ls")}
            ORDER BY m.year
            """
        ).fetchall()
        if missing_years:
            years = ", ".join(str(row[0]) for row in missing_years if row and row[0] is not None)
            raise ValueError(
                f"[enforce_postseason_flags] Missing canonical playoff_start_week in league_settings for years: {years}"
            )

        playoff_start_cte = f"""
            playoff_weeks AS (
                SELECT DISTINCT m.year, ls.playoff_start_week AS playoff_start
                FROM {matchup_table} m
                JOIN {settings_table} ls ON m.year = ls.year
                WHERE {self._db_filter("m")}
                  AND {self._db_filter("ls")}
            )
        """

        total = 0

        # Build opponent-presence predicates (same logic as resolve_hidden_managers)
        if "opponent_franchise_id" in matchup_cols:
            no_opponent_predicate = (
                "m.opponent_franchise_id IS NULL OR TRIM(COALESCE(m.opponent_franchise_id, '')) = ''"
            )
            has_opponent_predicate = (
                "m.opponent_franchise_id IS NOT NULL AND TRIM(COALESCE(m.opponent_franchise_id, '')) <> ''"
            )
        else:
            no_opponent_predicate = "m.opponent IS NULL OR TRIM(COALESCE(m.opponent, '')) = ''"
            has_opponent_predicate = "m.opponent IS NOT NULL AND TRIM(COALESCE(m.opponent, '')) <> ''"

        # franchise_id needed by Steps 0.5, 2, 3, 4 for franchise-level operations
        if "franchise_id" not in matchup_cols:
            logger.warning("[enforce_postseason_flags] franchise_id required; skipping advanced steps")
            pflag_fid_col = "manager"  # fallback for Step -1 and 0 only
        else:
            pflag_fid_col = "franchise_id"

        # Step -1: Flag regular-season bye rows. Odd-team-count leagues (e.g.
        # pigskin 2014 at 9 teams) have one team with a natural bye each
        # regular-season week. The fetcher correctly emits these rows with
        # opponent NULL, but `is_bye_week` is a "sql"-source column in the
        # canonical schema (canonical_matchup.py:130), so the fetcher's
        # value is DROPPED during upload (upload_matchups only inserts
        # "api" + "join_key" source columns — see RAW_COLUMNS). After
        # upload, the only step that re-sets `is_bye_week=1` is Step 0
        # below, which is scoped to postseason weeks only. That leaves
        # regular-season bye rows with is_bye_week=0, tripping a cascade of
        # validator checks (matchups_opponent_populated,
        # matchups_team_points_not_null, matchups_power_rating_populated,
        # luck_shuffle_*, etc. — anything filtered by `is_bye_week = 0`).
        sql_regular_byes = f"""
            WITH {playoff_start_cte}
            UPDATE {matchup_table} m
            SET is_bye_week = 1
            FROM playoff_weeks pw
            WHERE m.year = pw.year
              AND m.week < pw.playoff_start
              AND ({no_opponent_predicate})
              AND COALESCE(m.is_bye_week, 0) = 0
              AND {self._db_filter("m")}
        """
        total += self._execute(sql_regular_byes, "enforce_postseason_flags: Flag regular-season bye rows")

        # Step 0: Mark postseason no-opponent rows as bye weeks. Per project
        # policy (memory: feedback_phantom_rows_no_flags) phantom rows where
        # the team didn't play must NEVER carry postseason flags — only
        # `is_bye_week` is set here. `is_playoffs`, `is_consolation`, and
        # `postseason` are intentionally untouched (they remain NULL/0).
        sql_byes = f"""
            WITH {playoff_start_cte}
            UPDATE {matchup_table} m
            SET is_bye_week = 1
            FROM playoff_weeks pw
            WHERE m.year = pw.year
              AND m.week >= pw.playoff_start
              AND ({no_opponent_predicate})
              AND COALESCE(m.is_bye_week, 0) = 0
              AND COALESCE(m.is_playoffs, 0) = 0
              AND COALESCE(m.is_consolation, 0) = 0
              AND {self._db_filter("m")}
        """
        total += self._execute(sql_byes, "enforce_postseason_flags: Flag playoff bye rows")

        # Step 1: Flag unflagged postseason rows with opponents as consolation
        sql_consolation = f"""
            WITH {playoff_start_cte}
            UPDATE {matchup_table} m
            SET is_consolation = 1, postseason = 1
            FROM playoff_weeks pw
            WHERE m.year = pw.year
              AND m.week >= pw.playoff_start
              AND COALESCE(m.is_playoffs, 0) = 0
              AND COALESCE(m.is_consolation, 0) = 0
              AND COALESCE(m.is_bye_week, 0) = 0
              AND ({has_opponent_predicate})
              AND {self._db_filter("m")}
        """
        total += self._execute(sql_consolation, "enforce_postseason_flags: Flag unflagged consolation games")

        # Step 2: Format phantom bye rows (eliminated teams with no future game)
        # Don't delete — the simulations UI needs a row per manager per week.
        # Phantom byes stay as is_bye_week=1 with NO is_playoffs or is_consolation
        if pflag_fid_col != "franchise_id":
            return total

        # Phantom byes carry no postseason flags per project policy
        # (memory: feedback_phantom_rows_no_flags). The is_playoffs / is_consolation
        # zeroing is redundant after the Step 0 change but kept explicit so this
        # write remains the canonical "phantom row shape" enforcer.
        set_clauses = ["is_consolation = 0", "is_playoffs = 0", "postseason = 0"]
        # Zero out odds
        for odds_col in ["p_playoffs", "p_bye", "p_semis", "p_final", "p_champ"]:
            if odds_col in matchup_cols:
                set_clauses.append(f"{odds_col} = 0")
        # No game played — explicit NULLs/zeros
        for null_col in [
            "team_points",
            "opponent_points",
            "margin",
            "optimal_points",
            "bench_points",
            "total_player_points",
        ]:
            if null_col in matchup_cols:
                set_clauses.append(f"{null_col} = NULL")
        for zero_col in ["win", "loss", "tie"]:
            if zero_col in matchup_cols:
                set_clauses.append(f"{zero_col} = 0")

        # Carry forward cumulative stats from last real game.
        # Every column here must freeze the team's state at elimination
        # so bye rows are proper placeholders (UI, validators, aggregations).
        _carry_forward_candidates = [
            # Record
            "wins_to_date",
            "losses_to_date",
            "ties_to_date",
            "points_scored_to_date",
            "cumulative_wins",
            "cumulative_losses",
            "cumulative_ties",
            "win_streak",
            "loss_streak",
            # Rating / model
            "power_rating",
            "team_mu",
            "team_sigma",
            "felo_score",
            "felo_tier",
            # Seeding / placement
            "placement_rank",
            "final_playoff_seed",
            "playoff_seed_to_date",
            "season_result",
            # Manager season/career stats
            "manager_season_mean",
            "manager_season_median",
            "manager_all_time_ranking",
            "manager_all_time_percentile",
            "manager_all_time_gp",
            "manager_all_time_wins",
            "manager_all_time_losses",
            "manager_all_time_ties",
            "manager_all_time_win_pct",
        ]
        _carry_forward_cols = [c for c in _carry_forward_candidates if c in matchup_cols]

        for col in _carry_forward_cols:
            set_clauses.append(
                f"{col} = COALESCE("
                f"(SELECT g.{col} FROM {matchup_table} g "
                f"WHERE g.year = b.year AND g.{pflag_fid_col} = b.{pflag_fid_col} "
                "AND COALESCE(g.is_bye_week, 0) = 0 "
                f"ORDER BY g.week DESC LIMIT 1), b.{col})"
            )

        sql_phantoms = f"""
            WITH {playoff_start_cte}
            UPDATE {matchup_table} b
            SET {", ".join(set_clauses)}
            FROM playoff_weeks pw
            WHERE b.year = pw.year
              AND COALESCE(b.is_bye_week, 0) = 1
              AND b.week >= pw.playoff_start
              AND {self._db_filter("b")}
              AND NOT EXISTS (
                  SELECT 1 FROM {matchup_table} g
                  WHERE g.year = b.year AND g.{pflag_fid_col} = b.{pflag_fid_col}
                  AND g.week > b.week AND COALESCE(g.is_bye_week, 0) = 0
                  AND {self._db_filter("g")}
              )
        """
        total += self._execute(sql_phantoms, "enforce_postseason_flags: Fix phantom byes (zero odds, carry record)")

        # Step 3: Ensure every manager has a row for every playoff week.
        sql_fill_gaps = f"""
            WITH {playoff_start_cte},
            all_managers AS (
                SELECT DISTINCT year, manager, franchise_id
                FROM {matchup_table}
                WHERE franchise_id IS NOT NULL
                  AND TRIM(CAST(franchise_id AS VARCHAR)) <> ''
                  AND COALESCE(is_bye_week, 0) = 0
                  AND {self._db_filter()}
            ),
            all_playoff_weeks AS (
                SELECT pw.year, w.week, am.manager, am.franchise_id
                FROM playoff_weeks pw
                CROSS JOIN (
                    SELECT DISTINCT week
                    FROM {matchup_table}
                    WHERE {self._db_filter()}
                ) w
                JOIN all_managers am ON am.year = pw.year
                WHERE w.week >= pw.playoff_start
                AND w.week <= (
                    SELECT MAX(week)
                    FROM {matchup_table}
                    WHERE year = pw.year
                      AND {self._db_filter()}
                )
            ),
            missing AS (
                SELECT apw.*
                FROM all_playoff_weeks apw
                WHERE NOT EXISTS (
                    SELECT 1 FROM {matchup_table} m
                    WHERE m.year = apw.year AND m.week = apw.week
                      AND m.{pflag_fid_col} = apw.{pflag_fid_col}
                      AND {self._db_filter("m")}
                )
            )
            INSERT INTO {matchup_table}
                (db_name, year, week, cumulative_week, manager_week, manager, franchise_id,
                 is_bye_week, is_playoffs, is_consolation)
            SELECT
                '{self.db_name}',
                year,
                week,
                (CAST(year AS BIGINT) * 100 + CAST(week AS BIGINT)),
                TRIM(franchise_id) || '_' || CAST(year AS VARCHAR) || '_' || CAST(week AS VARCHAR),
                manager,
                franchise_id,
                1,
                0,
                0
            FROM missing
        """
        try:
            filled = self._execute(sql_fill_gaps, "enforce_postseason_flags: Fill missing playoff week rows")
            total += filled
        except Exception as e:
            logger.warning(f"[enforce_postseason_flags] Fill gaps failed (non-fatal): {e}")

        # Step 4: Carry forward cumulative stats onto ALL bye rows
        # (covers both phantom byes from Step 2 AND gap-filled rows from Step 3)
        _carry_cols_step4 = [c for c in _carry_forward_cols if c in matchup_cols]
        if _carry_cols_step4:
            _carry_set = ", ".join(
                f"{col} = COALESCE("
                f"(SELECT g.{col} FROM {matchup_table} g "
                f"WHERE g.year = b.year AND g.{pflag_fid_col} = b.{pflag_fid_col} "
                "AND COALESCE(g.is_bye_week, 0) = 0 "
                f"AND {self._db_filter('g')} "
                f"ORDER BY g.week DESC LIMIT 1), b.{col})"
                for col in _carry_cols_step4
            )
            sql_carry_stats = f"""
                WITH {playoff_start_cte}
                UPDATE {matchup_table} b
                SET {_carry_set}
                FROM playoff_weeks pw
                WHERE b.year = pw.year
                  AND COALESCE(b.is_bye_week, 0) = 1
                  AND b.week >= pw.playoff_start
                  AND {self._db_filter("b")}
            """
            total += self._execute(
                sql_carry_stats,
                "enforce_postseason_flags: Carry forward stats to all postseason bye rows",
            )

        return total

    def propagate_2week_round_flags(self) -> int:
        """Propagate playoff/consolation flags for 2-week playoff rounds.

        ESPN 2-week rounds report scores only on the first week (e.g., week 14
        has team_points, week 15 has team_points=0). The bracket tracers flag
        week 14 but not week 15. This propagates: if a franchise has is_playoffs
        or is_consolation on week N, and plays week N+1 with team_points=0 and
        no flag, copy the flag forward.

        MUST run AFTER shape_playoff_bracket_local + shape_consolation_bracket_local.
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        if "franchise_id" not in matchup_cols:
            return 0

        _propagate_cols = ["is_playoffs", "is_consolation", "postseason"]
        _propagate_extras = [
            col for col in ["playoff_round", "playoff_round_num", "playoff_week_index"] if col in matchup_cols
        ]
        _prop_set = ", ".join(
            f"{col} = prev.{col}" for col in _propagate_cols + _propagate_extras if col in matchup_cols
        )
        if not _prop_set:
            return 0

        # Run twice: once for week N→N+1, once for N+1→N+2 (handles 3+ week rounds)
        total = 0
        for pass_num in range(2):
            sql = f"""
                UPDATE {matchup_table} m
                SET {_prop_set}
                FROM {matchup_table} prev
                WHERE prev.year = m.year
                  AND prev.week = m.week - 1
                  AND prev.franchise_id = m.franchise_id
                  AND (COALESCE(CAST(prev.is_playoffs AS INT), 0) = 1
                       OR COALESCE(CAST(prev.is_consolation AS INT), 0) = 1)
                  AND COALESCE(CAST(m.is_playoffs AS INT), 0) = 0
                  AND COALESCE(CAST(m.is_consolation AS INT), 0) = 0
                  AND COALESCE(CAST(m.is_bye_week AS INT), 0) = 0
                  AND {self._db_filter("m")}
                  AND {self._db_filter("prev")}
            """
            total += self._execute(sql, f"propagate_2week_round_flags (pass {pass_num + 1})")
        return total

    def compute_derived_matchup_columns(self) -> int:
        """Compute cross-platform matchup columns that can be derived from core data.

        These columns are provided by Yahoo's API but must be computed for
        Sleeper and ESPN. Safe to run on all platforms — existing values
        are only overwritten if NULL.

        Columns computed:
        - total_matchup_score: team_points + opponent_points
        - close_margin: 1 if margin < 5% of total score, else 0
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")
        conn = self._get_connection()
        total = 0

        # Add columns if missing
        matchup_cols = self._ensure_columns(
            matchup_table,
            {
                "total_matchup_score": "DOUBLE",
                "close_margin": "INTEGER",
                "expected_spread": "DOUBLE",
                "expected_odds": "DOUBLE",
                "win_probability": "DOUBLE",
                "underdog_wins": "INTEGER",
                "favorite_losses": "INTEGER",
                "is_championship": "INTEGER",
            },
            matchup_cols,
        )
        self._invalidate_column_cache("matchup")
        matchup_cols = self._get_table_columns("matchup")

        # total_matchup_score = team_points + opponent_points (fill NULLs only)
        sql_tms = f"""
            UPDATE {matchup_table}
            SET total_matchup_score = ROUND(team_points + opponent_points, 2)
            WHERE total_matchup_score IS NULL
              AND team_points IS NOT NULL
              AND opponent_points IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql_tms, "derived: total_matchup_score")

        # close_margin = 1 when abs(margin) is within threshold
        # Threshold: margin within 5% of total score, minimum 3 points
        sql_cm = f"""
            UPDATE {matchup_table}
            SET close_margin = CASE
                WHEN ABS(COALESCE(margin, team_points - opponent_points))
                     <= GREATEST(0.05 * (team_points + opponent_points), 3)
                THEN 1 ELSE 0 END
            WHERE close_margin IS NULL
              AND team_points IS NOT NULL
              AND opponent_points IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql_cm, "derived: close_margin")

        # is_championship = 1 for championship round matchups (including multi-week)
        # Derived from playoff_round column which is set by all platforms
        playoff_round_type = ""
        try:
            describe_rows = conn.execute(f"DESCRIBE {matchup_table}").fetchall()
            col_types = {str(row[0]): str(row[1]).upper() for row in describe_rows}
            playoff_round_type = col_types.get("playoff_round", "")
        except Exception:
            playoff_round_type = ""

        if "playoff_round" in matchup_cols and any(
            token in playoff_round_type for token in ["CHAR", "TEXT", "STRING", "VARCHAR"]
        ):
            sql_ic = f"""
                UPDATE {matchup_table}
                SET is_championship = CASE
                    WHEN LOWER(COALESCE(playoff_round, '')) = 'championship' THEN 1
                    ELSE 0 END
                WHERE (is_championship IS NULL OR is_championship = 0)
                  AND {self._db_filter()}
            """
            total += self._execute(sql_ic, "derived: is_championship")
        elif "is_championship" in matchup_cols and "championship" in matchup_cols:
            sql_ic_fallback = f"""
                UPDATE {matchup_table}
                SET is_championship = CASE WHEN COALESCE(CAST(championship AS INT), 0) = 1 THEN 1 ELSE 0 END
                WHERE (is_championship IS NULL OR is_championship = 0)
                  AND {self._db_filter()}
            """
            total += self._execute(sql_ic_fallback, "derived: is_championship fallback")

        # Sim-derived columns are computed separately via
        # populate_sim_derived_matchup_columns(), which must run AFTER
        # playoff_odds_import has populated team_mu/team_sigma. See that
        # method for the derivation of expected_spread, expected_odds,
        # win_probability, underdog_wins, and favorite_losses.
        return total

    def populate_sim_derived_matchup_columns(self) -> int:
        """Compute matchup columns that depend on team_mu/team_sigma from sims.

        This was previously the tail of compute_derived_matchup_columns but
        had to be split out because compute_derived_matchup_columns runs in
        the SQLEnrichments Wave 3 (during the main import) — BEFORE
        playoff_odds_import.py writes team_mu/team_sigma — so it always hit
        the "team_mu not present" skip and the columns stayed NULL. The
        validator was then flagging every row:
            sim_expected_odds_populated: 7922 fails across 4 pilots
        by filtering on `expected_odds IS NULL AND p_playoffs IS NOT NULL`.

        Called from playoff_odds_import.py main() after process_parquet_files
        completes, so team_mu / team_sigma are fully populated by then.

        Columns written:
          expected_spread  = team_mu − opponent_team_mu
          expected_odds    = logistic-approx normal CDF of the strength
                             difference (win probability)
          win_probability  = expected_odds (Yahoo alias)
          underdog_wins    = 1 if expected_odds < 0.5 AND win = 1
          favorite_losses  = 1 if expected_odds > 0.5 AND loss = 1

        Idempotent via `IS NULL` guards on every UPDATE — safe to re-run.
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        if "team_mu" not in matchup_cols or "team_sigma" not in matchup_cols:
            logger.info(
                "[populate_sim_derived_matchup_columns] team_mu/team_sigma "
                "not present — playoff_odds_import.py must run first"
            )
            return 0

        # Ensure all sim-derived columns exist before any UPDATE so we don't
        # explode on columns added by a schema change that hasn't propagated.
        matchup_cols = self._ensure_columns(
            matchup_table,
            {
                "expected_spread": "DOUBLE",
                "expected_odds": "DOUBLE",
                "win_probability": "DOUBLE",
                "underdog_wins": "INTEGER",
                "favorite_losses": "INTEGER",
            },
            matchup_cols,
        )
        self._invalidate_column_cache("matchup")

        total = 0

        # expected_spread = team_mu - opponent_mu (via self-join on same matchup)
        sql_spread = f"""
            UPDATE {matchup_table} m
            SET expected_spread = ROUND(m.team_mu - opp.team_mu, 2)
            FROM {matchup_table} opp
            WHERE m.year = opp.year AND m.week = opp.week
              AND m.franchise_id = opp.opponent_franchise_id
              AND m.opponent_franchise_id = opp.franchise_id
              AND m.expected_spread IS NULL
              AND m.team_mu IS NOT NULL AND opp.team_mu IS NOT NULL
              AND {self._db_filter("m")}
              AND {self._db_filter("opp")}
        """
        total += self._execute(sql_spread, "sim_derived: expected_spread")

        # expected_odds = win probability from normal CDF of strength difference
        # P(win) = Phi((mu_team - mu_opp) / sqrt(sigma_team^2 + sigma_opp^2))
        # DuckDB doesn't have a normal CDF, so use the logistic approximation:
        # Phi(x) ≈ 1 / (1 + exp(-1.7 * x))
        sql_odds = f"""
            UPDATE {matchup_table} m
            SET expected_odds = ROUND(
                1.0 / (1.0 + EXP(-1.7 * (m.team_mu - opp.team_mu)
                    / GREATEST(SQRT(m.team_sigma * m.team_sigma + opp.team_sigma * opp.team_sigma), 0.1)
                )), 4)
            FROM {matchup_table} opp
            WHERE m.year = opp.year AND m.week = opp.week
              AND m.franchise_id = opp.opponent_franchise_id
              AND m.opponent_franchise_id = opp.franchise_id
              AND m.expected_odds IS NULL
              AND m.team_mu IS NOT NULL AND opp.team_mu IS NOT NULL
              AND m.team_sigma IS NOT NULL AND opp.team_sigma IS NOT NULL
              AND {self._db_filter("m")}
              AND {self._db_filter("opp")}
        """
        total += self._execute(sql_odds, "sim_derived: expected_odds")

        # win_probability = same as expected_odds (Yahoo uses this name)
        sql_wp = f"""
            UPDATE {matchup_table}
            SET win_probability = expected_odds
            WHERE win_probability IS NULL AND expected_odds IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql_wp, "sim_derived: win_probability")

        # underdog_wins = 1 when expected_odds < 0.5 AND win = 1
        sql_uw = f"""
            UPDATE {matchup_table}
            SET underdog_wins = CASE
                WHEN expected_odds < 0.5 AND COALESCE(CAST(win AS INT), 0) = 1 THEN 1
                ELSE 0 END
            WHERE underdog_wins IS NULL AND expected_odds IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql_uw, "sim_derived: underdog_wins")

        # favorite_losses = 1 when expected_odds > 0.5 AND loss = 1
        sql_fl = f"""
            UPDATE {matchup_table}
            SET favorite_losses = CASE
                WHEN expected_odds > 0.5 AND COALESCE(CAST(loss AS INT), 0) = 1 THEN 1
                ELSE 0 END
            WHERE favorite_losses IS NULL AND expected_odds IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql_fl, "sim_derived: favorite_losses")

        return total

    def normalize_matchup_flags(self) -> int:
        """Normalize boolean/int flags to avoid NULL drift.

        Ensures is_bye_week, is_playoffs, is_consolation are boolean FALSE when unset,
        and is_final_regular_week is integer 0 when unset. Does NOT infer playoffs
        or consolation from bye weeks.
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        set_parts = []
        null_checks = []

        if "is_bye_week" in matchup_cols:
            set_parts.append("is_bye_week = COALESCE(CAST(is_bye_week AS BOOLEAN), FALSE)")
            null_checks.append("is_bye_week IS NULL")
        if "is_playoffs" in matchup_cols:
            set_parts.append("is_playoffs = COALESCE(CAST(is_playoffs AS BOOLEAN), FALSE)")
            null_checks.append("is_playoffs IS NULL")
        if "is_consolation" in matchup_cols:
            set_parts.append("is_consolation = COALESCE(CAST(is_consolation AS BOOLEAN), FALSE)")
            null_checks.append("is_consolation IS NULL")
        if "is_final_regular_week" in matchup_cols:
            set_parts.append("is_final_regular_week = COALESCE(CAST(is_final_regular_week AS INTEGER), 0)")
            null_checks.append("is_final_regular_week IS NULL")

        if not set_parts:
            return 0

        where_clause = f"AND ({' OR '.join(null_checks)})" if null_checks else ""
        sql = f"""
            UPDATE {matchup_table}
            SET {", ".join(set_parts)}
            WHERE {self._db_filter()}
            {where_clause}
        """
        total = self._execute(sql, "normalize_matchup_flags: fill NULL booleans")

        # Harmonize postseason flags across duplicate team-week rows. A literal
        # consolation label wins: Yahoo marks its consolation-bracket rows with
        # both raw flags, while the canonical representation is (0, 1).
        if "is_playoffs" in matchup_cols and "is_consolation" in matchup_cols and "franchise_id" in matchup_cols:
            id_col = "franchise_id"
            sql_flags = f"""
                WITH flags AS (
                    SELECT db_name, year, week, {id_col} AS team_id,
                           MAX(CASE WHEN COALESCE(CAST(is_playoffs AS INT), 0) = 1 THEN 1 ELSE 0 END) AS any_playoffs,
                           MAX(CASE WHEN COALESCE(CAST(is_consolation AS INT), 0) = 1 THEN 1 ELSE 0 END) AS any_consolation
                    FROM {matchup_table}
                    WHERE {self._db_filter()}
                    GROUP BY db_name, year, week, {id_col}
                )
                UPDATE {matchup_table} m
                SET is_playoffs = CASE WHEN f.any_consolation = 1 THEN 0 ELSE f.any_playoffs END,
                    is_consolation = f.any_consolation
                FROM flags f
                WHERE m.db_name = f.db_name
                  AND m.year = f.year
                  AND m.week = f.week
                  AND m.{id_col} = f.team_id
                  AND {self._db_filter("m")}
                  AND (
                      COALESCE(CAST(m.is_playoffs AS INT), 0) != CASE WHEN f.any_consolation = 1 THEN 0 ELSE f.any_playoffs END
                      OR COALESCE(CAST(m.is_consolation AS INT), 0) != f.any_consolation
                  )
            """
            total += self._execute(sql_flags, "normalize_matchup_flags: harmonize postseason flags per team-week")

        return total

    def compute_win_loss_and_projections(self) -> int:
        """Compute win/loss flags and projection-based metrics.

        Cross-platform — works on Yahoo, Sleeper, ESPN identically.
        All columns derived from team_points, opponent_points, team_projected_points.
        Fills NULLs only — won't overwrite existing values.

        Columns set:
        - win, loss, tie: binary flags from team_points vs opponent_points
        - proj_score_error, abs_proj_score_error: actual - projected
        - above_proj_score, below_proj_score: binary flags
        - proj_wins, proj_losses: projected outcome flags
        - win_vs_spread, lose_vs_spread: actual vs expected spread
        - gpa: GPA from letter grade (A+ → 4.0, F → 0.0)
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")
        conn = self._get_connection()
        total = 0

        # Ensure columns exist
        matchup_cols = self._ensure_columns(
            matchup_table,
            {
                "win": "INTEGER",
                "loss": "INTEGER",
                "tie": "INTEGER",
                "proj_score_error": "DOUBLE",
                "abs_proj_score_error": "DOUBLE",
                "above_proj_score": "INTEGER",
                "below_proj_score": "INTEGER",
                "proj_wins": "INTEGER",
                "proj_losses": "INTEGER",
                "win_vs_spread": "INTEGER",
                "lose_vs_spread": "INTEGER",
                "gpa": "DOUBLE",
            },
            matchup_cols,
        )
        self._invalidate_column_cache("matchup")

        # Batch 1: win/loss/tie + projection metrics in one UPDATE
        sql = f"""
            UPDATE {matchup_table}
            SET
                win = CASE WHEN team_points > opponent_points THEN 1 ELSE 0 END,
                loss = CASE WHEN team_points < opponent_points THEN 1 ELSE 0 END,
                tie = CASE WHEN team_points = opponent_points THEN 1 ELSE 0 END,
                proj_score_error = CASE WHEN team_projected_points IS NOT NULL
                    THEN ROUND(team_points - team_projected_points, 2) END,
                abs_proj_score_error = CASE WHEN team_projected_points IS NOT NULL
                    THEN ROUND(ABS(team_points - team_projected_points), 2) END,
                above_proj_score = CASE WHEN team_projected_points IS NOT NULL
                    THEN CASE WHEN team_points > team_projected_points THEN 1 ELSE 0 END END,
                below_proj_score = CASE WHEN team_projected_points IS NOT NULL
                    THEN CASE WHEN team_points < team_projected_points THEN 1 ELSE 0 END END,
                proj_wins = CASE WHEN team_projected_points IS NOT NULL
                    AND opponent_projected_points IS NOT NULL
                    THEN CASE WHEN team_projected_points > opponent_projected_points THEN 1 ELSE 0 END END,
                proj_losses = CASE WHEN team_projected_points IS NOT NULL
                    AND opponent_projected_points IS NOT NULL
                    THEN CASE WHEN team_projected_points < opponent_projected_points THEN 1 ELSE 0 END END
            WHERE win IS NULL
              AND team_points IS NOT NULL
              AND opponent_points IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql, "derived: win/loss/tie + projections")

        # Batch 1b: projection cascade follow-up. Batch 1 only fires when
        # `win` is NULL, but all platform fetchers already populate win/loss/
        # tie from the api (canonical schema marks them "api" source). That
        # means the projection-cascade columns (proj_score_error, proj_wins,
        # above/below_proj_score) never get written by batch 1 — they depend
        # on team_projected_points, which is populated in an earlier step
        # (populate_team_projected_points) but cascades only when batch 1
        # fires. This follow-up UPDATE targets rows where team projections
        # exist but the derived columns are still NULL. Idempotent via the
        # proj_score_error IS NULL guard.
        sql_proj_cascade = f"""
            UPDATE {matchup_table}
            SET
                proj_score_error = ROUND(team_points - team_projected_points, 2),
                abs_proj_score_error = ROUND(ABS(team_points - team_projected_points), 2),
                above_proj_score = CASE WHEN team_points > team_projected_points THEN 1 ELSE 0 END,
                below_proj_score = CASE WHEN team_points < team_projected_points THEN 1 ELSE 0 END,
                proj_wins = CASE WHEN opponent_projected_points IS NOT NULL
                    AND team_projected_points > opponent_projected_points THEN 1
                    WHEN opponent_projected_points IS NOT NULL THEN 0
                    ELSE NULL END,
                proj_losses = CASE WHEN opponent_projected_points IS NOT NULL
                    AND team_projected_points < opponent_projected_points THEN 1
                    WHEN opponent_projected_points IS NOT NULL THEN 0
                    ELSE NULL END
            WHERE proj_score_error IS NULL
              AND team_projected_points IS NOT NULL
              AND team_points IS NOT NULL
              AND {self._db_filter()}
        """
        total += self._execute(sql_proj_cascade, "derived: projection cascade (follow-up when win pre-populated)")

        # Batch 2: spread-based metrics (depend on expected_spread from compute_derived_matchup_columns)
        if "expected_spread" in self._get_table_columns("matchup"):
            sql_spread = f"""
                UPDATE {matchup_table}
                SET
                    win_vs_spread = CASE WHEN margin > expected_spread THEN 1 ELSE 0 END,
                    lose_vs_spread = CASE WHEN margin < expected_spread THEN 1 ELSE 0 END
                WHERE win_vs_spread IS NULL
                  AND margin IS NOT NULL
                  AND expected_spread IS NOT NULL
                  AND {self._db_filter()}
            """
            total += self._execute(sql_spread, "derived: win/lose vs spread")

        # Normalize empty grade strings to NULL before GPA mapping.
        if "grade" in self._get_table_columns("matchup"):
            sql_grade_clean = f"""
                UPDATE {matchup_table}
                SET grade = NULL
                WHERE grade IS NOT NULL
                  AND TRIM(grade) = ''
                  AND {self._db_filter()}
            """
            total += self._execute(sql_grade_clean, "derived: normalize empty grade")

        # Batch 3: GPA from letter grade
        if "grade" in self._get_table_columns("matchup"):
            sql_gpa = f"""
                UPDATE {matchup_table}
                SET gpa = CASE grade
                    WHEN 'A+' THEN 4.0 WHEN 'A' THEN 3.85 WHEN 'A-' THEN 3.7
                    WHEN 'B+' THEN 3.35 WHEN 'B' THEN 3.0 WHEN 'B-' THEN 2.7
                    WHEN 'C+' THEN 2.35 WHEN 'C' THEN 2.0 WHEN 'C-' THEN 1.7
                    WHEN 'D+' THEN 1.35 WHEN 'D' THEN 1.0 WHEN 'D-' THEN 0.7
                    WHEN 'F' THEN 0.0
                    ELSE NULL END
                WHERE gpa IS NULL AND grade IS NOT NULL
                  AND {self._db_filter()}
            """
            total += self._execute(sql_gpa, "derived: gpa from grade")

        return total

    def compute_league_weekly_stats(self) -> int:
        """Compute league-wide weekly aggregates and all-play records.

        Cross-platform — works on Yahoo, Sleeper, ESPN identically.
        Uses window functions for efficient single-pass computation.

        Columns set:
        - weekly_mean, weekly_median: per-manager averages (same as team_points for single matchup)
        - league_weekly_mean, league_weekly_median: league-wide per week
        - above_league_median, below_league_median: binary flags
        - teams_beat_this_week, opponent_teams_beat_this_week: all-play record
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")
        total = 0

        # Ensure columns exist
        matchup_cols = self._ensure_columns(
            matchup_table,
            {
                "weekly_mean": "DOUBLE",
                "weekly_median": "DOUBLE",
                "league_weekly_mean": "DOUBLE",
                "league_weekly_median": "DOUBLE",
                "above_league_median": "INTEGER",
                "below_league_median": "INTEGER",
                "teams_beat_this_week": "INTEGER",
                "opponent_teams_beat_this_week": "INTEGER",
            },
            matchup_cols,
        )
        self._invalidate_column_cache("matchup")

        # Two-step approach: league stats first, then all-play
        # (DuckDB can't reference UPDATE target alias in JOIN ON clauses)

        # Step 1: League weekly mean/median
        sql_league = f"""
            WITH league_stats AS (
                SELECT year, week,
                    AVG(team_points) AS lw_mean,
                    MEDIAN(team_points) AS lw_median
                FROM {matchup_table}
                WHERE team_points IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY year, week
            )
            UPDATE {matchup_table} t
            SET
                weekly_mean = t.team_points,
                weekly_median = t.team_points,
                league_weekly_mean = ROUND(ls.lw_mean, 2),
                league_weekly_median = ROUND(ls.lw_median, 2),
                above_league_median = CASE WHEN t.team_points > ls.lw_median THEN 1 ELSE 0 END,
                below_league_median = CASE WHEN t.team_points < ls.lw_median THEN 1 ELSE 0 END
            FROM league_stats ls
            WHERE t.year = ls.year AND t.week = ls.week
              AND t.league_weekly_mean IS NULL
              AND t.team_points IS NOT NULL
              AND {self._db_filter("t")}
        """
        total += self._execute(sql_league, "derived: league weekly mean/median")

        # Step 2: All-play records (teams beaten this week)
        sql_allplay = f"""
            WITH all_play AS (
                SELECT
                    m.year, m.week, m.franchise_id AS _mgr_key,
                    COUNT(*) FILTER (WHERE m.team_points > o.team_points) AS beats
                FROM {matchup_table} m
                CROSS JOIN (
                    SELECT DISTINCT year, week, franchise_id AS _mgr_key, team_points
                    FROM {matchup_table}
                    WHERE team_points IS NOT NULL
                      AND {self._db_filter()}
                ) o
                WHERE m.year = o.year AND m.week = o.week
                  AND m.franchise_id != o._mgr_key
                  AND m.team_points IS NOT NULL
                  AND {self._db_filter("m")}
                GROUP BY m.year, m.week, m.franchise_id
            )
            UPDATE {matchup_table} t
            SET teams_beat_this_week = ap.beats
            FROM all_play ap
            WHERE t.year = ap.year AND t.week = ap.week
              AND t.franchise_id = ap._mgr_key
              AND t.teams_beat_this_week IS NULL
              AND {self._db_filter("t")}
        """
        total += self._execute(sql_allplay, "derived: all-play (teams beat this week)")

        # Step 3: Opponent's all-play record
        sql_opp = f"""
            UPDATE {matchup_table} t
            SET opponent_teams_beat_this_week = opp.teams_beat_this_week
            FROM {matchup_table} opp
            WHERE t.year = opp.year AND t.week = opp.week
              AND t.opponent_franchise_id = opp.franchise_id
              AND opp.teams_beat_this_week IS NOT NULL
              AND t.opponent_teams_beat_this_week IS NULL
              AND {self._db_filter("t")}
              AND {self._db_filter("opp")}
        """
        total += self._execute(sql_opp, "derived: opponent all-play")

        return total

    # =========================================================================
    # Cumulative matchup enrichments (replaces cumulative_stats.py)
    # =========================================================================

    def cumulative_records(self) -> int:
        """Calculate cumulative win/loss records and streaks.

        Sets on matchup table:
        - wins_to_date, losses_to_date, ties_to_date (season, regular season only)
        - points_scored_to_date (season, excludes consolation)
        - cumulative_wins, cumulative_losses, cumulative_ties (all-time running total)
        - win_streak, loss_streak (current active streak, resets on opposite outcome)
        - playoff_seed_to_date (weekly standing rank by record + points)
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        required = {"franchise_id", "year", "week", "win", "loss", "team_points"}
        if not required.issubset(matchup_cols):
            missing = required - matchup_cols
            logger.warning(f"[cumulative_records] Missing columns: {missing}")
            return 0

        total = 0

        # --- Season-to-date records (regular season only) ---
        if "wins_to_date" in matchup_cols:
            has_consolation = "is_consolation" in matchup_cols
            consolation_filter = "AND COALESCE(is_consolation, 0) != 1" if has_consolation else ""
            consolation_filter_pts = "AND COALESCE(is_consolation, 0) != 1" if has_consolation else ""

            # H2H+Median: when uses_median is true for a year, include
            # above_league_median wins/losses in the running totals so that
            # playoff seeding (which derives from wins_to_date) is correct.
            has_median_col = "above_league_median" in matchup_cols
            settings_table = self._qualified_name("league_settings") if self._table_exists("league_settings") else None
            use_median_join = has_median_col and settings_table is not None

            if use_median_join:
                median_win_expr = (
                    "+ CASE WHEN COALESCE(ls.uses_median, false) THEN COALESCE(mt.above_league_median, 0) ELSE 0 END"
                )
                median_loss_expr = (
                    "+ CASE WHEN COALESCE(ls.uses_median, false) AND mt.above_league_median = 0 THEN 1 ELSE 0 END"
                )
                from_clause = f"{matchup_table} mt LEFT JOIN {settings_table} ls ON mt.year = ls.year"
                t = "mt"
            else:
                median_win_expr = ""
                median_loss_expr = ""
                from_clause = f"{matchup_table} mt"
                t = "mt"

            sql_season = f"""
                UPDATE {matchup_table} m SET
                    wins_to_date = sub.wins_td,
                    losses_to_date = sub.losses_td,
                    ties_to_date = sub.ties_td,
                    points_scored_to_date = sub.pts_td
                FROM (
                    SELECT {t}.franchise_id, {t}.year, {t}.week,
                        SUM(CASE WHEN COALESCE(CAST({t}.is_playoffs AS INT), 0) != 1
                                      {consolation_filter.replace("is_consolation", f"{t}.is_consolation")}
                                 THEN COALESCE({t}.win, 0) {median_win_expr} ELSE 0 END)
                            OVER (PARTITION BY {t}.franchise_id, {t}.year ORDER BY {t}.week
                                  ROWS UNBOUNDED PRECEDING) AS wins_td,
                        SUM(CASE WHEN COALESCE(CAST({t}.is_playoffs AS INT), 0) != 1
                                      {consolation_filter.replace("is_consolation", f"{t}.is_consolation")}
                                 THEN COALESCE({t}.loss, 0) {median_loss_expr} ELSE 0 END)
                            OVER (PARTITION BY {t}.franchise_id, {t}.year ORDER BY {t}.week
                                  ROWS UNBOUNDED PRECEDING) AS losses_td,
                        SUM(CASE WHEN COALESCE(CAST({t}.is_playoffs AS INT), 0) != 1
                                      {consolation_filter.replace("is_consolation", f"{t}.is_consolation")}
                                 THEN COALESCE({t}.tie, 0) ELSE 0 END)
                            OVER (PARTITION BY {t}.franchise_id, {t}.year ORDER BY {t}.week
                                  ROWS UNBOUNDED PRECEDING) AS ties_td,
                        SUM(CASE WHEN 1=1 {consolation_filter_pts.replace("is_consolation", f"{t}.is_consolation")}
                                 THEN COALESCE({t}.team_points, 0) ELSE 0 END)
                            OVER (PARTITION BY {t}.franchise_id, {t}.year ORDER BY {t}.week
                                  ROWS UNBOUNDED PRECEDING) AS pts_td
                FROM {from_clause}
                WHERE {t}.franchise_id IS NOT NULL
                  AND {self._db_filter(t)}
                ) sub WHERE m.year = sub.year AND m.week = sub.week
                  AND m.franchise_id = sub.franchise_id
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_season, "cumulative_records: Season-to-date W/L/T + points")

        # --- All-time cumulative records (running total across years) ---
        if "cumulative_wins" in matchup_cols:
            sql_alltime = f"""
                UPDATE {matchup_table} m SET
                    cumulative_wins = sub.cum_w,
                    cumulative_losses = sub.cum_l,
                    cumulative_ties = sub.cum_t
                FROM (
                    SELECT franchise_id, year, week,
                        SUM(COALESCE(win, 0))
                            OVER (PARTITION BY franchise_id ORDER BY year, week
                                  ROWS UNBOUNDED PRECEDING) AS cum_w,
                        SUM(COALESCE(loss, 0))
                            OVER (PARTITION BY franchise_id ORDER BY year, week
                                  ROWS UNBOUNDED PRECEDING) AS cum_l,
                        SUM(COALESCE(tie, 0))
                            OVER (PARTITION BY franchise_id ORDER BY year, week
                                  ROWS UNBOUNDED PRECEDING) AS cum_t
                    FROM {matchup_table}
                    WHERE franchise_id IS NOT NULL
                      AND {self._db_filter()}
                ) sub WHERE m.year = sub.year AND m.week = sub.week
                  AND m.franchise_id = sub.franchise_id
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_alltime, "cumulative_records: All-time cumulative W/L/T")

        # --- Win/loss streaks (gap-and-island approach) ---
        if "win_streak" in matchup_cols and "loss_streak" in matchup_cols:
            has_tie = "tie" in matchup_cols
            has_consolation = "is_consolation" in matchup_cols
            has_bye = "is_bye_week" in matchup_cols
            has_placeholder = "is_placeholder" in matchup_cols
            tie_result_filter = "OR COALESCE(CAST(tie AS INT), 0) = 1" if has_tie else ""
            tie_select = "COALESCE(CAST(tie AS INT), 0)" if has_tie else "0"
            consolation_filter = "AND COALESCE(CAST(is_consolation AS INT), 0) != 1" if has_consolation else ""
            bye_filter = "AND COALESCE(CAST(is_bye_week AS INT), 0) != 1" if has_bye else ""
            placeholder_filter = "AND COALESCE(CAST(is_placeholder AS INT), 0) != 1" if has_placeholder else ""
            sql_clear_streaks = f"""
                UPDATE {matchup_table}
                SET win_streak = 0,
                    loss_streak = 0
                WHERE {self._db_filter()}
            """
            total += self._execute(sql_clear_streaks, "cumulative_records: Clear win/loss streaks")

            sql_streaks = f"""
                WITH eligible AS (
                    SELECT
                        franchise_id,
                        year,
                        week,
                        COALESCE(CAST(win AS INT), 0) AS win,
                        COALESCE(CAST(loss AS INT), 0) AS loss,
                        {tie_select} AS tie
                    FROM {matchup_table}
                    WHERE franchise_id IS NOT NULL
                      AND {self._db_filter()}
                      {consolation_filter}
                      {bye_filter}
                      {placeholder_filter}
                      AND (
                        COALESCE(CAST(win AS INT), 0) = 1
                        OR COALESCE(CAST(loss AS INT), 0) = 1
                        {tie_result_filter}
                      )
                ),
                grouped AS (
                    SELECT franchise_id, year, week, win, loss, tie,
                        SUM(CASE WHEN win = 1 THEN 0 ELSE 1 END)
                            OVER (PARTITION BY franchise_id ORDER BY year, week
                                  ROWS UNBOUNDED PRECEDING) AS win_grp,
                        SUM(CASE WHEN loss = 1 THEN 0 ELSE 1 END)
                            OVER (PARTITION BY franchise_id ORDER BY year, week
                                  ROWS UNBOUNDED PRECEDING) AS loss_grp
                    FROM eligible
                ),
                streaks AS (
                    SELECT franchise_id, year, week,
                        CASE WHEN win = 1
                             THEN SUM(CASE WHEN win = 1 THEN 1 ELSE 0 END)
                                  OVER (PARTITION BY franchise_id, win_grp ORDER BY year, week
                                        ROWS UNBOUNDED PRECEDING)
                             ELSE 0 END AS w_streak,
                        CASE WHEN loss = 1
                             THEN SUM(CASE WHEN loss = 1 THEN 1 ELSE 0 END)
                                  OVER (PARTITION BY franchise_id, loss_grp ORDER BY year, week
                                        ROWS UNBOUNDED PRECEDING)
                             ELSE 0 END AS l_streak
                FROM grouped
                )
                UPDATE {matchup_table} m
                SET win_streak = s.w_streak,
                    loss_streak = s.l_streak
                FROM streaks s
                WHERE m.year = s.year AND m.week = s.week
                  AND m.franchise_id = s.franchise_id
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_streaks, "cumulative_records: Win/loss streaks")

        # --- Playoff seed to date (weekly standings rank) ---
        if "playoff_seed_to_date" in matchup_cols and "wins_to_date" in matchup_cols:
            has_consolation = "is_consolation" in matchup_cols
            consolation_filter = "AND COALESCE(is_consolation, 0) != 1" if has_consolation else ""

            sql_seed = f"""
                UPDATE {matchup_table} m SET playoff_seed_to_date = sub.seed
                FROM (
                    SELECT year, week, franchise_id,
                        ROW_NUMBER() OVER (
                            PARTITION BY year, week
                            ORDER BY wins_to_date DESC,
                                     ties_to_date DESC,
                                     points_scored_to_date DESC
                        ) AS seed
                FROM {matchup_table}
                WHERE franchise_id IS NOT NULL
                      AND COALESCE(CAST(is_playoffs AS INT), 0) != 1
                      {consolation_filter}
                      AND {self._db_filter()}
                ) sub WHERE m.year = sub.year AND m.week = sub.week
                  AND m.franchise_id = sub.franchise_id
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_seed, "cumulative_records: Playoff seed to date")

            # Carry forward playoff_seed_to_date into postseason weeks.
            # The RANK above only covers regular season (is_playoffs != 1).
            # Playoff weeks get NULL, but downstream consumers (sims, luck,
            # validators) expect every week to have a seed.  Lock in the last
            # regular-season value for all playoff/consolation weeks.
            sql_carry_seed = f"""
                UPDATE {matchup_table} m SET playoff_seed_to_date = seed_src.seed
                FROM (
                    WITH settings AS (
                        SELECT year, TRY_CAST(playoff_start_week AS INTEGER) AS playoff_start_week
                        FROM {self._qualified_name("league_settings")}
                        WHERE playoff_start_week IS NOT NULL
                          AND {self._db_filter()}
                    ),
                    last_seed AS (
                        SELECT mt.year, mt.franchise_id,
                            TRY_CAST(mt.playoff_seed_to_date AS INTEGER) AS seed,
                            ROW_NUMBER() OVER (
                                PARTITION BY mt.year, mt.franchise_id
                                ORDER BY TRY_CAST(mt.week AS INTEGER) DESC
                            ) AS rn
                        FROM {matchup_table} mt
                        JOIN settings s ON mt.year = s.year
                        WHERE mt.franchise_id IS NOT NULL
                          AND mt.playoff_seed_to_date IS NOT NULL
                          AND TRY_CAST(mt.week AS INTEGER) < s.playoff_start_week
                          AND {self._db_filter("mt")}
                    )
                    SELECT year, franchise_id, seed FROM last_seed WHERE rn = 1
                ) seed_src
                WHERE m.year = seed_src.year
                  AND m.franchise_id = seed_src.franchise_id
                  AND m.playoff_seed_to_date IS NULL
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_carry_seed, "cumulative_records: Carry playoff_seed_to_date into postseason")

        # Freeze each team's end-of-regular-season seed onto all rows for the year.
        # This keeps quick-import/Sleeper paths aligned with the canonical DDL even
        # when the fetcher already supplies playoff structure and skips bracket tracing.
        seed_target_cols = [col for col in ("final_playoff_seed", "playoff_seed") if col in matchup_cols]
        if seed_target_cols and "playoff_seed_to_date" in matchup_cols and self._table_exists("league_settings"):
            settings_table = self._qualified_name("league_settings")
            settings_cols = self._get_table_columns("league_settings")
            if "playoff_start_week" in settings_cols:
                seed_set_parts = []
                if "final_playoff_seed" in seed_target_cols:
                    # COALESCE preserves the bracket tracer's authoritative
                    # seeds (which reflect actual bracket seeding, including
                    # commissioner overrides / reseeding).  On a fresh local
                    # import the column starts NULL so COALESCE fills from
                    # seed_src.seed; after the bracket tracer writes, the
                    # post-bracket run preserves those values.
                    seed_set_parts.append("final_playoff_seed = COALESCE(final_playoff_seed, seed_src.seed)")
                if "playoff_seed" in seed_target_cols:
                    seed_set_parts.append("playoff_seed = COALESCE(playoff_seed, seed_src.seed)")

                sql_freeze_seed = f"""
                    UPDATE {matchup_table} m SET
                        {", ".join(seed_set_parts)}
                    FROM (
                        WITH settings AS (
                            SELECT year, TRY_CAST(playoff_start_week AS INTEGER) AS playoff_start_week
                            FROM {settings_table}
                            WHERE playoff_start_week IS NOT NULL
                              AND {self._db_filter()}
                        ),
                        frozen_seed_source AS (
                            SELECT
                                mt.year,
                                mt.franchise_id,
                                TRY_CAST(mt.playoff_seed_to_date AS INTEGER) AS seed,
                                ROW_NUMBER() OVER (
                                    PARTITION BY mt.year, mt.franchise_id
                                    ORDER BY
                                        CASE
                                            WHEN TRY_CAST(mt.week AS INTEGER) = s.playoff_start_week - 1 THEN 0
                                            ELSE 1
                                        END,
                                        TRY_CAST(mt.week AS INTEGER) DESC
                                ) AS rn
                            FROM {matchup_table} mt
                            JOIN settings s ON mt.year = s.year
                            WHERE mt.franchise_id IS NOT NULL
                              AND mt.playoff_seed_to_date IS NOT NULL
                              AND TRY_CAST(mt.week AS INTEGER) < s.playoff_start_week
                              AND {self._db_filter("mt")}
                        )
                        SELECT year, franchise_id, seed
                        FROM frozen_seed_source
                        WHERE rn = 1
                    ) seed_src
                    WHERE m.year = seed_src.year
                      AND m.franchise_id = seed_src.franchise_id
                      AND {self._db_filter("m")}
                """
                total += self._execute(sql_freeze_seed, "cumulative_records: Freeze final playoff seeds")

        return total

    def manager_season_ppg(self) -> int:
        """Calculate running season PPG (mean + median) per manager.

        Sets: manager_season_mean, manager_season_median
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        if "franchise_id" not in matchup_cols or "team_points" not in matchup_cols:
            logger.warning("[manager_season_ppg] Missing franchise_id or team_points")
            return 0

        total = 0

        # Running mean (window AVG is straightforward)
        if "manager_season_mean" in matchup_cols:
            sql_mean = f"""
                UPDATE {matchup_table} m SET
                    manager_season_mean = sub.avg_pts
                FROM (
                    SELECT franchise_id, year, week,
                        ROUND(AVG(team_points) OVER (
                            PARTITION BY franchise_id, year
                            ORDER BY week
                            ROWS UNBOUNDED PRECEDING
                        ), 2) AS avg_pts
                    FROM {matchup_table}
                    WHERE franchise_id IS NOT NULL
                      AND team_points IS NOT NULL
                      AND {self._db_filter()}
                ) sub WHERE m.year = sub.year AND m.week = sub.week
                  AND m.franchise_id = sub.franchise_id
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_mean, "manager_season_ppg: Running season mean")

        # Season-level median (DuckDB PERCENTILE_CONT doesn't support ROWS frame,
        # so compute full-season median and broadcast to all rows in that season)
        if "manager_season_median" in matchup_cols:
            sql_median = f"""
                WITH season_median AS (
                    SELECT franchise_id, year,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY team_points) AS med_pts
                    FROM {matchup_table}
                    WHERE franchise_id IS NOT NULL
                      AND team_points IS NOT NULL
                      AND {self._db_filter()}
                    GROUP BY franchise_id, year
                )
                UPDATE {matchup_table} m SET
                    manager_season_median = ROUND(sm.med_pts, 2)
                FROM season_median sm
                WHERE m.franchise_id = sm.franchise_id
                  AND m.year = sm.year
                  AND {self._db_filter("m")}
            """
            total += self._execute(sql_median, "manager_season_ppg: Season median")

        return total

    def matchup_rankings(self) -> int:
        """Calculate per-manager matchup rankings (this week vs history).

        Sets: manager_all_time_ranking, manager_all_time_percentile, manager_season_ranking
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        if "franchise_id" not in matchup_cols or "team_points" not in matchup_cols:
            logger.warning("[matchup_rankings] Missing franchise_id or team_points")
            return 0

        target_cols = {"manager_all_time_ranking", "manager_all_time_percentile", "manager_season_ranking"}
        available = target_cols & matchup_cols
        if not available:
            logger.info("[matchup_rankings] No target columns found, skipping")
            return 0

        set_parts = []
        if "manager_all_time_ranking" in matchup_cols:
            set_parts.append("manager_all_time_ranking = sub.at_rank")
        if "manager_all_time_percentile" in matchup_cols:
            set_parts.append("manager_all_time_percentile = sub.at_pctile")
        if "manager_season_ranking" in matchup_cols:
            set_parts.append("manager_season_ranking = sub.s_rank")

        select_parts = [
            "franchise_id, year, week",
            "RANK() OVER (PARTITION BY franchise_id ORDER BY team_points DESC) AS at_rank",
            "ROUND(PERCENT_RANK() OVER (PARTITION BY franchise_id ORDER BY team_points ASC) * 100, 2) AS at_pctile",
            "DENSE_RANK() OVER (PARTITION BY franchise_id, year ORDER BY team_points DESC) AS s_rank",
        ]

        sql = f"""
            UPDATE {matchup_table} m SET
                {", ".join(set_parts)}
            FROM (
            SELECT {", ".join(select_parts)}
            FROM {matchup_table}
            WHERE franchise_id IS NOT NULL
              AND team_points IS NOT NULL
              AND {self._db_filter()}
        ) sub WHERE m.year = sub.year AND m.week = sub.week
              AND m.franchise_id = sub.franchise_id
              AND {self._db_filter("m")}
        """
        return self._execute(sql, "matchup_rankings: All-time + season rankings")

    def all_time_manager_stats(self) -> int:
        """Calculate all-time career stats per manager (broadcast to every row).

        Sets: manager_all_time_gp, manager_all_time_wins, manager_all_time_losses,
              manager_all_time_ties, manager_all_time_win_pct
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        if "franchise_id" not in matchup_cols:
            logger.warning("[all_time_manager_stats] Missing franchise_id")
            return 0

        target_cols = {
            "manager_all_time_gp",
            "manager_all_time_wins",
            "manager_all_time_losses",
            "manager_all_time_ties",
            "manager_all_time_win_pct",
        }
        available = target_cols & matchup_cols
        if not available:
            logger.info("[all_time_manager_stats] No target columns found, skipping")
            return 0

        set_parts = []
        if "manager_all_time_gp" in matchup_cols:
            set_parts.append("manager_all_time_gp = c.gp")
        if "manager_all_time_wins" in matchup_cols:
            set_parts.append("manager_all_time_wins = c.wins")
        if "manager_all_time_losses" in matchup_cols:
            set_parts.append("manager_all_time_losses = c.losses")
        if "manager_all_time_ties" in matchup_cols:
            set_parts.append("manager_all_time_ties = c.ties")
        if "manager_all_time_win_pct" in matchup_cols:
            set_parts.append("manager_all_time_win_pct = c.win_pct")

        sql = f"""
            WITH career AS (
                SELECT franchise_id,
                    COUNT(*) AS gp,
                    SUM(COALESCE(win, 0)) AS wins,
                    SUM(COALESCE(loss, 0)) AS losses,
                    SUM(COALESCE(tie, 0)) AS ties,
                    ROUND(
                        SUM(COALESCE(win, 0))::DOUBLE
                        / NULLIF(SUM(COALESCE(win, 0)) + SUM(COALESCE(loss, 0)) + SUM(COALESCE(tie, 0)), 0),
                        4
                    ) AS win_pct
                FROM {matchup_table}
                WHERE franchise_id IS NOT NULL
                  AND {self._db_filter()}
                GROUP BY franchise_id
            )
            UPDATE {matchup_table} m SET
                {", ".join(set_parts)}
            FROM career c
            WHERE m.franchise_id = c.franchise_id
              AND {self._db_filter("m")}
        """
        return self._execute(sql, "all_time_manager_stats: Career W/L/T + win%")

    def inflation_rate(self) -> int:
        """Calculate year-over-year scoring inflation relative to earliest year.

        Sets: inflation_rate (1.0 for base year, ratio for others)
        """
        if not self._table_exists("matchup"):
            return 0

        matchup_table = self._qualified_name("matchup")
        matchup_cols = self._get_table_columns("matchup")

        if "inflation_rate" not in matchup_cols:
            logger.info("[inflation_rate] inflation_rate column not found, skipping")
            return 0

        if "team_points" not in matchup_cols:
            logger.warning("[inflation_rate] team_points column not found")
            return 0

        sql = f"""
            WITH year_avg AS (
                SELECT year, AVG(team_points) AS avg_pts
                FROM {matchup_table}
                WHERE team_points IS NOT NULL AND team_points > 0
                  AND {self._db_filter()}
                GROUP BY year
            ),
            base AS (
                SELECT avg_pts AS base_avg
                FROM year_avg
                ORDER BY year ASC
                LIMIT 1
            )
            UPDATE {matchup_table} m SET
                inflation_rate = ROUND(ya.avg_pts / NULLIF(b.base_avg, 0), 4)
            FROM year_avg ya, base b
            WHERE m.year = ya.year
              AND {self._db_filter("m")}
        """
        return self._execute(sql, "inflation_rate: Year-over-year scoring ratio")

    def build_all_play(self) -> int:
        """Build all_play table: every franchise vs every other franchise per week.

        CROSS JOIN matchup with itself to produce one row per
        (franchise_id, opponent_franchise_id, year, week) pair.
        Regular-season only (excludes playoffs and consolation).

        **Local-only**: uses CREATE OR REPLACE TABLE, which is safe against
        a per-league DuckDB file but would wipe every league's rows against
        the centralized ``___leagues`` catalog. The safety guard in
        ``multi_league.core.sql_utils.validate_scoped_sql`` blocks this
        pattern at runtime when SQLEnrichments is constructed without a
        ``data_dir``; the main import flow always passes a ``data_dir``.
        """
        if not self._table_exists("matchup"):
            return 0

        m = self._qualified_name("matchup")
        target = self._qualified_name("all_play")

        sql = f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                '{self.db_name}' AS db_name,
                a.year, a.week, a.franchise_id,
                b.franchise_id AS opponent_franchise_id,
                CASE WHEN a.team_points > b.team_points THEN 'W'
                     WHEN a.team_points < b.team_points THEN 'L'
                     ELSE 'T' END AS result,
                a.team_points AS points,
                b.team_points AS opponent_points
            FROM {m} a CROSS JOIN {m} b
            WHERE a.year = b.year AND a.week = b.week
              AND a.franchise_id != b.franchise_id
              AND a.franchise_id IS NOT NULL AND b.franchise_id IS NOT NULL
              AND a.team_points IS NOT NULL AND b.team_points IS NOT NULL
              AND COALESCE(a.is_playoffs, 0) = 0 AND COALESCE(a.is_consolation, 0) = 0
              AND COALESCE(b.is_playoffs, 0) = 0 AND COALESCE(b.is_consolation, 0) = 0
              AND {self._db_filter("a")}
              AND {self._db_filter("b")}
        """
        return self._execute(sql, "build_all_play: cross-join matchup for all-play records")

    def build_schedule_swap(self) -> int:
        """Build schedule_swap table: your score vs each other manager's opponent.

        "What would Joe's record be if he had Mike's schedule?"
        Regular-season only (excludes playoffs and consolation).
        """
        if not self._table_exists("matchup"):
            return 0

        m = self._qualified_name("matchup")
        target = self._qualified_name("schedule_swap")

        sql = f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                '{self.db_name}' AS db_name,
                a.year, a.week, a.franchise_id,
                b.franchise_id AS schedule_of_franchise_id,
                CASE WHEN a.team_points > b.opponent_points THEN 'W'
                     WHEN a.team_points < b.opponent_points THEN 'L'
                     ELSE 'T' END AS result,
                a.team_points AS my_points,
                b.opponent_points AS their_opponent_points
            FROM {m} a CROSS JOIN {m} b
            WHERE a.year = b.year AND a.week = b.week
              AND (a.franchise_id = b.franchise_id OR b.opponent_franchise_id != a.franchise_id)
              AND a.franchise_id IS NOT NULL AND b.franchise_id IS NOT NULL
              AND a.team_points IS NOT NULL AND b.opponent_points IS NOT NULL
              AND COALESCE(a.is_playoffs, 0) = 0 AND COALESCE(a.is_consolation, 0) = 0
              AND COALESCE(b.is_playoffs, 0) = 0 AND COALESCE(b.is_consolation, 0) = 0
              AND {self._db_filter("a")}
              AND {self._db_filter("b")}
        """
        return self._execute(sql, "build_schedule_swap: cross-join matchup for schedule swap records")

    def build_h2h_season(self) -> int:
        """Pre-aggregate all_play weekly results into season-level h2h records.

        Creates/replaces h2h_season table:
        - franchise_id, opponent_franchise_id, year
        - wins, losses, ties, games

        "What would Joe's record be if he only played Mike every week?"
        """
        if not self._table_exists("all_play"):
            logger.warning("[build_h2h_season] all_play table not found")
            return 0

        all_play = self._qualified_name("all_play")
        target = self._qualified_name("h2h_season")

        sql = f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                db_name,
                franchise_id,
                opponent_franchise_id,
                year,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties,
                COUNT(*) AS games
            FROM {all_play}
            WHERE {self._db_filter()}
            GROUP BY db_name, franchise_id, opponent_franchise_id, year
        """
        return self._execute(sql, "build_h2h_season: aggregate all_play to season records")

    def build_schedule_from_matchup(self) -> int:
        """Derive the schedule table from matchup data.

        Projects matchup columns into the canonical schedule schema.
        Replaces separate schedule fetchers on all platforms — schedule
        is always a subset of matchup data.
        """
        m = self._qualified_name("matchup")
        target = self._qualified_name("schedule")
        sql = f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                '{self.db_name}' AS db_name,
                year,
                week,
                cumulative_week,
                manager,
                manager_guid,
                franchise_id,
                franchise_name,
                team_name,
                platform,
                league_id,
                manager_week,
                COALESCE(REPLACE(manager, ' ', '') || CAST(year AS VARCHAR), '') AS manager_year,
                opponent,
                opponent_guid,
                opponent_franchise_id,
                COALESCE(REPLACE(opponent, ' ', '') || CAST(cumulative_week AS VARCHAR), '') AS opponent_week,
                COALESCE(REPLACE(opponent, ' ', '') || CAST(year AS VARCHAR), '') AS opponent_year,
                team_points,
                opponent_points,
                win,
                loss,
                CASE WHEN is_playoffs THEN 1 ELSE 0 END AS is_playoffs,
                CASE WHEN is_consolation THEN 1 ELSE 0 END AS is_consolation,
                CASE WHEN postseason THEN 1 ELSE 0 END AS postseason,
                playoff_round,
                playoff_round_num,
                CASE WHEN playoff_round_num IS NOT NULL THEN playoff_round_num - 1 ELSE NULL END AS playoff_week_index,
                CASE WHEN playoff_round_num = 1 THEN 1 ELSE 0 END AS quarterfinal,
                CASE WHEN playoff_round_num = 2 THEN 1 ELSE 0 END AS semifinal,
                CASE WHEN is_championship THEN 1 ELSE 0 END AS championship,
                CASE WHEN is_consolation AND playoff_round IS NOT NULL THEN playoff_round ELSE NULL END AS consolation_round,
                CASE WHEN is_consolation AND playoff_round_num = 1 THEN 1 ELSE 0 END AS consolation_semifinal,
                CASE WHEN is_consolation AND playoff_round_num = 2 THEN 1 ELSE 0 END AS consolation_final,
                CASE WHEN placement_game IS NOT NULL THEN 1 ELSE 0 END AS placement_game,
                champion,
                sacko,
                placement_rank
            FROM {m}
            WHERE {self._db_filter("m")}
            ORDER BY year, week, manager
        """
        return self._execute(sql, "build_schedule_from_matchup: derive schedule table from matchup")

    def build_schedule_swap_season(self) -> int:
        """Pre-aggregate schedule_swap weekly results into season-level records.

        Creates/replaces schedule_swap_season table:
        - franchise_id, schedule_of_franchise_id, year
        - wins, losses, ties, games

        "What would Joe's record be if he had Mike's schedule?"
        """
        if not self._table_exists("schedule_swap"):
            logger.warning("[build_schedule_swap_season] schedule_swap table not found")
            return 0

        swap = self._qualified_name("schedule_swap")
        target = self._qualified_name("schedule_swap_season")

        sql = f"""
            CREATE OR REPLACE TABLE {target} AS
            SELECT
                db_name,
                franchise_id,
                schedule_of_franchise_id,
                year,
                SUM(CASE WHEN result = 'W' THEN 1 ELSE 0 END) AS wins,
                SUM(CASE WHEN result = 'L' THEN 1 ELSE 0 END) AS losses,
                SUM(CASE WHEN result = 'T' THEN 1 ELSE 0 END) AS ties,
                COUNT(*) AS games
            FROM {swap}
            WHERE {self._db_filter()}
            GROUP BY db_name, franchise_id, schedule_of_franchise_id, year
        """
        return self._execute(sql, "build_schedule_swap_season: aggregate schedule_swap to season records")
