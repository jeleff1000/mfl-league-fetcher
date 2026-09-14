"""Pipeline enrichment checks — validate that transformations ran correctly.

Also contains local DuckDB pre-upload validators (check_* functions) that run
BEFORE upload to MotherDuck to catch issues early.
"""

import logging
from pathlib import Path

import duckdb

from ._runner import SQLCheck

logger = logging.getLogger(__name__)

_TYPE_ALIASES = {
    "INTEGER": {"INTEGER", "INT", "INT32", "SIGNED"},
    "BIGINT": {"BIGINT", "INT64", "LONG", "HUGEINT"},
    "DOUBLE": {"DOUBLE", "DOUBLE PRECISION", "FLOAT", "REAL", "DECIMAL", "NUMERIC"},
    "BOOLEAN": {"BOOLEAN", "BOOL", "LOGICAL"},
    "VARCHAR": {"VARCHAR", "TEXT", "STRING"},
}


def _canonicalize_duckdb_type(dtype: str) -> str:
    return str(dtype).upper().split("(")[0].strip()


def _quote_ident(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _resolve_table_ref(conn: duckdb.DuckDBPyConnection, table: str) -> str:
    """Prefer the canonical public schema, but support bare/main-schema test tables."""
    if "." in table:
        parts = [part.strip().strip('"') for part in table.split(".", 1)]
        if len(parts) == 2:
            schema_name, table_name = parts
            return f"{_quote_ident(schema_name)}.{_quote_ident(table_name)}"
        return table

    row = conn.execute(
        """
        SELECT table_schema
        FROM information_schema.tables
        WHERE table_name = ?
        ORDER BY
            CASE
                WHEN table_schema = 'public' THEN 0
                WHEN table_schema = 'main' THEN 1
                ELSE 2
            END,
            table_schema
        LIMIT 1
        """,
        [table],
    ).fetchone()

    schema_name = row[0] if row else "public"
    return f"{_quote_ident(schema_name)}.{_quote_ident(table)}"


# ---------------------------------------------------------------------------
# Local DuckDB pre-upload validators
# These accept a duckdb.DuckDBPyConnection and return list[str] of error msgs.
# ---------------------------------------------------------------------------


def check_transform_output_non_empty(
    conn: duckdb.DuckDBPyConnection,
    tables: list[str],
) -> list[str]:
    """Verify each canonical table has > 0 rows after transforms."""
    errors = []
    for table in tables:
        table_ref = _resolve_table_ref(conn, table)
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0]
            if count == 0:
                errors.append(f"TRANSFORM_OUTPUT_NON_EMPTY: {table} has 0 rows after transforms")
        except duckdb.CatalogException:
            errors.append(f"TRANSFORM_OUTPUT_NON_EMPTY: {table} does not exist")
    return errors


def check_pre_upload_sanity(conn: duckdb.DuckDBPyConnection) -> list[str]:
    """Check basic invariants before uploading to MotherDuck."""
    errors = []

    try:
        matchup_ref = _resolve_table_ref(conn, "matchup")
        cols = {r[0] for r in conn.execute(f"DESCRIBE {matchup_ref}").fetchall()}
        if "win" not in cols:
            errors.append("PRE_UPLOAD_SANITY: matchup missing 'win' column")
        if "loss" not in cols:
            errors.append("PRE_UPLOAD_SANITY: matchup missing 'loss' column")
    except duckdb.CatalogException:
        pass

    try:
        player_ref = _resolve_table_ref(conn, "player_fantasy")
        cols = {r[0] for r in conn.execute(f"DESCRIBE {player_ref}").fetchall()}
        if "player_week" not in cols:
            errors.append("PRE_UPLOAD_SANITY: player_fantasy missing 'player_week' column")
        else:
            null_count = conn.execute(f"SELECT COUNT(*) FROM {player_ref} WHERE player_week IS NULL").fetchone()[0]
            total = conn.execute(f"SELECT COUNT(*) FROM {player_ref}").fetchone()[0]
            if total > 0 and null_count / total > 0.5:
                errors.append(f"PRE_UPLOAD_SANITY: player_fantasy has {null_count}/{total} NULL player_week values")
    except duckdb.CatalogException:
        pass

    try:
        draft_ref = _resolve_table_ref(conn, "draft")
        cols = {r[0] for r in conn.execute(f"DESCRIBE {draft_ref}").fetchall()}
        if "round" not in cols:
            errors.append("PRE_UPLOAD_SANITY: draft missing 'round' column")
    except duckdb.CatalogException:
        pass

    for table_name in ["matchup", "player_fantasy", "transactions", "schedule"]:
        try:
            table_ref = _resolve_table_ref(conn, table_name)
            cols = {r[0] for r in conn.execute(f"DESCRIBE {table_ref}").fetchall()}
            required = {"year", "week", "cumulative_week"}
            if not required.issubset(cols):
                continue
            null_count = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {table_ref}
                WHERE year IS NOT NULL
                  AND week IS NOT NULL
                  AND cumulative_week IS NULL
                """
            ).fetchone()[0]
            if null_count:
                errors.append(f"PRE_UPLOAD_SANITY: {table_name} has {null_count} NULL cumulative_week values")
        except duckdb.CatalogException:
            pass

    return errors


def check_parquet_ingest_completeness(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    parquet_dir: str,
) -> list[str]:
    """Verify ingested table row count matches source parquet files."""
    errors = []
    parquet_path = Path(parquet_dir)
    parquet_files = list(parquet_path.glob("*.parquet"))
    if not parquet_files:
        return []

    try:
        glob_pattern = str(parquet_path / "*.parquet").replace("\\", "/")
        table_ref = _resolve_table_ref(conn, table)
        source_count = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{glob_pattern}')").fetchone()[0]
        table_count = conn.execute(f"SELECT COUNT(*) FROM {table_ref}").fetchone()[0]

        if source_count != table_count:
            errors.append(
                f"PARQUET_INGEST_COMPLETENESS: {table} has {table_count} rows "
                f"but source parquets have {source_count} rows"
            )
    except Exception as e:
        errors.append(f"PARQUET_INGEST_COMPLETENESS: error checking {table}: {e}")

    return errors


def check_column_schema_drift(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    expected_columns: set[str],
) -> list[str]:
    """Verify a table has all expected columns after transforms."""
    errors = []
    try:
        table_ref = _resolve_table_ref(conn, table)
        actual = {r[0] for r in conn.execute(f"DESCRIBE {table_ref}").fetchall()}
        missing = expected_columns - actual
        for col in sorted(missing):
            errors.append(f"COLUMN_SCHEMA_DRIFT: {table} missing expected column '{col}'")
    except duckdb.CatalogException:
        errors.append(f"COLUMN_SCHEMA_DRIFT: table {table} does not exist")
    return errors


def check_column_type_drift(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    expected_types: dict[str, str],
) -> list[str]:
    """Verify canonical DuckDB types for a table after transforms."""
    errors = []
    try:
        table_ref = _resolve_table_ref(conn, table)
        actual = {str(name): str(dtype) for name, dtype, *_ in conn.execute(f"DESCRIBE {table_ref}").fetchall()}
    except duckdb.CatalogException:
        return [f"COLUMN_TYPE_DRIFT: table {table} does not exist"]

    for column_name, expected_type in expected_types.items():
        actual_type = actual.get(column_name)
        if actual_type is None:
            continue
        expected_norm = _canonicalize_duckdb_type(expected_type)
        actual_norm = _canonicalize_duckdb_type(actual_type)
        if actual_norm == expected_norm:
            continue
        if actual_norm in _TYPE_ALIASES.get(expected_norm, {expected_norm}):
            continue
        errors.append(f"COLUMN_TYPE_DRIFT: {table}.{column_name} expected {expected_type} but found {actual_type}")
    return errors


def check_memory_usage(
    conn: duckdb.DuckDBPyConnection,
    threshold_mb: float = 2048,
) -> list[str]:
    """Check DuckDB memory usage is under threshold (default 2 GB)."""
    rows = conn.execute("SELECT tag, memory_usage_bytes FROM duckdb_memory()").fetchall()
    total_bytes = sum(r[1] for r in rows)
    total_mb = total_bytes / (1024 * 1024)

    if total_mb > threshold_mb:
        return [f"LOCAL_DUCKDB_MEMORY: {total_mb:.0f} MB exceeds threshold of {threshold_mb:.0f} MB"]
    return []


_ROSTERED_FILTER = (
    "manager IS NOT NULL "
    "AND TRIM(manager) != '' "
    "AND LOWER(TRIM(manager)) NOT IN ('unrostered','fa','free agent','waivers')"
)

ALL_CHECKS = [
    # 1 — NFL_player_id coverage for rostered players
    SQLCheck(
        name="pl_nfl_player_id_coverage",
        table="player_fantasy",
        severity="ERROR",
        description="NFL_player_id populated for >90% of rostered players",
        sql=(f"SELECT COUNT(*) FROM public.player_fantasy WHERE NFL_player_id IS NULL AND {_ROSTERED_FILTER}"),
        needs_columns=["NFL_player_id"],
        threshold=50,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 2 — At least some NFL_player_ids resolved
    SQLCheck(
        name="pl_nfl_player_id_zero_pct",
        table="player_fantasy",
        severity="ERROR",
        description="At least some NFL_player_ids resolved (not 0% coverage)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE NFL_player_id IS NOT NULL "
            f"AND {_ROSTERED_FILTER}"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["NFL_player_id"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 3 — manager_lamar populated for started players
    SQLCheck(
        name="pl_lamar_outputs",
        table="player_fantasy",
        severity="WARNING",
        description="manager_lamar populated for started players",
        sql=("SELECT COUNT(*) FROM public.player_fantasy WHERE is_started = 1 AND manager_lamar IS NULL"),
        needs_columns=["manager_lamar", "is_started"],
        threshold=100,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 4 — player_lamar has non-zero values
    SQLCheck(
        name="pl_lamar_not_all_zero",
        table="player_fantasy",
        severity="WARNING",
        description="player_lamar has non-zero values (LAMAR enrichment ran)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE player_lamar != 0 AND player_lamar IS NOT NULL"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["player_lamar"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 5 — league_wide_optimal_player column exists and populated
    SQLCheck(
        name="pl_optimal_outputs",
        table="player_fantasy",
        severity="WARNING",
        description="league_wide_optimal_player column exists and has flagged rows",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE league_wide_optimal_player = 1"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["league_wide_optimal_player"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 6 — At least 1 optimal player per (year, week)
    SQLCheck(
        name="pl_optimal_per_week",
        table="player_fantasy",
        severity="WARNING",
        description="At least 1 optimal player flagged per (year, week)",
        sql=(
            "SELECT COUNT(*) FROM ("
            "SELECT year, week FROM public.player_fantasy "
            "WHERE league_wide_optimal_player = 1 "
            "GROUP BY year, week "
            "HAVING COUNT(*) < 1"
            ")"
        ),
        needs_columns=["league_wide_optimal_player"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 7 — position_rank column exists and not all NULL
    SQLCheck(
        name="pl_rank_column_exists",
        table="player_fantasy",
        severity="WARNING",
        description="position_rank column exists and is populated",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE position_rank IS NOT NULL"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["position_rank"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 8 — position_week_rank populated
    SQLCheck(
        name="pl_position_week_rank_exists",
        table="player_fantasy",
        severity="INFO",
        description="position_week_rank column populated",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE position_week_rank IS NOT NULL"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["position_week_rank"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 9 — clutch_equity populated (only after playoff odds run)
    SQLCheck(
        name="pl_clutch_equity_outputs",
        table="player_fantasy",
        severity="INFO",
        description="clutch_equity populated (expected after playoff odds enrichment)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE clutch_equity IS NOT NULL"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["clutch_equity"],
        tags=["pipeline"],
        fix_action="retransform_agg",
    ),
    # 10 — fantasy_points not all zero/null for rostered+started (recent years)
    SQLCheck(
        name="pl_fantasy_points_populated",
        table="player_fantasy",
        severity="ERROR",
        description="fantasy_points populated for rostered+started players (NULL-free, year >= 2020)",
        sql=(
            "SELECT COUNT(*) FROM public.player_fantasy "
            "WHERE is_started = 1 "
            "AND fantasy_points IS NULL "
            "AND year >= 2020 "
            f"AND {_ROSTERED_FILTER}"
        ),
        needs_columns=["fantasy_points", "is_started"],
        threshold=0,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 11 — No negative fantasy_points (except DEF/K)
    SQLCheck(
        name="pl_fantasy_points_negative",
        table="player_fantasy",
        severity="WARNING",
        description="No extreme negative fantasy_points (< -15) for modern rostered non-DEF/K players",
        sql=(
            "SELECT COUNT(*) FROM public.player_fantasy "
            "WHERE fantasy_points < -15 "
            "AND year >= 2020 "
            "AND position != 'DEF' "
            "AND position != 'K' "
            f"AND {_ROSTERED_FILTER}"
        ),
        needs_columns=["fantasy_points", "position"],
        threshold=0,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 12 — DEF players have non-null fantasy_points
    SQLCheck(
        name="pl_def_fantasy_points",
        table="player_fantasy",
        severity="WARNING",
        description="DEF players have non-null fantasy_points when started",
        sql=(
            "SELECT COUNT(*) FROM public.player_fantasy "
            "WHERE position = 'DEF' "
            "AND is_started = 1 "
            "AND fantasy_points IS NULL"
        ),
        needs_columns=["position", "is_started", "fantasy_points"],
        threshold=20,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 13 — Unrostered players present (full import indicator)
    SQLCheck(
        name="pl_unrostered_exist",
        table="player_fantasy",
        severity="INFO",
        description="Unrostered players present (indicates full import ran)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE LOWER(TRIM(manager)) IN ('unrostered','fa','free agent','waivers')"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        threshold=999999,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 14 — is_started is 0 or 1 only
    SQLCheck(
        name="pl_is_started_binary",
        table="player_fantasy",
        severity="ERROR",
        description="is_started is binary (0 or 1) — no invalid values",
        sql=("SELECT COUNT(*) FROM public.player_fantasy WHERE is_started IS NOT NULL AND is_started NOT IN (0, 1)"),
        needs_columns=["is_started"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 15 — points column matches fantasy_points
    SQLCheck(
        name="pl_sync_points",
        table="player_fantasy",
        severity="WARNING",
        description="points column matches fantasy_points for started players",
        sql=(
            "SELECT COUNT(*) FROM public.player_fantasy "
            "WHERE ABS(COALESCE(points, 0) - COALESCE(fantasy_points, 0)) > 0.01 "
            "AND is_started = 1 "
            "AND fantasy_points IS NOT NULL"
        ),
        needs_columns=["points", "fantasy_points", "is_started"],
        threshold=50,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 16 — lineup_position/fantasy_position populated for rostered players
    SQLCheck(
        name="pl_lineup_position_populated",
        table="player_fantasy",
        severity="WARNING",
        description="fantasy_position or lineup_position populated for rostered players",
        sql=(
            "SELECT COUNT(*) FROM public.player_fantasy "
            f"WHERE {_ROSTERED_FILTER} "
            "AND COALESCE(fantasy_position, lineup_position) IS NULL"
        ),
        needs_columns=["manager"],
        threshold=100,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 17 — QBs have reasonable count per year
    SQLCheck(
        name="pl_enrichment_qb_count",
        table="player_fantasy",
        severity="WARNING",
        description="At least 4 distinct QBs started per year",
        sql=(
            "SELECT COUNT(*) FROM ("
            "SELECT year, COUNT(DISTINCT NFL_player_id) AS cnt "
            "FROM public.player_fantasy "
            "WHERE position = 'QB' AND is_started = 1 "
            "GROUP BY year "
            "HAVING cnt < 4"
            ")"
        ),
        needs_columns=["position", "is_started", "NFL_player_id"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 18 — RBs have reasonable count per year
    SQLCheck(
        name="pl_enrichment_rb_count",
        table="player_fantasy",
        severity="WARNING",
        description="At least 8 distinct RBs started per year",
        sql=(
            "SELECT COUNT(*) FROM ("
            "SELECT year, COUNT(DISTINCT NFL_player_id) AS cnt "
            "FROM public.player_fantasy "
            "WHERE position = 'RB' AND is_started = 1 "
            "GROUP BY year "
            "HAVING cnt < 8"
            ")"
        ),
        needs_columns=["position", "is_started", "NFL_player_id"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 19 — WRs have reasonable count per year
    SQLCheck(
        name="pl_enrichment_wr_count",
        table="player_fantasy",
        severity="WARNING",
        description="At least 8 distinct WRs started per year",
        sql=(
            "SELECT COUNT(*) FROM ("
            "SELECT year, COUNT(DISTINCT NFL_player_id) AS cnt "
            "FROM public.player_fantasy "
            "WHERE position = 'WR' AND is_started = 1 "
            "GROUP BY year "
            "HAVING cnt < 8"
            ")"
        ),
        needs_columns=["position", "is_started", "NFL_player_id"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 20 — win column populated in player_fantasy
    SQLCheck(
        name="pl_matchup_to_player_win",
        table="player_fantasy",
        severity="WARNING",
        description="win column populated (matchup enrichment ran)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER (WHERE win IS NOT NULL) = 0 THEN 1 ELSE 0 END FROM public.player_fantasy"
        ),
        needs_columns=["win"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 21 — team_points populated
    SQLCheck(
        name="pl_matchup_to_player_team_points",
        table="player_fantasy",
        severity="WARNING",
        description="team_points column populated (matchup enrichment ran)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE team_points IS NOT NULL"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["team_points"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 22 — round column populated (from draft enrichment)
    SQLCheck(
        name="pl_draft_to_player_round",
        table="player_fantasy",
        severity="INFO",
        description="round column populated from draft enrichment",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE round IS NOT NULL AND round > 0"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["round"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 23 — player_lamar_ytd populated
    SQLCheck(
        name="pl_lamar_ytd_populated",
        table="player_fantasy",
        severity="INFO",
        description="player_lamar_ytd populated (cumulative LAMAR enrichment ran)",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE player_lamar_ytd IS NOT NULL"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["player_lamar_ytd"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 24 — bench_lamar populated for non-started players
    SQLCheck(
        name="pl_bench_lamar_populated",
        table="player_fantasy",
        severity="INFO",
        description="bench_lamar populated for benched players",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE bench_lamar IS NOT NULL AND is_started = 0"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["bench_lamar", "is_started"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 25 — ppg column populated
    SQLCheck(
        name="pl_ppg_outputs",
        table="player_fantasy",
        severity="INFO",
        description="ppg column populated with non-zero values",
        sql=(
            "SELECT CASE WHEN COUNT(*) FILTER ("
            "WHERE ppg IS NOT NULL AND ppg > 0"
            ") = 0 THEN 1 ELSE 0 END "
            "FROM public.player_fantasy"
        ),
        needs_columns=["ppg"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 26 — franchise_id populated for rostered players
    SQLCheck(
        name="pl_franchise_id_populated",
        table="player_fantasy",
        severity="WARNING",
        description="franchise_id populated for rostered players",
        sql=(f"SELECT COUNT(*) FROM public.player_fantasy WHERE franchise_id IS NULL AND {_ROSTERED_FILTER}"),
        needs_columns=["franchise_id"],
        threshold=100,
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 27 — player_week never NULL
    SQLCheck(
        name="pl_player_week_not_null",
        table="player_fantasy",
        severity="ERROR",
        description="player_week is never NULL (required for super_table JOIN)",
        sql=(
            "SELECT COUNT(*) FROM public.player_fantasy "
            "WHERE player_week IS NULL "
            "AND COALESCE(fantasy_position, '') NOT IN ('TAXI', 'IR')"
        ),
        needs_columns=["player_week"],
        tags=["pipeline"],
        fix_action="retransform",
    ),
    # 28 — year and week never NULL
    SQLCheck(
        name="pl_year_week_not_null",
        table="player_fantasy",
        severity="ERROR",
        description="year and week are never NULL",
        sql=("SELECT COUNT(*) FROM public.player_fantasy WHERE year IS NULL OR week IS NULL"),
        tags=["pipeline"],
        fix_action="retransform",
    ),
]
