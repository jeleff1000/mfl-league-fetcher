"""Helpers for stable player_fantasy publish identities."""

from __future__ import annotations


def qident(column: str) -> str:
    return '"' + column.replace('"', '""') + '"'


def player_week_repair_statements(
    table_ref: str,
    columns: set[str],
    *,
    db_filter: str = "1=1",
) -> list[tuple[str, str]]:
    """Return SQL statements that guarantee non-null player_week identities.

    The canonical join key remains ``NFL_player_id_year_week`` whenever those
    components exist. Rows that cannot form that key still need a stable
    publish identity, so they get a deterministic UNMAPPED key based on the
    source identifiers that are present.
    """
    if "player_week" not in columns:
        return []

    statements: list[tuple[str, str]] = []

    if {"NFL_player_id", "year", "week"}.issubset(columns):
        nfl_id_expr = 'TRIM(CAST("NFL_player_id" AS VARCHAR))'
        canonical_expr = f"{nfl_id_expr} || '_' || CAST(year AS VARCHAR) || '_' || CAST(week AS VARCHAR)"
        statements.append(
            (
                "canonical",
                f"""
                UPDATE {table_ref}
                SET player_week = {canonical_expr}
                WHERE NULLIF({nfl_id_expr}, '') IS NOT NULL
                  AND year IS NOT NULL
                  AND week IS NOT NULL
                  AND (
                    NULLIF(TRIM(CAST(player_week AS VARCHAR)), '') IS NULL
                    OR player_week <> {canonical_expr}
                  )
                  AND {db_filter}
                """,
            )
        )

    fallback_columns = [
        "db_name",
        "league_id",
        "league_key",
        "year",
        "week",
        "NFL_player_id",
        "sleeper_player_id",
        "yahoo_player_id",
        "yahoo_player_key",
        "espn_player_id",
        "player_id",
        "player_key",
        "player",
        "team_key",
        "team_name",
        "manager_week",
        "franchise_id",
        "manager_guid",
        "manager",
        "fantasy_position",
        "roster_position",
        "position",
        "nfl_team",
        "is_started",
        "is_rostered",
        "cumulative_week",
    ]
    fallback_parts = [
        f"COALESCE(CAST({qident(column)} AS VARCHAR), '')" for column in fallback_columns if column in columns
    ]
    if not fallback_parts:
        fallback_parts = ["''"]

    year_part = "COALESCE(CAST(year AS VARCHAR), 'UNKNOWN_YEAR')" if "year" in columns else "'UNKNOWN_YEAR'"
    week_part = "COALESCE(CAST(week AS VARCHAR), 'UNKNOWN_WEEK')" if "week" in columns else "'UNKNOWN_WEEK'"
    fallback_expr = (
        "'UNMAPPED_' || sha256(concat_ws(chr(31), "
        + ", ".join(fallback_parts)
        + f")) || '_' || {year_part} || '_' || {week_part}"
    )

    statements.append(
        (
            "fallback",
            f"""
            UPDATE {table_ref}
            SET player_week = {fallback_expr}
            WHERE NULLIF(TRIM(CAST(player_week AS VARCHAR)), '') IS NULL
              AND {db_filter}
            """,
        )
    )

    return statements


def _string_expr(alias: str, column: str) -> str:
    return f"COALESCE(CAST({alias}.{qident(column)} AS VARCHAR), '')"


def _rostered_priority_expr(alias: str, columns: set[str]) -> str:
    if "manager" not in columns:
        return "0"
    manager = _string_expr(alias, "manager")
    unrostered_values = "'', 'fa', 'free agent', 'unrostered', 'waivers'"
    return (
        f"CASE WHEN NULLIF(TRIM({manager}), '') IS NOT NULL "
        f"AND LOWER(TRIM({manager})) NOT IN ({unrostered_values}) "
        "THEN 1 ELSE 0 END"
    )


def _presence_expr(alias: str, column: str) -> str:
    return (
        f"CASE WHEN {alias}.{qident(column)} IS NOT NULL "
        f"AND NULLIF(TRIM(CAST({alias}.{qident(column)} AS VARCHAR)), '') IS NOT NULL "
        "THEN 1 ELSE 0 END"
    )


def _presence_score_expr(alias: str, columns: set[str], candidates: list[str]) -> str:
    parts = [_presence_expr(alias, column) for column in candidates if column in columns]
    if not parts:
        return "0"
    return " + ".join(parts)


def player_week_publish_dedup_statements(
    table_ref: str,
    columns: set[str],
    *,
    db_filter: str = "1=1",
    temp_table: str = "_player_fantasy_publish_dedup",
) -> list[tuple[str, str]]:
    """Return SQL statements that enforce one row per non-blank player_week.

    The server-side delta protocol publishes ``player_fantasy`` by
    ``db_name/player_week``. Transformation code should therefore collapse any
    late duplicate before analytics and upload. Preference order keeps real
    rostered rows over unrostered expansion rows, then started rows, then active
    lineup slots, then a deterministic row order.
    """
    if "player_week" not in columns:
        return []

    alias = "p"
    rostered_priority = _rostered_priority_expr(alias, columns)
    started_priority = (
        f"COALESCE(CAST({alias}.{qident('is_started')} AS INTEGER), 0)" if "is_started" in columns else "0"
    )
    slot_expr = (
        f"UPPER(TRIM({_string_expr(alias, 'fantasy_position')}))"
        if "fantasy_position" in columns
        else (f"UPPER(TRIM({_string_expr(alias, 'roster_position')}))" if "roster_position" in columns else "''")
    )
    points_priority = (
        f"COALESCE(CAST({alias}.{qident('fantasy_points')} AS DOUBLE), -1.0e308)"
        if "fantasy_points" in columns
        else "-1.0e308"
    )
    franchise_expr = _string_expr(alias, "franchise_id") if "franchise_id" in columns else "''"
    team_key_expr = _string_expr(alias, "team_key") if "team_key" in columns else "''"
    manager_expr = _string_expr(alias, "manager") if "manager" in columns else "''"
    nfl_player_expr = _string_expr(alias, "NFL_player_id") if "NFL_player_id" in columns else "''"

    temp_ref = qident(temp_table)
    create_sql = f"""
        CREATE OR REPLACE TEMP TABLE {temp_ref} AS
        SELECT row_id
        FROM (
            SELECT
                p.row_id,
                ROW_NUMBER() OVER (
                    PARTITION BY p.player_week_key
                    ORDER BY
                        {rostered_priority} DESC,
                        {started_priority} DESC,
                        CASE WHEN {slot_expr} NOT IN ('', 'BN', 'IR', 'TAXI', 'RES') THEN 1 ELSE 0 END DESC,
                        {points_priority} DESC,
                        {franchise_expr},
                        {team_key_expr},
                        {manager_expr},
                        {nfl_player_expr},
                        p.row_id
                ) AS keep_rank
            FROM (
                SELECT
                    p.rowid AS row_id,
                    NULLIF(TRIM(CAST(p.{qident('player_week')} AS VARCHAR)), '') AS player_week_key,
                    p.*
                FROM {table_ref} p
                WHERE NULLIF(TRIM(CAST(p.{qident('player_week')} AS VARCHAR)), '') IS NOT NULL
                  AND {db_filter}
            ) p
        ) ranked
        WHERE keep_rank > 1
    """

    delete_sql = f"""
        DELETE FROM {table_ref} p
        USING {temp_ref} d
        WHERE p.rowid = d.row_id
          AND {db_filter}
    """

    drop_sql = f"DROP TABLE IF EXISTS {temp_ref}"
    return [("stage_duplicates", create_sql), ("delete_duplicates", delete_sql), ("drop_temp", drop_sql)]


def matchup_publish_dedup_statements(
    table_ref: str,
    columns: set[str],
    *,
    db_filter: str = "1=1",
    temp_table: str = "_matchup_publish_dedup",
) -> list[tuple[str, str]]:
    """Return SQL statements that enforce one row per matchup manager_week.

    The delta publish protocol treats ``matchup`` as one team-week row keyed by
    ``db_name/manager_week``. Yahoo recovery and playoff-gap repairs can
    occasionally leave a duplicate shell row beside the real matchup row. Keep
    the row with the most usable matchup information and remove the weaker
    duplicate before analytics/upload.
    """
    if "manager_week" not in columns:
        return []

    alias = "m"
    db_key_expr = f"NULLIF(TRIM(CAST({alias}.{qident('db_name')} AS VARCHAR)), '')" if "db_name" in columns else "''"
    result_presence = _presence_score_expr(
        alias,
        columns,
        ["team_points", "opponent_points", "win", "loss", "tie", "margin", "total_matchup_score"],
    )
    opponent_presence = _presence_score_expr(
        alias,
        columns,
        ["opponent", "opponent_franchise_id", "opponent_guid", "matchup_id", "matchup_key"],
    )
    manager_presence = _presence_score_expr(
        alias,
        columns,
        ["franchise_id", "manager_guid", "team_key", "manager", "team_name", "league_id", "platform"],
    )
    richness_columns = [
        column for column in sorted(columns) if column not in {"manager_week", "manager_year_week", "db_name"}
    ]
    richness_score = _presence_score_expr(alias, columns, richness_columns)
    non_bye_priority = (
        f"CASE WHEN COALESCE(TRY_CAST({alias}.{qident('is_bye_week')} AS INTEGER), 0) = 0 THEN 1 ELSE 0 END"
        if "is_bye_week" in columns
        else "1"
    )
    team_points_priority = (
        f"COALESCE(TRY_CAST({alias}.{qident('team_points')} AS DOUBLE), -1.0e308)"
        if "team_points" in columns
        else "-1.0e308"
    )
    opponent_points_priority = (
        f"COALESCE(TRY_CAST({alias}.{qident('opponent_points')} AS DOUBLE), -1.0e308)"
        if "opponent_points" in columns
        else "-1.0e308"
    )
    year_expr = _string_expr(alias, "year") if "year" in columns else "''"
    week_expr = _string_expr(alias, "week") if "week" in columns else "''"
    franchise_expr = _string_expr(alias, "franchise_id") if "franchise_id" in columns else "''"
    team_key_expr = _string_expr(alias, "team_key") if "team_key" in columns else "''"
    manager_expr = _string_expr(alias, "manager") if "manager" in columns else "''"
    team_name_expr = _string_expr(alias, "team_name") if "team_name" in columns else "''"
    matchup_id_expr = _string_expr(alias, "matchup_id") if "matchup_id" in columns else "''"
    matchup_key_expr = _string_expr(alias, "matchup_key") if "matchup_key" in columns else "''"

    temp_ref = qident(temp_table)
    create_sql = f"""
        CREATE OR REPLACE TEMP TABLE {temp_ref} AS
        SELECT row_id
        FROM (
            SELECT
                m.row_id,
                ROW_NUMBER() OVER (
                    PARTITION BY m.db_name_key, m.manager_week_key
                    ORDER BY
                        {result_presence} DESC,
                        {opponent_presence} DESC,
                        {non_bye_priority} DESC,
                        {manager_presence} DESC,
                        {richness_score} DESC,
                        {team_points_priority} DESC,
                        {opponent_points_priority} DESC,
                        {year_expr},
                        {week_expr},
                        {franchise_expr},
                        {team_key_expr},
                        {manager_expr},
                        {team_name_expr},
                        {matchup_id_expr},
                        {matchup_key_expr},
                        m.row_id
                ) AS keep_rank
            FROM (
                SELECT
                    m.rowid AS row_id,
                    {db_key_expr} AS db_name_key,
                    NULLIF(TRIM(CAST(m.{qident('manager_week')} AS VARCHAR)), '') AS manager_week_key,
                    m.*
                FROM {table_ref} m
                WHERE NULLIF(TRIM(CAST(m.{qident('manager_week')} AS VARCHAR)), '') IS NOT NULL
                  AND {db_filter}
            ) m
        ) ranked
        WHERE keep_rank > 1
    """

    delete_sql = f"""
        DELETE FROM {table_ref} m
        USING {temp_ref} d
        WHERE m.rowid = d.row_id
          AND {db_filter}
    """

    drop_sql = f"DROP TABLE IF EXISTS {temp_ref}"
    return [("stage_duplicates", create_sql), ("delete_duplicates", delete_sql), ("drop_temp", drop_sql)]
