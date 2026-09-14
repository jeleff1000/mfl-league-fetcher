"""Compact cache-only matchup aggregation primitives.

The public builder calls these deterministic DuckDB transformations after cache
schema validation.  They deliberately aggregate league facts before packing a
single outer player/time record; a player cannot escape as duplicate UI rows.
"""

from __future__ import annotations

import re

import duckdb

try:
    from . import position_slots_contract as _POSITION_SLOTS
except ImportError:  # Script invocation from this directory.
    import position_slots_contract as _POSITION_SLOTS


_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_QUALIFIED_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")


def _relation_sql(name: str) -> str:
    """Return a safely-quoted DuckDB relation name.

    The runner supplies fixed catalog/schema/table names; refusing arbitrary SQL
    here makes the aggregation boundary deterministic and testable.
    """

    if not _QUALIFIED_IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid relation name: {name!r}")
    return ".".join(f'"{part}"' for part in name.split("."))


def _table_sql(name: str) -> str:
    if not _IDENTIFIER.fullmatch(name):
        raise ValueError(f"Invalid output table name: {name!r}")
    return f'"{name}"'


def _canonical_position_sql(expression: str) -> str:
    """Return the immutable serving-position mapping for cached player facts.

    Cache rows may retain platform-level IDP labels even though the product has
    only DL/LB/DB. Normalize before every inventory/numerator aggregation so
    aliases never create separate denominator populations.
    """

    normalized = f"UPPER(TRIM(CAST({expression} AS VARCHAR)))"
    return f"""
      CASE
        WHEN {normalized} IN ('FB', 'RB') THEN 'RB'
        WHEN {normalized} IN ('PK', 'K') THEN 'K'
        WHEN {normalized} IN ('DE', 'DT', 'NT', 'DL', 'EDGE') THEN 'DL'
        WHEN {normalized} IN ('ILB', 'MLB', 'OLB', 'LB') THEN 'LB'
        WHEN {normalized} IN ('CB', 'FS', 'SS', 'S', 'DB') THEN 'DB'
        WHEN {normalized} IN ('DST', 'D/ST', 'DEFENSE', 'DEF') THEN 'DEF'
        ELSE {normalized}
      END
    """


def _capacity_tier_sql(expression: str) -> str:
    """Return the already-normalized literal team-size cohort label.

    A retired ``10t``/``12t`` label is deliberately *not* translated here:
    translating it would conceal the lost 8/14 allocation.  The narrow-lane
    validation rejects it before an artifact can be built.
    """

    return f"CAST({expression} AS VARCHAR)"


_BAD_IDENTITY_ROW_COLUMNS = {
    "is_rostered",
    "is_started",
    "manager",
    "team_key",
    "team_name",
    "team_points",
    "fantasy_points",
}


# The immutable cache is intentionally wide.  A compact build needs only these
# rollup facts; retaining all platform IDs and raw payload columns in a 20M-row
# TEMP lane can consume tens of GiB before aggregation starts.
_NARROW_ROLLUP_SOURCE_COLUMNS = (
    ("db_name", "VARCHAR"),
    ("year", "INTEGER"),
    ("week", "INTEGER"),
    ("NFL_player_id", "VARCHAR"),
    ("position", "VARCHAR"),
    ("cohort_position_eligible", "INTEGER"),
    ("cohort_teams", "VARCHAR"),
    ("cohort_roster", "VARCHAR"),
    ("cohort_scoring", "VARCHAR"),
    ("cohort_pass_td", "VARCHAR"),
    ("cohort_playoff_teams", "VARCHAR"),
    ("cohort_dynasty", "VARCHAR"),
    ("cohort_best_ball", "VARCHAR"),
    ("is_rostered", "INTEGER"),
    ("is_started", "INTEGER"),
    ("win", "DOUBLE"),
    ("clutch_equity", "DOUBLE"),
    ("is_playoffs", "INTEGER"),
    ("champion", "INTEGER"),
    ("manager", "VARCHAR"),
    ("team_key", "VARCHAR"),
    ("team_name", "VARCHAR"),
    ("team_points", "DOUBLE"),
    ("fantasy_points", "DOUBLE"),
)


def _narrow_rollup_projection(source_columns: set[str]) -> str:
    """Project narrow facts and literal team aliases in the one cache scan."""

    projection = [
        f"CAST(p.{_table_sql(column)} AS {sql_type}) AS {_table_sql(column)}"
        if column.lower() in source_columns
        else f"CAST(NULL AS {sql_type}) AS {_table_sql(column)}"
        for column, sql_type in _NARROW_ROLLUP_SOURCE_COLUMNS
    ]
    team_expression = (
        f"CAST(p.{_table_sql('cohort_teams')} AS VARCHAR)"
        if "cohort_teams" in source_columns
        else "CAST(NULL AS VARCHAR)"
    )
    projection.extend(
        [
            f"{team_expression} AS {_table_sql('cohort_teams_rostered')}",
            f"{team_expression} AS {_table_sql('cohort_teams_started')}",
        ]
    )
    return ",\n            ".join(projection)


def _season_active_week_relation(source_table: str) -> str:
    """Return the sibling cached player-active-week lookup for a player source."""

    parts = source_table.split(".")
    return ".".join([*parts[:-1], "player_active_week"])


def _source_without_bad_identity_rows(
    connection: duckdb.DuckDBPyConnection,
    source_table: str,
) -> str:
    """Return the cache source with synthetic player-pool rows removed.

    The historical cache contains two confirmed expanded-player-pool artifacts:
    rows marked rostered with no team identity or points, and identityless rows
    for players that do not exist in the same season's cached NFL week map.
    Neither is a roster fact or an eligible-league denominator. The second rule
    deliberately preserves a real inactive rostered player whenever their ID
    appears in any NFL week that season.

    Older unit fixtures and compatibility inputs may not expose these fields;
    in that case there is no observable bad shape to exclude.
    """

    source = _relation_sql(source_table)
    columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {source}").fetchall()
    }
    if "__compact_source_guarded" in columns:
        return source
    if not _BAD_IDENTITY_ROW_COLUMNS.issubset(columns):
        return source

    exact_bad_shape = """
        COALESCE(CAST(p.is_rostered AS INTEGER), 0) = 1
        AND COALESCE(CAST(p.is_started AS INTEGER), 0) = 0
        AND COALESCE(TRIM(CAST(p.manager AS VARCHAR)), '') = ''
        AND COALESCE(TRIM(CAST(p.team_key AS VARCHAR)), '') = ''
        AND COALESCE(TRIM(CAST(p.team_name AS VARCHAR)), '') = ''
        AND p.team_points IS NULL
        AND COALESCE(CAST(p.fantasy_points AS DOUBLE), 0.0) = 0.0
    """
    active_lookup_name = _season_active_week_relation(source_table)
    active_lookup = _relation_sql(active_lookup_name)
    try:
        active_columns = {
            str(row[0]).lower()
            for row in connection.execute(f"DESCRIBE {active_lookup}").fetchall()
        }
    except duckdb.CatalogException:
        active_columns = set()
    if not {"nfl_player_id", "year"}.issubset(active_columns):
        return f"(SELECT * FROM {source} AS p WHERE NOT ({exact_bad_shape}))"

    inactive_identityless_shape = f"""
        COALESCE(TRIM(CAST(p.team_key AS VARCHAR)), '') = ''
        AND COALESCE(TRIM(CAST(p.team_name AS VARCHAR)), '') = ''
        AND NOT EXISTS (
          SELECT 1
          FROM {active_lookup} AS a
          WHERE CAST(a.NFL_player_id AS VARCHAR) = CAST(p.NFL_player_id AS VARCHAR)
            AND CAST(a.year AS INTEGER) = CAST(p.year AS INTEGER)
        )
    """
    return f"""
      (SELECT * FROM {source} AS p
       WHERE NOT ({exact_bad_shape})
         AND NOT ({inactive_identityless_shape}))
    """


def _team_capacity_column(
    connection: duckdb.DuckDBPyConnection,
    source_table: str,
    *,
    stat: str,
) -> str:
    """Return the literal league-size cohort column for either metric family.

    Rostered and started use separate numerator/denominator definitions, but
    neither is permitted to change a league's team-size cohort.  The legacy
    derived columns may still exist in an input lane for compatibility; they
    are intentionally ignored.
    """

    if stat not in {"rostered", "started"}:
        raise ValueError(f"unsupported capacity statistic: {stat}")
    return "cohort_teams"


def materialize_narrow_position_year_source(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    output_table: str,
    year: int,
    position: str,
) -> None:
    """Materialize one guarded cache lane for reuse by compact rollups.

    This is a connection-local DuckDB TEMP table: it reads the immutable cache
    once, applies the confirmed invalid-row guards once, and is discarded when
    the runner connection closes.  It is not a cache artifact or a new lineage.
    """

    source = _source_without_bad_identity_rows(connection, source_table)
    output = _table_sql(output_table)
    source_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {_relation_sql(source_table)}").fetchall()
    }
    projection = _narrow_rollup_projection(source_columns)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output} AS
        SELECT {projection}, TRUE AS __compact_source_guarded
        FROM {source} p
        WHERE CAST(p.year AS INTEGER) = ?
          AND {_canonical_position_sql('p.position')} = ?
        """,
        [year, position],
    )

def validate_narrow_position_capacity_allocation(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
) -> dict[str, object]:
    """Fail closed unless every eligible league has one valid tier per stat.

    This reads the already-materialized, connection-local position lane.  It
    therefore adds a negligible gate after the one raw-cache scan and makes
    allocation observable in every GitHub runner task. ``ALL`` is a real
    pooled lane: it covers the documented 2003--2010 historic pool and the
    rare nonhistorical league-year whose source settings omit team count. It
    must never be silently treated as a literal 8/10/12/14 tier.
    """

    source = _relation_sql(source_table)
    labels_sql = ", ".join(repr(label) for label in _POSITION_SLOTS.TIER_LABELS)
    rows = connection.execute(
        f"""
        WITH eligible_lanes AS (
          SELECT
            CAST(db_name AS VARCHAR) AS db_name,
            CAST(year AS INTEGER) AS year,
            CAST(cohort_roster AS VARCHAR) AS cohort_roster,
            COUNT(DISTINCT cohort_teams_rostered) FILTER (
              WHERE cohort_teams_rostered IN ({labels_sql})
            ) AS rostered_valid_tiers,
            COUNT(*) FILTER (
              WHERE cohort_teams_rostered IS NULL
                 OR cohort_teams_rostered NOT IN ({labels_sql})
            ) AS rostered_invalid_rows,
            COUNT(DISTINCT cohort_teams_started) FILTER (
              WHERE cohort_teams_started IN ({labels_sql})
            ) AS started_valid_tiers,
            COUNT(*) FILTER (
              WHERE cohort_teams_started IS NULL
                 OR cohort_teams_started NOT IN ({labels_sql})
            ) AS started_invalid_rows,
            MIN(cohort_teams_rostered) FILTER (
              WHERE cohort_teams_rostered IN ({labels_sql})
            ) AS rostered_tier,
            MIN(cohort_teams_started) FILTER (
              WHERE cohort_teams_started IN ({labels_sql})
            ) AS started_tier
          FROM {source}
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
            AND CAST(year AS INTEGER) NOT BETWEEN 2003 AND 2010
            AND cohort_teams_rostered <> 'ALL'
            AND cohort_teams_started <> 'ALL'
          GROUP BY 1, 2, 3
        )
        SELECT
          COUNT(*)::BIGINT AS eligible_lanes,
          COUNT(*) FILTER (
            WHERE rostered_valid_tiers <> 1 OR rostered_invalid_rows <> 0
          )::BIGINT AS invalid_rostered_lanes,
          COUNT(*) FILTER (
            WHERE started_valid_tiers <> 1 OR started_invalid_rows <> 0
          )::BIGINT AS invalid_started_lanes
        FROM eligible_lanes
        """
    ).fetchone()
    pooled_rows = connection.execute(
        f"""
        WITH pooled_lanes AS (
          SELECT
            CAST(db_name AS VARCHAR) AS db_name,
            CAST(year AS INTEGER) AS year,
            CAST(cohort_roster AS VARCHAR) AS cohort_roster,
            COUNT(DISTINCT cohort_teams_rostered) FILTER (
              WHERE cohort_teams_rostered = 'ALL'
            ) AS rostered_all_tiers,
            COUNT(*) FILTER (
              WHERE cohort_teams_rostered IS NULL OR cohort_teams_rostered <> 'ALL'
            ) AS rostered_non_all_rows,
            COUNT(DISTINCT cohort_teams_started) FILTER (
              WHERE cohort_teams_started = 'ALL'
            ) AS started_all_tiers,
            COUNT(*) FILTER (
              WHERE cohort_teams_started IS NULL OR cohort_teams_started <> 'ALL'
            ) AS started_non_all_rows
          FROM {source}
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
            AND (
              CAST(year AS INTEGER) BETWEEN 2003 AND 2010
              OR cohort_teams_rostered = 'ALL'
              OR cohort_teams_started = 'ALL'
            )
          GROUP BY 1, 2, 3
        )
        SELECT
          COUNT(*) FILTER (
            WHERE year BETWEEN 2003 AND 2010
          )::BIGINT AS historical_lanes,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010
          )::BIGINT AS unknown_team_count_pooled_lanes,
          COUNT(*) FILTER (
            WHERE rostered_all_tiers <> 1 OR rostered_non_all_rows <> 0
               OR started_all_tiers <> 1 OR started_non_all_rows <> 0
          )::BIGINT AS invalid_pooled_lanes
        FROM pooled_lanes
        """
    ).fetchone()
    # The grouped LIST query above deliberately keeps the result compact, but
    # it cannot provide independent tier totals when roster/start differ. Read
    # those small lane groups directly for transparent runner logging.
    distributions = connection.execute(
        f"""
        WITH eligible_lanes AS (
          SELECT
            CAST(db_name AS VARCHAR) AS db_name,
            CAST(year AS INTEGER) AS year,
            CAST(cohort_roster AS VARCHAR) AS cohort_roster,
            MIN(cohort_teams_rostered) AS rostered_tier,
            MIN(cohort_teams_started) AS started_tier,
            COUNT(DISTINCT cohort_teams_rostered) FILTER (WHERE cohort_teams_rostered IN ({labels_sql})) AS rostered_valid_tiers,
            COUNT(*) FILTER (WHERE cohort_teams_rostered IS NULL OR cohort_teams_rostered NOT IN ({labels_sql})) AS rostered_invalid_rows,
            COUNT(DISTINCT cohort_teams_started) FILTER (WHERE cohort_teams_started IN ({labels_sql})) AS started_valid_tiers,
            COUNT(*) FILTER (WHERE cohort_teams_started IS NULL OR cohort_teams_started NOT IN ({labels_sql})) AS started_invalid_rows
          FROM {source}
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
            AND CAST(year AS INTEGER) NOT BETWEEN 2003 AND 2010
            AND cohort_teams_rostered <> 'ALL'
            AND cohort_teams_started <> 'ALL'
          GROUP BY 1, 2, 3
        )
        SELECT stat, tier, COUNT(*)::BIGINT AS lanes
        FROM (
          SELECT 'rostered' AS stat, rostered_tier AS tier
          FROM eligible_lanes
          WHERE rostered_valid_tiers = 1 AND rostered_invalid_rows = 0
          UNION ALL
          SELECT 'started', started_tier
          FROM eligible_lanes
          WHERE started_valid_tiers = 1 AND started_invalid_rows = 0
        )
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall()
    # ``rows`` has the authoritative lane count and invalid counts.  Raise
    # before the heavy core/grade rollups if even one eligible lane is unmapped.
    visible_lanes, invalid_rostered, invalid_started = rows
    historical_lanes, unknown_team_count_pooled, invalid_pooled = pooled_rows
    result: dict[str, object] = {
        "eligible_lanes": (
            int(visible_lanes or 0)
            + int(historical_lanes or 0)
            + int(unknown_team_count_pooled or 0)
        ),
        "visible_eligible_lanes": int(visible_lanes or 0),
        "historical_all_lanes": int(historical_lanes or 0),
        "unknown_team_count_pooled_lanes": int(unknown_team_count_pooled or 0),
        "invalid_rostered_lanes": int(invalid_rostered or 0),
        "invalid_started_lanes": int(invalid_started or 0),
        "invalid_pooled_lanes": int(invalid_pooled or 0),
        "rostered_tiers": {},
        "started_tiers": {},
    }
    for stat, tier, lanes in distributions:
        result[f"{stat}_tiers"][str(tier)] = int(lanes)
    if result["invalid_rostered_lanes"]:
        raise RuntimeError(
            "invalid rostered capacity allocation: "
            + str(result["invalid_rostered_lanes"])
            + " eligible league lanes do not have exactly one visible tier"
        )
    if result["invalid_started_lanes"]:
        raise RuntimeError(
            "invalid started capacity allocation: "
            + str(result["invalid_started_lanes"])
            + " eligible league lanes do not have exactly one visible tier"
        )
    if result["invalid_pooled_lanes"]:
        raise RuntimeError(
            "invalid pooled capacity allocation: "
            + str(result["invalid_pooled_lanes"])
            + " league lanes are not consistently pooled to ALL"
        )
    for stat in ("rostered", "started"):
        tier_total = sum(result[f"{stat}_tiers"].values())
        if tier_total != result["visible_eligible_lanes"]:
            raise RuntimeError(
                f"{stat} capacity distribution accounts for {tier_total} visible "
                f"league lanes; expected {result['visible_eligible_lanes']}"
            )
        ten_twelve = sum(
            result[f"{stat}_tiers"].get(tier, 0)
            for tier in ("10tm", "12tm")
        )
        result[f"{stat}_10_12_lanes"] = ten_twelve
        result[f"{stat}_10_12_share"] = (
            ten_twelve / result["visible_eligible_lanes"]
            if result["visible_eligible_lanes"]
            else None
        )
    return result


def materialize_player_id_subset_source(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    output_table: str,
    player_ids: list[str],
) -> None:
    """Materialize one temporary player bucket from an already-guarded lane."""

    if not player_ids:
        raise ValueError("player_ids must not be empty")
    source = _relation_sql(source_table)
    output = _table_sql(output_table)
    placeholders = ", ".join("?" for _ in player_ids)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output} AS
        SELECT *
        FROM {source}
        WHERE CAST(NFL_player_id AS VARCHAR) IN ({placeholders})
        """,
        player_ids,
    )


def materialize_weekly_outer_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    valid_player_weeks_table: str | None = None,
    output_table: str,
    year: int,
    week: int | None,
    player_id: str | None,
) -> None:
    """Materialize one compact weekly outer row for a player/time cache slice.

    This first primitive intentionally emits the full-pool ``ALL`` cell only.
    Cohort fanout resolution is layered on this exact raw-count contract rather
    than duplicating its denominator and roster semantics.  When a cached
    regular-game map is supplied, output identity weeks are restricted to that
    map: roster/admin facts from NFL byes or postseason cannot manufacture UI
    rows.  The population denominator remains the complete source population
    for each retained player week.
    """

    source = _source_without_bad_identity_rows(connection, source_table)
    valid_player_weeks = (
        None if valid_player_weeks_table is None else _relation_sql(valid_player_weeks_table)
    )
    output = _table_sql(output_table)
    week_predicate = "" if week is None else "AND CAST(week AS INTEGER) = ?"
    valid_player_week_cte = (
        """
        valid_player_weeks AS (
          SELECT DISTINCT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week
          FROM {valid_player_weeks}
        ),
        """.format(valid_player_weeks=valid_player_weeks)
        if valid_player_weeks is not None
        else ""
    )
    valid_player_week_join = (
        """
          INNER JOIN valid_player_weeks g
            ON g.NFL_player_id = CAST(f.NFL_player_id AS VARCHAR)
           AND g.year = CAST(f.year AS INTEGER)
           AND g.week = CAST(f.week AS INTEGER)
        """
        if valid_player_weeks is not None
        else ""
    )
    parameters: list[object] = [year]
    if week is not None:
        parameters.append(week)
    parameters.extend([player_id, year])
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH {valid_player_week_cte}population AS (
          SELECT
            db_name, year, week, NFL_player_id, {_canonical_position_sql('position')} AS position,
            cohort_position_eligible, is_rostered, is_started, win, clutch_equity
          FROM {source}
          WHERE CAST(year AS INTEGER) = ?
            {week_predicate}
        ),
        eligible_by_position AS (
          SELECT
            CAST(week AS INTEGER) AS week,
            CAST(position AS VARCHAR) AS position,
            COUNT(DISTINCT CAST(db_name AS VARCHAR))::BIGINT AS eligible_leagues
          FROM population
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
          GROUP BY 1, 2
        ),
        player_per_league AS (
          SELECT
            CAST(f.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(f.week AS INTEGER) AS week,
            CAST(f.db_name AS VARCHAR) AS db_name,
            MAX(CAST(f.position AS VARCHAR)) AS position,
            MAX(CASE
              WHEN f.is_rostered IS NULL OR CAST(f.is_rostered AS INTEGER) <> 0 THEN 1
              ELSE 0
            END)::BIGINT AS rostered,
            MAX(CASE WHEN CAST(f.is_started AS INTEGER) = 1 THEN 1 ELSE 0 END)::BIGINT AS started,
            MAX(CASE
              WHEN CAST(f.is_started AS INTEGER) = 1
               AND CAST(f.win AS DOUBLE) BETWEEN 0 AND 1 THEN 1
              ELSE 0
            END)::BIGINT AS valid_started_outcome,
            MAX(CASE
              WHEN CAST(f.is_started AS INTEGER) = 1
               AND CAST(f.win AS DOUBLE) BETWEEN 0 AND 1 THEN CAST(f.win AS DOUBLE)
            END) AS win_equivalent,
            AVG(CASE WHEN CAST(f.is_started AS INTEGER) = 1 THEN CAST(f.clutch_equity AS DOUBLE) END)
              AS clutch_when_started
          FROM population f
          {valid_player_week_join}
          WHERE CAST(f.NFL_player_id AS VARCHAR) = ?
          GROUP BY 1, 2, 3
        ),
        player_metric AS (
          SELECT
            p.NFL_player_id,
            p.week,
            p.position,
            SUM(p.rostered)::BIGINT AS rostered_leagues,
            SUM(p.started)::BIGINT AS started_leagues,
            SUM(p.valid_started_outcome)::BIGINT AS valid_started_outcomes,
            SUM(p.win_equivalent) AS win_equivalent,
            COALESCE(SUM(p.clutch_when_started), 0.0) / NULLIF(MAX(e.eligible_leagues), 0)
              AS clutch_weekly_average,
            MAX(e.eligible_leagues)::BIGINT AS eligible_leagues
          FROM player_per_league p
          INNER JOIN eligible_by_position e ON e.position = p.position AND e.week = p.week
          GROUP BY p.NFL_player_id, p.week, p.position
        ),
        cell_metric AS (
          SELECT
            'ALL'::VARCHAR AS cohort_key,
            NFL_player_id,
            week,
            position,
            rostered_leagues,
            started_leagues,
            valid_started_outcomes,
            win_equivalent,
            clutch_weekly_average,
            eligible_leagues,
            100.0 * rostered_leagues / NULLIF(eligible_leagues, 0) AS roster_rate_pct,
            100.0 * started_leagues / NULLIF(eligible_leagues, 0) AS start_rate_pct,
            100.0 * win_equivalent / NULLIF(valid_started_outcomes, 0) AS win_rate_pct,
            1.0 * started_leagues / NULLIF(eligible_leagues, 0) AS expected_starts,
            1.0 * started_leagues / NULLIF(eligible_leagues, 0)
              * win_equivalent / NULLIF(valid_started_outcomes, 0) AS expected_wins,
            1.0 * started_leagues / NULLIF(eligible_leagues, 0)
              * (1.0 - win_equivalent / NULLIF(valid_started_outcomes, 0)) AS expected_losses
          FROM player_metric
        )
        SELECT
          NFL_player_id,
          ?::INTEGER AS year,
          week,
          MAX(position) AS position,
          list(struct_pack(
            cohort_key := cohort_key,
            rostered_leagues := rostered_leagues,
            started_leagues := started_leagues,
            eligible_leagues := eligible_leagues,
            valid_started_outcomes := valid_started_outcomes,
            win_equivalent := win_equivalent,
            roster_rate_pct := roster_rate_pct,
            start_rate_pct := start_rate_pct,
            win_rate_pct := win_rate_pct,
            expected_starts := expected_starts,
            expected_wins := expected_wins,
            expected_losses := expected_losses,
            clutch_weekly_average := clutch_weekly_average
          ) ORDER BY cohort_key) AS cohort_cells
        FROM cell_metric
        GROUP BY NFL_player_id, week
        """,
        parameters,
    )


def materialize_weekly_core_cohort_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    valid_player_weeks_table: str | None = None,
    target_players_table: str | None = None,
    output_table: str,
    year: int,
    week: int | None,
    player_id: str | None,
    position: str | None = None,
    split_threshold: int = 150,
    team_stat: str = "started",
    include_roster_map: bool = True,
) -> None:
    """Build compact weekly core cells with independent cohort-dimension pooling.

    The core key deliberately excludes playoff bracket.  A source league is
    resolved independently for each of the six core dimensions, so a thin PPR
    dimension can pool to ``ALL`` while its thick team-size or roster dimension
    remains exact.  The resulting cells contain every raw count needed for the
    core weekly rates; they do not yet carry bracket-specific playoff grades.
    """

    if split_threshold < 1:
        raise ValueError("split_threshold must be positive")
    source = _source_without_bad_identity_rows(connection, source_table)
    teams_column = _team_capacity_column(
        connection, source_table, stat=team_stat
    )
    valid_player_weeks = (
        None if valid_player_weeks_table is None else _relation_sql(valid_player_weeks_table)
    )
    target_players = (
        None if target_players_table is None else _relation_sql(target_players_table)
    )
    output = _table_sql(output_table)
    week_predicate = "" if week is None else "AND CAST(week AS INTEGER) = ?"
    position_predicate = "" if position is None else f"AND {_canonical_position_sql('position')} = ?"
    valid_player_week_cte = (
        """
        valid_player_weeks AS (
          SELECT DISTINCT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week
          FROM {valid_player_weeks}
        ),
        """.format(valid_player_weeks=valid_player_weeks)
        if valid_player_weeks is not None
        else ""
    )
    valid_player_week_join = (
        """
          INNER JOIN valid_player_weeks g
            ON g.NFL_player_id = CAST(p.NFL_player_id AS VARCHAR)
           AND g.year = CAST(p.year AS INTEGER)
           AND g.week = CAST(p.week AS INTEGER)
        """
        if valid_player_weeks is not None
        else ""
    )
    target_player_join = (
        ""
        if target_players is None
        else f"""
          INNER JOIN {target_players} target
            ON CAST(target.NFL_player_id AS VARCHAR) = CAST(p.NFL_player_id AS VARCHAR)
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH {valid_player_week_cte}population AS (
          SELECT
            db_name, year, week, NFL_player_id, {_canonical_position_sql('position')} AS position,
            cohort_position_eligible,
            {_capacity_tier_sql(_table_sql(teams_column))} AS cohort_teams,
            cohort_roster,
            cohort_scoring, cohort_pass_td, cohort_playoff_teams,
            cohort_dynasty, cohort_best_ball,
            is_rostered, is_started, win, clutch_equity
          FROM {source}
          WHERE CAST(year AS INTEGER) = ?
            {week_predicate}
            {position_predicate}
        ),
        eligible_inventory AS (
          SELECT DISTINCT
            CAST(db_name AS VARCHAR) AS db_name,
            CAST(week AS INTEGER) AS week,
            CAST(position AS VARCHAR) AS position,
            CAST(cohort_teams AS VARCHAR) AS cohort_teams,
            CAST(cohort_roster AS VARCHAR) AS cohort_roster,
            CAST(cohort_scoring AS VARCHAR) AS cohort_scoring,
            CAST(cohort_pass_td AS VARCHAR) AS cohort_pass_td,
            CAST(cohort_playoff_teams AS VARCHAR) AS cohort_playoff_teams,
            CAST(cohort_dynasty AS VARCHAR) AS cohort_dynasty,
            CAST(cohort_best_ball AS VARCHAR) AS cohort_best_ball
          FROM population
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
        ),
        dimension_counts AS (
          SELECT
            position, week,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '08tm') AS teams_08tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '10tm') AS teams_10tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '12tm') AS teams_12tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '14tm') AS teams_14tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'flx') AS roster_flx,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'sflx') AS roster_sflx,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'idp') AS roster_idp,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'std') AS scoring_std,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'half') AS scoring_half,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'ppr') AS scoring_ppr,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_pass_td = '4pt') AS pass_td_4pt,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_pass_td = '6pt') AS pass_td_6pt,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_dynasty = 'redraft') AS dynasty_redraft,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_dynasty = 'dynasty') AS dynasty_dynasty,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_best_ball = 'managed') AS best_ball_managed,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_best_ball = 'best_ball') AS best_ball_best_ball
          FROM eligible_inventory
          GROUP BY position, week
        ),
        resolved_inventory AS (
          SELECT
            i.*,
            CASE WHEN
              (i.cohort_teams = '08tm' AND c.teams_08tm >= {split_threshold})
              OR (i.cohort_teams = '10tm' AND c.teams_10tm >= {split_threshold})
              OR (i.cohort_teams = '12tm' AND c.teams_12tm >= {split_threshold})
              OR (i.cohort_teams = '14tm' AND c.teams_14tm >= {split_threshold})
              THEN i.cohort_teams ELSE 'ALL' END AS q_teams,
            CASE WHEN
              (i.cohort_roster = 'flx' AND c.roster_flx >= {split_threshold})
              OR (i.cohort_roster = 'sflx' AND c.roster_sflx >= {split_threshold})
              OR (i.cohort_roster = 'idp' AND c.roster_idp >= {split_threshold})
              THEN i.cohort_roster ELSE 'ALL' END AS q_roster,
            CASE WHEN
              (i.cohort_scoring = 'std' AND c.scoring_std >= {split_threshold})
              OR (i.cohort_scoring = 'half' AND c.scoring_half >= {split_threshold})
              OR (i.cohort_scoring = 'ppr' AND c.scoring_ppr >= {split_threshold})
              THEN i.cohort_scoring ELSE 'ALL' END AS q_scoring,
            CASE WHEN
              (i.cohort_pass_td = '4pt' AND c.pass_td_4pt >= {split_threshold})
              OR (i.cohort_pass_td = '6pt' AND c.pass_td_6pt >= {split_threshold})
              THEN i.cohort_pass_td ELSE 'ALL' END AS q_pass_td,
            CASE WHEN
              (i.cohort_dynasty = 'redraft' AND c.dynasty_redraft >= {split_threshold})
              OR (i.cohort_dynasty = 'dynasty' AND c.dynasty_dynasty >= {split_threshold})
              THEN i.cohort_dynasty ELSE 'ALL' END AS q_dynasty,
            CASE WHEN
              (i.cohort_best_ball = 'managed' AND c.best_ball_managed >= {split_threshold})
              OR (i.cohort_best_ball = 'best_ball' AND c.best_ball_best_ball >= {split_threshold})
              THEN i.cohort_best_ball ELSE 'ALL' END AS q_best_ball
          FROM eligible_inventory i
          INNER JOIN dimension_counts c USING (position, week)
        ),
        eligible_profile AS (
          SELECT
            position, week, q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball,
            COUNT(DISTINCT db_name)::BIGINT AS eligible_leagues
          FROM resolved_inventory
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ),
        eligible_cube AS (
          SELECT
            position, week,
            CASE WHEN GROUPING(q_teams) = 1 THEN 'ALL' ELSE q_teams END AS q_teams,
            CASE WHEN GROUPING(q_roster) = 1 THEN 'ALL' ELSE q_roster END AS q_roster,
            CASE WHEN GROUPING(q_scoring) = 1 THEN 'ALL' ELSE q_scoring END AS q_scoring,
            CASE WHEN GROUPING(q_pass_td) = 1 THEN 'ALL' ELSE q_pass_td END AS q_pass_td,
            CASE WHEN GROUPING(q_dynasty) = 1 THEN 'ALL' ELSE q_dynasty END AS q_dynasty,
            CASE WHEN GROUPING(q_best_ball) = 1 THEN 'ALL' ELSE q_best_ball END AS q_best_ball,
            GROUPING(q_teams) AS grouped_teams,
            GROUPING(q_roster) AS grouped_roster,
            GROUPING(q_scoring) AS grouped_scoring,
            GROUPING(q_pass_td) AS grouped_pass_td,
            GROUPING(q_dynasty) AS grouped_dynasty,
            GROUPING(q_best_ball) AS grouped_best_ball,
            SUM(eligible_leagues)::BIGINT AS eligible_leagues
          FROM eligible_profile
          GROUP BY position, week, CUBE(q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball)
        ),
        eligible_cell AS (
          SELECT
            position, week, q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball,
            MAX(eligible_leagues)::BIGINT AS eligible_leagues
          FROM eligible_cube
          WHERE (grouped_teams = 1 OR q_teams <> 'ALL')
            AND (grouped_roster = 1 OR q_roster <> 'ALL')
            AND (grouped_scoring = 1 OR q_scoring <> 'ALL')
            AND (grouped_pass_td = 1 OR q_pass_td <> 'ALL')
            AND (grouped_dynasty = 1 OR q_dynasty <> 'ALL')
            AND (grouped_best_ball = 1 OR q_best_ball <> 'ALL')
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ),
        player_per_league AS (
          SELECT
            CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(p.week AS INTEGER) AS week,
            CAST(p.db_name AS VARCHAR) AS db_name,
            MAX(CAST(p.position AS VARCHAR)) AS position,
            r.q_teams,
            r.q_roster,
            r.q_scoring,
            r.q_pass_td,
            r.q_dynasty,
            r.q_best_ball,
            MAX(CASE WHEN p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END)::BIGINT
              AS rostered,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1 THEN 1 ELSE 0 END)::BIGINT AS started,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1
                           AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN 1 ELSE 0 END)::BIGINT
              AS valid_started_outcome,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1
                           AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN CAST(p.win AS DOUBLE) END)
              AS win_equivalent,
            AVG(CASE WHEN CAST(p.is_started AS INTEGER) = 1 THEN CAST(p.clutch_equity AS DOUBLE) END)
              AS clutch_when_started
          FROM population p
          {valid_player_week_join}
          INNER JOIN (
            SELECT CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
              CAST(p.position AS VARCHAR) AS position
          FROM population p
          {target_player_join}
          WHERE (? IS NULL OR CAST(p.NFL_player_id AS VARCHAR) = ?)
          GROUP BY 1, 2
          HAVING MAX(CASE WHEN p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END) = 1
          ) candidates
            ON candidates.NFL_player_id = CAST(p.NFL_player_id AS VARCHAR)
           AND candidates.position = CAST(p.position AS VARCHAR)
          INNER JOIN resolved_inventory r
            ON r.db_name = CAST(p.db_name AS VARCHAR)
           AND r.week = CAST(p.week AS INTEGER)
           AND r.position = CAST(p.position AS VARCHAR)
           AND r.cohort_teams IS NOT DISTINCT FROM {_capacity_tier_sql('p.cohort_teams')}
           AND r.cohort_roster IS NOT DISTINCT FROM CAST(p.cohort_roster AS VARCHAR)
           AND r.cohort_scoring IS NOT DISTINCT FROM CAST(p.cohort_scoring AS VARCHAR)
           AND r.cohort_pass_td IS NOT DISTINCT FROM CAST(p.cohort_pass_td AS VARCHAR)
           AND r.cohort_playoff_teams IS NOT DISTINCT FROM CAST(p.cohort_playoff_teams AS VARCHAR)
           AND r.cohort_dynasty IS NOT DISTINCT FROM CAST(p.cohort_dynasty AS VARCHAR)
           AND r.cohort_best_ball IS NOT DISTINCT FROM CAST(p.cohort_best_ball AS VARCHAR)
          GROUP BY 1, 2, 3, 5, 6, 7, 8, 9, 10
        ),
        player_leaf AS (
          SELECT
            NFL_player_id, week, position,
            q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball,
            SUM(rostered)::BIGINT AS rostered_leagues,
            SUM(started)::BIGINT AS started_leagues,
            SUM(valid_started_outcome)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(win_equivalent), 0.0) AS win_equivalent,
            COALESCE(SUM(clutch_when_started), 0.0) AS clutch_sum,
            COUNT(clutch_when_started)::BIGINT AS clutch_leagues
          FROM player_per_league
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ),
        player_cube AS (
          SELECT
            NFL_player_id, week, position,
            CASE WHEN GROUPING(q_teams) = 1 THEN 'ALL' ELSE q_teams END AS q_teams,
            CASE WHEN GROUPING(q_roster) = 1 THEN 'ALL' ELSE q_roster END AS q_roster,
            CASE WHEN GROUPING(q_scoring) = 1 THEN 'ALL' ELSE q_scoring END AS q_scoring,
            CASE WHEN GROUPING(q_pass_td) = 1 THEN 'ALL' ELSE q_pass_td END AS q_pass_td,
            CASE WHEN GROUPING(q_dynasty) = 1 THEN 'ALL' ELSE q_dynasty END AS q_dynasty,
            CASE WHEN GROUPING(q_best_ball) = 1 THEN 'ALL' ELSE q_best_ball END AS q_best_ball,
            GROUPING(q_teams) AS grouped_teams,
            GROUPING(q_roster) AS grouped_roster,
            GROUPING(q_scoring) AS grouped_scoring,
            GROUPING(q_pass_td) AS grouped_pass_td,
            GROUPING(q_dynasty) AS grouped_dynasty,
            GROUPING(q_best_ball) AS grouped_best_ball,
            SUM(rostered_leagues)::BIGINT AS rostered_leagues,
            SUM(started_leagues)::BIGINT AS started_leagues,
            SUM(valid_started_outcomes)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(win_equivalent), 0.0) AS win_equivalent,
            COALESCE(SUM(clutch_sum), 0.0) AS clutch_sum,
            SUM(clutch_leagues)::BIGINT AS clutch_leagues
          FROM player_leaf
          GROUP BY NFL_player_id, week, position,
            CUBE(q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball)
        ),
        player_cell AS (
          SELECT
            NFL_player_id, week, position,
            q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball,
            rostered_leagues, started_leagues, valid_started_outcomes, win_equivalent,
            clutch_sum, clutch_leagues
          FROM player_cube
          WHERE (grouped_teams = 1 OR q_teams <> 'ALL')
            AND (grouped_roster = 1 OR q_roster <> 'ALL')
            AND (grouped_scoring = 1 OR q_scoring <> 'ALL')
            AND (grouped_pass_td = 1 OR q_pass_td <> 'ALL')
            AND (grouped_dynasty = 1 OR q_dynasty <> 'ALL')
            AND (grouped_best_ball = 1 OR q_best_ball <> 'ALL')
        ),
        cell_metric AS (
          SELECT
            p.NFL_player_id, p.week, p.position,
            p.q_teams, p.q_roster, p.q_scoring, p.q_pass_td, p.q_dynasty, p.q_best_ball,
            p.rostered_leagues,
            p.started_leagues,
            p.valid_started_outcomes,
            p.win_equivalent,
            ROUND(p.clutch_sum / NULLIF(e.eligible_leagues, 0), 9) AS clutch_weekly_average,
            e.eligible_leagues
          FROM player_cell p
          INNER JOIN eligible_cell e
            ON e.position = p.position
           AND e.week = p.week
           AND e.q_teams = p.q_teams AND e.q_roster = p.q_roster
           AND e.q_scoring = p.q_scoring AND e.q_pass_td = p.q_pass_td
           AND e.q_dynasty = p.q_dynasty AND e.q_best_ball = p.q_best_ball
        ),
        split_profile AS (
          SELECT
            position, week,
            CASE WHEN teams_08tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_08tm_split,
            CASE WHEN teams_10tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_10tm_split,
            CASE WHEN teams_12tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_12tm_split,
            CASE WHEN teams_14tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_14tm_split,
            CASE WHEN roster_flx >= {split_threshold} THEN 1 ELSE 0 END AS roster_flx_split,
            CASE WHEN roster_sflx >= {split_threshold} THEN 1 ELSE 0 END AS roster_sflx_split,
            CASE WHEN roster_idp >= {split_threshold} THEN 1 ELSE 0 END AS roster_idp_split,
            CASE WHEN scoring_std >= {split_threshold} THEN 1 ELSE 0 END AS scoring_std_split,
            CASE WHEN scoring_half >= {split_threshold} THEN 1 ELSE 0 END AS scoring_half_split,
            CASE WHEN scoring_ppr >= {split_threshold} THEN 1 ELSE 0 END AS scoring_ppr_split,
            CASE WHEN pass_td_4pt >= {split_threshold} THEN 1 ELSE 0 END AS pass_td_4pt_split,
            CASE WHEN pass_td_6pt >= {split_threshold} THEN 1 ELSE 0 END AS pass_td_6pt_split,
            CASE WHEN dynasty_redraft >= {split_threshold} THEN 1 ELSE 0 END AS dynasty_redraft_split,
            CASE WHEN dynasty_dynasty >= {split_threshold} THEN 1 ELSE 0 END AS dynasty_dynasty_split,
            CASE WHEN best_ball_managed >= {split_threshold} THEN 1 ELSE 0 END AS best_ball_managed_split,
            CASE WHEN best_ball_best_ball >= {split_threshold} THEN 1 ELSE 0 END AS best_ball_best_ball_split
          FROM dimension_counts
        )
        SELECT
          m.NFL_player_id,
          ?::INTEGER AS year,
          m.week,
          MAX(m.position) AS position,
          struct_pack(
            teams_08tm_split := MAX(s.teams_08tm_split),
            teams_10tm_split := MAX(s.teams_10tm_split),
            teams_12tm_split := MAX(s.teams_12tm_split),
            teams_14tm_split := MAX(s.teams_14tm_split),
            roster_flx_split := MAX(s.roster_flx_split),
            roster_sflx_split := MAX(s.roster_sflx_split),
            roster_idp_split := MAX(s.roster_idp_split),
            scoring_std_split := MAX(s.scoring_std_split),
            scoring_half_split := MAX(s.scoring_half_split),
            scoring_ppr_split := MAX(s.scoring_ppr_split),
            pass_td_4pt_split := MAX(s.pass_td_4pt_split),
            pass_td_6pt_split := MAX(s.pass_td_6pt_split),
            dynasty_redraft_split := MAX(s.dynasty_redraft_split),
            dynasty_dynasty_split := MAX(s.dynasty_dynasty_split),
            best_ball_managed_split := MAX(s.best_ball_managed_split),
            best_ball_best_ball_split := MAX(s.best_ball_best_ball_split)
          ) AS cohort_profile,
          list(struct_pack(
            q_teams := q_teams,
            q_roster := q_roster,
            q_scoring := q_scoring,
            q_pass_td := q_pass_td,
            q_dynasty := q_dynasty,
            q_best_ball := q_best_ball,
            eligible_leagues := eligible_leagues,
            rostered_leagues := rostered_leagues,
            started_leagues := started_leagues,
            valid_started_outcomes := valid_started_outcomes,
            win_equivalent := win_equivalent,
            roster_rate_pct := 100.0 * rostered_leagues / NULLIF(eligible_leagues, 0),
            start_rate_pct := 100.0 * started_leagues / NULLIF(eligible_leagues, 0),
            win_rate_pct := 100.0 * win_equivalent / NULLIF(valid_started_outcomes, 0),
            expected_starts := started_leagues::DOUBLE / NULLIF(eligible_leagues, 0),
            expected_wins := COALESCE(
              started_leagues::DOUBLE / NULLIF(eligible_leagues, 0)
              * win_equivalent / NULLIF(valid_started_outcomes, 0),
              0.0
            ),
            expected_losses := COALESCE(
              started_leagues::DOUBLE / NULLIF(eligible_leagues, 0)
              * (1.0 - win_equivalent / NULLIF(valid_started_outcomes, 0)),
              0.0
            ),
            clutch_weekly_average := clutch_weekly_average
          ) ORDER BY q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball) AS cohort_cells
        FROM cell_metric m
        INNER JOIN split_profile s ON s.position = m.position AND s.week = m.week
        GROUP BY m.NFL_player_id, m.week
        """,
        [year, *([] if week is None else [week]), *([] if position is None else [position]), player_id, player_id, year],
    )
    _compact_weekly_requested_cells(
        connection,
        output_table=output_table,
        split_threshold=split_threshold,
    )
    if include_roster_map and team_stat == "started":
        roster_output_table = f"{output_table}_roster_map"
        materialize_weekly_core_cohort_cells(
            connection,
            source_table=source_table,
            valid_player_weeks_table=valid_player_weeks_table,
            target_players_table=target_players_table,
            output_table=roster_output_table,
            year=year,
            week=week,
            player_id=player_id,
            position=position,
            split_threshold=split_threshold,
            team_stat="rostered",
            include_roster_map=False,
        )
        materialize_compact_core_selector_indices(
            connection, output_table=roster_output_table
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {output} AS
            SELECT
              started.*,
              roster.cohort_cells AS roster_cells,
              roster.core_selector_indices AS roster_selector_indices
            FROM {output} started
            INNER JOIN {_table_sql(roster_output_table)} roster
              ON roster.NFL_player_id = started.NFL_player_id
             AND roster.year = started.year
             AND roster.week = started.week
             AND roster.position = started.position
            """
        )
        connection.execute(f"DROP TABLE {_table_sql(roster_output_table)}")


def _compact_weekly_requested_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
    split_threshold: int,
) -> None:
    """Keep only cells reachable from one of the 288 supported UI requests.

    The aggregation query uses a cube to obtain correct partial pools.  A cube
    also emits cells no UI request can ever select.  This deterministic local
    compaction resolves each request to its most-specific cell with at least
    ``split_threshold`` rostered leagues.  A thin exact player sample therefore
    falls back to an already-materialized parent; it can never be served as a
    low-n exact cell.  Rows with no qualifying parent disappear entirely.
    """
    output = _table_sql(output_table)
    temp = _table_sql(f"{output_table}_requested_cells")
    _compact_core_cells_after_rostered_floor(
        connection,
        output_table=output_table,
        rostered_field="rostered_leagues",
        split_threshold=split_threshold,
    )
    return
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {temp} AS
        WITH requested AS (
          SELECT *
          FROM (VALUES ('08tm'), ('10tm'), ('12tm'), ('14tm')) teams(teams)
          CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) roster(roster)
          CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) scoring(scoring)
          CROSS JOIN (VALUES ('4pt'), ('6pt')) pass_td(pass_td)
          CROSS JOIN (VALUES ('redraft'), ('dynasty')) dynasty(dynasty)
          CROSS JOIN (VALUES ('managed'), ('best_ball')) best_ball(best_ball)
        ),
        candidates AS (
          SELECT r.NFL_player_id, r.year, r.week, r.position, r.cohort_profile,
            q.teams, q.roster, q.scoring, q.pass_td, q.dynasty, q.best_ball, cell,
            (
              CASE WHEN cell.q_teams <> 'ALL' THEN 32 ELSE 0 END
              + CASE WHEN cell.q_roster <> 'ALL' THEN 16 ELSE 0 END
              + CASE WHEN cell.q_scoring <> 'ALL' THEN 8 ELSE 0 END
              + CASE WHEN cell.q_pass_td <> 'ALL' THEN 4 ELSE 0 END
              + CASE WHEN cell.q_dynasty <> 'ALL' THEN 2 ELSE 0 END
              + CASE WHEN cell.q_best_ball <> 'ALL' THEN 1 ELSE 0 END
            ) AS retained_specificity
          FROM {output} r
          CROSS JOIN requested q
          CROSS JOIN UNNEST(r.cohort_cells) AS u(cell)
          WHERE cell.rostered_leagues >= {split_threshold}
            AND cell.q_teams IN ('ALL', q.teams)
            AND cell.q_roster IN ('ALL', q.roster)
            AND cell.q_scoring IN ('ALL', q.scoring)
            AND cell.q_pass_td IN ('ALL', q.pass_td)
            AND cell.q_dynasty IN ('ALL', q.dynasty)
            AND cell.q_best_ball IN ('ALL', q.best_ball)
        ),
        selected AS (
          SELECT NFL_player_id, year, week, position, cohort_profile, cell
          FROM candidates
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY NFL_player_id, year, week, position,
              teams, roster, scoring, pass_td, dynasty, best_ball
            ORDER BY retained_specificity DESC, cell.eligible_leagues ASC,
              cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
              cell.q_dynasty, cell.q_best_ball
          ) = 1
          UNION ALL
          SELECT r.NFL_player_id, r.year, r.week, r.position, r.cohort_profile, cell
          FROM {output} r
          CROSS JOIN UNNEST(r.cohort_cells) AS u(cell)
          WHERE cell.rostered_leagues >= {split_threshold}
            AND cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
            AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        ),
        dedup AS (
          SELECT NFL_player_id, year, week, position, cohort_profile,
            cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
            cell.q_dynasty, cell.q_best_ball, ANY_VALUE(cell) AS cell
          FROM selected
          GROUP BY ALL
        )
        SELECT NFL_player_id, year, week, position, ANY_VALUE(cohort_profile) AS cohort_profile,
          list(cell ORDER BY q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball) AS cohort_cells
        FROM dedup
        GROUP BY NFL_player_id, year, week, position
        """
    )
    connection.execute(f"CREATE OR REPLACE TABLE {output} AS SELECT * FROM {temp}")
    oversized = connection.execute(
        f"SELECT COUNT(*) FROM {output} WHERE list_count(cohort_cells) > 289"
    ).fetchone()[0]
    if oversized:
        raise RuntimeError("Weekly compact output exceeds 288 reachable requests plus its ALL audit cell")


def _materialize_compact_core_selector_indices_by_profile(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
) -> None:
    """Attach the 288-format request -> compact-cell ordinal access path.

    Compact rows intentionally keep one player/time/position outer row and a
    nested metric-cell list.  The split profile determines which cell answers
    each user format request.  Build that lookup once per distinct profile,
    rather than scanning the nested list for every live query row.
    """
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_selector_requests AS
      SELECT
        (teams.ordinal * 72 + roster.ordinal * 24 + scoring.ordinal * 8
          + pass_td.ordinal * 4 + dynasty.ordinal * 2 + best_ball.ordinal + 1)::USMALLINT
          AS request_ordinal,
        teams.q_teams,
        roster.q_roster,
        scoring.q_scoring,
        pass_td.q_pass_td,
        dynasty.q_dynasty,
        best_ball.q_best_ball
      FROM (VALUES ('08tm', 0), ('10tm', 1), ('12tm', 2), ('14tm', 3)) AS teams(q_teams, ordinal)
      CROSS JOIN (VALUES ('flx', 0), ('sflx', 1), ('idp', 2)) AS roster(q_roster, ordinal)
      CROSS JOIN (VALUES ('std', 0), ('half', 1), ('ppr', 2)) AS scoring(q_scoring, ordinal)
      CROSS JOIN (VALUES ('4pt', 0), ('6pt', 1)) AS pass_td(q_pass_td, ordinal)
      CROSS JOIN (VALUES ('redraft', 0), ('dynasty', 1)) AS dynasty(q_dynasty, ordinal)
      CROSS JOIN (VALUES ('managed', 0), ('best_ball', 1)) AS best_ball(q_best_ball, ordinal)
    """)
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_selector_profiles AS
      SELECT ROW_NUMBER() OVER ()::INTEGER AS profile_id, cohort_profile, cohort_cells
      FROM (
        SELECT cohort_profile, ANY_VALUE(cohort_cells) AS cohort_cells
        FROM {output_table}
        GROUP BY cohort_profile
      ) profiles
    """)
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_selector_cells AS
      SELECT p.profile_id, ordinal::USMALLINT AS cell_ordinal, cell
      FROM _compact_selector_profiles p
      CROSS JOIN UNNEST(p.cohort_cells) WITH ORDINALITY AS cells(cell, ordinal)
    """)
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_selector_indices AS
      WITH candidates AS (
        SELECT p.profile_id, r.request_ordinal, c.cell_ordinal,
          (
            CASE WHEN c.cell.q_teams = CASE
         WHEN r.q_teams = '08tm' AND p.cohort_profile.teams_08tm_split = 1 THEN '08tm'
         WHEN r.q_teams = '10tm' AND p.cohort_profile.teams_10tm_split = 1 THEN '10tm'
         WHEN r.q_teams = '12tm' AND p.cohort_profile.teams_12tm_split = 1 THEN '12tm'
         WHEN r.q_teams = '14tm' AND p.cohort_profile.teams_14tm_split = 1 THEN '14tm'
         ELSE 'ALL' END THEN 32 ELSE 0 END
            + CASE WHEN c.cell.q_roster = CASE
         WHEN r.q_roster = 'flx' AND p.cohort_profile.roster_flx_split = 1 THEN 'flx'
         WHEN r.q_roster = 'sflx' AND p.cohort_profile.roster_sflx_split = 1 THEN 'sflx'
         WHEN r.q_roster = 'idp' AND p.cohort_profile.roster_idp_split = 1 THEN 'idp'
         ELSE 'ALL' END THEN 16 ELSE 0 END
            + CASE WHEN c.cell.q_scoring = CASE
         WHEN r.q_scoring = 'std' AND p.cohort_profile.scoring_std_split = 1 THEN 'std'
         WHEN r.q_scoring = 'half' AND p.cohort_profile.scoring_half_split = 1 THEN 'half'
         WHEN r.q_scoring = 'ppr' AND p.cohort_profile.scoring_ppr_split = 1 THEN 'ppr'
         ELSE 'ALL' END THEN 8 ELSE 0 END
            + CASE WHEN c.cell.q_pass_td = CASE
         WHEN r.q_pass_td = '4pt' AND p.cohort_profile.pass_td_4pt_split = 1 THEN '4pt'
         WHEN r.q_pass_td = '6pt' AND p.cohort_profile.pass_td_6pt_split = 1 THEN '6pt'
         ELSE 'ALL' END THEN 4 ELSE 0 END
            + CASE WHEN c.cell.q_dynasty = CASE
         WHEN r.q_dynasty = 'redraft' AND p.cohort_profile.dynasty_redraft_split = 1 THEN 'redraft'
         WHEN r.q_dynasty = 'dynasty' AND p.cohort_profile.dynasty_dynasty_split = 1 THEN 'dynasty'
         ELSE 'ALL' END THEN 2 ELSE 0 END
            + CASE WHEN c.cell.q_best_ball = CASE
         WHEN r.q_best_ball = 'managed' AND p.cohort_profile.best_ball_managed_split = 1 THEN 'managed'
         WHEN r.q_best_ball = 'best_ball' AND p.cohort_profile.best_ball_best_ball_split = 1 THEN 'best_ball'
         ELSE 'ALL' END THEN 1 ELSE 0 END
          ) AS retained_specificity
        FROM _compact_selector_profiles p
        CROSS JOIN _compact_selector_requests r
        INNER JOIN _compact_selector_cells c ON c.profile_id = p.profile_id
        WHERE c.cell.q_teams IN ('ALL', CASE
          WHEN r.q_teams = '08tm' AND p.cohort_profile.teams_08tm_split = 1 THEN '08tm'
          WHEN r.q_teams = '10tm' AND p.cohort_profile.teams_10tm_split = 1 THEN '10tm'
          WHEN r.q_teams = '12tm' AND p.cohort_profile.teams_12tm_split = 1 THEN '12tm'
          WHEN r.q_teams = '14tm' AND p.cohort_profile.teams_14tm_split = 1 THEN '14tm'
          ELSE 'ALL' END)
          AND c.cell.q_roster IN ('ALL', CASE
          WHEN r.q_roster = 'flx' AND p.cohort_profile.roster_flx_split = 1 THEN 'flx'
          WHEN r.q_roster = 'sflx' AND p.cohort_profile.roster_sflx_split = 1 THEN 'sflx'
          WHEN r.q_roster = 'idp' AND p.cohort_profile.roster_idp_split = 1 THEN 'idp'
          ELSE 'ALL' END)
          AND c.cell.q_scoring IN ('ALL', CASE
          WHEN r.q_scoring = 'std' AND p.cohort_profile.scoring_std_split = 1 THEN 'std'
          WHEN r.q_scoring = 'half' AND p.cohort_profile.scoring_half_split = 1 THEN 'half'
          WHEN r.q_scoring = 'ppr' AND p.cohort_profile.scoring_ppr_split = 1 THEN 'ppr'
          ELSE 'ALL' END)
          AND c.cell.q_pass_td IN ('ALL', CASE
          WHEN r.q_pass_td = '4pt' AND p.cohort_profile.pass_td_4pt_split = 1 THEN '4pt'
          WHEN r.q_pass_td = '6pt' AND p.cohort_profile.pass_td_6pt_split = 1 THEN '6pt'
          ELSE 'ALL' END)
          AND c.cell.q_dynasty IN ('ALL', CASE
          WHEN r.q_dynasty = 'redraft' AND p.cohort_profile.dynasty_redraft_split = 1 THEN 'redraft'
          WHEN r.q_dynasty = 'dynasty' AND p.cohort_profile.dynasty_dynasty_split = 1 THEN 'dynasty'
          ELSE 'ALL' END)
          AND c.cell.q_best_ball IN ('ALL', CASE
          WHEN r.q_best_ball = 'managed' AND p.cohort_profile.best_ball_managed_split = 1 THEN 'managed'
          WHEN r.q_best_ball = 'best_ball' AND p.cohort_profile.best_ball_best_ball_split = 1 THEN 'best_ball'
          ELSE 'ALL' END)
      ), selected AS (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY profile_id, request_ordinal
          ORDER BY retained_specificity DESC, cell_ordinal
        ) AS selector_rank
        FROM candidates
      )
      SELECT profile_id,
        list(cell_ordinal::USMALLINT ORDER BY request_ordinal) AS core_selector_indices
      FROM selected
      WHERE selector_rank = 1
      GROUP BY profile_id
    """)
    invalid = connection.execute("""
      SELECT COUNT(*)
      FROM _compact_selector_indices
      WHERE list_count(core_selector_indices) <> 288
    """).fetchone()[0]
    if invalid:
        raise RuntimeError(f"compact core selector index incomplete for {invalid} split profiles")
    source_projection = _replace_selector_projection(
        connection,
        output_table=output_table,
        selector_column="core_selector_indices",
    )
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_with_selector_indices AS
      SELECT {source_projection}, selector.core_selector_indices
      FROM {output_table} source
      INNER JOIN _compact_selector_profiles profile
        ON source.cohort_profile = profile.cohort_profile
      INNER JOIN _compact_selector_indices selector
        ON selector.profile_id = profile.profile_id
    """)
    connection.execute(
        f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM _compact_with_selector_indices"
    )



def _replace_selector_projection(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
    selector_column: str,
) -> str:
    """Project an outer row while replacing a prior selector map, if present."""

    columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {_table_sql(output_table)}").fetchall()
    }
    return (
        f"source.* EXCLUDE ({_table_sql(selector_column)})"
        if selector_column.lower() in columns
        else "source.*"
    )

def _compact_core_cells_after_rostered_floor(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
    rostered_field: str,
    split_threshold: int,
) -> None:
    """Filter thin player cells, then resolve 288 requests once per layout.

    The raw cube can contain many cells per outer row.  Expanding every row by
    all requests is exactly the memory shape that OOMs a large QB lane.  First
    remove cells below the player rostered-sample floor.  The existing exact
    selector builder then shares a request map across every identical retained
    selector layout; finally retain only the mapped cells.  This keeps adaptive
    fallback entirely build-side without a player-row cross product.
    """

    if rostered_field not in {"rostered_leagues", "rostered_league_weeks"}:
        raise ValueError(f"unsupported rostered sample field: {rostered_field}")
    output = _table_sql(output_table)
    floored = _table_sql(f"{output_table}_rostered_floor")
    reduced = _table_sql(f"{output_table}_requested_cells")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {floored} AS
        SELECT source.* REPLACE (
          list_filter(
            cohort_cells,
            cell -> cell.{rostered_field} >= {split_threshold}
              AND cell.eligible_leagues >= {split_threshold}
          ) AS cohort_cells
        )
        FROM {output} AS source
        WHERE list_count(
          list_filter(
            cohort_cells,
            cell -> cell.{rostered_field} >= {split_threshold}
              AND cell.eligible_leagues >= {split_threshold}
          )
        ) > 0
        """
    )
    connection.execute(f"CREATE OR REPLACE TABLE {output} AS SELECT * FROM {floored}")
    # This groups identical retained layouts before considering 288 UI formats.
    materialize_compact_exact_core_selector_indices(
        connection,
        output_table=output_table,
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {reduced} AS
        SELECT source.* REPLACE (
          list_distinct(list_concat(
            list_select(
              source.cohort_cells,
              list_sort(list_distinct(source.core_selector_indices))
            ),
            list_filter(
              source.cohort_cells,
              cell -> cell.q_teams = 'ALL' AND cell.q_roster = 'ALL'
                AND cell.q_scoring = 'ALL' AND cell.q_pass_td = 'ALL'
                AND cell.q_dynasty = 'ALL' AND cell.q_best_ball = 'ALL'
            )
          )) AS cohort_cells
        )
        FROM {output} AS source
        """
    )
    connection.execute(f"CREATE OR REPLACE TABLE {output} AS SELECT * FROM {reduced}")
    materialize_compact_exact_core_selector_indices(
        connection,
        output_table=output_table,
    )
    oversized = connection.execute(
        f"SELECT COUNT(*) FROM {output} WHERE list_count(cohort_cells) > 289"
    ).fetchone()[0]
    if oversized:
        raise RuntimeError("Core compact output exceeds 288 reachable requests plus its ALL audit cell")


def materialize_compact_core_selector_indices(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
) -> None:
    """Attach core selector ordinals keyed by each realized nested-cell layout.

    A split profile does not guarantee that all player/time rows have the same
    populated cells.  Sharing an arbitrary profile representative can attach
    an out-of-range ordinal to a shorter list, so the serving path shares maps
    only across identical layouts.
    """
    materialize_compact_exact_core_selector_indices(
        connection,
        output_table=output_table,
    )


def materialize_compact_grade_selector_indices(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
) -> None:
    """Attach grade selector ordinals keyed by each realized grade-cell layout."""
    materialize_compact_exact_grade_selector_indices(
        connection,
        output_table=output_table,
    )


def materialize_compact_exact_core_selector_indices(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
) -> None:
    """Attach the four-tier, 288-format nearest-existing-cell ordinal map.

    Career cells already contain their built cohort values but no season split
    profile. The serving contract therefore selects the nearest existing
    partial pool for a requested format, with fully pooled ALL as its final
    fallback. The index is shared by identical cell layouts, then attached to
    each compact outer row without expanding player rows.
    """
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_exact_selector_requests AS
      SELECT
        (teams.ordinal * 72 + roster.ordinal * 24 + scoring.ordinal * 8
          + pass_td.ordinal * 4 + dynasty.ordinal * 2 + best_ball.ordinal + 1)::USMALLINT
          AS request_ordinal,
        teams.q_teams,
        roster.q_roster,
        scoring.q_scoring,
        pass_td.q_pass_td,
        dynasty.q_dynasty,
        best_ball.q_best_ball
      FROM (VALUES ('08tm', 0), ('10tm', 1), ('12tm', 2), ('14tm', 3)) AS teams(q_teams, ordinal)
      CROSS JOIN (VALUES ('flx', 0), ('sflx', 1), ('idp', 2)) AS roster(q_roster, ordinal)
      CROSS JOIN (VALUES ('std', 0), ('half', 1), ('ppr', 2)) AS scoring(q_scoring, ordinal)
      CROSS JOIN (VALUES ('4pt', 0), ('6pt', 1)) AS pass_td(q_pass_td, ordinal)
      CROSS JOIN (VALUES ('redraft', 0), ('dynasty', 1)) AS dynasty(q_dynasty, ordinal)
      CROSS JOIN (VALUES ('managed', 0), ('best_ball', 1)) AS best_ball(q_best_ball, ordinal)
    """)
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_exact_selector_layouts AS
      WITH source_layouts AS (
        SELECT
          list_transform(cohort_cells, cell ->
            cell.q_teams || '|' || cell.q_roster || '|' || cell.q_scoring || '|'
            || cell.q_pass_td || '|' || cell.q_dynasty || '|' || cell.q_best_ball
          ) AS selector_layout,
          cohort_cells
        FROM {output_table}
      )
      SELECT ROW_NUMBER() OVER ()::INTEGER AS layout_id,
        selector_layout,
        ANY_VALUE(cohort_cells) AS cohort_cells
      FROM source_layouts
      GROUP BY selector_layout
    """)
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_exact_selector_cells AS
      SELECT layout.layout_id, ordinal::USMALLINT AS cell_ordinal, cell
      FROM _compact_exact_selector_layouts layout
      CROSS JOIN UNNEST(layout.cohort_cells) WITH ORDINALITY AS cells(cell, ordinal)
    """)
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_exact_selector_indices AS
      WITH candidates AS (
        SELECT layout.layout_id, request.request_ordinal, cell.cell_ordinal,
          (CASE WHEN cell.cell.q_teams <> 'ALL' THEN 32 ELSE 0 END
           + CASE WHEN cell.cell.q_roster <> 'ALL' THEN 16 ELSE 0 END
           + CASE WHEN cell.cell.q_scoring <> 'ALL' THEN 8 ELSE 0 END
           + CASE WHEN cell.cell.q_pass_td <> 'ALL' THEN 4 ELSE 0 END
           + CASE WHEN cell.cell.q_dynasty <> 'ALL' THEN 2 ELSE 0 END
           + CASE WHEN cell.cell.q_best_ball <> 'ALL' THEN 1 ELSE 0 END
          ) AS retained_specificity
        FROM _compact_exact_selector_layouts layout
        CROSS JOIN _compact_exact_selector_requests request
        INNER JOIN _compact_exact_selector_cells cell ON cell.layout_id = layout.layout_id
        WHERE cell.cell.q_teams IN ('ALL', request.q_teams)
          AND cell.cell.q_roster IN ('ALL', request.q_roster)
          AND cell.cell.q_scoring IN ('ALL', request.q_scoring)
          AND cell.cell.q_pass_td IN ('ALL', request.q_pass_td)
          AND cell.cell.q_dynasty IN ('ALL', request.q_dynasty)
          AND cell.cell.q_best_ball IN ('ALL', request.q_best_ball)
      ), selected AS (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY layout_id, request_ordinal
          ORDER BY retained_specificity DESC, cell_ordinal
        ) AS selector_rank
        FROM candidates
      )
      SELECT layout_id,
        list(cell_ordinal::USMALLINT ORDER BY request_ordinal) AS core_selector_indices
      FROM selected
      WHERE selector_rank = 1
      GROUP BY layout_id
    """)
    invalid = connection.execute("""
      SELECT COUNT(*)
      FROM _compact_exact_selector_indices
      WHERE list_count(core_selector_indices) <> 288
         OR list_extract(core_selector_indices, 1) IS NULL
         OR list_extract(core_selector_indices, 288) IS NULL
    """).fetchone()[0]
    if invalid:
        raise RuntimeError(f"compact exact selector index incomplete for {invalid} cell layouts")
    source_projection = _replace_selector_projection(
        connection,
        output_table=output_table,
        selector_column="core_selector_indices",
    )
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_exact_with_selector_indices AS
      SELECT {source_projection}, selector.core_selector_indices
      FROM {output_table} source
      INNER JOIN _compact_exact_selector_layouts layout
        ON list_transform(source.cohort_cells, cell ->
          cell.q_teams || '|' || cell.q_roster || '|' || cell.q_scoring || '|'
          || cell.q_pass_td || '|' || cell.q_dynasty || '|' || cell.q_best_ball
        ) = layout.selector_layout
      INNER JOIN _compact_exact_selector_indices selector
        ON selector.layout_id = layout.layout_id
    """)
    connection.execute(
        f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM _compact_exact_with_selector_indices"
    )
    dangling = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {output_table} source
        CROSS JOIN range(1, 289) AS request(request_ordinal)
        WHERE COALESCE(
          list_extract(source.core_selector_indices, request.request_ordinal),
          0
        ) NOT BETWEEN 1 AND list_count(source.cohort_cells)
        """
    ).fetchone()[0]
    if dangling:
        raise RuntimeError(
            f"compact core selector index points outside final packed cells for {dangling} requests"
        )


def materialize_compact_exact_grade_selector_indices(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
) -> None:
    """Attach the 864 visible core/bracket request -> grade-cell ordinal map."""
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_exact_grade_requests AS
      SELECT
        (teams.ordinal * 216 + roster.ordinal * 72 + scoring.ordinal * 24
          + pass_td.ordinal * 12 + dynasty.ordinal * 6 + best_ball.ordinal * 3
          + playoff_teams.ordinal + 1)::USMALLINT AS request_ordinal,
        teams.q_teams, roster.q_roster, scoring.q_scoring, pass_td.q_pass_td,
        dynasty.q_dynasty, best_ball.q_best_ball, playoff_teams.q_playoff_teams
      FROM (VALUES ('08tm', 0), ('10tm', 1), ('12tm', 2), ('14tm', 3)) AS teams(q_teams, ordinal)
      CROSS JOIN (VALUES ('flx', 0), ('sflx', 1), ('idp', 2)) AS roster(q_roster, ordinal)
      CROSS JOIN (VALUES ('std', 0), ('half', 1), ('ppr', 2)) AS scoring(q_scoring, ordinal)
      CROSS JOIN (VALUES ('4pt', 0), ('6pt', 1)) AS pass_td(q_pass_td, ordinal)
      CROSS JOIN (VALUES ('redraft', 0), ('dynasty', 1)) AS dynasty(q_dynasty, ordinal)
      CROSS JOIN (VALUES ('managed', 0), ('best_ball', 1)) AS best_ball(q_best_ball, ordinal)
      CROSS JOIN (VALUES ('4po', 0), ('6po', 1), ('8po', 2))
        AS playoff_teams(q_playoff_teams, ordinal)
    """)
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_exact_grade_layouts AS
      WITH source_layouts AS (
        SELECT
          list_transform(grade_cells, grade ->
            grade.q_teams || '|' || grade.q_roster || '|' || grade.q_scoring || '|'
            || grade.q_pass_td || '|' || grade.q_dynasty || '|' || grade.q_best_ball
            || '|' || grade.q_playoff_teams
          ) AS selector_layout,
          grade_cells
        FROM {output_table}
      )
      SELECT ROW_NUMBER() OVER ()::INTEGER AS layout_id,
        selector_layout,
        ANY_VALUE(grade_cells) AS grade_cells
      FROM source_layouts
      GROUP BY selector_layout
    """)
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_exact_grade_cells AS
      SELECT layout.layout_id, ordinal::USMALLINT AS cell_ordinal, grade
      FROM _compact_exact_grade_layouts layout
      CROSS JOIN UNNEST(layout.grade_cells) WITH ORDINALITY AS cells(grade, ordinal)
    """)
    connection.execute("""
      CREATE OR REPLACE TEMP TABLE _compact_exact_grade_indices AS
      WITH candidates AS (
        SELECT layout.layout_id, request.request_ordinal, cell.cell_ordinal,
          (CASE WHEN cell.grade.q_teams <> 'ALL' THEN 1 ELSE 0 END
           + CASE WHEN cell.grade.q_roster <> 'ALL' THEN 1 ELSE 0 END
           + CASE WHEN cell.grade.q_scoring <> 'ALL' THEN 1 ELSE 0 END
           + CASE WHEN cell.grade.q_pass_td <> 'ALL' THEN 1 ELSE 0 END
           + CASE WHEN cell.grade.q_dynasty <> 'ALL' THEN 1 ELSE 0 END
           + CASE WHEN cell.grade.q_best_ball <> 'ALL' THEN 1 ELSE 0 END
           + CASE WHEN cell.grade.q_playoff_teams <> 'ALL' THEN 1 ELSE 0 END
          ) AS retained_dimension_count
        FROM _compact_exact_grade_layouts layout
        CROSS JOIN _compact_exact_grade_requests request
        INNER JOIN _compact_exact_grade_cells cell ON cell.layout_id = layout.layout_id
        WHERE cell.grade.q_teams IN ('ALL', request.q_teams)
          AND cell.grade.q_roster IN ('ALL', request.q_roster)
          AND cell.grade.q_scoring IN ('ALL', request.q_scoring)
          AND cell.grade.q_pass_td IN ('ALL', request.q_pass_td)
          AND cell.grade.q_dynasty IN ('ALL', request.q_dynasty)
          AND cell.grade.q_best_ball IN ('ALL', request.q_best_ball)
          AND cell.grade.q_playoff_teams IN ('ALL', request.q_playoff_teams)
      ), selected AS (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY layout_id, request_ordinal
          ORDER BY retained_dimension_count DESC, cell_ordinal
        ) AS selector_rank
        FROM candidates
      )
      SELECT layout_id,
        list(cell_ordinal::USMALLINT ORDER BY request_ordinal) AS grade_selector_indices
      FROM selected
      WHERE selector_rank = 1
      GROUP BY layout_id
    """)
    invalid = connection.execute("""
      SELECT COUNT(*)
      FROM _compact_exact_grade_indices
      WHERE list_count(grade_selector_indices) <> 864
         OR list_extract(grade_selector_indices, 1) IS NULL
         OR list_extract(grade_selector_indices, 864) IS NULL
    """).fetchone()[0]
    if invalid:
        raise RuntimeError(f"compact exact grade selector index incomplete for {invalid} cell layouts")
    source_projection = _replace_selector_projection(
        connection,
        output_table=output_table,
        selector_column="grade_selector_indices",
    )
    connection.execute(f"""
      CREATE OR REPLACE TEMP TABLE _compact_exact_with_grade_selector_indices AS
      SELECT {source_projection}, selector.grade_selector_indices
      FROM {output_table} source
      INNER JOIN _compact_exact_grade_layouts layout
        ON list_transform(source.grade_cells, grade ->
          grade.q_teams || '|' || grade.q_roster || '|' || grade.q_scoring || '|'
          || grade.q_pass_td || '|' || grade.q_dynasty || '|' || grade.q_best_ball
          || '|' || grade.q_playoff_teams
        ) = layout.selector_layout
      INNER JOIN _compact_exact_grade_indices selector
        ON selector.layout_id = layout.layout_id
    """)
    connection.execute(
        f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM _compact_exact_with_grade_selector_indices"
    )


def materialize_season_bracket_grade_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    nfl_stats_table: str,
    player_regular_weeks_table: str | None = None,
    target_players_table: str | None = None,
    output_table: str,
    year: int,
    player_id: str | None,
    position: str | None = None,
    split_threshold: int = 150,
) -> None:
    """Build season playoff/champ cells for every effective core+bracket key.

    The denominator is every position-eligible league in the resolved season
    cell.  Core dimensions and playoff bracket each split independently at the
    season floor; eliminated teams remain in every eligible denominator.
    """

    if split_threshold < 1:
        raise ValueError("split_threshold must be positive")
    source = _source_without_bad_identity_rows(connection, source_table)
    teams_column = _team_capacity_column(connection, source_table, stat="started")
    stats = _relation_sql(nfl_stats_table)
    target_players_relation = (
        None if target_players_table is None else _relation_sql(target_players_table)
    )
    output = _table_sql(output_table)
    if player_regular_weeks_table is None:
        regular_week_cte = f"""
        player_regular_weeks AS (
          SELECT DISTINCT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week
          FROM {stats}
          WHERE (? IS NULL OR CAST(NFL_player_id AS VARCHAR) = ?)
            AND CAST(year AS INTEGER) = ?
            AND week IS NOT NULL
            AND COALESCE(season_type, 'REG') = 'REG'
        ),
        """
    else:
        regular_weeks = _relation_sql(player_regular_weeks_table)
        regular_week_cte = f"""
        player_regular_weeks AS (
          SELECT DISTINCT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week
          FROM {regular_weeks}
          WHERE (? IS NULL OR CAST(NFL_player_id AS VARCHAR) = ?)
            AND CAST(year AS INTEGER) = ?
            AND CAST(week AS INTEGER) BETWEEN 1 AND 18
        ),
        """
    source_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {source}").fetchall()
    }
    rostered_select = "is_rostered" if "is_rostered" in source_columns else "1"
    target_player_join = (
        ""
        if target_players_relation is None
        else f"""
          INNER JOIN {target_players_relation} bucket
            ON CAST(bucket.NFL_player_id AS VARCHAR) = CAST(population.NFL_player_id AS VARCHAR)
        """
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH {regular_week_cte}
        population AS (
          SELECT
            db_name, year, week, NFL_player_id, {_canonical_position_sql('position')} AS position,
            cohort_position_eligible,
            {_capacity_tier_sql(_table_sql(teams_column))} AS cohort_teams,
            cohort_roster,
            cohort_scoring, cohort_pass_td, cohort_playoff_teams,
            cohort_dynasty, cohort_best_ball,
            {rostered_select} AS is_rostered, is_started, is_playoffs, champion
          FROM {source}
          WHERE CAST(year AS INTEGER) = ?
            AND (? IS NULL OR {_canonical_position_sql('position')} = ?)
        ),
        target_players AS (
          SELECT DISTINCT CAST(population.NFL_player_id AS VARCHAR) AS NFL_player_id
          FROM population
          INNER JOIN player_regular_weeks candidate_week
            ON candidate_week.NFL_player_id = CAST(population.NFL_player_id AS VARCHAR)
           AND candidate_week.year = CAST(population.year AS INTEGER)
           AND candidate_week.week = CAST(population.week AS INTEGER)
          {target_player_join}
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
            AND (? IS NULL OR CAST(population.NFL_player_id AS VARCHAR) = ?)
          GROUP BY 1
          HAVING MAX(CASE WHEN is_rostered IS NULL OR CAST(is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END) = 1
        ),
        valid_player_facts AS (
          SELECT p.*
          FROM population p
          INNER JOIN player_regular_weeks rw
            ON rw.NFL_player_id = CAST(p.NFL_player_id AS VARCHAR)
           AND rw.year = CAST(p.year AS INTEGER)
           AND rw.week = CAST(p.week AS INTEGER)
        ),
        eligible_inventory AS (
          SELECT DISTINCT
            CAST(db_name AS VARCHAR) AS db_name,
            CAST(position AS VARCHAR) AS position,
            CAST(cohort_teams AS VARCHAR) AS cohort_teams,
            CAST(cohort_roster AS VARCHAR) AS cohort_roster,
            CAST(cohort_scoring AS VARCHAR) AS cohort_scoring,
            CAST(cohort_pass_td AS VARCHAR) AS cohort_pass_td,
            CAST(cohort_playoff_teams AS VARCHAR) AS cohort_playoff_teams
            , CAST(cohort_dynasty AS VARCHAR) AS cohort_dynasty
            , CAST(cohort_best_ball AS VARCHAR) AS cohort_best_ball
          FROM population
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
        ),
        dimension_counts AS (
          SELECT
            position,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '08tm') AS teams_08tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '10tm') AS teams_10tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '12tm') AS teams_12tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '14tm') AS teams_14tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'flx') AS roster_flx,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'sflx') AS roster_sflx,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'idp') AS roster_idp,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'std') AS scoring_std,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'half') AS scoring_half,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'ppr') AS scoring_ppr,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_pass_td = '4pt') AS pass_td_4pt,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_pass_td = '6pt') AS pass_td_6pt,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_dynasty = 'redraft') AS dynasty_redraft,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_dynasty = 'dynasty') AS dynasty_dynasty,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_best_ball = 'managed') AS best_ball_managed,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_best_ball = 'best_ball') AS best_ball_best_ball,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_playoff_teams = '4po') AS bracket_4po,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_playoff_teams = '6po') AS bracket_6po,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_playoff_teams = '8po') AS bracket_8po
          FROM eligible_inventory
          GROUP BY position
        ),
        resolved_inventory AS (
          SELECT
            i.*,
            CASE WHEN (i.cohort_teams = '08tm' AND c.teams_08tm >= {split_threshold})
                       OR (i.cohort_teams = '10tm' AND c.teams_10tm >= {split_threshold})
                       OR (i.cohort_teams = '12tm' AND c.teams_12tm >= {split_threshold})
                       OR (i.cohort_teams = '14tm' AND c.teams_14tm >= {split_threshold})
                 THEN i.cohort_teams ELSE 'ALL' END AS q_teams,
            CASE WHEN (i.cohort_roster = 'flx' AND c.roster_flx >= {split_threshold})
                       OR (i.cohort_roster = 'sflx' AND c.roster_sflx >= {split_threshold})
                       OR (i.cohort_roster = 'idp' AND c.roster_idp >= {split_threshold})
                 THEN i.cohort_roster ELSE 'ALL' END AS q_roster,
            CASE WHEN (i.cohort_scoring = 'std' AND c.scoring_std >= {split_threshold})
                       OR (i.cohort_scoring = 'half' AND c.scoring_half >= {split_threshold})
                       OR (i.cohort_scoring = 'ppr' AND c.scoring_ppr >= {split_threshold})
                 THEN i.cohort_scoring ELSE 'ALL' END AS q_scoring,
            CASE WHEN (i.cohort_pass_td = '4pt' AND c.pass_td_4pt >= {split_threshold})
                       OR (i.cohort_pass_td = '6pt' AND c.pass_td_6pt >= {split_threshold})
                 THEN i.cohort_pass_td ELSE 'ALL' END AS q_pass_td,
            CASE WHEN (i.cohort_dynasty = 'redraft' AND c.dynasty_redraft >= {split_threshold})
                       OR (i.cohort_dynasty = 'dynasty' AND c.dynasty_dynasty >= {split_threshold})
                 THEN i.cohort_dynasty ELSE 'ALL' END AS q_dynasty,
            CASE WHEN (i.cohort_best_ball = 'managed' AND c.best_ball_managed >= {split_threshold})
                       OR (i.cohort_best_ball = 'best_ball' AND c.best_ball_best_ball >= {split_threshold})
                 THEN i.cohort_best_ball ELSE 'ALL' END AS q_best_ball,
            CASE WHEN (i.cohort_playoff_teams = '4po' AND c.bracket_4po >= {split_threshold})
                       OR (i.cohort_playoff_teams = '6po' AND c.bracket_6po >= {split_threshold})
                       OR (i.cohort_playoff_teams = '8po' AND c.bracket_8po >= {split_threshold})
                 THEN i.cohort_playoff_teams ELSE 'ALL' END AS q_playoff_teams
          FROM eligible_inventory i
          INNER JOIN dimension_counts c USING (position)
        ),
        eligible_cube AS (
          SELECT position,
            CASE WHEN GROUPING(q_teams)=1 THEN 'ALL' ELSE q_teams END AS q_teams,
            CASE WHEN GROUPING(q_roster)=1 THEN 'ALL' ELSE q_roster END AS q_roster,
            CASE WHEN GROUPING(q_scoring)=1 THEN 'ALL' ELSE q_scoring END AS q_scoring,
            CASE WHEN GROUPING(q_pass_td)=1 THEN 'ALL' ELSE q_pass_td END AS q_pass_td,
            CASE WHEN GROUPING(q_dynasty)=1 THEN 'ALL' ELSE q_dynasty END AS q_dynasty,
            CASE WHEN GROUPING(q_best_ball)=1 THEN 'ALL' ELSE q_best_ball END AS q_best_ball,
            CASE WHEN GROUPING(q_playoff_teams)=1 THEN 'ALL' ELSE q_playoff_teams END AS q_playoff_teams,
            GROUPING(q_teams) AS g_teams, GROUPING(q_roster) AS g_roster,
            GROUPING(q_scoring) AS g_scoring, GROUPING(q_pass_td) AS g_pass_td,
            GROUPING(q_dynasty) AS g_dynasty, GROUPING(q_best_ball) AS g_best_ball,
            GROUPING(q_playoff_teams) AS g_playoff_teams,
            COUNT(DISTINCT db_name)::BIGINT AS eligible_leagues
          FROM resolved_inventory
          GROUP BY position, CUBE(q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball, q_playoff_teams)
        ),
        eligible_cell AS (
          SELECT position, q_teams, q_roster, q_scoring, q_pass_td,
            q_dynasty, q_best_ball, q_playoff_teams, MAX(eligible_leagues)::BIGINT AS eligible_leagues
          FROM eligible_cube
          WHERE (g_teams=1 OR q_teams<>'ALL') AND (g_roster=1 OR q_roster<>'ALL')
            AND (g_scoring=1 OR q_scoring<>'ALL') AND (g_pass_td=1 OR q_pass_td<>'ALL')
            AND (g_dynasty=1 OR q_dynasty<>'ALL') AND (g_best_ball=1 OR q_best_ball<>'ALL')
            AND (g_playoff_teams=1 OR q_playoff_teams<>'ALL')
          GROUP BY 1,2,3,4,5,6,7,8
        ),
        -- Collapse week facts once, resolve the physical league's effective
        -- selectors once, then cube those compact leaves.  Do not multiply a
        -- league by every exact-or-ALL selector combination.
        player_season_facts AS (
          SELECT
            CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(p.db_name AS VARCHAR) AS db_name,
            CAST(p.position AS VARCHAR) AS position,
            CAST(p.cohort_teams AS VARCHAR) AS cohort_teams,
            CAST(p.cohort_roster AS VARCHAR) AS cohort_roster,
            CAST(p.cohort_scoring AS VARCHAR) AS cohort_scoring,
            CAST(p.cohort_pass_td AS VARCHAR) AS cohort_pass_td,
            CAST(p.cohort_playoff_teams AS VARCHAR) AS cohort_playoff_teams,
            CAST(p.cohort_dynasty AS VARCHAR) AS cohort_dynasty,
            CAST(p.cohort_best_ball AS VARCHAR) AS cohort_best_ball,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1
                           AND (CAST(p.is_playoffs AS INTEGER) = 1 OR CAST(p.champion AS INTEGER) = 1) THEN 1 ELSE 0 END)::BIGINT
              AS playoff_credit,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1
                           AND CAST(p.champion AS INTEGER) = 1 THEN 1 ELSE 0 END)::BIGINT
              AS champ_credit
          FROM valid_player_facts p
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ),
        player_resolved_leaf AS (
          SELECT
            CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
            r.position, r.q_teams, r.q_roster, r.q_scoring, r.q_pass_td,
            r.q_dynasty, r.q_best_ball, r.q_playoff_teams,
            p.playoff_credit,
            p.champ_credit
          FROM player_season_facts p
          INNER JOIN resolved_inventory r
            ON r.db_name = CAST(p.db_name AS VARCHAR)
           AND r.position = CAST(p.position AS VARCHAR)
           AND r.cohort_teams IS NOT DISTINCT FROM CAST(p.cohort_teams AS VARCHAR)
           AND r.cohort_roster IS NOT DISTINCT FROM CAST(p.cohort_roster AS VARCHAR)
           AND r.cohort_scoring IS NOT DISTINCT FROM CAST(p.cohort_scoring AS VARCHAR)
           AND r.cohort_pass_td IS NOT DISTINCT FROM CAST(p.cohort_pass_td AS VARCHAR)
           AND r.cohort_playoff_teams IS NOT DISTINCT FROM CAST(p.cohort_playoff_teams AS VARCHAR)
           AND r.cohort_dynasty IS NOT DISTINCT FROM CAST(p.cohort_dynasty AS VARCHAR)
           AND r.cohort_best_ball IS NOT DISTINCT FROM CAST(p.cohort_best_ball AS VARCHAR)
        ),
        player_resolved_profile AS (
          SELECT NFL_player_id, position, q_teams, q_roster, q_scoring, q_pass_td,
            q_dynasty, q_best_ball, q_playoff_teams,
            SUM(playoff_credit)::BIGINT AS playoff_credit,
            SUM(champ_credit)::BIGINT AS champ_credit
          FROM player_resolved_leaf
          GROUP BY 1,2,3,4,5,6,7,8,9
        ),
        metric_cube AS (
          SELECT
            NFL_player_id, position,
            CASE WHEN GROUPING(q_teams)=1 THEN 'ALL' ELSE q_teams END AS q_teams,
            CASE WHEN GROUPING(q_roster)=1 THEN 'ALL' ELSE q_roster END AS q_roster,
            CASE WHEN GROUPING(q_scoring)=1 THEN 'ALL' ELSE q_scoring END AS q_scoring,
            CASE WHEN GROUPING(q_pass_td)=1 THEN 'ALL' ELSE q_pass_td END AS q_pass_td,
            CASE WHEN GROUPING(q_dynasty)=1 THEN 'ALL' ELSE q_dynasty END AS q_dynasty,
            CASE WHEN GROUPING(q_best_ball)=1 THEN 'ALL' ELSE q_best_ball END AS q_best_ball,
            CASE WHEN GROUPING(q_playoff_teams)=1 THEN 'ALL' ELSE q_playoff_teams END AS q_playoff_teams,
            GROUPING(q_teams) AS g_teams, GROUPING(q_roster) AS g_roster,
            GROUPING(q_scoring) AS g_scoring, GROUPING(q_pass_td) AS g_pass_td,
            GROUPING(q_dynasty) AS g_dynasty, GROUPING(q_best_ball) AS g_best_ball,
            GROUPING(q_playoff_teams) AS g_playoff_teams,
            SUM(playoff_credit)::BIGINT AS playoff_leagues,
            SUM(champ_credit)::BIGINT AS champ_leagues
          FROM player_resolved_profile
          GROUP BY NFL_player_id, position,
            CUBE(q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball, q_playoff_teams)
        ),
        cell_metric AS (
          SELECT NFL_player_id, position, q_teams, q_roster, q_scoring, q_pass_td,
            q_dynasty, q_best_ball, q_playoff_teams,
            MAX(playoff_leagues)::BIGINT AS playoff_leagues,
            MAX(champ_leagues)::BIGINT AS champ_leagues
          FROM metric_cube
          WHERE (g_teams=1 OR q_teams<>'ALL') AND (g_roster=1 OR q_roster<>'ALL')
            AND (g_scoring=1 OR q_scoring<>'ALL') AND (g_pass_td=1 OR q_pass_td<>'ALL')
            AND (g_dynasty=1 OR q_dynasty<>'ALL') AND (g_best_ball=1 OR q_best_ball<>'ALL')
            AND (g_playoff_teams=1 OR q_playoff_teams<>'ALL')
          GROUP BY 1,2,3,4,5,6,7,8,9
        )
        SELECT
          t.NFL_player_id,
          ?::INTEGER AS year,
          e.position,
          list(struct_pack(
            q_teams := e.q_teams,
            q_roster := e.q_roster,
            q_scoring := e.q_scoring,
            q_pass_td := e.q_pass_td,
            q_dynasty := e.q_dynasty,
            q_best_ball := e.q_best_ball,
            q_playoff_teams := e.q_playoff_teams,
            eligible_leagues := e.eligible_leagues,
            playoff_leagues := COALESCE(m.playoff_leagues, 0),
            champ_leagues := COALESCE(m.champ_leagues, 0),
            playoff_rate_pct := 100.0 * COALESCE(m.playoff_leagues, 0) / NULLIF(e.eligible_leagues, 0),
            champ_rate_pct := 100.0 * COALESCE(m.champ_leagues, 0) / NULLIF(e.eligible_leagues, 0)
          ) ORDER BY e.q_teams, e.q_roster, e.q_scoring, e.q_pass_td, e.q_dynasty, e.q_best_ball, e.q_playoff_teams) AS grade_cells
        FROM target_players t
        CROSS JOIN eligible_cell e
        LEFT JOIN cell_metric m
          ON m.NFL_player_id = t.NFL_player_id
         AND m.position = e.position
         AND m.q_teams = e.q_teams AND m.q_roster = e.q_roster
         AND m.q_scoring = e.q_scoring AND m.q_pass_td = e.q_pass_td
         AND m.q_dynasty = e.q_dynasty AND m.q_best_ball = e.q_best_ball
         AND m.q_playoff_teams = e.q_playoff_teams
        GROUP BY t.NFL_player_id, e.position
        """,
        [player_id, player_id, year, year, position, position, player_id, player_id, year],
    )
    _compact_season_grade_cells(
        connection,
        output_table=output_table,
        split_threshold=split_threshold,
    )
    invalid = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {output}, UNNEST(grade_cells) AS u(cell)
        WHERE cell.champ_leagues > cell.playoff_leagues
        """
    ).fetchone()[0]
    if invalid:
        raise RuntimeError("Champ credit cannot exceed playoff credit")


def _materialize_season_grade_requested_layouts(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    output_table: str,
    split_threshold: int,
) -> None:
    """Resolve compact grade-cell ordinals once for each cell layout.

    Every player in a position/year normally has the same grade cell layout,
    even though their grade *values* differ.  The 864 requested UI formats
    therefore need only one ordinal map per distinct layout.  Returning
    source-cell ordinals (rather than grade structs) lets the caller select
    each player's own values without a player x 864 x lattice cross product.
    """

    source = _relation_sql(source_table)
    output = _table_sql(output_table)
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output} AS
        WITH source_layouts AS (
          SELECT
            list_transform(grade_cells, grade ->
              grade.q_teams || '|' || grade.q_roster || '|' || grade.q_scoring || '|'
              || grade.q_pass_td || '|' || grade.q_dynasty || '|' || grade.q_best_ball
              || '|' || grade.q_playoff_teams
            ) AS selector_layout,
            grade_cells
          FROM {source}
        ),
        layouts AS (
          SELECT ROW_NUMBER() OVER ()::INTEGER AS layout_id,
            selector_layout,
            ANY_VALUE(grade_cells) AS grade_cells
          FROM source_layouts
          GROUP BY selector_layout
        ),
        layout_cells AS (
          SELECT layout.layout_id, ordinal::USMALLINT AS cell_ordinal, grade
          FROM layouts AS layout
          CROSS JOIN UNNEST(layout.grade_cells) WITH ORDINALITY AS cells(grade, ordinal)
        ),
        requested AS (
          SELECT *
          FROM (VALUES ('08tm'), ('10tm'), ('12tm'), ('14tm')) teams(teams)
          CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) roster(roster)
          CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) scoring(scoring)
          CROSS JOIN (VALUES ('4pt'), ('6pt')) pass_td(pass_td)
          CROSS JOIN (VALUES ('redraft'), ('dynasty')) dynasty(dynasty)
          CROSS JOIN (VALUES ('managed'), ('best_ball')) best_ball(best_ball)
          CROSS JOIN (VALUES ('4po'), ('6po'), ('8po')) playoff_teams(playoff_teams)
        ),
        candidates AS (
          SELECT
            layout.layout_id,
            q.teams, q.roster, q.scoring, q.pass_td,
            q.dynasty, q.best_ball, q.playoff_teams,
            cell.cell_ordinal,
            cell.grade.eligible_leagues AS eligible_leagues,
            cell.grade.q_teams AS q_teams,
            cell.grade.q_roster AS q_roster,
            cell.grade.q_scoring AS q_scoring,
            cell.grade.q_pass_td AS q_pass_td,
            cell.grade.q_dynasty AS q_dynasty,
            cell.grade.q_best_ball AS q_best_ball,
            cell.grade.q_playoff_teams AS q_playoff_teams,
            (
              CASE WHEN cell.grade.q_teams <> 'ALL' THEN 1 ELSE 0 END
              + CASE WHEN cell.grade.q_roster <> 'ALL' THEN 1 ELSE 0 END
              + CASE WHEN cell.grade.q_scoring <> 'ALL' THEN 1 ELSE 0 END
              + CASE WHEN cell.grade.q_pass_td <> 'ALL' THEN 1 ELSE 0 END
              + CASE WHEN cell.grade.q_dynasty <> 'ALL' THEN 1 ELSE 0 END
              + CASE WHEN cell.grade.q_best_ball <> 'ALL' THEN 1 ELSE 0 END
              + CASE WHEN cell.grade.q_playoff_teams <> 'ALL' THEN 1 ELSE 0 END
            ) AS retained_dimension_count
          FROM layouts AS layout
          CROSS JOIN requested q
          INNER JOIN layout_cells AS cell ON cell.layout_id = layout.layout_id
          WHERE cell.grade.eligible_leagues >= {split_threshold}
            AND cell.grade.q_teams IN ('ALL', q.teams)
            AND cell.grade.q_roster IN ('ALL', q.roster)
            AND cell.grade.q_scoring IN ('ALL', q.scoring)
            AND cell.grade.q_pass_td IN ('ALL', q.pass_td)
            AND cell.grade.q_dynasty IN ('ALL', q.dynasty)
            AND cell.grade.q_best_ball IN ('ALL', q.best_ball)
            AND cell.grade.q_playoff_teams IN ('ALL', q.playoff_teams)
        ),
        selected AS (
          SELECT layout_id, cell_ordinal
          FROM candidates
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY layout_id,
              teams, roster, scoring, pass_td, dynasty, best_ball, playoff_teams
            ORDER BY retained_dimension_count DESC,
              eligible_leagues ASC,
              q_teams, q_roster, q_scoring, q_pass_td,
              q_dynasty, q_best_ball, q_playoff_teams
          ) = 1
          UNION ALL
          SELECT cell.layout_id, cell.cell_ordinal
          FROM layout_cells AS cell
          WHERE cell.grade.q_teams='ALL' AND cell.grade.q_roster='ALL' AND cell.grade.q_scoring='ALL'
            AND cell.grade.q_pass_td='ALL' AND cell.grade.q_dynasty='ALL' AND cell.grade.q_best_ball='ALL'
            AND cell.grade.q_playoff_teams='ALL'
        ),
        dedup AS (
          SELECT DISTINCT layout_id, cell_ordinal
          FROM selected
        )
        SELECT d.layout_id, layout.selector_layout,
          list(d.cell_ordinal ORDER BY
            cell.grade.q_teams, cell.grade.q_roster, cell.grade.q_scoring,
            cell.grade.q_pass_td, cell.grade.q_dynasty, cell.grade.q_best_ball,
            cell.grade.q_playoff_teams
          ) AS grade_cell_indices
        FROM dedup AS d
        INNER JOIN layout_cells AS cell
          ON cell.layout_id = d.layout_id AND cell.cell_ordinal = d.cell_ordinal
        INNER JOIN layouts AS layout ON layout.layout_id = d.layout_id
        GROUP BY d.layout_id, layout.selector_layout
        """
    )


def _compact_season_grade_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
    split_threshold: int,
) -> None:
    """Keep the narrowest >= floor grade parent for every UI request."""

    output = _table_sql(output_table)
    layouts_name = f"{output_table}_grade_layouts"
    layouts = _table_sql(layouts_name)
    requested_cells = _table_sql(f"{output_table}_requested_cells")
    _materialize_season_grade_requested_layouts(
        connection,
        source_table=output_table,
        output_table=layouts_name,
        split_threshold=split_threshold,
    )
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {requested_cells} AS
        SELECT source.* REPLACE (
          list_select(source.grade_cells, layouts.grade_cell_indices) AS grade_cells
        )
        FROM {output} AS source
        INNER JOIN {layouts} AS layouts
          ON list_transform(source.grade_cells, grade ->
            grade.q_teams || '|' || grade.q_roster || '|' || grade.q_scoring || '|'
            || grade.q_pass_td || '|' || grade.q_dynasty || '|' || grade.q_best_ball
            || '|' || grade.q_playoff_teams
          ) = layouts.selector_layout
        """
    )
    connection.execute(f"CREATE OR REPLACE TABLE {output} AS SELECT * FROM {requested_cells}")
    oversized = connection.execute(
        f"SELECT COUNT(*) FROM {output} WHERE list_count(grade_cells) > 865"
    ).fetchone()[0]
    if oversized:
        raise RuntimeError("Season compact grade output exceeds reachable core/bracket requests plus ALL audit")


def materialize_season_bracket_grade_position_batch(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    nfl_stats_table: str,
    player_regular_weeks_table: str | None = None,
    target_players_table: str | None = None,
    output_table: str,
    year: int,
    position: str,
    split_threshold: int = 150,
) -> None:
    """Build all season grades through the same full cohort-aware SQL path."""
    materialize_season_bracket_grade_cells(
        connection,
        source_table=source_table,
        nfl_stats_table=nfl_stats_table,
        player_regular_weeks_table=player_regular_weeks_table,
        target_players_table=target_players_table,
        output_table=output_table,
        year=year,
        player_id=None,
        position=position,
        split_threshold=split_threshold,
    )
    return

    """Legacy implementation retained below temporarily; unreachable."""
    if split_threshold < 1:
        raise ValueError("split_threshold must be positive")
    source, stats, output = _relation_sql(source_table), _relation_sql(nfl_stats_table), _table_sql(output_table)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH targets AS (SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) id FROM {source} WHERE CAST(year AS INTEGER)=? AND CAST(position AS VARCHAR)=?),
        regular_weeks AS (SELECT DISTINCT CAST(s.NFL_player_id AS VARCHAR) AS id,CAST(s.week AS INTEGER) AS week FROM {stats} s INNER JOIN targets t ON t.id=CAST(s.NFL_player_id AS VARCHAR) WHERE CAST(s.year AS INTEGER)=? AND s.week IS NOT NULL AND COALESCE(s.season_type,'REG')='REG'),
        population AS (SELECT db_name,year,week,NFL_player_id,position,cohort_position_eligible,cohort_playoff_teams,is_started,is_playoffs,champion FROM {source} WHERE CAST(year AS INTEGER)=? AND CAST(position AS VARCHAR)=?),
        inventory AS (SELECT DISTINCT CAST(db_name AS VARCHAR) db,CAST(position AS VARCHAR) pos,CAST(cohort_playoff_teams AS VARCHAR) bracket FROM population WHERE CAST(cohort_position_eligible AS INTEGER)=1),
        counts AS (SELECT pos,COUNT(DISTINCT db) FILTER(WHERE bracket='4po') c4,COUNT(DISTINCT db) FILTER(WHERE bracket='6po') c6,COUNT(DISTINCT db) FILTER(WHERE bracket='8po') c8 FROM inventory GROUP BY 1),
        resolved AS (
          SELECT i.*, 'ALL' q FROM inventory i
          UNION ALL
          SELECT i.*,i.bracket q FROM inventory i INNER JOIN counts c USING(pos)
          WHERE (i.bracket='4po' AND c4>={split_threshold}) OR (i.bracket='6po' AND c6>={split_threshold}) OR (i.bracket='8po' AND c8>={split_threshold})
        ),
        eligible AS (SELECT pos,q,COUNT(DISTINCT db)::BIGINT e FROM resolved GROUP BY 1,2),
        player_league AS (
          SELECT CAST(p.NFL_player_id AS VARCHAR) id,CAST(p.db_name AS VARCHAR) db,r.pos,r.q,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND (CAST(p.is_playoffs AS INTEGER)=1 OR CAST(p.champion AS INTEGER)=1) THEN 1 ELSE 0 END)::BIGINT playoff,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND CAST(p.champion AS INTEGER)=1 THEN 1 ELSE 0 END)::BIGINT champ
          FROM population p INNER JOIN regular_weeks w ON w.id=CAST(p.NFL_player_id AS VARCHAR) AND w.week=CAST(p.week AS INTEGER)
          INNER JOIN resolved r ON r.db=CAST(p.db_name AS VARCHAR) AND r.pos=CAST(p.position AS VARCHAR) AND r.bracket IS NOT DISTINCT FROM CAST(p.cohort_playoff_teams AS VARCHAR)
          GROUP BY 1,2,3,4
        ),
        metrics AS (SELECT p.id,p.pos,p.q,MAX(e.e)::BIGINT e,SUM(p.playoff)::BIGINT playoff,SUM(p.champ)::BIGINT champ FROM player_league p INNER JOIN eligible e ON e.pos=p.pos AND e.q=p.q GROUP BY 1,2,3),
        cells AS (SELECT t.id,e.pos,e.q,e.e,COALESCE(m.playoff,0)::BIGINT playoff,COALESCE(m.champ,0)::BIGINT champ FROM (SELECT DISTINCT id FROM regular_weeks) t CROSS JOIN eligible e LEFT JOIN metrics m ON m.id=t.id AND m.pos=e.pos AND m.q=e.q)
        SELECT id AS NFL_player_id,?::INTEGER AS year,MAX(pos) AS position,list(struct_pack(q_playoff_teams:=q,eligible_leagues:=e,playoff_leagues:=playoff,champ_leagues:=champ,playoff_rate_pct:=100.0*playoff/NULLIF(e,0),champ_rate_pct:=100.0*champ/NULLIF(e,0)) ORDER BY q) AS grade_cells FROM cells GROUP BY id
        """,
        [year, position, year, year, position, year],
    )
    bad = connection.execute(f"SELECT COUNT(*) FROM {output}, UNNEST(grade_cells) u(cell) WHERE cell.champ_leagues>cell.playoff_leagues").fetchone()[0]
    if bad:
        raise RuntimeError("Champ credit cannot exceed playoff credit")


def materialize_season_compact_outer_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    core_outer_table: str,
    grade_outer_table: str,
    output_table: str,
) -> None:
    """Join already-aggregated season core and grade cells without re-rolling facts."""

    core = _relation_sql(core_outer_table)
    grade = _relation_sql(grade_outer_table)
    output = _table_sql(output_table)
    core_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {core}").fetchall()
    }
    roster_cells = "c.roster_cells" if "roster_cells" in core_columns else "c.cohort_cells"
    roster_indices = (
        "c.roster_selector_indices"
        if "roster_selector_indices" in core_columns
        else "NULL::USMALLINT[]"
    )
    mismatch = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {core} c
        FULL OUTER JOIN {grade} g
          ON CAST(g.NFL_player_id AS VARCHAR) = CAST(c.NFL_player_id AS VARCHAR)
         AND CAST(g.year AS INTEGER) = CAST(c.year AS INTEGER)
         AND CAST(g.position AS VARCHAR) = CAST(c.position AS VARCHAR)
        WHERE c.NFL_player_id IS NULL OR g.NFL_player_id IS NULL
        """
    ).fetchone()[0]
    if mismatch:
        raise RuntimeError(f"season core/grade outer identities differ: {mismatch}")
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        SELECT
          CAST(c.NFL_player_id AS VARCHAR) AS NFL_player_id,
          CAST(c.year AS INTEGER) AS year,
          CAST(c.position AS VARCHAR) AS position,
          c.cohort_cells,
          c.cohort_profile,
          {roster_cells} AS roster_cells,
          {roster_indices} AS roster_selector_indices,
          g.grade_cells
        FROM {core} c
        INNER JOIN {grade} g
          ON CAST(g.NFL_player_id AS VARCHAR) = CAST(c.NFL_player_id AS VARCHAR)
         AND CAST(g.year AS INTEGER) = CAST(c.year AS INTEGER)
         AND CAST(g.position AS VARCHAR) = CAST(c.position AS VARCHAR)
        """
    )


def materialize_compact_grade_sidecar(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    output_table: str,
    grain: str,
) -> None:
    """Store resolved grade selectors in typed lanes without expanding player rows."""
    if grain not in {"season", "career"}:
        raise ValueError(f"unsupported grade sidecar grain: {grain}")
    source = _relation_sql(source_table)
    output = _table_sql(output_table)
    time = "r.year," if grain == "season" else ""
    career = grain == "career"
    connection.execute(f"""
      CREATE OR REPLACE TABLE {output} AS
      SELECT
        r.NFL_player_id,
        {time}
        r.position,
        list_transform(range(1, 865), ordinal ->
          list_extract(r.grade_cells, list_extract(r.grade_selector_indices, ordinal)).playoff_rate_pct
        ) AS playoff_rate_values,
        list_transform(range(1, 865), ordinal ->
          list_extract(r.grade_cells, list_extract(r.grade_selector_indices, ordinal)).champ_rate_pct
        ) AS champ_rate_values,
        list_transform(range(1, 865), ordinal ->
          list_extract(r.grade_cells, list_extract(r.grade_selector_indices, ordinal)).eligible_{"league_seasons" if career else "leagues"}
        ) AS eligible_values
        {", list_transform(range(1, 865), ordinal -> list_extract(r.grade_cells, list_extract(r.grade_selector_indices, ordinal)).expected_playoffs) AS expected_playoff_values, list_transform(range(1, 865), ordinal -> list_extract(r.grade_cells, list_extract(r.grade_selector_indices, ordinal)).expected_champs) AS expected_champ_values" if career else ""}
      FROM {source} r
      ORDER BY position, NFL_player_id
    """)
    source_rows = connection.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
    sidecar_rows = connection.execute(f"SELECT COUNT(*) FROM {output}").fetchone()[0]
    if source_rows != sidecar_rows:
        raise RuntimeError(f"grade sidecar row count mismatch: {sidecar_rows} != {source_rows}")


def materialize_career_compact_outer_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    season_compact_table: str,
    output_table: str,
    core_outer_table: str | None = None,
    grade_outer_table: str | None = None,
) -> None:
    """Roll compact season cells to one career outer row, then join its grades.

    Start-driven cells and roster-driven cells use different observed-capacity
    team maps.  The compact season bundle retains both maps, so career must
    aggregate them independently as well; otherwise a roster-rate selector
    would accidentally use the started-capacity market after the first year.
    """

    if (core_outer_table is None) != (grade_outer_table is None):
        raise ValueError("career core and grade tables must be provided together")
    if core_outer_table is None:
        core_outer_table = "_career_core"
        grade_outer_table = "_career_grade"
        materialize_career_core_cohort_cells(
            connection,
            season_outer_table=season_compact_table,
            output_table=core_outer_table,
            player_id=None,
        )
        materialize_career_cohort_grade_cells(
            connection,
            season_outer_table=season_compact_table,
            output_table=grade_outer_table,
            player_id=None,
        )
    core = _relation_sql(core_outer_table)
    grade = _relation_sql(grade_outer_table)
    output = _table_sql(output_table)
    season = _relation_sql(season_compact_table)
    season_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {season}").fetchall()
    }
    roster_core: str | None = None
    if {"roster_cells", "roster_selector_indices", "core_selector_indices"} <= season_columns:
        # This is a tiny view over the compact season output, not a new cache
        # or a second raw-data aggregation.  The career aggregator only needs
        # cells plus their request-index map.
        connection.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _career_roster_season_source AS
            SELECT
              NFL_player_id,
              year,
              position,
              roster_cells AS cohort_cells,
              roster_selector_indices AS core_selector_indices
            FROM {season}
            """
        )
        roster_core = "_career_roster_core"
        materialize_career_core_cohort_cells(
            connection,
            season_outer_table="_career_roster_season_source",
            output_table=roster_core,
            player_id=None,
        )
        materialize_compact_core_selector_indices(
            connection,
            output_table=roster_core,
        )
    core_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {core}").fetchall()
    }
    fallback_roster_cells = "c.cohort_cells"
    fallback_roster_indices = (
        "c.core_selector_indices"
        if "core_selector_indices" in core_columns
        else "NULL::USMALLINT[]"
    )
    mismatch = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {core} c
        FULL OUTER JOIN {grade} g
          ON CAST(g.NFL_player_id AS VARCHAR) = CAST(c.NFL_player_id AS VARCHAR)
         AND CAST(g.position AS VARCHAR) = CAST(c.position AS VARCHAR)
        WHERE c.NFL_player_id IS NULL OR g.NFL_player_id IS NULL
        """
    ).fetchone()[0]
    if mismatch:
        raise RuntimeError(f"career core/grade outer identities differ: {mismatch}")
    roster_join = ""
    roster_projection = (
        f"{fallback_roster_cells} AS roster_cells, {fallback_roster_indices} AS roster_selector_indices"
    )
    if roster_core is not None:
        roster_relation = _relation_sql(roster_core)
        roster_join = f"""
        INNER JOIN {roster_relation} r
          ON CAST(r.NFL_player_id AS VARCHAR) = CAST(c.NFL_player_id AS VARCHAR)
         AND CAST(r.position AS VARCHAR) = CAST(c.position AS VARCHAR)
        """
        roster_projection = (
            "r.cohort_cells AS roster_cells, "
            "r.core_selector_indices AS roster_selector_indices"
        )
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        SELECT
          CAST(c.NFL_player_id AS VARCHAR) AS NFL_player_id,
          CAST(c.position AS VARCHAR) AS position,
          c.cohort_cells,
          {roster_projection},
          g.grade_cells
        FROM {core} c
        INNER JOIN {grade} g
          ON CAST(g.NFL_player_id AS VARCHAR) = CAST(c.NFL_player_id AS VARCHAR)
         AND CAST(g.position AS VARCHAR) = CAST(c.position AS VARCHAR)
        {roster_join}
        """
    )
    if roster_core is not None:
        connection.execute("DROP TABLE _career_roster_core")
        connection.execute("DROP TABLE _career_roster_season_source")


def materialize_season_core_cohort_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    nfl_stats_table: str,
    output_table: str,
    year: int,
    player_id: str,
    position: str | None = None,
    split_threshold: int = 150,
    team_stat: str = "started",
    include_roster_map: bool = True,
) -> None:
    """Build season core cells with six independent exact-or-pooled selectors.

    This is intentionally separate from bracket grades.  It derives all core
    league behavior from the cache and uses cached NFL regular weeks only to
    decide which player/team weeks are start opportunities and active weeks.
    Every eligible league emits a broad ``ALL`` selector and, when the season
    population clears ``split_threshold``, its exact selector for each core
    dimension.
    """

    if split_threshold < 1:
        raise ValueError("split_threshold must be positive")
    source = _source_without_bad_identity_rows(connection, source_table)
    teams_column = _team_capacity_column(connection, source_table, stat=team_stat)
    stats = _relation_sql(nfl_stats_table)
    output = _table_sql(output_table)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH player_regular_stats AS (
          SELECT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week,
            NULLIF(TRIM(CAST(nfl_team AS VARCHAR)), '') AS nfl_team,
            MAX(CASE WHEN
              COALESCE(CAST(offense_snaps AS BIGINT), 0)
              + COALESCE(CAST(defense_snaps AS BIGINT), 0)
              + COALESCE(CAST(special_teams_snaps AS BIGINT), 0) > 0
              THEN 1 ELSE 0 END
            )::BIGINT AS active_week
          FROM {stats}
          WHERE CAST(year AS INTEGER) = ?
            AND week IS NOT NULL
            AND COALESCE(season_type, 'REG') = 'REG'
            AND CAST(NFL_player_id AS VARCHAR) = ?
          GROUP BY 1, 2, 3, 4
        ),
        player_team_year AS (
          SELECT NFL_player_id, year, MODE(nfl_team) AS nfl_team
          FROM player_regular_stats
          WHERE nfl_team IS NOT NULL
          GROUP BY 1, 2
        ),
        team_regular_game_weeks AS (
          SELECT DISTINCT
            CAST(s.year AS INTEGER) AS year,
            CAST(s.week AS INTEGER) AS week,
            NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') AS nfl_team
          FROM {stats} s
          INNER JOIN player_team_year pty
            ON pty.year = CAST(s.year AS INTEGER)
           AND pty.nfl_team = NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '')
          WHERE CAST(s.year AS INTEGER) = ?
            AND s.week IS NOT NULL
            AND COALESCE(s.season_type, 'REG') = 'REG'
        ),
        player_game_weeks AS (
          SELECT
            pty.NFL_player_id,
            pty.year,
            twg.week,
            COALESCE(MAX(rs.active_week), 0)::BIGINT AS active_week
          FROM player_team_year pty
          INNER JOIN team_regular_game_weeks twg
            ON twg.year = pty.year AND twg.nfl_team = pty.nfl_team
          LEFT JOIN player_regular_stats rs
            ON rs.NFL_player_id = pty.NFL_player_id
           AND rs.year = pty.year
           AND rs.week = twg.week
          GROUP BY 1, 2, 3
        ),
        population AS (
          SELECT
            db_name, year, week, NFL_player_id, {_canonical_position_sql('position')} AS position,
            cohort_position_eligible, {_capacity_tier_sql(_table_sql(teams_column))} AS cohort_teams, cohort_roster,
            cohort_scoring, cohort_pass_td, cohort_playoff_teams,
            cohort_dynasty, cohort_best_ball,
            is_rostered, is_started, win, clutch_equity
          FROM {source}
          WHERE CAST(year AS INTEGER) = ?
            AND (? IS NULL OR {_canonical_position_sql('position')} = ?)
        ),
        target_player_facts AS (
          SELECT *
          FROM population
          WHERE CAST(NFL_player_id AS VARCHAR) = ?
        ),
        target_positions AS (
          SELECT DISTINCT CAST(position AS VARCHAR) AS position
          FROM target_player_facts
          WHERE position IS NOT NULL
        ),
        eligible_inventory AS (
          SELECT DISTINCT
            CAST(p.db_name AS VARCHAR) AS db_name,
            CAST(p.week AS INTEGER) AS week,
            CAST(p.position AS VARCHAR) AS position,
            CAST(p.cohort_teams AS VARCHAR) AS cohort_teams,
            CAST(p.cohort_roster AS VARCHAR) AS cohort_roster,
            CAST(p.cohort_scoring AS VARCHAR) AS cohort_scoring,
            CAST(p.cohort_pass_td AS VARCHAR) AS cohort_pass_td,
            CAST(p.cohort_playoff_teams AS VARCHAR) AS cohort_playoff_teams,
            CAST(p.cohort_dynasty AS VARCHAR) AS cohort_dynasty,
            CAST(p.cohort_best_ball AS VARCHAR) AS cohort_best_ball
          FROM population p
          INNER JOIN target_positions tp
            ON tp.position = CAST(p.position AS VARCHAR)
          INNER JOIN player_game_weeks g
            ON g.year = CAST(p.year AS INTEGER)
           AND g.week = CAST(p.week AS INTEGER)
          WHERE CAST(p.cohort_position_eligible AS INTEGER) = 1
        ),
        dimension_counts AS (
          SELECT
            position,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '08tm') AS teams_08tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '10tm') AS teams_10tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '12tm') AS teams_12tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_teams = '14tm') AS teams_14tm,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'flx') AS roster_flx,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'sflx') AS roster_sflx,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_roster = 'idp') AS roster_idp,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'std') AS scoring_std,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'half') AS scoring_half,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_scoring = 'ppr') AS scoring_ppr,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_pass_td = '4pt') AS pass_td_4pt,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_pass_td = '6pt') AS pass_td_6pt,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_dynasty = 'redraft') AS dynasty_redraft,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_dynasty = 'dynasty') AS dynasty_dynasty,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_best_ball = 'managed') AS best_ball_managed,
            COUNT(DISTINCT db_name) FILTER (WHERE cohort_best_ball = 'best_ball') AS best_ball_best_ball
          FROM eligible_inventory
          GROUP BY position
        ),
        resolved_inventory AS (
          SELECT
            i.*,
            CASE WHEN
              (i.cohort_teams = '08tm' AND c.teams_08tm >= {split_threshold})
              OR (i.cohort_teams = '10tm' AND c.teams_10tm >= {split_threshold})
              OR (i.cohort_teams = '12tm' AND c.teams_12tm >= {split_threshold})
              OR (i.cohort_teams = '14tm' AND c.teams_14tm >= {split_threshold})
              THEN i.cohort_teams ELSE 'ALL' END AS q_teams,
            CASE WHEN
              (i.cohort_roster = 'flx' AND c.roster_flx >= {split_threshold})
              OR (i.cohort_roster = 'sflx' AND c.roster_sflx >= {split_threshold})
              OR (i.cohort_roster = 'idp' AND c.roster_idp >= {split_threshold})
              THEN i.cohort_roster ELSE 'ALL' END AS q_roster,
            CASE WHEN
              (i.cohort_scoring = 'std' AND c.scoring_std >= {split_threshold})
              OR (i.cohort_scoring = 'half' AND c.scoring_half >= {split_threshold})
              OR (i.cohort_scoring = 'ppr' AND c.scoring_ppr >= {split_threshold})
              THEN i.cohort_scoring ELSE 'ALL' END AS q_scoring,
            CASE WHEN
              (i.cohort_pass_td = '4pt' AND c.pass_td_4pt >= {split_threshold})
              OR (i.cohort_pass_td = '6pt' AND c.pass_td_6pt >= {split_threshold})
              THEN i.cohort_pass_td ELSE 'ALL' END AS q_pass_td,
            CASE WHEN
              (i.cohort_dynasty = 'redraft' AND c.dynasty_redraft >= {split_threshold})
              OR (i.cohort_dynasty = 'dynasty' AND c.dynasty_dynasty >= {split_threshold})
              THEN i.cohort_dynasty ELSE 'ALL' END AS q_dynasty,
            CASE WHEN
              (i.cohort_best_ball = 'managed' AND c.best_ball_managed >= {split_threshold})
              OR (i.cohort_best_ball = 'best_ball' AND c.best_ball_best_ball >= {split_threshold})
              THEN i.cohort_best_ball ELSE 'ALL' END AS q_best_ball
          FROM eligible_inventory i
          INNER JOIN dimension_counts c USING (position)
        ),
        expanded_inventory AS (
          SELECT
            r.*,
            teams.selector_teams,
            roster.selector_roster,
            scoring.selector_scoring,
            pass_td.selector_pass_td,
            dynasty.selector_dynasty,
            best_ball.selector_best_ball
          FROM resolved_inventory r
          CROSS JOIN UNNEST(CASE WHEN r.q_teams = 'ALL' THEN ['ALL'::VARCHAR]
            ELSE [r.q_teams, 'ALL'::VARCHAR] END) AS teams(selector_teams)
          CROSS JOIN UNNEST(CASE WHEN r.q_roster = 'ALL' THEN ['ALL'::VARCHAR]
            ELSE [r.q_roster, 'ALL'::VARCHAR] END) AS roster(selector_roster)
          CROSS JOIN UNNEST(CASE WHEN r.q_scoring = 'ALL' THEN ['ALL'::VARCHAR]
            ELSE [r.q_scoring, 'ALL'::VARCHAR] END) AS scoring(selector_scoring)
          CROSS JOIN UNNEST(CASE WHEN r.q_pass_td = 'ALL' THEN ['ALL'::VARCHAR]
            ELSE [r.q_pass_td, 'ALL'::VARCHAR] END) AS pass_td(selector_pass_td)
          CROSS JOIN UNNEST(CASE WHEN r.q_dynasty = 'ALL' THEN ['ALL'::VARCHAR]
            ELSE [r.q_dynasty, 'ALL'::VARCHAR] END) AS dynasty(selector_dynasty)
          CROSS JOIN UNNEST(CASE WHEN r.q_best_ball = 'ALL' THEN ['ALL'::VARCHAR]
            ELSE [r.q_best_ball, 'ALL'::VARCHAR] END) AS best_ball(selector_best_ball)
        ),
        eligible_cell_week AS (
          SELECT
            position, week,
            selector_teams AS q_teams, selector_roster AS q_roster,
            selector_scoring AS q_scoring, selector_pass_td AS q_pass_td,
            selector_dynasty AS q_dynasty, selector_best_ball AS q_best_ball,
            COUNT(DISTINCT db_name)::BIGINT AS eligible_leagues
          FROM expanded_inventory
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ),
        eligible_cell_season AS (
          SELECT
            position,
            selector_teams AS q_teams, selector_roster AS q_roster,
            selector_scoring AS q_scoring, selector_pass_td AS q_pass_td,
            selector_dynasty AS q_dynasty, selector_best_ball AS q_best_ball,
            COUNT(DISTINCT db_name)::BIGINT AS eligible_leagues
          FROM expanded_inventory
          GROUP BY 1, 2, 3, 4, 5, 6, 7
        ),
        player_per_league_week AS (
          SELECT
            CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(p.year AS INTEGER) AS year,
            CAST(p.week AS INTEGER) AS week,
            CAST(p.db_name AS VARCHAR) AS db_name,
            MAX(CAST(p.position AS VARCHAR)) AS position,
            r.selector_teams AS q_teams,
            r.selector_roster AS q_roster,
            r.selector_scoring AS q_scoring,
            r.selector_pass_td AS q_pass_td,
            r.selector_dynasty AS q_dynasty,
            r.selector_best_ball AS q_best_ball,
            MAX(g.active_week)::BIGINT AS active_week,
            MAX(CASE WHEN p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END)::BIGINT
              AS rostered,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1 THEN 1 ELSE 0 END)::BIGINT AS started,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1
                           AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN 1 ELSE 0 END)::BIGINT
              AS valid_started_outcome,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1
                           AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN CAST(p.win AS DOUBLE) END)
              AS win_equivalent,
            AVG(CASE WHEN CAST(p.is_started AS INTEGER) = 1 THEN CAST(p.clutch_equity AS DOUBLE) END)
              AS clutch_when_started
          FROM target_player_facts p
          INNER JOIN player_game_weeks g
            ON g.NFL_player_id = CAST(p.NFL_player_id AS VARCHAR)
           AND g.year = CAST(p.year AS INTEGER)
           AND g.week = CAST(p.week AS INTEGER)
          INNER JOIN expanded_inventory r
            ON r.db_name = CAST(p.db_name AS VARCHAR)
           AND r.week = CAST(p.week AS INTEGER)
           AND r.position = CAST(p.position AS VARCHAR)
           AND r.cohort_teams IS NOT DISTINCT FROM {_capacity_tier_sql('p.cohort_teams')}
           AND r.cohort_roster IS NOT DISTINCT FROM CAST(p.cohort_roster AS VARCHAR)
           AND r.cohort_scoring IS NOT DISTINCT FROM CAST(p.cohort_scoring AS VARCHAR)
           AND r.cohort_pass_td IS NOT DISTINCT FROM CAST(p.cohort_pass_td AS VARCHAR)
           AND r.cohort_playoff_teams IS NOT DISTINCT FROM CAST(p.cohort_playoff_teams AS VARCHAR)
           AND r.cohort_dynasty IS NOT DISTINCT FROM CAST(p.cohort_dynasty AS VARCHAR)
           AND r.cohort_best_ball IS NOT DISTINCT FROM CAST(p.cohort_best_ball AS VARCHAR)
          GROUP BY 1, 2, 3, 4, 6, 7, 8, 9, 10, 11
        ),
        weekly_metric AS (
          SELECT
            p.NFL_player_id, p.year, p.week, p.position,
            p.q_teams, p.q_roster, p.q_scoring, p.q_pass_td, p.q_dynasty, p.q_best_ball,
            MAX(p.active_week)::BIGINT AS active_week,
            SUM(p.rostered)::BIGINT AS rostered_leagues,
            SUM(p.started)::BIGINT AS started_leagues,
            SUM(p.valid_started_outcome)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(p.win_equivalent), 0.0) AS win_equivalent,
            ROUND(COALESCE(SUM(p.clutch_when_started), 0.0) / NULLIF(MAX(e.eligible_leagues), 0), 9)
              AS clutch_weekly_average,
            MAX(e.eligible_leagues)::BIGINT AS eligible_leagues
          FROM player_per_league_week p
          INNER JOIN eligible_cell_week e
            ON e.position = p.position AND e.week = p.week
           AND e.q_teams = p.q_teams AND e.q_roster = p.q_roster
           AND e.q_scoring = p.q_scoring AND e.q_pass_td = p.q_pass_td
           AND e.q_dynasty = p.q_dynasty AND e.q_best_ball = p.q_best_ball
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
        ),
        player_calendar AS (
          SELECT
            NFL_player_id,
            year,
            SUM(active_week)::BIGINT AS active_weeks,
            SUM(CASE WHEN active_week = 0 THEN 1 ELSE 0 END)::BIGINT AS inactive_weeks
          FROM player_game_weeks
          GROUP BY 1, 2
        ),
        season_metric AS (
          SELECT
            w.NFL_player_id, w.year, w.position,
            w.q_teams, w.q_roster, w.q_scoring, w.q_pass_td, w.q_dynasty, w.q_best_ball,
            MAX(e.eligible_leagues)::BIGINT AS eligible_leagues,
            SUM(w.rostered_leagues)::BIGINT AS rostered_league_weeks,
            SUM(w.eligible_leagues)::BIGINT AS eligible_league_weeks,
            SUM(w.started_leagues)::BIGINT AS started_league_weeks,
            SUM(w.valid_started_outcomes)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(w.win_equivalent), 0.0) AS win_equivalent,
            COALESCE(
              SUM(w.started_leagues)::DOUBLE / NULLIF(SUM(w.eligible_leagues), 0)
              * MAX(c.active_weeks),
              0.0
            ) AS expected_starts,
            COALESCE(
              SUM(w.started_leagues)::DOUBLE / NULLIF(SUM(w.eligible_leagues), 0)
              * MAX(c.active_weeks)
              * SUM(w.win_equivalent) / NULLIF(SUM(w.valid_started_outcomes), 0),
              0.0
            ) AS expected_wins,
            COALESCE(
              SUM(w.started_leagues)::DOUBLE / NULLIF(SUM(w.eligible_leagues), 0)
              * MAX(c.active_weeks)
              * (1.0 - SUM(w.win_equivalent) / NULLIF(SUM(w.valid_started_outcomes), 0)),
              0.0
            ) AS expected_losses,
            ROUND(COALESCE(SUM(w.clutch_weekly_average), 0.0), 9) AS clutch_season_sum,
            SUM(CASE WHEN w.active_week = 1 THEN w.started_leagues ELSE 0 END)::BIGINT
              AS healthy_started_league_weeks,
            SUM(CASE WHEN w.active_week = 1 THEN w.eligible_leagues ELSE 0 END)::BIGINT
              AS healthy_eligible_league_weeks,
            MAX(c.active_weeks)::BIGINT AS active_weeks,
            MAX(c.inactive_weeks)::BIGINT AS inactive_weeks
          FROM weekly_metric w
          INNER JOIN eligible_cell_season e
            ON e.position = w.position
           AND e.q_teams = w.q_teams AND e.q_roster = w.q_roster
           AND e.q_scoring = w.q_scoring AND e.q_pass_td = w.q_pass_td
           AND e.q_dynasty = w.q_dynasty AND e.q_best_ball = w.q_best_ball
          INNER JOIN player_calendar c
            ON c.NFL_player_id = w.NFL_player_id AND c.year = w.year
          GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        ),
        split_profile AS (
          SELECT
            position,
            CASE WHEN teams_08tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_08tm_split,
            CASE WHEN teams_10tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_10tm_split,
            CASE WHEN teams_12tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_12tm_split,
            CASE WHEN teams_14tm >= {split_threshold} THEN 1 ELSE 0 END AS teams_14tm_split,
            CASE WHEN roster_flx >= {split_threshold} THEN 1 ELSE 0 END AS roster_flx_split,
            CASE WHEN roster_sflx >= {split_threshold} THEN 1 ELSE 0 END AS roster_sflx_split,
            CASE WHEN roster_idp >= {split_threshold} THEN 1 ELSE 0 END AS roster_idp_split,
            CASE WHEN scoring_std >= {split_threshold} THEN 1 ELSE 0 END AS scoring_std_split,
            CASE WHEN scoring_half >= {split_threshold} THEN 1 ELSE 0 END AS scoring_half_split,
            CASE WHEN scoring_ppr >= {split_threshold} THEN 1 ELSE 0 END AS scoring_ppr_split,
            CASE WHEN pass_td_4pt >= {split_threshold} THEN 1 ELSE 0 END AS pass_td_4pt_split,
            CASE WHEN pass_td_6pt >= {split_threshold} THEN 1 ELSE 0 END AS pass_td_6pt_split,
            CASE WHEN dynasty_redraft >= {split_threshold} THEN 1 ELSE 0 END AS dynasty_redraft_split,
            CASE WHEN dynasty_dynasty >= {split_threshold} THEN 1 ELSE 0 END AS dynasty_dynasty_split,
            CASE WHEN best_ball_managed >= {split_threshold} THEN 1 ELSE 0 END AS best_ball_managed_split,
            CASE WHEN best_ball_best_ball >= {split_threshold} THEN 1 ELSE 0 END AS best_ball_best_ball_split
          FROM dimension_counts
        )
        SELECT
          m.NFL_player_id,
          m.year,
          MAX(m.position) AS position,
          struct_pack(
            teams_08tm_split := MAX(s.teams_08tm_split),
            teams_10tm_split := MAX(s.teams_10tm_split),
            teams_12tm_split := MAX(s.teams_12tm_split),
            teams_14tm_split := MAX(s.teams_14tm_split),
            roster_flx_split := MAX(s.roster_flx_split),
            roster_sflx_split := MAX(s.roster_sflx_split),
            roster_idp_split := MAX(s.roster_idp_split),
            scoring_std_split := MAX(s.scoring_std_split),
            scoring_half_split := MAX(s.scoring_half_split),
            scoring_ppr_split := MAX(s.scoring_ppr_split),
            pass_td_4pt_split := MAX(s.pass_td_4pt_split),
            pass_td_6pt_split := MAX(s.pass_td_6pt_split),
            dynasty_redraft_split := MAX(s.dynasty_redraft_split),
            dynasty_dynasty_split := MAX(s.dynasty_dynasty_split),
            best_ball_managed_split := MAX(s.best_ball_managed_split),
            best_ball_best_ball_split := MAX(s.best_ball_best_ball_split)
          ) AS cohort_profile,
          list(struct_pack(
            q_teams := q_teams,
            q_roster := q_roster,
            q_scoring := q_scoring,
            q_pass_td := q_pass_td,
            q_dynasty := q_dynasty,
            q_best_ball := q_best_ball,
            eligible_leagues := eligible_leagues,
            rostered_league_weeks := rostered_league_weeks,
            eligible_league_weeks := eligible_league_weeks,
            started_league_weeks := started_league_weeks,
            healthy_started_league_weeks := healthy_started_league_weeks,
            healthy_eligible_league_weeks := healthy_eligible_league_weeks,
            valid_started_outcomes := valid_started_outcomes,
            win_equivalent := win_equivalent,
            roster_rate_pct := 100.0 * rostered_league_weeks / NULLIF(eligible_league_weeks, 0),
            start_rate_pct := 100.0 * started_league_weeks / NULLIF(eligible_league_weeks, 0),
            healthy_start_rate_pct := 100.0 * healthy_started_league_weeks / NULLIF(healthy_eligible_league_weeks, 0),
            win_rate_pct := 100.0 * win_equivalent / NULLIF(valid_started_outcomes, 0),
            expected_starts := expected_starts,
            expected_wins := expected_wins,
            expected_losses := expected_losses,
            clutch_season_sum := clutch_season_sum,
            active_weeks := active_weeks,
            inactive_weeks := inactive_weeks
          ) ORDER BY q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball) AS cohort_cells
        FROM season_metric m
        INNER JOIN split_profile s ON s.position = m.position
        GROUP BY m.NFL_player_id, m.year
        """,
        [year, player_id, year, year, position, position, player_id],
    )
    _compact_season_requested_cells(
        connection,
        output_table=output_table,
        split_threshold=split_threshold,
    )


def _compact_season_requested_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    output_table: str,
    split_threshold: int,
) -> None:
    """Keep season core cells reachable from a supported request plus ALL audit.

    The season aggregation deliberately expands selectors to derive correct
    independent partial pools. Only the 288 request tuples can reach the API;
    retaining every expansion would make a fully split player carry 1,296
    nested cells. This final in-DuckDB projection preserves only player cells
    with at least ``split_threshold`` rostered league-weeks, falling back to
    the most-specific remaining parent for each request.  Thus a player with
    no qualifying rostered sample has no served season row.
    """

    output = _table_sql(output_table)
    temp = _table_sql(f"{output_table}_requested_cells")
    _compact_core_cells_after_rostered_floor(
        connection,
        output_table=output_table,
        rostered_field="rostered_league_weeks",
        split_threshold=split_threshold,
    )
    return
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {temp} AS
        WITH requested AS (
          SELECT *
          FROM (VALUES ('08tm'), ('10tm'), ('12tm'), ('14tm')) teams(teams)
          CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) roster(roster)
          CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) scoring(scoring)
          CROSS JOIN (VALUES ('4pt'), ('6pt')) pass_td(pass_td)
          CROSS JOIN (VALUES ('redraft'), ('dynasty')) dynasty(dynasty)
          CROSS JOIN (VALUES ('managed'), ('best_ball')) best_ball(best_ball)
        ),
        candidates AS (
          SELECT r.NFL_player_id, r.year, r.position, r.cohort_profile,
            q.teams, q.roster, q.scoring, q.pass_td, q.dynasty, q.best_ball, cell,
            (
              CASE WHEN cell.q_teams <> 'ALL' THEN 32 ELSE 0 END
              + CASE WHEN cell.q_roster <> 'ALL' THEN 16 ELSE 0 END
              + CASE WHEN cell.q_scoring <> 'ALL' THEN 8 ELSE 0 END
              + CASE WHEN cell.q_pass_td <> 'ALL' THEN 4 ELSE 0 END
              + CASE WHEN cell.q_dynasty <> 'ALL' THEN 2 ELSE 0 END
              + CASE WHEN cell.q_best_ball <> 'ALL' THEN 1 ELSE 0 END
            ) AS retained_specificity
          FROM {output} r
          CROSS JOIN requested q
          CROSS JOIN UNNEST(r.cohort_cells) AS u(cell)
          WHERE cell.rostered_league_weeks >= {split_threshold}
            AND cell.q_teams IN ('ALL', q.teams)
            AND cell.q_roster IN ('ALL', q.roster)
            AND cell.q_scoring IN ('ALL', q.scoring)
            AND cell.q_pass_td IN ('ALL', q.pass_td)
            AND cell.q_dynasty IN ('ALL', q.dynasty)
            AND cell.q_best_ball IN ('ALL', q.best_ball)
        ),
        selected AS (
          SELECT NFL_player_id, year, position, cohort_profile, cell
          FROM candidates
          QUALIFY ROW_NUMBER() OVER (
            PARTITION BY NFL_player_id, year, position,
              teams, roster, scoring, pass_td, dynasty, best_ball
            ORDER BY retained_specificity DESC, cell.eligible_leagues ASC,
              cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
              cell.q_dynasty, cell.q_best_ball
          ) = 1
          UNION ALL
          SELECT r.NFL_player_id, r.year, r.position, r.cohort_profile, cell
          FROM {output} r
          CROSS JOIN UNNEST(r.cohort_cells) AS u(cell)
          WHERE cell.rostered_league_weeks >= {split_threshold}
            AND cell.q_teams='ALL' AND cell.q_roster='ALL' AND cell.q_scoring='ALL'
            AND cell.q_pass_td='ALL' AND cell.q_dynasty='ALL' AND cell.q_best_ball='ALL'
        ),
        dedup AS (
          SELECT NFL_player_id, year, position, cohort_profile,
            cell.q_teams, cell.q_roster, cell.q_scoring, cell.q_pass_td,
            cell.q_dynasty, cell.q_best_ball, ANY_VALUE(cell) AS cell
          FROM selected
          GROUP BY ALL
        )
        SELECT NFL_player_id, year, position, ANY_VALUE(cohort_profile) AS cohort_profile,
          list(cell ORDER BY q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball) AS cohort_cells
        FROM dedup
        GROUP BY NFL_player_id, year, position
        """
    )
    connection.execute(f"CREATE OR REPLACE TABLE {output} AS SELECT * FROM {temp}")
    oversized = connection.execute(
        f"SELECT COUNT(*) FROM {output} WHERE list_count(cohort_cells) > 289"
    ).fetchone()[0]
    if oversized:
        raise RuntimeError("Season compact output exceeds 288 reachable requests plus its ALL audit cell")


def materialize_season_core_cohort_position_batch(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    nfl_stats_table: str,
    player_team_game_week_table: str | None = None,
    player_active_week_table: str | None = None,
    target_players_table: str | None = None,
    output_table: str,
    year: int,
    position: str,
    split_threshold: int = 150,
    team_stat: str = "started",
    include_roster_map: bool = True,
) -> None:
    """Build all compact season core rows for one position/year in one query.

    Raw player-league facts are reduced to actual cohort profiles before the
    six-dimension cube, so selector cells are not produced by multiplying raw
    facts by every selector combination.
    """
    if split_threshold < 1:
        raise ValueError("split_threshold must be positive")
    if (player_team_game_week_table is None) != (player_active_week_table is None):
        raise ValueError(
            "player_team_game_week_table and player_active_week_table must be supplied together"
        )
    source = _source_without_bad_identity_rows(connection, source_table)
    teams_column = _team_capacity_column(connection, source_table, stat=team_stat)
    stats = _relation_sql(nfl_stats_table)
    target_players_relation = (
        None if target_players_table is None else _relation_sql(target_players_table)
    )
    output = _table_sql(output_table)
    target_player_join = (
        ""
        if target_players_relation is None
        else f"""
          INNER JOIN {target_players_relation} bucket
            ON CAST(bucket.NFL_player_id AS VARCHAR) = CAST(p.NFL_player_id AS VARCHAR)
        """
    )
    if player_team_game_week_table is None:
        calendar_parameters = [year]
        calendar_ctes = f"""
        player_regular_stats AS (
          SELECT CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(s.year AS INTEGER) AS year, CAST(s.week AS INTEGER) AS week,
            NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') AS nfl_team,
            MAX(CASE WHEN COALESCE(CAST(s.offense_snaps AS BIGINT),0)
              + COALESCE(CAST(s.defense_snaps AS BIGINT),0)
              + COALESCE(CAST(s.special_teams_snaps AS BIGINT),0) > 0 THEN 1 ELSE 0 END)::BIGINT AS active_week
          FROM {stats} s INNER JOIN target_players t
            ON t.NFL_player_id = CAST(s.NFL_player_id AS VARCHAR)
          WHERE CAST(s.year AS INTEGER) = ? AND s.week IS NOT NULL
            AND COALESCE(s.season_type, 'REG') = 'REG'
          GROUP BY 1,2,3,4
        ),
        player_team_year AS (
          SELECT NFL_player_id, year, MODE(nfl_team) AS nfl_team
          FROM player_regular_stats WHERE nfl_team IS NOT NULL GROUP BY 1,2
        ),
        team_regular_game_weeks AS (
          SELECT DISTINCT CAST(s.year AS INTEGER) AS year, CAST(s.week AS INTEGER) AS week,
            NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') AS nfl_team
          FROM {stats} s INNER JOIN player_team_year p
            ON p.year = CAST(s.year AS INTEGER)
           AND p.nfl_team = NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '')
          WHERE s.week IS NOT NULL AND COALESCE(s.season_type, 'REG') = 'REG'
        ),
        player_game_weeks AS (
          SELECT p.NFL_player_id, p.year, g.week,
            COALESCE(MAX(r.active_week),0)::BIGINT AS active_week
          FROM player_team_year p INNER JOIN team_regular_game_weeks g
            ON g.year=p.year AND g.nfl_team=p.nfl_team
          LEFT JOIN player_regular_stats r ON r.NFL_player_id=p.NFL_player_id AND r.year=p.year AND r.week=g.week
          GROUP BY 1,2,3
        ),
        """
    else:
        team_weeks = _relation_sql(player_team_game_week_table)
        active_weeks = _relation_sql(player_active_week_table)
        active_expression = "1" if position.upper() in {"DEF", "DST", "D/ST", "DEFENSE"} else "CASE WHEN a.NFL_player_id IS NOT NULL THEN 1 ELSE 0 END"
        calendar_parameters = [year]
        calendar_ctes = f"""
        player_game_weeks AS (
          SELECT CAST(t.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(t.year AS INTEGER) AS year, CAST(t.week AS INTEGER) AS week,
            MAX({active_expression})::BIGINT AS active_week
          FROM {team_weeks} t
          INNER JOIN target_players p
            ON p.NFL_player_id = CAST(t.NFL_player_id AS VARCHAR)
          LEFT JOIN {active_weeks} a
            ON CAST(a.NFL_player_id AS VARCHAR) = CAST(t.NFL_player_id AS VARCHAR)
           AND CAST(a.year AS INTEGER) = CAST(t.year AS INTEGER)
           AND CAST(a.week AS INTEGER) = CAST(t.week AS INTEGER)
          WHERE CAST(t.year AS INTEGER) = ?
            AND CAST(t.week AS INTEGER) BETWEEN 1 AND 18
          GROUP BY 1,2,3
        ),
        """
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH target_players AS (
          SELECT DISTINCT CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id
          FROM {source} p
          {target_player_join}
          WHERE CAST(p.year AS INTEGER) = ? AND {_canonical_position_sql('p.position')} = ?
          GROUP BY 1
          HAVING MAX(CASE WHEN p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END) = 1
        ),
        {calendar_ctes}
        population AS (
          SELECT db_name,year,week,NFL_player_id,{_canonical_position_sql('position')} AS position,cohort_position_eligible,
            {_capacity_tier_sql(_table_sql(teams_column))} AS cohort_teams,
            cohort_roster,cohort_scoring,cohort_pass_td,cohort_playoff_teams,
            cohort_dynasty,cohort_best_ball,is_rostered,is_started,win,clutch_equity
          FROM {source}
          WHERE CAST(year AS INTEGER)=? AND {_canonical_position_sql('position')}=?
        ),
        inventory AS (
          SELECT DISTINCT CAST(db_name AS VARCHAR) AS db_name, CAST(week AS INTEGER) AS week,
            CAST(position AS VARCHAR) AS position, CAST(cohort_teams AS VARCHAR) AS teams,
            CAST(cohort_roster AS VARCHAR) roster, CAST(cohort_scoring AS VARCHAR) scoring,
            CAST(cohort_pass_td AS VARCHAR) pass_td, CAST(cohort_playoff_teams AS VARCHAR) playoff_teams,
            CAST(cohort_dynasty AS VARCHAR) dynasty, CAST(cohort_best_ball AS VARCHAR) best_ball
          FROM population WHERE CAST(cohort_position_eligible AS INTEGER)=1
        ),
        counts AS (
          SELECT position,
            COUNT(DISTINCT db_name) FILTER(WHERE teams='08tm') c08,
            COUNT(DISTINCT db_name) FILTER(WHERE teams='10tm') c10,
            COUNT(DISTINCT db_name) FILTER(WHERE teams='12tm') c12,
            COUNT(DISTINCT db_name) FILTER(WHERE teams='14tm') c14,
            COUNT(DISTINCT db_name) FILTER(WHERE roster='flx') cflx, COUNT(DISTINCT db_name) FILTER(WHERE roster='sflx') csflx, COUNT(DISTINCT db_name) FILTER(WHERE roster='idp') cidp,
            COUNT(DISTINCT db_name) FILTER(WHERE scoring='std') cstd, COUNT(DISTINCT db_name) FILTER(WHERE scoring='half') chalf, COUNT(DISTINCT db_name) FILTER(WHERE scoring='ppr') cppr,
            COUNT(DISTINCT db_name) FILTER(WHERE pass_td='4pt') c4, COUNT(DISTINCT db_name) FILTER(WHERE pass_td='6pt') c6,
            COUNT(DISTINCT db_name) FILTER(WHERE dynasty='redraft') credraft, COUNT(DISTINCT db_name) FILTER(WHERE dynasty='dynasty') cdynasty,
            COUNT(DISTINCT db_name) FILTER(WHERE best_ball='managed') cmanaged, COUNT(DISTINCT db_name) FILTER(WHERE best_ball='best_ball') cbb
          FROM inventory GROUP BY 1
        ),
        resolved AS (
          SELECT i.*,
            CASE WHEN (teams='08tm' AND c08>={split_threshold})
                   OR (teams='10tm' AND c10>={split_threshold})
                   OR (teams='12tm' AND c12>={split_threshold})
                   OR (teams='14tm' AND c14>={split_threshold}) THEN teams ELSE 'ALL' END qt,
            CASE WHEN (roster='flx' AND cflx>={split_threshold}) OR (roster='sflx' AND csflx>={split_threshold}) OR (roster='idp' AND cidp>={split_threshold}) THEN roster ELSE 'ALL' END qr,
            CASE WHEN (scoring='std' AND cstd>={split_threshold}) OR (scoring='half' AND chalf>={split_threshold}) OR (scoring='ppr' AND cppr>={split_threshold}) THEN scoring ELSE 'ALL' END qs,
            CASE WHEN (pass_td='4pt' AND c4>={split_threshold}) OR (pass_td='6pt' AND c6>={split_threshold}) THEN pass_td ELSE 'ALL' END qp,
            CASE WHEN (dynasty='redraft' AND credraft>={split_threshold}) OR (dynasty='dynasty' AND cdynasty>={split_threshold}) THEN dynasty ELSE 'ALL' END qd,
            CASE WHEN (best_ball='managed' AND cmanaged>={split_threshold}) OR (best_ball='best_ball' AND cbb>={split_threshold}) THEN best_ball ELSE 'ALL' END qb
          FROM inventory i INNER JOIN counts c USING(position)
        ),
        eligible_week_leaf AS (
          SELECT position, week, qt, qr, qs, qp, qd, qb,
            COUNT(DISTINCT db_name)::BIGINT AS e
          FROM resolved
          GROUP BY 1,2,3,4,5,6,7,8
        ),
        eligible_week_cube AS (
          SELECT position, week,
            CASE WHEN GROUPING(qt)=1 THEN 'ALL' ELSE qt END st,
            CASE WHEN GROUPING(qr)=1 THEN 'ALL' ELSE qr END sr,
            CASE WHEN GROUPING(qs)=1 THEN 'ALL' ELSE qs END ss,
            CASE WHEN GROUPING(qp)=1 THEN 'ALL' ELSE qp END sp,
            CASE WHEN GROUPING(qd)=1 THEN 'ALL' ELSE qd END sd,
            CASE WHEN GROUPING(qb)=1 THEN 'ALL' ELSE qb END sb,
            GROUPING(qt) gt,GROUPING(qr) gr,GROUPING(qs) gs,
            GROUPING(qp) gp,GROUPING(qd) gd,GROUPING(qb) gb,
            SUM(e)::BIGINT e
          FROM eligible_week_leaf
          GROUP BY position, week, CUBE(qt,qr,qs,qp,qd,qb)
        ),
        eweek AS (
          SELECT position, week, st, sr, ss, sp, sd, sb, MAX(e)::BIGINT e
          FROM eligible_week_cube
          WHERE (gt=1 OR st<>'ALL') AND (gr=1 OR sr<>'ALL')
            AND (gs=1 OR ss<>'ALL') AND (gp=1 OR sp<>'ALL')
            AND (gd=1 OR sd<>'ALL') AND (gb=1 OR sb<>'ALL')
          GROUP BY 1,2,3,4,5,6,7,8
        ),
        eligible_season_source AS (
          SELECT DISTINCT position, db_name, qt, qr, qs, qp, qd, qb
          FROM resolved
        ),
        eligible_season_cube AS (
          SELECT position,
            CASE WHEN GROUPING(qt)=1 THEN 'ALL' ELSE qt END st,
            CASE WHEN GROUPING(qr)=1 THEN 'ALL' ELSE qr END sr,
            CASE WHEN GROUPING(qs)=1 THEN 'ALL' ELSE qs END ss,
            CASE WHEN GROUPING(qp)=1 THEN 'ALL' ELSE qp END sp,
            CASE WHEN GROUPING(qd)=1 THEN 'ALL' ELSE qd END sd,
            CASE WHEN GROUPING(qb)=1 THEN 'ALL' ELSE qb END sb,
            GROUPING(qt) gt,GROUPING(qr) gr,GROUPING(qs) gs,
            GROUPING(qp) gp,GROUPING(qd) gd,GROUPING(qb) gb,
            COUNT(DISTINCT db_name)::BIGINT e
          FROM eligible_season_source
          GROUP BY position, CUBE(qt,qr,qs,qp,qd,qb)
        ),
        eseason AS (
          SELECT position, st, sr, ss, sp, sd, sb, MAX(e)::BIGINT e
          FROM eligible_season_cube
          WHERE (gt=1 OR st<>'ALL') AND (gr=1 OR sr<>'ALL')
            AND (gs=1 OR ss<>'ALL') AND (gp=1 OR sp<>'ALL')
            AND (gd=1 OR sd<>'ALL') AND (gb=1 OR sb<>'ALL')
          GROUP BY 1,2,3,4,5,6,7
        ),
        per_league AS (
          SELECT CAST(p.NFL_player_id AS VARCHAR) AS id,CAST(p.year AS INTEGER) AS year,CAST(p.week AS INTEGER) AS week,CAST(p.db_name AS VARCHAR) AS db,
            r.position,r.qt,r.qr,r.qs,r.qp,r.qd,r.qb,MAX(g.active_week)::BIGINT active,
            MAX(CASE WHEN p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER)<>0 THEN 1 ELSE 0 END)::BIGINT rostered,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER)=1 THEN 1 ELSE 0 END)::BIGINT started,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN 1 ELSE 0 END)::BIGINT AS is_valid,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER)=1 AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN CAST(p.win AS DOUBLE) END) won,
            AVG(CASE WHEN CAST(p.is_started AS INTEGER)=1 THEN CAST(p.clutch_equity AS DOUBLE) END) clutch
          FROM population p INNER JOIN player_game_weeks g ON g.NFL_player_id=CAST(p.NFL_player_id AS VARCHAR) AND g.year=CAST(p.year AS INTEGER) AND g.week=CAST(p.week AS INTEGER)
          INNER JOIN resolved r ON r.db_name=CAST(p.db_name AS VARCHAR) AND r.week=CAST(p.week AS INTEGER) AND r.position=CAST(p.position AS VARCHAR)
            AND r.teams IS NOT DISTINCT FROM {_capacity_tier_sql('p.cohort_teams')} AND r.roster IS NOT DISTINCT FROM CAST(p.cohort_roster AS VARCHAR)
            AND r.scoring IS NOT DISTINCT FROM CAST(p.cohort_scoring AS VARCHAR) AND r.pass_td IS NOT DISTINCT FROM CAST(p.cohort_pass_td AS VARCHAR)
            AND r.playoff_teams IS NOT DISTINCT FROM CAST(p.cohort_playoff_teams AS VARCHAR) AND r.dynasty IS NOT DISTINCT FROM CAST(p.cohort_dynasty AS VARCHAR) AND r.best_ball IS NOT DISTINCT FROM CAST(p.cohort_best_ball AS VARCHAR)
          GROUP BY 1,2,3,4,5,6,7,8,9,10,11
        ),
        profile AS (SELECT id,year,week,position,qt,qr,qs,qp,qd,qb,MAX(active) active,SUM(rostered)::BIGINT r,SUM(started)::BIGINT s,SUM(is_valid)::BIGINT v,COALESCE(SUM(won),0.0) w,COALESCE(SUM(clutch),0.0) cs,COUNT(clutch)::BIGINT cn FROM per_league GROUP BY 1,2,3,4,5,6,7,8,9,10),
        cube AS (
          SELECT id,year,week,position,CASE WHEN GROUPING(qt)=1 THEN 'ALL' ELSE qt END st,CASE WHEN GROUPING(qr)=1 THEN 'ALL' ELSE qr END sr,CASE WHEN GROUPING(qs)=1 THEN 'ALL' ELSE qs END ss,CASE WHEN GROUPING(qp)=1 THEN 'ALL' ELSE qp END sp,CASE WHEN GROUPING(qd)=1 THEN 'ALL' ELSE qd END sd,CASE WHEN GROUPING(qb)=1 THEN 'ALL' ELSE qb END sb,
            GROUPING(qt) gt,GROUPING(qr) gr,GROUPING(qs) gs,GROUPING(qp) gp,GROUPING(qd) gd,GROUPING(qb) gb,MAX(active)::BIGINT active,SUM(r)::BIGINT r,SUM(s)::BIGINT s,SUM(v)::BIGINT v,SUM(w) w,SUM(cs) cs,SUM(cn)::BIGINT cn
          FROM profile GROUP BY id,year,week,position,CUBE(qt,qr,qs,qp,qd,qb)
        ),
        weekly AS (
          SELECT c.*,e.e, ROUND(c.cs/NULLIF(c.cn,0), 9) clutch
          FROM cube c INNER JOIN eweek e ON e.position=c.position AND e.week=c.week AND e.st=c.st AND e.sr=c.sr AND e.ss=c.ss AND e.sp=c.sp AND e.sd=c.sd AND e.sb=c.sb
          WHERE (c.gt=1 OR c.st<>'ALL') AND (c.gr=1 OR c.sr<>'ALL') AND (c.gs=1 OR c.ss<>'ALL') AND (c.gp=1 OR c.sp<>'ALL') AND (c.gd=1 OR c.sd<>'ALL') AND (c.gb=1 OR c.sb<>'ALL')
        ),
        observed_players AS (SELECT DISTINCT id,year,position FROM weekly),
        calendar_eligible AS (
          SELECT g.NFL_player_id id,g.year,e.position,e.st,e.sr,e.ss,e.sp,e.sd,e.sb,
            SUM(e.e)::BIGINT ew,
            SUM(CASE WHEN g.active_week=1 THEN e.e ELSE 0 END)::BIGINT he,
            SUM(g.active_week)::BIGINT aw,
            SUM(CASE WHEN g.active_week=0 THEN 1 ELSE 0 END)::BIGINT iw
          FROM player_game_weeks g
          INNER JOIN observed_players p ON p.id=g.NFL_player_id AND p.year=g.year
          INNER JOIN eweek e ON e.position=p.position AND e.week=g.week
          GROUP BY 1,2,3,4,5,6,7,8,9
        ),
        season AS (
          SELECT ce.id,ce.year,ce.position,ce.st,ce.sr,ce.ss,ce.sp,ce.sd,ce.sb,
            MAX(es.e)::BIGINT e,
            COALESCE(SUM(w.r),0)::BIGINT r,
            ce.ew,
            COALESCE(SUM(w.s),0)::BIGINT s,
            COALESCE(SUM(w.v),0)::BIGINT v,
            COALESCE(SUM(w.w),0.0) won,
            COALESCE(SUM(CASE WHEN w.active=1 THEN w.s ELSE 0 END),0)::BIGINT hs,
            ce.he,
            COALESCE(SUM(w.s)::DOUBLE/NULLIF(ce.ew,0)*ce.aw,0.0) xs,
            COALESCE(SUM(w.s)::DOUBLE/NULLIF(ce.ew,0)*ce.aw*SUM(w.w)/NULLIF(SUM(w.v),0),0.0) xw,
            COALESCE(SUM(w.s)::DOUBLE/NULLIF(ce.ew,0)*ce.aw*(1-SUM(w.w)/NULLIF(SUM(w.v),0)),0.0) xl,
            ROUND(COALESCE(SUM(w.clutch),0.0), 9) clutch,
            ce.aw::BIGINT aw,ce.iw::BIGINT iw
          FROM calendar_eligible ce
          LEFT JOIN weekly w ON w.id=ce.id AND w.year=ce.year AND w.position=ce.position
            AND w.st=ce.st AND w.sr=ce.sr AND w.ss=ce.ss AND w.sp=ce.sp AND w.sd=ce.sd AND w.sb=ce.sb
          INNER JOIN eseason es ON es.position=ce.position AND es.st=ce.st AND es.sr=ce.sr AND es.ss=ce.ss AND es.sp=ce.sp AND es.sd=ce.sd AND es.sb=ce.sb
          GROUP BY 1,2,3,4,5,6,7,8,9,ce.ew,ce.he,ce.aw,ce.iw
        )
        SELECT id AS NFL_player_id, year, MAX(season.position) AS position,
          list(struct_pack(q_teams:=st,q_roster:=sr,q_scoring:=ss,q_pass_td:=sp,q_dynasty:=sd,q_best_ball:=sb,eligible_leagues:=e,rostered_league_weeks:=r,eligible_league_weeks:=ew,started_league_weeks:=s,healthy_started_league_weeks:=hs,healthy_eligible_league_weeks:=he,valid_started_outcomes:=v,win_equivalent:=won,roster_rate_pct:=100.0*r/NULLIF(ew,0),start_rate_pct:=100.0*s/NULLIF(ew,0),healthy_start_rate_pct:=100.0*hs/NULLIF(he,0),win_rate_pct:=100.0*won/NULLIF(v,0),expected_starts:=xs,expected_wins:=xw,expected_losses:=xl,clutch_season_sum:=clutch,active_weeks:=aw,inactive_weeks:=iw) ORDER BY st,sr,ss,sp,sd,sb) AS cohort_cells,
          ANY_VALUE(struct_pack(
            teams_08tm_split:=c08>={split_threshold},teams_10tm_split:=c10>={split_threshold},
            teams_12tm_split:=c12>={split_threshold},teams_14tm_split:=c14>={split_threshold},
            roster_flx_split:=cflx>={split_threshold},roster_sflx_split:=csflx>={split_threshold},roster_idp_split:=cidp>={split_threshold},
            scoring_std_split:=cstd>={split_threshold},scoring_half_split:=chalf>={split_threshold},scoring_ppr_split:=cppr>={split_threshold},
            pass_td_4pt_split:=c4>={split_threshold},pass_td_6pt_split:=c6>={split_threshold},
            dynasty_redraft_split:=credraft>={split_threshold},dynasty_dynasty_split:=cdynasty>={split_threshold},
            best_ball_managed_split:=cmanaged>={split_threshold},best_ball_best_ball_split:=cbb>={split_threshold}
          )) AS cohort_profile
        FROM season CROSS JOIN counts
        GROUP BY id,year
        """,
        [year, position, *calendar_parameters, year, position],
    )
    _compact_season_requested_cells(
        connection,
        output_table=output_table,
        split_threshold=split_threshold,
    )
    if include_roster_map and team_stat == "started":
        roster_output_table = f"{output_table}_roster_map"
        materialize_season_core_cohort_position_batch(
            connection,
            source_table=source_table,
            nfl_stats_table=nfl_stats_table,
            player_team_game_week_table=player_team_game_week_table,
            player_active_week_table=player_active_week_table,
            target_players_table=target_players_table,
            output_table=roster_output_table,
            year=year,
            position=position,
            split_threshold=split_threshold,
            team_stat="rostered",
            include_roster_map=False,
        )
        materialize_compact_core_selector_indices(
            connection, output_table=roster_output_table
        )
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {output} AS
            SELECT
              started.*,
              roster.cohort_cells AS roster_cells,
              roster.core_selector_indices AS roster_selector_indices
            FROM {output} started
            INNER JOIN {_table_sql(roster_output_table)} roster
              ON roster.NFL_player_id = started.NFL_player_id
             AND roster.year = started.year
             AND roster.position = started.position
            """
        )
        connection.execute(f"DROP TABLE {_table_sql(roster_output_table)}")


def materialize_season_outer_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    source_table: str,
    nfl_stats_table: str,
    output_table: str,
    year: int | None,
    player_id: str,
) -> None:
    """Materialize one compact season record from cached league and NFL facts.

    League behavior comes only from ``source_table``.  The cached NFL table is
    used solely to derive the player's regular-season team game weeks and their
    snap-based active/inactive status.  That prevents a bye or an NFL postseason
    roster record from becoming a weekly/season opportunity, without asking Fly
    or a settings/API service for calendar data.
    """

    source = _source_without_bad_identity_rows(connection, source_table)
    stats = _relation_sql(nfl_stats_table)
    output = _table_sql(output_table)
    stats_year_predicate = "CAST(year AS INTEGER) = ?" if year is not None else "TRUE"
    population_year_predicate = "CAST(year AS INTEGER) = ?" if year is not None else "TRUE"
    parameters: list[object] = []
    if year is not None:
        parameters.append(year)
    parameters.append(player_id)
    if year is not None:
        parameters.append(year)
    parameters.append(player_id)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH player_regular_stats AS (
          SELECT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week,
            NULLIF(TRIM(CAST(nfl_team AS VARCHAR)), '') AS nfl_team,
            MAX(CASE WHEN
              COALESCE(CAST(offense_snaps AS BIGINT), 0)
              + COALESCE(CAST(defense_snaps AS BIGINT), 0)
              + COALESCE(CAST(special_teams_snaps AS BIGINT), 0) > 0
              THEN 1 ELSE 0 END
            )::BIGINT AS active_week
          FROM {stats}
          WHERE {stats_year_predicate}
            AND week IS NOT NULL
            AND COALESCE(season_type, 'REG') = 'REG'
            AND CAST(NFL_player_id AS VARCHAR) = ?
          GROUP BY 1, 2, 3, 4
        ),
        player_team_year AS (
          SELECT NFL_player_id, year, MODE(nfl_team) AS nfl_team
          FROM player_regular_stats
          WHERE nfl_team IS NOT NULL
          GROUP BY 1, 2
        ),
        team_regular_game_weeks AS (
          SELECT DISTINCT
            CAST(s.year AS INTEGER) AS year,
            CAST(s.week AS INTEGER) AS week,
            NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') AS nfl_team
          FROM {stats} s
          INNER JOIN player_team_year pty
            ON pty.year = CAST(s.year AS INTEGER)
           AND pty.nfl_team = NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '')
          WHERE s.week IS NOT NULL
            AND COALESCE(s.season_type, 'REG') = 'REG'
        ),
        player_game_weeks AS (
          SELECT
            pty.NFL_player_id,
            pty.year,
            twg.week,
            COALESCE(MAX(rs.active_week), 0)::BIGINT AS active_week
          FROM player_team_year pty
          INNER JOIN team_regular_game_weeks twg
            ON twg.year = pty.year AND twg.nfl_team = pty.nfl_team
          LEFT JOIN player_regular_stats rs
            ON rs.NFL_player_id = pty.NFL_player_id
           AND rs.year = pty.year
           AND rs.week = twg.week
          GROUP BY 1, 2, 3
        ),
        population AS (
          SELECT
            db_name, year, week, NFL_player_id, position,
            cohort_position_eligible, is_rostered, is_started, win, clutch_equity,
            is_playoffs, champion
          FROM {source}
          WHERE {population_year_predicate}
        ),
        eligible_by_position_week AS (
          SELECT
            CAST(week AS INTEGER) AS week,
            CAST(position AS VARCHAR) AS position,
            COUNT(DISTINCT CAST(db_name AS VARCHAR))::BIGINT AS eligible_leagues
          FROM population
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
          GROUP BY 1, 2
        ),
        eligible_by_position_season AS (
          SELECT
            CAST(position AS VARCHAR) AS position,
            COUNT(DISTINCT CAST(db_name AS VARCHAR))::BIGINT AS eligible_leagues
          FROM population
          WHERE CAST(cohort_position_eligible AS INTEGER) = 1
          GROUP BY 1
        ),
        player_per_league_week AS (
          SELECT
            CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(p.year AS INTEGER) AS year,
            CAST(p.week AS INTEGER) AS week,
            CAST(p.db_name AS VARCHAR) AS db_name,
            MAX(CAST(p.position AS VARCHAR)) AS position,
            MAX(g.active_week)::BIGINT AS active_week,
            MAX(CASE
              WHEN p.is_rostered IS NULL OR CAST(p.is_rostered AS INTEGER) <> 0 THEN 1
              ELSE 0
            END)::BIGINT AS rostered,
            MAX(CASE WHEN CAST(p.is_started AS INTEGER) = 1 THEN 1 ELSE 0 END)::BIGINT AS started,
            MAX(CASE
              WHEN CAST(p.is_started AS INTEGER) = 1
               AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN 1
              ELSE 0
            END)::BIGINT AS valid_started_outcome,
            MAX(CASE
              WHEN CAST(p.is_started AS INTEGER) = 1
               AND CAST(p.win AS DOUBLE) BETWEEN 0 AND 1 THEN CAST(p.win AS DOUBLE)
            END) AS win_equivalent,
            AVG(CASE WHEN CAST(p.is_started AS INTEGER) = 1 THEN CAST(p.clutch_equity AS DOUBLE) END)
              AS clutch_when_started,
            MAX(CASE
              WHEN CAST(p.is_started AS INTEGER) = 1 AND (CAST(p.is_playoffs AS INTEGER) = 1 OR CAST(p.champion AS INTEGER) = 1) THEN 1
              ELSE 0
            END)::BIGINT AS playoff_credit,
            MAX(CASE
              WHEN CAST(p.is_started AS INTEGER) = 1 AND CAST(p.champion AS INTEGER) = 1 THEN 1
              ELSE 0
            END)::BIGINT AS champ_credit
          FROM population p
          INNER JOIN player_game_weeks g
            ON g.NFL_player_id = CAST(p.NFL_player_id AS VARCHAR)
           AND g.year = CAST(p.year AS INTEGER)
           AND g.week = CAST(p.week AS INTEGER)
          WHERE CAST(p.NFL_player_id AS VARCHAR) = ?
          GROUP BY 1, 2, 3, 4
        ),
        weekly_metric AS (
          SELECT
            p.NFL_player_id,
            p.year,
            p.week,
            p.position,
            MAX(p.active_week)::BIGINT AS active_week,
            SUM(p.rostered)::BIGINT AS rostered_leagues,
            SUM(p.started)::BIGINT AS started_leagues,
            SUM(p.valid_started_outcome)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(p.win_equivalent), 0.0) AS win_equivalent,
            ROUND(COALESCE(SUM(p.clutch_when_started), 0.0) / NULLIF(MAX(e.eligible_leagues), 0), 9)
              AS clutch_weekly_average,
            MAX(e.eligible_leagues)::BIGINT AS eligible_leagues
          FROM player_per_league_week p
          INNER JOIN eligible_by_position_week e
            ON e.position = p.position AND e.week = p.week
          GROUP BY 1, 2, 3, 4
        ),
        player_league_season AS (
          SELECT
            NFL_player_id,
            year,
            position,
            db_name,
            MAX(playoff_credit)::BIGINT AS playoff_credit,
            MAX(champ_credit)::BIGINT AS champ_credit
          FROM player_per_league_week
          GROUP BY 1, 2, 3, 4
        ),
        player_outcome_summary AS (
          SELECT
            NFL_player_id,
            year,
            position,
            SUM(playoff_credit)::BIGINT AS playoff_leagues,
            SUM(champ_credit)::BIGINT AS champ_leagues
          FROM player_league_season
          GROUP BY 1, 2, 3
        ),
        player_calendar AS (
          SELECT
            NFL_player_id,
            year,
            SUM(active_week)::BIGINT AS active_weeks,
            SUM(CASE WHEN active_week = 0 THEN 1 ELSE 0 END)::BIGINT AS inactive_weeks
          FROM player_game_weeks
          GROUP BY 1, 2
        ),
        season_metric AS (
          SELECT
            w.NFL_player_id,
            w.year,
            w.position,
            MAX(e.eligible_leagues)::BIGINT AS eligible_leagues,
            SUM(w.rostered_leagues)::BIGINT AS rostered_league_weeks,
            SUM(w.eligible_leagues)::BIGINT AS eligible_league_weeks,
            SUM(w.started_leagues)::BIGINT AS started_league_weeks,
            SUM(w.valid_started_outcomes)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(w.win_equivalent), 0.0) AS win_equivalent,
            COALESCE(SUM(w.started_leagues::DOUBLE / NULLIF(w.eligible_leagues, 0)), 0.0) AS expected_starts,
            COALESCE(SUM(
              w.started_leagues::DOUBLE / NULLIF(w.eligible_leagues, 0)
              * w.win_equivalent / NULLIF(w.valid_started_outcomes, 0)
            ), 0.0) AS expected_wins,
            COALESCE(SUM(
              w.started_leagues::DOUBLE / NULLIF(w.eligible_leagues, 0)
              * (1.0 - w.win_equivalent / NULLIF(w.valid_started_outcomes, 0))
            ), 0.0) AS expected_losses,
            COALESCE(SUM(w.clutch_weekly_average), 0.0) AS clutch_season_sum,
            SUM(CASE WHEN w.active_week = 1 THEN w.started_leagues ELSE 0 END)::BIGINT
              AS healthy_started_league_weeks,
            SUM(CASE WHEN w.active_week = 1 THEN w.eligible_leagues ELSE 0 END)::BIGINT
              AS healthy_eligible_league_weeks,
            MAX(o.playoff_leagues)::BIGINT AS playoff_leagues,
            MAX(o.champ_leagues)::BIGINT AS champ_leagues,
            MAX(c.active_weeks)::BIGINT AS active_weeks,
            MAX(c.inactive_weeks)::BIGINT AS inactive_weeks
          FROM weekly_metric w
          INNER JOIN eligible_by_position_season e ON e.position = w.position
          INNER JOIN player_outcome_summary o
            ON o.NFL_player_id = w.NFL_player_id AND o.year = w.year AND o.position = w.position
          INNER JOIN player_calendar c ON c.NFL_player_id = w.NFL_player_id AND c.year = w.year
          GROUP BY 1, 2, 3
        ),
        cell_metric AS (
          SELECT
            'ALL'::VARCHAR AS cohort_key,
            *,
            100.0 * rostered_league_weeks / NULLIF(eligible_league_weeks, 0) AS roster_rate_pct,
            100.0 * started_league_weeks / NULLIF(eligible_league_weeks, 0) AS start_rate_pct,
            100.0 * healthy_started_league_weeks / NULLIF(healthy_eligible_league_weeks, 0)
              AS healthy_start_rate_pct,
            100.0 * win_equivalent / NULLIF(valid_started_outcomes, 0) AS win_rate_pct,
            100.0 * champ_leagues / NULLIF(eligible_leagues, 0) AS champ_rate_pct,
            100.0 * playoff_leagues / NULLIF(eligible_leagues, 0) AS playoff_rate_pct
          FROM season_metric
        )
        SELECT
          NFL_player_id,
          year,
          MAX(position) AS position,
          list(struct_pack(
            cohort_key := cohort_key,
            eligible_leagues := eligible_leagues,
            rostered_league_weeks := rostered_league_weeks,
            eligible_league_weeks := eligible_league_weeks,
            started_league_weeks := started_league_weeks,
            healthy_started_league_weeks := healthy_started_league_weeks,
            healthy_eligible_league_weeks := healthy_eligible_league_weeks,
            valid_started_outcomes := valid_started_outcomes,
            win_equivalent := win_equivalent,
            roster_rate_pct := roster_rate_pct,
            start_rate_pct := start_rate_pct,
            healthy_start_rate_pct := healthy_start_rate_pct,
            win_rate_pct := win_rate_pct,
            expected_starts := expected_starts,
            expected_wins := expected_wins,
            expected_losses := expected_losses,
            clutch_season_sum := clutch_season_sum,
            playoff_leagues := playoff_leagues,
            champ_leagues := champ_leagues,
            playoff_rate_pct := playoff_rate_pct,
            champ_rate_pct := champ_rate_pct,
            active_weeks := active_weeks,
            inactive_weeks := inactive_weeks
          ) ORDER BY cohort_key) AS cohort_cells
        FROM cell_metric
        GROUP BY NFL_player_id, year
        """,
        parameters,
    )


def materialize_career_core_cohort_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    season_outer_table: str,
    output_table: str,
    player_id: str | None,
) -> None:
    """Roll annually resolved core cells into requested-key career cells.

    Pooling is decided independently in each season's stored profile.  Career
    cells are then keyed by every selectable requested cohort, so an API lookup
    never has to recalculate or guess an all-years pooling rule.
    """

    season_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {_relation_sql(season_outer_table)}").fetchall()
    }
    if "core_selector_indices" not in season_columns:
        materialize_compact_core_selector_indices(
            connection,
            output_table=season_outer_table,
        )
    seasons = _relation_sql(season_outer_table)
    output = _table_sql(output_table)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH requested_keys AS (
          SELECT
            (teams.ordinal * 72 + roster.ordinal * 24 + scoring.ordinal * 8
              + pass_td.ordinal * 4 + dynasty.ordinal * 2 + best_ball.ordinal + 1)::USMALLINT
              AS request_ordinal,
            teams.q_teams, roster.q_roster, scoring.q_scoring, pass_td.q_pass_td,
            dynasty.q_dynasty, best_ball.q_best_ball
          FROM (VALUES ('08tm', 0), ('10tm', 1), ('12tm', 2), ('14tm', 3)) AS teams(q_teams, ordinal)
          CROSS JOIN (VALUES ('flx', 0), ('sflx', 1), ('idp', 2)) AS roster(q_roster, ordinal)
          CROSS JOIN (VALUES ('std', 0), ('half', 1), ('ppr', 2)) AS scoring(q_scoring, ordinal)
          CROSS JOIN (VALUES ('4pt', 0), ('6pt', 1)) AS pass_td(q_pass_td, ordinal)
          CROSS JOIN (VALUES ('redraft', 0), ('dynasty', 1)) AS dynasty(q_dynasty, ordinal)
          CROSS JOIN (VALUES ('managed', 0), ('best_ball', 1)) AS best_ball(q_best_ball, ordinal)
        ),
        season_cells AS (
          SELECT
            CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            CAST(s.position AS VARCHAR) AS position,
            r.q_teams AS requested_teams,
            r.q_roster AS requested_roster,
            r.q_scoring AS requested_scoring,
            r.q_pass_td AS requested_pass_td,
            r.q_dynasty AS requested_dynasty,
            r.q_best_ball AS requested_best_ball,
            cell.*
          FROM {seasons} s
          CROSS JOIN requested_keys r
          CROSS JOIN UNNEST(list_value(list_extract(
            s.cohort_cells,
            list_extract(s.core_selector_indices, r.request_ordinal)
          ))) AS u(cell)
          WHERE (? IS NULL OR CAST(s.NFL_player_id AS VARCHAR) = ?)
        ),
        career_metric AS (
          SELECT
            NFL_player_id,
            MAX(position) AS position,
            requested_teams, requested_roster, requested_scoring, requested_pass_td,
            requested_dynasty, requested_best_ball,
            SUM(rostered_league_weeks)::BIGINT AS rostered_league_weeks,
            SUM(eligible_league_weeks)::BIGINT AS eligible_league_weeks,
            SUM(started_league_weeks)::BIGINT AS started_league_weeks,
            SUM(healthy_started_league_weeks)::BIGINT AS healthy_started_league_weeks,
            SUM(healthy_eligible_league_weeks)::BIGINT AS healthy_eligible_league_weeks,
            SUM(valid_started_outcomes)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(win_equivalent), 0.0) AS win_equivalent,
            AVG(100.0 * rostered_league_weeks / NULLIF(eligible_league_weeks, 0))
              AS roster_rate_pct,
            AVG(100.0 * started_league_weeks / NULLIF(eligible_league_weeks, 0))
              AS start_rate_pct,
            AVG(100.0 * healthy_started_league_weeks / NULLIF(healthy_eligible_league_weeks, 0))
              AS healthy_start_rate_pct,
            AVG(100.0 * win_equivalent / NULLIF(valid_started_outcomes, 0))
              AS win_rate_pct,
            COALESCE(SUM(expected_starts), 0.0)::DOUBLE AS expected_starts,
            COALESCE(SUM(expected_wins), 0.0)::DOUBLE AS expected_wins,
            COALESCE(SUM(expected_losses), 0.0)::DOUBLE AS expected_losses,
            ROUND(COALESCE(SUM(clutch_season_sum), 0.0), 9)::DOUBLE AS clutch_career_sum,
            COUNT(*)::BIGINT AS qualifying_seasons,
            SUM(active_weeks)::BIGINT AS active_weeks,
            SUM(inactive_weeks)::BIGINT AS inactive_weeks
          FROM season_cells
          GROUP BY NFL_player_id, requested_teams, requested_roster, requested_scoring,
            requested_pass_td, requested_dynasty, requested_best_ball
        )
        SELECT
          NFL_player_id,
          MAX(position) AS position,
          list(struct_pack(
            q_teams := requested_teams,
            q_roster := requested_roster,
            q_scoring := requested_scoring,
            q_pass_td := requested_pass_td,
            q_dynasty := requested_dynasty,
            q_best_ball := requested_best_ball,
            rostered_league_weeks := rostered_league_weeks,
            eligible_league_weeks := eligible_league_weeks,
            started_league_weeks := started_league_weeks,
            healthy_started_league_weeks := healthy_started_league_weeks,
            healthy_eligible_league_weeks := healthy_eligible_league_weeks,
            valid_started_outcomes := valid_started_outcomes,
            win_equivalent := win_equivalent,
            roster_rate_pct := roster_rate_pct,
            start_rate_pct := start_rate_pct,
            healthy_start_rate_pct := healthy_start_rate_pct,
            win_rate_pct := win_rate_pct,
            expected_starts := expected_starts,
            expected_wins := expected_wins,
            expected_losses := expected_losses,
            clutch_career_sum := clutch_career_sum,
            qualifying_seasons := qualifying_seasons,
            active_weeks := active_weeks,
            inactive_weeks := inactive_weeks
          ) ORDER BY requested_teams, requested_roster, requested_scoring, requested_pass_td,
            requested_dynasty, requested_best_ball) AS cohort_cells
        FROM career_metric
        GROUP BY NFL_player_id
        """,
        [player_id, player_id],
    )


def materialize_career_cohort_grade_cells(
    connection: duckdb.DuckDBPyConnection,
    *,
    season_outer_table: str,
    output_table: str,
    player_id: str | None,
) -> None:
    """Materialize requested-key career playoff/champ cells from season grades.

    A season's core split profile resolves its requested format before this
    aggregation.  Bracket selection is also resolved here: an exact bracket
    cell wins when that season built one, otherwise the prebuilt ``ALL`` grade
    supplies the requested cell.  The API consequently selects one immutable
    career grade cell and never performs a runtime fallback.
    """

    season_columns = {
        str(row[0]).lower()
        for row in connection.execute(f"DESCRIBE {_relation_sql(season_outer_table)}").fetchall()
    }
    if "grade_selector_indices" not in season_columns:
        materialize_compact_grade_selector_indices(
            connection,
            output_table=season_outer_table,
        )
    seasons = _relation_sql(season_outer_table)
    output = _table_sql(output_table)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH requested_keys AS (
          SELECT
            (teams.ordinal * 72 + roster.ordinal * 24 + scoring.ordinal * 8
              + pass_td.ordinal * 4 + dynasty.ordinal * 2 + best_ball.ordinal + 1)::USMALLINT
              AS core_request_ordinal,
            teams.q_teams, roster.q_roster, scoring.q_scoring, pass_td.q_pass_td,
            dynasty.q_dynasty, best_ball.q_best_ball
          FROM (VALUES ('08tm', 0), ('10tm', 1), ('12tm', 2), ('14tm', 3)) AS teams(q_teams, ordinal)
          CROSS JOIN (VALUES ('flx', 0), ('sflx', 1), ('idp', 2)) AS roster(q_roster, ordinal)
          CROSS JOIN (VALUES ('std', 0), ('half', 1), ('ppr', 2)) AS scoring(q_scoring, ordinal)
          CROSS JOIN (VALUES ('4pt', 0), ('6pt', 1)) AS pass_td(q_pass_td, ordinal)
          CROSS JOIN (VALUES ('redraft', 0), ('dynasty', 1)) AS dynasty(q_dynasty, ordinal)
          CROSS JOIN (VALUES ('managed', 0), ('best_ball', 1)) AS best_ball(q_best_ball, ordinal)
        ),
        requested_grades AS (
          SELECT k.*, b.q_playoff_teams,
            ((k.core_request_ordinal - 1) * 3 + b.ordinal + 1)::USMALLINT
              AS grade_request_ordinal
          FROM requested_keys k
          CROSS JOIN (VALUES ('4po', 0), ('6po', 1), ('8po', 2))
            AS b(q_playoff_teams, ordinal)
        ),
        season_candidates AS (
          SELECT
            CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            CAST(s.position AS VARCHAR) AS position,
            r.q_teams AS requested_teams,
            r.q_roster AS requested_roster,
            r.q_scoring AS requested_scoring,
            r.q_pass_td AS requested_pass_td,
            r.q_dynasty AS requested_dynasty,
            r.q_best_ball AS requested_best_ball,
            r.q_playoff_teams AS requested_playoff_teams,
            grade.*
          FROM {seasons} s
          CROSS JOIN requested_grades r
          CROSS JOIN UNNEST(list_value(list_extract(
            s.grade_cells,
            list_extract(s.grade_selector_indices, r.grade_request_ordinal)
          ))) AS u(grade)
          WHERE (? IS NULL OR CAST(s.NFL_player_id AS VARCHAR) = ?)
        ),
        selected_season_grade AS (SELECT * FROM season_candidates),
        career_grade AS (
          SELECT
            NFL_player_id,
            MAX(position) AS position,
            requested_teams, requested_roster, requested_scoring,
            requested_pass_td, requested_dynasty, requested_best_ball,
            requested_playoff_teams,
            SUM(eligible_leagues)::BIGINT AS eligible_league_seasons,
            SUM(playoff_leagues)::BIGINT AS playoff_credits,
            SUM(champ_leagues)::BIGINT AS champ_credits,
            AVG(playoff_rate_pct) AS playoff_rate_pct,
            AVG(champ_rate_pct) AS champ_rate_pct,
            SUM(playoff_rate_pct / 100.0) AS expected_playoffs,
            SUM(champ_rate_pct / 100.0) AS expected_champs
          FROM selected_season_grade
          GROUP BY NFL_player_id, requested_teams, requested_roster,
            requested_scoring, requested_pass_td, requested_dynasty,
            requested_best_ball, requested_playoff_teams
        )
        SELECT
          NFL_player_id,
          MAX(position) AS position,
          list(struct_pack(
            q_teams := requested_teams,
            q_roster := requested_roster,
            q_scoring := requested_scoring,
            q_pass_td := requested_pass_td,
            q_dynasty := requested_dynasty,
            q_best_ball := requested_best_ball,
            q_playoff_teams := requested_playoff_teams,
            eligible_league_seasons := eligible_league_seasons,
            playoff_credits := playoff_credits,
            champ_credits := champ_credits,
            playoff_rate_pct := playoff_rate_pct,
            champ_rate_pct := champ_rate_pct,
            expected_playoffs := expected_playoffs,
            expected_champs := expected_champs
          ) ORDER BY requested_teams, requested_roster, requested_scoring,
            requested_pass_td, requested_dynasty, requested_best_ball,
            requested_playoff_teams) AS grade_cells
        FROM career_grade
        GROUP BY NFL_player_id
        """,
        [player_id, player_id],
    )
    invalid = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM {output}, UNNEST(grade_cells) AS u(cell)
        WHERE cell.champ_credits > cell.playoff_credits
           OR cell.champ_rate_pct > cell.playoff_rate_pct
        """
    ).fetchone()[0]
    if invalid:
        raise RuntimeError("Champ credit/rate cannot exceed playoff credit/rate")


def materialize_career_outer_rows(
    connection: duckdb.DuckDBPyConnection,
    *,
    season_outer_table: str,
    output_table: str,
    player_id: str,
) -> None:
    """Roll compact season cells into one compact career record.

    Exposure/outcome rates recompute from their accumulated numerator and
    denominator counts.  Career clutch and expected totals sum season values;
    playoff and championship rates are deliberately the arithmetic mean of
    annual rates, while their expected variants sum annual probabilities.
    """

    seasons = _relation_sql(season_outer_table)
    output = _table_sql(output_table)
    connection.execute(
        f"""
        CREATE OR REPLACE TABLE {output} AS
        WITH season_cells AS (
          SELECT
            CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            CAST(s.position AS VARCHAR) AS position,
            cell.*
          FROM {seasons} s,
               UNNEST(s.cohort_cells) AS u(cell)
          WHERE CAST(s.NFL_player_id AS VARCHAR) = ?
            AND cell.cohort_key = 'ALL'
        ),
        career_metric AS (
          SELECT
            NFL_player_id,
            MAX(position) AS position,
            SUM(rostered_league_weeks)::BIGINT AS rostered_league_weeks,
            SUM(eligible_league_weeks)::BIGINT AS eligible_league_weeks,
            SUM(started_league_weeks)::BIGINT AS started_league_weeks,
            SUM(healthy_started_league_weeks)::BIGINT AS healthy_started_league_weeks,
            SUM(healthy_eligible_league_weeks)::BIGINT AS healthy_eligible_league_weeks,
            SUM(valid_started_outcomes)::BIGINT AS valid_started_outcomes,
            COALESCE(SUM(win_equivalent), 0.0) AS win_equivalent,
            COALESCE(SUM(expected_starts), 0.0)::DOUBLE AS expected_starts,
            COALESCE(SUM(expected_wins), 0.0)::DOUBLE AS expected_wins,
            COALESCE(SUM(expected_losses), 0.0)::DOUBLE AS expected_losses,
            COALESCE(SUM(clutch_season_sum), 0.0)::DOUBLE AS clutch_career_sum,
            AVG(champ_rate_pct) AS champ_rate_pct,
            COALESCE(SUM(champ_rate_pct / 100.0), 0.0)::DOUBLE AS expected_champs,
            AVG(playoff_rate_pct) AS playoff_rate_pct,
            COALESCE(SUM(playoff_rate_pct / 100.0), 0.0)::DOUBLE AS expected_playoffs,
            COUNT(*)::BIGINT AS qualifying_seasons,
            SUM(active_weeks)::BIGINT AS active_weeks,
            SUM(inactive_weeks)::BIGINT AS inactive_weeks
          FROM season_cells
          GROUP BY NFL_player_id
        ),
        cell_metric AS (
          SELECT
            'ALL'::VARCHAR AS cohort_key,
            *,
            100.0 * rostered_league_weeks / NULLIF(eligible_league_weeks, 0) AS roster_rate_pct,
            100.0 * started_league_weeks / NULLIF(eligible_league_weeks, 0) AS start_rate_pct,
            100.0 * healthy_started_league_weeks / NULLIF(healthy_eligible_league_weeks, 0)
              AS healthy_start_rate_pct,
            100.0 * win_equivalent / NULLIF(valid_started_outcomes, 0) AS win_rate_pct
          FROM career_metric
        )
        SELECT
          NFL_player_id,
          MAX(position) AS position,
          list(struct_pack(
            cohort_key := cohort_key,
            rostered_league_weeks := rostered_league_weeks,
            eligible_league_weeks := eligible_league_weeks,
            started_league_weeks := started_league_weeks,
            healthy_started_league_weeks := healthy_started_league_weeks,
            healthy_eligible_league_weeks := healthy_eligible_league_weeks,
            valid_started_outcomes := valid_started_outcomes,
            win_equivalent := win_equivalent,
            roster_rate_pct := roster_rate_pct,
            start_rate_pct := start_rate_pct,
            healthy_start_rate_pct := healthy_start_rate_pct,
            win_rate_pct := win_rate_pct,
            expected_starts := expected_starts,
            expected_wins := expected_wins,
            expected_losses := expected_losses,
            clutch_career_sum := clutch_career_sum,
            champ_rate_pct := champ_rate_pct,
            expected_champs := expected_champs,
            playoff_rate_pct := playoff_rate_pct,
            expected_playoffs := expected_playoffs,
            qualifying_seasons := qualifying_seasons,
            active_weeks := active_weeks,
            inactive_weeks := inactive_weeks
          ) ORDER BY cohort_key) AS cohort_cells
        FROM cell_metric
        GROUP BY NFL_player_id
        """,
        [player_id],
    )
