"""Materialize the one canonical research player schema.

This is intentionally a build-time operation.  Readers never choose between
native and backfill columns: the resolved values are written once, and the
parallel BF fields are removed from the output table.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


FORBIDDEN = {"is_playoffs_bf", "made_po_bf", "made_po"}
COHORT_COLUMNS = (
    "cohort_teams",
    "cohort_roster",
    "cohort_scoring",
    "cohort_pass_td",
    "cohort_playoff_teams",
    "cohort_dynasty",
    "cohort_best_ball",
)
COHORT_SUPPORT_COLUMNS = ("cohort_position_eligible",)
TEAM_COHORT_LABELS = ("08tm", "10tm", "12tm", "14tm")
DERIVED_AVAILABILITY_TABLES = ("player_active_week", "player_team_game_week")


def qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def canonical_position_sql(value: str) -> str:
    """Return the cache's canonical research-position expression.

    The player-ID position map is supplied by the caller.  Defensive player
    labels deliberately collapse to the lineup-denominator families used by
    the research table: DB, DL, and LB.
    """
    normalized = f"UPPER(TRIM(CAST({value} AS VARCHAR)))"
    return f"""
        CASE
          WHEN {normalized} IN ('CB', 'DB', 'FS', 'S', 'SAF', 'SS') THEN 'DB'
          WHEN {normalized} IN ('DE', 'DL', 'DT', 'EDGE', 'NT') THEN 'DL'
          WHEN {normalized} IN ('ILB', 'LB', 'MLB', 'OLB') THEN 'LB'
          WHEN {normalized} = 'PK' THEN 'K'
          WHEN {normalized} IN ('D/ST', 'DST') THEN 'DEF'
          ELSE {normalized}
        END
    """


def inspect_missing_team_count_league_years(
    con: duckdb.DuckDBPyConnection,
    schema: str = "public",
) -> list[dict[str, object]]:
    """Return evidence for represented league-years lacking a literal team count.

    This is deliberately read-only inventory logic.  It identifies whether the
    issue is a missing settings row or a null setting and exposes the distinct
    observed fantasy team keys without assigning a cohort by inference.
    """
    player_cols = {row[0] for row in con.execute(
        f"DESCRIBE {qi(schema)}.player_fantasy"
    ).fetchall()}
    settings_cols = {row[0] for row in con.execute(
        f"DESCRIBE {qi(schema)}.league_settings"
    ).fetchall()}
    if "num_teams" not in settings_cols:
        raise SystemExit("league_settings has no num_teams column")
    team_key_expr = (
        "COUNT(DISTINCT NULLIF(TRIM(CAST(team_key AS VARCHAR)), ''))::BIGINT"
        if "team_key" in player_cols else "NULL::BIGINT"
    )
    platform_expr = (
        "MAX(NULLIF(TRIM(CAST(platform AS VARCHAR)), ''))"
        if "platform" in settings_cols else "NULL::VARCHAR"
    )
    rows = con.execute(f"""
        WITH player_lanes AS (
          SELECT CAST(db_name AS VARCHAR) AS db_name,
                 CAST(year AS INTEGER) AS year,
                 COUNT(*)::BIGINT AS player_rows,
                 {team_key_expr} AS distinct_team_keys
          FROM {qi(schema)}.player_fantasy
          GROUP BY 1, 2
        ), settings_lanes AS (
          SELECT CAST(db_name AS VARCHAR) AS db_name,
                 CAST(year AS INTEGER) AS year,
                 COUNT(*)::BIGINT AS settings_rows,
                 {platform_expr} AS platform,
                 MAX(CAST(num_teams AS INTEGER)) AS num_teams
          FROM {qi(schema)}.league_settings
          GROUP BY 1, 2
        )
        SELECT p.db_name, p.year,
               COALESCE(s.settings_rows, 0)::BIGINT AS settings_rows,
               s.platform, s.num_teams, p.player_rows, p.distinct_team_keys
        FROM player_lanes p
        LEFT JOIN settings_lanes s USING (db_name, year)
        WHERE s.num_teams IS NULL
        ORDER BY p.year, p.db_name
    """).fetchall()
    keys = (
        "db_name", "year", "settings_rows", "platform", "num_teams",
        "player_rows", "distinct_team_keys",
    )
    return [dict(zip(keys, row)) for row in rows]


def audit_team_cohort_allocation(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """Fail closed unless every represented league-year has one literal team tier.

    This runs immediately after canonical fanout.  It checks the immutable
    cache's materialized player rows against their one source
    ``league_settings.num_teams`` value; positions, roster construction, and
    roster/start events are deliberately absent from this comparison.
    """

    lanes = con.execute("""
        WITH player_lanes AS (
          SELECT
            CAST(db_name AS VARCHAR) AS db_name,
            CAST(year AS INTEGER) AS year,
            MIN(CAST(cohort_teams AS VARCHAR)) AS min_team_label,
            MAX(CAST(cohort_teams AS VARCHAR)) AS max_team_label,
            COUNT(DISTINCT CAST(cohort_teams AS VARCHAR)) AS team_label_count
          FROM public.player_fantasy
          GROUP BY 1, 2
        ), resolved AS (
          SELECT
            p.*,
            s.num_teams,
            CASE
              WHEN p.year BETWEEN 2003 AND 2010 THEN 'ALL'
              -- Preserve an unknown-size league in the all-leagues pool.
              -- A specific team tier without a factual team count would be
              -- fabricated cohort evidence.
              WHEN s.num_teams IS NULL THEN 'ALL'
              WHEN s.num_teams <= 9 THEN '08tm'
              WHEN s.num_teams <= 11 THEN '10tm'
              WHEN s.num_teams <= 13 THEN '12tm'
              ELSE '14tm'
            END AS expected_team_label
          FROM player_lanes p
          LEFT JOIN public.league_settings s
            ON s.db_name = p.db_name AND CAST(s.year AS INTEGER) = p.year
        )
        SELECT
          COUNT(*)::BIGINT AS represented_league_years,
          COUNT(*) FILTER (WHERE year BETWEEN 2003 AND 2010)::BIGINT AS historical_league_years,
          COUNT(*) FILTER (WHERE year NOT BETWEEN 2003 AND 2010)::BIGINT AS non_historical_league_years,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010 AND num_teams IS NOT NULL
          )::BIGINT AS known_team_count_league_years,
          COUNT(*) FILTER (
            WHERE expected_team_label IS NULL
               OR team_label_count <> 1
               OR min_team_label <> expected_team_label
               OR max_team_label <> expected_team_label
          )::BIGINT AS invalid_league_years,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010 AND expected_team_label = '08tm'
          )::BIGINT AS teams_08tm,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010 AND expected_team_label = '10tm'
          )::BIGINT AS teams_10tm,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010 AND expected_team_label = '12tm'
          )::BIGINT AS teams_12tm,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010 AND expected_team_label = '14tm'
          )::BIGINT AS teams_14tm,
          COUNT(*) FILTER (
            WHERE year NOT BETWEEN 2003 AND 2010 AND num_teams IS NULL
          )::BIGINT AS missing_team_count_league_years
        FROM resolved
    """).fetchone()
    (
        represented, historical, non_historical, known_team_count, invalid,
        teams_08, teams_10, teams_12, teams_14, missing_team_count,
    ) = (int(value or 0) for value in lanes)
    buckets = {
        "08tm": teams_08,
        "10tm": teams_10,
        "12tm": teams_12,
        "14tm": teams_14,
    }
    ten_twelve = teams_10 + teams_12
    ten_twelve_share = ten_twelve / known_team_count if known_team_count else None
    result: dict[str, object] = {
        "represented_league_years": represented,
        "historical_league_years": historical,
        "non_historical_league_years": non_historical,
        "known_team_count_league_years": known_team_count,
        "invalid_league_years": invalid,
        "unknown_team_count_league_years": missing_team_count,
        "team_buckets": buckets,
        "ten_twelve_league_years": ten_twelve,
        "ten_twelve_share": ten_twelve_share,
    }
    if invalid:
        raise SystemExit(
            "team cohort allocation failed: "
            f"invalid_league_years={invalid} "
            f"unknown_team_count_league_years={missing_team_count}"
        )
    # "Mostly 10/12" is a transparent population invariant: more than half
    # of non-historic league-years with a factual team count must be in those
    # literal tiers.  Unknown sizes remain pooled, never fabricated.
    if ten_twelve_share is not None and ten_twelve_share <= 0.5:
        raise SystemExit(
            "team cohort allocation failed: 10tm+12tm are not a majority of "
            f"known-size non-historical league-years (share={ten_twelve_share:.4f})"
        )
    return result


def tables(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name='base' AND schema_name='public'"
    ).fetchall()]


def normalize(base: Path, out: Path, ops_cache: Path | None = None) -> None:
    if out.exists():
        out.unlink()
    con = duckdb.connect(str(out))
    con.execute(f"ATTACH '{base.resolve().as_posix().replace(chr(39), chr(39)*2)}' AS base (READ_ONLY)")
    if ops_cache is not None:
        con.execute(f"ATTACH '{ops_cache.resolve().as_posix().replace(chr(39), chr(39)*2)}' AS research_ops (READ_ONLY)")
    base_tables = tables(con)
    if "player_fantasy" not in base_tables:
        raise SystemExit("base cache has no public.player_fantasy")
    if "league_settings" not in base_tables:
        raise SystemExit("base cache has no public.league_settings")

    con.execute("CREATE SCHEMA public")
    for table in base_tables:
        # These two lookups are rebuilt from the supplied ops cache below.
        # Do not copy a prior materialization first: a schema-only cache update
        # legitimately starts from a cache that already contains them.
        if table == "player_fantasy" or (ops_cache is not None and table in DERIVED_AVAILABILITY_TABLES):
            continue
        con.execute(f"CREATE TABLE public.{qi(table)} AS SELECT * FROM base.public.{qi(table)}")

    pcols = [r[0] for r in con.execute("DESCRIBE base.public.player_fantasy").fetchall()]
    if "is_playoffs" not in pcols and "is_playoffs_bf" not in pcols:
        raise SystemExit("player_fantasy has no playoff signal to resolve")
    has_bf_week = "is_playoffs_bf" in pcols
    has_bf_team = "made_po_bf" in pcols
    has_seed = "final_playoff_seed" in pcols
    settings_cols = {r[0] for r in con.execute("DESCRIBE base.public.league_settings").fetchall()}
    if "platform" not in settings_cols:
        raise SystemExit("league_settings has no platform column; cannot materialize canonical platform")
    bracket = "s.playoff_teams" if "playoff_teams" in settings_cols else "NULL::INTEGER"
    position_join = ""
    source_position_expr = "p.position"
    if ops_cache is not None and "position" in pcols:
        bio_cols = {r[0] for r in con.execute(
            "DESCRIBE research_ops.nfl_historical.player_bio"
        ).fetchall()}
        bio_position = "nfl_position" if "nfl_position" in bio_cols else "position" if "position" in bio_cols else None
        if "NFL_player_id" in bio_cols and bio_position:
            # player_bio is the compact canonical identity/position cache. It
            # avoids scanning and regrouping the full 350-column weekly table.
            position_join = f"""
            LEFT JOIN (
                SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                       MODE(NULLIF(TRIM(CAST({bio_position} AS VARCHAR)), '')) AS position
                FROM research_ops.nfl_historical.player_bio
                GROUP BY 1
            ) ap ON CAST(p.NFL_player_id AS VARCHAR)=ap.NFL_player_id
            """
            source_position_expr = "COALESCE(ap.position, p.position)"
        else:
            ops_cols = {r[0] for r in con.execute(
                "DESCRIBE research_ops.nfl_historical.nfl_player_stats_all"
            ).fetchall()}
            if not {"NFL_player_id", "year", "week"} <= ops_cols:
                raise SystemExit(
                    "ops cache cannot resolve player positions: missing identity columns "
                    f"{sorted({ 'NFL_player_id', 'year', 'week' } - ops_cols)}"
                )
            if "position" in ops_cols and "nfl_position" in ops_cols:
                ops_position = "COALESCE(position, nfl_position)"
            elif "position" in ops_cols:
                ops_position = "position"
            elif "nfl_position" in ops_cols:
                ops_position = "nfl_position"
            else:
                raise SystemExit("ops cache cannot resolve player positions: missing position/nfl_position")
            season_type_filter = (
                "AND COALESCE(season_type, 'REG') = 'REG'"
                if "season_type" in ops_cols else ""
            )
            position_join = f"""
            LEFT JOIN (
                SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                       CAST(year AS INTEGER) AS year,
                       CAST(week AS INTEGER) AS week,
                       MODE(CAST({ops_position} AS VARCHAR)) AS position
                FROM research_ops.nfl_historical.nfl_player_stats_all
                WHERE week IS NOT NULL {season_type_filter}
                GROUP BY 1,2,3
            ) ap ON CAST(p.NFL_player_id AS VARCHAR)=ap.NFL_player_id
                AND CAST(p.year AS INTEGER)=ap.year
                AND CAST(p.week AS INTEGER)=ap.week
            """
            source_position_expr = "COALESCE(ap.position, p.position)"

    position_expr = canonical_position_sql(source_position_expr)

    # These are row-level cohort labels, not an aggregate and not a second lineage.
    # They are propagated from the unique league-year settings row so every player row
    # can be grouped without repeatedly reinterpreting settings at read time.
    required_settings = {
        "num_teams", "scoring_rec", "scoring_pass_td", "playoff_teams",
        "is_dynasty", "sleeper_best_ball",
    }
    missing_settings = sorted(required_settings - settings_cols)
    if missing_settings:
        raise SystemExit(
            "league_settings cannot materialize the seven cohort dimensions; "
            f"missing columns={missing_settings}"
        )
    roster_columns = {
        "roster_IDP", "roster_DL", "roster_LB", "roster_DB", "roster_DB_LB",
        "roster_DL_LB", "roster_SUPER_FLEX", "roster_K", "roster_DEF",
    }
    missing_roster = sorted(roster_columns - settings_cols)
    if missing_roster:
        raise SystemExit(
            "league_settings cannot materialize roster structure; "
            f"missing columns={missing_roster}"
        )
    def roster_slot(name: str) -> str:
        return f"COALESCE(s.{qi(name)},0)" if name in settings_cols else "0"

    rb_slots = " + ".join(roster_slot(x) for x in ("roster_RB", "roster_FLX", "roster_REC_FLEX", "roster_SUPER_FLEX", "roster_R/T"))
    wr_slots = " + ".join(roster_slot(x) for x in ("roster_WR", "roster_FLX", "roster_REC_FLEX", "roster_SUPER_FLEX", "roster_W/R"))
    te_slots = " + ".join(roster_slot(x) for x in ("roster_TE", "roster_FLX", "roster_REC_FLEX", "roster_SUPER_FLEX", "roster_W/R", "roster_R/T"))
    qb_slots = " + ".join(roster_slot(x) for x in ("roster_QB", "roster_SUPER_FLEX"))
    duplicate_settings = con.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT db_name, CAST(year AS INTEGER) AS year
            FROM base.public.league_settings
            GROUP BY 1, 2
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]
    if duplicate_settings:
        raise SystemExit(
            "league_settings is not unique at (db_name, year); "
            f"duplicate league-years={duplicate_settings}"
        )
    cohort_exprs = {
        # Literal league size is its own cohort axis.  Neighbouring uncommon
        # sizes are binned to the nearest supported 8/10/12/14 label; roster
        # construction never participates in this classification.
        "cohort_teams": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                        "WHEN s.num_teams IS NULL THEN 'ALL' "
                        "WHEN s.num_teams <= 9 THEN '08tm' "
                        "WHEN s.num_teams <= 11 THEN '10tm' "
                        "WHEN s.num_teams <= 13 THEN '12tm' ELSE '14tm' END",
        "cohort_roster": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                         "WHEN COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL,0)+COALESCE(s.roster_LB,0) "
                         "+COALESCE(s.roster_DB,0)+COALESCE(s.roster_DB_LB,0)+COALESCE(s.roster_DL_LB,0) > 0 THEN 'idp' "
                         "WHEN COALESCE(s.roster_SUPER_FLEX,0) > 0 THEN 'sflx' ELSE 'flx' END",
        "cohort_scoring": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                          "WHEN COALESCE(s.scoring_rec,0) = 0 THEN 'std' "
                          "WHEN COALESCE(s.scoring_rec,0) < 0.75 THEN 'half' ELSE 'ppr' END",
        "cohort_pass_td": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                          "WHEN COALESCE(s.scoring_pass_td,4) >= 5 THEN '6pt' ELSE '4pt' END",
        "cohort_playoff_teams": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                                "WHEN s.playoff_teams IS NULL THEN 'ALL' "
                                "WHEN s.playoff_teams <= 5 THEN '4po' "
                                "WHEN s.playoff_teams <= 7 THEN '6po' ELSE '8po' END",
        "cohort_dynasty": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                          "WHEN COALESCE(s.is_dynasty, false) THEN 'dynasty' ELSE 'redraft' END",
        "cohort_best_ball": "CASE WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 'ALL' "
                            "WHEN COALESCE(s.sleeper_best_ball, false) THEN 'best_ball' ELSE 'managed' END",
        # The player row carries the position, so materialize the position-slot
        # denominator eligibility beside the seven cohort labels.  This removes
        # the last settings lookup from the rollup while preserving K/DEF and IDP
        # eligibility, which cannot be inferred from the format label alone.
        "cohort_position_eligible": "CASE "
                                    "WHEN CAST(p.year AS INTEGER) BETWEEN 2003 AND 2010 THEN 1 "
                                    "WHEN UPPER(TRIM(CAST({position_expr} AS VARCHAR))) IN ('QB','RB','WR','TE') THEN 1 "
                                    "WHEN UPPER(TRIM(CAST({position_expr} AS VARCHAR))) IN ('K','PK') "
                                    "     AND COALESCE(s.roster_K,0)>0 THEN 1 "
                                    "WHEN UPPER(TRIM(CAST({position_expr} AS VARCHAR))) IN ('DEF','DST','D/ST') "
                                    "     AND COALESCE(s.roster_DEF,0)>0 THEN 1 "
                                    "WHEN UPPER(TRIM(CAST({position_expr} AS VARCHAR))) = 'DB' "
                                    "     AND (COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DB,0)+COALESCE(s.roster_DB_LB,0)>0) THEN 1 "
                                    "WHEN UPPER(TRIM(CAST({position_expr} AS VARCHAR))) = 'DL' "
                                    "     AND (COALESCE(s.roster_IDP,0)+COALESCE(s.roster_DL,0)+COALESCE(s.roster_DL_LB,0)>0) THEN 1 "
                                    "WHEN UPPER(TRIM(CAST({position_expr} AS VARCHAR))) = 'LB' "
                                    "     AND (COALESCE(s.roster_IDP,0)+COALESCE(s.roster_LB,0)+COALESCE(s.roster_DB_LB,0)+COALESCE(s.roster_DL_LB,0)>0) THEN 1 "
                                    "ELSE 0 END",
    }
    cohort_exprs = {
        column: expr.format(position_expr=position_expr, qb_slots=qb_slots, rb_slots=rb_slots, wr_slots=wr_slots, te_slots=te_slots)
        for column, expr in cohort_exprs.items()
    }
    resolved_week = (
        "CASE WHEN p.is_playoffs IS NOT NULL THEN CAST(p.is_playoffs AS INTEGER) "
        "ELSE CAST(p.is_playoffs_bf AS INTEGER) END"
        if has_bf_week and "is_playoffs" in pcols else
        "CAST(p.is_playoffs AS INTEGER)" if "is_playoffs" in pcols else
        "CAST(p.is_playoffs_bf AS INTEGER)"
    )

    select: list[str] = []
    for column in pcols:
        if column in FORBIDDEN or column == "made_playoffs" or column in COHORT_COLUMNS or column in COHORT_SUPPORT_COLUMNS:
            continue
        if column == "is_playoffs":
            if has_bf_week:
                select.append(
                    f"{resolved_week} AS is_playoffs"
                )
            else:
                select.append("CAST(p.is_playoffs AS INTEGER) AS is_playoffs")
        elif column == "platform":
            select.append(
                "COALESCE(NULLIF(LOWER(TRIM(CAST(p.platform AS VARCHAR))), ''), "
                "NULLIF(LOWER(TRIM(CAST(s.platform AS VARCHAR))), '')) AS platform"
            )
        elif column == "position" and position_join:
            select.append(f"{position_expr} AS {qi(column)}")
        else:
            select.append(f"p.{qi(column)} AS {qi(column)}")
    select.extend(
        f"{expr} AS {qi(column)}" for column, expr in cohort_exprs.items()
    )
    if "is_playoffs" not in pcols:
        select.append("CAST(p.is_playoffs_bf AS INTEGER) AS is_playoffs")
    platform_fallback = (
        "CASE "
        "WHEN LOWER(CAST(p.db_name AS VARCHAR)) LIKE 'smpl_yahoo%' THEN 'yahoo' "
        "WHEN LOWER(CAST(p.db_name AS VARCHAR)) LIKE 'smpl_mfl%' THEN 'mfl' "
        "WHEN LOWER(CAST(p.db_name AS VARCHAR)) LIKE 'smpl_ffl%' THEN 'fleaflicker' "
        "ELSE NULL END"
    )
    if "platform" not in pcols:
        select.append(
            "COALESCE(NULLIF(LOWER(TRIM(CAST(s.platform AS VARCHAR))), ''), "
            f"{platform_fallback}) AS platform"
        )
    else:
        # The player fold has a platform column, but older rows may be null.
        # Fill only from settings or an unambiguous source-ID prefix.
        for i, expr in enumerate(select):
            if expr == 'p."platform" AS "platform"':
                select[i] = (
                    "COALESCE(NULLIF(LOWER(TRIM(CAST(p.platform AS VARCHAR))), ''), "
                    "NULLIF(LOWER(TRIM(CAST(s.platform AS VARCHAR))), ''), "
                    f"{platform_fallback}) AS platform"
                )
                break

    # Team-season qualification is distinct from a playoff week.  Resolve it
    # once using the strongest available evidence, then store only this field.
    seed_expr = (
        f"CASE WHEN p.final_playoff_seed IS NOT NULL AND {bracket} IS NOT NULL "
        f"THEN CAST(p.final_playoff_seed AS INTEGER) BETWEEN 1 AND CAST({bracket} AS INTEGER) END"
        if has_seed else "NULL::BOOLEAN"
    )
    bf_team_expr = "CAST(p.made_po_bf AS INTEGER)" if has_bf_team else "NULL::INTEGER"
    select.append(
        f"CASE WHEN {bf_team_expr} IS NOT NULL THEN {bf_team_expr} "
        f"WHEN {seed_expr} IS NOT NULL THEN CAST(({seed_expr}) AS INTEGER) "
        f"WHEN ({resolved_week})=1 THEN 1 ELSE NULL END AS made_playoffs"
    )
    con.execute(f"""
        CREATE TABLE public.player_fantasy AS
        SELECT {', '.join(select)}
        FROM base.public.player_fantasy p
        {position_join}
        LEFT JOIN base.public.league_settings s
          ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
        WHERE p.db_name IS NOT NULL
    """)

    # Keep the two narrow availability lookups in the canonical GitHub cache.
    # Release rollups must not rebuild these relationships from the 350-column
    # weekly supertable on every cohort slice.
    if ops_cache is not None:
        ops_cols = {r[0] for r in con.execute(
            "DESCRIBE research_ops.nfl_historical.nfl_player_stats_all"
        ).fetchall()}
        if {"NFL_player_id", "year", "week"} <= ops_cols:
            reg_filter = (
                "AND COALESCE(season_type, 'REG') = 'REG'"
                if "season_type" in ops_cols else ""
            )
            con.execute("""
              CREATE TABLE public.player_active_week AS
              SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                     CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week
              FROM research_ops.nfl_historical.nfl_player_stats_all
              WHERE NFL_player_id IS NOT NULL AND week IS NOT NULL
            """ + reg_filter)
            if "nfl_team" in ops_cols:
                con.execute("""
                  CREATE TABLE public.player_team_game_week AS
                  WITH player_team AS (
                    SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
                           CAST(year AS INTEGER) AS year,
                           MODE(NULLIF(TRIM(CAST(nfl_team AS VARCHAR)), '')) AS nfl_team
                    FROM research_ops.nfl_historical.nfl_player_stats_all
                    WHERE NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL
                      AND week IS NOT NULL
                """ + reg_filter + """
                    GROUP BY 1,2
                  ), team_week AS (
                    SELECT DISTINCT CAST(year AS INTEGER) AS year,
                           CAST(week AS INTEGER) AS week,
                           NULLIF(TRIM(CAST(nfl_team AS VARCHAR)), '') AS nfl_team
                    FROM research_ops.nfl_historical.nfl_player_stats_all
                    WHERE nfl_team IS NOT NULL AND week IS NOT NULL
                """ + reg_filter + """
                  )
                  SELECT DISTINCT pt.NFL_player_id, pt.year, tw.week
                  FROM player_team pt
                  JOIN team_week tw USING (year, nfl_team)
                """)

    output_cols = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    forbidden = FORBIDDEN & output_cols
    required = {"is_playoffs", "made_playoffs"} - output_cols
    missing_cohort_columns = (set(COHORT_COLUMNS) | set(COHORT_SUPPORT_COLUMNS)) - output_cols
    null_db = con.execute(
        "SELECT COUNT(*) FROM public.player_fantasy WHERE db_name IS NULL"
    ).fetchone()[0]
    platform_null = con.execute(
        "SELECT COUNT(*) FROM public.player_fantasy WHERE platform IS NULL OR TRIM(CAST(platform AS VARCHAR))=''"
    ).fetchone()[0]
    cohort_nulls = con.execute(
        "SELECT COUNT(*) FROM public.player_fantasy WHERE "
        + " OR ".join(f"{qi(column)} IS NULL" for column in (COHORT_COLUMNS + COHORT_SUPPORT_COLUMNS))
    ).fetchone()[0]
    if forbidden or required or missing_cohort_columns or null_db or platform_null or cohort_nulls:
        unresolved = con.execute(
            "SELECT CAST(p.db_name AS VARCHAR), CAST(p.year AS INTEGER), "
            "CAST(p.team_key AS VARCHAR), CAST(p.team_name AS VARCHAR), "
            "CAST(p.platform AS VARCHAR), COUNT(*) "
            "FROM public.player_fantasy p "
            "WHERE p.platform IS NULL OR TRIM(CAST(p.platform AS VARCHAR))='' "
            "GROUP BY 1,2,3,4,5 ORDER BY 6 DESC LIMIT 20"
        ).fetchall()
        raise SystemExit(
            f"canonical schema failed: forbidden={sorted(forbidden)} "
            f"missing={sorted(required | missing_cohort_columns)} null_db={null_db} "
            f"platform_null={platform_null} cohort_null_rows={cohort_nulls} "
            f"unresolved_sample={unresolved}"
        )
    team_allocation = audit_team_cohort_allocation(con)
    con.close()
    print({
        "tables": len(base_tables),
        "player_columns": len(output_cols),
        "player_rows": _count(out),
        "team_allocation": team_allocation,
    })


def _count(path: Path) -> int:
    con = duckdb.connect(str(path), read_only=True)
    value = con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0]
    con.close()
    return int(value)


def audit_team_count_input(path: Path) -> dict[str, object]:
    """Inspect a restored cache without materializing or changing it."""
    con = duckdb.connect(str(path), read_only=True)
    result = {
        "cache": str(path),
        "missing_team_count_league_years": inspect_missing_team_count_league_years(con),
    }
    con.close()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--ops-cache", type=Path)
    parser.add_argument(
        "--audit-team-count-input",
        type=Path,
        help="Read-only inventory of represented league-years lacking league_settings.num_teams",
    )
    args = parser.parse_args()
    if args.audit_team_count_input is not None:
        if args.base is not None or args.out is not None or args.ops_cache is not None:
            parser.error("--audit-team-count-input cannot be combined with --base/--out/--ops-cache")
        print(audit_team_count_input(args.audit_team_count_input))
        return
    if args.base is None or args.out is None:
        parser.error("--base and --out are required unless --audit-team-count-input is used")
    normalize(args.base, args.out, args.ops_cache)


if __name__ == "__main__":
    main()
