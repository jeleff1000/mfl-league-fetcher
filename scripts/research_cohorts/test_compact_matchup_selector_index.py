import duckdb

from scripts.research_cohorts.compact_matchup_cells import (
    _compact_season_grade_cells,
    _materialize_season_grade_requested_layouts,
    materialize_compact_core_selector_indices,
    materialize_compact_exact_core_selector_indices,
    materialize_compact_exact_grade_selector_indices,
    materialize_compact_grade_sidecar,
    materialize_compact_grade_selector_indices,
    materialize_career_core_cohort_cells,
    materialize_career_cohort_grade_cells,
)


def test_season_grade_request_resolution_is_shared_by_identical_outer_layouts():
    """The 864-request fallback map is resolved once per cell layout, not player.

    Two outer player rows with identical complete grade lattices must produce
    one resolved layout.  This is the boundary that prevents a full player
    batch from cross joining every player against every UI request.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season_grade AS
          SELECT id AS NFL_player_id, 2025 AS year, 'RB' AS position,
            list_value(
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                q_playoff_teams := 'ALL', eligible_leagues := 1000::BIGINT,
                playoff_leagues := 500::BIGINT, champ_leagues := 100::BIGINT,
                playoff_rate_pct := 50.0, champ_rate_pct := 10.0
              ),
              struct_pack(
                q_teams := '12tm', q_roster := 'flx', q_scoring := 'half',
                q_pass_td := '4pt', q_dynasty := 'redraft', q_best_ball := 'managed',
                q_playoff_teams := '6po', eligible_leagues := 200::BIGINT,
                playoff_leagues := 100::BIGINT, champ_leagues := 20::BIGINT,
                playoff_rate_pct := 50.0, champ_rate_pct := 10.0
              )
            ) AS grade_cells
          FROM (VALUES ('p1'), ('p2')) players(id)
        """)

        _materialize_season_grade_requested_layouts(
            con,
            source_table="season_grade",
            output_table="resolved_layouts",
            split_threshold=150,
        )

        assert con.execute("SELECT COUNT(*) FROM resolved_layouts").fetchone() == (1,)
        assert con.execute("""
          SELECT list_count(grade_cell_indices),
            list_extract(grade_cell_indices, 1),
            list_extract(grade_cell_indices, 2)
          FROM resolved_layouts
        """).fetchone() == (2, 2, 1)

        _compact_season_grade_cells(
            con,
            output_table="season_grade",
            split_threshold=150,
        )
        assert con.execute("""
          SELECT COUNT(*), MIN(list_count(grade_cells)), MAX(list_count(grade_cells))
          FROM season_grade
        """).fetchone() == (2, 2, 2)
    finally:
        con.close()


def test_compact_grade_sidecar_preserves_one_value_per_requested_grade_selector():
    """The serving sidecar must be a row-for-row numeric projection of grades.

    A request resolves an ordinal through ``grade_selector_indices``.  The
    sidecar keeps that lookup O(1) without retaining 864 structs in the hot
    query, and it must preserve the grade eligibility denominator as well.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season AS
          WITH grades AS (
            SELECT list(
              struct_pack(
                playoff_rate_pct := CAST(ordinal AS DOUBLE),
                champ_rate_pct := CAST(ordinal * 2 AS DOUBLE),
                eligible_leagues := CAST(ordinal * 3 AS BIGINT)
              ) ORDER BY ordinal
            ) AS grade_cells
            FROM range(1, 865) ordinals(ordinal)
          )
          SELECT
            '00-0000001' AS NFL_player_id,
            2025 AS year,
            'RB' AS position,
            grade_cells,
            range(1, 865) AS grade_selector_indices
          FROM grades
        """)

        materialize_compact_grade_sidecar(
            con,
            source_table="season",
            output_table="grade_season",
            grain="season",
        )

        assert con.execute("""
          SELECT
            list_count(playoff_rate_values),
            list_extract(playoff_rate_values, 323),
            list_extract(champ_rate_values, 323),
            list_extract(eligible_values, 323)
          FROM grade_season
        """).fetchone() == (864, 323.0, 646.0, 969)
    finally:
        con.close()


def _core_cells_sql(profile: str) -> str:
    return f"""
      WITH requested AS (
        SELECT * FROM (VALUES
          ('08tm'), ('10tm'), ('12tm'), ('14tm')
        ) teams(q_teams)
        CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) roster(q_roster)
        CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) scoring(q_scoring)
        CROSS JOIN (VALUES ('4pt'), ('6pt')) pass_td(q_pass_td)
        CROSS JOIN (VALUES ('redraft'), ('dynasty')) dynasty(q_dynasty)
        CROSS JOIN (VALUES ('managed'), ('best_ball')) best_ball(q_best_ball)
      ), resolved AS (
        SELECT DISTINCT
          CASE
            WHEN q_teams='08tm' AND {profile}.teams_08tm_split = 1 THEN q_teams
            WHEN q_teams='10tm' AND {profile}.teams_10tm_split = 1 THEN q_teams
            WHEN q_teams='12tm' AND {profile}.teams_12tm_split = 1 THEN q_teams
            WHEN q_teams='14tm' AND {profile}.teams_14tm_split = 1 THEN q_teams
            ELSE 'ALL'
          END AS q_teams,
          CASE WHEN {profile}.roster_flx_split = 1 THEN q_roster ELSE 'ALL' END AS q_roster,
          CASE WHEN {profile}.scoring_std_split = 1 THEN q_scoring ELSE 'ALL' END AS q_scoring,
          CASE WHEN {profile}.pass_td_4pt_split = 1 THEN q_pass_td ELSE 'ALL' END AS q_pass_td,
          CASE WHEN {profile}.dynasty_redraft_split = 1 THEN q_dynasty ELSE 'ALL' END AS q_dynasty,
          CASE WHEN {profile}.best_ball_managed_split = 1 THEN q_best_ball ELSE 'ALL' END AS q_best_ball
        FROM requested
      )
      SELECT list(struct_pack(
        q_teams := q_teams, q_roster := q_roster, q_scoring := q_scoring,
        q_pass_td := q_pass_td, q_dynasty := q_dynasty, q_best_ball := q_best_ball,
        marker := q_teams || ':' || q_roster || ':' || q_scoring || ':' || q_pass_td || ':' || q_dynasty || ':' || q_best_ball
      ) ORDER BY q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball)
      FROM resolved
    """


def test_core_selector_indices_resolve_every_request_without_expanding_outer_rows():
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE compact AS
          WITH profiles AS (
            SELECT 1 AS id, struct_pack(
              teams_08tm_split := 0, teams_10tm_split := 0,
              teams_12tm_split := 0, teams_14tm_split := 0,
              roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
              scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
              pass_td_4pt_split := 0, pass_td_6pt_split := 0,
              dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
              best_ball_managed_split := 0, best_ball_best_ball_split := 0
            ) AS cohort_profile
            UNION ALL
            SELECT 2 AS id, struct_pack(
              teams_08tm_split := 1, teams_10tm_split := 1,
              teams_12tm_split := 1, teams_14tm_split := 1,
              roster_flx_split := 1, roster_sflx_split := 1, roster_idp_split := 1,
              scoring_std_split := 1, scoring_half_split := 1, scoring_ppr_split := 1,
              pass_td_4pt_split := 1, pass_td_6pt_split := 1,
              dynasty_redraft_split := 1, dynasty_dynasty_split := 1,
              best_ball_managed_split := 1, best_ball_best_ball_split := 1
            ) AS cohort_profile
          )
          SELECT id, cohort_profile,
            CASE WHEN id = 1 THEN (
              SELECT list(struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'ALL'))
            ) ELSE (
              SELECT list(struct_pack(
                q_teams := q_teams, q_roster := q_roster, q_scoring := q_scoring,
                q_pass_td := q_pass_td, q_dynasty := q_dynasty, q_best_ball := q_best_ball,
                marker := q_teams || ':' || q_roster || ':' || q_scoring || ':' || q_pass_td || ':' || q_dynasty || ':' || q_best_ball
              ) ORDER BY q_teams, q_roster, q_scoring, q_pass_td, q_dynasty, q_best_ball)
              FROM (SELECT * FROM (VALUES ('08tm'), ('10tm'), ('12tm'), ('14tm')) teams(q_teams)
                CROSS JOIN (VALUES ('flx'), ('sflx'), ('idp')) roster(q_roster)
                CROSS JOIN (VALUES ('std'), ('half'), ('ppr')) scoring(q_scoring)
                CROSS JOIN (VALUES ('4pt'), ('6pt')) pass_td(q_pass_td)
                CROSS JOIN (VALUES ('redraft'), ('dynasty')) dynasty(q_dynasty)
                CROSS JOIN (VALUES ('managed'), ('best_ball')) best_ball(q_best_ball))
            ) END AS cohort_cells
          FROM profiles
        """)

        materialize_compact_core_selector_indices(con, output_table="compact")

        rows = con.execute("""
          SELECT id, list_count(core_selector_indices) AS index_count,
            list_extract(cohort_cells, list_extract(core_selector_indices, 1)).marker AS first_marker,
            list_extract(cohort_cells, list_extract(core_selector_indices, 288)).marker AS last_marker
          FROM compact ORDER BY id
        """).fetchall()
        assert rows == [
            (1, 288, "ALL", "ALL"),
            (2, 288, "08tm:flx:std:4pt:redraft:managed", "14tm:idp:ppr:6pt:dynasty:best_ball"),
        ]
    finally:
        con.close()


def test_core_selector_indices_use_the_nearest_existing_partial_pool():
    """Independent splits may not have a populated six-way intersection.

    The selector must retain the highest-priority available split rather than
    fail the whole profile or fabricate a zero-denominator exact cell.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE compact AS
          SELECT 1 AS id,
            struct_pack(
              teams_08tm_split := 1, teams_10tm_split := 1,
              teams_12tm_split := 1, teams_14tm_split := 1,
              roster_flx_split := 1, roster_sflx_split := 1, roster_idp_split := 1,
              scoring_std_split := 1, scoring_half_split := 1, scoring_ppr_split := 1,
              pass_td_4pt_split := 1, pass_td_6pt_split := 1,
              dynasty_redraft_split := 1, dynasty_dynasty_split := 1,
              best_ball_managed_split := 1, best_ball_best_ball_split := 1
            ) AS cohort_profile,
            list_value(
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'ALL'
              ),
              struct_pack(
                q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'TEAM_POOL'
              )
            ) AS cohort_cells
        """)

        materialize_compact_core_selector_indices(con, output_table="compact")

        assert con.execute("""
          SELECT list_extract(cohort_cells, list_extract(core_selector_indices, 1)).marker
          FROM compact
        """).fetchone()[0] == "TEAM_POOL"
    finally:
        con.close()


def test_core_selector_indices_key_by_actual_cell_layout_not_shared_split_flags():
    """A shared split profile does not guarantee a shared cell-list layout.

    Mapping both rows through one arbitrary profile representative can attach
    ordinal two to a one-cell row.  Every serving ordinal must address a cell
    in its own nested list.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE compact AS
          WITH profile AS (
            SELECT struct_pack(
              teams_08tm_split := 1, teams_10tm_split := 1,
              teams_12tm_split := 1, teams_14tm_split := 1,
              roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
              scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
              pass_td_4pt_split := 0, pass_td_6pt_split := 0,
              dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
              best_ball_managed_split := 0, best_ball_best_ball_split := 0
            ) AS cohort_profile
          )
          SELECT 1 AS id, cohort_profile, list_value(
            struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'ALL'),
            struct_pack(q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'EIGHT')
          ) AS cohort_cells
          FROM profile
          UNION ALL
          SELECT 2 AS id, cohort_profile, list_value(
            struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'ALL')
          ) AS cohort_cells
          FROM profile
        """)

        materialize_compact_core_selector_indices(con, output_table="compact")

        assert con.execute("""
          SELECT COUNT(*)
          FROM compact row, UNNEST(row.core_selector_indices) u(ordinal)
          WHERE ordinal < 1 OR ordinal > list_count(row.cohort_cells)
        """).fetchone()[0] == 0
    finally:
        con.close()


def test_exact_core_selector_indices_prefer_exact_cell_then_pool_fallback():
    """Career selectors must use an exact cohort cell when it exists.

    Removing the exact-cell preference would make the first request incorrectly
    use the pooled ALL cell; removing the fallback would leave the final
    request unresolved.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE career AS
          SELECT 1 AS id,
            list_value(
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                marker := 'ALL'
              ),
              struct_pack(
                q_teams := '08tm', q_roster := 'flx', q_scoring := 'std',
                q_pass_td := '4pt', q_dynasty := 'redraft', q_best_ball := 'managed',
                marker := 'EXACT'
              )
            ) AS cohort_cells
        """)

        materialize_compact_exact_core_selector_indices(con, output_table="career")

        assert con.execute("""
          SELECT
            list_count(core_selector_indices),
            list_extract(cohort_cells, list_extract(core_selector_indices, 1)).marker,
            list_extract(cohort_cells, list_extract(core_selector_indices, 288)).marker
          FROM career
        """).fetchone() == (288, "EXACT", "ALL")
    finally:
        con.close()


def test_exact_core_selector_indices_use_a_partial_pool_before_the_all_cell():
    """Career must preserve an available partial cohort when exact is absent."""
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE career AS
          SELECT 1 AS id,
            list_value(
              struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'ALL'),
              struct_pack(q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', marker := 'TEAM_POOL')
            ) AS cohort_cells
        """)

        materialize_compact_exact_core_selector_indices(con, output_table="career")

        assert con.execute("""
          SELECT list_extract(cohort_cells, list_extract(core_selector_indices, 1)).marker
          FROM career
        """).fetchone()[0] == "TEAM_POOL"
    finally:
        con.close()


def test_grade_selector_indices_resolve_the_bracket_without_scanning_grade_cells():
    """Season grade selector must preserve a split 6-team bracket cell.

    Removing bracket resolution would make request ordinal three incorrectly
    select ALL; selecting a missing bracket must still fall back to ALL.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season AS
          SELECT 1 AS id,
            struct_pack(
              teams_08tm_split := 0, teams_10tm_split := 0,
              teams_12tm_split := 0, teams_14tm_split := 0,
              roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
              scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
              pass_td_4pt_split := 0, pass_td_6pt_split := 0,
              dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
              best_ball_managed_split := 0, best_ball_best_ball_split := 0
            ) AS cohort_profile,
            list_value(
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                q_playoff_teams := 'ALL', marker := 'ALL'
              ),
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                q_playoff_teams := '6po', marker := 'SIX'
              )
            ) AS grade_cells
        """)

        materialize_compact_grade_selector_indices(con, output_table="season")

        assert con.execute("""
          SELECT
            list_count(grade_selector_indices),
            list_extract(grade_cells, list_extract(grade_selector_indices, 2)).marker,
            list_extract(grade_cells, list_extract(grade_selector_indices, 864)).marker
          FROM season
        """).fetchone() == (864, "SIX", "ALL")
    finally:
        con.close()


def test_grade_selector_indices_use_nearest_existing_core_pool_before_all():
    """Grade selectors use the same partial-pool rule as core metrics."""
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season AS
          SELECT 1 AS id,
            struct_pack(
              teams_08tm_split := 1, teams_10tm_split := 1,
              teams_12tm_split := 1, teams_14tm_split := 1,
              roster_flx_split := 1, roster_sflx_split := 1, roster_idp_split := 1,
              scoring_std_split := 1, scoring_half_split := 1, scoring_ppr_split := 1,
              pass_td_4pt_split := 1, pass_td_6pt_split := 1,
              dynasty_redraft_split := 1, dynasty_dynasty_split := 1,
              best_ball_managed_split := 1, best_ball_best_ball_split := 1
            ) AS cohort_profile,
            list_value(
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                q_playoff_teams := 'ALL', marker := 'ALL'
              ),
              struct_pack(
                q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                q_playoff_teams := 'ALL', marker := 'TEAM_POOL'
              )
            ) AS grade_cells
        """)

        materialize_compact_grade_selector_indices(con, output_table="season")

        assert con.execute("""
          SELECT list_extract(grade_cells, list_extract(grade_selector_indices, 1)).marker
          FROM season
        """).fetchone()[0] == "TEAM_POOL"
    finally:
        con.close()


def test_grade_selector_indices_key_by_actual_grade_layout_not_shared_split_flags():
    """Rows sharing core flags may still expose different bracket-cell lists."""
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season AS
          WITH profile AS (
            SELECT struct_pack(
              teams_08tm_split := 0, teams_10tm_split := 0,
              teams_12tm_split := 0, teams_14tm_split := 0,
              roster_flx_split := 0, roster_sflx_split := 0, roster_idp_split := 0,
              scoring_std_split := 0, scoring_half_split := 0, scoring_ppr_split := 0,
              pass_td_4pt_split := 0, pass_td_6pt_split := 0,
              dynasty_redraft_split := 0, dynasty_dynasty_split := 0,
              best_ball_managed_split := 0, best_ball_best_ball_split := 0
            ) AS cohort_profile
          )
          SELECT 1 AS id, cohort_profile, list_value(
            struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL', marker := 'ALL'),
            struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := '6po', marker := 'SIX')
          ) AS grade_cells
          FROM profile
          UNION ALL
          SELECT 2 AS id, cohort_profile, list_value(
            struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL', marker := 'ALL')
          ) AS grade_cells
          FROM profile
        """)

        materialize_compact_grade_selector_indices(con, output_table="season")

        assert con.execute("""
          SELECT COUNT(*)
          FROM season row, UNNEST(row.grade_selector_indices) u(ordinal)
          WHERE ordinal < 1 OR ordinal > list_count(row.grade_cells)
        """).fetchone()[0] == 0
    finally:
        con.close()


def test_exact_grade_selector_indices_keep_a_pooled_bracket_when_core_is_exact():
    """Career grade cells resolve exact-core/exact-bracket before ALL fallback."""
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE career AS
          SELECT 1 AS id,
            list_value(
              struct_pack(
                q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL',
                q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
                q_playoff_teams := 'ALL', marker := 'ALL'
              ),
              struct_pack(
                q_teams := '08tm', q_roster := 'flx', q_scoring := 'std',
                q_pass_td := '4pt', q_dynasty := 'redraft', q_best_ball := 'managed',
                q_playoff_teams := 'ALL', marker := 'EXACT_CORE'
              ),
              struct_pack(
                q_teams := '08tm', q_roster := 'flx', q_scoring := 'std',
                q_pass_td := '4pt', q_dynasty := 'redraft', q_best_ball := 'managed',
                q_playoff_teams := '6po', marker := 'EXACT_SIX'
              )
            ) AS grade_cells
        """)

        materialize_compact_exact_grade_selector_indices(con, output_table="career")

        assert con.execute("""
          SELECT
            list_count(grade_selector_indices),
            list_extract(grade_cells, list_extract(grade_selector_indices, 2)).marker,
            list_extract(grade_cells, list_extract(grade_selector_indices, 864)).marker
          FROM career
        """).fetchone() == (864, "EXACT_SIX", "ALL")
    finally:
        con.close()


def test_exact_grade_selector_indices_use_a_partial_core_pool_before_all():
    """Career grade selection preserves the nearest existing core pool."""
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE career AS
          SELECT 1 AS id,
            list_value(
              struct_pack(q_teams := 'ALL', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL', marker := 'ALL'),
              struct_pack(q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL', q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL', q_playoff_teams := 'ALL', marker := 'TEAM_POOL')
            ) AS grade_cells
        """)

        materialize_compact_exact_grade_selector_indices(con, output_table="career")

        assert con.execute("""
          SELECT list_extract(grade_cells, list_extract(grade_selector_indices, 1)).marker
          FROM career
        """).fetchone()[0] == "TEAM_POOL"
    finally:
        con.close()


def test_career_core_rollup_consumes_the_prebuilt_season_selector_map():
    """Career must retain a season's valid partial-pool selection.

    Re-running an exact profile lookup drops the row because no six-way exact
    cell exists; consuming the season ordinal map retains all 288 requests.
    """
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season AS
          SELECT 'p1' AS NFL_player_id, 2025 AS year, 'RB' AS position,
            struct_pack(
              teams_08tm_split := 1, teams_10tm_split := 1,
              teams_12tm_split := 1, teams_14tm_split := 1,
              roster_flx_split := 1, roster_sflx_split := 1, roster_idp_split := 1,
              scoring_std_split := 1, scoring_half_split := 1, scoring_ppr_split := 1,
              pass_td_4pt_split := 1, pass_td_6pt_split := 1,
              dynasty_redraft_split := 1, dynasty_dynasty_split := 1,
              best_ball_managed_split := 1, best_ball_best_ball_split := 1
            ) AS cohort_profile,
            list_resize([1::USMALLINT], 288, 1::USMALLINT) AS core_selector_indices,
            list_value(struct_pack(
              q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL',
              q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
              rostered_league_weeks := 3::BIGINT, eligible_league_weeks := 4::BIGINT,
              started_league_weeks := 2::BIGINT, healthy_started_league_weeks := 2::BIGINT,
              healthy_eligible_league_weeks := 3::BIGINT, valid_started_outcomes := 2::BIGINT,
              win_equivalent := 1.0, roster_rate_pct := 75.0, start_rate_pct := 50.0,
              healthy_start_rate_pct := 66.67, win_rate_pct := 50.0,
              expected_starts := 0.5, expected_wins := 0.25, expected_losses := 0.25,
              clutch_season_sum := 1.0, active_weeks := 3::BIGINT, inactive_weeks := 1::BIGINT
            )) AS cohort_cells
        """)

        materialize_career_core_cohort_cells(
            con,
            season_outer_table="season",
            output_table="career",
            player_id=None,
        )

        assert con.execute("""
          SELECT list_count(cohort_cells),
            list_extract(cohort_cells, 1).q_teams,
            list_extract(cohort_cells, 1).q_roster
          FROM career
        """).fetchone() == (288, "08tm", "flx")
    finally:
        con.close()


def test_career_grade_rollup_consumes_the_prebuilt_season_selector_map():
    """Career grades retain season partial pools through their ordinal map."""
    con = duckdb.connect()
    try:
        con.execute("""
          CREATE TABLE season AS
          SELECT 'p1' AS NFL_player_id, 2025 AS year, 'RB' AS position,
            struct_pack(
              teams_08tm_split := 1, teams_10tm_split := 1,
              teams_12tm_split := 1, teams_14tm_split := 1,
              roster_flx_split := 1, roster_sflx_split := 1, roster_idp_split := 1,
              scoring_std_split := 1, scoring_half_split := 1, scoring_ppr_split := 1,
              pass_td_4pt_split := 1, pass_td_6pt_split := 1,
              dynasty_redraft_split := 1, dynasty_dynasty_split := 1,
              best_ball_managed_split := 1, best_ball_best_ball_split := 1
            ) AS cohort_profile,
            list_resize([1::USMALLINT], 864, 1::USMALLINT) AS grade_selector_indices,
            list_value(struct_pack(
              q_teams := '08tm', q_roster := 'ALL', q_scoring := 'ALL',
              q_pass_td := 'ALL', q_dynasty := 'ALL', q_best_ball := 'ALL',
              q_playoff_teams := 'ALL', eligible_leagues := 4::BIGINT,
              playoff_leagues := 2::BIGINT, champ_leagues := 1::BIGINT,
              playoff_rate_pct := 50.0, champ_rate_pct := 25.0
            )) AS grade_cells
        """)

        materialize_career_cohort_grade_cells(
            con,
            season_outer_table="season",
            output_table="career",
            player_id=None,
        )

        assert con.execute("""
          SELECT list_count(grade_cells),
            list_extract(grade_cells, 1).q_teams,
            list_extract(grade_cells, 1).q_roster
          FROM career
        """).fetchone() == (864, "08tm", "flx")
    finally:
        con.close()
