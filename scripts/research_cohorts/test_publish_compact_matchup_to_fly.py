from __future__ import annotations

import duckdb
import pytest

import publish_compact_matchup_to_fly as P


def _bundle(path: str = ":memory:") -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(path)
    con.execute("""
        CREATE TABLE research_matchup_compact_weekly AS
        SELECT '00-0033280'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year,
               1::INTEGER AS week, 'RB'::VARCHAR AS position,
               [{'q_teams':'ALL','q_roster':'ALL','q_scoring':'ALL','q_pass_td':'ALL',
                 'q_dynasty':'ALL','q_best_ball':'ALL','roster_rate_pct':99.0,
                 'start_rate_pct':98.0,'win_rate_pct':61.0,
                 'eligible_leagues':9837::BIGINT,'rostered_leagues':9766::BIGINT,
                 'started_leagues':9600::BIGINT,'valid_started_outcomes':9579::BIGINT,
                 'win_equivalent':6019.0}] AS cohort_cells,
               list_value(1::USMALLINT) AS roster_cells,
               list_resize([1::USMALLINT], 288, 1::USMALLINT) AS core_selector_indices,
               list_resize([1::USMALLINT], 288, 1::USMALLINT) AS roster_selector_indices
    """)
    con.execute("""
        CREATE TABLE research_matchup_compact_season AS
        SELECT '00-0033280'::VARCHAR AS NFL_player_id, 2025::INTEGER AS year,
               'RB'::VARCHAR AS position,
               [{'q_teams':'ALL','q_roster':'ALL','q_scoring':'ALL','q_pass_td':'ALL',
                 'q_dynasty':'ALL','q_best_ball':'ALL','roster_rate_pct':99.0,
                 'start_rate_pct':98.0,'healthy_start_rate_pct':98.0,'win_rate_pct':61.0,
                 'eligible_leagues':9980::BIGINT,'rostered_league_weeks':157712::BIGINT,
                 'eligible_league_weeks':158687::BIGINT,'started_league_weeks':155272::BIGINT,
                 'valid_started_outcomes':149661::BIGINT,'win_equivalent':91505.0,
                 'active_weeks':17::INTEGER,'inactive_weeks':0::INTEGER}] AS cohort_cells,
               [{'q_teams':'ALL','q_roster':'ALL','q_scoring':'ALL','q_pass_td':'ALL',
                 'q_dynasty':'ALL','q_best_ball':'ALL','q_playoff_teams':'ALL','champ_rate_pct':12.0,'playoff_rate_pct':55.0,
                 'eligible_leagues':9980::BIGINT,'eligible_league_seasons':9980::BIGINT,
                 'expected_champs':0.12,'expected_playoffs':0.55}] AS grade_cells,
               list_value(1::USMALLINT) AS roster_cells,
               list_resize([1::USMALLINT], 288, 1::USMALLINT) AS core_selector_indices,
               list_resize([1::USMALLINT], 288, 1::USMALLINT) AS roster_selector_indices,
               list_resize([1::USMALLINT], 864, 1::USMALLINT) AS grade_selector_indices
    """)
    con.execute("""
        CREATE TABLE research_matchup_compact_career AS
        SELECT '00-0033280'::VARCHAR AS NFL_player_id, 'RB'::VARCHAR AS position,
               [{'q_teams':'ALL','q_roster':'ALL','q_scoring':'ALL','q_pass_td':'ALL',
                 'q_dynasty':'ALL','q_best_ball':'ALL','roster_rate_pct':99.0,
                 'start_rate_pct':98.0,'healthy_start_rate_pct':98.0,'win_rate_pct':61.0,
                 'qualifying_seasons':8::INTEGER}] AS cohort_cells,
               [{'q_teams':'ALL','q_roster':'ALL','q_scoring':'ALL','q_pass_td':'ALL',
                 'q_dynasty':'ALL','q_best_ball':'ALL','q_playoff_teams':'ALL','champ_rate_pct':12.0,'playoff_rate_pct':55.0,
                 'eligible_leagues':9980::BIGINT,'eligible_league_seasons':80000::BIGINT,
                 'expected_champs':0.96,'expected_playoffs':4.4}] AS grade_cells,
               list_value(1::USMALLINT) AS roster_cells,
               list_resize([1::USMALLINT], 288, 1::USMALLINT) AS core_selector_indices,
               list_resize([1::USMALLINT], 288, 1::USMALLINT) AS roster_selector_indices,
               list_resize([1::USMALLINT], 864, 1::USMALLINT) AS grade_selector_indices
    """)
    con.execute("""
        CREATE TABLE research_matchup_compact_serving_weekly AS
        SELECT ordinal::USMALLINT AS request_ordinal, NFL_player_id, year, week, position,
               9837::BIGINT AS eligible_population, 99.0 AS roster_rate_pct, 98.0 AS start_rate_pct,
               NULL::DOUBLE AS healthy_start_rate_pct, 61.0 AS win_rate_pct,
               0.0 AS expected_starts, 0.0 AS expected_wins, 0.0 AS expected_losses,
               0.0 AS clutch_value, NULL::BIGINT AS active_weeks, NULL::BIGINT AS inactive_weeks,
               9600::BIGINT AS started_weeks, NULL::BIGINT AS qualifying_seasons, 1 AS _serving_row
        FROM research_matchup_compact_weekly CROSS JOIN range(1, 145) request(ordinal)
    """)
    con.execute("""
        CREATE TABLE research_matchup_compact_serving_season AS
        SELECT ordinal::USMALLINT AS request_ordinal, NFL_player_id, year, position,
               9980::BIGINT AS eligible_population, 99.0 AS roster_rate_pct, 98.0 AS start_rate_pct,
               98.0 AS healthy_start_rate_pct, 61.0 AS win_rate_pct,
               0.0 AS expected_starts, 0.0 AS expected_wins, 0.0 AS expected_losses,
               0.0 AS clutch_value, 17::BIGINT AS active_weeks, 0::BIGINT AS inactive_weeks,
               0::BIGINT AS started_weeks, NULL::BIGINT AS qualifying_seasons,
               55.0 AS playoff_rate_pct, 12.0 AS champ_rate_pct,
               NULL::DOUBLE AS expected_playoffs, NULL::DOUBLE AS expected_champs,
               9980::BIGINT AS grade_eligible_population, 1 AS _serving_row
        FROM research_matchup_compact_season CROSS JOIN range(1, 577) request(ordinal)
    """)
    con.execute("""
        CREATE TABLE research_matchup_compact_serving_career AS
        SELECT ordinal::USMALLINT AS request_ordinal, NFL_player_id, position,
               80000::BIGINT AS eligible_population, 99.0 AS roster_rate_pct, 98.0 AS start_rate_pct,
               98.0 AS healthy_start_rate_pct, 61.0 AS win_rate_pct,
               0.0 AS expected_starts, 0.0 AS expected_wins, 0.0 AS expected_losses,
               0.0 AS clutch_value, 0::BIGINT AS active_weeks, 0::BIGINT AS inactive_weeks,
               0::BIGINT AS started_weeks, 8::BIGINT AS qualifying_seasons,
               55.0 AS playoff_rate_pct, 12.0 AS champ_rate_pct,
               4.4 AS expected_playoffs, 0.96 AS expected_champs,
               80000::BIGINT AS grade_eligible_population, 1 AS _serving_row
        FROM research_matchup_compact_career CROSS JOIN range(1, 577) request(ordinal)
    """)
    return con


def test_compact_publisher_targets_nested_compact_tables_and_narrow_grade_sidecars():
    """Grade sidecars stay one row per player/time, never one row per selector."""
    assert P.COMPACT_TABLES == [
        "research_matchup_compact_weekly",
        "research_matchup_compact_season",
        "research_matchup_compact_career",
        "research_matchup_compact_grade_season",
        "research_matchup_compact_grade_career",
    ]


def test_compact_publisher_expires_and_warms_the_scoped_vercel_research_cache(monkeypatch):
    """A successful Fly publish must leave canonical research reads warm on Vercel."""
    calls: list[tuple[str, str]] = []

    class Response:
        status_code = 200
        text = "ok"

        @staticmethod
        def json():
            return {"revalidated": True, "scope": "research-matchup"}

        @staticmethod
        def raise_for_status():
            return None

    def post(url, *, params, timeout):
        calls.append(("POST", url))
        assert params == {
            "scope": "research-matchup",
            "secret": "cache-secret",
            "strategy": "expire",
        }
        assert timeout == P.VERCEL_REVALIDATE_TIMEOUT_SECONDS
        return Response()

    def get(url, *, headers, timeout):
        calls.append(("GET", url))
        assert headers["User-Agent"] == "leaguehistory-ops-cache-warmer/1.0"
        assert timeout == P.VERCEL_WARM_TIMEOUT_SECONDS
        return Response()

    monkeypatch.setattr(P.requests, "post", post)
    monkeypatch.setattr(P.requests, "get", get)
    # This unit test validates URL coverage, not real Fly backpressure.  Keep
    # its synthetic 1,296-request fanout short while production stays bounded
    # at the conservative worker concurrency.
    monkeypatch.setenv("REVALIDATION_SECRET", "cache-secret")

    P.refresh_vercel_research_matchup_cache("https://example.test")

    assert calls[0] == ("POST", "https://example.test/api/revalidate")
    warmed = [url for method, url in calls if method == "GET"]
    # Prime the exact page-one sort surface the UI exposes: a manager can
    # switch to either the top or bottom 100 without triggering a packed-cell
    # scan in the request path.  36 formats × (5 weekly + 6 season + 6
    # career metrics) × two directions, plus the season/career initial
    # Start-% page (weekly Start-% desc is already in the sort surface).
    assert len(warmed) == 108
    assert any("/api/research/12t-flx-half-4pt/matchup?" in url and "grain=season" in url for url in warmed)
    assert any("/api/research/12t-flx-ppr-4pt/matchup?" in url and "grain=career" in url for url in warmed)


def test_weekly_serving_table_is_split_into_deterministic_transport_partitions():
    assert P.WEEKLY_PARTITIONS == 3
    assert [P.weekly_partition_table(index) for index in range(P.WEEKLY_PARTITIONS)] == [
        "research_matchup_compact_weekly_p00",
        "research_matchup_compact_weekly_p01",
        "research_matchup_compact_weekly_p02",
    ]


def test_compact_preflight_accepts_valid_cells_and_cmc_week_one_golden():
    summary = P.preflight_bundle(_bundle())
    assert summary["research_matchup_compact_weekly"]["rows"] == 1
    assert summary["research_matchup_compact_season"]["rows"] == 1
    assert summary["research_matchup_compact_career"]["rows"] == 1
    assert P.cmc_week_one_golden(_bundle()) == (9837, 9766, 9600, 9579, 6019.0)


def test_compact_preflight_rejects_duplicate_outer_key():
    con = _bundle()
    con.execute("INSERT INTO research_matchup_compact_weekly SELECT * FROM research_matchup_compact_weekly")
    with pytest.raises(SystemExit, match="duplicate outer identity"):
        P.preflight_bundle(con)


def test_compact_preflight_rejects_impossible_grade_relationship():
    con = _bundle()
    con.execute("""
        UPDATE research_matchup_compact_season
        SET grade_cells = [{'q_teams':'ALL','q_roster':'ALL','q_scoring':'ALL','q_pass_td':'ALL',
                            'q_dynasty':'ALL','q_best_ball':'ALL','q_playoff_teams':'ALL','champ_rate_pct':60.0,'playoff_rate_pct':50.0,
                            'eligible_leagues':9980::BIGINT,'eligible_league_seasons':9980::BIGINT,
                            'expected_champs':0.60,'expected_playoffs':0.50}]
    """)
    with pytest.raises(SystemExit, match="champ.*playoff"):
        P.preflight_bundle(con)


def test_compact_preflight_rejects_missing_selector_access_path():
    con = _bundle()
    con.execute("ALTER TABLE research_matchup_compact_weekly DROP core_selector_indices")
    with pytest.raises(SystemExit, match="core_selector_indices"):
        P.preflight_bundle(con)

def test_compact_preflight_rejects_missing_roster_selector_access_path():
    con = _bundle()
    con.execute("ALTER TABLE research_matchup_compact_weekly DROP roster_selector_indices")
    with pytest.raises(SystemExit, match="roster_selector_indices"):
        P.preflight_bundle(con)


def test_compact_preflight_accepts_grade_cells_split_by_core_selector():
    con = _bundle()
    con.execute("""
        UPDATE research_matchup_compact_season
        SET grade_cells = [
            {'q_teams':'ALL','q_roster':'ALL','q_scoring':'half','q_pass_td':'ALL',
             'q_dynasty':'ALL','q_best_ball':'ALL','q_playoff_teams':'ALL','champ_rate_pct':12.0,'playoff_rate_pct':55.0,
             'eligible_leagues':9980::BIGINT,'eligible_league_seasons':9980::BIGINT,
             'expected_champs':0.12,'expected_playoffs':0.55},
            {'q_teams':'ALL','q_roster':'ALL','q_scoring':'full','q_pass_td':'ALL',
             'q_dynasty':'ALL','q_best_ball':'ALL','q_playoff_teams':'ALL','champ_rate_pct':13.0,'playoff_rate_pct':56.0,
             'eligible_leagues':9980::BIGINT,'eligible_league_seasons':9980::BIGINT,
             'expected_champs':0.13,'expected_playoffs':0.56}
        ]
    """)
    assert P.preflight_bundle(con)["research_matchup_compact_season"]["rows"] == 1


def test_selector_code_is_stable_for_the_all_pool():
    con = _bundle()
    code = con.execute(
        "SELECT " + P._cohort_code_sql() + " FROM research_matchup_compact_weekly r, UNNEST(r.cohort_cells) u(cell)"
    ).fetchone()[0]
    assert code == 0


def test_live_verification_emits_valid_cmc_selector_sql(tmp_path):
    bundle = tmp_path / "compact.duckdb"
    _bundle(str(bundle)).close()

    class Reader:
        def __init__(self):
            self.sql: list[str] = []

        def query(self, sql: str, *, database: str):
            assert database == "___ops"
            self.sql.append(sql)
            if "COUNT(*) AS n" in sql:
                if "UNION ALL" in sql:
                    return [{"n": 1}]
                return [{"n": 1 if "p00" in sql else 0 if "weekly_p" in sql else 1}]
            return [{
                "eligible_leagues": 9837,
                "rostered_leagues": 9766,
                "started_leagues": 9600,
                "valid_started_outcomes": 9579,
                "win_equivalent": 6019.0,
            }]

    reader = Reader()
    P.verify_live(
        reader,
        {
            "research_matchup_compact_weekly_p00": 1,
            "research_matchup_compact_weekly_p01": 0,
            "research_matchup_compact_weekly_p02": 0,
            "research_matchup_compact_season": 1,
            "research_matchup_compact_career": 1,
        },
        bundle,
    )
    cmc_query = reader.sql[-1]
    assert "win_equivalent,\n            FROM" not in cmc_query
