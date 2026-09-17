from types import SimpleNamespace

from multi_league.data_fetchers.shared.merge_source_copier import (
    copy_merge_source_to_public,
    maybe_copy_merge_source_to_public,
    _refresh_homepage_manager_profiles,
    _refresh_homepage_manager_rankings,
    _refresh_matchup_career,
    _refresh_transaction_player_career,
)


class FakeReader:
    def __init__(self):
        self.queries = []
        self.scalar_queries = []

    def query(self, sql: str, database: str):
        self.queries.append((sql, database))
        if "SELECT DISTINCT TRY_CAST(year AS INTEGER)" in sql and "db_name = 'source_db'" in sql:
            return [{"year": 2019}, {"year": 2020}, {"year": 2021}]
        if "SELECT DISTINCT TRY_CAST(year AS INTEGER)" in sql and "db_name = 'target_db'" in sql:
            return [{"year": 2021}, {"year": 2022}]
        if "column_name IN ('db_name', 'year')" in sql:
            return [{"table_name": "matchup"}, {"table_name": "player_fantasy"}]
        if "table_name = 'matchup'" in sql:
            return [
                {"column_name": "db_name"},
                {"column_name": "year"},
                {"column_name": "week"},
                {"column_name": "manager"},
                {"column_name": "opponent"},
            ]
        if "table_name = 'player_fantasy'" in sql:
            return [
                {"column_name": "db_name"},
                {"column_name": "year"},
                {"column_name": "week"},
                {"column_name": "manager"},
                {"column_name": "player"},
            ]
        if "table_name = 'player_fantasy_season'" in sql:
            return [
                {"column_name": name}
                for name in [
                    "db_name",
                    "NFL_player_id",
                    "year",
                    "player",
                    "position",
                    "nfl_team",
                    "fantasy_points",
                    "player_lamar",
                    "manager_lamar",
                    "clutch_equity",
                    "games_started",
                    "games_rostered",
                    "wins",
                    "losses",
                    "managers",
                    "franchise_id",
                    "team_points",
                    "opponent_points",
                    "playoff_games",
                    "playoff_wins",
                    "playoff_losses",
                    "championships",
                    "optimal_player_count",
                    "league_wide_optimal_count",
                    "fantasy_position",
                    "last_updated",
                ]
            ]
        if "table_name = 'player_fantasy_career'" in sql:
            return [
                {"column_name": name}
                for name in [
                    "db_name",
                    "NFL_player_id",
                    "first_year",
                    "last_year",
                ]
            ]
        if "table_name = 'matchup_season'" in sql:
            return [
                {"column_name": name}
                for name in [
                    "db_name",
                    "manager",
                    "year",
                    "franchise_id",
                    "games",
                    "wins",
                    "losses",
                    "ties",
                    "total_team_points",
                    "total_opponent_points",
                    "close_wins",
                    "close_games",
                    "above_league_median",
                    "below_league_median",
                    "close_losses",
                    "blowout_wins",
                    "blowout_losses",
                    "optimal_games",
                    "optimal_actual_pts",
                    "optimal_ceiling_pts",
                    "optimal_wins",
                    "optimal_missed_wins",
                    "optimal_lucky_wins",
                    "optimal_wins_actual",
                    "optimal_losses_actual",
                    "optimal_outcome_changes",
                    "optimal_margin",
                    "proj_games",
                    "proj_wins",
                    "proj_losses",
                    "proj_total_team_points",
                    "proj_total_opponent_points",
                    "proj_total_proj",
                    "proj_opp_proj",
                    "proj_above_proj",
                    "proj_below_proj",
                    "proj_beat_spread",
                    "proj_margin_total",
                    "proj_upset_wins",
                    "proj_upset_losses",
                    "proj_total_error",
                    "proj_expected_wins",
                    "max_win_streak",
                    "max_loss_streak",
                    "max_team_points",
                    "min_team_points",
                    "power_rating",
                    "avg_seed",
                    "p_playoffs",
                    "p_bye",
                    "p_semis",
                    "p_final",
                    "p_champ",
                    "exp_final_wins",
                    "made_playoffs",
                    "is_champion",
                    "is_sacko",
                    "avg_gpa",
                ]
            ]
        if "table_name = 'matchup_career'" in sql:
            return [
                {"column_name": name}
                for name in [
                    "db_name",
                    "manager",
                    "franchise_id",
                ]
            ]
        return []

    def query_scalar(self, sql: str, database: str):
        self.scalar_queries.append((sql, database))
        if "public.matchup" in sql:
            return 4
        if "public.player_fantasy_career" in sql:
            return 2
        if "public.player_fantasy" in sql:
            return 6
        return 0


class FakeWriter:
    def __init__(self):
        self.statements = []

    def execute(self, sql: str, database: str):
        self.statements.append((sql, database))
        return []


def test_copy_merge_source_to_public_duplicates_source_rows_server_side():
    reader = FakeReader()
    writer = FakeWriter()
    logs = []
    ctx = SimpleNamespace(
        merge_source={
            "source_db": "source_db",
            "manager_mapping": {"Old Manager": "New Manager"},
        }
    )

    stats = copy_merge_source_to_public(
        ctx,
        "target_db",
        reader=reader,
        writer=writer,
        log_func=logs.append,
    )

    assert stats["status"] == "copied"
    assert stats["merge_years"] == "2019, 2020"
    assert stats["matchup"] == 4
    assert stats["player_fantasy"] == 6
    assert stats["player_fantasy_career"] == 2

    writes = "\n".join(sql for sql, _database in writer.statements)
    assert "INSERT INTO ___leagues.public.matchup" in writes
    assert "INSERT INTO ___leagues.public.player_fantasy_career" in writes
    assert "'target_db' AS \"db_name\"" in writes
    assert "FROM ___leagues.public.matchup" in writes
    assert "db_name = 'source_db'" in writes
    assert "TRY_CAST(year AS INTEGER) IN (2019, 2020)" in writes
    assert "LOWER(TRIM(COALESCE(manager, ''))) NOT IN" not in writes
    assert "WHEN \"manager\" = 'Old Manager' THEN 'New Manager'" in writes
    assert (
        "WHEN LOWER(REGEXP_REPLACE(TRIM(COALESCE(\"manager\", '')), '[^A-Za-z0-9]+', '', 'g')) = 'oldmanager' THEN 'New Manager'"
        in writes
    )
    assert "SELECT *" not in writes


def test_copy_merge_source_to_public_honors_year_range_without_year_probe():
    reader = FakeReader()
    writer = FakeWriter()
    ctx = SimpleNamespace(
        merge_source={
            "source_db": "source_db",
            "year_range": {"start": 2017, "end": 2018},
        }
    )

    stats = copy_merge_source_to_public(
        ctx,
        "target_db",
        reader=reader,
        writer=writer,
        log_func=lambda _msg: None,
    )

    assert stats["merge_years"] == "2017, 2018"
    assert not any("SELECT DISTINCT TRY_CAST(year AS INTEGER)" in sql for sql, _database in reader.queries)


def test_copy_merge_source_to_public_copies_multiple_sources():
    reader = FakeReader()
    writer = FakeWriter()
    ctx = SimpleNamespace(
        merge_sources=[
            {
                "source_db": "source_db",
                "year_range": {"start": 2018, "end": 2018},
            },
            {
                "source_db": "source_db_two",
                "year_range": {"start": 2019, "end": 2019},
            },
        ]
    )

    stats = copy_merge_source_to_public(
        ctx,
        "target_db",
        reader=reader,
        writer=writer,
        log_func=lambda _msg: None,
    )

    assert stats["status"] == "copied_multi"
    assert stats["source_count"] == 2
    assert stats["source_1_db"] == "source_db"
    assert stats["source_1_merge_years"] == "2018"
    assert stats["source_2_db"] == "source_db_two"
    assert stats["source_2_merge_years"] == "2019"
    assert stats["matchup"] == 8
    assert stats["player_fantasy"] == 12


def test_copy_merge_source_to_public_noops_without_merge_source():
    stats = copy_merge_source_to_public(
        SimpleNamespace(merge_source=None),
        "target_db",
        reader=FakeReader(),
        writer=FakeWriter(),
    )

    assert stats == {"status": "no_merge_source"}


def test_maybe_copy_merge_source_to_public_loads_saved_context(tmp_path):
    (tmp_path / "sleeper_context.json").write_text(
        """
        {
          "league_name": "Target",
          "platform": "sleeper",
          "start_year": 2020,
          "end_year": 2021,
          "import_mode": "full",
          "merge_source": {
            "source_db": "source_db",
            "year_range": {"start": 2019, "end": 2019}
          }
        }
        """,
        encoding="utf-8",
    )

    stats = maybe_copy_merge_source_to_public(
        data_dir=tmp_path,
        target_db_name="target_db",
        reader=FakeReader(),
        writer=FakeWriter(),
        log_func=lambda _msg: None,
    )

    assert stats["status"] == "copied"
    assert stats["merge_years"] == "2019"
    assert stats["single_year_import"] == "false"


def test_maybe_copy_merge_source_to_public_loads_saved_merge_sources_context(tmp_path):
    (tmp_path / "sleeper_context.json").write_text(
        """
        {
          "league_name": "Target",
          "platform": "sleeper",
          "start_year": 2020,
          "end_year": 2021,
          "import_mode": "full",
          "merge_sources": [
            {
              "source_db": "source_db",
              "year_range": {"start": 2019, "end": 2019}
            },
            {
              "source_db": "source_db_two",
              "year_range": {"start": 2020, "end": 2020}
            }
          ]
        }
        """,
        encoding="utf-8",
    )

    stats = maybe_copy_merge_source_to_public(
        data_dir=tmp_path,
        target_db_name="target_db",
        reader=FakeReader(),
        writer=FakeWriter(),
        log_func=lambda _msg: None,
    )

    assert stats["status"] == "copied_multi"
    assert stats["source_count"] == 2


def test_refresh_matchup_career_stores_win_pct_as_fraction():
    reader = FakeReader()
    writer = FakeWriter()

    _refresh_matchup_career(
        reader=reader,
        writer=writer,
        target_db="target_db",
        log_func=lambda _msg: None,
    )

    writes = "\n".join(sql for sql, _database in writer.statements)
    assert "(SUM(wins) + 0.5 * SUM(ties)) / NULLIF(SUM(games), 0) AS win_pct" in writes
    assert "AS win_pct,\n            SUM(close_wins)" in writes
    assert "* 100 AS win_pct" not in writes


def test_refresh_homepage_manager_rankings_includes_playoff_results():
    class HomepageReader(FakeReader):
        def query(self, sql: str, database: str):
            if "table_name = 'matchup_career'" in sql:
                return [{"column_name": name} for name in ["db_name", "manager", "franchise_id", "wins"]]
            if "table_name = 'homepage_manager_rankings'" in sql:
                return [{"column_name": name} for name in ["db_name", "manager", "franchise_id"]]
            return super().query(sql, database)

    reader = HomepageReader()
    writer = FakeWriter()

    _refresh_homepage_manager_rankings(
        reader=reader,
        writer=writer,
        target_db="target_db",
        log_func=lambda _msg: None,
    )

    writes = "\n".join(sql for sql, _database in writer.statements)
    assert "playoff_results AS" in writes
    assert "c.wins + COALESCE(p.playoff_wins, 0) AS wins" in writes
    assert "AND COALESCE(CAST(is_playoffs AS INT), 0) = 1" in writes
    assert "AND COALESCE(CAST(is_consolation AS INT), 0) = 0" in writes


def test_refresh_homepage_profiles_reconciles_stale_ids_only_for_unique_names():
    class HomepageProfileReader(FakeReader):
        def query(self, sql: str, database: str):
            if "table_name = 'matchup_career'" in sql:
                return [
                    {"column_name": name}
                    for name in ["db_name", "manager", "franchise_id", "wins"]
                ]
            if "table_name = 'homepage_manager_profiles'" in sql:
                return [
                    {"column_name": name}
                    for name in ["db_name", "manager", "franchise_id"]
                ]
            return super().query(sql, database)

    reader = HomepageProfileReader()
    writer = FakeWriter()

    _refresh_homepage_manager_profiles(
        reader=reader,
        writer=writer,
        target_db="target_db",
        log_func=lambda _msg: None,
    )

    writes = "\n".join(sql for sql, _database in writer.statements)
    assert "unique_career_managers AS" in writes
    assert "unique_profile_managers AS" in writes
    assert "HAVING COUNT(*) = 1" in writes
    assert "SET franchise_id = c.franchise_id" in writes


def test_refresh_transaction_player_career_dedupes_two_sided_trade_rows():
    class TransactionReader(FakeReader):
        def query(self, sql: str, database: str):
            if "table_name = 'transactions'" in sql:
                return [
                    {"column_name": name}
                    for name in [
                        "db_name",
                        "player",
                        "position",
                        "manager",
                        "source_manager",
                        "year",
                        "transaction_type",
                        "transaction_id",
                        "faab_bid",
                        "traded_pick_season",
                        "traded_pick_round",
                        "traded_pick_original_owner",
                    ]
                ]
            if "table_name = 'transaction_player_career'" in sql:
                return [
                    {"column_name": name}
                    for name in [
                        "db_name",
                        "player",
                        "position",
                    ]
                ]
            return super().query(sql, database)

    reader = TransactionReader()
    writer = FakeWriter()

    _refresh_transaction_player_career(
        reader=reader,
        writer=writer,
        target_db="target_db",
        log_func=lambda _msg: None,
    )

    writes = "\n".join(sql for sql, _database in writer.statements)
    assert "WITH deduped_trade_rows AS" in writes
    assert "ROW_NUMBER() OVER" in writes
    assert "t.transaction_type IN ('trade', 'trade_pick')" in writes
    assert "transaction_type NOT IN ('trade', 'trade_pick')" in writes
    assert "ORDER BY t.manager, t.source_manager" in writes
    assert "COALESCE(CAST(t.traded_pick_season AS VARCHAR), '')" in writes
