from __future__ import annotations

import pandas as pd
import pytest

from multi_league.core.league_update_ownership import (
    PreservationError,
    assert_refresh_preservation,
)


def test_preservation_gate_rejects_current_only_career_rebuild():
    before = {
        "matchup_career": pd.DataFrame([{
            "db_name": "league_a",
            "franchise_id": "f1",
            "games": 101,
            "wins": 61,
            "inflation_rate": 1.2,
        }]),
    }
    after = {
        "matchup_career": pd.DataFrame([{
            "db_name": "league_a",
            "franchise_id": "f1",
            "games": 1,
            "wins": 1,
            "inflation_rate": None,
        }]),
    }

    with pytest.raises(PreservationError, match="career history shrank"):
        assert_refresh_preservation(before, after, active_year=2026)


def test_preservation_gate_accepts_added_active_season_history():
    before = {
        "matchup_career": pd.DataFrame([{
            "db_name": "league_a", "franchise_id": "f1", "games": 101, "wins": 61,
        }]),
        "league_context": pd.DataFrame([{
            "db_name": "league_a", "league_name": "League A",
            "manager_name_overrides_json": '{"f1":"Joe"}',
        }]),
    }
    after = {
        "matchup_career": pd.DataFrame([{
            "db_name": "league_a", "franchise_id": "f1", "games": 102, "wins": 62,
        }]),
        "league_context": before["league_context"].copy(),
    }

    receipt = assert_refresh_preservation(before, after, active_year=2026)

    assert receipt["historical_rows_preserved"] is True
    assert receipt["user_configuration_preserved"] is True


def test_preservation_gate_rejects_changed_historical_source_identity():
    before = {
        "matchup": pd.DataFrame([{
            "db_name": "league_a",
            "year": 2025,
            "week": 1,
            "manager_week": "Joe_2025_1",
            "manager": "Joe",
            "franchise_id": "stable-franchise",
            "team_points": 100.0,
        }]),
    }
    after = {
        "matchup": before["matchup"].assign(franchise_id="replacement-franchise"),
    }

    with pytest.raises(PreservationError, match="historical source rows changed"):
        assert_refresh_preservation(before, after, active_year=2026)


def test_preservation_gate_rejects_loss_of_shared_team_alias_during_active_refresh():
    aliases = {
        "Elizabeth": "Elizabeth + Joe",
        "Megan": "Meg + Sammie",
        "Robert": "RJ + Abdulai",
    }
    before = {
        "league_context": pd.DataFrame([{
            "db_name": "afi_data",
            "manager_name_overrides_json": __import__("json").dumps(aliases),
        }]),
        "matchup": pd.DataFrame([
            {"db_name": "afi_data", "year": 2026, "week": 1,
             "franchise_id": "f-elizabeth", "manager_week": "Elizabeth_2026_1",
             "manager": "Elizabeth + Joe", "team_points": 141.26},
            {"db_name": "afi_data", "year": 2026, "week": 1,
             "franchise_id": "f-megan", "manager_week": "Megan_2026_1",
             "manager": "Meg + Sammie", "team_points": 125.42},
            {"db_name": "afi_data", "year": 2026, "week": 1,
             "franchise_id": "f-robert", "manager_week": "Robert_2026_1",
             "manager": "RJ + Abdulai", "team_points": 89.10},
        ]),
    }
    after = {
        "league_context": before["league_context"].copy(),
        "matchup": before["matchup"].assign(
            manager=["Elizabeth + Joe", "Megan", "RJ + Abdulai"],
            team_points=[141.26, 126.42, 89.10],
        ),
    }

    accepted = {
        "league_context": before["league_context"].copy(),
        "matchup": before["matchup"].assign(
            team_points=[141.26, 126.42, 89.10],
        ),
    }
    assert assert_refresh_preservation(
        before, accepted, active_year=2026
    )["user_configuration_preserved"]

    with pytest.raises(PreservationError, match="active alias changed"):
        assert_refresh_preservation(before, after, active_year=2026)


def test_preservation_gate_rejects_homepage_value_becoming_null():
    before = {
        "homepage_league_summary": pd.DataFrame([{
            "db_name": "league_a",
            "best_career_clutch_player": "Player One",
            "best_career_clutch_value": 14.5,
        }]),
    }
    after = {
        "homepage_league_summary": pd.DataFrame([{
            "db_name": "league_a",
            "best_career_clutch_player": None,
            "best_career_clutch_value": None,
        }]),
    }

    with pytest.raises(PreservationError, match="preserved value became null"):
        assert_refresh_preservation(before, after, active_year=2026)


def test_refresh_aggregates_build_active_season_rollups_but_not_careers(tmp_path, monkeypatch):
    from multi_league.core.local_db import LocalLeagueDB
    from scripts import refresh_yahoo_active_season

    monkeypatch.setattr(
        refresh_yahoo_active_season,
        "_attach_ops_cache_for_enrichment",
        lambda _local: None,
    )

    local = LocalLeagueDB(tmp_path, "league_a")
    try:
        for table_name in ("player_fantasy", "draft", "transactions"):
            local.ensure_table(table_name)
        conn = local.connect()
        conn.execute("ATTACH ':memory:' AS ___ops")
        conn.execute("CREATE SCHEMA ___ops.nfl_historical")
        conn.execute(
            "CREATE TABLE ___ops.nfl_historical.player_bio "
            "(NFL_player_id VARCHAR, player VARCHAR)"
        )
        conn.execute(
            "CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all "
            "(NFL_player_id VARCHAR, player_week VARCHAR, player VARCHAR, "
            "nfl_team VARCHAR, year INTEGER, week INTEGER)"
        )
        refresh_yahoo_active_season._run_refresh_aggregates(
            local,
            db_name="league_a",
            active_year=2026,
            work_dir=tmp_path,
            has_finalized_matchups=False,
        )

        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            ).fetchall()
        }
        assert "player_fantasy_career" not in tables
        assert {
            "player_fantasy_season",
            "player_fantasy_season_all",
            "draft_manager_season",
            "transaction_manager_season",
            "transaction_report_card",
        }.issubset(tables)
    finally:
        local.close()


def test_target_year_simulation_and_clutch_leave_historical_values_untouched(
    tmp_path,
    monkeypatch,
):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.transformations.matchup import playoff_odds_import
    from scripts import refresh_yahoo_active_season

    local = LocalLeagueDB(tmp_path, "league_a")
    try:
        for table_name in ("matchup", "schedule", "league_settings", "player_fantasy"):
            local.ensure_table(table_name)
        matchup_rows = []
        schedule_rows = []
        player_rows = []
        scores = {"A": 120.0, "B": 90.0, "C": 110.0, "D": 100.0}
        opponents = {"A": "B", "B": "A", "C": "D", "D": "C"}
        for year in (2025, 2026):
            for index, manager in enumerate(("A", "B", "C", "D"), start=1):
                opponent = opponents[manager]
                matchup_rows.append({
                    "db_name": "league_a",
                    "year": year,
                    "week": 1,
                    "manager": manager,
                    "franchise_id": f"f-{manager}",
                    "manager_week": f"{manager}_{year}_1",
                    "opponent": opponent,
                    "opponent_franchise_id": f"f-{opponent}",
                    "team_points": scores[manager],
                    "opponent_points": scores[opponent],
                    "win": int(scores[manager] > scores[opponent]),
                    "loss": int(scores[manager] < scores[opponent]),
                    "tie": 0,
                    "is_playoffs": False,
                    "is_consolation": False,
                    "is_bye_week": False,
                    "wins_to_date": int(scores[manager] > scores[opponent]),
                    "losses_to_date": int(scores[manager] < scores[opponent]),
                    "ties_to_date": 0,
                    "p_champ": 77.7 if year == 2025 else None,
                })
                schedule_rows.append({
                    "db_name": "league_a",
                    "year": year,
                    "week": 1,
                    "manager": manager,
                    "franchise_id": f"f-{manager}",
                    "manager_week": f"{manager}_{year}_1",
                    "opponent": opponent,
                    "opponent_franchise_id": f"f-{opponent}",
                    "is_playoffs": False,
                    "is_consolation": False,
                })
                player_rows.append({
                    "db_name": "league_a",
                    "player_week": f"p-{manager}_{year}_1",
                    "NFL_player_id": f"p-{manager}",
                    "year": year,
                    "week": 1,
                    "manager": manager,
                    "franchise_id": f"f-{manager}",
                    "player": f"Player {manager}",
                    "position": "RB",
                    "fantasy_position": "RB",
                    "manager_lamar": 1.0,
                    "player_lamar": 1.0,
                    "clutch_equity": 4.5 if year == 2025 else None,
                })
        local._insert_into_table("matchup", pd.DataFrame(matchup_rows))
        local._insert_into_table("schedule", pd.DataFrame(schedule_rows))
        local._insert_into_table("player_fantasy", pd.DataFrame(player_rows))
        settings = {
            year: {
                "num_teams": 4,
                "num_playoff_teams": 2,
                "bye_teams": 0,
                "playoff_start_week": 15,
                "uses_playoff_reseeding": 0,
                "uses_median": 0,
            }
            for year in (2025, 2026)
        }
        settings_rows = [
            {"db_name": "league_a", "year": year, **row}
            for year, row in settings.items()
        ]
        local._insert_into_table("league_settings", pd.DataFrame(settings_rows))
        monkeypatch.setattr(playoff_odds_import, "N_SIMS", 100)
        monkeypatch.setattr(
            playoff_odds_import,
            "TARGET_COLS",
            playoff_odds_import.PlayoffConfig(
                playoff_slots=2,
                bye_slots=0,
                num_teams=4,
                regular_season_weeks=14,
                use_median=False,
                bracket_reseed=False,
            ).target_cols,
        )

        local.close()
        refresh_yahoo_active_season._run_refresh_simulations(
            db_name="league_a",
            active_year=2026,
            current_week=1,
            work_dir=tmp_path,
            n_sims=100,
        )
        local.connect()

        assert local.connect().execute(
            "SELECT DISTINCT p_champ FROM public.matchup WHERE year = 2025"
        ).fetchall() == [(77.7,)]
        assert local.connect().execute(
            "SELECT COUNT(*) FROM public.matchup WHERE year = 2026 AND p_champ IS NOT NULL"
        ).fetchone()[0] == 4

        assert local.connect().execute(
            "SELECT DISTINCT clutch_equity FROM public.player_fantasy WHERE year = 2025"
        ).fetchall() == [(4.5,)]
        assert local.connect().execute(
            "SELECT COUNT(*) FROM public.player_fantasy "
            "WHERE year = 2026 AND clutch_equity IS NOT NULL"
        ).fetchone()[0] == 4
    finally:
        local.close()
