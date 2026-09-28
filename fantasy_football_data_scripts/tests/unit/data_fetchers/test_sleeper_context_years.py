import sys
from pathlib import Path

import pytest


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.sleeper.sleeper_context import (
    SleeperContext,
    discover_league_history,
    resolve_years_to_fetch,
)
from sleeper_initial_import import _resolve_quick_import_years


def test_get_processing_years_prefers_discovered_league_ids(tmp_path):
    ctx = SleeperContext(
        league_id="123",
        league_name="NYU FFL",
        username="tester",
        start_year=2024,
        end_year=2025,
        league_ids={"2018": "a", "2020": "b", "2025": "c"},
        data_directory=tmp_path,
    )

    assert ctx.get_processing_years(season_cap=2025) == [2018, 2020, 2025]


def test_get_processing_years_falls_back_to_configured_range(tmp_path):
    ctx = SleeperContext(
        league_id="123",
        league_name="NYU FFL",
        username="tester",
        start_year=2019,
        end_year=2021,
        league_ids={},
        data_directory=tmp_path,
    )

    assert ctx.get_processing_years(season_cap=2025) == [2019, 2020, 2021]


def test_resolve_years_to_fetch_accepts_multi_year_filter(tmp_path):
    ctx = SleeperContext(
        league_id="123",
        league_name="NYU FFL",
        username="tester",
        start_year=2019,
        end_year=2021,
        data_directory=tmp_path,
    )

    assert resolve_years_to_fetch(ctx, [2025, "2026", 2025, "bad"]) == [2025, 2026]


def test_quick_import_includes_previous_scored_season_for_empty_current_shell(tmp_path):
    ctx = SleeperContext(
        league_id="2026",
        league_name="North Missouri Football League",
        username="tester",
        start_year=2026,
        end_year=2026,
        league_ids={"2024": "2024", "2025": "2025", "2026": "2026"},
        data_directory=tmp_path,
    )

    assert _resolve_quick_import_years(
        ctx,
        2026,
        {"season": "2026", "previous_season": "2025", "season_has_scores": False},
    ) == [2025, 2026]


def test_quick_import_includes_previous_scored_season_for_offseason_shell(tmp_path):
    ctx = SleeperContext(
        league_id="2026",
        league_name="Seattle Dynasty Boys",
        username="tester",
        start_year=2026,
        end_year=2026,
        league_ids={"2022": "2022", "2023": "2023", "2024": "2024", "2025": "2025", "2026": "2026"},
        data_directory=tmp_path,
    )

    assert _resolve_quick_import_years(
        ctx,
        2026,
        {
            "season": "2026",
            "previous_season": "2025",
            "season_has_scores": True,
            "season_type": "off",
            "week": 0,
            "leg": 0,
            "display_week": 0,
        },
    ) == [2025, 2026]


def test_quick_import_uses_only_current_season_once_scores_exist(tmp_path):
    ctx = SleeperContext(
        league_id="2026",
        league_name="North Missouri Football League",
        username="tester",
        start_year=2026,
        end_year=2026,
        league_ids={"2025": "2025", "2026": "2026"},
        data_directory=tmp_path,
    )

    assert _resolve_quick_import_years(
        ctx,
        2026,
        {"season": "2026", "previous_season": "2025", "season_has_scores": True},
    ) == [2026]


def test_history_rejects_sleeper_pickem_products_before_fantasy_fetches():
    class PickemClient:
        def get_league(self, league_id):
            return {
                "league_id": league_id,
                "name": "Sunday Massacre",
                "season": "2026",
                "sport": "pickem:nfl",
                "previous_league_id": None,
            }

        def get_league_matchups(self, *_args, **_kwargs):
            pytest.fail("pick'em products must be rejected before fantasy matchup discovery")

    with pytest.raises(ValueError, match="not a Sleeper fantasy-football league"):
        discover_league_history(PickemClient(), "1384569135408644096")


def test_history_treats_sleeper_zero_previous_id_as_the_end_of_the_chain():
    leagues = {
        "s26": {"league_id": "s26", "season": "2026", "sport": "nfl", "previous_league_id": "s25"},
        "s25": {"league_id": "s25", "season": "2025", "sport": "nfl", "previous_league_id": "s24"},
        "s24": {"league_id": "s24", "season": "2024", "sport": "nfl", "previous_league_id": "s23"},
        "s23": {"league_id": "s23", "season": "2023", "sport": "nfl", "previous_league_id": "0"},
    }

    class Client:
        def get_league(self, league_id):
            assert league_id != "0"
            return leagues[league_id]

    assert discover_league_history(Client(), "s26", skip_empty_seasons=False) == {
        "2023": "s23",
        "2024": "s24",
        "2025": "s25",
        "2026": "s26",
    }


def test_full_import_fails_closed_when_native_history_discovery_errors():
    source = (SCRIPT_ROOT / "sleeper_initial_import.py").read_text(encoding="utf-8")
    phase = source[source.index("        if not ctx.league_ids:") :]
    phase = phase[:phase.index("        else:\n            log(f\"[HISTORY] Using")]
    error_handler = phase[phase.index("            except Exception as e:") :]

    assert "raise RuntimeError" in error_handler
    assert "ctx.league_ids = {str(ctx.start_year): ctx.league_id}" not in error_handler
