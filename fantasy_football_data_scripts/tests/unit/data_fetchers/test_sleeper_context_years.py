import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext, resolve_years_to_fetch
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
