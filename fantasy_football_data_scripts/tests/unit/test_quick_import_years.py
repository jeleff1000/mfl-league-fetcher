import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[2]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.core.year_filter_utils import resolve_quick_import_years, unscored_current_shell_years
from multi_league.data_fetchers.espn.espn_context import ESPNContext
from espn_initial_import import _resolve_espn_quick_import_years, _discover_espn_import_years


def test_shared_quick_years_include_previous_for_empty_current_shell():
    assert resolve_quick_import_years(
        [2024, 2025, 2026],
        2026,
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
    ) == [2025, 2026]


def test_shared_quick_years_use_current_once_scores_exist():
    assert resolve_quick_import_years(
        [2025, 2026],
        2026,
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": True},
    ) == [2026]


def test_shared_quick_years_include_previous_for_offseason_week_zero_shell():
    assert resolve_quick_import_years(
        [2025, 2026],
        2026,
        {
            "league_season": "2026",
            "previous_season": "2025",
            "season_has_scores": True,
            "season_type": "off",
            "week": 0,
            "leg": 0,
            "display_week": 0,
        },
    ) == [2025, 2026]


def test_shared_quick_years_can_include_prior_available_for_empty_shell_fallback():
    assert resolve_quick_import_years(
        [2021, 2022],
        2022,
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
        include_previous_available=True,
    ) == [2021, 2022]


def test_shared_quick_years_do_not_include_prior_available_by_default():
    assert resolve_quick_import_years(
        [2021, 2022],
        2022,
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
    ) == [2022]


def test_unscored_current_shell_years_identifies_only_current_shell():
    assert unscored_current_shell_years(
        [2025, 2026],
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
    ) == {2026}


def test_unscored_current_shell_years_handles_sleeper_offseason_state():
    assert unscored_current_shell_years(
        [2025, 2026],
        {
            "league_season": "2026",
            "previous_season": "2025",
            "season_has_scores": True,
            "season_type": "off",
            "week": 0,
            "leg": 0,
            "display_week": 0,
        },
    ) == {2026}


def test_espn_quick_years_use_discovered_previous_scored_year(tmp_path, monkeypatch):
    import espn_initial_import

    monkeypatch.setattr(espn_initial_import, "get_current_nfl_season_year", lambda: 2026)
    ctx = ESPNContext(
        league_id=123,
        league_name="ESPN Test",
        start_year=2026,
        end_year=2026,
        data_directory=tmp_path,
        import_mode="quick",
    )

    assert _resolve_espn_quick_import_years(
        ctx,
        2026,
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
        available_years=[2025],
    ) == [2025, 2026]


def test_espn_quick_years_keep_discovered_prior_year_when_state_metadata_has_scores(tmp_path, monkeypatch):
    import espn_initial_import

    monkeypatch.setattr(espn_initial_import, "get_current_nfl_season_year", lambda: 2026)
    ctx = ESPNContext(
        league_id=123,
        league_name="ESPN Metadata Drift",
        start_year=2026,
        end_year=2026,
        data_directory=tmp_path,
        import_mode="quick",
    )

    assert _resolve_espn_quick_import_years(
        ctx,
        2026,
        {"league_season": "2026", "season_has_scores": True},
        available_years=[2025],
    ) == [2025, 2026]


def test_espn_quick_years_seed_previous_scored_year_without_discovery(tmp_path, monkeypatch):
    import espn_initial_import

    monkeypatch.setattr(espn_initial_import, "get_current_nfl_season_year", lambda: 2026)
    ctx = ESPNContext(
        league_id=123,
        league_name="ESPN Startup",
        start_year=2026,
        end_year=2026,
        data_directory=tmp_path,
        import_mode="quick",
    )

    assert _resolve_espn_quick_import_years(
        ctx,
        2026,
        {"league_season": "2026", "previous_season": "2025", "season_has_scores": False},
    ) == [2025, 2026]


def test_espn_context_quick_years_bypass_preseason_date_cap(tmp_path):
    ctx = ESPNContext(
        league_id=123,
        league_name="ESPN Startup",
        start_year=2026,
        end_year=2026,
        data_directory=tmp_path,
        import_mode="quick",
        quick_import_years=[2026],
    )

    assert list(ctx.get_year_range()) == [2026]


def test_espn_full_history_discovery_uses_the_league_id_mapped_to_each_year(tmp_path, monkeypatch):
    import espn_initial_import

    probes: list[tuple[int, tuple[int, ...]]] = []

    class _Client:
        def __init__(self, league_id, *_args):
            self.league_id = league_id

        def discover_available_years(self, *, years):
            probes.append((self.league_id, tuple(years)))
            return list(years)

    monkeypatch.setattr(espn_initial_import, "ESPNAPIClient", _Client)
    ctx = ESPNContext(
        league_id=200,
        league_name="Mapped ESPN history",
        start_year=2013,
        end_year=2014,
        league_ids={"2013": 100, "2014": 200},
        data_directory=tmp_path,
    )

    assert _discover_espn_import_years(ctx) == [2013, 2014]
    assert probes == [(100, (2013,)), (200, (2014,))]
