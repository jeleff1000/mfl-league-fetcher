import sys
from pathlib import Path


SCRIPT_ROOT = Path(__file__).resolve().parents[3]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient, ESPNAPIError
from multi_league.data_fetchers.espn.espn_context import ESPNContext
from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.data_fetchers.espn.espn_league_settings import fetch_and_save_all_settings


class _FakeLeague:
    current_week = 17
    teams = [object()]


class _ScopedFailureClient(ESPNAPIClient):
    def __init__(self):
        super().__init__(league_id=123)
        self.probed_years = []

    def get_league(self, year: int):
        self.probed_years.append(year)
        if year == 2014:
            return _FakeLeague()
        raise ESPNAPIError(f"private for {year}", status_code=401)


def test_espn_discovery_scoped_years_do_not_early_stop_after_recent_failures():
    client = _ScopedFailureClient()

    available_years = client.discover_available_years(years=[2019, 2018, 2017, 2016, 2015, 2014])

    assert available_years == [2014]
    assert client.probed_years == [2019, 2018, 2017, 2016, 2015, 2014]


def test_espn_context_uses_explicit_import_years_instead_of_min_max_range(tmp_path):
    ctx = ESPNContext(
        league_id=123,
        league_name="Scoped ESPN",
        start_year=2014,
        end_year=2019,
        explicit_import_years=[2014, 2017, 2019],
        data_directory=tmp_path,
    )

    assert ctx.get_year_range() == [2014, 2017, 2019]


def test_espn_context_preserves_explicit_current_season_for_full_import(tmp_path):
    current_season = get_current_nfl_season_year()
    ctx = ESPNContext(
        league_id=123,
        league_name="Startup ESPN",
        start_year=current_season,
        end_year=current_season,
        data_directory=tmp_path,
        import_mode="full",
    )

    assert list(ctx.get_year_range()) == [current_season]


def test_espn_settings_fetch_returns_empty_for_no_target_years():
    class _EmptyYearsContext:
        def get_year_range(self):
            return []

    assert fetch_and_save_all_settings(_EmptyYearsContext()) == {}
