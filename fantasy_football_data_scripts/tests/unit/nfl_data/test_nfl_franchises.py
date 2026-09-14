import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from nfl_data.nfl_franchises import get_nfl_franchise_number


def test_reused_abbreviations_resolve_by_year_and_source_code():
    assert get_nfl_franchise_number("BAL", 1980) == 26
    assert get_nfl_franchise_number("BAL", 2000) == 21

    assert get_nfl_franchise_number("CLE", 1994) == 23
    assert get_nfl_franchise_number("CLE", 2000) == 23

    assert get_nfl_franchise_number("HOU", 1998) == 28
    assert get_nfl_franchise_number("HOU", 2002) == 25

    assert get_nfl_franchise_number("DAL", 1960) == 1
    assert get_nfl_franchise_number("DTX", 1960) == 30
