import pytest
from pathlib import Path

from scripts.yahoo_cookie_runner import _load_league_keys


def test_cookie_runner_requires_explicit_league_keys():
    with pytest.raises(ValueError, match="explicit Yahoo league keys"):
        _load_league_keys(None)


def test_cookie_runner_parses_long_workflow_year_key_mapping_without_path_probe(monkeypatch):
    workflow_value = (
        "{2009:223.l.25022,2010:242.l.302674,2011:257.l.186986,2012:273.l.152394,"
        "2013:314.l.42132,2014:331.l.88881,2015:348.l.52980,2016:359.l.20924,"
        "2017:371.l.19686,2018:380.l.62205,2019:390.l.17671,2020:399.l.27360,"
        "2021:406.l.31250,2022:414.l.28879,2023:423.l.1420,2024:449.l.23041,"
        "2025:461.l.13925,2026:470.l.20679}"
    )

    def reject_path_probe(self):
        raise OSError("workflow mapping is not a filesystem path")

    monkeypatch.setattr(Path, "is_file", reject_path_probe)

    assert _load_league_keys(workflow_value) == {
        2009: "223.l.25022",
        2010: "242.l.302674",
        2011: "257.l.186986",
        2012: "273.l.152394",
        2013: "314.l.42132",
        2014: "331.l.88881",
        2015: "348.l.52980",
        2016: "359.l.20924",
        2017: "371.l.19686",
        2018: "380.l.62205",
        2019: "390.l.17671",
        2020: "399.l.27360",
        2021: "406.l.31250",
        2022: "414.l.28879",
        2023: "423.l.1420",
        2024: "449.l.23041",
        2025: "461.l.13925",
        2026: "470.l.20679",
    }
