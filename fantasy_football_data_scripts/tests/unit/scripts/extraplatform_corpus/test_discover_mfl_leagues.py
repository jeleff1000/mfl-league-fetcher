import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[5] / "scripts" / "extraplatform_corpus" / "discover_mfl_leagues.py"
SPEC = importlib.util.spec_from_file_location("discover_mfl_leagues", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_parse_id_bands_supports_multiple_season_strata():
    assert MODULE.parse_id_bands("10000-19999,20000-29999") == [
        (10000, 19999),
        (20000, 29999),
    ]


def test_stratified_ids_are_deterministic_and_stay_in_band():
    ids = MODULE.stratified_ids([(100, 109), (200, 209)], per_band=3, seed=7)
    assert ids == MODULE.stratified_ids([(100, 109), (200, 209)], per_band=3, seed=7)
    assert len(ids) == 6
    assert all(100 <= value <= 109 or 200 <= value <= 209 for value in ids)


def test_dedupe_keeps_each_season_league_pair_and_latest_record_for_pair():
    records = [
        {"id": "123", "year": 2020, "name": "old"},
        {"id": "123", "year": 2020, "name": "new"},
        {"id": "123", "year": 2021, "name": "next"},
    ]
    result = MODULE.dedupe_season_records(records)
    assert {(row["year"], row["id"]) for row in result} == {
        (2020, "123"),
        (2021, "123"),
    }
    assert next(row for row in result if row["year"] == 2020)["name"] == "new"
