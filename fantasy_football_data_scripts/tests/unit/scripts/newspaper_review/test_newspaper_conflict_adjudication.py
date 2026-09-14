from __future__ import annotations

import json
from pathlib import Path

import duckdb

from scripts.newspaper_review import newspaper_conflict_adjudication as adjudication
from scripts.newspaper_review.newspaper_conflict_adjudication import (
    adjudicate_one,
    canonical_score,
    norm,
)


def _row(field: str, values: list[str], value_sources: dict, box: str = "193512010was",
         key: str = "") -> dict:
    return {
        "audit_conflict_id": "t1",
        "surface": "game_candidate",
        "boxscore_id": box,
        "field_name": field,
        "comparison_key": key or f"{box}|{field}",
        "values_json": json.dumps(values),
        "value_sources_json": json.dumps(value_sources),
    }


def _write_parquet(
    con: duckdb.DuckDBPyConnection,
    path: Path,
    table_name: str,
    columns: list[tuple[str, str]],
    rows: list[tuple],
) -> None:
    definitions = ", ".join(f"{name} {data_type}" for name, data_type in columns)
    con.execute(f"CREATE TABLE {table_name} ({definitions})")
    if rows:
        placeholders = ", ".join("?" for _ in columns)
        con.executemany(f"INSERT INTO {table_name} VALUES ({placeholders})", rows)
    con.execute(f"COPY {table_name} TO '{path.as_posix()}' (FORMAT PARQUET)")


def _lake_witness_fixture(
    tmp_path: Path,
    monkeypatch,
    *,
    boxscore_ids: tuple[str, ...] = ("b1",),
    team_game_rows: list[tuple] | None = None,
    game_info_rows: list[tuple] | None = None,
    team_stat_rows: list[tuple] | None = None,
) -> dict:
    con = duckdb.connect()
    paths = {
        "TEAM_GAMES": tmp_path / "team_games.parquet",
        "GAME_INFO": tmp_path / "game_info.parquet",
        "TEAM_STATS": tmp_path / "team_stats.parquet",
        "SCORING": tmp_path / "scoring.parquet",
    }
    _write_parquet(
        con,
        paths["TEAM_GAMES"],
        "team_games_fixture",
        [
            ("boxscore_id", "VARCHAR"),
            ("team_code", "VARCHAR"),
            ("team_points", "INTEGER"),
            ("opponent_code", "VARCHAR"),
            ("opponent_points", "INTEGER"),
            ("is_home", "BOOLEAN"),
        ],
        team_game_rows or [],
    )
    _write_parquet(
        con,
        paths["GAME_INFO"],
        "game_info_fixture",
        [("boxscore_id", "VARCHAR"), ("info", "VARCHAR"), ("stat", "VARCHAR")],
        game_info_rows or [],
    )
    _write_parquet(
        con,
        paths["TEAM_STATS"],
        "team_stats_fixture",
        [
            ("boxscore_id", "VARCHAR"),
            ("stat", "VARCHAR"),
            ("vis_stat", "VARCHAR"),
            ("home_stat", "VARCHAR"),
        ],
        team_stat_rows or [],
    )
    _write_parquet(
        con,
        paths["SCORING"],
        "scoring_fixture",
        [
            ("boxscore_id", "VARCHAR"),
            ("description", "VARCHAR"),
            ("description_link_ids", "VARCHAR"),
        ],
        [],
    )
    for name, path in paths.items():
        monkeypatch.setattr(adjudication, name, path)
    try:
        return adjudication.lake_witnesses(con, list(boxscore_ids))
    finally:
        con.close()


def test_norm_keeps_integer_zero():
    # regression: `str(value or "")` erased every shutout side
    assert norm(0) == "0"
    assert norm("3,000") == "3000"
    assert norm(None) == ""


def test_canonical_score_keeps_shutouts_and_orders_pairs():
    assert canonical_score("RII", 3, "GNB", 0) == "gnb=0|rii=3"
    assert canonical_score("gnb", 0, "rii", 3) == "gnb=0|rii=3"


def test_external_witness_resolves_single_match():
    wit = {"scores": {"b1": {"canonical": "gnb=0|rii=3",
                               "by_team": {"gnb": "0", "rii": "3"}}},
           "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("final_score", ["gnb=0|rii=3", "gnb=3|rii=0"],
               {"gnb=0|rii=3": ["docA"], "gnb=3|rii=0": ["docB"]}, box="b1")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "RESOLVED_EXTERNAL"
    assert out["resolved_value"] == "gnb=0|rii=3"


def test_external_witness_escalates_when_no_value_matches():
    wit = {"scores": {"b1": {"canonical": "bos=13|pit=3",
                               "by_team": {"bos": "13", "pit": "3"}}},
           "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("final_score", ["bos=13|pit=6", "bos=13|pit=7"],
               {"bos=13|pit=6": ["docA"], "bos=13|pit=7": ["docB"]}, box="b1")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "ESCALATED_NO_MATCH"


def test_team_keyed_scalar_score_matches_only_that_team():
    wit = {"scores": {"b1": {"canonical": "crd=6|gnb=14",
                               "by_team": {"crd": "6", "gnb": "14"}}},
           "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("final_score", ["0", "6"], {"0": ["docA"], "6": ["docB"]},
               box="b1", key="b1|final_score|crd")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "RESOLVED_EXTERNAL"
    assert out["resolved_value"] == "6"


def test_identityless_scalar_score_does_not_match_either_team_side():
    wit = {"scores": {"b1": {"canonical": "crd=6|gnb=14",
                               "by_team": {"crd": "6", "gnb": "14"}}},
           "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("final_score", ["0", "6"], {"0": ["docA"], "6": ["docB"]},
               box="b1")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "HELD_TIE"
    assert out["resolved_value"] == ""


def test_conflicting_score_pairs_and_cross_row_fragments_provide_no_coverage(
    tmp_path,
    monkeypatch,
):
    wit = _lake_witness_fixture(
        tmp_path,
        monkeypatch,
        boxscore_ids=("b1", "b2"),
        team_game_rows=[
            ("b1", "PIT", 10, "WAS", 12, True),
            ("b1", "PIT", 14, "WAS", 7, True),
            ("b2", "PIT", 10, "WAS", None, True),
            ("b2", "PIT", None, "WAS", 12, True),
        ],
    )
    assert "b1" not in wit["scores"]
    assert "b2" not in wit["scores"]
    row = _row(
        "final_score",
        ["pit=10|was=12", "pit=14|was=7"],
        {"pit=10|was=12": ["docA"], "pit=14|was=7": ["docB"]},
        box="b1",
    )
    assert adjudicate_one(row, wit)["verdict"] == "HELD_TIE"


def test_conflicting_attendance_candidates_provide_no_coverage(tmp_path, monkeypatch):
    wit = _lake_witness_fixture(
        tmp_path,
        monkeypatch,
        game_info_rows=[
            ("b1", "attendance", "10000"),
            ("b1", "attendance", "12000"),
        ],
    )
    assert "b1" not in wit["attendance"]
    row = _row(
        "attendance",
        ["10000", "12000"],
        {"10000": ["docA"], "12000": ["docB"]},
        box="b1",
    )
    assert adjudicate_one(row, wit)["verdict"] == "HELD_TIE"


def test_conflicting_first_down_candidates_provide_no_team_coverage(
    tmp_path,
    monkeypatch,
):
    wit = _lake_witness_fixture(
        tmp_path,
        monkeypatch,
        team_game_rows=[("b1", "PIT", 12, "WAS", 10, True)],
        team_stat_rows=[
            ("b1", "First Downs", "10", "12"),
            ("b1", "First Downs", "11", "13"),
        ],
    )
    assert "b1" not in wit["first_downs"]
    row = _row(
        "first_downs",
        ["12", "13"],
        {"12": ["docA"], "13": ["docB"]},
        box="b1",
        key="b1|first_downs|pit",
    )
    assert adjudicate_one(row, wit)["verdict"] == "HELD_TIE"


def test_first_downs_resolve_only_for_the_team_in_the_comparison_key():
    wit = {"scores": {}, "attendance": {},
           "first_downs": {"b1": {"was": "10", "pit": "12"}}, "events": {}}
    row = _row("first_downs", ["10", "12"], {"10": ["docA"], "12": ["docB"]},
               box="b1", key="b1|first_downs|pit")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "RESOLVED_EXTERNAL"
    assert out["resolved_value"] == "12"


def test_legacy_first_downs_tuple_does_not_guess_a_team_side():
    wit = {"scores": {}, "attendance": {},
           "first_downs": {"b1": ("10", "12")}, "events": {}}
    row = _row("first_downs", ["10", "12"], {"10": ["docA"], "12": ["docB"]},
               box="b1", key="b1|first_downs|pit")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "HELD_TIE"
    assert out["resolved_value"] == ""


def test_majority_only_without_external_witness():
    wit = {"scores": {}, "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("attendance", ["10000", "14000"],
               {"10000": ["docA", "docB"], "14000": ["docC"]}, box="nocover")
    out = adjudicate_one(row, wit)
    assert out["verdict"] == "RESOLVED_MAJORITY"
    assert out["resolved_value"] == "10000"


def test_tie_without_witness_is_held():
    wit = {"scores": {}, "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("attendance", ["10000", "14000"],
               {"10000": ["docA"], "14000": ["docB"]}, box="nocover")
    assert adjudicate_one(row, wit)["verdict"] == "HELD_TIE"


def test_narrative_fields_never_promote():
    wit = {"scores": {}, "attendance": {}, "first_downs": {}, "events": {}}
    row = _row("western_division_race_status", ["a", "b"],
               {"a": ["docA"], "b": ["docB"]})
    assert adjudicate_one(row, wit)["verdict"] == "NON_ATOM"
