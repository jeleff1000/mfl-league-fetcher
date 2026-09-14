"""Unit tests for staging_reader (Fly-backed staging I/O)."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pandas as pd
import pytest

from multi_league.data_fetchers.shared import staging_reader


@pytest.fixture
def mock_reader(monkeypatch):
    reader = MagicMock()
    monkeypatch.setattr(staging_reader, "_get_reader", lambda: reader)
    return reader


@pytest.fixture
def mock_writer(monkeypatch):
    writer = MagicMock()
    monkeypatch.setattr(staging_reader, "_get_writer", lambda: writer)
    return writer


def _tables_response(names):
    return [{"table_name": n} for n in names]


def test_read_returns_empty_dict_when_no_tables(mock_reader):
    mock_reader.query.return_value = []
    result = staging_reader.read_staging_data("kmffl")
    assert result == {}


def test_read_returns_settings_and_tabular(mock_reader):
    settings_payload = {
        "platform": "yahoo",
        "league_key": "449.l.198278",
        "num_teams": 12,
    }
    mock_reader.query.side_effect = [
        _tables_response(
            [
                "staging_settings",
                "staging_matchup",
                "staging_player",
            ]
        ),
    ]
    mock_reader.query_df.side_effect = [
        pd.DataFrame(
            [
                {
                    "year": 2013,
                    "raw_settings_json": json.dumps(settings_payload),
                    "filename": "league_settings_2013.json",
                }
            ]
        ),
        pd.DataFrame([{"year": 2013, "week": 1, "manager": "Alice"}]),
        pd.DataFrame([{"year": 2013, "week": 1, "player": "Someone"}]),
    ]

    result = staging_reader.read_staging_data("kmffl")

    assert set(result.keys()) == {"settings", "matchup", "player"}
    assert result["settings"] == [
        {
            "year": 2013,
            "settings_json": settings_payload,
            "filename": "league_settings_2013.json",
        }
    ]
    assert isinstance(result["matchup"], pd.DataFrame) and len(result["matchup"]) == 1
    assert isinstance(result["player"], pd.DataFrame) and len(result["player"]) == 1


def test_read_handles_legacy_settings_json_column(mock_reader):
    payload = {"platform": "yahoo", "league_key": "x.l.1"}
    mock_reader.query.side_effect = [_tables_response(["staging_settings"])]
    mock_reader.query_df.side_effect = [
        pd.DataFrame(
            [
                {
                    "year": 2014,
                    "settings_json": json.dumps(payload),
                    "filename": "legacy.json",
                }
            ]
        )
    ]
    result = staging_reader.read_staging_data("kmffl")
    assert result["settings"][0]["settings_json"] == payload


def test_read_with_table_name_returns_dataframe(mock_reader):
    mock_reader.query.side_effect = [_tables_response(["staging_matchup"])]
    mock_reader.query_df.side_effect = [pd.DataFrame([{"year": 2013, "week": 1}, {"year": 2014, "week": 1}])]
    result = staging_reader.read_staging_data("kmffl", table_name="matchup")
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_read_with_table_name_missing_returns_none(mock_reader):
    mock_reader.query.return_value = []
    result = staging_reader.read_staging_data("kmffl", table_name="matchup")
    assert result is None


def test_read_filters_by_db_name(mock_reader):
    """Every read SQL must filter by db_name so leagues can't see each other's staging."""
    mock_reader.query.side_effect = [_tables_response(["staging_matchup"])]
    mock_reader.query_df.side_effect = [pd.DataFrame()]

    staging_reader.read_staging_data("kmffl", table_name="matchup")

    sql = mock_reader.query_df.call_args[0][0]
    assert "WHERE db_name = 'kmffl'" in sql
    # And it must target the shared ___leagues.staging schema.
    assert "___leagues.staging.staging_matchup" in sql


def test_read_with_year_filter_adds_and_clause(mock_reader):
    mock_reader.query.side_effect = [_tables_response(["staging_matchup"])]
    mock_reader.query_df.side_effect = [pd.DataFrame()]

    staging_reader.read_staging_data("kmffl", table_name="matchup", year=2013)

    sql = mock_reader.query_df.call_args[0][0]
    assert "WHERE db_name = 'kmffl'" in sql
    assert "AND year = 2013" in sql


def test_write_settings_writes_files(tmp_path, mock_reader):
    payload = {"platform": "yahoo", "league_key": "449.l.198278", "num_teams": 12}
    mock_reader.query.side_effect = [_tables_response(["staging_settings"])]
    mock_reader.query_df.side_effect = [
        pd.DataFrame(
            [
                {
                    "year": 2013,
                    "raw_settings_json": json.dumps(payload),
                    "filename": "ignored",
                }
            ]
        )
    ]

    years = staging_reader.write_staging_settings_to_files("kmffl", str(tmp_path))

    assert years == [2013]
    written = list(tmp_path.glob("league_settings_2013_*.json"))
    assert len(written) == 1
    loaded = json.loads(written[0].read_text())
    assert loaded["league_key"] == "449.l.198278"
    assert loaded["year"] == 2013
    assert "fetched_at" in loaded


def test_pre_fetch_staging_settings_wraps_write(tmp_path, mock_reader):
    payload = {"platform": "yahoo", "league_key": "x.l.1"}
    mock_reader.query.side_effect = [_tables_response(["staging_settings"])]
    mock_reader.query_df.side_effect = [
        pd.DataFrame([{"year": 2013, "raw_settings_json": json.dumps(payload), "filename": "f"}])
    ]
    ctx = SimpleNamespace(league_name="kmffl", data_directory=str(tmp_path))
    logs = []

    years = staging_reader.pre_fetch_staging_settings(ctx, log_func=logs.append)

    assert years == {2013}
    assert (tmp_path / "league_settings").exists()


def test_pre_fetch_staging_settings_returns_empty_when_no_staging(tmp_path, mock_reader):
    mock_reader.query.return_value = []
    ctx = SimpleNamespace(league_name="empty_db", data_directory=str(tmp_path))
    assert staging_reader.pre_fetch_staging_settings(ctx) == set()


def test_pre_fetch_staging_settings_skips_storage_in_corpus_mode(tmp_path, monkeypatch):
    monkeypatch.setenv("CORPUS_MODE", "1")
    ctx = SimpleNamespace(league_name="smpl_offline", data_directory=str(tmp_path))
    called = False

    def unexpected_write(*_args, **_kwargs):
        nonlocal called
        called = True
        raise AssertionError("corpus mode must not read centralized staging settings")

    monkeypatch.setattr(staging_reader, "write_staging_settings_to_files", unexpected_write)

    assert staging_reader.pre_fetch_staging_settings(ctx) == set()
    assert called is False


def test_clear_staging_tables_deletes_this_leagues_rows(mock_reader, mock_writer):
    """clear_staging_tables must DELETE WHERE db_name = ? for every existing
    staging table so other leagues' rows survive."""
    mock_reader.query.return_value = _tables_response(
        [
            "staging_matchup",
            "staging_player",
            "staging_draft",
            "staging_transactions",
            "staging_schedule",
            "staging_settings",
        ]
    )
    staging_reader.clear_staging_tables("kmffl")

    executed = [call.args[0] for call in mock_writer.execute.call_args_list]
    for table in (
        "staging_matchup",
        "staging_player",
        "staging_draft",
        "staging_transactions",
        "staging_schedule",
        "staging_settings",
    ):
        assert any(
            f"DELETE FROM ___leagues.staging.{table}" in s and "WHERE db_name = 'kmffl'" in s for s in executed
        ), f"expected DELETE for {table} scoped to db_name='kmffl'"


def test_clear_staging_tables_skips_missing_tables(mock_reader, mock_writer):
    """Missing staging tables must be skipped, not DELETE'd (Fly 500s on missing)."""
    mock_reader.query.return_value = _tables_response(["staging_matchup", "staging_settings"])
    staging_reader.clear_staging_tables("kmffl")

    executed = [call.args[0] for call in mock_writer.execute.call_args_list]
    # Only the two existing tables get DELETE statements.
    assert len(executed) == 2
    assert any("DELETE FROM ___leagues.staging.staging_matchup" in s for s in executed)
    assert any("DELETE FROM ___leagues.staging.staging_settings" in s for s in executed)
    # And no DELETE was issued against missing tables.
    for missing in ("staging_player", "staging_draft", "staging_transactions", "staging_schedule"):
        assert not any(f"staging.{missing}" in s for s in executed), f"should skip {missing}"
