"""Unit tests for the keeper_config DDL migration script.

Mocks FlyReader/FlyWriter — no live Fly access required.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from migrate_keeper_config_to_ddl import (  # noqa: E402
    MigrationResult,
    SkipReason,
    migrate_centralized,
)


@pytest.fixture
def fake_reader():
    return MagicMock()


@pytest.fixture
def fake_writer():
    return MagicMock()


@pytest.fixture
def snapshot_writer(tmp_path):
    f = tmp_path / "snapshot.jsonl"
    return open(f, "w")


def test_skips_league_without_keeper_config_table(fake_reader, fake_writer, snapshot_writer):
    fake_reader.query_df.return_value = pd.DataFrame()
    result = migrate_centralized(fake_reader, fake_writer, snapshot_writer)
    assert result == SkipReason.no_table


def test_skips_when_already_migrated(fake_reader, fake_writer, snapshot_writer):
    # Schema query: no rules_json column
    fake_reader.query_df.return_value = pd.DataFrame({"column_name": ["db_name", "year", "enabled"]})
    result = migrate_centralized(fake_reader, fake_writer, snapshot_writer)
    assert result == SkipReason.already_migrated


def test_full_migration_runs_alter_then_updates_then_drop(fake_reader, fake_writer, snapshot_writer):
    # First call returns schema; second returns rows
    fake_reader.query_df.side_effect = [
        pd.DataFrame({"column_name": ["db_name", "year", "rules_json", "updated_at"]}),
        pd.DataFrame(
            [
                {
                    "db_name": "foo",
                    "year": 0,
                    "rules_json": '{"enabled": true, "draft_type": "snake"}',
                    "updated_at": "2026-01-01",
                },
            ]
        ),
    ]
    result = migrate_centralized(fake_reader, fake_writer, snapshot_writer)
    assert isinstance(result, MigrationResult)
    assert result.rows_migrated == 1
    assert result.invalid == 0
    # Snapshot was written
    snapshot_writer.flush()
    snapshot_path = Path(snapshot_writer.name)
    snapshot_writer.close()
    lines = snapshot_path.read_text().strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["db_name"] == "foo"
    assert rec["year"] == 0


def test_malformed_json_logged_and_marked_invalid(fake_reader, fake_writer, snapshot_writer):
    fake_reader.query_df.side_effect = [
        pd.DataFrame({"column_name": ["db_name", "year", "rules_json", "updated_at"]}),
        pd.DataFrame(
            [
                {
                    "db_name": "foo",
                    "year": 0,
                    "rules_json": "not valid json{{",
                    "updated_at": "2026-01-01",
                },
            ]
        ),
    ]
    result = migrate_centralized(fake_reader, fake_writer, snapshot_writer)
    assert isinstance(result, MigrationResult)
    assert result.invalid == 1
    assert result.rows_migrated == 0  # invalid rows do NOT count as migrated
