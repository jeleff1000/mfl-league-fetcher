from dataclasses import replace

import pytest

from scripts.playoff_backfill.evidence_contract import EvidenceRow, dedupe_key, validate_rows


def row(**overrides):
    values = dict(
        db_name="smpl_mfl_1",
        year=2018,
        week=14,
        NFL_player_id="nfl-1",
        made_po_bf=1,
        is_playoffs_bf=1,
        source_platform="mfl",
        evidence_kind="championship_bracket",
        source_id="team-1",
        generated_at="2026-07-27T00:00:00Z",
    )
    values.update(overrides)
    return EvidenceRow(**values)


def test_valid_championship_evidence_has_no_violations():
    assert validate_rows([row()]) == []


def test_playoff_evidence_requires_made_playoffs():
    violations = validate_rows([row(made_po_bf=0, is_playoffs_bf=1)])
    assert any("is_playoffs_bf" in message for message in violations)


def test_duplicate_sidecar_keys_are_rejected():
    violations = validate_rows([row(), row()])
    assert any("duplicate" in message.lower() for message in violations)


def test_consolation_evidence_is_not_valid_championship_evidence():
    violations = validate_rows([row(evidence_kind="consolation")])
    assert any("consolation" in message.lower() for message in violations)


def test_dedupe_key_is_lake_week_player_tuple():
    assert dedupe_key(row()) == ("smpl_mfl_1", 2018, 14, "nfl-1")
