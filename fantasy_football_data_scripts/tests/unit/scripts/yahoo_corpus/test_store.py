from __future__ import annotations

import hashlib
import json

import pyarrow.parquet as pq

from scripts.yahoo_corpus.store import CensusStore


def _league_row() -> dict:
    return {
        "league_key": "461.l.12345",
        "league_id": "12345",
        "game_key": "461",
        "season": 2025,
        "league_name": "Private Fixture League",
        "fetch_status": "success",
        "classification_status": "classified",
        "cohort_slug": "12t_flx_ppr_4pt",
        "settings_sha256": "abc",
        "grant_id": "grant-0001",
    }


def test_write_xml_uses_deterministic_path_and_hash(tmp_path) -> None:
    store = CensusStore(tmp_path)
    xml = "<fantasy_content>\nfixture\n</fantasy_content>"

    first = store.write_xml("461.l.12345", xml)
    second = store.write_xml("461.l.12345", xml)

    expected = tmp_path / "raw" / "461" / "12345.xml"
    assert first.path == expected
    assert second.path == expected
    assert expected.read_text(encoding="utf-8") == xml
    assert first.sha256 == hashlib.sha256(expected.read_bytes()).hexdigest()
    assert second.sha256 == first.sha256


def test_checkpoint_survives_restart_and_deduplicates_league_key(tmp_path) -> None:
    store = CensusStore(tmp_path)
    store.record_league(_league_row())
    changed = _league_row() | {"cohort_slug": "12t_flx_half_4pt"}
    store.record_league(changed)
    store.flush()

    restarted = CensusStore(tmp_path)

    assert restarted.is_complete("461.l.12345")
    assert len(restarted.league_rows) == 1
    assert restarted.league_rows["461.l.12345"]["cohort_slug"] == "12t_flx_half_4pt"


def test_flush_separates_private_manifest_from_public_summary(tmp_path) -> None:
    store = CensusStore(tmp_path)
    row = _league_row()
    store.record_league(row)
    store.record_credential(
        {
            "grant_id": "grant-0001",
            "status": "success",
            "account_hash": "private-account-hash",
            "credential_rows": 2,
        }
    )
    store.flush()

    private = pq.read_table(tmp_path / "league_year_manifest_private.parquet").to_pylist()
    public = pq.read_table(tmp_path / "league_year_manifest.parquet").to_pylist()
    summary_text = (tmp_path / "summary.json").read_text(encoding="utf-8")
    summary = json.loads(summary_text)

    assert private[0]["league_name"] == "Private Fixture League"
    assert "league_name" not in public[0]
    assert "grant_id" not in public[0]
    assert "Private Fixture League" not in summary_text
    assert "private-account-hash" not in summary_text
    assert summary["credential_rows"] == 2
    assert summary["unique_grants"] == 1
    assert summary["successful_grants"] == 1
    assert summary["unique_league_years"] == 1
    assert summary["cohort_counts"] == {"12t_flx_ppr_4pt": 1}


def test_credential_status_requires_closed_vocabulary(tmp_path) -> None:
    store = CensusStore(tmp_path)

    try:
        store.record_credential({"grant_id": "grant-1", "status": "maybe", "credential_rows": 1})
    except ValueError as exc:
        assert "credential status" in str(exc)
    else:
        raise AssertionError("invalid credential status should fail")
