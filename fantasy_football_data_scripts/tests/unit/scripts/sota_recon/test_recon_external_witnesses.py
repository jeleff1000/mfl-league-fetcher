from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

from scripts.sota_recon.recon_external_witnesses import (
    apply_proposed_bio_patches,
    build_parser,
    external_resolution_ledger,
    main,
    merge_resolution_ledgers,
    reconcile_external_witnesses,
    team_score,
)


def test_team_score_normalizes_historical_code_and_full_name() -> None:
    assert team_score("CHI", "Chicago Staleys") == 1.0
    assert team_score("BOS", "Boston Redskins") == 1.0


def test_external_resolution_ledger_preserves_provenance_and_contract() -> None:
    decisions = pd.DataFrame(
        [
            {
                "task_id": "hardhold-17",
                "boxscore_id": "192110020chi",
                "year": 1921,
                "raw_team": "CHI",
                "raw_player": "Halas",
                "resolution_status": "resolved_existing_bio",
                "resolved_NFL_player_id": "HalaGe20",
                "resolved_player": "George Halas",
                "bio_candidate_count": 1,
                "witness_candidate_count": 3,
                "witness_sources_json": '["nflcom", "statscrew"]',
                "witness_urls_json": '["https://nfl.test/halas"]',
            },
            {"task_id": "held", "resolution_status": "held_no_candidate"},
        ]
    )

    ledger = external_resolution_ledger(decisions)

    assert ledger["identity_task_id"].tolist() == ["hardhold-17"]
    assert ledger.iloc[0]["verdict"] == "resolved_external_witness_corroborated"
    assert ledger.iloc[0]["resolved_nfl_id"] == "HalaGe20"
    assert ledger.iloc[0]["witness_sources_json"] == '["nflcom", "statscrew"]'


def test_merge_resolution_ledgers_upgrades_holds_and_queues_id_conflicts() -> None:
    base = pd.DataFrame(
        [
            {"identity_task_id": "same", "verdict": "resolved_pfr_corroborated", "resolved_nfl_id": "A"},
            {"identity_task_id": "upgrade", "verdict": "hold_unique_but_uncorroborated", "resolved_nfl_id": None},
            {"identity_task_id": "conflict", "verdict": "resolved_pfr_corroborated", "resolved_nfl_id": "C1"},
        ]
    )
    external = pd.DataFrame(
        [
            {"identity_task_id": "same", "verdict": "resolved_external_witness_corroborated", "resolved_nfl_id": "A"},
            {"identity_task_id": "upgrade", "verdict": "resolved_external_witness_corroborated", "resolved_nfl_id": "B"},
            {"identity_task_id": "conflict", "verdict": "resolved_external_witness_corroborated", "resolved_nfl_id": "C2"},
            {"identity_task_id": "new", "verdict": "resolved_external_witness_corroborated", "resolved_nfl_id": "D"},
        ]
    )

    combined, collisions, summary = merge_resolution_ledgers(base, external)

    by_task = combined.set_index("identity_task_id")
    assert by_task.loc["same", "verdict"] == "resolved_pfr_corroborated"
    assert by_task.loc["upgrade", "resolved_nfl_id"] == "B"
    assert by_task.loc["conflict", "resolved_nfl_id"] == "C1"
    assert by_task.loc["new", "resolved_nfl_id"] == "D"
    assert collisions["identity_task_id"].tolist() == ["conflict"]
    assert summary == {"added": 1, "upgraded_holds": 1, "corroborated_existing": 1, "collisions": 1}


def test_merge_resolution_ledgers_compares_ids_in_canonical_bio_space() -> None:
    base = pd.DataFrame(
        [
            {
                "identity_task_id": "same-person",
                "verdict": "resolved_pfr_corroborated",
                "resolved_nfl_id": "WagnSi20",
            }
        ]
    )
    external = pd.DataFrame(
        [
            {
                "identity_task_id": "same-person",
                "verdict": "resolved_external_witness_corroborated",
                "resolved_nfl_id": "HIST-99594812",
            }
        ]
    )

    combined, collisions, summary = merge_resolution_ledgers(
        base, external, id_aliases={"WagnSi20": "HIST-99594812"}
    )

    assert combined.iloc[0]["resolved_nfl_id"] == "WagnSi20"
    assert collisions.empty
    assert summary["corroborated_existing"] == 1


def test_unique_team_name_year_witness_resolves_existing_bio() -> None:
    tasks = pd.DataFrame([{"task_id": "a", "raw_player": "Sies", "raw_team": "Dayton Triangles", "year": 1921}])
    witnesses = pd.DataFrame(
        [{"source": "nflcom", "season": 1921, "team": "dayton-triangles", "player": "Herb Sies", "source_player_id": "herb-sies", "source_url": "https://nfl.test/sies"}]
    )
    bio = pd.DataFrame([{"NFL_player_id": "SiesHe20", "player": "Herb Sies", "first_year": 1920, "last_year": 1925, "pfr_id": "SiesHe20"}])
    decisions, additions, summary = reconcile_external_witnesses(tasks, witnesses, bio)
    assert decisions.iloc[0]["resolution_status"] == "resolved_existing_bio"
    assert decisions.iloc[0]["resolved_NFL_player_id"] == "SiesHe20"
    assert additions.empty
    assert summary["unlocked_tasks"] == 1


def test_newspaper_hardhold_schema_uses_identity_task_id_and_nfl_team() -> None:
    tasks = pd.DataFrame(
        [
            {
                "identity_task_id": "hardhold-17",
                "raw_player": "Sies",
                "nfl_team": "dayton-triangles",
                "year": 1921,
            }
        ]
    )
    witnesses = pd.DataFrame(
        [
            {
                "source": "statscrew",
                "season": 1921,
                "team": "dayton-triangles",
                "player": "Herb Sies",
                "source_player_id": "herb-sies",
                "source_url": "https://statscrew.test/sies",
            }
        ]
    )
    bio = pd.DataFrame(
        [
            {
                "NFL_player_id": "SiesHe20",
                "player": "Herb Sies",
                "first_year": 1920,
                "last_year": 1925,
                "pfr_id": "SiesHe20",
            }
        ]
    )

    decisions, _, summary = reconcile_external_witnesses(tasks, witnesses, bio)

    assert decisions.iloc[0]["task_id"] == "hardhold-17"
    assert decisions.iloc[0]["raw_team"] == "dayton-triangles"
    assert decisions.iloc[0]["resolved_NFL_player_id"] == "SiesHe20"
    assert summary["unlocked_tasks"] == 1


def test_spelling_variant_and_proposed_id_create_reproducible_bio_addition() -> None:
    tasks = pd.DataFrame([{"task_id": "b", "raw_player": "Chamberlain", "raw_team": "Chicago Staleys", "year": 1921, "nid": "ChamGu20"}])
    witnesses = pd.DataFrame(
        [{"source": "nflcom", "season": 1921, "team": "chicago-staleys", "player": "Guy Chamberlin", "source_player_id": "guy-chamberlin", "source_url": "https://nfl.test/chamberlin"}]
    )
    bio = pd.DataFrame(columns=["NFL_player_id", "player", "first_year", "last_year", "pfr_id"])
    decisions, additions, summary = reconcile_external_witnesses(tasks, witnesses, bio)
    assert decisions.iloc[0]["resolution_status"] == "resolved_new_bio"
    assert additions.iloc[0]["NFL_player_id"] == "ChamGu20"
    assert additions.iloc[0]["player"] == "Guy Chamberlin"
    assert summary["bio_additions"] == 1


def test_multiple_bio_candidates_remain_held() -> None:
    tasks = pd.DataFrame([{"task_id": "c", "raw_player": "King", "raw_team": "Chicago Staleys", "year": 1921}])
    witnesses = pd.DataFrame(
        [{"source": "nflcom", "season": 1921, "team": "chicago-staleys", "player": "John King", "source_player_id": "john-king", "source_url": "https://nfl.test/king"}]
    )
    bio = pd.DataFrame(
        [
            {"NFL_player_id": "KingJo20", "player": "John King", "first_year": 1920, "last_year": 1922},
            {"NFL_player_id": "KingJo21", "player": "John King", "first_year": 1921, "last_year": 1924},
        ]
    )
    decisions, _, summary = reconcile_external_witnesses(tasks, witnesses, bio)
    assert decisions.iloc[0]["resolution_status"] == "held_multiple_candidates"
    assert summary["unlocked_tasks"] == 0


def test_collision_cli_contract_has_explicit_materialization_and_ledger_paths() -> None:
    args = build_parser().parse_args(
        [
            "--resolve-roster-collisions",
            "--tasks", "collisions.parquet",
            "--player-bio", "bio.parquet",
            "--pfa-materialization", "pfa.parquet",
            "--decision-ledger", "decisions.parquet",
            "--evidence-ledger", "evidence.parquet",
            "--proposed-bio-patch", "patch.parquet",
        ]
    )

    assert args.resolve_roster_collisions is True
    assert args.pfa_materialization == Path("pfa.parquet")


def test_patch_application_is_mutually_exclusive_with_collision_resolution() -> None:
    parser = build_parser()

    with pytest.raises(SystemExit):
        parser.parse_args(
            [
                "--resolve-roster-collisions",
                "--apply-proposed-bio-patch", "patch.parquet",
            ]
        )


def test_collision_cli_writes_decision_evidence_and_proposed_patch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tasks_path = tmp_path / "tasks.parquet"
    bio_path = tmp_path / "bio.parquet"
    pfa_path = tmp_path / "pfa.parquet"
    decision_path = tmp_path / "decisions.parquet"
    evidence_path = tmp_path / "evidence.parquet"
    patch_path = tmp_path / "patch.parquet"
    pd.DataFrame(
        [
            {
                "collision_id": "joe-1",
                "raw_player": "Joe Washington",
                "year": 2001,
                "raw_team": "WAS",
                "position": "RB",
                "nfl_position": "RB",
                "candidate_ids_json": '["00-joe", "WashJo00"]',
            }
        ]
    ).to_parquet(tasks_path, index=False)
    pd.DataFrame(
        [
            {
                "NFL_player_id": "00-joe", "player": "Joe Washington",
                "first_year": 1998, "last_year": 2004, "position": "RB",
                "nfl_position": "RB", "pfr_id": "WashJo00",
            },
            {
                "NFL_player_id": "WashJo00", "player": "Joe Washington",
                "first_year": 1998, "last_year": 2004, "position": "RB",
                "nfl_position": "RB", "gsis_id": "00-joe",
            },
        ]
    ).to_parquet(bio_path, index=False)
    pd.DataFrame(
        columns=[
            "season", "team", "player", "source_player_id", "position",
            "nfl_position", "game_id", "lineage_leaves",
        ]
    ).to_parquet(pfa_path, index=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "recon_external_witnesses",
            "--resolve-roster-collisions",
            "--tasks", str(tasks_path),
            "--player-bio", str(bio_path),
            "--pfa-materialization", str(pfa_path),
            "--decision-ledger", str(decision_path),
            "--evidence-ledger", str(evidence_path),
            "--proposed-bio-patch", str(patch_path),
        ],
    )

    assert main() == 0
    assert pd.read_parquet(decision_path).iloc[0]["canonical_player_id"] == "00-joe"
    assert not pd.read_parquet(evidence_path).empty
    assert "WashJo00" in pd.read_parquet(patch_path).iloc[0]["changes_json"]


def test_collision_cli_reads_parquet_list_struct_lineage_and_emits_every_leaf(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tasks_path = tmp_path / "tasks.parquet"
    bio_path = tmp_path / "bio.parquet"
    pfa_path = tmp_path / "pfa.parquet"
    decision_path = tmp_path / "decisions.parquet"
    evidence_path = tmp_path / "evidence.parquet"
    patch_path = tmp_path / "patch.parquet"
    pd.DataFrame(
        [
            {
                "collision_id": "alex-typed-lineage", "raw_player": "Alex Smith",
                "year": 2005, "raw_team": "WAS", "position": "QB",
                "nfl_position": "QB", "candidate_ids_json": '["00-alex"]',
            }
        ]
    ).to_parquet(tasks_path, index=False)
    pd.DataFrame(
        [
            {
                "NFL_player_id": "00-alex", "player": "Alex Smith",
                "first_year": 2000, "last_year": 2010, "position": "QB",
                "nfl_position": "QB",
            }
        ]
    ).to_parquet(bio_path, index=False)
    pd.DataFrame(
        [
            {
                "season": 2005, "team": "WAS", "player": "Alex Smith",
                "source_player_id": "00-alex", "position": "QB", "nfl_position": "QB",
                "game_id": "2005-was-1",
                "lineage_leaves": [
                    {"shard_id": 4, "manifest_sha256": "4" * 64},
                    {"shard_id": 9, "manifest_sha256": "9" * 64},
                ],
            }
        ]
    ).to_parquet(pfa_path, index=False)
    parquet_lineage = pd.read_parquet(pfa_path).iloc[0]["lineage_leaves"]
    assert not isinstance(parquet_lineage, (list, tuple))
    assert hasattr(parquet_lineage, "tolist")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "recon_external_witnesses", "--resolve-roster-collisions",
            "--tasks", str(tasks_path), "--player-bio", str(bio_path),
            "--pfa-materialization", str(pfa_path),
            "--decision-ledger", str(decision_path),
            "--evidence-ledger", str(evidence_path),
            "--proposed-bio-patch", str(patch_path),
        ],
    )

    assert main() == 0
    assert pd.read_parquet(decision_path).iloc[0]["canonical_player_id"] == "00-alex"
    evidence = pd.read_parquet(evidence_path)
    leaves = evidence.loc[evidence["evidence_type"] == "pfa_lineage_leaf"]
    assert set(zip(leaves["shard_id"], leaves["manifest_sha256"], strict=True)) == {
        (4, "4" * 64),
        (9, "9" * 64),
    }


def test_explicit_patch_application_is_atomic_and_never_overwrites_populated_bio(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bio_path = tmp_path / "bio.parquet"
    patch_path = tmp_path / "patch.parquet"
    original = pd.DataFrame(
        [{"NFL_player_id": "00-alex", "player": "Alex Smith", "nfl_position": None}]
    )
    original.to_parquet(bio_path, index=False)
    pd.DataFrame(
        [
            {
                "collision_id": "alex-1",
                "status": "candidate_patch",
                "player_id": "00-alex",
                "changes_json": '{"player": "Wrong Name", "nfl_position": "QB"}',
                "reason": None,
            }
        ]
    ).to_parquet(patch_path, index=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "recon_external_witnesses",
            "--player-bio", str(bio_path),
            "--apply-proposed-bio-patch", str(patch_path),
        ],
    )

    with pytest.raises(ValueError, match="refuses to overwrite populated field player"):
        main()

    pd.testing.assert_frame_equal(pd.read_parquet(bio_path), original)


def test_alias_patch_application_is_idempotent_and_additive(tmp_path: Path) -> None:
    bio_path = tmp_path / "bio.parquet"
    patch_path = tmp_path / "patch.parquet"
    pd.DataFrame(
        [
            {
                "NFL_player_id": "00-joe", "player": "Joe Washington",
                "id_aliases": '["legacy-joe"]',
            }
        ]
    ).to_parquet(bio_path, index=False)
    pd.DataFrame(
        [
            {
                "collision_id": "joe-1", "status": "candidate_patch",
                "player_id": "00-joe",
                "changes_json": '{"id_aliases": ["WashJo00", "legacy-joe"]}',
                "reason": None,
            }
        ]
    ).to_parquet(patch_path, index=False)

    assert apply_proposed_bio_patches(bio_path, patch_path) == 1
    assert apply_proposed_bio_patches(bio_path, patch_path) == 0
    aliases = pd.read_parquet(bio_path).iloc[0]["id_aliases"]
    assert tuple(aliases) == ("WashJo00", "legacy-joe")
