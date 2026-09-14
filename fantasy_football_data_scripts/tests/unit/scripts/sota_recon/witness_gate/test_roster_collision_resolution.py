from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from scripts.sota_recon.witness_gate.identity_lane import IdentityCandidate, IdentityQuery
from scripts.sota_recon.witness_gate.roster_collision_resolution import (
    GSIS_PRIMARY_ERA_START,
    GSIS_PRIMARY_ERA_VERSION,
    build_collision_ledgers,
    propose_collision_bio_patch,
    resolve_roster_collision,
)


def _candidate(player_id: str, name: str = "Alex Smith", **overrides: object) -> IdentityCandidate:
    values: dict[str, object] = {
        "player_id": player_id,
        "name": name,
        "start_year": 2000,
        "end_year": 2010,
        "teams": ("WAS",),
        "position": "QB",
        "nfl_position": "QB",
        "source_ids": (),
        "aliases": (),
        "proof_leaf": f"bio:{player_id}",
    }
    values.update(overrides)
    return IdentityCandidate(**values)


def _pfa(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_duplicate_ids_merge_into_one_person_and_preserve_losing_alias() -> None:
    query = IdentityQuery("Joe Washington", 2001, team="WAS", position="RB", nfl_position="RB")
    gsis = _candidate(
        "00-0000001",
        "Joe Washington",
        position="RB", nfl_position="RB",
        source_ids=("pfr-WashJo00",),
    )
    pfr = _candidate(
        "pfr-WashJo00",
        "Joe Washington",
        position="RB", nfl_position="RB",
        source_ids=("00-0000001",),
    )

    decision = resolve_roster_collision(query, [pfr, gsis], pfa_appearances=_pfa())

    assert decision.status == "resolved"
    assert decision.canonical_player_id == "00-0000001"
    assert decision.id_aliases == ("pfr-WashJo00",)
    assert decision.rule == "gsis_primary_with_pfr_alias"
    assert decision.evidence[0]["era_policy_version"] == GSIS_PRIMARY_ERA_VERSION
    assert {
        row["player_id"]
        for row in decision.evidence
        if row["evidence_type"] == "identity_candidate"
    } == {"00-0000001", "pfr-WashJo00"}


def test_pre_gsis_player_keeps_pfr_primary() -> None:
    year = GSIS_PRIMARY_ERA_START - 1
    query = IdentityQuery("Pat Historic", year, team="CHI", position="QB")
    gsis = _candidate(
        "00-0099999", "Pat Historic", start_year=year - 2, end_year=year + 2,
        teams=("CHI",), source_ids=("HistPa00",),
    )
    pfr = _candidate(
        "HistPa00", "Pat Historic", start_year=year - 2, end_year=year + 2,
        teams=("CHI",), source_ids=(),
    )

    decision = resolve_roster_collision(query, [gsis, pfr], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "HistPa00"
    assert decision.id_aliases == ("00-0099999",)
    assert decision.rule == "pfr_primary_before_gsis_era"


def test_broad_position_resolves_same_name_same_team_same_year() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="DB")
    offense = _candidate("00-offense")
    defense = _candidate("00-defense", position="DB", nfl_position="DB")

    decision = resolve_roster_collision(query, [offense, defense], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "00-defense"
    assert decision.rule == "broad_position"


def test_alias_resolves_before_normalized_name() -> None:
    query = IdentityQuery("Buster Brown", 2005, team="WAS", position="QB")
    alias = _candidate("00-alias", name="James Brown", aliases=("Buster Brown",))
    fuzzy = _candidate("00-fuzzy", name="Buster Browne")

    decision = resolve_roster_collision(query, [fuzzy, alias], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "00-alias"
    assert decision.rule == "alias"


def test_active_year_excludes_an_otherwise_identical_candidate() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB")
    active = _candidate("00-active")
    inactive = _candidate("00-inactive", start_year=2012, end_year=2018)

    decision = resolve_roster_collision(query, [inactive, active], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "00-active"
    assert decision.rule == "active_year"


def test_primary_detailed_position_breaks_broad_position_tie() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="RB", nfl_position="HB")
    quarterback = _candidate("00-hb", position="RB", nfl_position="HB")
    receiver = _candidate("00-fb", position="RB", nfl_position="FB")

    decision = resolve_roster_collision(query, [receiver, quarterback], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "00-hb"
    assert decision.rule == "primary_position"


def test_source_continuity_breaks_remaining_tie() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB")
    continuous = _candidate("00-continuous", source_ids=("pfa-alex", "mfl-123"))
    sparse = _candidate("00-sparse", source_ids=("pfa-other",))

    decision = resolve_roster_collision(query, [sparse, continuous], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "00-continuous"
    assert decision.rule == "source_continuity"


def test_historical_id_alias_is_a_verified_source_id_crosswalk() -> None:
    query = IdentityQuery(
        "J. Washington", 2005, team="WAS", position="QB", source_id="WashJo00"
    )
    historical = _candidate("00-joe", name="Joe Washington", source_ids=("WashJo00",))
    namesake = _candidate("00-other", name="Jay Washington")

    decision = resolve_roster_collision(query, [namesake, historical], pfa_appearances=_pfa())

    assert decision.canonical_player_id == "00-joe"
    assert decision.rule == "verified_source_id_crosswalk"
    assert decision.id_aliases == ("WashJo00",)


def test_joe_washington_resolves_from_general_pfa_position_evidence() -> None:
    query = IdentityQuery("Joe Washington", 1978, team="BAL", position="RB", nfl_position="RB")
    running_back = _candidate(
        "WashJo00", "Joe Washington", start_year=1976, end_year=1985,
        teams=("BAL",), position="RB", nfl_position=None,
    )
    defender = _candidate(
        "WashJo01", "Joe Washington", start_year=1977, end_year=1980,
        teams=("BAL",), position="DB", nfl_position="DB",
    )
    appearances = _pfa(
        {
            "season": 1978,
            "team": "BAL",
            "player": "Joe Washington",
            "source_player_id": "WashJo00",
            "position": "RB",
            "nfl_position": "RB",
            "game_id": "1978-09-03-bal",
            "lineage_leaves": [
                {"shard_id": 4, "manifest_sha256": "a" * 64},
                {"shard_id": 9, "manifest_sha256": "b" * 64},
            ],
        }
    )

    decision = resolve_roster_collision(query, [defender, running_back], pfa_appearances=appearances)

    assert decision.canonical_player_id == "WashJo00"
    assert decision.rule == "broad_position"
    pfa_leaves = [row for row in decision.evidence if row["evidence_type"] == "pfa_lineage_leaf"]
    assert {(row["shard_id"], row["manifest_sha256"]) for row in pfa_leaves} == {
        (4, "a" * 64),
        (9, "b" * 64),
    }


def test_exact_unresolved_tie_is_order_independent_collision_review() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB", nfl_position="QB")
    left = _candidate("00-a")
    right = _candidate("00-b")

    forward = resolve_roster_collision(query, [left, right], pfa_appearances=_pfa())
    reverse = resolve_roster_collision(query, [right, left], pfa_appearances=_pfa())

    assert forward == reverse
    assert forward.status == "collision_review"
    assert forward.canonical_player_id is None
    assert forward.rule == "exact_tie"


def test_only_ineligible_candidate_requires_collision_review() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB")
    inactive = _candidate("00-inactive", start_year=2012, end_year=2018)

    decision = resolve_roster_collision(query, [inactive], pfa_appearances=_pfa())

    assert decision.status == "collision_review"
    assert decision.canonical_player_id is None
    assert decision.rule == "no_eligible_candidate"


def test_conflicting_known_broad_facts_cannot_make_candidate_eligible() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="RB")
    canonical_qb = _candidate("00-alex", position="QB", nfl_position="QB")
    appearances = _pfa(
        {
            "season": 2005, "team": "WAS", "player": "Alex Smith",
            "source_player_id": "00-alex", "position": "RB", "nfl_position": "RB",
            "game_id": "game-1",
            "lineage_leaves": [{"shard_id": 1, "manifest_sha256": "1" * 64}],
        }
    )

    decision = resolve_roster_collision(query, [canonical_qb], pfa_appearances=appearances)

    assert decision.status == "collision_review"
    assert decision.canonical_player_id is None
    assert decision.rule == "no_eligible_candidate"


def test_shared_source_crosswalk_continues_to_normalized_name_precedence() -> None:
    query = IdentityQuery(
        "Alex Smith", 2005, team="WAS", position="QB", source_id="shared-id"
    )
    exact = _candidate("00-exact", source_ids=("shared-id",))
    other = _candidate("00-other", name="Alan Smith", source_ids=("shared-id",))

    decision = resolve_roster_collision(query, [other, exact], pfa_appearances=_pfa())

    assert decision.status == "resolved"
    assert decision.canonical_player_id == "00-exact"
    assert decision.rule == "normalized_name"


def test_shared_alias_continues_to_normalized_name_precedence() -> None:
    query = IdentityQuery("Buster Brown", 2005, team="WAS", position="QB")
    exact = _candidate("00-exact", name="Buster Brown", aliases=("Buster Brown",))
    other = _candidate("00-other", name="James Brown", aliases=("Buster Brown",))

    decision = resolve_roster_collision(query, [other, exact], pfa_appearances=_pfa())

    assert decision.status == "resolved"
    assert decision.canonical_player_id == "00-exact"
    assert decision.rule == "normalized_name"


def test_ledgers_emit_one_decision_and_every_leaf_evidence_row() -> None:
    query = IdentityQuery("Joe Washington", 1978, team="BAL", position="RB")
    candidate = _candidate(
        "WashJo00", "Joe Washington", start_year=1976, end_year=1985,
        teams=("BAL",), position="RB", nfl_position="RB",
    )
    appearances = _pfa(
        {
            "season": 1978, "team": "BAL", "player": "Joe Washington",
            "source_player_id": "WashJo00", "position": "RB", "nfl_position": "RB",
            "game_id": "game-1",
            "lineage_leaves": [
                {"shard_id": 1, "manifest_sha256": "1" * 64},
                {"shard_id": 2, "manifest_sha256": "2" * 64},
            ],
        }
    )
    decision = resolve_roster_collision(query, [candidate], pfa_appearances=appearances)

    decisions, evidence = build_collision_ledgers([("collision-1", decision)])

    assert len(decisions) == 1
    assert decisions.iloc[0]["collision_id"] == "collision-1"
    assert len(evidence.loc[evidence["evidence_type"] == "pfa_lineage_leaf"]) == 2


def test_proposed_bio_patch_unions_only_non_conflicting_empty_facts() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB", nfl_position="QB")
    candidate = _candidate("00-alex", source_ids=("AlexSm00",))
    decision = resolve_roster_collision(query, [candidate], pfa_appearances=_pfa())
    existing = {
        "NFL_player_id": "00-alex",
        "player": "Alex Smith",
        "position": "QB",
        "nfl_position": None,
        "first_year": 1999,
    }

    patch = propose_collision_bio_patch(existing, decision)

    assert patch.status == "candidate_patch"
    assert patch.changes == {"nfl_position": "QB", "id_aliases": ("AlexSm00",)}
    assert existing["first_year"] == 1999


def test_proposed_bio_patch_refuses_conflicting_populated_canonical_field() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB", nfl_position="QB")
    decision = resolve_roster_collision(query, [_candidate("00-alex")], pfa_appearances=_pfa())

    patch = propose_collision_bio_patch(
        {"NFL_player_id": "00-alex", "player": "Alex Smith", "nfl_position": "WR"},
        decision,
    )

    assert patch.status == "collision_review"
    assert patch.changes == {}
    assert patch.reason == "conflicting_nfl_position"


def test_duplicate_bio_facts_are_unioned_without_overwriting_canonical_values() -> None:
    query = IdentityQuery("Joe Washington", 2001, team="WAS", position="RB")
    gsis = _candidate(
        "00-joe", "Joe Washington", position=None, nfl_position=None,
        source_ids=("WashJo00",),
    )
    pfr = _candidate(
        "WashJo00", "Joe Washington", position="RB", nfl_position="HB",
        source_ids=("00-joe",),
    )
    decision = resolve_roster_collision(query, [pfr, gsis], pfa_appearances=_pfa())

    patch = propose_collision_bio_patch(
        {"NFL_player_id": "00-joe", "player": "Joe Washington", "position": None},
        decision,
    )

    assert patch.status == "candidate_patch"
    assert patch.changes == {
        "position": "RB",
        "nfl_position": "HB",
        "id_aliases": ("WashJo00",),
    }


def test_proposed_alias_patch_unions_existing_aliases_with_every_losing_id() -> None:
    query = IdentityQuery("Joe Washington", 2001, team="WAS", position="RB")
    gsis = _candidate(
        "00-joe", "Joe Washington", position="RB", nfl_position="RB",
        source_ids=("WashJo00",),
    )
    pfr = _candidate(
        "WashJo00", "Joe Washington", position="RB", nfl_position="RB",
        source_ids=("00-joe",),
    )
    decision = resolve_roster_collision(query, [pfr, gsis], pfa_appearances=_pfa())

    patch = propose_collision_bio_patch(
        {
            "NFL_player_id": "00-joe", "player": "Joe Washington",
            "position": "RB", "nfl_position": "RB",
            "id_aliases": '["legacy-joe"]',
        },
        decision,
    )

    assert patch.status == "candidate_patch"
    assert patch.changes["id_aliases"] == ("WashJo00", "legacy-joe")


def test_same_pfa_key_preserves_all_lineage_and_conflicting_semantics() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS")
    candidate = _candidate("00-alex", position=None, nfl_position=None)
    appearances = _pfa(
        {
            "season": 2005, "team": "WAS", "player": "Alex Smith",
            "source_player_id": "00-alex", "position": "QB", "nfl_position": "QB",
            "game_id": "same-game",
            "lineage_leaves": [{"shard_id": 1, "manifest_sha256": "1" * 64}],
        },
        {
            "season": 2005, "team": "WAS", "player": "Alex Smith",
            "source_player_id": "00-alex", "position": "RB", "nfl_position": "RB",
            "game_id": "same-game",
            "lineage_leaves": [{"shard_id": 2, "manifest_sha256": "2" * 64}],
        },
    )

    decision = resolve_roster_collision(query, [candidate], pfa_appearances=appearances)

    leaves = [row for row in decision.evidence if row["evidence_type"] == "pfa_lineage_leaf"]
    assert {(row["shard_id"], row["position"]) for row in leaves} == {(1, "QB"), (2, "RB")}


def test_pfa_lineage_leaves_must_be_typed_records() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB")
    appearances = _pfa(
        {
            "season": 2005, "team": "WAS", "player": "Alex Smith",
            "source_player_id": "00-alex", "position": "QB", "nfl_position": "QB",
            "game_id": "game-1", "lineage_leaves": ["not-a-record"],
        }
    )

    with pytest.raises(ValueError, match="typed lineage leaf"):
        resolve_roster_collision(query, [_candidate("00-alex")], pfa_appearances=appearances)


def test_malformed_array_like_lineage_leaf_still_fails_loudly() -> None:
    query = IdentityQuery("Alex Smith", 2005, team="WAS", position="QB")
    appearances = _pfa(
        {
            "season": 2005, "team": "WAS", "player": "Alex Smith",
            "source_player_id": "00-alex", "position": "QB", "nfl_position": "QB",
            "game_id": "game-1",
            "lineage_leaves": np.array([{"shard_id": 1}], dtype=object),
        }
    )

    with pytest.raises(ValueError, match="typed lineage leaf"):
        resolve_roster_collision(query, [_candidate("00-alex")], pfa_appearances=appearances)
