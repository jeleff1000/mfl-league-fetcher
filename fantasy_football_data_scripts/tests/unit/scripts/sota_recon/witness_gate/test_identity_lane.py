from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.identity_lane import (
    IdentityCandidate,
    IdentityQuery,
    propose_player_bio_patch,
    resolve_identity,
)


def candidate(
    player_id: str,
    name: str,
    team: str,
    year: int,
    *,
    position: str = "RB",
    nfl_position: str | None = None,
    aliases: tuple[str, ...] = (),
    source_ids: tuple[str, ...] = (),
) -> IdentityCandidate:
    return IdentityCandidate(
        player_id=player_id,
        name=name,
        start_year=year,
        end_year=year,
        teams=(team,),
        position=position,
        nfl_position=nfl_position,
        aliases=aliases,
        source_ids=source_ids,
    )


def test_exact_and_fuzzy_team_name_year_match_resolve_one_identity() -> None:
    candidates = (
        candidate("p1", "John O'Connor", "BOS", 1946),
        candidate("p2", "John Connor", "NYG", 1946),
    )

    exact = resolve_identity(IdentityQuery("John O'Connor", 1946, "BOS"), candidates)
    fuzzy = resolve_identity(IdentityQuery("Jon OConnor", 1946, "BOS"), candidates)

    assert exact.status == "resolved"
    assert exact.player_id == "p1"
    assert fuzzy.status == "resolved"
    assert fuzzy.player_id == "p1"


def test_two_plausible_players_require_collision_review() -> None:
    candidates = (
        candidate("p1", "Bill Smith", "CHI", 1948),
        candidate("p2", "William Smith", "CHI", 1948),
    )

    result = resolve_identity(IdentityQuery("Wm Smith", 1948, "CHI"), candidates, fuzzy_threshold=60)

    assert result.status == "collision_review"
    assert {item.player_id for item in result.candidates} == {"p1", "p2"}


def test_team_year_and_position_filters_prevent_false_name_match() -> None:
    candidates = (
        candidate("p1", "Alex Brown", "DET", 1950, position="RB"),
        candidate("p2", "Alex Brown", "DET", 1951, position="DB"),
    )

    result = resolve_identity(IdentityQuery("Alex Brown", 1951, "DET", position="DB"), candidates)

    assert result.status == "resolved"
    assert result.player_id == "p2"


def test_broad_position_compatibility_does_not_reject_historical_role() -> None:
    query = IdentityQuery(name="John Doe", year=1948, team="CHI", position="RB", nfl_position="B")
    historical = IdentityCandidate(
        player_id="pfr-1",
        name="John Doe",
        start_year=1947,
        end_year=1950,
        teams=("CHI",),
        position="RB",
        nfl_position="WB",
        aliases=(),
        source_ids=(),
    )

    assert resolve_identity(query, [historical]).status == "resolved"


def test_detailed_position_is_only_a_tie_breaker() -> None:
    candidates = (
        candidate("hb", "Pat Lee", "CHI", 1948, nfl_position="HB"),
        candidate("fb", "Pat Lee", "CHI", 1948, nfl_position="FB"),
    )

    decision = resolve_identity(
        IdentityQuery("Pat Lee", 1948, "CHI", position="RB", nfl_position="HB"),
        candidates,
    )

    assert decision.status == "resolved"
    assert decision.player_id == "hb"
    assert decision.evidence["winning_rule"] == "detailed_position"


def test_detailed_position_does_not_beat_an_exact_name() -> None:
    candidates = (
        candidate("exact", "Patrick Lee", "CHI", 1948, nfl_position="FB"),
        candidate("role", "Pat Lee", "CHI", 1948, nfl_position="HB"),
    )

    decision = resolve_identity(
        IdentityQuery("Patrick Lee", 1948, "CHI", position="RB", nfl_position="HB"),
        candidates,
        fuzzy_threshold=60,
    )

    assert decision.player_id == "exact"
    assert decision.evidence["winning_rule"] == "exact_name"


def test_source_id_alias_precedes_name_similarity() -> None:
    candidates = (
        candidate("crosswalk", "Johnathan Doe", "CHI", 1948, source_ids=("nfl:123",)),
        candidate("name", "John Doe", "CHI", 1948),
    )

    decision = resolve_identity(
        IdentityQuery("John Doe", 1948, "CHI", source_id="NFL:123"),
        candidates,
    )

    assert decision.player_id == "crosswalk"
    assert decision.evidence["winning_rule"] == "source_id"
    assert {row["player_id"] for row in decision.evidence["scored_candidates"]} == {
        "crosswalk",
        "name",
    }


def test_exact_alias_precedes_fuzzy_name() -> None:
    candidates = (
        candidate("alias", "William Smith", "CHI", 1948, aliases=("Wm Smith",)),
        candidate("fuzzy", "W Smith", "CHI", 1948),
    )

    decision = resolve_identity(IdentityQuery("Wm Smith", 1948, "CHI"), candidates, fuzzy_threshold=60)

    assert decision.player_id == "alias"
    assert decision.evidence["winning_rule"] == "alias"


@pytest.mark.parametrize("match_kind", ["source_id", "alias"])
@pytest.mark.parametrize("ineligibility", ["wrong_year", "wrong_team", "wrong_position"])
def test_exact_source_or_alias_cannot_bypass_known_eligibility(
    match_kind: str,
    ineligibility: str,
) -> None:
    bad_values = {
        "start_year": 1948,
        "end_year": 1948,
        "teams": ("CHI",),
        "position": "RB",
    }
    if ineligibility == "wrong_year":
        bad_values.update(start_year=1947, end_year=1947)
    elif ineligibility == "wrong_team":
        bad_values["teams"] = ("NYG",)
    else:
        bad_values["position"] = "DB"
    bad = IdentityCandidate(
        player_id="ineligible-crosswalk",
        name="Johnathan Doe",
        aliases=("John Doe",) if match_kind == "alias" else (),
        source_ids=("nfl:123",) if match_kind == "source_id" else (),
        **bad_values,
    )
    eligible_name = candidate("eligible-name", "John Doe", "CHI", 1948, position="RB")
    query = IdentityQuery(
        "John Doe",
        1948,
        "CHI",
        position="RB",
        source_id="nfl:123" if match_kind == "source_id" else None,
    )

    decision = resolve_identity(query, (bad, eligible_name))

    assert decision.player_id == "eligible-name"
    assert decision.evidence["winning_rule"] == "exact_name"


def test_missing_position_does_not_eliminate_candidate() -> None:
    no_position = candidate("p1", "Alex Brown", "DET", 1951, position=None)  # type: ignore[arg-type]

    result = resolve_identity(
        IdentityQuery("Alex Brown", 1951, "DET", position="DB", nfl_position="CB"),
        (no_position,),
    )

    assert result.player_id == "p1"


def test_source_continuity_is_the_last_tie_breaker() -> None:
    candidates = (
        candidate("single", "Alex Brown", "DET", 1951, source_ids=("pfr:1",)),
        candidate("continuous", "Alex Brown", "DET", 1951, source_ids=("pfr:2", "nfl:2")),
    )

    result = resolve_identity(IdentityQuery("Alex Brown", 1951, "DET"), candidates)

    assert result.player_id == "continuous"
    assert result.evidence["winning_rule"] == "source_continuity"


def test_no_candidate_stays_unresolved() -> None:
    result = resolve_identity(
        IdentityQuery("Nobody Here", 1940, "GB"),
        (candidate("p1", "Somebody Else", "GB", 1940),),
    )

    assert result.status == "unresolved"


def test_player_bio_patch_writes_position_and_nfl_position_separately() -> None:
    resolved = resolve_identity(
        IdentityQuery("Jane Doe", 1952, "CLE", position="OL", nfl_position="LS"),
        (candidate("p1", "Jane Doe", "CLE", 1952, position="OL", nfl_position="LS"),),
    )

    patch = propose_player_bio_patch(
        {"NFL_player_id": "p1", "player": "Jane Doe", "position": None, "nfl_position": None},
        resolved,
    )
    broad_conflict = propose_player_bio_patch(
        {"NFL_player_id": "p1", "player": "Jane Doe", "position": "DB", "nfl_position": None},
        resolved,
    )
    detailed_conflict = propose_player_bio_patch(
        {"NFL_player_id": "p1", "player": "Jane Doe", "position": "OL", "nfl_position": "C"},
        resolved,
    )

    assert patch.changes == {"position": "OL", "nfl_position": "LS"}
    assert broad_conflict.status == "collision_review"
    assert broad_conflict.changes == {}
    assert detailed_conflict.status == "collision_review"
    assert detailed_conflict.changes == {}
    assert "LS" not in {patch.changes["position"]}


def test_query_rejects_detailed_token_in_broad_position() -> None:
    with pytest.raises(ValueError, match="position must be one broad position"):
        IdentityQuery("Long Snapper", 1952, "CLE", position="LS")


def test_candidate_rejects_detailed_token_in_broad_position() -> None:
    with pytest.raises(ValueError, match="position must be one broad position"):
        candidate("p1", "Long Snapper", "CLE", 1952, position="LS")


def test_player_bio_patch_rejects_illegal_existing_broad_position() -> None:
    resolved = resolve_identity(
        IdentityQuery("Long Snapper", 1952, "CLE", position="OL", nfl_position="LS"),
        (candidate("p1", "Long Snapper", "CLE", 1952, position="OL", nfl_position="LS"),),
    )

    with pytest.raises(ValueError, match="position must be one broad position"):
        propose_player_bio_patch(
            {"NFL_player_id": "p1", "player": "Long Snapper", "position": "LS"},
            resolved,
        )


def test_player_bio_patch_fills_missing_values_but_never_overwrites_conflict() -> None:
    resolved = resolve_identity(
        IdentityQuery("Jane Doe", 1952, "CLE", position="QB"),
        (candidate("p1", "Jane Doe", "CLE", 1952, position="QB"),),
    )

    patch = propose_player_bio_patch(
        {"NFL_player_id": "p1", "player": None, "position": "QB"},
        resolved,
    )
    conflict = propose_player_bio_patch(
        {"NFL_player_id": "different", "player": "Jane Doe", "position": "QB"},
        resolved,
    )

    assert patch.changes == {"player": "Jane Doe"}
    assert conflict.status == "collision_review"
    assert conflict.changes == {}
