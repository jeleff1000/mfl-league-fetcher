from scripts.sota_recon.witness_gate.newspaper_identity_replay import (
    ResolvedNewspaperIdentity,
    classify_candidate_unlock,
)


def _identity(raw_player: str = "Cliff Battle") -> ResolvedNewspaperIdentity:
    return ResolvedNewspaperIdentity(
        identity_task_id="task-1",
        boxscore_id="193311050bos",
        raw_player=raw_player,
        team="BOS",
        year=1933,
        week=8,
        nfl_player_id="BattCl20",
        canonical_player="Cliff Battles",
        proof_leaves=("leaf:subject:BattCl20:1933:BOS",),
    )


def test_weekly_identity_match_unlocks_missing_player_week() -> None:
    decision = classify_candidate_unlock(
        "weekly_player_stat_cell",
        {
            "boxscore_id": "193311050bos",
            "player_raw": "Cliff Battle",
            "strict_hold_reason": "missing_player_week",
        },
        {("193311050bos", "Cliff Battle"): _identity()},
    )

    assert decision is not None
    assert decision.nfl_player_id == "BattCl20"
    assert decision.player_week == "BattCl20_1933_8"


def test_role_specific_identity_only_unlocks_matching_hold() -> None:
    identities = {("193311050bos", "Cliff Battle"): _identity()}

    unlocked = classify_candidate_unlock(
        "scoring_event",
        {
            "boxscore_id": "193311050bos",
            "scoring_player_raw": "Cliff Battle",
            "passer_player_raw": "Other Player",
            "strict_hold_reason": "missing_scoring_player_identity",
        },
        identities,
    )
    still_held = classify_candidate_unlock(
        "scoring_event",
        {
            "boxscore_id": "193311050bos",
            "scoring_player_raw": "Cliff Battle",
            "passer_player_raw": "Other Player",
            "strict_hold_reason": "missing_passer_identity",
        },
        identities,
    )

    assert unlocked is not None
    assert still_held is None


def test_non_identity_hold_is_never_unlocked() -> None:
    decision = classify_candidate_unlock(
        "play_by_play_event",
        {
            "boxscore_id": "193311050bos",
            "primary_player_raw": "Cliff Battle",
            "strict_hold_reason": "missing_play_type",
        },
        {("193311050bos", "Cliff Battle"): _identity()},
    )

    assert decision is None
