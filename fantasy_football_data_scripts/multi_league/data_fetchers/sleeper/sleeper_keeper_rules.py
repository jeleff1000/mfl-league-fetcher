"""
Sleeper Keeper Rules Inference

Analyzes draft data across consecutive seasons to infer keeper rules.
Sleeper doesn't expose keeper rules directly, so we detect them from patterns.

Detectable rules:
- Round cost: ROUND_DRAFTED - N (e.g., -1, -2, same round)
- FA pickup cost: Fixed round or last round
- Max years kept (if we have enough history)

Usage:
    from sleeper_keeper_rules import infer_keeper_rules

    rules = infer_keeper_rules(client, league_id)
    # Returns: {'round_cost': -1, 'fa_cost_round': 10, 'confidence': 0.95}
"""

import logging
from typing import Any
from collections import Counter

from .sleeper_api_client import SleeperAPIClient

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function."""
    logger.info(msg)
    print(msg)


def infer_keeper_rules(client: SleeperAPIClient, league_id: str, min_keepers_for_confidence: int = 5) -> dict[str, Any]:
    """
    Infer keeper rules by comparing draft data across consecutive seasons.

    Args:
        client: SleeperAPIClient instance
        league_id: Current season league ID
        min_keepers_for_confidence: Minimum keepers needed for high confidence

    Returns:
        Dict with:
        - round_cost: Round adjustment for drafted players (e.g., -1 = keep at round-1)
        - fa_cost_round: Fixed round for FA pickups (or None if unclear)
        - confidence: 0.0-1.0 confidence in the detected rule
        - sample_size: Number of keepers analyzed
        - details: Breakdown of detected patterns
    """
    result = {
        "round_cost": None,
        "fa_cost_round": None,
        "confidence": 0.0,
        "sample_size": 0,
        "details": {},
        "inferred": True,  # Flag that this was auto-detected
    }

    # Get current league info
    league = client.get_league(league_id)
    if not league:
        log(f"Could not fetch league {league_id}")
        return result

    league_type = league.get("settings", {}).get("type", 0)
    if league_type == 0:  # Redraft
        log("League is redraft, no keeper rules to infer")
        result["details"]["league_type"] = "redraft"
        return result

    prev_league_id = league.get("previous_league_id")
    if not prev_league_id:
        log("No previous league found, cannot infer keeper rules")
        result["details"]["error"] = "no_previous_league"
        return result

    # Get current and previous draft data
    current_draft_id = league.get("draft_id")
    prev_league = client.get_league(prev_league_id)
    if not prev_league:
        log(f"Could not fetch previous league {prev_league_id}")
        return result

    prev_draft_id = prev_league.get("draft_id")

    if not current_draft_id or not prev_draft_id:
        log("Missing draft IDs")
        return result

    current_picks = client.get_draft_picks(current_draft_id)
    prev_picks = client.get_draft_picks(prev_draft_id)

    if not current_picks or not prev_picks:
        log("Could not fetch draft picks")
        return result

    # Get draft settings for total rounds
    current_draft = client.get_draft(current_draft_id)
    total_rounds = current_draft.get("settings", {}).get("rounds", 15) if current_draft else 15

    # Build lookup of where players were drafted last year
    prev_draft_round = {}
    for pick in prev_picks:
        player_id = pick.get("player_id")
        if player_id:
            prev_draft_round[player_id] = {
                "round": pick.get("round"),
                "pick": pick.get("pick_no"),
                "roster_id": pick.get("roster_id"),
            }

    # Analyze keeper picks
    round_diffs = []  # For drafted players
    fa_keeper_rounds = []  # For FA pickups
    keeper_details = []

    for pick in current_picks:
        if not pick.get("is_keeper"):
            continue

        player_id = pick.get("player_id")
        current_round = pick.get("round")
        name = pick.get("metadata", {}).get("first_name", "") + " " + pick.get("metadata", {}).get("last_name", "")

        if player_id in prev_draft_round:
            prev = prev_draft_round[player_id]
            diff = current_round - prev["round"]
            round_diffs.append(diff)
            keeper_details.append(
                {
                    "player": name,
                    "prev_round": prev["round"],
                    "current_round": current_round,
                    "diff": diff,
                    "type": "drafted",
                }
            )
        else:
            fa_keeper_rounds.append(current_round)
            keeper_details.append(
                {"player": name, "prev_round": None, "current_round": current_round, "diff": None, "type": "fa_pickup"}
            )

    result["sample_size"] = len(round_diffs) + len(fa_keeper_rounds)
    result["details"]["keeper_breakdown"] = keeper_details
    result["details"]["total_rounds"] = total_rounds

    # Infer round cost for drafted players
    if round_diffs:
        diff_counts = Counter(round_diffs)
        most_common = diff_counts.most_common(1)[0]
        most_common_diff, count = most_common

        result["round_cost"] = most_common_diff
        result["confidence"] = count / len(round_diffs)
        result["details"]["round_diff_distribution"] = dict(diff_counts)

        log(f"Detected keeper rule: Round cost = {most_common_diff:+d}")
        log(f"  Confidence: {result['confidence']:.0%} ({count}/{len(round_diffs)} keepers)")

    # Infer FA pickup cost
    if fa_keeper_rounds:
        fa_counts = Counter(fa_keeper_rounds)
        most_common_fa = fa_counts.most_common(1)[0]

        result["fa_cost_round"] = most_common_fa[0]
        result["details"]["fa_round_distribution"] = dict(fa_counts)

        # Check if FA cost is "last round"
        if most_common_fa[0] >= total_rounds - 2:
            result["details"]["fa_rule_guess"] = "last_round"
        else:
            result["details"]["fa_rule_guess"] = f"fixed_round_{most_common_fa[0]}"

        log(f"FA pickup keepers at rounds: {dict(fa_counts)}")

    # Lower confidence if not enough keepers
    if result["sample_size"] < min_keepers_for_confidence:
        result["confidence"] *= 0.5
        result["details"]["low_sample_warning"] = True

    return result


def get_keeper_rule_description(rules: dict[str, Any]) -> str:
    """
    Generate human-readable description of keeper rules.

    Args:
        rules: Output from infer_keeper_rules()

    Returns:
        Human-readable string describing the rules
    """
    if not rules.get("round_cost") and not rules.get("fa_cost_round"):
        return "Keeper rules could not be detected"

    parts = []

    if rules.get("round_cost") is not None:
        cost = rules["round_cost"]
        if cost == 0:
            parts.append("Keep at same round drafted")
        elif cost == -1:
            parts.append("Keep at 1 round earlier than drafted")
        elif cost == -2:
            parts.append("Keep at 2 rounds earlier than drafted")
        elif cost < 0:
            parts.append(f"Keep at {abs(cost)} rounds earlier than drafted")
        else:
            parts.append(f"Keep at {cost} rounds later than drafted")

    if rules.get("fa_cost_round"):
        fa_round = rules["fa_cost_round"]
        fa_guess = rules.get("details", {}).get("fa_rule_guess", "")
        if "last_round" in fa_guess:
            parts.append(f"FA pickups: last round (R{fa_round})")
        else:
            parts.append(f"FA pickups: Round {fa_round}")

    confidence = rules.get("confidence", 0)
    conf_str = f" ({confidence:.0%} confidence)" if confidence < 1.0 else ""

    return "; ".join(parts) + conf_str
