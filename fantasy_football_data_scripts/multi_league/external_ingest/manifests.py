"""Per-table slot manifests for external file ingestion.

Each TableManifest defines:
- The set of canonical staging columns (slots) we accept from external files
- Aliases for each slot (file column synonyms)
- Satisfaction rules (which slot combinations make a file ingestable)
- Derivation rules (slots computable from other slots if missing)
"""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Callable


@dataclass(frozen=True)
class Slot:
    name: str  # canonical staging column name
    aliases: tuple[str, ...]  # source-column synonyms (will be normalized at compare time)
    group: str  # "identity" | "outcome" | "optional"
    derivable_from: Callable | None = None  # synthesize from other slots; takes dict[slot_name -> value]
    dtype: str = "str"  # "int" | "float" | "str" | "bool"


@dataclass(frozen=True)
class TableManifest:
    table: str  # "matchup" | "draft" | "transactions" | "player_fantasy"
    slots: tuple[Slot, ...]
    satisfaction_rules: tuple[Callable[[set[str]], bool], ...]


def _win_from_points(row: dict) -> int | None:
    tp, op = row.get("team_points"), row.get("opponent_points")
    if tp is None or op is None:
        return None
    return 1 if tp > op else 0


# === MATCHUP ===

MATCHUP_MANIFEST = TableManifest(
    table="matchup",
    slots=(
        Slot("year", ("year", "season", "yr"), "identity", dtype="int"),
        Slot("week", ("week", "wk", "matchup_week"), "identity", dtype="int"),
        Slot(
            "manager",
            ("manager", "owner", "name", "team_owner", "owner_name", "user"),
            "identity",
        ),
        Slot(
            "opponent",
            ("opponent", "opp", "opponent_manager", "vs", "vs_manager"),
            "identity",
        ),
        Slot(
            "team_points",
            ("team_points", "points_for", "pf", "score", "team_score"),
            "outcome",
            dtype="float",
        ),
        Slot(
            "opponent_points",
            ("opponent_points", "points_against", "pa", "opp_score"),
            "outcome",
            dtype="float",
        ),
        Slot(
            "win",
            ("win", "winner_flag", "w"),
            "outcome",
            derivable_from=_win_from_points,
            dtype="int",
        ),
        Slot("manager_guid", ("manager_guid", "owner_guid", "owner_id"), "optional"),
        Slot("team_name", ("team_name", "team", "franchise"), "optional"),
        Slot("team_key", ("team_key",), "optional"),
        Slot(
            "team_projected_points",
            ("team_projected_points", "projected_pf"),
            "optional",
            dtype="float",
        ),
        Slot(
            "opponent_projected_points",
            ("opponent_projected_points", "projected_pa"),
            "optional",
            dtype="float",
        ),
        Slot("division_id", ("division_id", "division"), "optional"),
        Slot(
            "is_playoffs",
            ("is_playoffs", "playoffs", "postseason"),
            "optional",
            dtype="int",
        ),
        Slot(
            "is_consolation",
            ("is_consolation", "consolation"),
            "optional",
            dtype="int",
        ),
    ),
    satisfaction_rules=(
        # Identity: all four required
        lambda filled: {"year", "week", "manager", "opponent"}.issubset(filled),
        # Outcome: either both points OR a winner column
        lambda filled: ({"team_points", "opponent_points"}.issubset(filled)) or ("win" in filled),
    ),
)


# === DRAFT ===

DRAFT_MANIFEST = TableManifest(
    table="draft",
    slots=(
        Slot("year", ("year", "season", "yr"), "identity", dtype="int"),
        Slot(
            "manager",
            ("manager", "owner", "name", "team_owner", "owner_name"),
            "identity",
        ),
        Slot("player", ("player", "player_name"), "identity"),
        Slot("round", ("round", "rd"), "outcome", dtype="int"),
        Slot("pick", ("pick", "pick_number", "overall_pick"), "outcome", dtype="int"),
        Slot("cost", ("cost", "auction_cost", "price"), "outcome", dtype="float"),
        Slot("draft_slot", ("draft_slot", "slot"), "optional", dtype="int"),
        Slot("yahoo_position", ("yahoo_position", "position"), "optional"),
        Slot("nfl_team", ("nfl_team", "team"), "optional"),
        Slot("manager_guid", ("manager_guid", "owner_guid", "owner_id"), "optional"),
        Slot("team_key", ("team_key",), "optional"),
        Slot(
            "is_keeper_status",
            ("is_keeper_status", "is_keeper"),
            "optional",
            dtype="int",
        ),
        Slot("is_keeper_cost", ("is_keeper_cost",), "optional", dtype="float"),
    ),
    satisfaction_rules=(
        lambda filled: {"year", "manager", "player"}.issubset(filled),
        # Either snake (round + pick) or auction (cost)
        lambda filled: ({"round", "pick"}.issubset(filled)) or ("cost" in filled),
    ),
)


# === TRANSACTIONS ===

TRANSACTIONS_MANIFEST = TableManifest(
    table="transactions",
    slots=(
        Slot("year", ("year", "season", "yr"), "identity", dtype="int"),
        Slot("week", ("week", "wk"), "identity", dtype="int"),
        Slot(
            "manager",
            ("manager", "owner", "name", "team_owner", "owner_name"),
            "identity",
        ),
        Slot("player", ("player", "player_name"), "identity"),
        Slot(
            "transaction_type",
            ("transaction_type", "type", "txn_type"),
            "identity",
        ),
        Slot("transaction_id", ("transaction_id", "txn_id"), "optional"),
        Slot("source_type", ("source_type", "source"), "optional"),
        Slot("destination", ("destination", "dest"), "optional"),
        Slot("faab_bid", ("faab_bid", "faab"), "optional", dtype="float"),
        Slot("manager_guid", ("manager_guid", "owner_guid", "owner_id"), "optional"),
    ),
    satisfaction_rules=(
        lambda filled: {
            "year",
            "week",
            "manager",
            "player",
            "transaction_type",
        }.issubset(filled),
    ),
)


# === PLAYER FANTASY ===

PLAYER_FANTASY_MANIFEST = TableManifest(
    table="player_fantasy",
    slots=(
        Slot("year", ("year", "season", "yr"), "identity", dtype="int"),
        Slot("week", ("week", "wk"), "identity", dtype="int"),
        Slot(
            "manager",
            ("manager", "owner", "name", "team_owner", "owner_name"),
            "identity",
        ),
        Slot("player", ("player", "player_name"), "identity"),
        Slot(
            "points",
            ("points", "fantasy_points", "score"),
            "outcome",
            dtype="float",
        ),
        Slot(
            "fantasy_position",
            ("fantasy_position", "lineup_slot", "roster_position"),
            "identity",
        ),
        Slot("nfl_team", ("nfl_team",), "optional"),
        Slot("position", ("position", "nfl_position"), "optional"),
        Slot("is_started", ("is_started", "started"), "optional", dtype="int"),
        Slot("manager_guid", ("manager_guid", "owner_guid", "owner_id"), "optional"),
        Slot("team_key", ("team_key",), "optional"),
    ),
    satisfaction_rules=(
        lambda filled: {
            "year",
            "week",
            "manager",
            "player",
            "points",
            "fantasy_position",
        }.issubset(filled),
    ),
)


_MANIFESTS = {
    "matchup": MATCHUP_MANIFEST,
    "draft": DRAFT_MANIFEST,
    "transactions": TRANSACTIONS_MANIFEST,
    "player_fantasy": PLAYER_FANTASY_MANIFEST,
}


def get_manifest(table: str) -> TableManifest:
    if table not in _MANIFESTS:
        raise KeyError(f"No manifest defined for table {table!r}; known: {sorted(_MANIFESTS)}")
    return _MANIFESTS[table]
