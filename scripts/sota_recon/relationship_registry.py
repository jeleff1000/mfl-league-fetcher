"""Machine-readable relationships used by the reconciliation lanes.

O.7 slice 4 (2026-07-26): the ad-hoc rows this registry carried are SUPERSEDED by
the committed relationship-edge contract (witness_gate/contracts/
relationship_edges.v1.json, §19.3 generate-and-test). Retirement is deliberate,
never silent: every retired row is listed in RETIRED with the superseding edge
id(s), and the parity test (test_relationship_registry) verifies each named edge
exists in the contract with a typed verdict — enforceable rows became gates
(recon_relationships consumes relationship_edges.row_gate_edges), refuted rows
became do-not-enforce registry entries with counterexamples. Either way the grid
now holds the relation with a receipt; this file no longer does.

RELATIONSHIPS keeps ONLY what the R1-R9 grid cannot express:
  source_definition   PBP-witness derivations (cross-source, not in-release)
  identity_limit      team-level PBP witnesses with no defender identity
  cross_team_bound    cross-team-week bound pending candidate-generator support
  scoring_law         fantasy-points engine decomposition (league-config atoms)
  context_gate        scoreboard-witness mirror (external witness lane)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Relationship:
    key: str
    kind: str
    lhs: str
    rhs: str
    formula: str
    year_min: int | None = None
    year_max: int | None = None
    tolerance: float = 0.0
    severity: str = "review"
    note: str = ""
    grain: str = "player_week"
    lane: str = "recon_relationships"


# Rows the edge contract cannot express -- the grid's complement, kept on purpose.
RELATIONSHIPS = (
    Relationship("passing_first_downs_pbp", "source_definition", "passing_first_downs",
                 "SUM(PBP.first_down_pass BY passer)", "PBP witness; attribution definition review",
                 year_min=1978, severity="review"),
    Relationship("rushing_first_downs_pbp", "source_definition", "rushing_first_downs",
                 "SUM(PBP.first_down_rush BY rusher)", "PBP witness; attribution definition review",
                 year_min=1978, severity="review"),
    Relationship("receiving_first_downs_pbp", "source_definition", "receiving_first_downs",
                 "SUM(PBP.first_down_pass BY receiver)", "PBP witness; attribution definition review",
                 year_min=1978, severity="review"),
    Relationship("def_air_yards_allowed_pbp", "identity_limit", "def_air_yards_allowed",
                 "SUM(PBP.air_yards BY defteam)",
                 "team-level PBP witness; no defender identity on target play",
                 year_min=1978, severity="review"),
    Relationship("def_yac_allowed_pbp", "identity_limit", "def_yards_after_catch_allowed",
                 "SUM(PBP.yards_after_catch BY defteam)",
                 "team-level PBP witness; no defender identity on target play",
                 year_min=1978, severity="review"),
    Relationship("punt_returns_le_punts", "cross_team_bound", "opponent punt_returns", "team punts",
                 "returns <= punts", year_min=1978, severity="hard", grain="team_game",
                 lane="recon_mirrors",
                 note="cross-TEAM bound (returning side vs punting side); the R5 "
                      "cross-side lane is eq-only today -- queued for the candidate "
                      "generator (§24.4 route), kept here until an edge exists"),
    Relationship("fantasy_points_from_atoms", "scoring_law", "fantasy points",
                 "scoring atoms", "fantasy points = configured atom decomposition",
                 year_min=1920, severity="hard", grain="player_week", lane="golden_points"),
    Relationship("def_points_allowed_scoreboard", "context_gate", "DEF points_allowed",
                 "opponent scoreboard points", "points_allowed = official opponent score",
                 year_min=1920, severity="hard", grain="team_game", lane="recon_context_gates"),
)


# Retired rows -> superseding edge id(s) in relationship_edges.v1.json. The parity
# test asserts every named edge exists there with a typed verdict; an edge that
# vanished from the contract fails the test -- retirement stays receipted forever.
RETIRED: dict[str, tuple[str, ...]] = {
    "fg_missed": ("R1:player_week:fg_missed=fg_att+fg_made",),
    "pat_missed": ("R1:player_week:pat_missed=pat_att+pat_made",),
    "receptions_le_targets": ("R2:player_week:receptions<=targets",),
    "passing_yards_per_attempt": ("R1:player_week:passing_yards_per_attempt=passing_yards+attempts",),
    "rushing_yards_per_carry": ("R1:player_week:rushing_yards_per_carry=rushing_yards+carries",),
    "receiving_yards_per_reception": ("R1:player_week:receiving_yards_per_reception=receiving_yards+receptions",),
    "receiving_yards_components":
        ("R1:player_week:receiving_yards=receiving_completed_air_yards+receiving_yards_after_catch",),
    "touches": ("R1:player_week:touches=carries+receptions",),
    "opportunities": ("R1:player_week:opportunities=carries+targets",),
    "scrimmage_tds": ("R1:player_week:scrimmage_tds=rushing_tds+receiving_tds",),
    "dropbacks": ("R1:player_week:dropbacks=attempts+sacks_suffered",),
    "passing_tds_eq_receiving_tds": ("R3:team_week:passing_tds=receiving_tds",),
    "passing_long_eq_receiving_long": ("R3:team_week:passing_long=receiving_long",),
    "passing_air_ge_receiving_air": ("R3:team_week:receiving_air_yards<=passing_air_yards",),
    "pass_int_eq_target_int": ("R3:team_week:passing_interceptions=receiving_target_interceptions",),
    "points_scored_eq_points_allowed": ("R8:league_year:total_points_scored=points_allowed",),
    # grain-aggregation law: superseded by the R6 class (584 per-stat edges);
    # representative receipts named, the class is contract-queryable (r_class R6)
    "weekly_to_season_sum": ("R6:player_season:receptions=receptions",),
    "season_to_career_sum": ("R6:player_career:receptions=receptions",),
    # player->team vertical law: superseded by the R4 team-witness class
    # (pfr_box_team_stats edges, kc side binding receipted 2026-07-26)
    "player_to_team_game_sum": ("R4:team_week:passing_yards=pfr_box_team_stats.passing_yards",),
}


def registry_keys() -> set[str]:
    return {r.key for r in RELATIONSHIPS}
