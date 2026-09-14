"""
sota_recon/relationship_candidates.py  --  O.6: the R1-R9 CANDIDATE GENERATOR (§19.3)

Mechanically proposes the relationship-edge space from stat_contracts.v1 structure.
NO data is touched here: the generator is a pure function of the committed contract,
so the candidate space itself is regen-diff-testable. Verdicts (CONFIRMED /
CONDITIONAL / REFUTED / UNDECIDABLE) come only from relationship_verdicts.py test
runs -- proposal is never proof (§19 proof-or-pending).

Proposal rules, by R class (master plan §3):
  R1 IDENTITY    name morphology (X_missed = X_att - X_made; *_per_* / *_pct rate
                 names resolved via the declared numerator/denominator map) + the
                 declared composite catalog (touches, scrimmage_*, dropbacks, ...)
  R2 BOUND       X_made <= X_att; *_lost <= *; rz_* <= base; explosive <= event
                 count; bucket <= parent; blocked <= att - made
  R3 MIRROR      passing_<suffix> <-> receiving_<suffix> at team-week grain via
                 prefix-strip matching + the irregular alias map (completions <->
                 receptions, interceptions <-> target_interceptions)
  R4 VERTICAL    player-credit stats vs registered team-role witnesses; newspaper
                 team witnesses are proposed but ESCALATED (newspaper root is never
                 arbitrated by this lane)
  R5 HORIZONTAL  '_allowed' morphology: DEF-row team value = opponent offense sum
  R6 GRAIN       weekly -> season/season_all -> career/career_all for every scoped
                 stat whose contract declares both grains and a SUM/MAX class
  R7 PARTITION   ^parent_<lo>_<hi>$ / ^parent_<lo>plus$ bucket families summing to
                 an existing parent (range-deduped; duplicate-range variants become
                 pairwise alias candidates instead)
  R8 CONSERVATION league-year closure duals (thrown = caught, lost = recovered)
  R9 SCORING     points <-> weighted unique-scoring-event decomposition variants

Empirically mined relations (relationship_verdicts.py's sieve) join this space with
proposal_basis="empirical_mined"; they are NOT generated here because they depend on
data, but they share the same Candidate shape and id scheme.

Run:  python -m scripts.sota_recon.relationship_candidates
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import asdict, dataclass, field

CONTRACTS_PATH = os.path.join(os.path.dirname(__file__), "witness_gate", "contracts",
                              "stat_contracts.v1.json")

# The stat-column scope: weekly, numeric, real stat families (ranks/ppg/lamar/
# fantasy-surface families are DERIVED_OUTPUT -- regenerable, not relationship atoms).
STAT_FAMILIES = {"passing", "rushing", "receiving", "kicking", "punting", "returns",
                 "defense", "fumbles", "general", "special_teams", "situational", "snaps"}


def _is_numeric(dtype: str) -> bool:
    # prefix predicate, NOT an allowlist: an int32 allowlist gap silently dropped
    # 412 columns (incl. receptions_40plus) from the O.6 candidate space.
    return dtype.startswith(("double", "float", "int", "bigint", "decimal",
                             "smallint", "tinyint", "hugeint", "uint"))


@dataclass(frozen=True)
class Candidate:
    cand_id: str
    r_class: str            # R1..R9
    kind: str
    op: str                 # eq | le
    lhs: str
    rhs: tuple[str, ...]
    grain: str              # player_week | team_week | league_year | player_season |
                            # player_season_all | player_career | player_career_all
    test_kind: str          # row_formula | row_rate | row_bound | partition_sum |
                            # team_mirror | team_bound | team_horizontal |
                            # league_conservation | grain_agg | team_witness_join
    proposal_basis: str
    weights: tuple[float, ...] = ()   # per-rhs coefficients; empty = all 1.0
    agg: str = ""                     # grain_agg only: SUM | MAX
    note: str = ""
    escalation: str = ""              # e.g. newspaper_root_crossing (never auto-run)
    population: str = "all"           # row population the rhs is scoped to:
                                      # all | team_def_row | idp_rows. IDP and team
                                      # defense are DUAL bookkeeping, never summed
                                      # together -- the separation IS a recon lane.


def _cid(r_class: str, op: str, lhs: str, rhs: tuple[str, ...], grain: str) -> str:
    sym = {"eq": "=", "le": "<="}[op]
    return f"{r_class}:{grain}:{lhs}{sym}{'+'.join(rhs)}"


def load_contract_stats() -> list[dict]:
    with open(CONTRACTS_PATH, encoding="utf-8") as f:
        return json.load(f)["stats"]


def scope_columns(stats: list[dict] | None = None) -> dict[str, dict]:
    stats = stats if stats is not None else load_contract_stats()
    return {s["stat_id"]: s for s in stats
            if "weekly" in s["grains"] and s["family"] in STAT_FAMILIES
            and _is_numeric(s["dtype"])}


# ---------------------------------------------------------------------------
# morphology rules
# ---------------------------------------------------------------------------

_BUCKET_RE = re.compile(r"^(?P<parent>.+?)_(?P<lo>\d+)(?:_(?P<hi>\d+))?(?:plus)?_?$")

# rate/pct columns -> (numerator, denominator). Declared, not guessed: a *_per_* name
# alone does not identify its denominator column.
RATE_MAP = {
    "passing_yards_per_attempt": ("passing_yards", "attempts"),
    "rushing_yards_per_carry": ("rushing_yards", "carries"),
    "receiving_yards_per_reception": ("receiving_yards", "receptions"),
    "receiving_yards_per_target": ("receiving_yards", "targets"),
    "punt_yards_per_punt": ("punt_yards", "punts"),
    "yards_per_touch": ("scrimmage_yards", "touches"),
    "catch_pct": ("receptions", "targets"),
    "completion_pct": ("completions", "attempts"),
    "fg_pct": ("fg_made", "fg_att"),
    "pat_pct": ("pat_made", "pat_att"),
    "passing_int_pct": ("passing_interceptions", "attempts"),
    "passing_td_pct": ("passing_tds", "attempts"),
}

# att/made column bases whose names do not follow {base}_att/{base}_made directly
_ATT_MADE_BASES = ("fg", "pat", "gwfg")

# declared row composites (semantically known; empirical mining catches the rest)
COMPOSITES = [
    ("touches", ("carries", "receptions")),
    ("total_touches", ("carries", "receptions")),
    ("opportunities", ("carries", "targets")),
    ("scrimmage_tds", ("rushing_tds", "receiving_tds")),
    ("rush_receive_td", ("rushing_tds", "receiving_tds")),
    ("scrimmage_yards", ("rushing_yards", "receiving_yards")),
    ("yds_from_scrimmage", ("rushing_yards", "receiving_yards")),
    ("dropbacks", ("attempts", "sacks_suffered")),
    ("turnovers", ("passing_interceptions", "fumbles_lost")),
    ("total_return_yards", ("kickoff_return_yards", "punt_return_yards")),
    ("all_purpose_yards", ("scrimmage_yards", "total_return_yards")),
    ("receiving_yards", ("receiving_completed_air_yards", "receiving_yards_after_catch")),
    ("passing_yards", ("passing_completed_air_yards", "passing_yards_after_catch")),
    ("rushing_yards", ("rushing_yards_before_contact", "rushing_yards_after_contact")),
    ("def_tackles_combined", ("def_tackles_solo", "def_tackle_assists")),
    ("fumbles", ("rushing_fumbles", "receiving_fumbles", "sack_fumbles")),
    ("fumbles_lost", ("rushing_fumbles_lost", "receiving_fumbles_lost", "sack_fumbles_lost")),
    ("fum_rec", ("fumble_recovery_own", "fumble_recovery_opp")),
    ("total_epa", ("passing_epa", "rushing_epa", "receiving_epa")),
    ("total_wpa", ("passing_wpa", "rushing_wpa", "receiving_wpa")),
    ("total_tds_accounted_for", ("rushing_tds", "receiving_tds", "def_tds", "special_teams_tds")),
    ("total_tds_scored", ("rushing_tds", "receiving_tds", "def_tds", "special_teams_tds")),
]

# lhs <= rhs bound pairs beyond pure morphology
BOUND_PAIRS = [
    ("completions", "attempts"),
    ("receptions", "targets"),
    ("targets", "attempts"),           # team grain handles the mirror-side version
    ("rz_carries", "carries"),
    ("rz_pass_att", "attempts"),
    ("rz_targets", "targets"),
    ("rz_rush_td", "rushing_tds"),
    ("rz_pass_td", "passing_tds"),
    ("rz_rec_td", "receiving_tds"),
    ("pass_explosive_20", "completions"),
    ("rush_explosive_10", "carries"),
    ("rec_explosive_20", "receptions"),
    ("pick6", "passing_interceptions"),
    ("def_int_ret_td", "def_interceptions"),
    ("kickoff_return_tds", "kickoff_returns"),
    ("punt_return_tds", "punt_returns"),
    ("fum_ret_td", "fum_rec"),
    ("sack_fumbles", "sacks_suffered"),
    ("def_tackles_solo", "def_tackles_combined"),
    ("dst_points_allowed", "points_allowed"),
    ("fg_blocked", "fg_att"),
    ("completions_50plus", "completions"),
    ("passing_first_downs", "completions"),
    ("receiving_first_downs", "receptions"),
]

# R3 team-week mirrors: passing-side <-> receiving-side. op=eq unless noted.
MIRROR_PAIRS = [
    ("passing_yards", "receiving_yards", "eq", "SUM"),
    ("passing_tds", "receiving_tds", "eq", "SUM"),
    ("completions", "receptions", "eq", "SUM"),
    ("passing_interceptions", "receiving_target_interceptions", "eq", "SUM"),
    ("passing_completed_air_yards", "receiving_completed_air_yards", "eq", "SUM"),
    ("passing_yards_after_catch", "receiving_yards_after_catch", "eq", "SUM"),
    ("passing_first_downs", "receiving_first_downs", "eq", "SUM"),
    ("passing_2pt_conversions", "receiving_2pt_conversions", "eq", "SUM"),
    ("passing_drops", "receiving_drops", "eq", "SUM"),
    ("passing_long", "receiving_long", "eq", "MAX"),
    ("receiving_air_yards", "passing_air_yards", "le", "SUM"),
    ("targets", "attempts", "le", "SUM"),
]

# R5 horizontal: (allowed-side DEF-row stat, opponent offense stat, opponent agg)
ALLOWED_MAP = [
    ("passing_yds_allowed", "passing_yards", "SUM"),
    ("passing_tds_allowed", "passing_tds", "SUM"),
    ("rushing_yds_allowed", "rushing_yards", "SUM"),
    ("rushing_tds_allowed", "rushing_tds", "SUM"),
    ("receiving_yds_allowed", "receiving_yards", "SUM"),
    ("receiving_tds_allowed", "receiving_tds", "SUM"),
    ("def_completions_allowed", "completions", "SUM"),
    ("def_targets_allowed", "targets", "SUM"),
    ("def_completion_yards_allowed", "receiving_yards", "SUM"),
    ("def_completion_tds_allowed", "passing_tds", "SUM"),
    ("def_air_yards_allowed", "passing_air_yards", "SUM"),
    ("def_yards_after_catch_allowed", "passing_yards_after_catch", "SUM"),
    ("points_allowed", "total_points_scored", "SUM"),
    ("def_epa_allowed", "total_epa", "SUM"),
    ("def_pass_epa_allowed", "passing_epa", "SUM"),
    ("def_rush_epa_allowed", "rushing_epa", "SUM"),
]
# R5 cross-side: OFFENSE-side stat (summed over a team's player rows) vs the
# OPPONENT's DEFENSE-side booking of the same events. Def-side stats are DUAL
# bookkeeping (team-DEF row AND IDP rows), so every pair is proposed once per
# population -- the two planes are never combined (O.6 structural law). These are
# the only possible relationship edges for the trench/pressure charting cells in
# the §16.1 burn-down queue (added O.7, 2026-07-26).
CROSS_SIDE_PAIRS = [
    # (defense-side stat, offense-side component stats summed on the opponent)
    ("def_sacks", ("sacks_suffered",)),
    ("def_sack_yards", ("sack_yards_lost",)),
    ("def_blitzes", ("passing_blitzed",)),
    ("def_hurries", ("passing_hurried",)),
    ("def_pressures", ("passing_pressured",)),
    ("def_tackles_missed", ("rushing_broken_tackles", "receiving_broken_tackles")),
]

# total_yds_allowed carries a known gross-vs-net definition split (§0.2): propose both.
TOTAL_YDS_VARIANTS = [
    ("gross_rush_plus_receiving", ("rushing_yards", "receiving_yards"), (1.0, 1.0)),
    ("net_rush_plus_pass_minus_sacks", ("rushing_yards", "passing_yards", "sack_yards_lost"),
     (1.0, 1.0, -1.0)),
]

# R8 league-year conservation duals. Def-side stats are DUAL-booked (team-DEF row
# AND IDP rows carry the same event) -- a def-side rhs MUST be population-scoped or
# the league sum double-counts (the 2026-07-26 "2:1 INT" finding was exactly this).
CONSERVATION_PAIRS = [
    ("passing_interceptions", "def_interceptions", "team_def_row",
     "thrown INTs = team-defense caught INTs"),
    ("passing_interceptions", "def_interceptions", "idp_rows",
     "thrown INTs = IDP-credited caught INTs"),
    ("passing_tds", "passing_tds_allowed", "all", "scored = allowed (league closure)"),
    ("rushing_tds", "rushing_tds_allowed", "all", "scored = allowed (league closure)"),
    ("passing_yards", "passing_yds_allowed", "all", "gained = allowed (league closure)"),
    ("rushing_yards", "rushing_yds_allowed", "all", "gained = allowed (league closure)"),
    ("fumbles_lost", "fum_rec", "team_def_row",
     "lost = team-defense recovered (own-recovery definition risk)"),
    ("fumbles_lost", "fum_rec", "idp_rows",
     "lost = IDP-credited recovered (own-recovery definition risk)"),
    ("fumbles_lost", "fumble_recovery_opp", "idp_rows",
     "lost = opponent-recovered (the takeaway split; own-recoveries excluded)"),
    ("total_points_scored", "points_allowed", "all", "scored = allowed (league closure)"),
]

# R4 IDP -> team-defense vertical: v26 keeps DUAL defensive bookkeeping (a team-DEF
# row per team-week AND individual IDP rows). The relation team_DEF_row = SUM(IDP
# rows) per team-week is the reconciliation between the two planes -- runnable
# entirely inside the release, proposed for EVERY defense/fumbles-family stat.
# Verdicts type the planes honestly: dual-booked stats get CONFIRMED/REFUTED;
# stats one plane lacks (IDP-only trench metrics, team-only *_allowed concepts with
# no defender identity) come out UNDECIDABLE -- itself the record of the gap.
IDP_TEAM_VERTICAL_FAMILIES = {"defense", "fumbles"}

# R9 scoring decomposition variants (weights per component). 2pt passing credits the
# THROWER no points in standard scoring -- variants differ exactly there.
SCORING_VARIANTS = [
    ("six_per_td_kicker_pts", ("total_tds_accounted_for", "fg_made", "pat_made"), (6.0, 3.0, 1.0)),
    ("six_per_td_kicker_2pt_safety",
     ("total_tds_accounted_for", "fg_made", "pat_made", "rushing_2pt_conversions",
      "receiving_2pt_conversions", "def_safeties"),
     (6.0, 3.0, 1.0, 2.0, 2.0, 2.0)),
]

# R4 vertical: player-credit stat -> registered team-role witness sources. The join
# itself is a K-plane contract (kc_planes); candidates carry the source, verdicts
# stay PENDING_TEST until the plane runs. Newspaper rows are ESCALATED by law.
VERTICAL_WITNESSES = [
    ("passing_yards", "pfr_box_team_stats"),
    ("rushing_yards", "pfr_box_team_stats"),
    ("passing_tds", "pfr_box_team_stats"),
    ("rushing_tds", "pfr_box_team_stats"),
    ("completions", "pfr_box_team_stats"),
    ("attempts", "pfr_box_team_stats"),
    ("passing_interceptions", "pfr_box_team_stats"),
    ("fumbles_lost", "pfr_box_team_stats"),
    # O.7: further packed team-stat lines measured 2026-07-26 — 'Rush-Yds-TDs' att,
    # 'Sacked-Yards' (1952+), 'Fumbles-Lost' first part. These are the ONLY known
    # second roots for the sack_yards_lost / fumbles burn-down cells (§16.1).
    ("carries", "pfr_box_team_stats"),
    ("sacks_suffered", "pfr_box_team_stats"),
    ("sack_yards_lost", "pfr_box_team_stats"),
    ("fumbles", "pfr_box_team_stats"),
    ("passing_yards", "newspaper_team_stats"),
    ("rushing_yards", "newspaper_team_stats"),
    ("total_points_scored", "newspaper_team_stats"),
]


def _bucket_families(cols: set[str]) -> dict[str, list[str]]:
    """parent -> deduped bucket columns (duplicate numeric ranges collapse to the
    first name in sorted order; the alternates surface as alias candidates)."""
    fams: dict[str, dict[tuple, str]] = {}
    for c in sorted(cols):
        m = _BUCKET_RE.match(c)
        if not m or m.group("parent") not in cols:
            continue
        rng = (int(m.group("lo")), int(m.group("hi")) if m.group("hi") else None)
        fams.setdefault(m.group("parent"), {}).setdefault(rng, c)
    return {p: [v for _, v in sorted(r.items(), key=lambda kv: kv[0][0])]
            for p, r in fams.items() if len(r) >= 2}


def _bucket_aliases(cols: set[str]) -> list[tuple[str, str]]:
    """Same-parent same-range bucket variants (fg_made_60_ / fg_made_60plus / ...)."""
    seen: dict[tuple[str, tuple], list[str]] = {}
    for c in sorted(cols):
        m = _BUCKET_RE.match(c)
        if not m or m.group("parent") not in cols:
            continue
        rng = (int(m.group("lo")), int(m.group("hi")) if m.group("hi") else None)
        seen.setdefault((m.group("parent"), rng), []).append(c)
    out = []
    for variants in seen.values():
        out += [(variants[0], v) for v in variants[1:]]
    return out


# ---------------------------------------------------------------------------
# generation
# ---------------------------------------------------------------------------

def generate(stats: list[dict] | None = None) -> list[Candidate]:
    scope = scope_columns(stats)
    cols = set(scope)
    out: list[Candidate] = []

    def add(r_class, kind, op, lhs, rhs, grain, test_kind, basis, *,
            weights=(), agg="", note="", escalation="", population="all"):
        rhs = tuple(rhs)
        if lhs not in cols and grain.startswith(("player", "team", "league")) \
                and test_kind != "team_witness_join":
            return
        if any(c not in cols for c in rhs) and test_kind != "team_witness_join":
            return
        cid = _cid(r_class, op, lhs, rhs, grain)
        if population != "all":
            cid += f"@{population}"
        out.append(Candidate(cid, r_class, kind, op,
                             lhs, rhs, grain, test_kind, basis, weights=tuple(weights),
                             agg=agg, note=note, escalation=escalation,
                             population=population))

    # R1: att/made/missed morphology
    for base in _ATT_MADE_BASES:
        att, made, missed = f"{base}_att", f"{base}_made", f"{base}_missed"
        if {att, made} <= cols:
            add("R2", "made_le_att", "le", made, (att,), "player_week", "row_bound",
                "morphology:att_made_missed")
            if missed in cols:
                add("R1", "diff_identity", "eq", missed, (att, made), "player_week",
                    "row_formula", "morphology:att_made_missed", weights=(1.0, -1.0))

    # R1: rate identities (definition-scoped: sources round; the runner tests raw
    # equality AND equality under declared 1dp/2dp rounding, typing CONDITIONAL)
    for rate, (num, den) in sorted(RATE_MAP.items()):
        if {rate, num, den} <= cols:
            add("R1", "rate_identity", "eq", rate, (num, den), "player_week",
                "row_rate", "morphology:rate_name")

    # R1: declared composites
    for lhs, comps in COMPOSITES:
        add("R1", "sum_identity", "eq", lhs, comps, "player_week", "row_formula",
            "declared_composite")

    # R1: bucket alias variants (same parent, same numeric range)
    for a, b in _bucket_aliases(cols):
        add("R1", "alias", "eq", b, (a,), "player_week", "row_formula",
            "morphology:duplicate_bucket_range")

    # R1: *_canonical rename aliases
    for c in sorted(cols):
        if c.endswith("_canonical") and c[: -len("_canonical")] in cols:
            add("R1", "alias", "eq", c, (c[: -len("_canonical")],), "player_week",
                "row_formula", "morphology:canonical_suffix")

    # R2: declared bound pairs + *_lost <= base morphology
    for lhs, rhs in BOUND_PAIRS:
        add("R2", "bound", "le", lhs, (rhs,), "player_week", "row_bound",
            "declared_bound")
    for c in sorted(cols):
        if c.endswith("_lost") and c[:-5] in cols:
            add("R2", "lost_le_total", "le", c, (c[:-5],), "player_week", "row_bound",
                "morphology:lost_suffix")
    add("R2", "bound_le_diff", "le", "fg_blocked", ("fg_att", "fg_made"), "player_week",
        "row_bound", "declared_bound", weights=(1.0, -1.0),
        note="blocked <= nonmade (fg_nonmade partition parent, §0.2)")

    # R7: bucket partitions (+ each bucket bounded by its parent)
    for parent, buckets in sorted(_bucket_families(cols).items()):
        add("R7", "partition_sum", "eq", parent, tuple(buckets), "player_week",
            "partition_sum", "morphology:bucket_family")
        for b in buckets:
            add("R2", "bucket_le_parent", "le", b, (parent,), "player_week",
                "row_bound", "morphology:bucket_family")
        if parent == "fg_missed" and "fg_blocked" in cols:
            # blocked kicks are misses without a distance bucket (measured
            # 2026-07-26: gap 578 == fg_blocked 578, 1999-2025)
            add("R7", "partition_sum", "eq", parent, (*buckets, "fg_blocked"),
                "player_week", "partition_sum", "declared_partition_extra",
                note="distance buckets + blocked = missed")

    # R3: team-week mirrors
    for a, b, op, agg in MIRROR_PAIRS:
        add("R3", "player_mirror", op, a, (b,), "team_week",
            "team_mirror" if op == "eq" else "team_bound",
            "declared_mirror", agg=agg)

    # R5: cross-side defense-booking vs opponent-offense sums (population-scoped duals)
    for def_stat, off_parts in CROSS_SIDE_PAIRS:
        for pop in ("team_def_row", "idp_rows"):
            add("R5", "defense_eq_opp_offense", "eq", def_stat, off_parts,
                "team_week", "team_cross_side", "declared_cross_side",
                population=pop,
                note="defense-side booking (plane-scoped) = opponent offense-side "
                     "sum; the def planes are never combined")

    # R5: horizontal allowed-morphology
    for allowed, base, agg in ALLOWED_MAP:
        add("R5", "offense_eq_allowed", "eq", allowed, (base,), "team_week",
            "team_horizontal", "morphology:allowed_suffix", agg=agg)
    for name, comps, weights in TOTAL_YDS_VARIANTS:
        add("R5", f"offense_eq_allowed:{name}", "eq", "total_yds_allowed", comps,
            "team_week", "team_horizontal", "morphology:allowed_suffix",
            weights=weights, agg="SUM",
            note="gross-vs-net definition split (§0.2) -- variants, not both true")

    # R6: grain aggregation edges from contract grains + aggregation class
    for stat_id, s in sorted(scope.items()):
        agg_class = s.get("aggregation_class")
        if agg_class not in ("SUM", "MAX"):
            continue
        grains = set(s["grains"])
        if "season" in grains:
            add("R6", "weekly_to_season", "eq", stat_id, (stat_id,), "player_season",
                "grain_agg", "contract_grains", agg=agg_class)
        if "season_all" in grains:
            add("R6", "weekly_to_season_all", "eq", stat_id, (stat_id,),
                "player_season_all", "grain_agg", "contract_grains", agg=agg_class)
        if "career" in grains and "season" in grains:
            add("R6", "season_to_career", "eq", stat_id, (stat_id,), "player_career",
                "grain_agg", "contract_grains", agg=agg_class)
        if "career_all" in grains and "season_all" in grains:
            add("R6", "season_to_career_all", "eq", stat_id, (stat_id,),
                "player_career_all", "grain_agg", "contract_grains", agg=agg_class)

    # R8: league-year conservation (def-side rhs population-scoped, never both planes)
    for a, b, pop, why in CONSERVATION_PAIRS:
        add("R8", "conservation", "eq", a, (b,), "league_year",
            "league_conservation", "declared_conservation", note=why, population=pop)

    # R4: IDP -> team-defense vertical, every defense/fumbles-family stat
    for stat_id, s in sorted(scope.items()):
        if s["family"] in IDP_TEAM_VERTICAL_FAMILIES:
            add("R4", "idp_team_defense_vertical", "eq", stat_id, (stat_id,),
                "team_week", "team_idp_vertical", "dual_bookkeeping_planes",
                population="team_def_row",
                note="team-DEF row = SUM(IDP rows) per team-week; the IDP/team-"
                     "defense separation IS the reconciliation lane")

    # R9: scoring decomposition variants
    for name, comps, weights in SCORING_VARIANTS:
        add("R9", f"scoring_partition:{name}", "eq", "total_points_scored", comps,
            "player_week", "row_formula", "declared_scoring_variant", weights=weights,
            note="variants, not both true; definition adjudication via verdicts")

    # R4: vertical player->team witnesses (join = K plane; newspaper escalated).
    # The witness source is part of the edge identity -- same stat vs two different
    # team witnesses is two candidates.
    for stat_id, source in VERTICAL_WITNESSES:
        rhs = (f"{source}.{stat_id}",)
        out.append(Candidate(_cid("R4", "eq", stat_id, rhs, "team_week"), "R4",
                             "vertical_team_sum", "eq", stat_id, rhs, "team_week",
                             "team_witness_join", "player_vs_team_witness",
                             note=f"team witness: {source}",
                             escalation=("newspaper_root_crossing"
                                         if "newspaper" in source else "")))

    # determinism + uniqueness (first proposal wins; later duplicate ids collapse)
    seen: dict[str, Candidate] = {}
    for c in out:
        seen.setdefault(c.cand_id, c)
    return sorted(seen.values(), key=lambda c: (c.r_class, c.cand_id))


def by_class(cands: list[Candidate] | None = None) -> dict[str, int]:
    cands = cands if cands is not None else generate()
    counts: dict[str, int] = {}
    for c in cands:
        counts[c.r_class] = counts.get(c.r_class, 0) + 1
    return dict(sorted(counts.items()))


def main() -> int:
    cands = generate()
    print(f"candidates: {len(cands)}")
    for k, v in by_class(cands).items():
        print(f"  {k}: {v}")
    esc = [c for c in cands if c.escalation]
    print(f"escalated (never auto-run): {len(esc)}")
    for c in esc:
        print(f"  {c.cand_id}  [{c.escalation}]")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
