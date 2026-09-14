#!/usr/bin/env python3
"""Produce the column EXPOSURE plan: what each physical column becomes in the UI.

The supertable is not 1,092 candidate columns. ~70% of it is variant families —
385 rank_*, 56 lamar_*, 187 pts_*/fpts_*, 30 ppg_* and so on — where the league's
scoring configuration selects exactly ONE physical column to fill ONE user-visible
slot. A user never sees 385 rank columns; they see "Pos Rank".

So exposure is planned in two lanes:

  variantSlot  N physical columns -> 1 config-resolved slot
  direct       1 physical column  -> 1 user-visible column (or an explicit non-exposure)

Only the direct lane is a per-column placement question, which is why the real
exposure backlog is ~103 columns rather than the ~816 a naive registry join reports.

    python scripts/player_column_coverage/build_exposure_plan.py
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

# Variant families: (slot id, user-visible slot, regex, what selects the variant, mode)
VARIANT_FAMILIES: list[tuple[str, str, str, str, str]] = [
    ("rank", "Pos Rank / Ovr Rank", r"^rank_",
     "position scope x grain (weekly/season/alltime) x total|ppg x scoring (4/5/6pt x 0ppr/half/ppr/tep/ppfd)", "rankings"),
    ("lamar", "LAMAR", r"^lamar_",
     "team count x flex|superflex x reception format x pass-TD value", "basic"),
    ("fpts", "Points", r"^fpts_",
     "pass-TD value x reception format", "basic"),
    ("ppg", "PPG", r"^ppg_",
     "grain (season/alltime) x pass-TD value x reception format", "basic"),
    ("pts_allow", "PA Tier", r"^pts_allow",
     "DST points-allowed band selected by league DST scoring", "advanced"),
    ("yds_allow", "Yds All Tier", r"^yds_allow_",
     "DST yards-allowed band selected by league DST scoring", "advanced"),
    ("pts_component", "(feeds Points)", r"^pts_",
     "per-stat scoring components summed into Points; never shown individually", "internal"),
    ("avg_pts_next", "Next-Yr Proj", r"^avg_pts_next_year_",
     "pass-TD value x reception format", "research"),
    ("rolling", "Form", r"^rolling_",
     "window length x scoring variant", "advanced"),
    ("consistency", "Consistency", r"^consistency_",
     "threshold x scoring variant", "advanced"),
    ("weighted", "Weighted Form", r"^weighted_",
     "decay weighting x scoring variant", "advanced"),
]

# Direct columns that are deliberately never user-visible.
INTERNAL_EXACT = {
    "NFL_player_id": "join key",
    "player_week": "join key",
    "recon_correction_log": "reconciliation audit trail",
    "data_source": "ingestion provenance",
    "nfl_franchise_number": "internal franchise key (display uses nfl_team)",
    "opponent_nfl_franchise_number": "internal franchise key",
    "headshot_url": "asset URL, rendered as the player image not a column",
    "season_type": "partition key; regular/post filtering is a control, not a column",
}
INTERNAL_MARKERS = ("_repaired_at_", "_recomputed_at_", "_merged_at_", "_populated_at_")

# Direct columns reviewed and deliberately declined, with the reason that settles it.
DECLINED_EXACT = {
    "def_yards_allowed": (
        "duplicate concept: total_yds_allowed = def_yards_allowed + sack yards "
        "(measured delta 14.99 vs def_sack_yards 15.13, residual 0.25). The product "
        "exposes one yards-allowed stat and total_yds_allowed wins — deeper history "
        "(1920 vs 1978) and it feeds the DST yds_allow_* scoring tiers"
    ),
    "fantasy_position": "lineup slot from league data, not an NFL stat — belongs to league tables",
}

# Legacy-vocabulary columns from a `legacy_motherduck` merge. All carry exactly 5 rows
# (Tommy Davis, SFO, 1961/1966/1968) out of 1,219,756, and every value is already held
# by a canonical column — verified row by row, so dropping them loses nothing:
#   fgm == fg_made | pts == pts_k_std | xp% == pat_pct x100 | fg% == fg_pct x100
#   2pm, sfty all zero
# Action is a pipeline DROP, not a display decision.
RETIRE_EXACT = {
    "fgm": "legacy alias of fg_made; identical on all 5 rows it covers",
    "pts": "legacy alias of pts_k_std; identical on all 5 rows it covers",
    "xp%": "legacy alias of pat_pct on a 0-100 scale (100.0 vs 1.00)",
    "fg%": "legacy alias of fg_pct on a 0-100 scale (100.0 vs 1.00)",
    "2pm": "legacy two-point alias; all zero across its 5 rows",
    "sfty": "legacy alias of def_safeties; all zero across its 5 rows",
    "rate": "legacy alias of passer_rating; empty-string typed, no usable values",
}

# Approved 2026-07-20: physical columns adopted to REPLACE a runtime calculation.
# Precompute rule — a value that can be precomputed must not be computed at read time.
REPLACES_RUNTIME = {
    "completion_pct", "passing_yards_per_attempt", "passing_td_pct", "passing_int_pct",
    "catch_pct", "receiving_yards_per_reception", "receiving_yards_per_target",
    "rushing_yards_per_carry", "punt_yards_per_punt",
    "scrimmage_yards", "total_tds",
}

# Approved 2026-07-20: success-rate family is backfillable to the model-free floor (1978),
# matching the def_* model-free columns rather than stopping at the PBP/EPA floor of 1999.
BACKFILL_TO_1978 = {"pass_success", "pass_success_plays", "rush_success", "rush_success_plays",
                    "rec_success", "rec_success_plays"}

# Approved 2026-07-20: already reaching users through a non-registry path.
# home_away drives the vs/@ distinction and is 100% populated 1920-2025 in this release
# (2003-14: 255,810/255,810; 2015-25: 283,069/283,069) — an earlier "empty post-2002"
# note was stale.
IN_USE_OFF_REGISTRY = {
    "home_away": "drives the vs/@ matchup distinction; fully populated 1920-2025",
}

# Group placement for columns with no registry category yet, inferred from the name.
# Ordered: first match wins.
NAME_GROUPS: list[tuple[str, str, str]] = [
    # Approved 2026-07-20: bonuses become a new Advanced lens, "Thresholds" — counts of
    # threshold games (300-yard passing games, 100-yard rushing games, and so on).
    (r"^bonus_", "advanced", "Thresholds"),
    (r"^ngs_", "advanced", "Tracking"),
    (r"^(pass_success|rush_success)", "advanced", "Efficiency"),
    (r"^(punt|p_)", "advanced", "Punting"),
    (r"^(pick6|sack|safet)", "basic+advanced", "Team Defense / Individual Defense"),
    (r"^(starter_position|season_type)", "context", "Game Context"),
    (r"^pts$", "basic", "Value"),
    (r"^(passing_adjusted|passing_net_yards|completion_pct|dropbacks|passing_completed_air)", "advanced", "Passing"),
    (r"^(receiving_completed_air|catch_pct|rec_)", "advanced", "Receiving"),
    (r"^(rush_|rushing_)", "advanced", "Rushing"),
    (r"^(fg|pat|xp|kick)", "basic+advanced", "Kicking"),
    (r"^(def_|idp_|turnovers)", "basic+advanced", "Team Defense / Individual Defense"),
    (r"^(touches|opportunities|yards_per_touch|yds_from_scrimmage|scrimmage_|all_purpose|total_td|total_return|rush_receive_td)", "basic", "Scrimmage"),
    (r"^(team_points|opponent_points|game_margin|total_points_scored|is_|home_away|primary_position|nfl_position)", "context", "Game Context"),
    (r"^(fantasy_points|lamar|points)", "basic", "Value"),
    # Generic catch-alls last, so specific rules above win.
    (r"^passing_", "advanced", "Passing"),
    (r"^receiving_", "advanced", "Receiving"),
    (r"^rushing_", "advanced", "Rushing"),
]

# Mode/group placement for the direct lane, by registry category.
CATEGORY_PLACEMENT = {
    "identity": ("context", "Identity"),
    "passing": ("basic+advanced", "Passing"),
    "rushing": ("basic+advanced", "Rushing"),
    "receiving": ("basic+advanced", "Receiving"),
    "kicking": ("basic+advanced", "Kicking"),
    "defense": ("basic+advanced", "Team Defense / Individual Defense"),
    "aggregate": ("basic", "Scrimmage"),
    "bio": ("profile", "Profile"),
    "derived": ("advanced", "Efficiency"),
    "value": ("basic", "Value"),
}

# Cohort inference from the positions that actually carry values.
OFFENSE = {"QB", "RB", "WR", "TE"}
IDP = {"DL", "LB", "DB", "CB", "S", "DE", "DT", "OLB", "ILB", "MLB", "SS", "FS", "NT"}


def cohorts_for(positions: list[str]) -> list[str]:
    if not positions:
        return []
    upper = {p.upper() for p in positions}
    if upper == {"DEF"}:
        return ["DEF"]
    if upper == {"K"} or upper == {"K", "P"}:
        return ["K"]
    if upper & IDP and not (upper & OFFENSE):
        return ["IDP", "DL", "LB", "DB"]
    hits = sorted(upper & OFFENSE)
    if len(hits) >= 3:
        return ["ALL", "SUPERFLEX", "FLEX", *hits]
    return ["ALL", *hits] if hits else ["ALL"]


def placement_for(row: dict[str, Any], name: str) -> tuple[str, str]:
    """Registry category first; fall back to name pattern so nothing lands Unassigned."""
    category = row.get("registryCategory")
    if category and category in CATEGORY_PLACEMENT:
        return CATEGORY_PLACEMENT[category]
    for pattern, mode, group in NAME_GROUPS:
        if re.match(pattern, name):
            return mode, group
    return "advanced", "Unassigned"


def build(ledger: dict[str, Any]) -> dict[str, Any]:
    rows = {row["sourceColumn"]: row for row in ledger["ledger"]}
    assigned: set[str] = set()
    slots: list[dict[str, Any]] = []

    for slot_id, label, pattern, selector, mode in VARIANT_FAMILIES:
        members = sorted(
            name for name in rows
            if re.match(pattern, name) and name not in assigned
        )
        if not members:
            continue
        assigned |= set(members)
        populated = [name for name in members if rows[name]["coverageState"] != "empty"]
        slots.append({
            "slotId": slot_id,
            "userVisibleSlot": label,
            "mode": mode,
            "physicalColumnCount": len(members),
            "populatedCount": len(populated),
            "emptyCount": len(members) - len(populated),
            "selectedBy": selector,
            "columns": members,
        })

    direct: list[dict[str, Any]] = []
    for name, row in rows.items():
        if name in assigned:
            continue
        if any(marker in name for marker in INTERNAL_MARKERS):
            disposition, reason, mode, group = "internal", "pipeline provenance stamp", None, None
        elif name in INTERNAL_EXACT:
            disposition, reason, mode, group = "internal", INTERNAL_EXACT[name], None, None
        elif row["family"] == "flag_provenance":
            disposition, reason, mode, group = "internal", "pipeline bookkeeping flag", None, None
        elif name in RETIRE_EXACT:
            disposition, reason, mode, group = "retire", RETIRE_EXACT[name], None, None
        elif name in DECLINED_EXACT:
            disposition, reason, mode, group = "declined", DECLINED_EXACT[name], None, None
        elif name in IN_USE_OFF_REGISTRY:
            mode, group = placement_for(row, name)
            disposition, reason = "exposed", IN_USE_OFF_REGISTRY[name]
        elif row["coverageState"] == "empty":
            disposition, reason, mode, group = "blocked", "zero non-null rows in the release", None, None
        elif not row["liveOnFly"]:
            mode, group = placement_for(row, name)
            disposition, reason = "gatedOnPromote", "canonical but absent from the live schema; exposable after the table swap"
        elif row["displayName"]:
            mode, group = placement_for(row, name)
            disposition, reason = "exposed", "registered and live"
        else:
            mode, group = placement_for(row, name)
            disposition, reason = "proposed", "live and populated but not registered — needs a placement decision"

        direct.append({
            "sourceColumn": name,
            "display": row["displayName"],
            "family": row["family"],
            "category": row["registryCategory"],
            "mode": mode,
            "group": group,
            "cohorts": cohorts_for(row["positionsWithValues"]),
            "minYear": row["minYear"],
            "maxYear": row["maxYear"],
            "coverageState": row["coverageState"],
            "liveOnFly": row["liveOnFly"],
            "disposition": disposition,
            "reason": reason,
            "replacesRuntimeCalculation": name in REPLACES_RUNTIME,
            "backfillTarget": 1978 if name in BACKFILL_TO_1978 else None,
        })

    direct.sort(key=lambda item: (item["disposition"], item["sourceColumn"]))
    counts: dict[str, int] = {}
    for item in direct:
        counts[item["disposition"]] = counts.get(item["disposition"], 0) + 1

    return {
        "provenance": ledger["provenance"],
        "summary": {
            "canonicalColumns": len(rows),
            "variantPhysicalColumns": len(assigned),
            "variantSlots": len(slots),
            "directColumns": len(direct),
            "directByDisposition": dict(sorted(counts.items())),
        },
        "variantSlots": slots,
        "directColumns": direct,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=Path("docs/player-column-coverage"))
    args = parser.parse_args()

    ledger = json.loads((args.out_dir / "ledger.json").read_text(encoding="utf-8"))
    plan = build(ledger)
    with (args.out_dir / "exposure-plan.json").open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(plan, handle, indent=2)
        handle.write("\n")

    summary = plan["summary"]
    print(f"canonical columns:        {summary['canonicalColumns']}")
    print(f"  variant physical:       {summary['variantPhysicalColumns']} -> {summary['variantSlots']} slots")
    print(f"  direct columns:         {summary['directColumns']}")
    print(f"    by disposition:       {summary['directByDisposition']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
