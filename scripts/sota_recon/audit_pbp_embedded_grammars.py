"""Column-by-column PBP embedded-grammar and promotion census.

This consumes the exhaustive raw-column profile and canonical schemas.  It writes
only an inspectable audit receipt; no supertable data is modified.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import duckdb

from scripts.sota_recon.sources import v26_plane


PROFILE = Path(r"D:\yahoo_oauth\docs\audits\pbp-2025-terminal-state-audit.json")
DESC_RECLASS = Path(r"D:\yahoo_oauth\docs\audits\pbp-desc-grammar-reclassification.json")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pbp-embedded-grammar-column-audit.json")


# These are source fields that contain an additional stable metric/grammar beyond
# a simple player identity or play atom.  They are intentionally conservative.
PROMOTION_SPECS = {
    "punt_inside_twenty": ("punt_in_20", "punt placement counter; no current canonical target found"),
    "punt_in_endzone": ("punt_in_endzone", "punt placement counter; no current canonical target found"),
    "punt_out_of_bounds": ("punt_out_of_bounds", "punt placement counter; no current canonical target found"),
    "punt_downed": ("punt_downed", "punt placement counter; no current canonical target found"),
    "punt_fair_catch": ("punt_fair_catch", "punt placement counter; no current canonical target found"),
    "kick_distance": ("kickoff_yds", "kick distance atom; no current canonical target found"),
    "kickoff_inside_twenty": ("kickoff_in_20", "kickoff placement counter; no current canonical target found"),
    "kickoff_in_endzone": ("kickoff_in_endzone", "kickoff placement counter; no current canonical target found"),
    "kickoff_out_of_bounds": ("kickoff_out_of_bounds", "kickoff placement counter; no current canonical target found"),
    "kickoff_downed": ("kickoff_downed", "kickoff placement counter; no current canonical target found"),
    "kickoff_fair_catch": ("kickoff_fair_catch", "kickoff placement counter; no current canonical target found"),
    "touchback": ("kickoff_tb / punt_tb", "generic touchback atom; play type separates kickoff and punt touchbacks"),
    "drive_play_count": ("drive_play_count", "team-drive volume metric"),
    "drive_time_of_possession": ("drive_time_of_possession", "team-drive time metric"),
    "drive_first_downs": ("drive_first_downs", "team-drive first-down metric"),
    "drive_inside20": ("drive_inside20", "team-drive red-zone-entry metric"),
    "drive_ended_with_score": ("drive_ended_with_score", "team-drive scoring outcome flag"),
    "drive_yards_penalized": ("drive_yards_penalized", "team-drive penalty-yard metric"),
    "lateral_receiving_yards": ("lateral_receiving_yards", "rare lateral-yard atom; candidate only if historical coverage warrants"),
    "lateral_rushing_yards": ("lateral_rushing_yards", "rare lateral-yard atom; candidate only if historical coverage warrants"),
    "own_kickoff_recovery": ("own_kickoff_recovery", "historical kickoff-recovery event; no dedicated canonical target found"),
    "lateral_recovery": ("lateral_recovery", "historical lateral-recovery event; no dedicated canonical target found"),
}


DERIVATION_INPUTS = {
    "first_down_rush", "first_down_pass", "first_down_penalty", "third_down_converted",
    "third_down_failed", "fourth_down_converted", "fourth_down_failed", "first_down",
    "series_success", "success", "yards_gained", "ydsnet", "passing_yards",
    "receiving_yards", "rushing_yards", "return_yards", "penalty_yards", "kick_distance",
    "drive_first_downs", "drive_ended_with_score", "series_result", "fixed_drive_result",
    "play_type_nfl", "two_point_conv_result", "defensive_two_point_conv",
}

MAPPED_GRAMMARS = {
    "field_goal_result": ("fg_made / fg_missed / fg_blocked", "result enum maps to existing kicking counters; blocked is also unsuccessful-attempt scope"),
    "extra_point_result": ("pat_made / pat_missed / pat_blocked", "result enum maps to existing PAT counters"),
    "two_point_conv_result": ("passing_2pt_conversions / rushing_2pt_conversions / receiving_2pt_conversions", "success/failure is combined with player role grammar"),
    "series_result": ("three_out / pts_def_3out / def_*_down counters", "series result combines with down and full-series state"),
    "play_type_nfl": ("event-family derivations", "typed play grammar drives counters; no duplicate text target"),
    "fixed_drive_result": ("drive/team derivations", "drive terminal grammar drives team/game metrics"),
    "fumble_recovery_1_yards": ("fumble_recovery_yards / fumble_recovery_yards_own / fumble_recovery_yards_opp", "recovery-role and team context determine target lane"),
    "fumble_recovery_2_yards": ("fumble_recovery_yards / fumble_recovery_yards_own / fumble_recovery_yards_opp", "rare second recovery role atom"),
    "own_kickoff_recovery_td": ("special_teams_tds", "kickoff recovery touchdown is a special-teams scoring witness"),
    "defensive_extra_point_attempt": ("defensive_two_point_attempt / pts_def_2pt", "defensive try context witness"),
    "defensive_extra_point_conv": ("defensive_two_point_conv / pts_def_2pt", "defensive try conversion witness"),
    "timeout": ("timeouts", "timeout event atom maps to existing timeout counter"),
    "punt_blocked": ("punts_blocked / pts_def_punt_block", "blocked-punt event witness"),
    "penalty_type": ("penalty / penalty_yards", "penalty grammar maps to event and yardage counters"),
}


def grammar_kind(column: str, dtype: str, terminal_class: str, distinct: int) -> str:
    name = column.lower()
    if column == "desc":
        return "FULL_TEXT_RESULT_GRAMMAR"
    if dtype == "VARCHAR" and any(k in name for k in ("time", "yard", "weather", "wind", "surface", "roof", "location", "transition")):
        return "COMPOUND_CONTEXT_GRAMMAR"
    if dtype == "VARCHAR" and terminal_class == "TERMINAL_STATE_ENUM":
        return "ENUM_RESULT_GRAMMAR"
    if dtype == "VARCHAR":
        return "IDENTITY_OR_CONTEXT_TEXT"
    if any(k in name for k in ("drive", "series", "down", "first_down", "inside", "attempt", "converted", "failed", "touchback", "fair_catch", "out_of_bounds", "recovery", "tackle", "fumble")):
        return "NUMERIC_EVENT_GRAMMAR"
    return "NUMERIC_ATOM"


def main() -> None:
    receipt = json.loads(PROFILE.read_text(encoding="utf-8"))
    profiles = receipt["column_profiles"]
    desc_reclass = json.loads(DESC_RECLASS.read_text(encoding="utf-8"))
    con = duckdb.connect()
    canonical_by_grain = {}
    for grain in ("weekly", "season", "career", "season_team"):
        path = v26_plane(grain)
        canonical_by_grain[grain] = {
            x[0] for x in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()
        }
    con.close()

    ledger = []
    for item in profiles:
        col = item["column"]
        dtype = item["dtype"]
        m = item.get("map_spec", {})
        disposition = m.get("disposition", "UNSPECIFIED")
        if col in PROMOTION_SPECS:
            action = "PROMOTION_CANDIDATE"
            target, reason = PROMOTION_SPECS[col]
            canonical_presence = {g: target in cols for g, cols in canonical_by_grain.items()}
        elif col in MAPPED_GRAMMARS:
            action = "DERIVATION_WITNESS"
            target, reason = MAPPED_GRAMMARS[col]
            target_tokens = re.findall(r"[A-Za-z_]+", target)
            canonical_presence = {g: any(t in cols for t in target_tokens) for g, cols in canonical_by_grain.items()}
        elif disposition in {"DERIVATION_INPUT_WITNESS", "STRUCTURED_SOURCE_WITNESS"} and col in DERIVATION_INPUTS:
            action = "DERIVATION_WITNESS"
            target = m.get("canonical")
            reason = "raw event atom/grammar is consumed by an explicit downstream derivation"
            canonical_presence = {g: bool(target and target in cols) for g, cols in canonical_by_grain.items()}
        elif disposition in {"CONTEXT_PROMOTION_CANDIDATE"}:
            action = "CONTEXT_PROMOTION_CANDIDATE"
            target = None
            reason = "stable game/context grammar; not a player-stat cell"
            canonical_presence = {g: False for g in canonical_by_grain}
        elif disposition in {"TERMINAL_STATE_ENUM", "CONTEXT_WITNESS", "STRUCTURED_SOURCE_WITNESS"} or item.get("terminal_class") == "TERMINAL_STATE_ENUM":
            action = "STRUCTURED_WITNESS"
            target = m.get("canonical")
            reason = "fully retained structured source grammar with no new standalone metric"
            canonical_presence = {g: bool(target and target in cols) for g, cols in canonical_by_grain.items()}
        else:
            action = "IDENTITY_PROVENANCE_OR_ATOM"
            target = m.get("canonical")
            reason = "identity, provenance, model state, or already-covered atom"
            canonical_presence = {g: bool(target and target in cols) for g, cols in canonical_by_grain.items()}
        ledger.append({
            "column": col,
            "dtype": dtype,
            "rows_2025": item["rows_2025"],
            "nonnull_2025": item["nonnull_2025"],
            "distinct_2025": item["distinct_2025"],
            "terminal_class": item["terminal_class"],
            "grammar_kind": grammar_kind(col, dtype, item["terminal_class"], item["distinct_2025"]),
            "existing_disposition": disposition,
            "action": action,
            "target_or_candidate": target,
            "canonical_presence_by_grain": canonical_presence,
            "reason": reason,
        })

    promotions = [x for x in ledger if x["action"] == "PROMOTION_CANDIDATE"]
    receipt_out = {
        "source_profile": str(PROFILE),
        "desc_reclassification": str(DESC_RECLASS),
        "scope": "all raw PBP columns; 2025 coverage and value-domain evidence",
        "raw_columns_reviewed": len(ledger),
        "grammar_kinds": {},
        "actions": {},
        "promotion_candidates": promotions,
        "ledger": ledger,
        "desc_grammar_status": {
            "normalized_grammar_count": desc_reclass["grammar_count"],
            "unclassified_after_reclassification": desc_reclass["unclassified_grammar_count_after_reclassification"],
        },
    }
    for x in ledger:
        receipt_out["grammar_kinds"][x["grammar_kind"]] = receipt_out["grammar_kinds"].get(x["grammar_kind"], 0) + 1
        receipt_out["actions"][x["action"]] = receipt_out["actions"].get(x["action"], 0) + 1
    OUT.write_text(json.dumps(receipt_out, indent=2), encoding="utf-8")
    print(json.dumps({
        "raw_columns_reviewed": len(ledger),
        "promotion_candidates": [x["column"] for x in promotions],
        "actions": receipt_out["actions"],
        "grammar_kinds": receipt_out["grammar_kinds"],
        "desc_unclassified": receipt_out["desc_grammar_status"]["unclassified_after_reclassification"],
        "output": str(OUT),
    }, indent=2))


if __name__ == "__main__":
    main()
