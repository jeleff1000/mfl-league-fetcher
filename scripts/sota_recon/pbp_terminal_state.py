"""Terminal-state closure audit for the PBP witness surface.

The legacy PBP contract proves that the 102 player-week atoms exist in the
rollup.  This audit closes the missing semantic layer: every PBP-capable field
is either materialized, contracted as a derivation, explicitly excluded with a
reason, or fails the terminal gate as unresolved.

Read-only.  No release values are changed.
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
from pathlib import Path

import duckdb

from .sources import PBP_MERGED, PBP_ROLLUP, PFR_BOX_PBP, PFR_BOX_SCORING, latest_v26

TEAM_DEF = Path(
    r"D:\league-history-data\nfl\raw\stathead\generated\pbp_team_defense_1978_2025\pbp_team_defense_week.parquet"
)

CONTRACT = Path(__file__).parent / "witness_gate" / "contracts" / "pbp_contracts.v1.json"
FORMULA_SOURCE = Path(__file__).parents[1] / "aggregate_merged_pbp_for_supertable_audit.py"

META = {
    "player_week", "NFL_player_id", "player", "position", "year", "week", "season_type",
    "nfl_team", "opponent_nfl_team", "games", "event_rows", "event_roles",
    "pbp_player_id", "pbp_player_id_clean", "pbp_player_name", "player_id_namespaces",
    "pbp_source_systems", "mapped_to_player_bio", "bio_lookup_candidate_count",
    "nfl_team_context_count", "opponent_context_count",
}

EXTENSIONS = {
    "passing_first_downs": {"source": "raw_pbp", "fields": ["first_down_pass", "passer_player_id"], "status": "witnessed_reconciliation", "reason": "direct PBP witness; definition conflicts remain review-only"},
    "rushing_first_downs": {"source": "raw_pbp", "fields": ["first_down_rush", "rusher_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "direct PBP witness under locked PBP-DISPOSITION-NO-PLAY-001; nullified plays excluded"},
    "rushing_fumbles": {"source": "raw_pbp", "fields": ["rush_attempt", "rusher_player_id", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "direct PBP witness under locked PBP-DISPOSITION-NO-PLAY-001; either fumble slot may identify the rusher"},
    "rushing_fumbles_lost": {"source": "raw_pbp", "fields": ["rush_attempt", "fumble_lost", "rusher_player_id", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "direct PBP witness under locked PBP-DISPOSITION-NO-PLAY-001; either fumble slot may identify the rusher"},
    "fumbles": {"source": "raw_pbp", "fields": ["fumble", "fumble_lost", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "sum of the two PBP fumble slots under the locked disposition taxonomy"},
    "fumbles_lost": {"source": "raw_pbp", "fields": ["fumble", "fumble_lost", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "sum of the two PBP fumble slots with fumble_lost=1 under the locked disposition taxonomy"},
    "receiving_fumbles": {"source": "raw_pbp", "fields": ["complete_pass", "receiver_player_id", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "completed-pass fumble partition; either fumble slot may identify the receiver"},
    "receiving_fumbles_lost": {"source": "raw_pbp", "fields": ["complete_pass", "fumble_lost", "receiver_player_id", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "completed-pass lost-fumble partition; either fumble slot may identify the receiver"},
    "sack_fumbles": {"source": "raw_pbp", "fields": ["sack", "passer_player_id", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "sack fumble partition; either fumble slot may identify the passer"},
    "sack_fumbles_lost": {"source": "raw_pbp", "fields": ["sack", "fumble_lost", "passer_player_id", "fumbled_1_player_id", "fumbled_2_player_id", "desc", "play_type"], "status": "witnessed_reconciliation", "reason": "sack lost-fumble partition; either fumble slot may identify the passer"},
    "receiving_first_downs": {"source": "raw_pbp", "fields": ["first_down_pass", "receiver_player_id"], "status": "witnessed_reconciliation", "reason": "direct PBP witness; definition conflicts remain review-only"},
    "passing_yards_after_catch": {"source": "raw_pbp", "fields": ["yards_after_catch", "passer_player_id"], "status": "witnessed_direct", "reason": "raw PBP component; needs licensed rollup lane"},
    "receiving_yards_after_catch": {"source": "raw_pbp", "fields": ["yards_after_catch", "receiver_player_id"], "status": "witnessed_direct", "reason": "raw PBP component; release column exists"},
    "passing_completed_air_yards": {"source": "raw_pbp", "fields": ["air_yards", "complete_pass", "passer_player_id"], "status": "witnessed_direct", "reason": "raw PBP component; release column exists"},
    "receiving_completed_air_yards": {"source": "raw_pbp", "fields": ["air_yards", "complete_pass", "receiver_player_id"], "status": "witnessed_direct", "reason": "raw PBP component; release column exists"},
    "receiving_target_interceptions": {"source": "raw_pbp", "fields": ["interception", "receiver_player_id"], "status": "witnessed_direct", "reason": "existing PBP backfill lane"},
    "pick6": {"source": "raw_pbp", "fields": ["interception", "return_touchdown", "interception_player_id"], "status": "witnessed_direct", "reason": "existing PBP backfill lane"},
    "def_success_allowed": {"source": "team_defense_rollup", "fields": ["def_success_allowed"], "status": "witnessed_direct", "reason": "team-defense PBP rollup exists"},
    "def_third_down_faced": {"source": "team_defense_rollup", "fields": ["def_third_down_faced"], "status": "witnessed_direct", "reason": "team-defense PBP rollup exists"},
    "def_third_down_allowed": {"source": "team_defense_rollup", "fields": ["def_third_down_allowed"], "status": "witnessed_direct", "reason": "team-defense PBP rollup exists"},
    "def_fourth_down_faced": {"source": "team_defense_rollup", "fields": ["def_fourth_down_faced"], "status": "witnessed_direct", "reason": "team-defense PBP rollup exists"},
    "def_fourth_down_allowed": {"source": "team_defense_rollup", "fields": ["def_fourth_down_allowed"], "status": "witnessed_direct", "reason": "team-defense PBP rollup exists"},
    "def_air_yards_allowed": {"source": "raw_pbp", "fields": ["air_yards", "defteam"], "status": "witnessed_reconciliation", "reason": "raw component exists; source-definition comparison is review-only"},
    "def_yards_after_catch_allowed": {"source": "raw_pbp", "fields": ["yards_after_catch", "defteam"], "status": "witnessed_reconciliation", "reason": "raw component exists; source-definition comparison is review-only"},
    "passing_pressured": {"source": "raw_pbp", "fields": ["qb_hit"], "status": "excluded", "reason": "qb_hit is not pressure; no exact PBP pressure field"},
    "receiving_broken_tackles": {"source": "raw_pbp", "fields": ["tackle_for_loss"], "status": "excluded", "reason": "PBP has tackle events but no exact broken-tackle attribution"},
    "rushing_broken_tackles": {"source": "raw_pbp", "fields": ["tackle_for_loss"], "status": "excluded", "reason": "PBP has tackle events but no exact broken-tackle attribution"},
    "def_knockdowns": {"source": "raw_pbp", "fields": ["qb_hit"], "status": "excluded", "reason": "qb_hit is not knockdown attribution"},
    "def_tackles_missed": {"source": "raw_pbp", "fields": [], "status": "excluded", "reason": "no missed-tackle event in merged PBP"},
}

RAW_CONTEXT_PREFIXES = (
    "game_", "home_", "away_", "total_", "posteam_", "defteam_", "score_", "drive",
    "quarter", "half_", "play_clock", "time", "weather", "stadium", "vegas", "prob",
    "wp", "ep", "spread", "total_line", "location", "roof", "surface", "temp", "wind",
    "coach", "series", "fixed_", "start_time", "end_clock", "end_yard", "order_",
    "nfl_api", "old_game", "play_id", "game_id", "season", "week", "qtr", "down",
    "yd", "yrd", "clock", "defteam", "div_game",
)

RAW_TECHNICAL = {
    "id", "name", "desc", "jersey_number", "fantasy", "fantasy_id", "fantasy_player_id",
    "fantasy_player_name", "player_id_namespace", "pbp_source_system",
}

RAW_EVENT_MAPPING = {
    "air_yards": "air_yards_components",
    "yards_after_catch": "yac_components",
    "epa": "role_epa", "wpa": "role_wpa", "qb_epa": "passing_epa",
    "success": "role_success", "complete_pass": "completions_receptions",
    "pass_attempt": "attempts", "rush_attempt": "carries", "pass_touchdown": "passing_tds",
    "rush_touchdown": "rushing_tds", "touchdown": "td_partition", "interception": "interceptions",
    "sack": "sacks", "fumble": "fumbles", "fumble_lost": "fumbles_lost",
    "first_down": "first_downs", "first_down_pass": "passing_receiving_first_downs",
    "first_down_rush": "rushing_first_downs", "first_down_penalty": "team_first_downs",
    "third_down_converted": "team_third_down", "third_down_failed": "team_third_down",
    "fourth_down_converted": "team_fourth_down", "fourth_down_failed": "team_fourth_down",
    "penalty": "team_penalty", "penalty_yards": "team_penalty",
    "field_goal_attempt": "field_goals", "kickoff_attempt": "kickoffs", "punt_attempt": "punts",
    "return_yards": "returns", "return_touchdown": "return_tds", "kick_distance": "kicking_distance",
    "solo_tackle": "def_tackling", "assist_tackle": "def_tackling",
    "tackle_with_assist": "def_tackling", "tackled_for_loss": "def_tackling",
    "qb_hit": "def_qb_hits", "safety": "def_safeties",
    "fumble_forced": "def_fumbles_forced", "pass_defense": "def_pass_defended",
}

RAW_CLASSIFIED_NON_ATOMS = set("""
aborted_play air_epa air_wpa comp_air_epa comp_air_wpa comp_yac_epa comp_yac_wpa cp cpoe
def_wp defensive_extra_point_attempt defensive_extra_point_conv defensive_two_point_attempt
defensive_two_point_conv extra_point_attempt extra_point_prob extra_point_result fg_prob
field_goal_result fumble_not_forced fumble_out_of_bounds fumble_recovery_1_yards
fumble_recovery_2_yards goal_to_go incomplete_pass kickoff_downed kickoff_fair_catch
kickoff_in_endzone kickoff_inside_twenty kickoff_out_of_bounds no_huddle no_score_prob
opp_fg_prob opp_safety_prob opp_td_prob out_of_bounds own_kickoff_recovery own_kickoff_recovery_td
pass pass_length pass_location pass_oe passer passer_id passer_jersey_number passing_yards
penalty_type play play_deleted play_type play_type_nfl posteam punt_blocked punt_downed
punt_fair_catch punt_in_endzone punt_inside_twenty punt_out_of_bounds qb_dropback qb_kneel
qb_scramble qb_spike receiver receiver_id receiver_jersey_number receiving_yards
replay_or_challenge replay_or_challenge_result result run_gap run_location rush rusher rusher_id
rusher_jersey_number rushing_yards safety_prob shotgun side_of_field sp special special_teams_play
st_play_type td_prob total touchback two_point_attempt two_point_conv_result
two_point_conversion_prob xpass xyac_epa xyac_fd xyac_mean_yardage xyac_median_yardage
xyac_success yac_epa yac_wpa yardline_100 yards_gained
""".split())

ATOM_APPLICABILITY = {
    **{c: {"raw_start": 1978, "raw_gap_years": [1993], "external_license_start": 1999,
           "application": "PBP formula; pre-1999 source-definition review; 1993 NULL unless EPA evidence is restored"}
       for c in ("pass_success", "pass_success_plays", "rush_success", "rush_success_plays",
                 "rec_success", "rec_success_plays", "passing_epa", "rushing_epa", "receiving_epa")},
    **{c: {"raw_start": 1999, "raw_gap_years": [], "external_license_start": 1999,
           "application": "PBP formula with win-probability model"}
       for c in ("passing_wpa", "rushing_wpa", "receiving_wpa")},
    **{c: {"raw_start": 2006, "raw_gap_years": [], "external_license_start": 2006,
           "application": "charted PBP component; intended air yards or CPOE parts"}
       for c in ("passing_air_yards", "receiving_air_yards", "passing_cpoe_sum", "passing_cpoe_n")},
    **{c: {"raw_start": 1994, "raw_gap_years": [], "external_license_start": 1994,
           "application": "two-point conversion event after rule-era start"}
       for c in ("passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions")},
}


def _raw_field_class(field: str) -> tuple[str, str]:
    if field in RAW_TECHNICAL or field.endswith(("_player_id", "_player_name", "_team")):
        return ("identity_or_technical", "not a stat atom")
    if field.startswith(RAW_CONTEXT_PREFIXES):
        return ("game_context_or_model", "not a player stat atom")
    if field in RAW_EVENT_MAPPING:
        return ("stat_input", RAW_EVENT_MAPPING[field])
    if field.startswith(("lateral_", "total_home_", "total_away_")):
        return ("event_variant_or_team_total", "not a separate player stat atom")
    if field in RAW_CLASSIFIED_NON_ATOMS:
        return ("classified_event_or_model_input", "contracted as input/context, not a separate player atom")
    return ("unclassified", "raw field needs disposition")


def _source_inventory(con, raw: str) -> dict:
    """Inventory distinct play/event lineages; merged PBP is not the whole universe."""
    merged = [
        dict(source_system=s, rows=int(n), first_year=int(lo), last_year=int(hi))
        for s, n, lo, hi in con.execute(
            f"SELECT pbp_source_system, COUNT(*), MIN(season), MAX(season) FROM '{raw}' GROUP BY 1 ORDER BY 1"
        ).fetchall()
    ]
    pfr_path = _q(PFR_BOX_PBP)
    pfr = {"source": "pfr_box_pbp", "path": pfr_path, "exists": Path(pfr_path).exists()}
    if pfr["exists"]:
        pfr.update(dict(
            rows=int(con.execute(f"SELECT COUNT(*) FROM '{pfr_path}'").fetchone()[0]),
            first_year=int(con.execute(f"SELECT MIN(season) FROM '{pfr_path}'").fetchone()[0]),
            last_year=int(con.execute(f"SELECT MAX(season) FROM '{pfr_path}'").fetchone()[0]),
            grain="play",
            status="present_not_yet_in_merged_atom_rollup",
        ))
    scoring_path = _q(PFR_BOX_SCORING)
    scoring = {"source": "pfr_box_scoring", "path": scoring_path, "exists": Path(scoring_path).exists(),
               "grain": "scoring_event", "status": "separate_event_witness"}
    nflcom_files = []
    for pattern in (r"D:\league-history-data\nfl\raw\nflcom\*pbp*",
                    r"D:\league-history-data\nfl\raw\nflcom\tables\*pbp*",
                    r"D:\league-history-data\nfl\raw\nflcom\tables\*play*"):
        nflcom_files.extend(p for p in glob.glob(pattern) if Path(p).is_file())
    nflcom = {"source": "nflcom", "play_level_files_found": len(nflcom_files),
              "status": "play_level_candidates_require_review" if nflcom_files
              else "no_separate_play_level_pbp_found"}
    return {"merged_lineages": merged, "pfr_pbp": pfr, "pfr_scoring": scoring, "nflcom": nflcom,
            "source_rule": "each play-level lineage must have its own schema, coverage, identity map, atom formulas, and witness gate"}


def _q(value: str | Path) -> str:
    return Path(getattr(value, "path", value)).as_posix()


def _fp(fields: list[str]) -> str:
    return "sha256:" + hashlib.sha256(json.dumps(fields, separators=(",", ":")).encode()).hexdigest()


def run(src: str | None = None, out: str | None = None) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    release, roll, raw = _q(src or latest_v26()), _q(PBP_ROLLUP), _q(PBP_MERGED)
    rel_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{release}'").fetchall()}
    roll_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{roll}'").fetchall()]
    raw_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{raw}'").fetchall()}
    stats = [c for c in roll_cols if c not in META]
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    base_contract = {
        "count": len(stats), "fingerprint": _fp(stats),
        "count_expected": contract["stat_columns_count"],
        "fingerprint_expected": contract["stat_columns_fingerprint"],
        "pass": len(stats) == contract["stat_columns_count"]
        and _fp(stats) == contract["stat_columns_fingerprint"],
    }
    license_floor_exceptions = {}
    for stat, policy in ATOM_APPLICABILITY.items():
        if stat in rel_cols and policy["external_license_start"] > policy["raw_start"]:
            n = con.execute(
                f"SELECT COUNT(*) FROM '{release}' WHERE year < {int(policy['external_license_start'])}"
                f" AND {stat} IS NOT NULL"
            ).fetchone()[0]
            if not n:
                continue
            license_floor_exceptions[stat] = {
                "raw_start": policy["raw_start"],
                "external_license_start": policy["external_license_start"],
                "release_cells_before_external_license": int(n),
            }
    extensions = {}
    for stat, item in EXTENSIONS.items():
        fields_present = sorted(set(item["fields"]) & raw_cols)
        missing = sorted(set(item["fields"]) - raw_cols)
        extensions[stat] = {
            **item,
            "release_present": stat in rel_cols,
            "raw_fields_present": fields_present,
            "raw_fields_missing": missing,
            "contract_status": item["status"],
        }
    witness_checks = {}
    for stat, pid, flag in (("passing_first_downs", "passer_player_id", "first_down_pass"),
                            ("rushing_first_downs", "rusher_player_id", "first_down_rush"),
                            ("receiving_first_downs", "receiver_player_id", "first_down_pass")):
        witness_checks[stat] = dict(zip(
            ("coverage_rows", "agreements"),
            con.execute(f"""
                WITH x AS (
                  SELECT {pid} AS gsis, season AS yr, week AS wk, season_type AS st,
                         SUM({flag}) AS v
                  FROM '{raw}' WHERE {pid} IS NOT NULL GROUP BY 1,2,3,4
                )
                SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(x.v-r.{stat}) <= 0.5)
                FROM x JOIN '{release}' r
                  ON r.NFL_player_id=x.gsis AND r.year=x.yr AND r.week=x.wk
                 AND r.season_type=x.st
                WHERE r.{stat} IS NOT NULL
            """).fetchone(),
        ))
        n = witness_checks[stat]["coverage_rows"]
        witness_checks[stat]["density"] = round(witness_checks[stat]["agreements"] / n, 6) if n else None
    raw_field_census = {}
    for field in sorted(raw_cols):
        category, disposition = _raw_field_class(field)
        raw_field_census[field] = {"category": category, "disposition": disposition}
    unclassified_raw_fields = [f for f, v in raw_field_census.items() if v["category"] == "unclassified"]
    source_inventory = _source_inventory(con, raw)
    unresolved = [k for k, v in extensions.items() if v["contract_status"].startswith("unresolved")]
    unmaterialized = [k for k, v in extensions.items() if v["contract_status"] == "unresolved_materialization"]
    con.close()
    reconciliation_review = [k for k, v in extensions.items()
                             if v["contract_status"] == "witnessed_reconciliation"]
    result = {
        "status": ("fail" if not base_contract["pass"] or unmaterialized or unclassified_raw_fields
                   else "review" if unresolved or reconciliation_review or license_floor_exceptions
                   else "pass"),
        "release": release, "raw_pbp": raw, "player_week_rollup": roll,
        "team_defense_rollup": str(TEAM_DEF), "base_contract": base_contract,
        "formula_source": str(FORMULA_SOURCE),
        "source_inventory": source_inventory,
        "atom_applicability": {c: ATOM_APPLICABILITY.get(c, {
            "raw_start": 1978, "raw_gap_years": [], "external_license_start": 1978,
            "application": "model-free PBP formula; verify source-specific event attribution"})
            for c in stats},
        "license_floor_exceptions": license_floor_exceptions,
        "rollup_stat_count": len(stats), "rollup_stat_columns": stats,
        "extensions": extensions, "unresolved_extensions": unresolved,
        "unmaterialized_extensions": unmaterialized,
        "reconciliation_review_extensions": reconciliation_review,
        "witness_checks": witness_checks,
        "raw_field_count": len(raw_cols),
        "raw_field_census": raw_field_census,
        "unclassified_raw_fields": unclassified_raw_fields,
        "terminal_rule": "every PBP-capable stat is materialized, witnessed, explicitly excluded, or listed as unresolved",
    }
    if out:
        Path(out).write_text(json.dumps(result, indent=2, sort_keys=True), encoding="utf-8")
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src")
    ap.add_argument("--out")
    a = ap.parse_args()
    print(json.dumps(run(a.src, a.out), indent=2, sort_keys=True))
