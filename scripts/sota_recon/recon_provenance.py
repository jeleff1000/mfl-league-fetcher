"""
sota_recon/recon_provenance.py -- classify EVERY atom by provenance tier (the completeness ledger).

Answers two questions precisely:
  (1) what does a WITNESS have that our super table LACKS / lost?  -> tier WITNESS_GAP (backfill targets)
  (2) what do we TRACK that no witness provides (we derive it)?    -> tier DERIVED (computed, not witnessed)

Every super-table column + every witnessed atom is partitioned into:
  WITNESSED   - a witness carries this atom AND our super table has it (validated by recon_nflcom etc.)
  WITNESS_GAP - a witness carries it, super table does NOT (or is empty in an era) = we lost/never ingested it
  DERIVED     - we COMPUTE it from atoms we hold; no witness provides it directly (opportunities, scrimmage...)
  COMPUTED    - our own scoring/ranking/model outputs (pts_*, fpts_*, rank_*, lamar_*, ppg, percentile)
  ORPHAN      - in super, no witness, not a known derivation -> VERIFY (could be a stale/unverifiable column)

Uses the cached recon matrix (witness_contracts --json) so it's fast (no re-introspection).

    python -m scripts.sota_recon.recon_provenance [--matrix fanned_matrix.json]
"""
from __future__ import annotations

import argparse
import glob
import json
import re
from pathlib import Path

import duckdb

# Atoms we DERIVE from other atoms -- NO witness provides them directly; they are tracked as computed.
# {atom: formula-note}
DERIVED_ATOMS = {
    "opportunities": "carries + targets",
    "touches": "carries + receptions",
    "yds_from_scrimmage": "rushing_yards + receiving_yards",
    "scrimmage_yards": "rushing_yards + receiving_yards",
    "scrimmage_tds": "rushing_tds + receiving_tds",
    "all_purpose_yards": "rushing + receiving + return yards",
    "yards_per_touch": "yds_from_scrimmage / touches",
    "yds_per_touch": "yds_from_scrimmage / touches",
    "total_tds_scored": "rush+rec+return+fum/int-ret TDs scored",
    "rush_receive_td": "rushing_tds + receiving_tds",
    "turnovers": "fumbles_lost + passing_interceptions",
    "total_points_scored": "6*TDs + kicking + 2pt",
    "total_return_yards": "kickoff_return_yards + punt_return_yards",
    "def_tackles_combined": "def_tackles_solo + def_tackle_assists",
    "dropbacks": "attempts + sacks_suffered + scrambles",
    # rate atoms = num/den (any era from the num/den atoms)
    "comp_pct": "completions/attempts", "yards_per_attempt": "passing_yards/attempts",
    "yards_per_carry": "rushing_yards/carries", "yards_per_reception": "receiving_yards/receptions",
    "catch_rate": "receptions/targets", "yards_per_target": "receiving_yards/targets",
    "passer_rating": "NFL formula from cmp/att/yds/td/int", "fg_pct": "fg_made/fg_att",
    "wopr": "1.5*target_share + 0.7*air_yards_share", "racr": "receiving_yards/air_yards",
    "pacr": "passing_yards/passing_air_yards", "adot": "air_yards/targets",
}

# our computed outputs (not directly-witnessed stats) -- prefixes/patterns
_COMPUTED_RE = re.compile(
    r"^(pts_|fpts_|rank_|lamar_|weighted_ppg|ppg_|clutch|replacement_|expected_|proj_|"
    r"consistency_|rolling_|avg_pts_next_year_|bonus_|dst_|yds_allow_|pts_allow_|"
    r"rz_)"
    r"|(_ppg$|_zscore$|_percentile$|_pctl$|_rank$|_grade$|_allowed$|_next_year)")
# our DST 'points/yards allowed' + game-context computed columns
_COMPUTED_EXACT = {"points_allowed", "total_yds_allowed", "dst_points_allowed", "dst_return_yards",
                   "fourth_down_stop", "three_out", "pass_success_plays", "rush_success_plays",
                   "rec_success_plays", "pass_explosive_20", "rush_explosive_10", "rec_explosive_20"}
# identity / metadata / audit-stamp columns (not stats)
_META_RE = re.compile(
    r"(_id$|_url$|_json$|_name$|^is_|_name_|slug|headshot|player_week|franchise|"
    r"^year$|^week$|^season|^team$|^player$|^position$|^nfl_position$|manager|^age$|"
    r"birth|height|weight|college|draft|hof|allpro|probowl|forty|bench|vertical|prov|recon_|"
    r"_repaired_at_|_recomputed_at_|_merged_at_|^data_source$|^event_rows$|^timeouts$|"
    r"game_date|home_away|nfl_team|opponent|team_points|game_margin|starter_position|"
    r"primary_position|fantasy_position|^pts$|^rate$|_canonical$)")


def _latest_v26():
    return sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                  key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]


# PBP/NGS-derived advanced stats (rolled up from plays; era floor ~1999 PBP-advanced / 2016 NGS)
_ADVANCED_RE = re.compile(
    r"(_epa$|_wpa$|_cpoe$|air_yards|_yards_after_catch$|_yards_after_contact$|_yards_before_contact$|"
    r"_broken_tackles$|_drops$|_hurried$|_hurries$|_pressured$|_pressures$|_hits$|_knockdowns$|_blitzed$|"
    r"_blitzes$|_poor_throws$|_scrambles$|snap|_share$|_adot$|target_interceptions|batted_passes|"
    r"tackles_missed|completed_air_yards|passer_rating_allowed)")


def classify(super_cols, witnessed_atoms):
    tiers = {"WITNESSED": [], "DERIVED": [], "ADVANCED": [], "COMPUTED": [], "ORPHAN": [], "META": []}
    for c in sorted(super_cols):
        if _META_RE.search(c):
            tiers["META"].append(c)
        elif _COMPUTED_RE.search(c) or c in _COMPUTED_EXACT:
            tiers["COMPUTED"].append(c)
        elif c in DERIVED_ATOMS:
            tiers["DERIVED"].append(c)
        elif _ADVANCED_RE.search(c):
            tiers["ADVANCED"].append(c)
        elif c in witnessed_atoms:
            tiers["WITNESSED"].append(c)
        else:
            tiers["ORPHAN"].append(c)
    # witness has it, super lacks it entirely = what we've lost / never ingested
    witness_gap = sorted(a for a in witnessed_atoms if a not in super_cols and a not in DERIVED_ATOMS)
    return tiers, witness_gap


def run(matrix_path=None):
    con = duckdb.connect()
    super_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{_latest_v26()}')").fetchall()}
    con.close()
    if matrix_path and Path(matrix_path).exists():
        m = json.loads(Path(matrix_path).read_text())
        witnessed = set(m.get("matrix", {}).keys())
    else:
        from .witness_contracts import run as wrun
        witnessed = set(wrun()["matrix"].keys())
    tiers, witness_gap = classify(super_cols, witnessed)
    print("=" * 90)
    print("ATOM PROVENANCE LEDGER  (super cols =", len(super_cols), "| witnessed atoms =", len(witnessed), ")")
    print("=" * 90)
    for t in ("WITNESSED", "DERIVED", "ADVANCED", "COMPUTED", "ORPHAN", "META"):
        print(f"\n[{t}] {len(tiers[t])}")
        if t in ("DERIVED", "ADVANCED", "ORPHAN"):
            print("  " + ", ".join(tiers[t]))
    print("\n" + "=" * 90)
    print(f"WITNESS_GAP -- a witness HAS it, our super table LACKS it ({len(witness_gap)}) = backfill / recover targets:")
    print("  " + ", ".join(witness_gap[:80]) + (" ..." if len(witness_gap) > 80 else ""))
    print("\nDERIVED tracked (we compute, no witness needed):")
    for a in sorted(DERIVED_ATOMS):
        have = "in super" if a in super_cols else "TODO-add"
        print(f"  {a:22} = {DERIVED_ATOMS[a]:42} [{have}]")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--matrix", default=None); a = ap.parse_args()
    run(matrix_path=a.matrix)
