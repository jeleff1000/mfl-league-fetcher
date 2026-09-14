"""
sota_recon/full_coverage.py  --  TOTAL source-table count for EVERY non-derived atom

Built by inverting a source->provides map (each source table -> the v26 columns it can witness,
own-side or via the opponent mirror), so the witness COUNT per column falls out and stays correct
as sources are added. Covers all ~190 non-derived atoms, not just the core 60. Marks each atom
DERIVED-DETERMINISTIC (recompute from atoms, not independently sourced) where applicable.

    python -m scripts.sota_recon.full_coverage            # per-atom source count
    python -m scripts.sota_recon.full_coverage --gaps      # atoms with <2 independent sources
"""
from __future__ import annotations
import argparse, collections
import pyarrow.parquet as pq
from .sources import latest_v26

# source_table -> (era_lo, era_hi, {v26_col: how}). how is informational.
# "opp:" prefix = witnessed via the OPPONENT's row (allowed-stats / defensive mirror).
SRC = {
    "player_offense": (1932, 2025, {
        "completions":"pass_cmp","attempts":"pass_att","passing_yards":"pass_yds","passing_tds":"pass_td",
        "passing_interceptions":"pass_int","sacks_suffered":"pass_sacked","sack_yards_lost":"pass_sacked_yds",
        "passing_long":"pass_long","carries":"rush_att","rushing_yards":"rush_yds","rushing_tds":"rush_td",
        "rushing_long":"rush_long","receptions":"rec","receiving_yards":"rec_yds","receiving_tds":"rec_td",
        "receiving_long":"rec_long","targets":"targets","fumbles":"fumbles","fumbles_lost":"fumbles_lost",
        # opponent mirror = this team's defense ALLOWED
        "passing_yds_allowed":"opp:pass_yds","passing_tds_allowed":"opp:pass_td",
        "rushing_yds_allowed":"opp:rush_yds","rushing_tds_allowed":"opp:rush_td",
        "receiving_yds_allowed":"opp:rec_yds","receiving_tds_allowed":"opp:rec_td",
        "def_completions_allowed":"opp:pass_cmp","def_completion_yards_allowed":"opp:pass_yds",
        "def_targets_allowed":"opp:targets","def_interceptions":"opp:pass_int","def_sacks":"opp:pass_sacked",
        "def_sack_yards":"opp:pass_sacked_yds"}),
    "player_defense": (1933, 2025, {
        "def_interceptions":"def_int","def_interception_yards":"def_int_yds","def_int_ret_td":"def_int_td",
        "def_sacks":"sacks","def_tackles_solo":"tackles_solo","def_tackle_assists":"tackles_assists",
        "def_tackles_with_assist":"tackles_combined","def_fumbles":"fumbles_rec","fum_rec":"fumbles_rec",
        "fum_rec_yds":"fumbles_rec_yds","fum_ret_td":"fumbles_rec_td","def_fumbles_forced":"fumbles_forced",
        "def_pass_defended":"pass_defended","def_tackles_for_loss":"tackles_loss","def_qb_hits":"qb_hits"}),
    "kicking": (1933, 2025, {
        "pat_made":"xpm","pat_att":"xpa","fg_made":"fgm","fgm":"fgm","fg_att":"fga","punts":"punt",
        "punt_yards":"punt_yds","punt_long":"punt_long"}),
    "returns": (1933, 2025, {
        "kickoff_returns":"kick_ret","kickoff_return_yards":"kick_ret_yds","kickoff_return_tds":"kick_ret_td",
        "kickoff_return_long":"kick_ret_long","punt_returns":"punt_ret","punt_return_yards":"punt_ret_yds",
        "punt_return_tds":"punt_ret_td","punt_return_long":"punt_ret_long","dst_return_yards":"kick_ret_yds+punt_ret_yds"}),
    "team_stats": (1920, 2025, {
        "completions":"Cmp-Att-Yd-TD-INT","attempts":"Cmp-Att-Yd-TD-INT","passing_yards":"Cmp-Att-Yd-TD-INT",
        "passing_tds":"Cmp-Att-Yd-TD-INT","passing_interceptions":"Cmp-Att-Yd-TD-INT","receptions":"Cmp-Att-Yd-TD-INT",
        "receiving_yards":"Cmp-Att-Yd-TD-INT","receiving_tds":"Cmp-Att-Yd-TD-INT","carries":"Rush-Yds-TDs",
        "rushing_yards":"Rush-Yds-TDs","rushing_tds":"Rush-Yds-TDs","sacks_suffered":"Sacked-Yards",
        "sack_yards_lost":"Sacked-Yards","fumbles":"Fumbles-Lost","fumbles_lost":"Fumbles-Lost",
        "penalties":"Penalties-Yards","penalty_yards":"Penalties-Yards","total_yds_allowed":"opp:Total Yards",
        "def_interceptions":"opp:Cmp-Att-Yd-TD-INT","def_sacks":"opp:Sacked-Yards","def_sack_yards":"opp:Sacked-Yards",
        "passing_yds_allowed":"opp:Net Pass Yards","def_completions_allowed":"opp:Cmp-Att-Yd-TD-INT",
        "passing_tds_allowed":"opp:Cmp-Att-Yd-TD-INT","rushing_yds_allowed":"opp:Rush-Yds-TDs",
        "rushing_tds_allowed":"opp:Rush-Yds-TDs"}),
    "scoring": (1920, 2025, {
        "passing_tds":"desc","rushing_tds":"desc","receiving_tds":"desc","kickoff_return_tds":"desc",
        "punt_return_tds":"desc","def_int_ret_td":"desc","fum_ret_td":"desc","def_tds":"desc",
        "special_teams_tds":"desc","def_safeties":"desc","sfty":"desc","pick6":"desc","fg_made":"desc",
        "passing_2pt_conversions":"desc","rushing_2pt_conversions":"desc","receiving_2pt_conversions":"desc",
        "fg_long":"desc","fg_made_0_19":"desc","fg_made_20_29":"desc","fg_made_30_39":"desc",
        "fg_made_40_49":"desc","fg_made_50_59":"desc","passing_tds_allowed":"opp:desc","rushing_tds_allowed":"opp:desc"}),
    "pbp": (1966, 2025, {
        "kickoff_returns":"detail","punt_returns":"detail","def_sacks":"detail","passing_first_downs":"detail",
        "rushing_first_downs":"detail","receiving_first_downs":"detail","passing_2pt_conversions":"detail",
        "rushing_2pt_conversions":"detail","receiving_2pt_conversions":"detail","rushing_fumbles":"detail",
        "receiving_fumbles":"detail","sack_fumbles":"detail","fg_blocked":"detail","fg_long":"detail",
        "fg_missed":"detail","pat_missed":"detail","pat_blocked":"detail","punts_blocked":"detail",
        "def_blk_kick":"detail","fum_rec":"detail","penalties":"detail","timeouts":"detail",
        "fg_made_0_19":"detail","fg_made_20_29":"detail","fg_made_30_39":"detail","fg_made_40_49":"detail",
        "fg_made_50_59":"detail","fg_made_60_":"detail","fum_ret_td":"detail","def_int_ret_td":"detail"}),
    "passing_advanced": (2018, 2025, {
        "passing_air_yards":"pass_air_yds","passing_yards_after_catch":"pass_yac","passing_drops":"pass_drops",
        "passing_poor_throws":"pass_poor_throws","passing_blitzed":"pass_blitzed","passing_hurried":"pass_hurried",
        "passing_hits":"pass_hits","passing_pressured":"pass_pressured","passing_first_downs":"pass_first_down",
        "rushing_scrambles":"rush_scrambles","sacks_suffered":"pass_sacked"}),
    "receiving_advanced": (2018, 2025, {
        "receiving_air_yards":"rec_air_yds","receiving_yards_after_catch":"rec_yac","receiving_adot":"rec_adot",
        "receiving_broken_tackles":"rec_broken_tackles","receiving_drops":"rec_drops",
        "receiving_first_downs":"rec_first_down","receiving_target_interceptions":"rec_target_int","targets":"targets"}),
    "rushing_advanced": (2018, 2025, {
        "rushing_yards_before_contact":"rush_yds_before_contact","rushing_yards_after_contact":"rush_yac",
        "rushing_broken_tackles":"rush_broken_tackles","rushing_first_downs":"rush_first_down","carries":"rush_att"}),
    "defense_advanced": (2018, 2025, {
        "def_pressures":"pressures","def_blitzes":"blitzes","def_hurries":"qb_hurry","def_knockdowns":"qb_knockdown",
        "def_tackles_missed":"tackles_missed","def_qb_hits":"qb_knockdown","def_completions_allowed":"def_cmp",
        "def_completion_yards_allowed":"def_cmp_yds","def_air_yards_allowed":"def_air_yds",
        "def_yards_after_catch_allowed":"def_yac","def_targets_allowed":"def_targets",
        "passing_hits":"opp:qb_knockdown","passing_pressured":"opp:pressures","passing_blitzed":"opp:blitzes",
        "passing_hurried":"opp:qb_hurry"}),
    "snap_counts": (2012, 2025, {
        "offense_snaps":"offense","defense_snaps":"defense","special_teams_snaps":"special_teams"}),
    "drives": (1998, 2025, {"three_out":"end_event","fourth_down_stop":"end_event"}),
    "nfl_team_games_all": (1920, 2025, {"points_allowed":"opponent_points"}),
}

# atoms that are DETERMINISTIC decompositions of other atoms -> recompute, not independently sourced
DETERMINISTIC = {
    "bonus_pass_300yd","bonus_pass_400yd","bonus_pass_25cmp","bonus_rush_100yd","bonus_rush_200yd",
    "bonus_rush_20att","bonus_rec_100yd","bonus_rec_200yd","bonus_rec_10rec","bonus_rush_rec_100yd",
    "bonus_rush_rec_200yd","completions_40plus","completions_50plus","passing_tds_40plus","passing_tds_50plus",
    "rushing_40plus","rushing_tds_40plus","rushing_tds_50plus","receptions_40plus","receiving_tds_40plus",
    "receiving_tds_50plus","receptions_0_4","receptions_5_9","receptions_10_19","receptions_20_29","receptions_30_39",
    "yds_allow_0_99","yds_allow_100_199","yds_allow_200_299","yds_allow_300_349","yds_allow_350_399",
    "yds_allow_400_449","yds_allow_450_499","yds_allow_500_549","yds_allow_550_plus","fg_missed","pat_missed",
    "fg_yds_over_30","fg_yards","fg_yards_canonical","fg_yards_over_30_canonical","fg_made_60_plus_canonical",
    "fg_made_distance","fg_missed_distance","2pm","fantasy_points_ppr","pts","fg_missed_0_19","fg_missed_20_29",
    "fg_missed_30_39","fg_missed_40_49","fg_missed_50_59","fg_missed_60_","misc_yards","passing_yards_per_attempt",
}


def build():
    inv = collections.defaultdict(list)   # v26_col -> [(source, era_lo, era_hi, how)]
    for src, (lo, hi, prov) in SRC.items():
        for col, how in prov.items():
            inv[col].append((src, lo, hi, how))
    return inv


def run(gaps=False):
    names = set(pq.read_schema(latest_v26()).names)
    inv = build()
    rows = []
    for col in sorted(inv):
        if col not in names:
            continue
        srcs = inv[col]
        n = len(srcs)
        rows.append((col, n, ",".join(s[0] for s in srcs)))
    # also list deterministic + sourceless
    det = sorted(d for d in DETERMINISTIC if d in names)
    if gaps:
        rows = [r for r in rows if r[1] < 2]
    return rows, det


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--gaps", action="store_true")
    a = ap.parse_args()
    rows, det = run(gaps=a.gaps)
    print("%-30s %5s  %s" % ("atom", "#src", "source tables"))
    print("-" * 110)
    for c, n, s in rows:
        print("%-30s %5d  %s" % (c, n, s))
    cnt = collections.Counter(r[1] for r in rows)
    print(f"\n{len(rows)} independently-sourced atoms | depth: " +
          ", ".join(f"{k}-src:{v}" for k, v in sorted(cnt.items(), reverse=True)))
    print(f"{len(det)} deterministic-decomposition atoms (recompute from atoms, not independently sourced)")
