"""Verify ESPN_STAT_ID_MAP matches espn_api library for stat_ids the library covers.

The library is the authoritative source for stat_id -> ESPN scoring-rule semantics.
For stat_ids in the library, our canonical key MUST match the library's label
(translated via LIBRARY_LABEL_TO_CANONICAL).

Stat_ids in the library that we intentionally do not map (e.g., FG Attempted
counters, every-N composite scoring, punter scoring, and per-game display
composites) are listed in INTENTIONALLY_UNMAPPED_LIBRARY_STAT_IDS - explicit
allowlist, not silent skip.

Source of truth: docs/superpowers/findings/2026-05-02-espn-stat-id-map-audit.md

Run with:
    cd fantasy_football_data_scripts && python -m pytest tests/unit/core/test_espn_stat_id_map.py -v
"""

import pytest
from espn_api.football.constant import SETTINGS_SCORING_FORMAT_MAP

from multi_league.core.scoring_config import ESPN_STAT_ID_MAP


# Map espn_api library labels -> our canonical scoring keys.
# Add a new entry here whenever a library stat_id we want to map gets a label
# we haven't translated yet. Source of truth for canonical keys: the DDL columns
# in ___leagues.public.league_settings (prefix scoring_) and ALL_SCORING_KEYS
# in multi_league/core/canonical_settings.py.
LIBRARY_LABEL_TO_CANONICAL: dict[str, str] = {
    # Passing core
    "Each Pass Attempted": "pass_att",
    "Each Pass Completed": "pass_cmp",
    "Passing Yards": "pass_yd",
    "TD Pass": "pass_td",
    "2pt Passing Conversion": "pass_2pt",
    "Interceptions Thrown": "pass_int",
    # Passing bonuses
    "40+ yard TD pass bonus": "pass_td_40p",
    "50+ yard TD pass bonus": "pass_td_50p",
    "300-399 yard passing game": "bonus_pass_yd_300",
    "400+ yard passing game": "bonus_pass_yd_400",
    "Passing First Down": "pass_fd",
    # Rushing core
    "Rushing Attempts": "rush_att",
    "Rushing Yards": "rush_yd",
    "TD Rush": "rush_td",
    "2pt Rushing Conversion": "rush_2pt",
    # Rushing bonuses
    "40+ yard TD rush bonus": "rush_td_40p",  # Fixed 2026-05-02: TD-only bonus, not any-yardage
    "50+ yard TD rush bonus": "rush_td_50p",
    "100-199 yard rushing game": "bonus_rush_yd_100",
    "200+ yard rushing game": "bonus_rush_yd_200",
    "Rushing First Down": "rush_fd",
    # Receiving core
    "Receptions": "rec",
    "Each reception": "rec",
    "Receiving Yards": "rec_yd",
    "TD Reception": "rec_td",
    "2pt Receiving Conversion": "rec_2pt",
    # Receiving bonuses
    "40+ yard TD rec bonus": "rec_td_40p",
    "50+ yard TD rec bonus": "rec_td_50p",
    "100-199 yard receiving game": "bonus_rec_yd_100",
    "200+ yard receiving game": "bonus_rec_yd_200",
    "Receiving First Down": "rec_fd",
    # Misc offense
    "Fumble Recovered for TD": "fum_rec_td",
    "Sacked": "pass_sack",
    "Total Fumbles": "fum",
    "Total Fumbles Lost": "fum_lost",
    # Kicker
    "FG Made (50+ yards)": "fgm_50p",
    "FG Missed (50+ yards)": "fgmiss_50p",
    "FG Made (40-49 yards)": "fgm_40_49",
    "FG Missed (40-49 yards)": "fgmiss_40_49",
    "FG Made (0-39 yards)": "fgm_0_39",
    "FG Missed (0-39 yards)": "fgmiss_0_39",
    "FG Made (50-59 yards)": "fgm_50_59",
    "FG Missed (50-59 yards)": "fgmiss_50_59",
    "FG Made (60+ yards)": "fgm_60p",
    "FG Missed (60+ yards)": "fgmiss_60p",
    "FG Made Yards": "fgm_yds",
    "Total FG Made": "fgm",
    "Total FG Missed": "fgmiss",
    "Each PAT Made": "xpm",
    "Each PAT Missed": "xpmiss",
    "2pt Return": "def_2pt",
    "1pt Safety": "one_pt_safe",
    # DST
    "0 points allowed": "pts_allow_0",
    "1-6 points allowed": "pts_allow_1_6",
    "7-13 points allowed": "pts_allow_7_13",
    "14-17 points allowed": "pts_allow_14_20",
    "18-21 points allowed": "pts_allow_14_20_alt",
    "22-27 points allowed": "pts_allow_21_27",
    "28-34 points allowed": "pts_allow_28_34",
    "35-45 points allowed": "pts_allow_35p",
    "46+ points allowed": "pts_allow_46p",
    "Points Allowed": "pts_allow",
    "Blocked Punt or FG return for TD": "def_blk_kick_td",
    "Fumble or INT Return for TD": "def_td",
    "Each Interception": "int",
    "Each Fumble Recovered": "fum_rec",
    "Blocked Punt, PAT or FG": "blk_kick",
    "Each Safety": "safe",
    "Each Sack": "sack",
    "Kickoff Return TD": "def_kr_td",
    "Punt Return TD": "def_pr_td",
    "Interception Return TD": "def_int_ret_td",
    "Fumble Return TD": "def_fum_ret_td",
    "Total Return TD": "def_st_td",
    "Each Fumble Forced": "ff",
    "Kickoff Return Yards": "def_kr_yd",
    "Punt Return Yards": "def_pr_yd",
    # DST team result / score / margin
    "Team Win": "team_win",
    "Team Loss": "team_loss",
    "Team Tie": "team_tie",
    "Points Scored": "team_pts",
    "Margin of Victory": "team_margin",
    "25+ point Win Margin": "team_win_margin_25p",
    "20-24 point Win Margin": "team_win_margin_20_24",
    "15-19 point Win Margin": "team_win_margin_15_19",
    "10-14 point Win Margin": "team_win_margin_10_14",
    "5-9 point Win Margin": "team_win_margin_5_9",
    "1-4 point Win Margin": "team_win_margin_1_4",
    "1-4 point Loss Margin": "team_loss_margin_1_4",
    "5-9 point Loss Margin": "team_loss_margin_5_9",
    "10-14 point Loss Margin": "team_loss_margin_10_14",
    "15-19 point Loss Margin": "team_loss_margin_15_19",
    "20-24 point Loss Margin": "team_loss_margin_20_24",
    "25+ point Loss Margin": "team_loss_margin_25p",
    # DST yards allowed
    "Less than 100 total yards allowed": "yds_allow_0_100",
    "100-199 total yards allowed": "yds_allow_100_199",
    "200-299 total yards allowed": "yds_allow_200_299",
    "300-349 total yards allowed": "yds_allow_300_349",
    "350-399 total yards allowed": "yds_allow_350_399",
    "400-449 total yards allowed": "yds_allow_400_449",
    "450-499 total yards allowed": "yds_allow_450_499",
    "500-549 total yards allowed": "yds_allow_500_549",
    "550+ total yards allowed": "yds_allow_550p",
    # IDP
    "Total Tackles": "idp_tkl",
    "Stuffs": "idp_tkl_loss",
    "Passes Defensed": "idp_pass_def",
}


# Stat_ids in the library that we explicitly do NOT map.
# Reasons documented inline. If a future ESPN league surfaces non-zero values
# for any of these, revisit and decide whether to add a canonical key + DDL
# column or keep skipping.
INTENTIONALLY_UNMAPPED_LIBRARY_STAT_IDS: set[int] = {
    # ---- Composites covered by base counters ----
    # Every-N passing yards (covered by stat_id 3 = pass_yd)
    5,
    6,
    7,
    8,
    9,
    10,
    # Each Incomplete Pass / Every-N pass completions / Every-N pass incompletions
    2,
    11,
    12,
    13,
    14,
    # Every-N rushing yards / rush attempts (covered by stat_ids 24 / 23)
    27,
    28,
    29,
    30,
    31,
    32,
    33,
    34,
    # Every-N receiving yards / receptions (covered by stat_ids 42 / 41)
    47,
    48,
    49,
    50,
    51,
    52,
    54,
    55,
    # ---- Per-game / percentage stats not in our scoring DDL ----
    21,  # Passing Completion Pct
    22,  # Passing Yards Per Game
    39,  # Rushing Yards Per Attempt
    40,  # Rushing Yards Per Game
    58,  # Receiving Target (display-only / not scored)
    59,  # Receiving Yards After Catch
    60,  # Receiving Yards Per Catch
    61,  # Receiving Yards Per Game
    137,  # Yards Allowed Per Game
    173,  # Margin of Victory Per Game
    174,  # Winning Pct
    197,  # D/ST Points Allowed Per Game
    # ---- Position-split fumble stats (we use unified fum / fum_lost) ----
    62,  # Total 2pt Conversions (covered by individual 2pt stat_ids 19/26/44)
    65,
    66,
    67,  # Passing/Rushing/Receiving Fumbles (we use total = stat_id 68)
    69,
    70,
    71,  # Passing/Rushing/Receiving Fumbles Lost (we use total = stat_id 72)
    # ---- FG Attempted (not scored — only Made/Missed score) ----
    73,
    75,
    78,
    81,
    84,
    87,
    199,
    202,
    204,
    205,
    # ---- FG yardage every-N composites (covered by stat_id 214 = fgm_yds) ----
    207,
    208,
    210,
    215,
    216,  # FG Missed Yards / FG Attempt Yards (per-yard composites)
    217,
    218,
    219,
    220,
    221,
    222,  # Every-N FG Made yards
    223,
    224,
    225,
    226,
    227,
    228,  # Every-N FG Missed yards
    229,
    230,
    231,
    232,
    233,
    234,  # Every-N FG Attempt yards
    # ---- Sub-bracket TD bonuses (we use 40+ / 50+ brackets only) ----
    175,
    176,
    177,
    178,  # 0-9/10-19/20-29/30-39 yd TD pass bonus
    179,
    180,
    181,
    182,  # 0-9/10-19/20-29/30-39 yd TD rush bonus
    183,
    184,
    185,
    186,  # 0-9/10-19/20-29/30-39 yd TD rec bonus
    # ---- D/ST-prefixed points-allowed (duplicates of 89-92, 121-125) ----
    187,
    188,
    189,
    190,
    191,
    192,
    193,
    194,
    195,
    196,
    # ---- Half-sack (would double-count with stat_id 99) ----
    100,
    # ---- IDP composites & advanced (covered by base counts 109/112/113) ----
    # 108 = QB Hit, 110 = Forced Fumble, 111 = Every 5 Total Tackles
    # 114-119 = Every-N IDP composites, 121-127 = misc IDP advanced
    101,
    107,
    108,
    110,
    111,
    114,
    115,
    116,
    117,
    118,
    119,
    121,
    126,
    127,
    # ---- Punter scoring (140-154) — no DDL columns; deferred 2026-05-02 ----
    140,
    141,
    142,
    143,
    144,
    145,
    146,
    147,
    148,
    149,
    150,
    151,
    152,
    153,
    154,
    159,
    # ---- Net Punts / Punt Yards (no DDL) ----
    138,
    139,
}


@pytest.mark.parametrize("stat_id", sorted(SETTINGS_SCORING_FORMAT_MAP.keys()))
def test_library_stat_id_is_either_mapped_or_explicitly_unmapped(stat_id):
    """Every espn_api stat_id is mapped by us OR explicitly in the unmapped allowlist.

    This catches new library stat_ids that need a decision on next library upgrade.
    """
    if stat_id in INTENTIONALLY_UNMAPPED_LIBRARY_STAT_IDS:
        return
    info = SETTINGS_SCORING_FORMAT_MAP[stat_id]
    assert stat_id in ESPN_STAT_ID_MAP, (
        f"espn_api stat_id {stat_id} ('{info.get('label', '?')}') is missing from "
        f"ESPN_STAT_ID_MAP. Either map it to a canonical key or add to "
        f"INTENTIONALLY_UNMAPPED_LIBRARY_STAT_IDS with a justification comment."
    )


@pytest.mark.parametrize("stat_id,canonical_key", sorted(ESPN_STAT_ID_MAP.items()))
def test_our_map_matches_library_canonical(stat_id, canonical_key):
    """Where the library has a stat_id, our canonical key matches the library's label.

    This catches wrong canonical-key assignments. If a stat_id is outside library
    coverage (manual mapping), we skip it here — covered by Fly-data sanity checks
    in the audit doc, not unit-tested.
    """
    if stat_id not in SETTINGS_SCORING_FORMAT_MAP:
        return
    library_label = SETTINGS_SCORING_FORMAT_MAP[stat_id]["label"]
    expected = LIBRARY_LABEL_TO_CANONICAL.get(library_label)
    assert expected is not None, (
        f"Library label '{library_label}' (stat_id {stat_id}) has no entry in "
        f"LIBRARY_LABEL_TO_CANONICAL — add one to make the test authoritative."
    )
    assert canonical_key == expected, (
        f"stat_id {stat_id}: ESPN_STAT_ID_MAP says '{canonical_key}' but "
        f"library label '{library_label}' translates to canonical '{expected}'."
    )
