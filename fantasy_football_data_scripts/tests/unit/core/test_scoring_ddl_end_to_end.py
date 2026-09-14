from __future__ import annotations

import warnings
from pathlib import Path

import duckdb
import pandas as pd
import pytest
from pandas.errors import PerformanceWarning

from multi_league.core.canonical_settings import (
    ALL_SCORING_KEYS,
    extract_scoring_settings_from_flat_row,
    flatten_settings,
    get_schema_keys,
)
from multi_league.core.scoring_config import (
    ESPN_MULTI_STAT_ID_MAP,
    ESPN_STAT_ID_MAP,
    IRREDUCIBLE_SCORING_KEYS,
    YAHOO_STAT_ID_MAP,
    normalize_yahoo,
)
from multi_league.data_fetchers.fantasy_points_calculator import calculate_all_fantasy_points
from multi_league.transformations.common.sql_base import (
    BONUS_COL_MAP,
    DEF_COL_MAP,
    IDP_COL_MAP,
    KICK_COL_MAP,
    SQLEnrichmentsBase,
)
from multi_league.transformations.player.modules.scoring_calculator import (
    _BONUS_KEY_TO_COL,
    _BONUS_THRESHOLD_KEY_TO_SOURCE,
    _SLEEPER_KEY_TO_STAT,
    compute_fantasy_points_sql_from_settings_row,
)
from multi_league.transformations.player.sql_player_enrichments import PlayerEnrichmentsMixin


YEAR = 2025
INTERESTING_WEEK = 3


class _PlayerRunner(PlayerEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def _rule_value(key: str) -> float:
    negative = ("int", "fum_lost", "fgmiss", "xpmiss", "pass_sack")
    if any(part in key for part in negative):
        return -1.0
    if key == "rec":
        return 1.0
    if key.endswith("_yd") or key.endswith("_yds") or key in {"pass_yd", "rush_yd", "rec_yd", "st_yd"}:
        return 0.04
    if "td" in key or key in {"def_2pt", "pass_2pt", "rush_2pt", "rec_2pt"}:
        return 2.0
    return 1.0


def _base_raw(platform: str, scoring: dict[str, float]) -> dict:
    if platform == "yahoo":
        return {
            "metadata": {
                "num_teams": 12,
                "draft_type": "snake",
                "num_playoff_teams": 6,
                "playoff_start_week": 15,
                "start_week": 1,
                "end_week": 17,
            },
            "canonical_scoring": scoring,
            "roster_position_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "K": 1, "DEF": 1, "BN": 6},
        }
    if platform == "espn":
        return {
            "settings": {
                "size": 12,
                "scoringSettings": {
                    "scoringItems": [
                        {"statId": stat_id, "points": _rule_value(canon_key)}
                        for stat_id, canon_key in sorted(ESPN_STAT_ID_MAP.items())
                    ]
                },
                "rosterSettings": {"lineupSlotCounts": {"0": 1, "2": 2, "4": 2, "6": 1, "16": 1, "17": 1, "20": 6}},
                "scheduleSettings": {"playoffTeamCount": 6, "matchupPeriodCount": 14},
                "draftSettings": {"type": "snake"},
            }
        }
    return {
        "settings": {"num_teams": 12, "playoff_teams": 6, "playoff_week_start": 15},
        "total_rosters": 12,
        "playoff_teams": 6,
        "scoring_settings": scoring,
        "roster_positions": ["QB", "RB", "RB", "WR", "WR", "TE", "K", "DEF", "BN", "BN"],
    }


def _flat_league(platform: str, scoring: dict[str, float], league_key: str | None = None) -> dict:
    return flatten_settings(
        _base_raw(platform, scoring), platform=platform, year=YEAR, league_key=league_key or platform
    )


def _flat_week_league(platform: str, scoring: dict[str, float]) -> dict:
    if platform == "espn":
        raw = {
            "scoring_settings": scoring,
            "num_teams": 12,
            "playoff_teams": 6,
            "playoff_start_week": 15,
            "regular_season_length": 14,
            "roster_position_counts": {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "K": 1, "DEF": 1, "BN": 6},
        }
        return flatten_settings(raw, platform="espn", year=YEAR, league_key="synthetic_week_3_espn")
    return _flat_league(platform, scoring, league_key=f"synthetic_week_3_{platform}")


def _present_scoring_keys(flat: dict) -> set[str]:
    return {
        key.removeprefix("scoring_")
        for key, value in flat.items()
        if isinstance(key, str) and key.startswith("scoring_") and value is not None
    }


def _all_rule_flat_for_platform(platform: str) -> dict:
    if platform == "yahoo":
        scoring = normalize_yahoo(
            [
                {"stat_id": stat_id, "points": _rule_value(canon_key)}
                for stat_id, canon_key in sorted(YAHOO_STAT_ID_MAP.items())
            ]
        )
        scoring.update({key: _rule_value(key) for key in _BONUS_THRESHOLD_KEY_TO_SOURCE})
    elif platform == "sleeper":
        scoring = {key: _rule_value(key) for key in ALL_SCORING_KEYS}
    else:
        scoring = {}

    flat = _flat_league(platform, scoring, league_key=f"pipeline_e2e_{platform}")
    flat["db_name"] = f"pipeline_e2e_{platform}"
    return flat


def _base_week3_row(player_id: str, position: str) -> dict:
    return {
        "player_week": f"{player_id}_{YEAR}_{INTERESTING_WEEK}",
        "NFL_player_id": player_id,
        "player": f"Synthetic {position}",
        "year": YEAR,
        "season": YEAR,
        "week": INTERESTING_WEEK,
        "season_type": "REG",
        "position": position,
        "nfl_position": position,
        "passing_yards": 0.0,
        "passing_tds": 0.0,
        "passing_interceptions": 0.0,
        "attempts": 0.0,
        "completions": 0.0,
        "passing_2pt_conversions": 0.0,
        "passing_first_downs": 0.0,
        "passing_tds_40plus": 0.0,
        "passing_tds_50plus": 0.0,
        "completions_40plus": 0.0,
        "completions_50plus": 0.0,
        "pick6": 0.0,
        "sacks_suffered": 0.0,
        "rushing_yards": 0.0,
        "rushing_tds": 0.0,
        "carries": 0.0,
        "rushing_2pt_conversions": 0.0,
        "rushing_first_downs": 0.0,
        "rushing_40plus": 0.0,
        "rushing_tds_40plus": 0.0,
        "rushing_tds_50plus": 0.0,
        "receiving_yards": 0.0,
        "receiving_tds": 0.0,
        "receptions": 0.0,
        "targets": 0.0,
        "receiving_2pt_conversions": 0.0,
        "receiving_first_downs": 0.0,
        "receptions_40plus": 0.0,
        "receiving_tds_40plus": 0.0,
        "receiving_tds_50plus": 0.0,
        "receptions_0_4": 0.0,
        "receptions_5_9": 0.0,
        "receptions_10_19": 0.0,
        "receptions_20_29": 0.0,
        "receptions_30_39": 0.0,
        "rushing_fumbles": 0.0,
        "sack_fumbles": 0.0,
        "receiving_fumbles": 0.0,
        "rushing_fumbles_lost": 0.0,
        "sack_fumbles_lost": 0.0,
        "receiving_fumbles_lost": 0.0,
        "kickoff_return_yards": 0.0,
        "punt_return_yards": 0.0,
        "dst_return_yards": 0.0,
        "special_teams_tds": 0.0,
        "special_teams_tackles_solo": 0.0,
        "fumble_recovery_yards_own": 0.0,
        "fumble_recovery_yards_opp": 0.0,
        "def_interception_yards": 0.0,
        "fg_made": 0.0,
        "fgm": 0.0,
        "fg_made_0_19": 0.0,
        "fg_made_20_29": 0.0,
        "fg_made_30_39": 0.0,
        "fg_made_40_49": 0.0,
        "fg_made_50_59": 0.0,
        "fg_made_60_": 0.0,
        "fg_made_60_plus_canonical": 0.0,
        "fg_made_distance": 0.0,
        "fg_yards_canonical": 0.0,
        "fg_yds_over_30": 0.0,
        "fg_yards_over_30_canonical": 0.0,
        "fg_pct": 0.0,
        "fg_missed": 0.0,
        "fg_missed_0_19": 0.0,
        "fg_missed_20_29": 0.0,
        "fg_missed_30_39": 0.0,
        "fg_missed_40_49": 0.0,
        "fg_missed_50_59": 0.0,
        "fg_missed_60_": 0.0,
        "pat_made": 0.0,
        "pat_missed": 0.0,
        "def_sacks": 0.0,
        "def_interceptions": 0.0,
        "def_fumbles_forced": 0.0,
        "def_fumbles": 0.0,
        "fum_rec": 0.0,
        "fum_ret_td": 0.0,
        "def_tds": 0.0,
        "def_safeties": 0.0,
        "fg_blocked": 0.0,
        "def_blk_kick": 0.0,
        "def_blk_kick_td": 0.0,
        "def_xpr": 0.0,
        "def_tackles_for_loss": 0.0,
        "three_out": 0.0,
        "fourth_down_stop": 0.0,
        "def_pass_defended": 0.0,
        "def_qb_hits": 0.0,
        "def_tackles_solo": 0.0,
        "def_tackle_assists": 0.0,
        "def_tackles_with_assist": 0.0,
        "fum_rec_yds": 0.0,
        "pts_allow": 0.0,
        "pts_allow_0": 0.0,
        "pts_allow_1_6": 0.0,
        "pts_allow_7_13": 0.0,
        "pts_allow_14_20": 0.0,
        "pts_allow_21_27": 0.0,
        "pts_allow_28_34": 0.0,
        "pts_allow_35_plus": 0.0,
        "total_yds_allowed": 0.0,
    }


def _fake_week3_super_table() -> pd.DataFrame:
    rows = [
        _base_week3_row("SYN_QB", "QB"),
        _base_week3_row("SYN_RB", "RB"),
        _base_week3_row("SYN_WR", "WR"),
        _base_week3_row("SYN_TE", "TE"),
        _base_week3_row("SYN_K", "K"),
        _base_week3_row("SYN_DEF", "DEF"),
        _base_week3_row("SYN_LB", "LB"),
    ]

    for row in rows:
        if row["position"] in {"QB", "RB", "WR", "TE"}:
            row.update(
                {
                    "passing_yards": 405.0,
                    "passing_tds": 3.0,
                    "passing_interceptions": 1.0,
                    "attempts": 42.0,
                    "completions": 26.0,
                    "passing_2pt_conversions": 1.0,
                    "passing_first_downs": 12.0,
                    "passing_tds_40plus": 1.0,
                    "passing_tds_50plus": 1.0,
                    "completions_40plus": 2.0,
                    "completions_50plus": 1.0,
                    "pick6": 1.0,
                    "sacks_suffered": 3.0,
                    "rushing_yards": 205.0,
                    "rushing_tds": 2.0,
                    "carries": 21.0,
                    "rushing_2pt_conversions": 1.0,
                    "rushing_first_downs": 8.0,
                    "rushing_40plus": 2.0,
                    "rushing_tds_40plus": 1.0,
                    "rushing_tds_50plus": 1.0,
                    "receiving_yards": 205.0,
                    "receiving_tds": 2.0,
                    "receptions": 10.0,
                    "targets": 12.0,
                    "receiving_2pt_conversions": 1.0,
                    "receiving_first_downs": 9.0,
                    "receptions_40plus": 2.0,
                    "receiving_tds_40plus": 1.0,
                    "receiving_tds_50plus": 1.0,
                    "receptions_0_4": 2.0,
                    "receptions_5_9": 2.0,
                    "receptions_10_19": 2.0,
                    "receptions_20_29": 2.0,
                    "receptions_30_39": 2.0,
                    "rushing_fumbles": 1.0,
                    "sack_fumbles": 1.0,
                    "receiving_fumbles": 1.0,
                    "rushing_fumbles_lost": 1.0,
                    "sack_fumbles_lost": 1.0,
                    "receiving_fumbles_lost": 1.0,
                    "kickoff_return_yards": 25.0,
                    "punt_return_yards": 50.0,
                    "special_teams_tds": 1.0,
                    "special_teams_tackles_solo": 2.0,
                    "fum_ret_td": 1.0,
                    "fumble_recovery_yards_own": 7.0,
                    "fumble_recovery_yards_opp": 9.0,
                    "def_interception_yards": 11.0,
                }
            )
        elif row["position"] == "K":
            row.update(
                {
                    "fg_made": 6.0,
                    "fgm": 6.0,
                    "fg_made_0_19": 1.0,
                    "fg_made_20_29": 1.0,
                    "fg_made_30_39": 1.0,
                    "fg_made_40_49": 1.0,
                    "fg_made_50_59": 1.0,
                    "fg_made_60_": 1.0,
                    "fg_made_60_plus_canonical": 1.0,
                    "fg_made_distance": 260.0,
                    "fg_yards_canonical": 260.0,
                    "fg_yds_over_30": 80.0,
                    "fg_yards_over_30_canonical": 80.0,
                    "fg_pct": 0.85,
                    "fg_missed": 1.0,
                    "fg_missed_0_19": 1.0,
                    "fg_missed_20_29": 1.0,
                    "fg_missed_30_39": 1.0,
                    "fg_missed_40_49": 1.0,
                    "fg_missed_50_59": 1.0,
                    "fg_missed_60_": 1.0,
                    "pat_made": 3.0,
                    "pat_missed": 1.0,
                }
            )
        elif row["position"] in {"DEF", "LB"}:
            row.update(
                {
                    "def_sacks": 3.0,
                    "def_interceptions": 2.0,
                    "def_fumbles_forced": 2.0,
                    "def_fumbles": 2.0,
                    "fum_rec": 2.0,
                    "fum_ret_td": 1.0,
                    "def_tds": 1.0,
                    "def_safeties": 1.0,
                    "fg_blocked": 1.0,
                    "def_blk_kick": 1.0,
                    "def_tackles_for_loss": 4.0,
                    "three_out": 3.0,
                    "fourth_down_stop": 2.0,
                    "def_pass_defended": 5.0,
                    "def_qb_hits": 4.0,
                    "def_tackles_solo": 9.0,
                    "def_tackle_assists": 4.0,
                    "def_tackles_with_assist": 13.0,
                    "def_interception_yards": 25.0,
                    "fum_rec_yds": 15.0,
                    "pts_allow": 10.0,
                    "pts_allow_0": 1.0,
                    "pts_allow_1_6": 1.0,
                    "pts_allow_7_13": 1.0,
                    "pts_allow_14_20": 1.0,
                    "pts_allow_21_27": 1.0,
                    "pts_allow_28_34": 1.0,
                    "pts_allow_35_plus": 1.0,
                    "total_yds_allowed": 555.0,
                }
            )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PerformanceWarning)
        stats = calculate_all_fantasy_points(pd.DataFrame(rows))
    required_cols = (
        set(DEF_COL_MAP.values())
        | set(IDP_COL_MAP.values())
        | set(BONUS_COL_MAP.values())
        | set(KICK_COL_MAP.values())
        | {
            "fpts_4pt_half",
            "pts_def_fum_rec_td",
            "pts_def_int_ret_td",
            "pts_def_fum_ret_td",
            "pts_def_blk_kick_td",
            "pts_def_kr_td",
            "pts_def_pr_td",
            "pts_def_st_td",
            "pts_def_2pt",
            "pts_def_st_ff",
            "pts_def_st_fum_rec",
            "pts_def_forced_punts",
            "pts_idp_blk_kick_td",
            "pts_idp_fum_ret_td",
            "pts_idp_xpr",
            "pts_idp_pass_def_3p",
            "yds_allow_0_99",
            "yds_allow_100_199",
            "yds_allow_200_299",
            "yds_allow_300_349",
            "yds_allow_350_399",
            "yds_allow_400_449",
            "yds_allow_450_499",
            "yds_allow_500_549",
            "yds_allow_550_plus",
        }
    )
    missing_cols = sorted(required_cols - set(stats.columns))
    if missing_cols:
        stats = pd.concat([stats, pd.DataFrame(0.0, index=stats.index, columns=missing_cols)], axis=1)

    def_mask = stats["position"] == "DEF"
    lb_mask = stats["position"] == "LB"
    k_mask = stats["position"] == "K"

    for col in set(DEF_COL_MAP.values()):
        stats.loc[def_mask, col] = stats.loc[def_mask, col].fillna(0.0)
        if stats.loc[def_mask, col].eq(0).all():
            stats.loc[def_mask, col] = 1.0
    for col in set(IDP_COL_MAP.values()):
        stats.loc[lb_mask, col] = stats.loc[lb_mask, col].fillna(0.0)
        if stats.loc[lb_mask, col].eq(0).all():
            stats.loc[lb_mask, col] = 1.0
    for col in {
        "pts_def_bonus_sack_2p",
        "pts_def_bonus_tkl_10p",
        "pts_def_bonus_int_td_50p",
        "pts_def_bonus_fum_td_50p",
    }:
        stats.loc[lb_mask, col] = 1.0
    for col in set(KICK_COL_MAP.values()):
        stats.loc[k_mask, col] = stats.loc[k_mask, col].fillna(0.0)
        if stats.loc[k_mask, col].eq(0).all():
            stats.loc[k_mask, col] = 1.0

    return stats.fillna(0.0)


def _score_direct_offense(flat: dict, row: dict) -> float:
    sql = compute_fantasy_points_sql_from_settings_row(flat)
    con = duckdb.connect()
    try:
        con.register("week_row", pd.DataFrame([row]))
        return float(con.execute(f"SELECT {sql} AS fantasy_points FROM week_row").fetchone()[0])
    finally:
        con.close()


def _num(row: dict, col: str) -> float:
    value = row.get(col, 0.0)
    try:
        if pd.isna(value):
            return 0.0
    except TypeError:
        pass
    return float(value or 0.0)


def _expected_def(row: dict, def_mults: dict[str, float]) -> float:
    if not def_mults:
        return _num(row, "pts_def_std")
    total = 0.0
    for col, mult in def_mults.items():
        if col == "pts_def_td":
            total += _num(row, "def_tds") * float(mult)
        else:
            total += _num(row, col) * float(mult)
    return total


def _expected_kicker(row: dict, kick_mults: dict[str, float], kick_col: str) -> float:
    if not kick_mults:
        return _num(row, kick_col)
    return sum(_num(row, col) * float(mult) for col, mult in kick_mults.items())


def _expected_idp(
    row: dict,
    idp_mults: dict[str, float],
    idp_variant: str,
    bonus_mults: dict[str, float],
) -> float:
    if idp_mults:
        base = 0.0
        for col, mult in idp_mults.items():
            if col.startswith("_") or float(mult) == 0.0:
                continue
            if col == "pts_idp_fr":
                base += _num(row, "fum_rec") * float(mult)
            elif col == "pts_idp_td":
                base += (_num(row, "def_tds") + _num(row, "fum_ret_td")) * float(mult)
            elif col == "pts_idp_fum_ret_td":
                base += _num(row, "fum_ret_td") * float(mult)
            elif col == "pts_idp_pass_def_3p":
                base += min(_num(row, "pts_idp_pd") * float(mult), 3.0)
            else:
                base += _num(row, col) * float(mult)
    else:
        base = _num(row, f"pts_idp_{idp_variant}")
    bonus = sum(_num(row, col) * float(mult) for col, mult in bonus_mults.items() if float(mult) != 0.0)
    return base + bonus


@pytest.mark.parametrize(
    ("platform", "scoring", "expected_keys"),
    [
        (
            "yahoo",
            normalize_yahoo(
                [
                    {"stat_id": stat_id, "points": _rule_value(canon_key)}
                    for stat_id, canon_key in sorted(YAHOO_STAT_ID_MAP.items())
                ]
            ),
            set(YAHOO_STAT_ID_MAP.values()),
        ),
        ("sleeper", {key: _rule_value(key) for key in ALL_SCORING_KEYS}, set(ALL_SCORING_KEYS)),
        (
            "espn",
            {},
            set(ESPN_STAT_ID_MAP.values()) | {key for keys in ESPN_MULTI_STAT_ID_MAP.values() for key in keys},
        ),
    ],
)
def test_minimum_fake_platform_leagues_round_trip_all_rules_to_ddl(platform, scoring, expected_keys):
    flat = _flat_league(platform, scoring)

    assert sorted(expected_keys - _present_scoring_keys(flat)) == []

    ddl_readback = extract_scoring_settings_from_flat_row(flat)
    assert sorted(expected_keys - set(ddl_readback)) == []


def test_all_sleeper_rules_survive_ddl_loader_into_pipeline_buckets():
    flat = _flat_league("sleeper", {key: _rule_value(key) for key in ALL_SCORING_KEYS}, league_key="all_rules")
    flat["db_name"] = "all_rules"
    frame = pd.DataFrame([{key: flat.get(key) for key in get_schema_keys()}])

    class _FakeResult:
        def fetchdf(self):
            return frame

    class _FakeConn:
        def execute(self, _sql):
            return _FakeResult()

    base = SQLEnrichmentsBase("all_rules", dry_run=True)
    base._table_exists = lambda _table_name: True
    base._get_connection = lambda: _FakeConn()

    roster_by_year, scoring_params = base.load_settings_from_db()
    year_settings = roster_by_year[YEAR]

    assert sorted(set(ALL_SCORING_KEYS) - set(year_settings["scoring_settings"])) == []
    assert set(DEF_COL_MAP.values()) <= set(year_settings["def_multipliers"])
    assert set(IDP_COL_MAP.values()) <= set(year_settings["idp_multipliers"])
    assert set(BONUS_COL_MAP.values()) <= set(year_settings["bonus_multipliers"])
    assert set(KICK_COL_MAP.values()) <= set(year_settings["kick_multipliers"])
    assert year_settings["kick_multipliers"]["fg_made"] == pytest.approx(_rule_value("fgm"))
    assert "fgm" not in year_settings["kick_multipliers"]
    assert "fg_missed_50_59" in year_settings["kick_multipliers"]
    assert "fg_missed_60_" in year_settings["kick_multipliers"]
    assert "pts_def_2pt" in year_settings["def_multipliers"]
    assert "pts_def_safety" in year_settings["def_multipliers"]
    assert IRREDUCIBLE_SCORING_KEYS == {}
    assert scoring_params["kick_col"] == "pts_k_yds"


def test_concrete_pipeline_consumers_account_for_every_ddl_scoring_key():
    concrete = set(_SLEEPER_KEY_TO_STAT)
    concrete.update(_BONUS_KEY_TO_COL)
    concrete.update(_BONUS_THRESHOLD_KEY_TO_SOURCE)
    for col_map in (DEF_COL_MAP, IDP_COL_MAP, BONUS_COL_MAP, KICK_COL_MAP):
        concrete.update(key.removeprefix("scoring_") for key in col_map)
    concrete.update(IRREDUCIBLE_SCORING_KEYS)

    assert sorted(set(ALL_SCORING_KEYS) - concrete) == []


@pytest.mark.parametrize("platform", ["yahoo", "sleeper", "espn"])
def test_fake_interesting_week_scores_from_flat_ddl_sql(platform):
    scoring = {
        "pass_att": 0.1,
        "pass_inc": -0.25,
        "pass_yd": 0.04,
        "pass_td": 4.0,
        "pass_int": -2.0,
        "rush_yd": 0.1,
        "rush_td": 6.0,
        "rec_yd": 0.1,
        "rec_td": 6.0,
        "rec": 1.0,
        "rec_targets": 0.25,
        "bonus_rec_te": 0.5,
        "bonus_rec_rb": 0.25,
        "bonus_rec_wr": 0.25,
        "pass_2pt": 2.0,
        "rush_2pt": 2.0,
        "rec_2pt": 2.0,
        "fum": -1.0,
        "fum_lost": -2.0,
        "pass_cmp": 0.25,
        "rush_att": 0.1,
        "pass_fd": 0.5,
        "rush_fd": 0.5,
        "rec_fd": 0.5,
        "pass_int_td": -2.0,
        "pass_sack": -1.0,
        "st_yd": 0.04,
        "st_td": 6.0,
        "fum_rec_td": 6.0,
        "fum_ret_yd": 0.04,
        "int_ret_yd": 0.04,
        "pass_td_40p": 2.0,
        "pass_td_50p": 1.0,
        "rush_td_40p": 2.0,
        "rush_td_50p": 1.0,
        "rec_td_40p": 1.0,
        "rec_td_50p": 1.0,
        "pass_cmp_40p": 1.0,
        "rush_40p": 1.0,
        "rec_40p": 1.0,
        "rec_40p_alt": 1.0,
        "pass_cmp_50p": 1.0,
        "rec_0_4": 1.0,
        "rec_5_9": 1.0,
        "rec_10_19": 1.0,
        "rec_20_29": 1.0,
        "rec_30_39": 1.0,
        "st_tkl_solo": 1.0,
        "bonus_pass_yd_300": 3.0,
        "bonus_pass_yd_400": 3.0,
        "bonus_rush_yd_100": 3.0,
        "bonus_rush_yd_200": 3.0,
        "bonus_rec_yd_100": 3.0,
        "bonus_rec_yd_200": 3.0,
        "bonus_rush_rec_yd_100": 3.0,
        "bonus_rush_rec_yd_200": 3.0,
        "bonus_pass_cmp_25": 3.0,
        "bonus_rush_att_20": 3.0,
    }
    flat = _flat_week_league(platform, scoring)
    sql = compute_fantasy_points_sql_from_settings_row(flat)

    for fragment in (
        "attempts",
        "targets",
        "rushing_fumbles",
        "def_interception_yards",
        "fumble_recovery_yards_opp",
        "pts_ret_yds",
        "pts_rec_te_bonus_p5",
        "pts_rec_40plus_1",
        "completions_50plus",
        "receptions_0_4",
        "special_teams_tackles_solo",
    ):
        assert fragment in sql

    row = {
        "position": "TE",
        "attempts": 42,
        "completions": 25,
        "passing_yards": 298,
        "passing_tds": 4,
        "passing_interceptions": 1,
        "rushing_yards": 102,
        "rushing_tds": 3,
        "receiving_yards": 145,
        "receiving_tds": 3,
        "receptions": 8,
        "targets": 10,
        "passing_2pt_conversions": 1,
        "rushing_2pt_conversions": 1,
        "receiving_2pt_conversions": 1,
        "rushing_fumbles": 1,
        "sack_fumbles": 1,
        "receiving_fumbles": 1,
        "rushing_fumbles_lost": 1,
        "sack_fumbles_lost": 1,
        "receiving_fumbles_lost": 1,
        "passing_first_downs": 12,
        "rushing_first_downs": 7,
        "receiving_first_downs": 8,
        "pick6": 1,
        "sacks_suffered": 3,
        "kickoff_return_yards": 23,
        "punt_return_yards": 10,
        "special_teams_tds": 1,
        "fum_ret_td": 1,
        "fumble_recovery_yards_own": 5,
        "fumble_recovery_yards_opp": 12,
        "def_interception_yards": 40,
        "completions_50plus": 1,
        "receptions_0_4": 2,
        "receptions_5_9": 3,
        "receptions_10_19": 2,
        "receptions_20_29": 1,
        "receptions_30_39": 1,
        "special_teams_tackles_solo": 2,
        "pts_pass_yd_p04": 298 * 0.04,
        "pts_pass_td_4": 4 * 4,
        "pts_pass_int_n2": -2,
        "pts_rush_yd_p1": 102 * 0.1,
        "pts_rush_td_6": 3 * 6,
        "pts_rec_yd_p1": 145 * 0.1,
        "pts_rec_td_6": 3 * 6,
        "pts_rec_1": 8,
        "pts_rec_te_bonus_p5": 4,
        "pts_pass_2pt_2": 2,
        "pts_rush_2pt_2": 2,
        "pts_rec_2pt_2": 2,
        "pts_fum_lost_n2": -6,
        "pts_pass_cmp_p25": 25 * 0.25,
        "pts_rush_att_p1": 17 * 0.1,
        "pts_pass_fd_p5": 6,
        "pts_rush_fd_p5": 3.5,
        "pts_rec_fd_p5": 4,
        "pts_pick6_n2": -2,
        "pts_sack_taken_n1": -3,
        "pts_ret_yds": (23 + 10) * 0.04,
        "pts_st_td_6": 6,
        "pts_fum_ret_td_6": 6,
        "pts_pass_td_40plus_2": 2,
        "pts_pass_td_50plus_1": 1,
        "pts_rush_td_40plus_2": 2,
        "pts_rush_td_50plus_1": 1,
        "pts_rec_td_40plus_1": 1,
        "pts_rec_td_50plus_1": 1,
        "pts_pass_cmp_40plus_1": 1,
        "pts_rush_40plus_1": 1,
        "pts_rec_40plus_1": 1,
        "bonus_pass_300yd": 1,
        "bonus_pass_400yd": 1,
        "bonus_rush_100yd": 1,
        "bonus_rush_200yd": 1,
        "bonus_rec_100yd": 1,
        "bonus_rec_200yd": 1,
        "bonus_rush_rec_100yd": 1,
        "bonus_rush_rec_200yd": 1,
        "bonus_pass_25cmp": 1,
        "bonus_rush_20att": 1,
    }

    con = duckdb.connect()
    con.register("week_row", pd.DataFrame([row]))
    scored = con.execute(f"SELECT {sql} AS fantasy_points FROM week_row").fetchone()[0]

    assert scored == pytest.approx(183.12)


@pytest.mark.parametrize("platform", ["yahoo", "sleeper", "espn"])
def test_arbitrary_threshold_bonuses_score_from_atomic_stats(platform):
    scoring = {key: 1.0 for key in _BONUS_THRESHOLD_KEY_TO_SOURCE}
    flat = _flat_week_league(platform, scoring)
    sql = compute_fantasy_points_sql_from_settings_row(flat)

    row = {
        "position": "WR",
        "passing_yards": 555.0,
        "rushing_yards": 305.0,
        "receiving_yards": 337.0,
        "kickoff_return_yards": 125.0,
        "punt_return_yards": 100.0,
        "dst_return_yards": 305.0,
    }

    con = duckdb.connect()
    try:
        con.register("week_row", pd.DataFrame([row]))
        scored = con.execute(f"SELECT {sql} AS fantasy_points FROM week_row").fetchone()[0]
    finally:
        con.close()

    assert scored == pytest.approx(float(len(_BONUS_THRESHOLD_KEY_TO_SOURCE)))


@pytest.mark.parametrize("platform", ["yahoo", "sleeper", "espn"])
def test_fake_platform_leagues_score_2025_week3_through_real_pipeline(platform):
    flat = _all_rule_flat_for_platform(platform)
    db_name = flat["db_name"]
    settings_df = pd.DataFrame([{key: flat.get(key) for key in get_schema_keys()}])
    stats = _fake_week3_super_table()
    player_rows = stats[["player_week", "NFL_player_id", "year", "week", "position"]].copy()
    player_rows.insert(0, "db_name", db_name)
    player_rows["manager"] = "Unrostered"
    player_rows["fantasy_points"] = None
    player_rows["bonus_points"] = None
    player_rows["te_premium_points"] = None
    bio = stats[["NFL_player_id", "nfl_position"]].copy()

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.register("_settings", settings_df)
    conn.register("_players", player_rows)
    conn.register("_stats", stats)
    conn.register("_bio", bio)
    conn.execute("CREATE TABLE public.league_settings AS SELECT * FROM _settings")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        SELECT
            db_name,
            player_week,
            NFL_player_id,
            year,
            week,
            position,
            manager,
            CAST(fantasy_points AS DOUBLE),
            CAST(bonus_points AS DOUBLE),
            CAST(te_premium_points AS DOUBLE)
        FROM _players
        """
    )
    conn.execute("CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all AS SELECT * FROM _stats")
    conn.execute("CREATE TABLE ___ops.nfl_historical.player_bio AS SELECT * FROM _bio")

    runner = _PlayerRunner(db_name=db_name, data_dir="local")
    runner._conn = conn
    runner._platform = platform

    try:
        updated = runner.populate_fantasy_points()
        assert updated == len(player_rows)
        actual = conn.execute(
            """
            SELECT NFL_player_id, fantasy_points, bonus_points, te_premium_points
            FROM public.player_fantasy
            ORDER BY NFL_player_id
            """
        ).fetchdf()
        year_settings = runner.roster_by_year[YEAR]
    finally:
        conn.close()

    assert actual["fantasy_points"].notna().all()
    actual_by_id = actual.set_index("NFL_player_id").to_dict("index")
    stat_by_id = stats.set_index("NFL_player_id").to_dict("index")

    for player_id in ("SYN_QB", "SYN_RB", "SYN_WR", "SYN_TE"):
        assert actual_by_id[player_id]["fantasy_points"] == pytest.approx(
            _score_direct_offense(flat, stat_by_id[player_id]),
            abs=0.01,
        )

    assert actual_by_id["SYN_K"]["fantasy_points"] == pytest.approx(
        _expected_kicker(
            stat_by_id["SYN_K"],
            year_settings.get("kick_multipliers", {}),
            year_settings.get("kick_col", "pts_k_std"),
        ),
        abs=0.01,
    )
    assert actual_by_id["SYN_DEF"]["fantasy_points"] == pytest.approx(
        _expected_def(stat_by_id["SYN_DEF"], year_settings.get("def_multipliers", {})),
        abs=0.01,
    )
    assert actual_by_id["SYN_LB"]["fantasy_points"] == pytest.approx(
        _expected_idp(
            stat_by_id["SYN_LB"],
            year_settings.get("idp_multipliers", {}),
            year_settings.get("idp_scoring", "std"),
            year_settings.get("bonus_multipliers", {}),
        ),
        abs=0.01,
    )


def test_yahoo_rostered_def_recomputes_custom_dst_rules_from_ddl():
    scoring = {
        "sack": 1.0,
        "pts_allow_35p": -4.0,
        "def_st_td": 6.0,
    }
    flat = _flat_league("yahoo", scoring, league_key="yahoo_custom_dst")
    flat["db_name"] = "yahoo_custom_dst"
    settings_df = pd.DataFrame([{key: flat.get(key) for key in get_schema_keys()}])

    stats = _fake_week3_super_table()
    def_mask = stats["NFL_player_id"] == "SYN_DEF"
    for col in set(DEF_COL_MAP.values()) | {"pts_def_std"}:
        stats.loc[def_mask, col] = 0.0
    stats.loc[def_mask, "pts_def_std"] = -2.0
    stats.loc[def_mask, "pts_def_sack"] = 2.0
    stats.loc[def_mask, "pts_allow_35_plus"] = 1.0
    stats.loc[def_mask, "pts_def_st_td"] = 1.0

    player_rows = stats.loc[def_mask, ["player_week", "NFL_player_id", "year", "week", "position"]].copy()
    player_rows.insert(0, "db_name", flat["db_name"])
    player_rows["manager"] = "Darren"
    player_rows["fantasy_points"] = -2.0
    player_rows["bonus_points"] = None
    player_rows["te_premium_points"] = None
    bio = stats.loc[def_mask, ["NFL_player_id", "nfl_position"]].copy()

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.register("_settings", settings_df)
    conn.register("_players", player_rows)
    conn.register("_stats", stats)
    conn.register("_bio", bio)
    conn.execute("CREATE TABLE public.league_settings AS SELECT * FROM _settings")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        SELECT
            db_name,
            player_week,
            NFL_player_id,
            year,
            week,
            position,
            manager,
            CAST(fantasy_points AS DOUBLE),
            CAST(bonus_points AS DOUBLE),
            CAST(te_premium_points AS DOUBLE)
        FROM _players
        """
    )
    conn.execute("CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all AS SELECT * FROM _stats")
    conn.execute("CREATE TABLE ___ops.nfl_historical.player_bio AS SELECT * FROM _bio")

    runner = _PlayerRunner(db_name=flat["db_name"], data_dir="local")
    runner._conn = conn
    runner._platform = "yahoo"

    try:
        updated = runner.populate_fantasy_points()
        scored = conn.execute("SELECT fantasy_points FROM public.player_fantasy").fetchone()[0]
    finally:
        conn.close()

    assert updated == 1
    assert scored == pytest.approx(4.0)


def test_yahoo_rostered_idp_recomputes_fumble_sources_from_ddl():
    scoring = {
        "idp_fum_rec": 2.0,
        "idp_def_td": 8.0,
    }
    flat = _flat_league("yahoo", scoring, league_key="yahoo_custom_idp")
    flat["db_name"] = "yahoo_custom_idp"
    settings_df = pd.DataFrame([{key: flat.get(key) for key in get_schema_keys()}])

    stats = _fake_week3_super_table()
    lb_mask = stats["NFL_player_id"] == "SYN_LB"
    stats = stats.loc[lb_mask].copy()
    stats.loc[:, "def_fumbles"] = 0.0
    stats.loc[:, "fum_rec"] = 1.0
    stats.loc[:, "def_tds"] = 0.0
    stats.loc[:, "fum_ret_td"] = 1.0
    # Prove populate_fantasy_points does not depend on stale precompute cols
    # for the two Yahoo IDP sources that caused live reconciliation drift.
    stats.loc[:, "pts_idp_fr"] = 0.0
    stats.loc[:, "pts_idp_td"] = 0.0

    player_rows = stats[["player_week", "NFL_player_id", "year", "week", "position"]].copy()
    player_rows.insert(0, "db_name", flat["db_name"])
    player_rows["manager"] = "Paul"
    player_rows["fantasy_position"] = "IDP"
    player_rows["fantasy_points"] = 0.0
    player_rows["bonus_points"] = None
    player_rows["te_premium_points"] = None
    bio = stats[["NFL_player_id", "nfl_position"]].copy()

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.register("_settings", settings_df)
    conn.register("_players", player_rows)
    conn.register("_stats", stats)
    conn.register("_bio", bio)
    conn.execute("CREATE TABLE public.league_settings AS SELECT * FROM _settings")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            fantasy_position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        SELECT
            db_name,
            player_week,
            NFL_player_id,
            year,
            week,
            position,
            fantasy_position,
            manager,
            CAST(fantasy_points AS DOUBLE),
            CAST(bonus_points AS DOUBLE),
            CAST(te_premium_points AS DOUBLE)
        FROM _players
        """
    )
    conn.execute("CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all AS SELECT * FROM _stats")
    conn.execute("CREATE TABLE ___ops.nfl_historical.player_bio AS SELECT * FROM _bio")

    runner = _PlayerRunner(db_name=flat["db_name"], data_dir="local")
    runner._conn = conn
    runner._platform = "yahoo"

    try:
        updated = runner.populate_fantasy_points()
        scored = conn.execute("SELECT fantasy_points FROM public.player_fantasy").fetchone()[0]
    finally:
        conn.close()

    assert updated == 1
    assert scored == pytest.approx(10.0)


def test_bring_the_mo_2012_w6_texans_dst_scores_from_yahoo_ddl():
    scoring = {
        "sack": 1.0,
        "int": 2.0,
        "fum_rec": 2.0,
        "safe": 2.0,
        "blk_kick": 2.0,
        "def_st_td": 6.0,
        "st_td": 6.0,
        "pts_allow_0": 10.0,
        "pts_allow_1_6": 7.0,
        "pts_allow_7_13": 4.0,
        "pts_allow_14_20": 1.0,
        "pts_allow_28_34": -1.0,
        "pts_allow_35p": -4.0,
    }
    flat = _flat_league("yahoo", scoring, league_key="bring_the_mo_ufkin_ruckus")
    flat["db_name"] = "bring_the_mo_ufkin_ruckus"
    flat["year"] = 2012
    settings_df = pd.DataFrame([{key: flat.get(key) for key in get_schema_keys()}])

    stats = _fake_week3_super_table()
    stats = stats.loc[stats["NFL_player_id"] == "SYN_DEF"].copy()
    stats.loc[:, "player_week"] = "DEF-25_2012_6"
    stats.loc[:, "NFL_player_id"] = "DEF-25"
    stats.loc[:, "player"] = "Texans DST"
    stats.loc[:, "year"] = 2012
    stats.loc[:, "season"] = 2012
    stats.loc[:, "week"] = 6
    stats.loc[:, "position"] = "DEF"
    stats.loc[:, "nfl_position"] = "DEF"
    for col in set(DEF_COL_MAP.values()) | {"pts_def_std"}:
        stats.loc[:, col] = 0.0
    stats.loc[:, "pts_def_std"] = -2.0
    stats.loc[:, "pts_def_sack"] = 2.0
    stats.loc[:, "pts_allow_35_plus"] = 1.0
    stats.loc[:, "pts_def_st_td"] = 1.0

    player_rows = stats[["player_week", "NFL_player_id", "year", "week", "position"]].copy()
    player_rows.insert(0, "db_name", flat["db_name"])
    player_rows["manager"] = "Darren"
    player_rows["fantasy_position"] = "DEF"
    player_rows["fantasy_points"] = -2.0
    player_rows["bonus_points"] = None
    player_rows["te_premium_points"] = None
    bio = stats[["NFL_player_id", "nfl_position"]].copy()

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.register("_settings", settings_df)
    conn.register("_players", player_rows)
    conn.register("_stats", stats)
    conn.register("_bio", bio)
    conn.execute("CREATE TABLE public.league_settings AS SELECT * FROM _settings")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            fantasy_position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy
        SELECT
            db_name,
            player_week,
            NFL_player_id,
            year,
            week,
            position,
            fantasy_position,
            manager,
            CAST(fantasy_points AS DOUBLE),
            CAST(bonus_points AS DOUBLE),
            CAST(te_premium_points AS DOUBLE)
        FROM _players
        """
    )
    conn.execute("CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all AS SELECT * FROM _stats")
    conn.execute("CREATE TABLE ___ops.nfl_historical.player_bio AS SELECT * FROM _bio")

    runner = _PlayerRunner(db_name=flat["db_name"], data_dir="local")
    runner._conn = conn
    runner._platform = "yahoo"

    try:
        updated = runner.populate_fantasy_points()
        scored = conn.execute(
            """
            SELECT fantasy_points
            FROM public.player_fantasy
            WHERE player_week = 'DEF-25_2012_6'
            """
        ).fetchone()[0]
    finally:
        conn.close()

    assert updated == 1
    assert scored == pytest.approx(4.0)


def test_local_super_table_cache_has_interesting_week_3_seed():
    root = Path(__file__).resolve().parents[4]
    player_path = root / "fantasy_football_data" / "cache" / "nflverse" / "stats_player_week_2025.parquet"
    dst_path = root / "fantasy_football_data" / "cache" / "nflverse" / "nflverse_dst_regenerated.parquet"
    pbp_path = root / "fantasy_football_data" / "cache" / "nflverse" / "nflverse_pbp_2025.parquet"
    if not player_path.exists() or not dst_path.exists() or not pbp_path.exists():
        pytest.skip("local nflverse cache is not present")

    con = duckdb.connect()
    row = con.execute(
        """
        WITH p AS (
          SELECT season, week,
            SUM(COALESCE(passing_tds,0)) pass_td,
            SUM(COALESCE(passing_interceptions,0)) pass_int,
            SUM(COALESCE(rushing_tds,0)) rush_td,
            SUM(COALESCE(receiving_tds,0)) rec_td,
            SUM(COALESCE(passing_2pt_conversions,0)+COALESCE(rushing_2pt_conversions,0)+COALESCE(receiving_2pt_conversions,0)) two_pt,
            SUM(COALESCE(special_teams_tds,0)) st_td,
            SUM(COALESCE(punt_return_yards,0)+COALESCE(kickoff_return_yards,0)) ret_yd,
            SUM(COALESCE(fg_made_50_59,0)) fgm_50,
            SUM(COALESCE(fg_made_60_,0)) fgm_60,
            SUM(COALESCE(fg_missed_50_59,0)) fgmiss_50,
            SUM(COALESCE(fg_missed_60_,0)) fgmiss_60,
            SUM(COALESCE(pat_missed,0)) xpmiss
          FROM read_parquet(?)
          WHERE season_type='REG' AND season=2025 AND week=3
          GROUP BY season, week
        ), pbp AS (
          SELECT season, week,
            SUM(CASE WHEN pass=1 AND complete_pass=1 AND yards_gained >= 50 THEN 1 ELSE 0 END) AS pass_cmp_50p,
            SUM(CASE WHEN complete_pass=1 AND receiving_yards BETWEEN 0 AND 4 THEN 1 ELSE 0 END) AS rec_0_4,
            SUM(CASE WHEN complete_pass=1 AND receiving_yards BETWEEN 5 AND 9 THEN 1 ELSE 0 END) AS rec_5_9,
            SUM(CASE WHEN complete_pass=1 AND receiving_yards BETWEEN 10 AND 19 THEN 1 ELSE 0 END) AS rec_10_19,
            SUM(CASE WHEN complete_pass=1 AND receiving_yards BETWEEN 20 AND 29 THEN 1 ELSE 0 END) AS rec_20_29,
            SUM(CASE WHEN complete_pass=1 AND receiving_yards BETWEEN 30 AND 39 THEN 1 ELSE 0 END) AS rec_30_39
          FROM read_parquet(?)
          WHERE season_type='REG' AND season=2025 AND week=3
          GROUP BY season, week
        ), d AS (
          SELECT year AS season, week,
            SUM(COALESCE(def_tds,0)) def_td,
            SUM(COALESCE(def_safeties,0)) safe,
            SUM(COALESCE(special_teams_tds,0)) dst_st_td,
            SUM(COALESCE(three_out,0)) three_out,
            SUM(COALESCE(fourth_down_stop,0)) fourth_stop,
            SUM(COALESCE(pts_allow_0,0)) shutouts,
            SUM(COALESCE(pts_allow_35_plus,0)) blowups
          FROM read_parquet(?)
          WHERE season_type='REG' AND year=2025 AND week=3
          GROUP BY year, week
        )
        SELECT
          (pass_td>0)::INT + (pass_int>0)::INT + (rush_td>0)::INT + (rec_td>0)::INT + (two_pt>0)::INT +
          (st_td>0)::INT + (ret_yd>0)::INT + (fgm_50>0)::INT + (fgm_60>0)::INT + (fgmiss_50>0)::INT +
          (fgmiss_60>0)::INT + (xpmiss>0)::INT + (def_td>0)::INT + (safe>0)::INT + (dst_st_td>0)::INT +
          (three_out>0)::INT + (fourth_stop>0)::INT + (shutouts>0)::INT + (blowups>0)::INT +
          (pass_cmp_50p>0)::INT + (rec_0_4>0)::INT + (rec_5_9>0)::INT + (rec_10_19>0)::INT +
          (rec_20_29>0)::INT + (rec_30_39>0)::INT AS coverage_score
        FROM p JOIN pbp USING (season, week) JOIN d USING (season, week)
        """,
        [str(player_path), str(pbp_path), str(dst_path)],
    ).fetchone()

    assert row[0] >= 24
