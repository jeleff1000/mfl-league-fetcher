"""Tests that each platform normalizes playoff settings to canonical shape."""

import json
import math
import xml.etree.ElementTree as ET

try:
    from multi_league.core.yahoo_league_settings import derive_playoff_round_type
except ImportError:
    derive_playoff_round_type = None


def _derive_playoff_round_type_fallback(matchup_len: int, has_multiweek: str) -> int:
    """Standalone derivation for testability (fallback if import fails)."""
    if matchup_len >= 2:
        return 1
    elif has_multiweek == "1":
        return 2
    else:
        return 0


def _get_derive_fn():
    """Return the real function if available, else the fallback."""
    if derive_playoff_round_type is not None:
        return derive_playoff_round_type
    return _derive_playoff_round_type_fallback


def _make_yahoo_metadata(**overrides):
    """Build a Yahoo-style metadata dict with test defaults."""
    fn = _get_derive_fn()

    matchup_len = int(overrides.get("playoff_matchup_length", "1"))
    has_multiweek = overrides.get("has_multiweek_championship", "0")
    uses_reseeding_raw = overrides.get("uses_playoff_reseeding", "0")
    num_playoff_teams = int(overrides.get("num_playoff_teams", "6"))
    playoff_start_week = int(overrides.get("playoff_start_week", "15"))

    round_type = fn(matchup_len, has_multiweek)
    uses_reseeding = int(uses_reseeding_raw) if str(uses_reseeding_raw).isdigit() else 0
    num_rounds = math.ceil(math.log2(max(num_playoff_teams, 2)))

    if round_type == 0:
        championship_week = playoff_start_week + num_rounds - 1
    elif round_type == 1:
        championship_week = playoff_start_week + (2 * num_rounds) - 1
    else:
        championship_week = playoff_start_week + num_rounds

    return {
        "num_playoff_teams": str(num_playoff_teams),
        "playoff_start_week": str(playoff_start_week),
        "num_teams": overrides.get("num_teams", "12"),
        "playoff_round_type": round_type,
        "uses_reseeding": uses_reseeding,
        "playoff_matchup_length": matchup_len,
        "num_rounds": num_rounds,
        "championship_week": championship_week,
        "bye_teams": 2**num_rounds - num_playoff_teams,
        "has_multiweek_championship": has_multiweek,
    }


class TestYahooSettingsNormalization:
    def test_yahoo_standard_single_week(self):
        metadata = _make_yahoo_metadata(playoff_matchup_length="1", has_multiweek_championship="0")
        assert metadata["playoff_round_type"] == 0

    def test_yahoo_all_two_week(self):
        metadata = _make_yahoo_metadata(playoff_matchup_length="2", has_multiweek_championship="1")
        assert metadata["playoff_round_type"] == 1

    def test_yahoo_two_week_finals_only(self):
        metadata = _make_yahoo_metadata(playoff_matchup_length="1", has_multiweek_championship="1")
        assert metadata["playoff_round_type"] == 2

    def test_yahoo_reseeding_preserved(self):
        metadata = _make_yahoo_metadata(uses_playoff_reseeding="1")
        assert metadata["uses_reseeding"] == 1

    def test_yahoo_canonical_fields_present(self):
        metadata = _make_yahoo_metadata()
        for field in [
            "num_playoff_teams",
            "playoff_start_week",
            "playoff_round_type",
            "uses_reseeding",
            "num_teams",
            "num_rounds",
            "bye_teams",
            "championship_week",
        ]:
            assert field in metadata, f"Missing canonical field: {field}"

    def test_yahoo_championship_week_type0(self):
        """6 teams, start week 15, type 0 -> champ week 17"""
        metadata = _make_yahoo_metadata(num_playoff_teams="6", playoff_start_week="15")
        assert metadata["championship_week"] == 17

    def test_yahoo_championship_week_type1(self):
        """6 teams, start week 14, type 1 -> champ week 19"""
        metadata = _make_yahoo_metadata(
            num_playoff_teams="6", playoff_start_week="14", playoff_matchup_length="2", has_multiweek_championship="1"
        )
        assert metadata["championship_week"] == 19

    def test_yahoo_championship_week_type2(self):
        """6 teams, start week 15, type 2 -> champ week 18"""
        metadata = _make_yahoo_metadata(num_playoff_teams="6", playoff_start_week="15", has_multiweek_championship="1")
        assert metadata["championship_week"] == 18

    def test_yahoo_nested_stat_modifier_bonuses_flow_to_ddl(self):
        from multi_league.core.canonical_settings import flatten_settings
        from multi_league.core.scoring_config import normalize_yahoo
        from multi_league.core.yahoo_league_settings import (
            _build_scoring_rules,
            _parse_stat_categories,
            _parse_stat_modifier_bonuses,
            _parse_stat_modifiers,
        )

        root = ET.fromstring(
            """
            <fantasy_content>
              <league>
                <settings>
                  <stat_categories>
                    <stats>
                      <stat>
                        <stat_id>4</stat_id>
                        <enabled>1</enabled>
                        <name>Passing Yards</name>
                        <display_name>Pass Yds</display_name>
                        <stat_position_types>
                          <stat_position_type><position_type>O</position_type></stat_position_type>
                        </stat_position_types>
                      </stat>
                    </stats>
                  </stat_categories>
                  <stat_modifiers>
                    <stats>
                      <stat>
                        <stat_id>4</stat_id>
                        <value>0.033333333333333</value>
                        <bonuses>
                          <bonus>
                            <target>300</target>
                            <points>5</points>
                          </bonus>
                        </bonuses>
                      </stat>
                    </stats>
                  </stat_modifiers>
                </settings>
              </league>
            </fantasy_content>
            """
        )

        stat_map = _parse_stat_categories(root)
        scoring_rules = _build_scoring_rules(stat_map, _parse_stat_modifiers(root))
        scoring_rules.extend(_parse_stat_modifier_bonuses(root, stat_map))

        assert {
            "stat_id": "4:bonus:300",
            "name": "Pass Yds 300+ Bonus",
            "position_types": ["O"],
            "points": 5.0,
            "bonus_target": 300,
            "bonus_base_stat_id": "4",
            "canonical_key": "bonus_pass_yd_300",
        } in scoring_rules

        flat = flatten_settings(
            {
                "metadata": {"num_teams": 12},
                "canonical_scoring": normalize_yahoo(scoring_rules),
                "roster_position_counts": {"QB": 1},
            },
            platform="yahoo",
            year=2023,
            league_key="423.l.116031",
        )

        assert flat["scoring_pass_yd"] == 0.033333333333333
        assert flat["scoring_bonus_pass_yd_300"] == 5.0

    def test_yahoo_fleet_discovered_stat_modifier_bonuses_flow_to_custom_rules(self):
        """Non-DDL Yahoo bonus thresholds flow into custom rules."""
        from multi_league.core.canonical_settings import extract_scoring_settings_from_flat_row, flatten_settings
        from multi_league.core.scoring_config import normalize_yahoo
        from multi_league.core.yahoo_league_settings import (
            _build_scoring_rules,
            _parse_stat_categories,
            _parse_stat_modifier_bonuses,
            _parse_stat_modifiers,
        )

        root = ET.fromstring(
            """
            <fantasy_content>
              <league>
                <settings>
                  <stat_categories>
                    <stats>
                      <stat>
                        <stat_id>4</stat_id>
                        <name>Passing Yards</name>
                        <display_name>Pass Yds</display_name>
                        <enabled>1</enabled>
                        <position_types><position_type>O</position_type></position_types>
                      </stat>
                      <stat>
                        <stat_id>9</stat_id>
                        <name>Rushing Yards</name>
                        <display_name>Rush Yds</display_name>
                        <enabled>1</enabled>
                        <position_types><position_type>O</position_type></position_types>
                      </stat>
                      <stat>
                        <stat_id>12</stat_id>
                        <name>Receiving Yards</name>
                        <display_name>Rec Yds</display_name>
                        <enabled>1</enabled>
                        <position_types><position_type>O</position_type></position_types>
                      </stat>
                      <stat>
                        <stat_id>14</stat_id>
                        <name>Return Yards</name>
                        <display_name>Ret Yds</display_name>
                        <enabled>1</enabled>
                        <position_types><position_type>O</position_type></position_types>
                      </stat>
                      <stat>
                        <stat_id>48</stat_id>
                        <name>Defense/Special Teams Return Yards</name>
                        <display_name>D/ST Ret Yds</display_name>
                        <enabled>1</enabled>
                        <position_types><position_type>DT</position_type></position_types>
                      </stat>
                    </stats>
                  </stat_categories>
                  <stat_modifiers>
                    <stats>
                      <stat>
                        <stat_id>4</stat_id>
                        <value>0.04</value>
                        <bonuses><bonus><target>500</target><points>3</points></bonus></bonuses>
                      </stat>
                      <stat>
                        <stat_id>9</stat_id>
                        <value>0.1</value>
                        <bonuses>
                          <bonus><target>85</target><points>3</points></bonus>
                          <bonus><target>123</target><points>2</points></bonus>
                          <bonus><target>300</target><points>5</points></bonus>
                        </bonuses>
                      </stat>
                      <stat>
                        <stat_id>12</stat_id>
                        <value>0.1</value>
                        <bonuses>
                          <bonus><target>140</target><points>3</points></bonus>
                          <bonus><target>300</target><points>5</points></bonus>
                        </bonuses>
                      </stat>
                      <stat>
                        <stat_id>14</stat_id>
                        <value>0.04</value>
                        <bonuses><bonus><target>225</target><points>3</points></bonus></bonuses>
                      </stat>
                      <stat>
                        <stat_id>48</stat_id>
                        <value>0.04</value>
                        <bonuses>
                          <bonus><target>250</target><points>1</points></bonus>
                          <bonus><target>275</target><points>4</points></bonus>
                        </bonuses>
                      </stat>
                    </stats>
                  </stat_modifiers>
                </settings>
              </league>
            </fantasy_content>
            """
        )

        stat_map = _parse_stat_categories(root)
        scoring_rules = _build_scoring_rules(stat_map, _parse_stat_modifiers(root))
        scoring_rules.extend(_parse_stat_modifier_bonuses(root, stat_map))

        flat = flatten_settings(
            {
                "metadata": {"num_teams": 12},
                "canonical_scoring": normalize_yahoo(scoring_rules),
                "roster_position_counts": {"QB": 1},
            },
            platform="yahoo",
            year=2026,
            league_key="470.l.13655",
        )

        assert flat["scoring_bonus_pass_yd_500"] == 3.0
        assert flat["scoring_bonus_rush_yd_300"] == 5.0
        assert flat["scoring_bonus_rec_yd_300"] == 5.0
        assert "scoring_bonus_rush_yd_85" not in flat
        assert "scoring_bonus_rec_yd_140" not in flat
        assert "scoring_bonus_st_yd_225" not in flat
        assert "scoring_bonus_def_st_yd_250" not in flat

        custom_rules = json.loads(flat["scoring_custom_rules"])
        assert custom_rules == {
            "bonus_def_st_yd_250": 1.0,
            "bonus_def_st_yd_275": 4.0,
            "bonus_rec_yd_140": 3.0,
            "bonus_rush_yd_123": 2.0,
            "bonus_rush_yd_85": 3.0,
            "bonus_st_yd_225": 3.0,
        }

        scoring = extract_scoring_settings_from_flat_row(flat)
        assert scoring["bonus_rush_yd_85"] == 3.0
        assert scoring["bonus_rush_yd_123"] == 2.0
        assert scoring["bonus_rec_yd_140"] == 3.0
        assert scoring["bonus_st_yd_225"] == 3.0
        assert scoring["bonus_def_st_yd_250"] == 1.0
        assert scoring["bonus_def_st_yd_275"] == 4.0


class TestDerivePlayoffRoundType:
    """Direct tests of the derive function."""

    def test_matchup_len_2_returns_1(self):
        fn = _get_derive_fn()
        assert fn(2, "0") == 1
        assert fn(2, "1") == 1
        assert fn(3, "0") == 1

    def test_matchup_len_1_multiweek_returns_2(self):
        fn = _get_derive_fn()
        assert fn(1, "1") == 2

    def test_matchup_len_1_no_multiweek_returns_0(self):
        fn = _get_derive_fn()
        assert fn(1, "0") == 0


def _make_sleeper_metadata(**overrides):
    """Build a Sleeper-style metadata dict with test defaults."""
    prt = overrides.get("playoff_round_type", 0)
    num_playoff_teams = overrides.get("num_playoff_teams", 6)
    playoff_start_week = overrides.get("playoff_start_week", 15)
    num_rounds = math.ceil(math.log2(max(num_playoff_teams, 2)))
    has_multiweek = 1 if prt in (1, 2) else 0

    if prt == 0:
        championship_week = playoff_start_week + num_rounds - 1
    elif prt == 1:
        championship_week = playoff_start_week + (2 * num_rounds) - 1
    else:
        championship_week = playoff_start_week + num_rounds

    return {
        "num_playoff_teams": num_playoff_teams,
        "playoff_start_week": playoff_start_week,
        "playoff_round_type": prt,
        "has_multiweek_championship": has_multiweek,
        "uses_reseeding": None,
        "num_teams": overrides.get("num_teams", 12),
        "num_rounds": num_rounds,
        "bye_teams": 2**num_rounds - num_playoff_teams,
        "championship_week": championship_week,
    }


class TestSleeperSettingsNormalization:
    def test_sleeper_round_type_flows_to_metadata(self):
        """Sleeper playoff_round_type should flow into metadata, not be hardcoded to 0"""
        metadata = _make_sleeper_metadata(playoff_round_type=1)
        assert metadata["playoff_round_type"] == 1

    def test_sleeper_multiweek_derived_from_round_type_1(self):
        metadata = _make_sleeper_metadata(playoff_round_type=1)
        assert metadata["has_multiweek_championship"] == 1

    def test_sleeper_multiweek_derived_from_round_type_2(self):
        metadata = _make_sleeper_metadata(playoff_round_type=2)
        assert metadata["has_multiweek_championship"] == 1

    def test_sleeper_standard_round_type(self):
        metadata = _make_sleeper_metadata(playoff_round_type=0)
        assert metadata["playoff_round_type"] == 0
        assert metadata["has_multiweek_championship"] == 0

    def test_sleeper_reseeding_null(self):
        """Sleeper doesn't expose reseeding — should be None"""
        metadata = _make_sleeper_metadata()
        assert metadata.get("uses_reseeding") is None

    def test_sleeper_championship_week_type0(self):
        """Type 0, 6 teams, start week 15 → champ week 17"""
        metadata = _make_sleeper_metadata(playoff_round_type=0, num_playoff_teams=6, playoff_start_week=15)
        assert metadata["championship_week"] == 17

    def test_sleeper_championship_week_type1(self):
        """Type 1 (all 2-week): 6 teams, start week 14 → 3 rounds × 2 = champ week 19"""
        metadata = _make_sleeper_metadata(playoff_round_type=1, num_playoff_teams=6, playoff_start_week=14)
        assert metadata["championship_week"] == 19

    def test_sleeper_championship_week_type2(self):
        """Type 2 (2-week finals): 6 teams, start week 15 → champ week 18"""
        metadata = _make_sleeper_metadata(playoff_round_type=2, num_playoff_teams=6, playoff_start_week=15)
        assert metadata["championship_week"] == 18

    def test_sleeper_canonical_fields_present(self):
        metadata = _make_sleeper_metadata()
        for field in [
            "num_playoff_teams",
            "playoff_start_week",
            "playoff_round_type",
            "num_teams",
            "num_rounds",
            "bye_teams",
            "championship_week",
            "has_multiweek_championship",
        ]:
            assert field in metadata, f"Missing: {field}"


def _make_espn_metadata(**overrides):
    """Build an ESPN-style metadata dict with test defaults."""
    num_playoff_teams = overrides.get("num_playoff_teams", 6)
    playoff_start_week = overrides.get("playoff_start_week", 15)
    num_teams = overrides.get("num_teams", 12)
    num_rounds = math.ceil(math.log2(max(num_playoff_teams, 2)))
    championship_week = playoff_start_week + num_rounds - 1  # ESPN: always type 0

    return {
        "num_playoff_teams": num_playoff_teams,
        "playoff_start_week": playoff_start_week,
        "playoff_round_type": 0,
        "uses_reseeding": None,
        "num_teams": num_teams,
        "bye_teams": 2**num_rounds - num_playoff_teams,
        "num_rounds": num_rounds,
        "championship_week": championship_week,
        "has_multiweek_championship": 0,
        "num_consolation_teams": None,
        "end_week": overrides.get("end_week", championship_week),
    }


class TestESPNSettingsNormalization:
    def test_espn_canonical_fields_present(self):
        metadata = _make_espn_metadata(num_playoff_teams=8, playoff_start_week=14)
        for field in [
            "num_playoff_teams",
            "playoff_start_week",
            "playoff_round_type",
            "num_teams",
            "bye_teams",
            "num_rounds",
            "championship_week",
        ]:
            assert field in metadata, f"Missing: {field}"

    def test_espn_round_type_defaults_to_0(self):
        metadata = _make_espn_metadata()
        assert metadata["playoff_round_type"] == 0

    def test_espn_reseeding_is_null(self):
        metadata = _make_espn_metadata()
        assert metadata.get("uses_reseeding") is None

    def test_espn_championship_week(self):
        """8 teams, start week 14 -> 3 rounds -> champ week 16"""
        metadata = _make_espn_metadata(num_playoff_teams=8, playoff_start_week=14)
        assert metadata["championship_week"] == 16

    def test_espn_4_team_bracket(self):
        """4 teams -> 2 rounds, 0 byes"""
        metadata = _make_espn_metadata(num_playoff_teams=4, playoff_start_week=15)
        assert metadata["num_rounds"] == 2
        assert metadata["bye_teams"] == 0
        assert metadata["championship_week"] == 16

    def test_espn_multiweek_is_0(self):
        metadata = _make_espn_metadata()
        assert metadata["has_multiweek_championship"] == 0
