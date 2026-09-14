import pandas as pd

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.core.canonical_settings import (
    ALL_SCORING_KEYS,
    YAHOO_DRAFT_TYPE_MAP,
    _col_type,
    extract_scoring_settings_from_flat_row,
    get_schema_keys,
)


def test_resolve_scoring_year_walks_past_empty_scoring_settings():
    """When the requested year exists but its scoring_settings is empty, walk to
    the nearest year whose scoring_settings has populated rec + pass_td. The
    league-wide fallback (self.ppr / self.pass_td_pts) is only the last resort
    when no year has scoring at all."""
    roster_by_year = {
        2015: {"QB": 1, "scoring_settings": {"rec": 0.5, "pass_td": 4}},
        # 2018 row exists in league_settings but the scoring sub-dict is empty
        # (real-world failure mode — see project_dynamic_scoring_config).
        2018: {"QB": 1, "scoring_settings": {}},
        2025: {"QB": 1, "scoring_settings": {"rec": 1.0, "pass_td": 6}},
    }
    # Instance-wide values intentionally distinct from EITHER 2015 or 2025
    # scoring so a stale fallback would stand out.
    base = SQLEnrichmentsBase("test_db", dry_run=True, roster_by_year=roster_by_year, ppr=0.0, pass_td_pts=5)

    resolved_year, year_settings = base._resolve_scoring_year(2018, roster_by_year)

    # 2015 (distance 3) is closer than 2025 (distance 7) — should win.
    assert resolved_year == 2015
    assert year_settings["scoring_settings"]["rec"] == 0.5
    assert year_settings["scoring_settings"]["pass_td"] == 4


def test_resolve_scoring_year_returns_exact_year_when_scoring_populated():
    """When the requested year has populated scoring, return it as-is (no walk)."""
    roster_by_year = {
        2015: {"QB": 1, "scoring_settings": {"rec": 0.5, "pass_td": 4}},
        2018: {"QB": 1, "scoring_settings": {"rec": 1.0, "pass_td": 6}},
    }
    base = SQLEnrichmentsBase("test_db", dry_run=True, roster_by_year=roster_by_year)

    resolved_year, year_settings = base._resolve_scoring_year(2018, roster_by_year)

    assert resolved_year == 2018
    assert year_settings["scoring_settings"]["rec"] == 1.0


def test_resolve_scoring_year_falls_through_when_no_year_has_scoring():
    """When no year has populated scoring, return the original nearest-neighbor
    resolution (so the caller's instance-wide fallback still has a settings
    dict to read kick_col / def_multipliers / etc. from)."""
    roster_by_year = {
        2015: {"QB": 1, "scoring_settings": {}},
        2018: {"QB": 1, "scoring_settings": {}},
    }
    base = SQLEnrichmentsBase("test_db", dry_run=True, roster_by_year=roster_by_year)

    resolved_year, year_settings = base._resolve_scoring_year(2018, roster_by_year)

    # No year qualifies — fall through to roster-based resolution.
    assert resolved_year == 2018
    assert year_settings == {"QB": 1, "scoring_settings": {}}


def test_get_scoring_for_year_walks_past_empty_scoring_settings():
    """End-to-end: _get_scoring_for_year must not silently fall back to the
    instance-wide (latest-year) PPR / pass_td_pts when a mid-history year
    happens to have an empty scoring_settings sub-dict. It should pick the
    nearest year whose scoring is populated."""
    roster_by_year = {
        2015: {
            "QB": 1,
            "scoring_settings": {"rec": 0.5, "pass_td": 4},
            "kick_col": "pts_k_std",
        },
        2018: {
            "QB": 1,
            "scoring_settings": {},  # broken row
            "kick_col": "pts_k_std",
        },
        2025: {
            "QB": 1,
            "scoring_settings": {"rec": 1.0, "pass_td": 6},
            "kick_col": "pts_k_yds",
        },
    }
    # Instance-wide is the latest year's value (1.0 / 6) — what the buggy
    # code returns for 2018. The correct answer is 2015 (nearest populated).
    base = SQLEnrichmentsBase("test_db", dry_run=True, roster_by_year=roster_by_year, ppr=1.0, pass_td_pts=6)

    result = base._get_scoring_for_year(2018, roster_by_year)

    assert result["resolved_year"] == 2015
    assert result["ppr"] == 0.5
    assert result["pass_td_pts"] == 4


def test_get_scoring_for_year_clamps_to_first_and_last_league_year():
    roster_by_year = {
        2015: {
            "QB": 1,
            "scoring_settings": {"rec": 0.5, "pass_td": 4},
            "kick_col": "pts_k_std",
            "bonus_multipliers": {"bonus_pass_300yd": 1.0},
            "te_premium": 0.0,
            "def_multipliers": {"pts_def_sack": 1.0},
        },
        2025: {
            "QB": 1,
            "scoring_settings": {"rec": 1.0, "pass_td": 6},
            "kick_col": "pts_k_yds",
            "bonus_multipliers": {"bonus_pass_300yd": 2.0},
            "te_premium": 0.5,
            "def_multipliers": {"pts_def_sack": 2.0},
        },
    }

    base = SQLEnrichmentsBase("test_db", dry_run=True, roster_by_year=roster_by_year)

    before_first = base._get_scoring_for_year(1950, roster_by_year)
    after_last = base._get_scoring_for_year(2030, roster_by_year)

    assert before_first["resolved_year"] == 2015
    assert before_first["ppr"] == 0.5
    assert before_first["pass_td_pts"] == 4
    assert before_first["kick_col"] == "pts_k_std"
    assert before_first["bonus_multipliers"] == {"bonus_pass_300yd": 1.0}
    assert before_first["te_premium"] == 0.0
    assert before_first["def_multipliers"] == {"pts_def_sack": 1.0}

    assert after_last["resolved_year"] == 2025
    assert after_last["ppr"] == 1.0
    assert after_last["pass_td_pts"] == 6
    assert after_last["kick_col"] == "pts_k_yds"
    assert after_last["bonus_multipliers"] == {"bonus_pass_300yd": 2.0}
    assert after_last["te_premium"] == 0.5
    assert after_last["def_multipliers"] == {"pts_def_sack": 2.0}


def test_load_settings_from_db_preserves_custom_dst_rules():
    class _FakeResult:
        def __init__(self, frame):
            self._frame = frame

        def fetchdf(self):
            return self._frame

    class _FakeConn:
        def __init__(self, frame):
            self._frame = frame

        def execute(self, _sql):
            return _FakeResult(self._frame)

    row = pd.DataFrame(
        [
            {
                "year": 2025,
                "roster_qb": 1,
                "scoring_rec": 0.5,
                "scoring_pass_td": 4.0,
                "scoring_pass_cmp": 0.5,
                "scoring_pass_yd": 0.033333333333333,
                "scoring_sack": 0.5,
                "scoring_int": 3.0,
                "scoring_fum_rec": 2.0,
                "scoring_def_td": 6.0,
                "scoring_def_st_td": 8.0,
                "scoring_tkl_loss": 0.5,
                "scoring_def_3_and_out": 0.5,
                "scoring_def_4_and_stop": 1.0,
                "scoring_pts_allow_7_13": -1.0,
                "scoring_pts_allow_14_20": -2.0,
                "scoring_pts_allow_21_27": -3.0,
                "scoring_pts_allow_28_34": -4.0,
                "scoring_pts_allow_35p": -5.0,
                "scoring_yds_allow_300_349": -2.0,
                "scoring_yds_allow_400_449": -3.0,
                "scoring_yds_allow_500_549": -5.0,
                "scoring_fgm_yds": 0.1,
            }
        ]
    )

    base = SQLEnrichmentsBase("test_db", dry_run=True)
    base._table_exists = lambda _table_name: True
    base._get_connection = lambda: _FakeConn(row)
    roster_by_year, _ = base.load_settings_from_db()

    year_settings = roster_by_year[2025]
    assert year_settings["kick_col"] == "pts_k_yds"
    assert year_settings["scoring_settings"]["pass_cmp"] == 0.5
    assert year_settings["scoring_settings"]["pass_yd"] == 0.033333333333333
    assert year_settings["def_multipliers"]["pts_def_sack"] == 0.5
    assert year_settings["def_multipliers"]["pts_def_int"] == 3.0
    assert year_settings["def_multipliers"]["pts_def_fr"] == 2.0
    # def_st_td is a SPECIAL TEAMS category, NOT a split of unified def_td.
    # Mode-detect should NOT fire here — both coexist independently.
    # pts_def_td uses its own multiplier (6.0) while pts_def_st_td gets its own (8.0).
    assert year_settings["def_multipliers"]["pts_def_td"] == 6.0
    assert year_settings["def_multipliers"]["pts_def_st_td"] == 8.0  # Task 4: dedicated column
    assert year_settings["def_multipliers"]["pts_def_tfl"] == 0.5
    assert year_settings["def_multipliers"]["pts_def_3out"] == 0.5
    assert year_settings["def_multipliers"]["pts_def_4stop"] == 1.0
    assert year_settings["def_multipliers"]["yds_allow_300_349"] == -2.0
    assert year_settings["def_multipliers"]["yds_allow_350_399"] == -2.0
    assert year_settings["def_multipliers"]["yds_allow_400_449"] == -3.0
    assert year_settings["def_multipliers"]["yds_allow_450_499"] == -3.0
    assert year_settings["def_multipliers"]["yds_allow_500_549"] == -5.0
    assert year_settings["def_multipliers"]["yds_allow_550_plus"] == -5.0


def test_load_settings_from_db_treats_missing_rec_as_standard():
    class _FakeResult:
        def __init__(self, frame):
            self._frame = frame

        def fetchdf(self):
            return self._frame

    class _FakeConn:
        def __init__(self, frame):
            self._frame = frame

        def execute(self, _sql):
            return _FakeResult(self._frame)

    rows = pd.DataFrame(
        [
            {
                "year": 2020,
                "roster_qb": 1,
                "scoring_rec": None,
                "scoring_pass_td": 6.0,
                "scoring_pass_yd": 0.04,
                "scoring_rush_yd": 0.066666666666667,
                "scoring_rec_yd": 0.066666666666667,
            }
        ]
    )

    base = SQLEnrichmentsBase("td_s_beer", dry_run=True)
    base._table_exists = lambda _table_name: True
    base._get_connection = lambda: _FakeConn(rows)

    roster_by_year, scoring_params = base.load_settings_from_db()

    assert roster_by_year[2020]["scoring_settings"]["rec"] == 0.0
    assert scoring_params["ppr"] == 0.0


def test_extract_scoring_settings_from_flat_row_strips_only_ddl_scoring_columns():
    row = {
        "year": 2025,
        "scoring_type": "ppr",
        "scoring_variant": "12t_sflx_ppr_6pt",
        "scoring_rec": 1.0,
        "scoring_pass_td": 6.0,
        "scoring_bonus_rec_te": None,
        "rec": 0.5,
    }

    assert extract_scoring_settings_from_flat_row(row) == {"rec": 1.0, "pass_td": 6.0}


def test_extract_scoring_settings_from_flat_row_merges_custom_rules_json():
    row = {
        "scoring_rec": 1.0,
        "scoring_custom_rules": '{"bonus_def_st_yd_275": 4.0, "bonus_rush_yd_123": 2.0}',
    }

    assert extract_scoring_settings_from_flat_row(row) == {
        "rec": 1.0,
        "bonus_def_st_yd_275": 4.0,
        "bonus_rush_yd_123": 2.0,
    }


def test_extract_scoring_settings_from_flat_row_reads_legacy_dynamic_columns():
    row = {
        "scoring_rec": 1.0,
        "scoring_bonus_rush_yd_85": 3.0,
        "scoring_bonus_def_st_yd_250": 1.0,
    }

    assert extract_scoring_settings_from_flat_row(row) == {
        "rec": 1.0,
        "bonus_rush_yd_85": 3.0,
        "bonus_def_st_yd_250": 1.0,
    }


def test_all_scoring_keys_includes_new_ddl_keys():
    """Every scoring key found in the wild must be in ALL_SCORING_KEYS."""
    required_new_keys = [
        "pass_fd",
        "pass_int_td",
        "rush_fd",
        "rec_0_4",
        "rec_5_9",
        "rec_10_19",
        "rec_20_29",
        "rec_30_39",
        "st_ff",
        "st_fum_rec",
        "st_tkl_solo",
        "pr_yd",
        "int_ret_yd",
        "fum_ret_yd",
        "def_st_fum_rec",
        "def_st_tkl_solo",
        "def_st_yd",
        "def_pass_def",
        "def_forced_punts",
        "def_kr_yd",
        "def_pr_yd",
        "fg_ret_yd",
        "blk_kick_ret_yd",
        "pts_allow_46p",
        "qb_hit",
        "sack_yd",
        "tkl",
        "tkl_ast",
        "tkl_solo",
        "yds_allow",
        "fgm",
        "fgm_yds_over_30",
        "fg_pct",
        "idp_sack_yd",
        "idp_pass_def_3p",
        "idp_pts_allow_0",
        "idp_pts_allow_1_6",
        "idp_pts_allow_7_13",
        "idp_pts_allow_14_20",
        "bonus_pass_cmp_25",
        "bonus_rush_att_20",
        "bonus_rush_rec_yd_100",
        "bonus_rush_rec_yd_200",
        "bonus_rec_rb",
        "bonus_rec_wr",
        "bonus_sack_2p",
        "bonus_tkl_10p",
        "bonus_def_fum_td_50p",
        "bonus_def_int_td_50p",
        "fgm_50_59_alt",
        "fgm_60p_alt",
        "fgmiss_50_59",
        "fgmiss_60p",
        "pts_allow_14_20_alt",
        "rec_40p_alt",
        "one_pt_safe",
    ]
    for key in required_new_keys:
        assert key in ALL_SCORING_KEYS, f"Missing scoring key: {key}"
    schema_keys = get_schema_keys()
    for key in required_new_keys:
        assert f"scoring_{key}" in schema_keys, f"Missing schema column: scoring_{key}"


def test_all_scoring_keys_have_pipeline_reader():
    from multi_league.core.scoring_config import IRREDUCIBLE_SCORING_KEYS
    from multi_league.transformations.common.sql_base import (
        BONUS_COL_MAP,
        DEF_COL_MAP,
        IDP_COL_MAP,
        KICK_COL_MAP,
    )
    from multi_league.transformations.player.modules.scoring_calculator import (
        _BONUS_THRESHOLD_KEY_TO_SOURCE,
        _BONUS_KEY_TO_COL,
        _SLEEPER_KEY_TO_STAT,
    )

    known = set()
    for col_map in (DEF_COL_MAP, IDP_COL_MAP, BONUS_COL_MAP, KICK_COL_MAP):
        known.update(key.removeprefix("scoring_") for key in col_map)
    known.update(_SLEEPER_KEY_TO_STAT)
    known.update(_BONUS_KEY_TO_COL)
    known.update(_BONUS_THRESHOLD_KEY_TO_SOURCE)
    known.update(IRREDUCIBLE_SCORING_KEYS)

    assert sorted(set(ALL_SCORING_KEYS) - known) == []


def test_merged_aliases_not_in_scoring_keys():
    """Aliases that are merged (def_st_ff, idp_fum_ret_yd) must NOT be in ALL_SCORING_KEYS."""
    assert "def_st_ff" not in ALL_SCORING_KEYS
    assert "idp_fum_ret_yd" not in ALL_SCORING_KEYS


# _normalize_scoring is module-private but tested directly as a pragmatic choice
from multi_league.core.canonical_settings import _normalize_scoring


def test_normalize_scoring_merges_def_st_ff_into_ff():
    """def_st_ff should be folded into ff when ff is absent."""
    scoring = {"def_st_ff": 1.5}
    result = _normalize_scoring(scoring)
    assert result["scoring_ff"] == 1.5
    assert "scoring_def_st_ff" not in result


def test_normalize_scoring_ff_takes_precedence_over_def_st_ff():
    """When both exist, ff value wins (they're always equal in practice)."""
    scoring = {"ff": 2.0, "def_st_ff": 2.0}
    result = _normalize_scoring(scoring)
    assert result["scoring_ff"] == 2.0


def test_normalize_scoring_merges_idp_fum_ret_yd():
    """idp_fum_ret_yd should fold into idp_fum_rec_yd."""
    scoring = {"idp_fum_ret_yd": 0.1}
    result = _normalize_scoring(scoring)
    assert result["scoring_idp_fum_rec_yd"] == 0.1
    assert "scoring_idp_fum_ret_yd" not in result


def test_normalize_scoring_none_input():
    """None scoring dict should produce all-None values."""
    result = _normalize_scoring(None)
    assert result["scoring_ff"] is None
    assert result["scoring_pass_fd"] is None


def test_ensure_columns_raises_for_missing_canonical_columns():
    base = SQLEnrichmentsBase("test_db", dry_run=True)

    try:
        base._ensure_columns("public.matchup", {"avg_seed": "DOUBLE"}, existing_cols={"year", "week", "manager"})
    except RuntimeError as exc:
        assert "avg_seed" in str(exc)
        assert "matchup" in str(exc)
    else:
        raise AssertionError("Expected canonical missing-column check to raise")


def test_yahoo_draft_type_map_normalizes_all_known_values():
    """Yahoo raw draft_type values must map to canonical snake/auction/unknown."""
    assert YAHOO_DRAFT_TYPE_MAP["self"] == "snake"
    assert YAHOO_DRAFT_TYPE_MAP["offline"] == "snake"
    assert YAHOO_DRAFT_TYPE_MAP["offline_snake"] == "snake"
    assert YAHOO_DRAFT_TYPE_MAP["linear"] == "snake"
    assert YAHOO_DRAFT_TYPE_MAP["snake"] == "snake"
    assert YAHOO_DRAFT_TYPE_MAP["auction"] == "auction"
    # "live" is ambiguous — must NOT be in map (falls through to "unknown")
    assert "live" not in YAHOO_DRAFT_TYPE_MAP


def test_variant_metadata_columns_use_varchar_not_double():
    assert _col_type("import_mode") == "VARCHAR"
    assert _col_type("scoring_variant") == "VARCHAR"
    assert _col_type("scoring_variant_first_year") == "VARCHAR"
    assert _col_type("playoff_bracket_source") == "VARCHAR"
    assert _col_type("playoff_seeding_rule") == "VARCHAR"
    assert _col_type("playoff_seeding_rule_by") == "INTEGER"
    assert _col_type("home_team_bonus") == "DOUBLE"
    assert _col_type("playoff_home_team_bonus") == "DOUBLE"
    assert _col_type("playoff_matchup_tie_rule") == "VARCHAR"
