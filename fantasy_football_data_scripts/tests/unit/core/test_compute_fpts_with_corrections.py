import duckdb
import pytest

from multi_league.core.scoring_config import (
    compute_fpts_composite_formula,
    compute_fpts_with_corrections,
    get_fpts_column,
    get_fpts_variant_columns,
)


SQL_COL_DEFAULTS = {
    "position": "RB",
    "fpts_4pt_half": 0,
    "fpts_4pt_half_ret": 0,
    "fpts_4pt_ppr": 0,
    "fpts_4pt_tep": 0,
    "fpts_5pt_half": 0,
    "passing_tds": 0,
    "receptions": 0,
    "kickoff_return_yards": 0,
    "punt_return_yards": 0,
    "pts_k_std": 0,
    "fg_yards_canonical": 0,
    "fg_yards_over_30_canonical": 0,
    "fg_made": 0,
    "fg_made_0_19": 0,
    "fg_made_20_29": 0,
    "fg_made_30_39": 0,
    "fg_made_40_49": 0,
    "fg_made_50_59": 0,
    "fg_made_60_plus_canonical": 0,
    "fg_missed": 0,
    "fg_missed_0_19": 0,
    "fg_missed_20_29": 0,
    "fg_missed_30_39": 0,
    "fg_missed_40_49": 0,
    "fg_missed_50_59": 0,
    "pat_made": 0,
    "pat_missed": 0,
    "pts_def_std": 0,
    "pts_def_sack": 0,
    "pts_def_int": 0,
    "pts_def_ff": 0,
    "pts_def_fr": 0,
    "pts_def_td": 0,
    "pts_def_safety": 0,
    "pts_def_block": 0,
    "pts_def_tfl": 0,
    "pts_def_3out": 0,
    "pts_def_4stop": 0,
    "pts_def_ret_yd": 0,
    "pts_def_ret_td": 0,
    "pts_allow_0": 0,
    "pts_allow_1_6": 0,
    "pts_allow_7_13": 0,
    "pts_allow_14_20": 0,
    "pts_allow_21_27": 0,
    "pts_allow_28_34": 0,
    "pts_allow_35_plus": 0,
    "yds_allow_0_99": 0,
    "yds_allow_100_199": 0,
    "yds_allow_200_299": 0,
    "yds_allow_300_349": 0,
    "yds_allow_350_399": 0,
    "yds_allow_400_449": 0,
    "yds_allow_450_499": 0,
    "yds_allow_500_549": 0,
    "yds_allow_550_plus": 0,
    "pts_idp_tackle_solo": 0,
    "pts_idp_tackle_assist": 0,
    "pts_idp_tkl_combined": 0,
    "pts_idp_sack": 0,
    "pts_idp_int": 0,
    "pts_idp_ff": 0,
    "pts_idp_fr": 0,
    "pts_idp_pd": 0,
    "pts_idp_qb_hit": 0,
    "pts_idp_tfl": 0,
    "pts_idp_safety": 0,
    "pts_idp_td": 0,
    "pts_idp_blk_kick": 0,
    "pts_idp_int_ret_yd": 0,
    "pts_idp_fum_rec_yd": 0,
    "def_sack_yards": 0,
}


def _literal(value):
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return repr(value)


def eval_sql(expr: str, **values) -> float:
    row = dict(SQL_COL_DEFAULTS)
    row.update(values)
    select_list = ", ".join(f"{_literal(value)} AS {col}" for col, value in row.items())
    result = duckdb.sql(f"SELECT ({expr}) AS value FROM (SELECT {select_list}) t").fetchone()[0]
    return float(result)


def test_exact_match_returns_bare_column():
    # The modal DST baseline now bakes return TDs at +6 (89% of league-years, median 6),
    # so an "exact" config must include def_st_td=6 to match the baked pts_def_std.
    assert compute_fpts_with_corrections({"def_st_td": 6}, "fpts_4pt_half") == "fpts_4pt_half"


def test_return_td_baseline_is_net_zero_for_leagues():
    # Baking return TD=6 into pts_def_std is invariant for league totals: a league that does NOT
    # score return TDs (empty/absent -> opt-in 0) gets a correction that removes exactly the baked
    # +6, netting to the pre-baking pts_def_std. A league that scores 6 is exact (bare column).
    no_ret = compute_fpts_with_corrections({}, "fpts_4pt_half")
    assert no_ret != "fpts_4pt_half"  # correction fires to strip the baked baseline
    # A DEF row whose only points are the baked return TD (fpts == pts_def_std == 6): a non-scoring
    # league's correction removes exactly that +6, netting to 0 (its pre-baking total).
    assert eval_sql(no_ret, fpts_4pt_half=6, pts_def_std=6) == pytest.approx(0.0)
    # A league scoring return TD at the modal 6 is exact -> bare column (keeps the +6).
    assert compute_fpts_with_corrections({"def_st_td": 6}, "fpts_4pt_half") == "fpts_4pt_half"


def test_composite_formula_registry_has_24_variants():
    cols = get_fpts_variant_columns()
    assert len(cols) == 24
    assert len(set(cols)) == 24
    assert compute_fpts_composite_formula("fpts_4pt_half") == (
        "COALESCE(pts_pass_4pt, 0) + COALESCE(pts_rush, 0) + "
        "COALESCE(pts_rec_half, 0) + COALESCE(pts_misc, 0) + "
        "COALESCE(pts_k_std, 0) + COALESCE(pts_def_std, 0)"
    )
    assert compute_fpts_composite_formula("fpts_6pt_tep_ret").endswith(" + COALESCE(pts_ret_yds, 0)")


def test_pass_td_delta():
    expr = compute_fpts_with_corrections({"pass_td": 5.25}, "fpts_5pt_half")
    assert eval_sql(expr, fpts_5pt_half=10, passing_tds=2) == pytest.approx(10.5)


def test_ppr_delta():
    expr = compute_fpts_with_corrections({"rec": 0.4}, "fpts_4pt_half")
    assert eval_sql(expr, fpts_4pt_half=20, receptions=10) == pytest.approx(19)


def test_tep_delta():
    expr = compute_fpts_with_corrections({"bonus_rec_te": 0.25}, "fpts_4pt_tep")
    assert eval_sql(expr, fpts_4pt_tep=30, position="TE", receptions=5) == pytest.approx(28.75)


def test_return_yard_delta():
    expr = compute_fpts_with_corrections({"kr_yd": 0.05, "pr_yd": 0.02}, "fpts_4pt_half_ret")
    assert eval_sql(expr, fpts_4pt_half_ret=10, kickoff_return_yards=100, punt_return_yards=50) == pytest.approx(10)


def test_kicker_profile_delta():
    expr = compute_fpts_with_corrections(
        {
            "fgm_0_19": 1,
            "fgm_20_29": 1,
            "fgm_30_39": 1,
            "fgm_40_49": 2,
            "fgm_50p": 3,
            "xpm": 1,
            "fgmiss": -1,
        },
        "fpts_4pt_half",
    )
    assert eval_sql(
        expr,
        fpts_4pt_half=18,
        pts_k_std=18,
        fg_made_0_19=1,
        fg_made_20_29=1,
        fg_made_30_39=1,
        fg_made_40_49=1,
        fg_made_50_59=1,
        pat_made=1,
        fg_missed=1,
    ) == pytest.approx(8)


def test_defense_pa_profile_delta():
    expr = compute_fpts_with_corrections(
        {
            "pts_allow_0": 5,
            "pts_allow_1_6": 4,
            "pts_allow_7_13": 3,
            "pts_allow_14_20": 1,
            "pts_allow_21_27": 0,
            "pts_allow_28_34": -1,
            "pts_allow_35p": -3,
        },
        "fpts_4pt_half",
    )
    assert eval_sql(expr, fpts_4pt_half=10, pts_def_std=10, pts_allow_0=1) == pytest.approx(5)


def test_idp_additive_delta():
    expr = compute_fpts_with_corrections(
        {"idp_tkl_solo": 1, "idp_sack": 2},
        "fpts_4pt_half",
    )
    assert eval_sql(
        expr,
        fpts_4pt_half=0,
        position="LB",
        pts_idp_tackle_solo=5,
        pts_idp_sack=1,
    ) == pytest.approx(7)


def test_cross_axis_delta():
    expr = compute_fpts_with_corrections(
        {
            "bonus_rec_te": 1.0,
            "fgm_0_19": 1,
            "fgm_20_29": 1,
            "fgm_30_39": 1,
            "fgm_40_49": 2,
            "fgm_50p": 3,
            "pts_allow_0": 5,
            "pts_allow_1_6": 4,
            "pts_allow_7_13": 3,
            "pts_allow_14_20": 1,
            "pts_allow_21_27": 0,
            "pts_allow_28_34": -1,
            "pts_allow_35p": -3,
        },
        "fpts_4pt_tep",
    )
    assert eval_sql(
        expr,
        fpts_4pt_tep=100,
        position="TE",
        receptions=4,
        pts_k_std=18,
        fg_made_0_19=1,
        fg_made_20_29=1,
        fg_made_30_39=1,
        fg_made_40_49=1,
        fg_made_50_59=1,
        pat_made=1,
        fg_missed=1,
        pts_def_std=10,
        pts_allow_0=1,
    ) == pytest.approx(87)


def test_pass_td_zero_edge():
    expr = compute_fpts_with_corrections({"pass_td": 0}, "fpts_4pt_half")
    assert eval_sql(expr, fpts_4pt_half=12, passing_tds=3) == pytest.approx(0)


def test_get_fpts_column_tie_breaks_pass_td_to_higher_variant():
    assert get_fpts_column({"pass_td": 4.5, "rec": 0.5}) == "fpts_5pt_half"


def test_flat_scoring_rows_must_be_normalized_at_ddl_boundary():
    with pytest.raises(ValueError, match="extract_scoring_settings_from_flat_row"):
        compute_fpts_with_corrections({"scoring_rec": 0.4}, "fpts_4pt_half")
