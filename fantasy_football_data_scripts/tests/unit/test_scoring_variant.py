"""Tests for scoring variant derivation."""

from multi_league.core.scoring_variant import derive_scoring_variant


def test_8_team_rounds_to_10t():
    assert derive_scoring_variant(8, False, False, 4, 0.5).startswith("10t_")


def test_10_team_maps_to_10t():
    assert derive_scoring_variant(10, False, False, 4, 0.5).startswith("10t_")


def test_11_team_rounds_to_10t():
    assert derive_scoring_variant(11, False, False, 4, 0.5).startswith("10t_")


def test_12_team_maps_to_12t():
    assert derive_scoring_variant(12, False, False, 4, 0.5).startswith("12t_")


def test_14_team_rounds_to_12t():
    assert derive_scoring_variant(14, False, False, 4, 0.5).startswith("12t_")


def test_16_team_rounds_to_12t():
    assert derive_scoring_variant(16, False, False, 4, 0.5).startswith("12t_")


def test_standard_flex():
    result = derive_scoring_variant(12, False, False, 4, 0.5)
    assert result == "12t_flx_half_4pt"


def test_superflex():
    result = derive_scoring_variant(12, True, False, 4, 0.5)
    assert result == "12t_sflx_half_4pt"


def test_idp():
    result = derive_scoring_variant(12, False, True, 4, 0.5)
    assert result == "12t_idp_half_4pt"


def test_superflex_beats_idp():
    result = derive_scoring_variant(12, True, True, 4, 0.5)
    assert result == "12t_sflx_half_4pt"


def test_4pt_pass_td():
    assert derive_scoring_variant(12, False, False, 4, 0.5) == "12t_flx_half_4pt"


def test_5pt_rounds_to_4pt():
    assert derive_scoring_variant(12, False, False, 5, 0.5) == "12t_flx_half_4pt"


def test_5_5pt_rounds_to_6pt():
    assert derive_scoring_variant(12, False, False, 5.5, 0.5) == "12t_flx_half_6pt"


def test_6pt_pass_td():
    assert derive_scoring_variant(12, False, False, 6, 0.5) == "12t_flx_half_6pt"


def test_0_ppr():
    assert derive_scoring_variant(12, False, False, 4, 0.0) == "12t_flx_std_4pt"


def test_half_ppr():
    assert derive_scoring_variant(12, False, False, 4, 0.5) == "12t_flx_half_4pt"


def test_full_ppr():
    assert derive_scoring_variant(12, False, False, 4, 1.0) == "12t_flx_ppr_4pt"


def test_0_25_ppr_rounds_to_std():
    assert derive_scoring_variant(12, False, False, 4, 0.25) == "12t_flx_std_4pt"


def test_0_75_ppr_rounds_to_ppr():
    assert derive_scoring_variant(12, False, False, 4, 0.75) == "12t_flx_ppr_4pt"


def test_tep_half_ppr_uses_half():
    result = derive_scoring_variant(12, False, False, 4, 0.5, te_premium=0.5)
    assert result == "12t_flx_half_4pt"


def test_tep_full_ppr_uses_ppr():
    result = derive_scoring_variant(12, False, False, 6, 1.0, te_premium=0.5)
    assert result == "12t_flx_ppr_6pt"


def test_10t_sflx_ppr_6pt():
    assert derive_scoring_variant(10, True, False, 6, 1.0) == "10t_sflx_ppr_6pt"


def test_12t_idp_std_4pt():
    assert derive_scoring_variant(12, False, True, 4, 0.0) == "12t_idp_std_4pt"


def test_string_inputs_are_coerced():
    assert derive_scoring_variant("12", "true", "false", "5", "0.75") == "12t_sflx_ppr_4pt"


def test_none_optional_settings_use_safe_defaults():
    assert derive_scoring_variant(10, False, False, None, 1.0, te_premium=None) == "10t_flx_ppr_4pt"


def test_output_has_4_parts():
    result = derive_scoring_variant(12, False, False, 4, 0.5)
    assert len(result.split("_")) == 4


def test_all_36_combos_are_valid():
    valid = {
        f"{size}_{roster}_{ppr}_{td}"
        for size in ("10t", "12t")
        for roster in ("flx", "sflx", "idp")
        for ppr in ("std", "half", "ppr")
        for td in ("4pt", "6pt")
    }

    for teams in [8, 10, 12, 14, 16]:
        for sf in [True, False]:
            for idp in [True, False]:
                for td in [3, 4, 5, 6, 7]:
                    for ppr in [0.0, 0.25, 0.5, 0.75, 1.0]:
                        result = derive_scoring_variant(teams, sf, idp, td, ppr)
                        assert result in valid, f"Invalid variant: {result}"
