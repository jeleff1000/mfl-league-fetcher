"""Tests for multi_league.core.roster_slots."""

import pytest
from multi_league.core.roster_slots import (
    FLEX_ELIGIBLE,
    NON_STARTER_SLOTS,
    DEDICATED_POSITIONS,
    IDP_FAMILIES,
    resolve,
    eligible_positions,
    is_flex,
    is_bench,
    get_flex_pools,
)


class TestConstants:
    """Verify canonical constants are correct."""

    def test_flex_eligible_has_all_canonical_types(self):
        expected = {"FLX", "SUPER_FLEX", "REC_FLEX", "W/R", "R/T", "IDP", "DL_LB", "DB_LB"}
        assert set(FLEX_ELIGIBLE.keys()) == expected

    def test_flx_eligible_positions(self):
        assert FLEX_ELIGIBLE["FLX"] == {"WR", "RB", "TE"}

    def test_super_flex_eligible_positions(self):
        assert FLEX_ELIGIBLE["SUPER_FLEX"] == {"QB", "WR", "RB", "TE"}

    def test_rec_flex_eligible_positions(self):
        assert FLEX_ELIGIBLE["REC_FLEX"] == {"WR", "TE"}

    def test_wr_flex_eligible_positions(self):
        assert FLEX_ELIGIBLE["W/R"] == {"WR", "RB"}

    def test_rt_flex_eligible_positions(self):
        assert FLEX_ELIGIBLE["R/T"] == {"RB", "TE"}

    def test_idp_eligible_positions(self):
        assert FLEX_ELIGIBLE["IDP"] == {"DL", "LB", "DB"}

    def test_dl_lb_eligible_positions(self):
        assert FLEX_ELIGIBLE["DL_LB"] == {"DL", "LB"}

    def test_db_lb_eligible_positions(self):
        assert FLEX_ELIGIBLE["DB_LB"] == {"DB", "LB"}

    def test_non_starter_slots(self):
        for slot in ("BN", "IR", "IL", "TAXI", "RESERVE", "RES", "COVID", "PUP", "INJ"):
            assert slot in NON_STARTER_SLOTS

    def test_dedicated_positions(self):
        for pos in ("QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB"):
            assert pos in DEDICATED_POSITIONS

    def test_idp_families(self):
        assert "LB" in IDP_FAMILIES
        assert "DL" in IDP_FAMILIES
        assert "DB" in IDP_FAMILIES
        assert "ILB" in IDP_FAMILIES["LB"]
        assert "DE" in IDP_FAMILIES["DL"]
        assert "CB" in IDP_FAMILIES["DB"]


class TestResolve:
    """Test alias resolution to canonical names."""

    # Yahoo offense aliases
    def test_wrt_resolves_to_flx(self):
        assert resolve("W/R/T") == "FLX"

    def test_flex_resolves_to_flx(self):
        assert resolve("FLEX") == "FLX"

    def test_qwrt_resolves_to_super_flex(self):
        assert resolve("Q/W/R/T") == "SUPER_FLEX"

    def test_op_resolves_to_super_flex(self):
        assert resolve("OP") == "SUPER_FLEX"

    def test_wt_resolves_to_rec_flex(self):
        assert resolve("W/T") == "REC_FLEX"

    def test_wrrb_flex_resolves_to_wr(self):
        assert resolve("WRRB_FLEX") == "W/R"

    # IDP aliases
    def test_idp_flex_resolves_to_idp(self):
        assert resolve("IDP_FLEX") == "IDP"

    def test_d_resolves_to_idp(self):
        assert resolve("D") == "IDP"

    def test_dp_resolves_to_idp(self):
        assert resolve("DP") == "IDP"

    # Granular IDP -> dedicated slot
    def test_de_resolves_to_dl(self):
        assert resolve("DE") == "DL"

    def test_cb_resolves_to_db(self):
        assert resolve("CB") == "DB"

    def test_ilb_resolves_to_lb(self):
        assert resolve("ILB") == "LB"

    # Passthrough for canonical and dedicated
    def test_qb_passthrough(self):
        assert resolve("QB") == "QB"

    def test_flx_passthrough(self):
        assert resolve("FLX") == "FLX"

    def test_super_flex_passthrough(self):
        assert resolve("SUPER_FLEX") == "SUPER_FLEX"

    # Case insensitive
    def test_case_insensitive(self):
        assert resolve("w/r/t") == "FLX"
        assert resolve("flex") == "FLX"

    # DST aliases
    def test_dst_resolves_to_def(self):
        assert resolve("DST") == "DEF"

    def test_d_st_resolves_to_def(self):
        assert resolve("D/ST") == "DEF"


class TestEligiblePositions:
    """Test eligible position lookups."""

    def test_flx_slot(self):
        assert eligible_positions("FLX") == {"WR", "RB", "TE"}

    def test_yahoo_alias(self):
        assert eligible_positions("W/R/T") == {"WR", "RB", "TE"}

    def test_super_flex(self):
        assert eligible_positions("SUPER_FLEX") == {"QB", "WR", "RB", "TE"}

    def test_idp(self):
        assert eligible_positions("IDP") == {"DL", "LB", "DB"}

    def test_dedicated_position_returns_itself(self):
        assert eligible_positions("QB") == {"QB"}
        assert eligible_positions("K") == {"K"}
        assert eligible_positions("DEF") == {"DEF"}
        assert eligible_positions("DL") == {"DL"}

    def test_unknown_raises(self):
        with pytest.raises(ValueError):
            eligible_positions("INVALID_SLOT")


class TestIsFlex:
    """Test flex detection."""

    def test_canonical_flex_types(self):
        assert is_flex("FLX") is True
        assert is_flex("SUPER_FLEX") is True
        assert is_flex("IDP") is True

    def test_aliases_are_flex(self):
        assert is_flex("W/R/T") is True
        assert is_flex("FLEX") is True
        assert is_flex("Q/W/R/T") is True

    def test_dedicated_not_flex(self):
        assert is_flex("QB") is False
        assert is_flex("RB") is False
        assert is_flex("K") is False

    def test_bench_not_flex(self):
        assert is_flex("BN") is False
        assert is_flex("IR") is False


class TestIsBench:
    """Test bench/non-starter detection."""

    def test_bench_slots(self):
        assert is_bench("BN") is True
        assert is_bench("IR") is True
        assert is_bench("TAXI") is True
        assert is_bench("BE") is True
        assert is_bench("ER") is True

    def test_starter_slots_not_bench(self):
        assert is_bench("QB") is False
        assert is_bench("FLX") is False
        assert is_bench("W/R/T") is False


class TestGetFlexPools:
    """Test roster-aware flex pool extraction."""

    def test_standard_league(self):
        roster = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "W/R/T": 1, "K": 1, "DEF": 1, "BN": 6}
        pools = get_flex_pools(roster)
        assert len(pools) == 1
        assert pools[0] == ("FLX", {"WR", "RB", "TE"}, 1)

    def test_superflex_league(self):
        roster = {"QB": 1, "RB": 2, "WR": 3, "TE": 1, "FLEX": 2, "SUPER_FLEX": 1, "K": 1, "DEF": 1}
        pools = get_flex_pools(roster)
        assert len(pools) == 2
        # Sorted by pool size: FLX (3) before SUPER_FLEX (4)
        assert pools[0][0] == "FLX"
        assert pools[0][2] == 2
        assert pools[1][0] == "SUPER_FLEX"
        assert pools[1][2] == 1

    def test_idp_league(self):
        roster = {
            "QB": 1,
            "RB": 2,
            "WR": 2,
            "TE": 1,
            "FLEX": 1,
            "K": 1,
            "DEF": 1,
            "LB": 2,
            "DL": 2,
            "DB": 2,
            "IDP_FLEX": 1,
        }
        pools = get_flex_pools(roster)
        canonical_names = [p[0] for p in pools]
        assert "FLX" in canonical_names
        assert "IDP" in canonical_names

    def test_no_flex_slots(self):
        roster = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "K": 1, "DEF": 1}
        pools = get_flex_pools(roster)
        assert pools == []

    def test_zero_count_flex_ignored(self):
        roster = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "W/R/T": 0, "K": 1, "DEF": 1}
        pools = get_flex_pools(roster)
        assert pools == []

    def test_sorted_by_pool_size_ascending(self):
        roster = {"QB": 1, "RB": 2, "WR": 2, "TE": 1, "SUPER_FLEX": 1, "REC_FLEX": 1, "FLEX": 1}
        pools = get_flex_pools(roster)
        sizes = [len(p[1]) for p in pools]
        assert sizes == sorted(sizes)

    def test_duplicate_aliases_deduplicated(self):
        """A league won't have both FLEX and W/R/T, but if it does, first wins."""
        roster = {"QB": 1, "FLEX": 2, "W/R/T": 1}
        pools = get_flex_pools(roster)
        assert len(pools) == 1  # Both resolve to FLX, only first processed
        assert pools[0][0] == "FLX"
