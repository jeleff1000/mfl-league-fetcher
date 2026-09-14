"""Tests for shared name normalization utilities."""

import pandas as pd

from multi_league.data_fetchers.shared.name_utils import normalize_name, apply_name_aliases


class TestNormalizeName:
    """Test normalize_name matches behavior of all existing implementations."""

    def test_empty_string(self):
        assert normalize_name("") == ""

    def test_none(self):
        assert normalize_name(None) == ""

    def test_nan_float(self):
        assert normalize_name(float("nan")) == ""

    def test_nan_pandas(self):
        assert normalize_name(pd.NA) == ""

    def test_basic_name(self):
        assert normalize_name("Patrick Mahomes") == "patrick mahomes"

    def test_suffix_jr(self):
        assert normalize_name("Marvin Harrison Jr") == "marvin harrison"

    def test_suffix_jr_dot(self):
        assert normalize_name("Marvin Harrison Jr.") == "marvin harrison"

    def test_suffix_sr(self):
        assert normalize_name("Someone Sr") == "someone"

    def test_suffix_sr_dot(self):
        assert normalize_name("Someone Sr.") == "someone"

    def test_suffix_ii(self):
        assert normalize_name("Mark Andrews II") == "mark andrews"

    def test_suffix_iii(self):
        assert normalize_name("Robert Griffin III") == "robert griffin"

    def test_suffix_iv(self):
        assert normalize_name("Henry IV") == "henry"

    def test_suffix_v(self):
        assert normalize_name("Henry V") == "henry"

    def test_accents(self):
        assert normalize_name("Jose Ramirez") == "jose ramirez"

    def test_apostrophe(self):
        assert normalize_name("Ja'Marr Chase") == "jamarr chase"

    def test_period_in_name(self):
        assert normalize_name("A.J. Brown") == "aj brown"

    def test_hyphen_in_name(self):
        assert normalize_name("Clyde Edwards-Helaire") == "clyde edwardshelaire"

    def test_extra_whitespace(self):
        assert normalize_name("  Patrick   Mahomes  ") == "patrick mahomes"

    def test_already_normalized(self):
        assert normalize_name("patrick mahomes") == "patrick mahomes"

    def test_numeric_input(self):
        assert normalize_name(123) == "123"

    def test_suffix_not_removed_mid_name(self):
        # "Junior" in the middle shouldn't be stripped
        result = normalize_name("Junior Seau")
        assert result == "junior seau"


class TestApplyNameAliases:
    def test_alias_match(self):
        aliases = {"pat mahomes": "patrick mahomes"}
        assert apply_name_aliases("pat mahomes", aliases) == "patrick mahomes"

    def test_no_match(self):
        aliases = {"pat mahomes": "patrick mahomes"}
        assert apply_name_aliases("josh allen", aliases) == "josh allen"

    def test_none_aliases(self):
        assert apply_name_aliases("josh allen", None) == "josh allen"

    def test_empty_aliases(self):
        assert apply_name_aliases("josh allen", {}) == "josh allen"
