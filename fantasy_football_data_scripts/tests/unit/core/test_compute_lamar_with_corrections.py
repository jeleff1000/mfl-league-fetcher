import duckdb
import pytest

from multi_league.core.scoring_config import compute_lamar_with_corrections
from multi_league.data_fetchers.research_lamar import wrap_lamar_query_with_corrections


SQL_COL_DEFAULTS = {
    "position": "RB",
    "lamar_12t_flx_half_4pt": 0,
    "lamar_12t_idp_half_4pt": 0,
    "receptions": 0,
    "replacement_receptions": 0,
    "pts_idp_tackle_solo": 0,
    "replacement_pts_idp_tackle_solo": 0,
    "pts_idp_sack": 0,
    "replacement_pts_idp_sack": 0,
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


def test_lamar_exact_match_returns_bare_column():
    assert compute_lamar_with_corrections({}, "lamar_12t_flx_half_4pt") == "lamar_12t_flx_half_4pt"


def test_lamar_replacement_delta_for_ppr():
    expr = compute_lamar_with_corrections({"rec": 0.4}, "lamar_12t_flx_half_4pt")
    assert eval_sql(
        expr,
        lamar_12t_flx_half_4pt=10,
        receptions=8,
        replacement_receptions=5,
    ) == pytest.approx(9.7)


def test_lamar_idp_correction_uses_replacement_atoms():
    expr = compute_lamar_with_corrections(
        {"idp_tkl_solo": 1, "idp_sack": 2},
        "lamar_12t_idp_half_4pt",
    )
    assert "replacement_pts_idp_tackle_solo" in expr
    assert eval_sql(
        expr,
        lamar_12t_idp_half_4pt=10,
        position="LB",
        pts_idp_tackle_solo=5,
        replacement_pts_idp_tackle_solo=3,
        pts_idp_sack=1,
        replacement_pts_idp_sack=0.5,
    ) == pytest.approx(13)


def test_wrap_lamar_query_passes_through_non_lamar_cols():
    out = wrap_lamar_query_with_corrections(
        ["NFL_player_id", "player", "lamar_12t_flx_half_4pt"],
        {},
        ["lamar_12t_flx_half_4pt"],
    )
    assert out == ["NFL_player_id", "player", "lamar_12t_flx_half_4pt"]


def test_wrap_lamar_query_aliases_corrected_lamar_cols():
    out = wrap_lamar_query_with_corrections(
        ["lamar_12t_flx_half_4pt"],
        {"scoring_rec": 0.4},
        ["lamar_12t_flx_half_4pt"],
    )
    assert out[0].endswith(" AS lamar_12t_flx_half_4pt")
    assert out[0].startswith("lamar_12t_flx_half_4pt + ")
