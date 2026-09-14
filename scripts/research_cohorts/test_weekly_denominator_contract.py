"""Regression tests for weekly outcome/championship denominator inputs."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "research_cohorts"))

import build_research_matchup_cohort as builder


def _sql(monkeypatch):
    monkeypatch.setattr(builder, "_PLAYER_COLUMNS", {
        "win", "loss", "tie", "team_points", "opponent_points", "champion",
        "clutch_equity", "manager", "team_key", "is_championship",
    })
    monkeypatch.setattr(builder, "_MATCHUP_COLUMNS", {
        "win", "loss", "tie", "team_points", "opponent_points", "manager",
        "team_key", "is_championship",
    })
    return builder.weekly_player_sql(2003)


def test_weekly_championship_credit_is_not_season_champion_credit(monkeypatch):
    sql = _sql(monkeypatch)
    champ_block = sql[sql.index("AS champ_started") - 700:sql.index("AS champ_started")]
    assert "is_championship" in champ_block
    assert "p.champion" not in champ_block
    assert "act.week IS NOT NULL" in champ_block
    assert "tgw.week IS NOT NULL" in champ_block


def test_weekly_clutch_requires_active_game_and_confirmed_outcome(monkeypatch):
    sql = _sql(monkeypatch)
    clutch_block = sql[sql.index("AS clutch_sum") - 500:sql.index("AS clutch_sum")]
    assert "act.week IS NOT NULL" in clutch_block
    assert "tgw.week IS NOT NULL" in clutch_block
    assert "p.outcome_confirmed=1" in clutch_block
    assert "p.clutch_equity" in clutch_block


def test_weekly_missing_player_outcome_has_matchup_fallback(monkeypatch):
    sql = _sql(monkeypatch)
    start = sql.index("pfd AS")
    pfd = sql[start:sql.index("FROM public.player_fantasy p", start)]
    assert "mfd AS" in sql
    assert "COALESCE(p.win, m.win" in pfd
    assert "COALESCE(p.loss, m.loss" in pfd
    assert "team_points" in pfd


def test_championship_marker_qualifies_denominator_but_not_credit(monkeypatch):
    monkeypatch.setattr(builder, "_PLAYER_COLUMNS", {
        "champion", "is_championship", "win", "loss", "tie",
    })
    patched = builder._denominator_sql_with_explicit_championship_signal(
        "MAX(CASE WHEN CAST(p.champion AS INT) = 1 THEN 1 ELSE 0 END) AS has_champ"
    )
    assert "p.is_championship" in patched
    assert "p.champion" in patched
    # This helper only changes eligibility SQL.  The weekly numerator remains
    # guarded by the explicit row-level signal and start/active/team-game tests.
    assert "started" not in patched


def test_matchup_only_championship_marker_qualifies_denominator(monkeypatch):
    monkeypatch.setattr(builder, "_PLAYER_COLUMNS", {"champion"})
    monkeypatch.setattr(builder, "_MATCHUP_COLUMNS", {"is_championship"})
    patched = builder._denominator_sql_with_explicit_championship_signal(
        "MAX(CASE WHEN CAST(p.champion AS INT) = 1 THEN 1 ELSE 0 END) AS has_champ"
    )
    assert "public.matchup m_ch" in patched
    assert "m_ch.is_championship" in patched


def test_position_denominators_use_position_slot_tiers():
    # The player primitives already use teams_by_pos_grp_sql; the denominator
    # lattice must use the same axes or K/DEF numerators join to a different
    # cohort and silently inherit the wrong league population.
    assert "teams_K" in builder.DENOM_SQL
    assert "teams_DEF" in builder.DENOM_SQL
    assert "teams_K" in builder.DENOM_WEEK_SQL
    assert "teams_DEF" in builder.DENOM_WEEK_SQL
