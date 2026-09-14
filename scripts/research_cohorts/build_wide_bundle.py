"""Build Fly-ready, player-keyed research cohort tables (the merge bundle) from the local
research_cohorts.duckdb chain output. One row per player identity; config values in
`{stat}_{teams}_{roster}_{ppr}_{td}` columns (36 slugs = 2 teams x 3 roster lanes
x 3 ppr x 2 td, matching lamar_{slug}).

COARSENING LADDER (Joe 2026-07-17; spec: research-eligibility-gates-2026-07-16.md §5).
Season + career tables serve each cell at its FINEST CONFIDENT rung, walking
4 -> 3 (drops td) -> 2 (format-blind: drops ppr+td) -> 0 (all pooled).

PER-METRIC MIN_STABLE: a flat n>=35 cohort gate replicates cohort MEANS, but PER-PLAYER
values need metric-specific sample. The rung walk is per STAT CLASS: each class serves from
the finest confident rung whose n_leagues clears ITS threshold. Thresholds RE-DERIVED
2026-07-18 on the 12.8k-league lake (ladder_stab.py) — the 07-17 values were within noise
EXCEPT draft_score, which moved 244 -> 643 (small-lake artifact). Consequences:
  * cheap stats keep their fine rungs; scores/clutch fall to coarser (bigger) rungs;
  * champ_wk / won / lost only answer where per-year pooling clears their large thresholds;
  * role players lose champ/win% via the frontend's player-sample gates (champ_elig,
    started_weeks) -- the data ships the raw components either way.

Schema contract (frontend research-cohort-query.ts): per slug, the DEFAULT class keeps
`level_{slug}` + `n_leagues_{slug}`; every non-default class C adds `level_{C}_{slug}` +
`n_{C}_{slug}` (NOT n_leagues_{C}_{slug} -- the API aliasMap reads n_srate_{slug} etc.).
A stat whose class cannot clear its threshold at ANY rung is NULL, and so is its lane --
an honest blank, not a silent approximation. Columns the API binds that a table cannot
produce (e.g. season n_years) are NULL-padded so route SQL never binder-errors.

Correctness gates (2026-07-19 -- restored after a parallel-session port dropped them):
  * confidence='confident' on laddered sources; <> 'insufficient' on weekly;
  * flx position gate: rung 0 pools EVERY league including IDP, so without it an IDP
    linebacker's surplus tops a flx board (observed: Kaden Elliss 141.0 at rung 0, 2024);
  * record ships won_pct/lost_pct (percent, won+lost = start_rate by construction), NEVER
    the raw wins_started counts (observed live: "Win %" max 40,305);
  * identity rows come only from ANSWERING rows (no fully-hollow rows).

Weekly tables stay rung-4-only (their builders emit no coarse rungs); they carry
level_{slug}=4 and duplicate class lanes where a value exists so the frontend reads ONE
schema shape.

CANONICAL POINTS/PPG (Joe 2026-07-19): points and ppg are NOT cohort-sampled statistics --
they serve straight from the super table's slug-scored weeklies (fpts_{td}_{ppr}; points do
not vary by team count, so the 12 slugs map onto 6 scoring variants). PPG = plain per-game
average over NFL-active REG weeks -- NOT start-weighted, NOT league-observed. This removes
cross-league scoring noise from the value lanes structurally (a poison league cannot touch
them at all) and answers wherever the player has NFL data, with no MIN_STABLE gate -- it is
population-exact, not sampled. Cohort tables keep owning usage/record/clutch; their
league-observed ppg_when_started / avg_fantasy_points stay in rc as diagnostics only.
"""
from __future__ import annotations
import os

from pathlib import Path

import duckdb

OUT_DIR = Path(os.environ.get("RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
RC_PATH = OUT_DIR / "research_cohorts.duckdb"
OPS_PATH = Path(os.environ.get(
    "RESEARCH_OPS_CACHE_PATH",
    "D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb",
))
BUNDLE_PATH = OUT_DIR / "research_merge_bundle.duckdb"

SLUGS = [f"{t}_{r}_{p}_{td}" for t in ("10t", "12t") for r in ("flx", "sflx", "idp")
         for p in ("std", "half", "ppr") for td in ("4pt", "6pt")]
# canonical LAMAR only exists for flex cells (build_research_matchup_cohort.py
# fails non-flx closed), so lamar_{slug} source columns are flex-only.
LAMAR_SLUGS = [s for s in SLUGS if "_flx_" in s]
MATCHUP_BRACKETS = ("4po", "6po", "8po")

# Stat classes -> MIN_STABLE (split-half r=0.70, elite pool; re-derived 2026-07-18 on the
# 12.8k lake). 'default' covers mean-type stats and player-sample counts (they ride the
# cohort-confidence gate, 35). Class membership is per stat, declared in TABLES below.
MIN_STABLE = {"default": 35, "srate": 50, "rate": 62, "adp": 116, "wscore": 165,
              "dscore": 643, "clutch": 532, "champ": 1301, "win": 2645,
              # ROSTER% FLOORS ARE SET BY THE HARDEST PLAYER TO MEASURE, WHICH IS THE 50% GUY.
              # Every other floor here answers "is this stat reliable" (split-half r=0.70).
              # These two answer Joe's actual question: how many leagues before a 50%-rostered
              # WR reliably reads between 45 and 55 -- +/-5 points at 85%.
              #
              # Required N is an inverted U in the rate itself, measured 2021-2025 on redraft
              # managed WRs in thick (300+ league) cohorts:
              #     0-5% rostered ->  10 leagues     45-55% ->  75 leagues
              #     15-30%        ->  60            95-100% ->  10
              # Ja'Marr Chase in 2024 reads 99.9% with an SD of 0.013 and is pinned by a
              # handful of leagues. So the floor is not an average, it is the peak of that
              # curve: satisfy the 50% player and every other player is already satisfied.
              "rrate": 140,      # WEEKLY  (R23)
              # 75 and not 90: the first measurement ran against a stale module that cut
              # teams into 2 tiers instead of 4, so each cohort mixed two team sizes and
              # carried variance that was an artefact of the cut, not of the leagues.
              # Splitting teams properly drops the between-league SD .317 -> .301 and the
              # requirement 90 -> 75.
              "rrates": 75}      # SEASON: 17 weeks buy 2.5x, not 17x -- see below

# Class lanes the frontend/API can request per dataset (research-cohort-query.ts METRICS).
# pad_api_columns NULL-pads level_{cls}/n_{cls} on every table, so a class listed here that a
# given grain does not use costs that grain two empty columns and nothing else.
LANE_CLASSES = ("srate", "rrate", "rrates", "rate", "adp", "wscore", "dscore", "clutch", "champ", "win")

# Columns each API dataset binds per slug (route SQL fails on a missing column, so every
# table NULL-pads what it cannot produce -- e.g. n_years exists only at career grain).
API_METRICS = {
    "draft": ["adp", "adp_source", "cost", "avg_cost_when_drafted", "auction_draft_rate",
              "draft_rate", "points", "lamar", "lamar_ppg", "draft_score", "draft_score_healthy",
              "n_drafted", "n_years", "n_auction_leagues", "n_auction_eligible_leagues",
              "earliest_adp", "earliest_adp_year", "latest_adp", "latest_adp_year",
              "highest_draft_rate", "highest_draft_rate_year", "lowest_draft_rate",
              "lowest_draft_rate_year", "highest_cost", "highest_cost_year", "lowest_cost",
              "lowest_cost_year", "career_auction_spend", "best_draft_score",
              "best_draft_score_year", "worst_draft_score", "worst_draft_score_year",
              "best_healthy_score", "best_healthy_score_year", "worst_healthy_score",
              "worst_healthy_score_year"],
    "transactions": ["add_rate", "faab", "add_lamar", "add_grade", "title_run",
                     "drop_regret", "n_add", "n_drop", "n_years"],
    "matchup": ["roster_rate", "start_rate", "healthy_start_rate", "win_rate", "expected_wins",
                "expected_losses",
                "expected_starts", "ppg", "points", "won", "lost", "clutch", "lamar", "lamar_per_season",
                "champ_wk", "playoff", "expected_champs", "expected_playoffs",
                "champ_total", "champ_started", "playoff_total", "playoff_started",
                "active_weeks", "inactive_weeks", "started_weeks", "eligible_leagues",
                "started_leagues", "healthy_elig", "champ_elig", "playoff_elig", "n_years"],
}

# (bundle_name, api_dataset, source_table, identity_cols, laddered?, {out_stat: (source_col, class)})
# n_drafted / n_add / champ_elig ship so boards can gate on the PLAYER's own sample --
# cohort confidence does not stop a single-league artifact from topping a population board
# (observed: Clayton Tune ADP 13 at n_drafted=1, Jacobs best-add at n_add=2).
TABLES = [
    ("research_draft", "draft", "draft", ["NFL_player_id", "year"], True,
     {"adp": ("adp", "adp"), "adp_source": ("adp_source", "adp"),
      "cost": ("cost_pct", "default"),
      "avg_cost_when_drafted": ("avg_auction_cost_pct", "default"),
      "auction_draft_rate": ("auction_draft_rate_pct", "rate"),
      "draft_rate": ("draft_rate_pct", "rate"), "market_delta": ("market_delta", "adp"),
      "points": ("avg_fantasy_points", "default"), "lamar": ("avg_manager_lamar", "default"),
      "lamar_ppg": ("avg_manager_lamar", "default"),
      "n_drafted": ("n_drafted", "default"),
      "n_auction_leagues": ("n_auction_leagues", "default"),
      "n_auction_eligible_leagues": ("n_auction_eligible_leagues", "default"),
      "draft_score": ("draft_score", "dscore"),
      "draft_score_healthy": ("draft_score_healthy", "dscore"),
      "best_pick": ("best_pick", "dscore"), "n_leagues": ("n_leagues", "default")}),
    ("research_draft_career", "draft", "draft_career", ["NFL_player_id"], True,
     {"n_years": ("n_years", "default"), "adp": ("adp", "adp"),
      "earliest_adp": ("earliest_adp", "adp"), "earliest_adp_year": ("earliest_adp_year", "adp"),
      "latest_adp": ("latest_adp", "adp"), "latest_adp_year": ("latest_adp_year", "adp"),
      "draft_rate": ("draft_rate_pct", "rate"), "points": ("avg_fantasy_points", "default"),
      "draft_score": ("draft_score", "dscore"), "lamar": ("avg_manager_lamar", "default"),
      "lamar_ppg": ("avg_manager_lamar", "default"),
      "highest_draft_rate": ("highest_draft_rate", "rate"), "highest_draft_rate_year": ("highest_draft_rate_year", "rate"),
      "lowest_draft_rate": ("lowest_draft_rate", "rate"), "lowest_draft_rate_year": ("lowest_draft_rate_year", "rate"),
      "cost": ("cost_pct", "default"), "avg_cost_when_drafted": ("avg_auction_cost_pct", "default"),
      "auction_draft_rate": ("auction_draft_rate_pct", "rate"),
      "highest_cost": ("highest_cost_pct", "default"), "highest_cost_year": ("highest_cost_pct_year", "default"),
      "lowest_cost": ("lowest_cost_pct", "default"), "lowest_cost_year": ("lowest_cost_pct_year", "default"),
      "career_auction_spend": ("career_auction_spend_pct", "default"),
      "best_draft_score": ("best_draft_score", "dscore"), "best_draft_score_year": ("best_draft_score_year", "dscore"),
      "worst_draft_score": ("worst_draft_score", "dscore"), "worst_draft_score_year": ("worst_draft_score_year", "dscore"),
      "draft_score_healthy": ("draft_score_healthy", "dscore"),
      "best_healthy_score": ("best_healthy_score", "dscore"), "best_healthy_score_year": ("best_healthy_score_year", "dscore"),
      "worst_healthy_score": ("worst_healthy_score", "dscore"), "worst_healthy_score_year": ("worst_healthy_score_year", "dscore"),
      "n_drafted": ("n_drafted", "default"), "n_auction_leagues": ("n_auction_leagues", "default"),
      "n_auction_eligible_leagues": ("n_auction_eligible_leagues", "default"),
      "n_leagues": ("n_leagues", "default")}),
    # faab is % OF BUDGET (Joe 2026-07-17) -- raw dollars pooled $100/$200/$1000 leagues
    # into one meaningless average (observed max 5,259). faab_bid keeps the raw legacy number.
    ("research_transactions", "transactions", "transactions", ["NFL_player_id", "year"], True,
     {"add_rate": ("add_rate_pct", "rate"), "faab": ("avg_faab_pct", "default"),
      "faab_bid": ("avg_faab_bid", "default"), "add_lamar": ("avg_add_lamar", "default"),
      "title_run": ("title_run", "wscore"), "drop_regret": ("avg_drop_regret", "default"),
      "n_add": ("n_add_leagues", "default"), "n_drop": ("n_drop_leagues", "default"),
      "n_leagues": ("n_leagues", "default")}),
    ("research_transactions_weekly", "transactions", "transactions_weekly",
     ["NFL_player_id", "year", "week"], False,
     {"add_rate": ("add_rate_pct", "rate"), "faab": ("avg_faab_pct", "default"),
      "faab_bid": ("avg_faab_bid", "default"), "add_lamar": ("avg_add_lamar", "default"),
      "add_grade": ("add_grade", "default"), "n_add_lg": ("n_add_lg", "default"),
      "n_leagues": ("n_leagues", "default")}),
    ("research_transactions_career", "transactions", "transactions_career", ["NFL_player_id"], True,
     {"n_years": ("n_years", "default"), "add_rate": ("add_rate_pct", "rate"),
      "faab": ("avg_faab_pct", "default"), "add_lamar": ("avg_add_lamar", "default"),
      "title_run": ("title_run", "wscore"), "drop_regret": ("avg_drop_regret", "default"),
      "n_add": ("n_add_leagues", "default"), "n_leagues": ("n_leagues", "default")}),
    ("research_matchup", "matchup", "matchup", ["NFL_player_id", "year"], True,
     # SEASON roster% floor is 90 (measured), not the start family's 50. A season is 17 weeks
     # but they are not 17 observations: within a league the weeks correlate at rho=0.37, so a
     # season is worth 2.5 independent weeks. That cuts the requirement from 207 leagues at
     # weekly grain to 90 at season -- real, but nowhere near the 12 that 17x would imply.
     {"roster_rate": ("roster_rate_pct", "rrates"),
      "start_rate": ("start_rate_pct", "srate"),
      "healthy_start_rate": ("healthy_start_rate_pct", "srate"),
      "ppg": ("ppg_when_started", "default"),
      # The current matchup source exposes PPG, not a total-points column;
      # canonicalization replaces this placeholder from the super table below.
      "points": ("total_points_observed", "default"),
      # T6 (Joe 2026-07-26): the served "Win %" IS won_pct = start% x win-when-started, on
      # the eligible-leagues denominator. win_rate_pct is the CONDITIONAL rate (given he
      # started) -- a different question on a different denominator, and it is what the
      # column used to read. Kept below as `win_rate_cond` for inspection, never displayed.
      "win_rate": ("win_rate_pct", "win"),
      "win_rate_cond": ("win_rate_pct", "win"),
      "expected_wins": ("expected_wins", "win"),
      "expected_losses": ("expected_losses", "win"),
      "expected_starts": ("expected_starts", "srate"),
      "won": ("won_pct", "win"), "lost": ("lost_pct", "win"),
      "clutch": ("avg_clutch_started", "clutch"), "lamar": ("total_lamar_started", "default"),
      # legacy lanes, kept inspectable so the denominator change stays auditable
      "champ_wk": ("champ_week_rate_pct", "champ"),
      "healthy_elig": ("healthy_eligible_league_weeks", "default"),
      "eligible_leagues": ("elig_league_weeks", "default"),
      "champ_elig": ("champ_elig_leagues", "default"),
      "playoff": ("playoff_rate_wkwt", "default"),
      "playoff_elig": ("playoff_eligible_leagues", "default"),
      # T7: the served four, all on the eligible-leagues denominator
      "champ_total": ("champ_total_pct", "champ"),
      "champ_started": ("champ_as_starter_pct", "champ"),
      "playoff_total": ("playoff_total_pct", "default"),
      "playoff_started": ("playoff_as_starter_pct", "default"),
      # the start-rate DENOMINATOR's scope, served so the rate is legible: a backup QB with
      # 4 active weeks and a season-long starter with 17 are not the same 20% (Joe 2026-07-20).
      # T8 adds the other side: weeks rostered but NOT on an NFL field.
      "active_weeks": ("nfl_active_weeks", "default"),
      "inactive_weeks": ("inactive_weeks", "default"),
      "started_weeks": ("started_weeks", "default"), "n_leagues": ("n_leagues", "default")}),
    ("research_matchup_weekly", "matchup", "matchup_weekly", ["NFL_player_id", "year", "week"], False,
     # WEEKLY roster% is the one stat whose required sample has been measured against a
     # precision target rather than a reliability one, so it gets its own floor (140, R23).
     # Season and career roster% stay on srate until they are measured the same way -- a
     # season is worth 3-4 weeks, not 17 (R4), so it cannot inherit the weekly number.
     {"roster_rate": ("roster_rate_pct", "rrate"),
      "start_rate": ("start_rate_pct", "srate"),
      "healthy_start_rate": ("healthy_start_rate_pct", "srate"),
      "ppg": ("ppg_when_started", "default"),
      "points": ("points_started", "default"),
      "win_rate": ("win_rate_pct", "win"),           # T6: same definition at every grain
      "win_rate_cond": ("win_rate_pct", "win"),
      "expected_wins": ("expected_wins", "win"),
      "expected_losses": ("expected_losses", "win"),
      "expected_starts": ("expected_starts", "srate"),
      "won": ("won_pct", "win"), "lost": ("lost_pct", "win"),
      "clutch": ("avg_clutch_started", "clutch"), "lamar": ("avg_lamar_started", "default"),
      "healthy_elig": ("healthy_eligible_leagues", "default"),
      "champ_elig": ("champ_eligible", "default"),
      "started_leagues": ("started_leagues", "default"),
      "eligible_leagues": ("eligible_leagues", "default"),
      "n_leagues": ("n_leagues", "default")}),
    ("research_matchup_career", "matchup", "matchup_career", ["NFL_player_id"], True,
     {"n_years": ("n_years", "default"), "roster_rate": ("roster_rate_pct", "srate"),
      "start_rate": ("start_rate_pct", "srate"),
      "healthy_start_rate": ("healthy_start_rate_pct", "srate"),
      "ppg": ("ppg_when_started", "default"), "points": ("total_points_observed", "default"),
      "win_rate": ("win_rate_pct", "win"),           # T6
      "win_rate_cond": ("win_rate_pct", "win"),
      "expected_wins": ("expected_wins", "win"),
      "expected_losses": ("expected_losses", "win"),
      "expected_starts": ("expected_starts", "srate"),
      "won": ("won_pct", "win"),
      "lost": ("lost_pct", "win"), "clutch": ("avg_clutch_started", "clutch"),
      "lamar": ("total_lamar_started", "default"), "lamar_per_season": ("total_lamar_started", "default"),
      "champ_wk": ("champ_week_rate_pct", "champ"),
      "playoff": ("playoff_rate_wkwt", "default"),
      "expected_champs": ("expected_champs", "default"),
      "expected_playoffs": ("expected_playoffs", "default"),
      "champ_total": ("champ_total_pct", "champ"),
      "champ_started": ("champ_as_starter_pct", "champ"),
      "playoff_total": ("playoff_total_pct", "default"),
      "playoff_started": ("playoff_as_starter_pct", "default"),
      "eligible_leagues": ("elig_league_weeks", "default"),
      "healthy_elig": ("healthy_eligible_league_weeks", "default"),
      "champ_elig": ("champ_elig_leagues", "default"), "started_weeks": ("started_weeks", "default"),
      "active_weeks": ("active_weeks", "default"),
      "inactive_weeks": ("inactive_weeks", "default"),
      "playoff_elig": ("playoff_eligible_leagues", "default"),
      "n_leagues": ("n_leagues", "default")}),
]

# The original research tables are intentionally kept one-row-per-player for
# compatibility. These narrow tables are the adaptive serving surface: one row
# is materialized for every requested seven-dimension combination, after the
# 150-league gate chooses the most specific usable source cell.
ADAPTIVE_TABLES = [
    "research_matchup_adaptive",
    "research_matchup_adaptive_weekly",
    "research_matchup_adaptive_career",
]
ADAPTIVE_MIN_LEAGUES = 150


def _adaptive_request_values() -> str:
    teams = ("10t", "12t")
    rosters = ("flx", "sflx", "idp")
    pprs = ("std", "half", "ppr")
    tds = ("4pt", "6pt")
    brackets = ("4po", "6po", "8po", "ALL")
    league_types = ("dynasty", "ALL")
    lineup_modes = ("best_ball", "ALL")
    rows = []
    for t in teams:
        for r in rosters:
            for p in pprs:
                for td in tds:
                    for bracket in brackets:
                        for league_type in league_types:
                            for lineup_mode in lineup_modes:
                                rows.append(
                                    "(" + ", ".join(
                                        repr(v) for v in (t, r, p, td, bracket, league_type, lineup_mode)
                                    ) + ")"
                                )
    return ",\n        ".join(rows)


def adaptive_matchup_sql(name: str, source: str, identity: list[str]) -> str:
    """Build the adaptive seven-dimension serving table.

    The source lattice contains every exact/pooled state. For each requested
    configuration, candidates may pool an individual dimension to ALL. A
    candidate with at least 150 position-eligible leagues wins over thin
    candidates; within that usable set, the most specific state wins. This is
    what makes a massive 4-team bracket stay split while a four-league 6-team
    bracket falls back independently.
    """
    idcs = ", ".join(identity + ["pos_grp"])
    req_values = _adaptive_request_values()
    match = " AND ".join(
        f"(r.{dim}=q.q_{dim} OR (q.q_{dim}<>'ALL' AND r.{dim}='ALL'))"
        for dim in ("teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode")
    )
    specificity = " + ".join(
        f"CASE WHEN q.q_{dim}<>'ALL' AND r.{dim}=q.q_{dim} THEN 1 ELSE 0 END"
        for dim in ("teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode")
    )
    partition = ", ".join([*(f"q_{dim}" for dim in ("teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode")), idcs])
    return f"""CREATE TABLE {name} AS
    WITH requests(q_teams,q_roster,q_ppr,q_td,q_bracket,q_league_type,q_lineup_mode) AS (
        VALUES {req_values}
    ), candidates AS (
        SELECT q.*, r.*,
               ({specificity}) AS _specificity,
               CASE WHEN r.n_leagues >= {ADAPTIVE_MIN_LEAGUES} THEN 1 ELSE 0 END AS _usable
        FROM requests q
        JOIN rc.{source} r ON {match}
        WHERE r.keeper_mode='ALL'
    ), chosen AS (
        SELECT * FROM candidates
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY {partition}
            ORDER BY _usable DESC, _specificity DESC, n_leagues DESC
        ) = 1
    )
    SELECT * FROM chosen"""

# FANTASY position, not roster-sheet position (Joe 2026-07-17): the super table carries
# composites ('QB,WR', 'RB,DL', 'K,P') and two-way listings -- for fantasy purposes a
# fullback/two-way back is an RB, Taysom-types resolve QB-first. One normalization, used by
# BOTH the ladder's eligibility gate and the display names table.
POS_NORM = '''CASE
    WHEN "position" LIKE '%QB%' THEN 'QB'
    WHEN "position" LIKE '%RB%' THEN 'RB'
    WHEN "position" LIKE '%WR%' THEN 'WR'
    WHEN "position" LIKE '%TE%' THEN 'TE'
    WHEN "position" = 'DEF' THEN 'DEF'
    WHEN "position" LIKE '%K%' THEN 'K'
    ELSE "position" END'''
FLX_POS = "('QB','RB','WR','TE','K','DEF')"

# slug -> super-table scoring-variant column (teams collapses: points don't depend on size)
def _fpts_col(slug: str) -> str:
    _, _, ppr, td = slug.split("_")
    return f"fpts_{td}_{'0ppr' if ppr == 'std' else ppr}"


# Canonical value columns overridden from the super table per (table, stat):
#   'ppg'    -> per-game average of the slug variant over the year's REG active weeks
#   'pts'    -> that season's variant total
#   'wk'     -> the exact week's variant points
CANON = {
    "research_draft": {"points": "pts", "lamar": "lamar_s", "lamar_ppg": "lamar_s_ppg"},
    "research_draft_career": {"points": "pts_c_sum", "lamar": "lamar_c", "lamar_ppg": "lamar_c_ppg"},
    # Matchup value lanes are population values, not averages of the leagues that
    # happened to roster/start a player. Usage and outcome lanes remain cohort-owned.
    "research_matchup": {
        "ppg": "ppg", "points": "pts", "lamar": "lamar_s",
        "active_weeks": "availability_season", "inactive_weeks": "availability_season",
    },
    "research_matchup_weekly": {"ppg": "wk", "points": "wk", "lamar": "lamar_w"},
    "research_matchup_career": {
        "ppg": "ppg_c", "points": "pts_c_sum", "lamar": "lamar_c",
        "active_weeks": "availability_career", "inactive_weeks": "availability_career",
    },
}


def _pivots(
    stats: dict[str, tuple[str, str]],
    classes: list[str],
    source_columns: set[str] | None = None,
    slugs: list[str] = SLUGS,
) -> list[str]:
    """Wide pivot expressions: one {stat}_{slug} per stat, plus the level/n lanes."""
    out = []
    for s in slugs:
        for st, (col, cl) in stats.items():
            # Cohort source schemas evolve independently of the wide API contract.
            # Missing source metrics are honest NULL lanes and are later replaced by
            # canonical super-table values where applicable.
            source_expr = col if source_columns is None or col in source_columns else "NULL"
            out.append(f"MAX(CASE WHEN slug='{s}' AND cls='{cl}' THEN {source_expr} END) AS {st}_{s}")
        # default class keeps the bare level_{slug}/n_leagues_{slug} contract; each
        # non-default class gets its own lane so mixed-basis rows stay honest per column.
        out.append(f"MAX(CASE WHEN slug='{s}' AND cls='default' THEN rung END) AS level_{s}")
        out.append(f"MAX(CASE WHEN slug='{s}' AND cls='default' THEN n_leagues END) AS n_leagues_{s}")
        for cl in classes:
            if cl == "default":
                continue
            out.append(f"MAX(CASE WHEN slug='{s}' AND cls='{cl}' THEN rung END) AS level_{cl}_{s}")
            out.append(f"MAX(CASE WHEN slug='{s}' AND cls='{cl}' THEN n_leagues END) AS n_{cl}_{s}")
    return out


def laddered_sql(name: str, src: str, idc: list[str], stats: dict[str, tuple[str, str]],
                 *, has_format: bool = False, source_columns: set[str] | None = None) -> str:
    """Finest rung per (identity, slug, CLASS) whose n_leagues clears the class threshold."""
    idcs = ", ".join(idc)
    classes = sorted({cl for _, cl in stats.values()})
    slug_rows = ", ".join(
        f"('{s}','{s.split('_')[0]}','{s.split('_')[1]}','{s.split('_')[2]}','{s.split('_')[3]}')"
        for s in SLUGS)
    cls_rows = ", ".join(f"('{cl}',{MIN_STABLE[cl]})" for cl in classes)
    # Draft confidence controls individual cells, not membership in the finished table.
    # Keep the normalized lattice as staging, then emit exactly one wide row per identity.
    universe_cte = ""
    final_from = "best"
    if "year" in idc:
        pos_view = "player_pos_year"
        pos_join = "pp.NFL_player_id = r.NFL_player_id AND pp.year = r.year"
    else:
        pos_view = "player_pos_career"
        pos_join = "pp.NFL_player_id = r.NFL_player_id"
    # The draft universe CTE has no slug in scope, so it keeps the plain flex gate.
    pos_filter = "pp.NFL_player_id IS NULL OR pp.has_flx"
    # The ladder DOES cross-join slugs: idp slugs legitimately carry IDP-only players, so
    # the flex-position gate must not drop them there (sflx starts the same positions).
    ladder_pos_filter = f"{pos_filter} OR s.s_roster='idp'"
    if name in {"research_draft", "research_draft_career"}:
        universe_ids = "r.NFL_player_id, r.year" if name == "research_draft" else "r.NFL_player_id"
        universe_cte = f""",
    universe AS (
      SELECT DISTINCT {universe_ids}
      FROM rc.draft r
      LEFT JOIN {pos_view} pp ON {pos_join}
      WHERE {pos_filter}
    )"""
        final_from = f"universe LEFT JOIN best USING ({idcs})"
    is_draft = name in {"research_draft", "research_draft_career"}
    source_confidence = "" if is_draft else "AND r.confidence='confident'"
    class_eligibility = (
        "AND ((cl.cls='adp' AND c.adp IS NOT NULL) "
        "OR (cl.cls='rate' AND c.draft_rate_pct IS NOT NULL) "
        "OR (cl.cls='dscore' AND c.draft_score IS NOT NULL) "
        "OR (cl.cls NOT IN ('adp','rate','dscore') "
        "AND c.confidence='confident' AND c.n_leagues >= cl.min_n))"
        if is_draft else "AND c.n_leagues >= cl.min_n"
    )
    format_filter = "AND r.format_level=0" if has_format else ""
    return f"""CREATE TABLE {name} AS
    WITH slugs(slug, s_teams, s_roster, s_ppr, s_td) AS (VALUES {slug_rows}),
    cls(cls, min_n) AS (VALUES {cls_rows}),
    cand AS (
      SELECT r.*, s.slug,
        CASE WHEN r.teams=s.s_teams AND r.roster=s.s_roster AND r.ppr=s.s_ppr AND r.td=s.s_td THEN 4
             WHEN r.teams=s.s_teams AND r.roster=s.s_roster AND r.ppr=s.s_ppr AND r.td='ALL' THEN 3
             WHEN r.teams=s.s_teams AND r.roster=s.s_roster AND r.ppr='ALL' AND r.td='ALL' THEN 2
             WHEN r.teams='ALL' AND r.roster='ALL' AND r.ppr='ALL' AND r.td='ALL' THEN 0
        END AS rung
      FROM rc.{src} r CROSS JOIN slugs s
      LEFT JOIN {pos_view} pp ON {pos_join}
      WHERE ({ladder_pos_filter})
        {format_filter}
        {source_confidence}
    ),
    best AS (
      SELECT * FROM (
        SELECT c.*, cl.cls,
               ROW_NUMBER() OVER (PARTITION BY {idcs}, slug, cl.cls ORDER BY rung DESC) rn
        FROM cand c CROSS JOIN cls cl
        WHERE c.rung IS NOT NULL {class_eligibility})
      WHERE rn=1
    ){universe_cte}
    SELECT {idcs}, {', '.join(_pivots(stats, classes, source_columns))} FROM {final_from} GROUP BY {idcs}"""


def exact_matchup_sql(
    name: str,
    src: str,
    idc: list[str],
    stats: dict[str, tuple[str, str]],
    *, has_format: bool = False, source_columns: set[str] | None = None,
    bracket: str = "6po",
) -> str:
    """Pivot exact matchup cells, using the mandated all-format 2003–10 pool."""
    idcs = ", ".join(idc)
    classes = sorted({cls for _, cls in stats.values()})
    has_year = "year" in idc
    slug_rows = ", ".join(
        f"('{slug}','{slug.split('_')[0]}','{slug.split('_')[1]}','{slug.split('_')[2]}','{slug.split('_')[3]}')"
        for slug in SLUGS
    )
    class_rows = ", ".join(f"('{cls}')" for cls in classes)
    format_filter = "AND r.format_level=0" if has_format else ""
    historic_rung = "CASE WHEN r.year BETWEEN 2003 AND 2010 THEN 0 ELSE 4 END" if has_year else "4"
    historic_join = """(r.year BETWEEN 2003 AND 2010 AND r.teams='ALL' AND r.roster='ALL'
             AND r.ppr='ALL' AND r.td='ALL' AND r.bracket='ALL')
          OR """ if has_year else ""
    modern_join = """(r.year NOT BETWEEN 2003 AND 2010 AND """ if has_year else ""
    modern_join_close = ")" if has_year else ""
    cohort_where = "(r.cohort_level=4 OR (r.year BETWEEN 2003 AND 2010 AND r.cohort_level=0))" if has_year else "r.cohort_level=4"
    return f"""CREATE TABLE {name} AS
      WITH slugs(slug,s_teams,s_roster,s_ppr,s_td) AS (VALUES {slug_rows}),
      cls(cls) AS (VALUES {class_rows}),
      exact AS (
        SELECT r.*,s.slug, {historic_rung} AS rung,c.cls
        FROM rc.{src} r
        JOIN slugs s ON (
          {historic_join}{modern_join}r.teams=s.s_teams
             AND r.roster=s.s_roster AND r.ppr=s.s_ppr AND r.td=s.s_td
             AND r.bracket='{bracket}'{modern_join_close}
        )
        CROSS JOIN cls c
        WHERE {cohort_where}
          AND r.confidence <> 'insufficient'
          {format_filter}
      )
      SELECT {idcs},{', '.join(_pivots(stats, classes, source_columns))}
      FROM exact GROUP BY {idcs}"""


def career_matchup_sql(
    name: str,
    src: str,
    idc: list[str],
    stats: dict[str, tuple[str, str]],
    *, source_columns: set[str] | None = None,
    bracket: str = "6po",
) -> str | None:
    """Build career matchup cells from season sufficient statistics.

    Career source rows have no year key.  Consequently, filtering that source to
    ``cohort_level=4`` cannot recover the mandated pooled 2003--2010 contribution:
    the historical all-format row is level 0 and its year information has already
    been aggregated away.  Re-aggregate the season table instead, selecting the
    level-0 historical rows and level-4 modern rows before summing additive fields.

    ``None`` is returned for old fixture schemas which do not carry the additive
    season ledger; those fixtures continue through the legacy selector and remain
    useful for testing the unrelated wide-bundle contract.
    """
    required = {
        "rostered_league_weeks", "roster_eligible_league_weeks",
        "started_team_game_weeks", "team_game_eligible_league_weeks",
        "started_active_weeks", "healthy_eligible_league_weeks",
        "champ_elig_leagues", "started_champ_active", "n_champ_leagues",
        "n_champ_start_leagues", "playoff_eligible_leagues", "n_final_po",
        "n_started_po", "po_wkwt_credit", "expected_wins", "expected_losses",
        "expected_starts", "sum_clutch_started_active", "started_weeks",
    }
    if source_columns is not None and not required.issubset(source_columns):
        return None

    idcs = ", ".join(idc)
    classes = sorted({cls for _, cls in stats.values()})
    # champ_week_rate_pct is a week-rate diagnostic. Current matchup season rows carry
    # the actual champion-eligible league-week denominator; old fixture schemas do not,
    # so retain their legacy fallback only for backward-compatible test fixtures.
    champ_week_den = (
        "SUM(champ_eligible_league_weeks) AS champ_eligible_league_weeks"
        if source_columns is not None and "champ_eligible_league_weeks" in source_columns
        else "SUM(champ_elig_leagues) AS champ_eligible_league_weeks"
    )
    slug_rows = ", ".join(
        f"('{slug}','{slug.split('_')[0]}','{slug.split('_')[1]}','{slug.split('_')[2]}','{slug.split('_')[3]}')"
        for slug in SLUGS
    )
    class_rows = ", ".join(f"('{cls}')" for cls in classes)
    return f"""CREATE TABLE {name} AS
      WITH slugs(slug,s_teams,s_roster,s_ppr,s_td) AS (VALUES {slug_rows}),
      cls(cls) AS (VALUES {class_rows}),
      selected AS (
        SELECT r.*, s.slug,
               CASE WHEN r.year BETWEEN 2003 AND 2010 THEN 0 ELSE 4 END AS rung
        FROM rc.matchup r
        JOIN slugs s ON (
          (r.year BETWEEN 2003 AND 2010 AND r.teams='ALL' AND r.roster='ALL'
             AND r.ppr='ALL' AND r.td='ALL' AND r.bracket='ALL')
          OR
          (r.year NOT BETWEEN 2003 AND 2010 AND r.teams=s.s_teams
             AND r.roster=s.s_roster AND r.ppr=s.s_ppr AND r.td=s.s_td
             AND r.bracket='{bracket}')
        )
        WHERE (r.cohort_level=4 OR
              (r.year BETWEEN 2003 AND 2010 AND r.cohort_level=0))
      ),
      a AS (
        SELECT NFL_player_id, slug, MAX(rung) AS rung,
          MAX(format_level) AS format_level,
          COUNT(DISTINCT year) AS n_years,
          SUM(n_leagues) AS n_leagues,
          SUM(active_weeks) AS active_weeks,
          SUM(started_weeks) AS started_weeks,
          SUM(elig_league_weeks) AS elig_league_weeks,
          SUM(champ_elig_leagues) AS champ_elig_leagues,
          {champ_week_den},
          SUM(po_n_rostered_leagues) AS po_n_rostered_leagues,
          -- A championship start is necessarily a playoff start. Keep the
          -- playoff-start league count closed when the source playoff signal
          -- is missing from that championship row.
          SUM(GREATEST(n_started_po, n_champ_start_leagues)) AS n_started_po,
          SUM(GREATEST(n_final_po, n_started_po, n_champ_start_leagues)) AS n_final_po,
          SUM(n_champ_leagues) AS n_champ_leagues,
          SUM(n_champ_start_leagues) AS n_champ_start_leagues,
          SUM(inactive_weeks) AS inactive_weeks,
          SUM(po_wkwt_credit) AS po_wkwt_credit,
          SUM(wins_started) AS wins_started,
          SUM(losses_started) AS losses_started,
          SUM(rostered_league_weeks) AS rostered_league_weeks,
          SUM(roster_eligible_league_weeks) AS roster_eligible_league_weeks,
          SUM(started_team_game_weeks) AS started_team_game_weeks,
          SUM(team_game_eligible_league_weeks) AS team_game_eligible_league_weeks,
          SUM(started_active_weeks) AS started_active_weeks,
          SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks,
          SUM(started_champ_active) AS started_champ_active,
          SUM(playoff_eligible_leagues) AS playoff_eligible_leagues,
          SUM(expected_wins) AS expected_wins,
          SUM(expected_losses) AS expected_losses,
          SUM(expected_starts) AS expected_starts,
          SUM(total_points_observed) AS total_points_observed,
          SUM(ppg_when_started * started_weeks)
            / NULLIF(SUM(CASE WHEN ppg_when_started IS NOT NULL THEN started_weeks END),0)
            AS ppg_when_started,
          SUM(total_lamar_started) AS total_lamar_started,
          SUM(sum_clutch_started_active) AS sum_clutch_started_active,
          SUM(1.0*n_champ_start_leagues/NULLIF(champ_elig_leagues,0)) AS expected_champs,
          SUM(1.0*GREATEST(n_started_po, n_champ_start_leagues)
            /NULLIF(playoff_eligible_leagues,0)) AS expected_playoffs
        FROM selected
        GROUP BY NFL_player_id, slug
      ),
      d AS (
        SELECT a.*,
          100.0*rostered_league_weeks/NULLIF(roster_eligible_league_weeks,0) AS roster_rate_pct,
          100.0*started_team_game_weeks/NULLIF(team_game_eligible_league_weeks,0) AS start_rate_pct,
          100.0*started_active_weeks/NULLIF(healthy_eligible_league_weeks,0) AS healthy_start_rate_pct,
          100.0*expected_wins/NULLIF(expected_starts,0) AS win_rate_pct,
          100.0*expected_wins/NULLIF(team_game_eligible_league_weeks,0) AS won_pct,
          100.0*expected_losses/NULLIF(team_game_eligible_league_weeks,0) AS lost_pct,
          -- clutch_equity is already expressed in percentage points by the
          -- source transform (e.g. 23.41 means +23.41%).  Do not apply the
          -- rate-lane 100x conversion here; that produced values such as
          -- 2341.57 in the weekly/season display.
          sum_clutch_started_active/NULLIF(champ_elig_leagues,0) AS avg_clutch_started,
          100.0*started_champ_active/NULLIF(champ_eligible_league_weeks,0) AS champ_week_rate_pct,
          100.0*po_wkwt_credit/NULLIF(po_n_rostered_leagues,0) AS playoff_rate_wkwt,
          100.0*n_final_po/NULLIF(playoff_eligible_leagues,0) AS playoff_total_pct,
          100.0*GREATEST(n_started_po, n_champ_start_leagues)
            /NULLIF(playoff_eligible_leagues,0) AS playoff_as_starter_pct,
          100.0*n_champ_leagues/NULLIF(champ_elig_leagues,0) AS champ_total_pct,
          100.0*n_champ_start_leagues/NULLIF(champ_elig_leagues,0) AS champ_as_starter_pct,
          CASE WHEN n_leagues >= 35 THEN 'confident'
               WHEN n_leagues >= 10 THEN 'mushy' ELSE 'insufficient' END AS confidence
        FROM a
      )
      SELECT {idcs},{', '.join(_pivots(stats, classes, None))}
      FROM d CROSS JOIN cls GROUP BY {idcs}"""


def weekly_sql(name: str, src: str, idc: list[str], stats: dict[str, tuple[str, str]],
               *, has_format: bool = False, source_columns: set[str] | None = None,
               bracket: str = "6po") -> str:
    """Weekly cells are exact after 2010 and use the mandated historic all-format pool."""
    idcs = ", ".join(idc)
    classes = sorted({cl for _, cl in stats.values()})
    matchup = name.startswith("research_matchup")
    slugs = SLUGS
    slug_rows = ", ".join(
        f"('{s}','{s.split('_')[0]}','{s.split('_')[1]}','{s.split('_')[2]}','{s.split('_')[3]}')"
        for s in slugs)
    # every class answers from the same rung-4 row: expand cand across the classes present.
    cls_rows = ", ".join(f"('{cl}')" for cl in classes)
    format_filter = "AND r.format_level=0" if has_format else ""
    return f"""CREATE TABLE {name} AS
    WITH slugs(slug, s_teams, s_roster, s_ppr, s_td) AS (VALUES {slug_rows}),
    cls(cls) AS (VALUES {cls_rows}),
    cand AS (
      SELECT r.*, s.slug,
             CASE WHEN r.year BETWEEN 2003 AND 2010 THEN 0 ELSE 4 END AS rung, cl.cls
      FROM rc.{src} r JOIN slugs s
        ON ((r.year BETWEEN 2003 AND 2010 AND r.teams='ALL' AND r.roster='ALL'
             AND r.ppr='ALL' AND r.td='ALL' AND r.bracket='ALL')
            OR (r.year NOT BETWEEN 2003 AND 2010 AND r.teams=s.s_teams
             AND r.roster=s.s_roster AND r.ppr=s.s_ppr AND r.td=s.s_td
             {"AND r.bracket='" + bracket + "'" if matchup else ""}))
      CROSS JOIN cls cl
      WHERE (r.cohort_level=4 OR (r.year BETWEEN 2003 AND 2010 AND r.cohort_level=0))
        AND r.confidence <> 'insufficient'
        {format_filter}
    )
    SELECT {idcs}, {', '.join(_pivots(stats, classes, source_columns, slugs))} FROM cand GROUP BY {idcs}"""


_CANON_SRC = {"ppg": ("canon_season", "ppg_", ["NFL_player_id", "year"]),
              "pts": ("canon_season", "pts_", ["NFL_player_id", "year"]),
              "wk": ("canon_week", "wk_", ["NFL_player_id", "year", "week"]),
              "lamar_w": ("canon_lamar_week", "wk_lamar_", ["NFL_player_id", "year", "week"]),
              "ppg_c": ("canon_career", "ppg_c_", ["NFL_player_id"]),
              "pts_c_sum": ("canon_career", "pts_sum_c_", ["NFL_player_id"]),
              "pts_c": ("canon_career", "pts_c_", ["NFL_player_id"]),
              "lamar_s": ("canon_lamar_season", "lamar_s_", ["NFL_player_id", "year"]),
              "lamar_s_ppg": ("canon_lamar_season", "lamar_s_ppg_", ["NFL_player_id", "year"]),
              "lamar_c": ("canon_lamar_career", "lamar_c_", ["NFL_player_id"]),
              "lamar_c_ppg": ("canon_lamar_career", "lamar_c_ppg_", ["NFL_player_id"])}


def _apply_canonical(con: duckdb.DuckDBPyConnection, name: str, idc: list[str]) -> None:
    """Replace the value lanes CANON declares with super-table canonical numbers (in place,
    on the cohort-scoped row universe). The lane is NULLed first so a row with no super-table
    match serves an honest blank -- never a leftover league-observed value silently mixed
    into a canonical column."""
    spec = CANON.get(name)
    if not spec:
        return
    for stat, kind in spec.items():
        if kind.startswith("availability_"):
            table = f"canon_{kind}"
            con.execute(f"UPDATE {name} SET " + ", ".join(
                f"{stat}_{s} = NULL" for s in SLUGS))
            sets = ", ".join(
                f"{stat}_{s} = c.{stat}_{s}"
                for s in SLUGS)
            on = " AND ".join(f"t.{k} = c.{k}" for k in idc)
            con.execute(f"UPDATE {name} t SET {sets} FROM {table} c WHERE {on}")
            continue
        table, prefix, keys = _CANON_SRC[kind]
        is_lamar = kind in ("lamar_w", "lamar_s", "lamar_s_ppg", "lamar_c", "lamar_c_ppg")
        # canonical LAMAR exists for flex cells only; the other lanes stay honestly blank
        # rather than borrowing a flex value they did not earn.
        slugs = LAMAR_SLUGS if is_lamar else SLUGS
        con.execute(f"UPDATE {name} SET " + ", ".join(f"{stat}_{s} = NULL" for s in slugs))
        sets = ", ".join(
            f"{stat}_{s} = ROUND(c.{prefix}{s if is_lamar else _fpts_col(s)}, 2)"
            for s in slugs)
        on = " AND ".join(f"t.{k} = c.{k}" for k in keys)
        con.execute(f"UPDATE {name} t SET {sets} FROM {table} c WHERE {on}")


def pad_api_columns(con: duckdb.DuckDBPyConnection, name: str, dataset: str) -> int:
    """NULL-pad columns the API binds that this table cannot produce (route SQL is strict)."""
    have = {r[1] for r in con.execute(f"PRAGMA table_info('{name}')").fetchall()}
    added = 0
    slugs = SLUGS
    for s in slugs:
        wanted = [f"{m}_{s}" for m in API_METRICS[dataset]]
        wanted += [f"level_{cl}_{s}" for cl in LANE_CLASSES] + [f"n_{cl}_{s}" for cl in LANE_CLASSES]
        for col in wanted:
            if col not in have:
                con.execute(f'ALTER TABLE {name} ADD COLUMN "{col}" DOUBLE')
                have.add(col)
                added += 1
    return added


def build_bundle(rc_path: Path = RC_PATH, ops_path: Path = OPS_PATH,
                 bundle_path: Path = BUNDLE_PATH,
                 only_datasets: set[str] | None = None) -> dict[str, int]:
    previous = bundle_path.with_suffix(".previous.duckdb")
    if bundle_path.exists():
        if previous.exists():
            previous.unlink()
        bundle_path.replace(previous)  # keep for the release gate's top-K diff
    w = duckdb.connect(str(bundle_path))
    memory_mb = os.environ.get("RESEARCH_WIDE_MEMORY_MB", "3500")
    w.execute(f"SET memory_limit='{memory_mb}MB'")
    w.execute(f"ATTACH '{Path(rc_path).as_posix()}' AS rc (READ_ONLY)")
    w.execute(f"ATTACH '{Path(ops_path).as_posix()}' AS ops (READ_ONLY)")

    # Slug position gate for the LADDER. Rung 0 pools EVERY league -- including IDP -- so
    # flx slugs only ship positions a flx roster can start. NULL positions are kept
    # (unknown != ineligible). Rung-4 cells never needed this (exact flx cells only contain
    # flx leagues' data), so weekly paths skip it.
    w.execute(f'''CREATE TEMP VIEW player_pos_year AS
      SELECT NFL_player_id, CAST(year AS INTEGER) AS year,
             BOOL_OR(({POS_NORM}) IN {FLX_POS}) AS has_flx
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE "position" IS NOT NULL AND year IS NOT NULL
      GROUP BY NFL_player_id, CAST(year AS INTEGER)''')
    w.execute(f'''CREATE TEMP VIEW player_pos_career AS
      SELECT NFL_player_id, BOOL_OR(({POS_NORM}) IN {FLX_POS}) AS has_flx
      FROM ops.nfl_historical.nfl_player_stats_all WHERE "position" IS NOT NULL
      GROUP BY NFL_player_id''')

    # Canonical points/ppg (Joe 2026-07-19): value lanes come from the super table's
    # slug-scored weeklies, not from pooled league observations. A REG-week row IS a game
    # played; a played game with no recorded points counts as 0, not a skipped game.
    variants = sorted({_fpts_col(s) for s in SLUGS})
    v_exprs = ", ".join(
        f"SUM(COALESCE({v}, 0)) / COUNT(*) AS ppg_{v}, SUM(COALESCE({v}, 0)) AS pts_{v}"
        for v in variants)
    w.execute(f"""CREATE TEMP TABLE canon_season AS
      SELECT NFL_player_id, CAST(year AS INTEGER) AS year, COUNT(*) AS games, {v_exprs}
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE season_type = 'REG' AND NFL_player_id IS NOT NULL AND week IS NOT NULL
      GROUP BY 1, 2""")
    w.execute(f"""CREATE TEMP TABLE canon_week AS
      SELECT NFL_player_id, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
             {', '.join(f'ANY_VALUE({v}) AS wk_{v}' for v in variants)}
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE season_type = 'REG' AND NFL_player_id IS NOT NULL AND week IS NOT NULL
      GROUP BY 1, 2, 3""")
    w.execute(f"""CREATE TEMP TABLE canon_lamar_week AS
      SELECT NFL_player_id, CAST(year AS INTEGER) AS year, CAST(week AS INTEGER) AS week,
             {', '.join(f'ANY_VALUE(lamar_{s}) AS wk_lamar_{s}' for s in LAMAR_SLUGS)}
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE season_type = 'REG' AND NFL_player_id IS NOT NULL AND week IS NOT NULL
      GROUP BY 1, 2, 3""")
    # career = the player's WHOLE career (Joe 2026-07-19: the super table tracks it -- a
    # Jerry Rice 2005 ADP row carries his per-game back to the '80s; capping at the cohort
    # window would throw away data we already have). Per-game over all seasons, and the
    # average SEASON total for the draft career "points" lane.
    w.execute(f"""CREATE TEMP TABLE canon_career AS
      SELECT NFL_player_id,
             {', '.join(f'SUM(pts_{v})/NULLIF(SUM(games),0) AS ppg_c_{v}, AVG(pts_{v}) AS pts_c_{v}, SUM(pts_{v}) AS pts_sum_c_{v}' for v in variants)}
      FROM canon_season GROUP BY 1""")
    w.execute(f"""CREATE TEMP TABLE canon_lamar_career AS
      SELECT NFL_player_id,
             {', '.join(f'SUM(lamar_{s}) AS lamar_c_{s}, AVG(lamar_{s}) AS lamar_c_ppg_{s}' for s in LAMAR_SLUGS)}
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE season_type = 'REG' AND NFL_player_id IS NOT NULL AND week IS NOT NULL
      GROUP BY 1""")
    w.execute(f"""CREATE TEMP TABLE canon_lamar_season AS
      SELECT NFL_player_id, CAST(year AS INTEGER) AS year,
             {', '.join(f'SUM(lamar_{s}) AS lamar_s_{s}, AVG(lamar_{s}) AS lamar_s_ppg_{s}' for s in LAMAR_SLUGS)}
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE season_type = 'REG' AND NFL_player_id IS NOT NULL AND week IS NOT NULL
      GROUP BY 1, 2""")
    # Availability is an NFL calendar property, never a property of the leagues
    # that happened to roster a player.  A missing stat row is an inactive week;
    # byes are excluded because the fixed team-game calendar is 16/17 games.
    availability_cols = ", ".join(
        # A source player can have duplicate/traded-team rows or an out-of-window
        # week.  Availability is bounded by the regular-season team-game calendar;
        # never publish a negative inactive count or more active games than that
        # calendar contains.
        f"LEAST(COUNT(DISTINCT week), "
        f"(CASE WHEN CAST(year AS INTEGER) <= 2020 THEN 16 ELSE 17 END)) "
        f"AS active_weeks_{s}, "
        f"GREATEST((CASE WHEN CAST(year AS INTEGER) <= 2020 THEN 16 ELSE 17 END) "
        f"- COUNT(DISTINCT week), 0) AS inactive_weeks_{s}"
        for s in SLUGS)
    w.execute(f"""CREATE TEMP TABLE canon_availability_season AS
      SELECT NFL_player_id, CAST(year AS INTEGER) AS year, {availability_cols}
      FROM ops.nfl_historical.nfl_player_stats_all
      WHERE season_type = 'REG' AND NFL_player_id IS NOT NULL AND week IS NOT NULL
      GROUP BY 1, 2""")
    career_availability_cols = ", ".join(
        f"SUM(active_weeks_{s}) AS active_weeks_{s}, SUM(inactive_weeks_{s}) AS inactive_weeks_{s}"
        for s in SLUGS)
    w.execute(f"""CREATE TEMP TABLE canon_availability_career AS
      SELECT NFL_player_id, {career_availability_cols}
      FROM canon_availability_season
      WHERE year BETWEEN 2003 AND 2025
      GROUP BY 1""")

    counts: dict[str, int] = {}
    table_specs = [spec for spec in TABLES
                   if only_datasets is None or spec[1] in only_datasets]
    if not table_specs:
        raise ValueError("only_datasets selected no supported bundle datasets")
    for name, dataset, src, idc, laddered, stats in table_specs:
        source_columns = {col[0] for col in w.execute(f"SELECT * FROM rc.{src} LIMIT 0").description}
        # Career matchup cells are rebuilt from the season ledger, while the output
        # pivot still contains the career-derived field names. Keep those two schemas
        # distinct: the career table intentionally omits the additive season columns.
        career_season_columns = None
        if name == "research_matchup_career":
            career_season_columns = {
                col[0] for col in w.execute("SELECT * FROM rc.matchup LIMIT 0").description
            }
        has_format = "format_level" in source_columns
        targets = [(name, "6po")]
        if dataset == "matchup":
            targets = [(name if bracket == "6po" else f"{name}_{bracket}", bracket)
                       for bracket in MATCHUP_BRACKETS]
        for target_name, bracket in targets:
            if dataset == "matchup" and laddered:
                sql = None
                if target_name.startswith("research_matchup_career"):
                    sql = career_matchup_sql(target_name, src, idc, stats,
                                             source_columns=career_season_columns, bracket=bracket)
                if sql is None:
                    sql = exact_matchup_sql(target_name, src, idc, stats, has_format=has_format,
                                            source_columns=source_columns, bracket=bracket)
            else:
                sql = (laddered_sql(target_name, src, idc, stats, has_format=has_format, source_columns=source_columns) if laddered
                       else weekly_sql(target_name, src, idc, stats, has_format=has_format,
                                       source_columns=source_columns, bracket=bracket))
            w.execute(sql)
            _apply_canonical(w, target_name, idc)
            if target_name.startswith("research_matchup_career"):
                w.execute(f"UPDATE {target_name} SET " + ", ".join(
                f"lamar_per_season_{slug} = lamar_{slug} / NULLIF(n_years_{slug}, 0)"
                for slug in SLUGS
                ))
            pad_api_columns(w, target_name, dataset)
            n = w.execute(f"SELECT COUNT(*) FROM {target_name}").fetchone()[0]
            counts[target_name] = n
            if laddered:
                lv = w.execute(f"""SELECT
                SUM(CASE WHEN level_12t_flx_ppr_4pt=4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN level_12t_flx_ppr_4pt=3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN level_12t_flx_ppr_4pt=2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN level_12t_flx_ppr_4pt=0 THEN 1 ELSE 0 END) FROM {target_name}""").fetchone()
                print(f"  {target_name}: {n:,} rows | 12t_flx_ppr_4pt default rungs 4/3/2/0 = "
                  f"{lv[0] or 0:,}/{lv[1] or 0:,}/{lv[2] or 0:,}/{lv[3] or 0:,}")
            else:
                print(f"  {target_name}: {n:,} rows (rung-4 only)")

    # Publish the adaptive narrow serving surfaces after the legacy wide tables
    # are built. They intentionally preserve all source metrics and dimensions;
    # the frontend filters request_* and never needs to infer a fallback at
    # request time.
    adaptive_specs = [
        ("research_matchup_adaptive", "matchup", ["NFL_player_id", "year"]),
        ("research_matchup_adaptive_weekly", "matchup_weekly", ["NFL_player_id", "year", "week"]),
        ("research_matchup_adaptive_career", "matchup_career", ["NFL_player_id"]),
    ]
    if only_datasets is None or "matchup" in only_datasets:
        available_sources = {
            row[0] for row in w.execute(
                "SELECT table_name FROM rc.information_schema.tables WHERE table_schema='main'"
            ).fetchall()
        }
        for adaptive_name, adaptive_source, adaptive_identity in adaptive_specs:
            if adaptive_source not in available_sources:
                # Small compatibility fixtures may predate the adaptive source
                # tables. Real release snapshots always carry all three.
                continue
            w.execute(adaptive_matchup_sql(adaptive_name, adaptive_source, adaptive_identity))
            counts[adaptive_name] = w.execute(
                f"SELECT COUNT(*) FROM {adaptive_name}"
            ).fetchone()[0]
            print(f"  {adaptive_name}: {counts[adaptive_name]:,} rows (150-league adaptive lattice)")

    w.execute(f'''CREATE TABLE research_player_names AS
      WITH ranked_stats AS (
        SELECT NFL_player_id, player, {POS_NORM} AS "position",
               "position" AS position_raw,
               ROW_NUMBER() OVER (
                 PARTITION BY NFL_player_id
                 ORDER BY year DESC NULLS LAST, week DESC NULLS LAST,
                          {POS_NORM} ASC NULLS LAST, "position" ASC NULLS LAST,
                          player ASC
               ) AS rn
        FROM ops.nfl_historical.nfl_player_stats_all
        WHERE player IS NOT NULL AND NFL_player_id IS NOT NULL
      )
      SELECT NFL_player_id, player, "position", position_raw
      FROM ranked_stats WHERE rn=1
      UNION ALL
      -- bio fallback (ledger D7): rostered-but-never-played players (stashed rookies, injured
      -- draftees -- Will Howard, Matt Corral) have NO stat rows, so stat-derived names left
      -- them rendering blank. Their IDs are correct GSIS; player_bio carries their identity.
      SELECT b.NFL_player_id, ANY_VALUE(b.player) AS player,
             ANY_VALUE({POS_NORM.replace('"position"', 'b.nfl_position')}) AS "position",
             ANY_VALUE(b.nfl_position) AS position_raw
      FROM ops.nfl_historical.player_bio b
      WHERE b.player IS NOT NULL AND b.NFL_player_id IS NOT NULL
        AND b.NFL_player_id NOT IN (
          SELECT DISTINCT NFL_player_id FROM ops.nfl_historical.nfl_player_stats_all
          WHERE player IS NOT NULL AND NFL_player_id IS NOT NULL)
      GROUP BY b.NFL_player_id''')
    counts["research_player_names"] = w.execute(
        "SELECT COUNT(*) FROM research_player_names").fetchone()[0]
    w.execute("DETACH rc")
    w.execute("DETACH ops")
    w.close()
    print(f"wide bundle: {bundle_path} ({bundle_path.stat().st_size / 1e6:.1f} MB)")
    return counts


if __name__ == "__main__":
    requested = {item.strip() for item in
                 os.environ.get("RESEARCH_WIDE_DATASETS", "").split(",") if item.strip()}
    build_bundle(only_datasets=requested or None)
