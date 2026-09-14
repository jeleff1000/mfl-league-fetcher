"""build_research_draft_cohort.py -- the draft-behavior cohort table (local parquet).

Grain: (format, teams, roster, ppr, td, year, NFL_player_id) with the coarsening LATTICE
baked in via GROUPING SETS ('ALL' sentinels on collapsed dims). Single-season format,
full-draft (>=10 rounds) snake picks only. Value stays in the super-table lamar_<slug>
columns; this table holds behavior + the market delta.

ELIGIBILITY GATES (docs/runbooks/research-eligibility-gates-2026-07-16.md):
  * Class A (position): a K/DEF pick only counts in leagues whose roster carries that slot,
    and the draft_rate denominator is scoped the same way (K -> leagues with a K slot, DEF ->
    leagues with a DEF slot, SKILL -> all). Only 75%/81% of single-season league-years carry
    K/DEF slots, so the ungated rate caps kickers at the slot-share -- the same structural
    artifact the matchup start_rate fix removed. BOTH sides must be scoped: gating the
    numerator alone reproduces the cap.
  * Keeper picks are excluded from the PRICE signal (adp / adp_p10 / adp_p90): a kept pick's
    slot is assigned by keeper rules, not bid by the room. is_keeper is 100% populated on all
    platforms (max_keepers is a 26.5% Yahoo capture gap -- never gate on it). Keeper LEAGUES
    stay in the cohort; n_drafted still counts kept acquisitions (roster-day presence).
    adp_incl_keepers_legacy retains the old definition so before/after is inspectable.

Columns: cohort dims + cohort_level + adp (robust MEDIAN pick, keeper-excluded) + adp_p10/p90 +
n_drafted + draft_rate + market_adp + market_delta + n_leagues (hidden) + confidence.

Reads the local ___leagues corpus (draft x league_settings, single_season 2002-2025), joins the
local market-ADP parquet offline. Local parquet output only.

    py -3 scripts/research_cohorts/build_research_draft_cohort.py
"""
from __future__ import annotations
import hashlib
import os

import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from draft_metric_sql import auction_bid_pct_sql, auction_cost_select_sql
from market_adp import expand_market_only_players
try:
    from .year_config import configured_years, year_predicate
    from .cohort_format_sql import (
        FORMAT_SELECT,
        cohort_league_settings_sql,
        grouping_sets_with_formats,
    )
except ImportError:
    from year_config import configured_years, year_predicate
    from cohort_format_sql import FORMAT_SELECT, cohort_league_settings_sql, grouping_sets_with_formats

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

OUT_DIR = Path(os.environ.get("RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
OUT = OUT_DIR / "research_draft_player_season.parquet"
_REQUESTED_YEARS = configured_years(default_start=2004)
YEARS = tuple(year for year in _REQUESTED_YEARS if year >= 2004)
YEAR_PREDICATE = year_predicate("ls.year", default_start=2004)
# Bump whenever player_sql semantics change; stale yearly aggregates can otherwise survive
# a correct source edit (v2 adds per-year auction classification to numerator and cost).
YEAR_CACHE_DIR = OUT_DIR / "draft_year_cache_v6_native_draft_floor"
# REFERENCE inputs this cycle does NOT build. nfl_market_adp is external (FFC JSON + Yahoo
# draft analysis), not lake-derived, so it does not move when the lake grows -- and a sample
# run must still resolve it from the production dir rather than an empty sample dir.
REF_DIR = Path(os.environ.get(
    "RESEARCH_REF_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
MARKET = REF_DIR / "nfl_market_adp.parquet"
THRESHOLDS = REF_DIR / "ladder_thresholds.json"  # versioned r-target table (ladder_stab.py)

MIN_DISPLAY, MIN_STABLE = 10, 35  # cohort-year league-count gates


def native_draft_rate_sql(
    drafted_expr: str = "j.n_drafted",
    leagues_expr: str = "j.n_leagues",
) -> str:
    """Return native cohort draft coverage; external market rows are not coverage."""
    return (
        f"CASE WHEN {drafted_expr} > 0 AND {leagues_expr} IS NOT NULL "
        f"THEN ROUND(100.0*{drafted_expr}/NULLIF({leagues_expr},0),2) END"
    )


def load_adp_r85() -> tuple[int | None, str | None]:
    """The n where own-cohort ADP reaches split-half r=0.85 (Joe 2026-07-18 §0.1: market ADP
    is the preferred instrument until the cohort's OWN adp clears this bar). Read from the
    versioned threshold table; absent -> (None, None) and the build serves own-ADP with a
    loud warning to run ladder_stab.py first."""
    if not THRESHOLDS.exists():
        return None, None
    import json
    doc = json.loads(THRESHOLDS.read_text())
    return (doc.get("metrics", {}).get("adp", {}).get("n_r85"), doc.get("version"))

_DRAFT_EXTRAS = (
    "COALESCE(s.roster_K, 0) AS k_slots",
    "COALESCE(s.roster_DEF, 0) AS def_slots",
)
_LS = cohort_league_settings_sql(extra_select=_DRAFT_EXTRAS)
_GSETS = grouping_sets_with_formats(
    [("teams", "roster", "ppr", "td"), ("teams", "roster", "ppr"),
     ("teams", "roster"), ()],
    tail=("NFL_player_id",),
)
# pos_grp is in EVERY denominator set: it scopes eligibility, it is not a dim that coarsens.
_GSETS_DENOM = grouping_sets_with_formats(
    [("teams", "roster", "ppr", "td"), ("teams", "roster", "ppr"),
     ("teams", "roster"), ()],
    tail=("year", "pos_grp"),
)


def pruned_psv_sql(year: int, source: str = "public.player_slug_value") -> str:
    """Canonical values limited to drafted player/slug pairs in eligible leagues."""
    return f"""
      SELECT v.NFL_player_id, v.week, v.slug, v.lamar, v.fpts
      FROM {source} v
      JOIN (SELECT DISTINCT slug, NFL_player_id FROM bdr) needed
        USING (slug, NFL_player_id)
      WHERE v.year = {year}
    """


def stable_sum_sql(expression: str) -> str:
    """Accumulate floating inputs deterministically, preserving a DOUBLE output."""
    return f"CAST(SUM(CAST(({expression}) AS DECIMAL(38,12))) AS DOUBLE)"


def stable_avg_sql(expression: str) -> str:
    """Average via the same order-stable accumulator used by additive shard stats."""
    return (
        f"CAST(SUM(CAST(({expression}) AS DECIMAL(38,12))) "
        f"/ NULLIF(COUNT({expression}),0) AS DOUBLE)"
    )

def player_sql(year: int, *, per_league: bool = False) -> str:
    # per-year to bound each scan on Fly (mirrors the matchup builder's pattern).
    cohort_select = (
        "db_name, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode"
        if per_league
        else f"COALESCE(teams,'ALL') teams, COALESCE(roster,'ALL') roster, "
             f"COALESCE(ppr,'ALL') ppr, COALESCE(td,'ALL') td, {FORMAT_SELECT}"
    )
    position_select = "MAX(position) AS position, " if per_league else ""
    group_by = (
        "teams, roster, ppr, td, league_type, lineup_mode, keeper_mode, "
        "db_name, NFL_player_id"
        if per_league else _GSETS
    )
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_DRAFT_EXTRAS)}),
pos AS (SELECT NFL_player_id, position, broad_position, broad_positions FROM public.player_position WHERE year = {year}),
-- QUARANTINE (2026-07-17): leagues whose enrichments accumulate across the Sleeper
-- previous_league_id lineage. Signature: any draft row with a physically impossible season
-- total (>700). Measured contamination in these leagues: draft manager_lamar p99 2x / max
-- 687,199; txn ros-lamar max 4,317; weekly lamar max 115,416. Their BEHAVIOR (picks, adds,
-- starts) is real and stays; their VALUE enrichments are lies and go NULL until the pipeline
-- enrichment is fixed and re-ingested.
bad AS (
  SELECT DISTINCT d.db_name
  FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year
  WHERE d.total_fantasy_points > 700),
-- RECREATION (Joe 2026-07-17: recreate, don't NULL). The corrupt leagues' own rows are
-- unusable (even weekly points corrupt), so replacements come from the super table's
-- slug-matched precomputed values: season fpts for total_fantasy_points, and the
-- rostered-window slug lamar (roster FACTS in player_fantasy are clean) for manager_lamar.
-- pq (a pipeline model z-score) is not recreatable and stays NULL for these leagues.
bdr AS (
  SELECT DISTINCT ls.teams || '_flx_' || ls.ppr || '_' || ls.td AS slug,
         d.NFL_player_id
  FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year
  WHERE d.year = {year} AND d.NFL_player_id IS NOT NULL AND ls.roster = 'flx'),
psv AS MATERIALIZED ({pruned_psv_sql(year)}),
recp AS (SELECT slug, NFL_player_id, {stable_sum_sql('fpts')} AS season_fpts FROM psv GROUP BY 1, 2),
-- CANONICAL manager LAMAR (Joe 2026-07-19 ledger D2, extended to draft = D16, 2026-07-21).
-- LAMAR VALUE must NEVER be league-native: 376 corrupt league-years escape every gate and
-- carry native draft manager_lamar to 2,214 (a served 12t_flx_half_6pt row hit 934, past the
-- release gate's +/-600 bound). The value is the super-table slug canon summed over the
-- rostered window (roster FACTS in player_fantasy are clean); native survives only as the
-- *_native_legacy diagnostic. Now computed for EVERY flx league, not just the >700-quarantined
-- ones. Non-flx has no slug in the 12-slug canon and FAILS CLOSED to NULL (D2).
-- DISTINCT dedups the 32 corpus leagues carrying 2-10 identical player_fantasy rows per
-- (db,week,player) -- undeduped SUM multiplies the value (same 24x fanout class as the txn ROS
-- lane and the matchup pfd guard).
recl AS (
  SELECT pf.db_name, pf.NFL_player_id, {stable_sum_sql('psv.lamar')} AS rost_lamar
  FROM (SELECT DISTINCT p.db_name, p.NFL_player_id, p.week,
               ls.teams, ls.ppr, ls.td
        FROM public.player_fantasy p
        JOIN ls ON ls.db_name = p.db_name AND ls.year = p.year
        WHERE p.year = {year} AND ls.roster = 'flx'
          AND CAST(p.is_rostered AS INT) = 1 AND p.NFL_player_id IS NOT NULL) pf
  JOIN psv ON psv.slug = pf.teams || '_flx_' || pf.ppr || '_' || pf.td
        AND psv.NFL_player_id = pf.NFL_player_id AND psv.week = pf.week
  GROUP BY 1, 2),
dsize AS (
  SELECT d.db_name, d.year, COUNT(*) AS picks,
         COUNT(*) FILTER (WHERE COALESCE(d.cost,0) > 0) AS paid_picks
  FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year
  WHERE d.pick IS NOT NULL AND d.year = {year} GROUP BY 1,2
),
-- AUCTION BUDGET (Joe 2026-07-20). Auction cost only compares across leagues as a % of
-- budget -- a $50 bid means nothing until you know it was out of $200 or $400. No platform
-- writes the budget into our flat settings (276 columns checked: only waiver_budget, which
-- is FAAB and a different quantity), so it is DERIVED from the spending itself:
--
--   1. 3rd-HIGHEST team spend -- Joe's rule. Not the max, because leagues that allow
--      auction-dollar trading let a team or two spend past the nominal cap. Teams otherwise
--      spend essentially their whole budget, so this sits on the true number: validated on
--      sampled leagues at exactly 400 / 1000 / 200.
--   2. top spend, when fewer than 3 teams carry an identity.
--   3. total spend / teams -- the identity-free fallback, ~0.7% low because a little is
--      always left on the table. This is what the lake uses until the re-fold populates
--      manager/franchise_id on draft rows (contract widened 2026-07-20).
aspend AS (
  SELECT d.db_name, COALESCE(d.manager, d.franchise_id) AS team, SUM(d.cost) AS spend
  FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year
  WHERE d.year = {year} AND COALESCE(d.cost,0) > 0
    AND COALESCE(d.manager, d.franchise_id) IS NOT NULL
  GROUP BY 1, 2),
arank AS (SELECT db_name, spend,
                 ROW_NUMBER() OVER (PARTITION BY db_name ORDER BY spend DESC) AS rk
          FROM aspend),
atop AS (SELECT db_name, MAX(CASE WHEN rk = 3 THEN spend END) AS third,
                MAX(CASE WHEN rk = 1 THEN spend END) AS top1
         FROM arank GROUP BY 1),
atot AS (
  SELECT d.db_name, SUM(d.cost) AS total_cost, MAX(ls.num_teams) AS nt
  FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year
  WHERE d.year = {year} AND COALESCE(d.cost,0) > 0
  GROUP BY 1),
abud AS (
  SELECT t.db_name,
         COALESCE(a.third, a.top1, t.total_cost / NULLIF(t.nt, 0)) AS budget,
         CASE WHEN a.third IS NOT NULL THEN 'third_spend'
              WHEN a.top1  IS NOT NULL THEN 'top_spend'
              ELSE 'total_over_teams' END AS budget_basis
  FROM atot t LEFT JOIN atop a ON a.db_name = t.db_name),
picks AS (
  -- include auction too (scores are capital-normalized); ADP stays snake-only via FILTER.
  SELECT d.NFL_player_id, ls.teams, ls.roster, ls.ppr, ls.td,
         ls.league_type, ls.lineup_mode, ls.keeper_mode,
         d.db_name, d.pick, d.cost,
         z.paid_picks >= 0.25 * z.picks AS is_auction,
         -- auction price as % of that league's budget: THE unit that compares across leagues
         CASE WHEN z.paid_picks >= 0.25 * z.picks AND COALESCE(d.cost,0) > 0
              THEN {auction_bid_pct_sql('d.cost', 'ab.budget')} END AS cost_pct,
         ab.budget_basis,
         COALESCE(d.is_keeper, false) AS is_keeper,
         pos.position AS position,
         CASE WHEN b.db_name IS NULL THEN COALESCE(d.draft_value_zscore, d.pick_quality_zscore) END AS pq,
         -- D16: canonical for EVERY flx league (recl), NULL for non-flx (fail closed). Native
         -- kept only as the legacy diagnostic. total_fantasy_points keeps its >700-quarantine
         -- recreation -- that is a POINTS-corruption gate, orthogonal to D2's LAMAR ruling.
         recl.rost_lamar AS manager_lamar,
         d.manager_lamar AS manager_lamar_native_legacy,
         CASE WHEN b.db_name IS NULL THEN d.total_fantasy_points ELSE recp.season_fpts END AS total_fantasy_points,
         -- which eligibility denominator this player belongs to (see DENOM_SQL)
         CASE WHEN pos.position IN ('K','DEF') THEN pos.position ELSE 'SKILL' END AS pos_grp
  FROM public.draft d
  JOIN ls ON d.db_name = ls.db_name AND d.year = ls.year
  JOIN dsize z ON z.db_name = d.db_name AND z.year = d.year
  LEFT JOIN bad b ON b.db_name = d.db_name
  LEFT JOIN recl ON recl.db_name = d.db_name AND recl.NFL_player_id = d.NFL_player_id
  LEFT JOIN recp ON b.db_name IS NOT NULL AND ls.roster = 'flx'
         AND recp.slug = ls.teams || '_flx_' || ls.ppr || '_' || ls.td
         AND recp.NFL_player_id = d.NFL_player_id
  LEFT JOIN pos ON pos.NFL_player_id = d.NFL_player_id
  LEFT JOIN abud ab ON ab.db_name = d.db_name
  WHERE d.pick IS NOT NULL AND d.NFL_player_id IS NOT NULL AND d.year = {year}
    AND z.picks >= 10 * NULLIF(ls.num_teams,0)
    -- Class A: a K/DEF pick only counts in leagues where that position can actually start.
    AND (COALESCE(pos.position,'') <> 'K'   OR ls.k_slots   > 0)
    AND (COALESCE(pos.position,'') <> 'DEF' OR ls.def_slots > 0)
      -- ELIGIBILITY (Joe 2026-07-20): a player only counts in cohorts whose roster can
      -- actually start his position. Broad class comes from position-taxonomy.v1, so
      -- detailed labels (RDE, RG, OLB...) resolve correctly. Without this a punter and
      -- IDP players leaked into flx boards -- IDP clutch is legitimate IN an IDP cohort,
      -- the defect is the leak.
      -- ANY of a player's classes may make him eligible: dual-eligible players (Taysom
      -- Hill 'QB,TE') must not be dropped just because the compound string maps to nothing.
      AND (list_has_any(pos.broad_positions, ['QB','RB','WR','TE','K','DEF'])
           OR (ls.roster = 'idp' AND list_has_any(pos.broad_positions, ['DL','LB','DB'])))
)
SELECT {cohort_select},
       {year} AS year, NFL_player_id, {position_select}
       -- functionally determined by the player, so MAX() adds no rows and keeps the
       -- lattice unchanged while still carrying the join key to the denominator.
       MAX(pos_grp) AS pos_grp,
       -- price signal is keeper-excluded: a kept pick's slot is rule-assigned, not room-bid.
       -- adp is the MEAN pick (Joe 2026-07-17: the real average is jagged — 31.8, not 32.0 —
       -- and the UI renders it in round units). adp_med keeps the robust median as the
       -- diagnostic; the n>=5 player gate is what defends the mean against joke picks.
       ROUND(AVG(CASE WHEN COALESCE(cost,0)=0 AND NOT is_keeper THEN pick END),2) AS adp,
       ROUND(MEDIAN(CASE WHEN COALESCE(cost,0)=0 AND NOT is_keeper THEN pick END),1) AS adp_med,
       ROUND(QUANTILE_CONT(CASE WHEN COALESCE(cost,0)=0 AND NOT is_keeper THEN pick END,0.1),1) AS adp_p10,
       ROUND(QUANTILE_CONT(CASE WHEN COALESCE(cost,0)=0 AND NOT is_keeper THEN pick END,0.9),1) AS adp_p90,
       -- kept for diagnostics: the OLD definition (keepers in), so before/after is
       -- inspectable rather than a claim. Not for display.
       ROUND(MEDIAN(CASE WHEN COALESCE(cost,0)=0 THEN pick END),1) AS adp_incl_keepers_legacy,
       COUNT(DISTINCT db_name) AS n_drafted,
       ROUND({stable_avg_sql('pq')},3) AS avg_pick_quality,
       ROUND({stable_avg_sql('manager_lamar')},2) AS avg_manager_lamar,
       ROUND({stable_avg_sql('manager_lamar_native_legacy')},2) AS avg_manager_lamar_native_legacy,
       ROUND(AVG(CASE WHEN is_auction AND COALESCE(cost,0)>0 THEN cost END),1) AS avg_auction_cost,
       -- the comparable unit: raw dollars are meaningless across a $200 and a $400 league
       ROUND({stable_avg_sql('cost_pct')},2) AS avg_auction_cost_pct,
       ROUND(MEDIAN(cost_pct),2) AS med_auction_cost_pct,
       COUNT(DISTINCT CASE WHEN is_auction AND COALESCE(cost,0)>0 THEN db_name END) AS n_auction_leagues,
       MAX(budget_basis) AS auction_budget_basis,
       ROUND({stable_avg_sql('CASE WHEN is_auction AND COALESCE(cost,0)>0 THEN manager_lamar/NULLIF(cost,0) END')},3) AS avg_lamar_per_dollar,
       -- validity gate, NOT a stat choice: 284 single-season-classified keeper-lineage
       -- leagues carry MULTI-YEAR totals in total_fantasy_points (Derek Watt 1,799 = his
       -- 7-season career; 4,226 rows > 700). No real NFL season has reached 700, so >700 is
       -- corrupt input, refused here until the pipeline enrichment is fixed upstream.
       ROUND({stable_avg_sql('CASE WHEN total_fantasy_points <= 700 THEN total_fantasy_points END')},1) AS avg_fantasy_points
       ,SUM(CASE WHEN COALESCE(cost,0)=0 AND NOT is_keeper THEN pick END) AS sum_adp_pick
       ,COUNT(CASE WHEN COALESCE(cost,0)=0 AND NOT is_keeper AND pick IS NOT NULL THEN 1 END) AS n_adp_pick
       ,{stable_sum_sql('pq')} AS sum_pick_quality
       ,COUNT(pq) AS n_pick_quality
       ,{stable_sum_sql('manager_lamar')} AS sum_manager_lamar
       ,COUNT(manager_lamar) AS n_manager_lamar
       ,{stable_sum_sql('manager_lamar_native_legacy')} AS sum_manager_lamar_native
       ,COUNT(manager_lamar_native_legacy) AS n_manager_lamar_native
       ,SUM(CASE WHEN is_auction AND COALESCE(cost,0)>0 THEN cost END) AS sum_auction_cost
       ,COUNT(CASE WHEN is_auction AND COALESCE(cost,0)>0 THEN 1 END) AS n_auction_cost
       ,{stable_sum_sql('cost_pct')} AS sum_auction_cost_pct
       ,COUNT(cost_pct) AS n_auction_cost_pct
       ,{stable_sum_sql('CASE WHEN is_auction AND COALESCE(cost,0)>0 THEN manager_lamar/NULLIF(cost,0) END')} AS sum_lamar_per_dollar
       ,COUNT(CASE WHEN is_auction AND COALESCE(cost,0)>0
                   AND manager_lamar IS NOT NULL THEN 1 END) AS n_lamar_per_dollar
       ,{stable_sum_sql('CASE WHEN total_fantasy_points <= 700 THEN total_fantasy_points END')} AS sum_fantasy_points
       ,COUNT(CASE WHEN total_fantasy_points <= 700
                   AND total_fantasy_points IS NOT NULL THEN 1 END) AS n_fantasy_points
FROM picks GROUP BY {group_by}
"""

# The denominator must be ELIGIBILITY-SCOPED, not just the numerator. Gating a kicker's picks
# while dividing by every full-draft league still caps him at the share of leagues carrying a
# K slot. One denominator per position-group:
#   K     -> full-draft leagues with a K slot
#   DEF   -> full-draft leagues with a DEF slot
#   SKILL -> every full-draft league (QB/RB/WR/TE start in all of them)
DENOM_SQL = f"""
WITH ls AS ({_LS}),
dsize AS (
  SELECT db_name, year, COUNT(*) AS picks,
         COUNT(*) FILTER (WHERE COALESCE(cost,0) > 0) AS paid_picks
  FROM public.draft WHERE pick IS NOT NULL GROUP BY 1,2
),
full_leagues AS (
  SELECT ls.db_name, ls.year, ls.teams, ls.roster, ls.ppr, ls.td,
         ls.league_type, ls.lineup_mode, ls.keeper_mode, ls.k_slots, ls.def_slots,
         z.paid_picks >= 0.25 * z.picks AS is_auction
  FROM ls JOIN dsize z ON z.db_name = ls.db_name AND z.year = ls.year
  WHERE z.picks >= 10 * NULLIF(ls.num_teams,0) AND {YEAR_PREDICATE}
),
elig AS (
  SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
         is_auction, 'SKILL' AS pos_grp FROM full_leagues
  UNION ALL SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode,
         keeper_mode, is_auction, 'K' FROM full_leagues WHERE k_slots > 0
  UNION ALL SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode,
         keeper_mode, is_auction, 'DEF' FROM full_leagues WHERE def_slots > 0
)
SELECT COALESCE(teams,'ALL') teams, COALESCE(roster,'ALL') roster,
       COALESCE(ppr,'ALL') ppr, COALESCE(td,'ALL') td, {FORMAT_SELECT}, year, pos_grp,
       COUNT(DISTINCT db_name) AS n_leagues,
       COUNT(DISTINCT CASE WHEN is_auction THEN db_name END) AS n_auction_eligible_leagues
FROM elig GROUP BY {_GSETS_DENOM}
"""


def main() -> None:
    # load_env + Fly reads
    import os
    env_text = (ROOT / ".env").read_text(encoding="utf-8") if (ROOT / ".env").exists() else ""
    for line in env_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))
    from local_reader import LocalReader, source_fingerprint
    fly = LocalReader()  # local (real snapshot UNION grind corpus); offline, no Fly dependency

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    # DuckDB's default relative .tmp directory is shared by every local process. A long
    # historical scan can otherwise collide with another reader's spill files on Windows.
    temp_dir = OUT_DIR / f".duckdb_tmp_draft_{os.getpid()}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    fly.con.execute(f"SET temp_directory='{temp_dir.as_posix()}/reader'")
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute(f"SET temp_directory='{temp_dir.as_posix()}/aggregate'")
    players: list[dict] = []
    # Full-union and merge-safe real-delta scans must never share yearly cache files.
    # The rows have identical schemas but represent different source populations.
    year_cache_dir = YEAR_CACHE_DIR / source_fingerprint()
    year_cache_dir.mkdir(parents=True, exist_ok=True)
    for y in YEARS:
        query_hash = hashlib.sha1(player_sql(y).encode()).hexdigest()[:12]
        year_cache = year_cache_dir / f"research_draft_{y}_{query_hash}.parquet"
        if year_cache.exists():
            rows = pq.read_table(year_cache).to_pylist()
            basis = "cache"
        else:
            rows = fly.query(player_sql(y), "___leagues")
            pq.write_table(pa.Table.from_pylist(rows), year_cache)
            basis = "local scan"
        players.extend(rows)
        print(f"  [draft] {y}: {len(rows):,} cohort-player rows ({basis})", flush=True)
    denoms = fly.query(DENOM_SQL, "___leagues")
    market_rows = pq.read_table(MARKET).to_pylist()
    external_only = expand_market_only_players(players, denoms, market_rows)
    players.extend(external_only)
    print(f"  [draft] external-only player/cohort rows added: {len(external_only):,}")
    con.register("p", pa.Table.from_pylist(players))
    con.register("d", pa.Table.from_pylist(denoms))
    # Market ADP lanes, most-specific first (2026-07-18: use the external sources PROPERLY
    # per cohort instead of pooling):
    #   mkt_t  -- FFC format ADP at the cohort's OWN league size (FFC ships 10t AND 12t
    #             files; averaging them threw away a dimension we cohort on),
    #   mkt_f  -- FFC format ADP pooled across sizes (fallback when one size is missing),
    #   mkt_b  -- Yahoo format-BLIND ADP 2005-2025 (year fallback: FFC half starts 2018,
    #             ppr 2010, and EVERY format ends 2024 -- without this 2025 market ADP is 0%).
    con.execute(f"""CREATE VIEW mkt_t AS
        SELECT year, market_format, CASE WHEN teams <= 11 THEN '10t' ELSE '12t' END AS teams,
               NFL_player_id,
               SUM(adp * COALESCE(NULLIF(times_drafted, 0), 1)) /
                 NULLIF(SUM(COALESCE(NULLIF(times_drafted, 0), 1)), 0) AS adp
        FROM '{MARKET.as_posix()}' WHERE adp IS NOT NULL AND teams IS NOT NULL GROUP BY 1,2,3,4""")
    con.execute(f"""CREATE VIEW mkt_f AS
        SELECT year, market_format, NFL_player_id,
               SUM(adp * COALESCE(NULLIF(times_drafted, 0), 1)) /
                 NULLIF(SUM(COALESCE(NULLIF(times_drafted, 0), 1)), 0) AS adp
        FROM '{MARKET.as_posix()}' WHERE adp IS NOT NULL GROUP BY 1,2,3""")
    con.execute(f"""CREATE VIEW mkt_b AS
        SELECT year, NFL_player_id,
               SUM(adp * COALESCE(NULLIF(times_drafted, 0), 1)) /
                 NULLIF(SUM(COALESCE(NULLIF(times_drafted, 0), 1)), 0) AS adp,
               SUM(pct_drafted * COALESCE(NULLIF(times_drafted, 0), 1)) FILTER (WHERE pct_drafted IS NOT NULL) /
                 NULLIF(SUM(COALESCE(NULLIF(times_drafted, 0), 1)) FILTER (WHERE pct_drafted IS NOT NULL), 0) AS pct_drafted
        FROM '{MARKET.as_posix()}' WHERE adp IS NOT NULL AND market_format='blind' GROUP BY 1,2""")

    # ADP source selection (Joe 2026-07-18 §0.1): market ADP is the preferred instrument
    # until the cohort's OWN adp clears its split-half r=0.85 target from the versioned
    # threshold table. The SERVED `adp` column carries the selected instrument (downstream
    # grades/isotonic ride it -- "isotonic may fit on market ADP" falls out); `adp_own`
    # retains the cohort's raw signal, `adp_source` marks the basis per row.
    adp_r85, thr_version = load_adp_r85()
    if adp_r85 is None:
        print("[draft-cohort] WARNING: no ladder_thresholds.json (run ladder_stab.py) -- "
              "serving OWN adp everywhere, unversioned", flush=True)
    own_fallback = "j.n_drafted >= 5 AND j.adp_own IS NOT NULL AND j.avg_fantasy_points IS NOT NULL"

    # cohort_level: how many dims are real (not ALL); market_format pick per cohort.
    auction_cost_sql = auction_cost_select_sql(
        "j.avg_auction_cost_pct", "j.n_auction_leagues", "j.n_auction_eligible_leagues")
    con.execute(f"""
      CREATE TABLE final AS
      WITH j AS (
        SELECT p.* RENAME (adp AS adp_own), d.n_leagues, d.n_auction_eligible_leagues,
          (CASE WHEN p.teams<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN p.roster<>'ALL' THEN 1 ELSE 0 END)
          +(CASE WHEN p.ppr<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN p.td<>'ALL' THEN 1 ELSE 0 END) AS cohort_level,
          (CASE WHEN p.league_type<>'ALL' THEN 1 ELSE 0 END)
          +(CASE WHEN p.lineup_mode<>'ALL' THEN 1 ELSE 0 END)
          +(CASE WHEN p.keeper_mode<>'ALL' THEN 1 ELSE 0 END) AS format_level,
          CASE WHEN p.roster='sflx' THEN 'sflx'
               WHEN p.ppr IN ('std','half','ppr') THEN p.ppr ELSE 'blind' END AS mkt_fmt
        -- pos_grp in the join: each player divides by the leagues that could START them,
        -- not by every full-draft league in the cohort.
        FROM p JOIN d USING (
          teams, roster, ppr, td, league_type, lineup_mode, keeper_mode, year, pos_grp
        )
      )
      SELECT j.teams, j.roster, j.ppr, j.td,
             j.league_type, j.lineup_mode, j.keeper_mode, j.cohort_level, j.format_level,
             j.year, j.NFL_player_id,
             -- served instrument: own once stable at r=0.85, else market (finest lane first),
             -- else own even when thin (honest fallback, labeled) rather than a blank.
             COALESCE(mt.adp, mf.adp, mb.adp,
                      CASE WHEN {own_fallback} THEN j.adp_own END) AS adp,
             CASE WHEN mt.adp IS NOT NULL THEN 'market_' || j.mkt_fmt || '_' || j.teams
                  WHEN mf.adp IS NOT NULL THEN 'market_' || j.mkt_fmt
                  WHEN mb.adp IS NOT NULL THEN 'market_blind'
                  WHEN {own_fallback} THEN 'own'
                  END AS adp_source,
             j.adp_own, j.adp_med, j.adp_p10, j.adp_p90, j.adp_incl_keepers_legacy, j.n_drafted, j.n_leagues,
             j.n_auction_eligible_leagues,
             {native_draft_rate_sql()} AS draft_rate_pct,
             mb.pct_drafted AS market_draft_rate_pct,
             CASE WHEN mb.pct_drafted IS NOT NULL THEN 'market_blind' ELSE 'own' END AS draft_rate_source,
             COALESCE(mt.adp, mf.adp, mb.adp) AS market_adp,
             -- behavioral delta stays own-vs-market regardless of which instrument serves.
             ROUND(j.adp_own - COALESCE(mt.adp, mf.adp, mb.adp),1) AS market_delta,
             CASE WHEN mt.adp IS NOT NULL THEN j.mkt_fmt || '_' || j.teams
                  WHEN mf.adp IS NOT NULL THEN j.mkt_fmt
                  WHEN mb.adp IS NOT NULL THEN 'blind' END AS market_basis,
             j.avg_pick_quality, j.avg_manager_lamar, j.avg_manager_lamar_native_legacy, j.avg_auction_cost,
             -- auction price as % of budget: the cross-league-comparable unit (Joe 2026-07-20)
             j.avg_auction_cost_pct, j.med_auction_cost_pct, j.n_auction_leagues,
             {auction_cost_sql},
             j.auction_budget_basis,
             j.avg_lamar_per_dollar, j.avg_fantasy_points,
             j.sum_adp_pick, j.n_adp_pick, j.sum_pick_quality, j.n_pick_quality,
             j.sum_manager_lamar, j.n_manager_lamar,
             j.sum_manager_lamar_native, j.n_manager_lamar_native,
             j.sum_auction_cost, j.n_auction_cost,
             j.sum_auction_cost_pct, j.n_auction_cost_pct,
             j.sum_lamar_per_dollar, j.n_lamar_per_dollar,
             j.sum_fantasy_points, j.n_fantasy_points,
             CASE WHEN j.n_leagues >= {MIN_STABLE} THEN 'confident'
                  WHEN j.n_leagues >= {MIN_DISPLAY} THEN 'mushy' ELSE 'insufficient' END AS confidence
      FROM j
      LEFT JOIN mkt_t mt ON mt.year=j.year AND mt.NFL_player_id=j.NFL_player_id
           AND mt.market_format = j.mkt_fmt AND j.teams <> 'ALL' AND mt.teams = j.teams
      LEFT JOIN mkt_f mf ON mf.year=j.year AND mf.NFL_player_id=j.NFL_player_id
           AND mf.market_format = j.mkt_fmt
      LEFT JOIN mkt_b mb ON mb.year=j.year AND mb.NFL_player_id=j.NFL_player_id
    """)
    con.execute(f"COPY (SELECT * FROM final ORDER BY year, cohort_level DESC, avg_pick_quality DESC NULLS LAST) TO '{OUT.as_posix()}'")

    n, ncells, nconf = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT (teams||roster||ppr||td||league_type||lineup_mode||keeper_mode||year)), "
        "COUNT(*) FILTER (WHERE confidence='confident') FROM final").fetchone()
    withmkt = con.execute("SELECT COUNT(*) FROM final WHERE market_adp IS NOT NULL").fetchone()[0]
    print(f"[draft-cohort] {n:,} rows -> {OUT}")
    print(f"[draft-cohort] {ncells:,} cohort-year cells | {nconf:,} confident rows | market_adp joined on {withmkt:,}")
    print(f"[draft-cohort] adp r=0.85 target: n>={adp_r85} (thresholds {thr_version or 'MISSING'})")
    for src, cnt in con.execute(
            "SELECT COALESCE(adp_source,'(null)'), COUNT(*) FROM final GROUP BY 1 ORDER BY 2 DESC").fetchall():
        print(f"    adp_source {src}: {cnt:,}")


if __name__ == "__main__":
    main()
