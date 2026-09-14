"""build_research_txn_cohort.py -- transaction (waiver) behavior + scoring cohort table.

Mirrors the per-league `aggregate_transaction_player_career` pipeline transform, but grouped
by cohort instead of by manager/league, so best-added / most-regretful-drop boards can be
ranked the same way the league pages do -- just segmented by rule-set.

Same lattice/gating pattern as the draft family (build_research_draft_cohort.py); source is
the transactions table, read PER YEAR via LocalReader (local snapshots only — this build
NEVER HITS FLY; see the runbook's Fly-boundary section). Single-season format, 2015-2025.

ELIGIBILITY GATES (docs/runbooks/research-eligibility-gates-2026-07-16.md):
  * Class A (position): a K/DEF add or drop only counts in leagues whose roster carries that
    slot, and the add/drop-rate denominators are scoped the same way (K -> leagues with a K
    slot, DEF -> leagues with a DEF slot, SKILL -> all). Only 75%/81% of single-season
    league-years carry K/DEF slots, so the ungated rate caps kicker-streaming at the
    slot-share -- the same structural artifact the matchup start_rate fix removed. BOTH sides
    must be scoped: gating the numerator alone reproduces the cap.

Grain: (teams, roster, ppr, td, year, NFL_player_id) with GROUPING-SETS lattice.
Columns: cohort dims + cohort_level
  + add_rate/drop_rate/net_add_rate (% of cohort leagues) + n_add_leagues/n_drop_leagues
  + avg_faab_bid
  + avg_transaction_score  (add quality index, 100-centered; pipeline AVG)
  + avg_add_lamar          (realized value while managed; manager_lamar_ros_managed -> best-added)
  + avg_drop_regret        (value the dropped player then piled up; player_lamar_ros_total -> worst-drop)
  + n_leagues (hidden) + confidence.

    py -3 scripts/research_cohorts/build_research_txn_cohort.py
"""
from __future__ import annotations
import os

import sys
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
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
OUT = OUT_DIR / "research_txn_player_season.parquet"
MIN_DISPLAY, MIN_STABLE = 10, 35
YEARS = configured_years()
YEAR_PREDICATE = year_predicate()

_FORMAT_FIELDS = [
    pa.field("teams", pa.string()), pa.field("roster", pa.string()),
    pa.field("ppr", pa.string()), pa.field("td", pa.string()),
    pa.field("league_type", pa.string()), pa.field("lineup_mode", pa.string()),
    pa.field("keeper_mode", pa.string()),
]
PLAYER_SCHEMA = pa.schema([
    *_FORMAT_FIELDS, pa.field("year", pa.int64()), pa.field("NFL_player_id", pa.string()),
    pa.field("pos_grp", pa.string()), pa.field("n_add_leagues", pa.int64()),
    pa.field("n_drop_leagues", pa.int64()),
    *[pa.field(name, pa.float64()) for name in (
        "avg_faab_pct", "avg_faab_bid", "avg_transaction_score", "avg_add_lamar",
        "avg_drop_regret", "sum_faab_pct",
    )],
    pa.field("n_faab_pct", pa.int64()), pa.field("sum_faab_bid", pa.float64()),
    pa.field("n_faab_bid", pa.int64()), pa.field("sum_transaction_score", pa.float64()),
    pa.field("n_transaction_score", pa.int64()), pa.field("sum_add_lamar", pa.float64()),
    pa.field("n_add_lamar", pa.int64()), pa.field("sum_drop_regret", pa.float64()),
    pa.field("n_drop_regret", pa.int64()),
])
DENOM_SCHEMA = pa.schema([
    *_FORMAT_FIELDS, pa.field("year", pa.int64()), pa.field("pos_grp", pa.string()),
    pa.field("n_leagues", pa.int64()),
])


def typed_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Keep empty historical shards queryable by retaining their SQL contract."""
    return pa.Table.from_pylist(rows, schema=schema)

# match the pipeline's transaction-type families (aggregate_transaction_context.py)
_ADD = "('add','pickup','claim','waiver','add/drop')"

_TXN_EXTRAS = (
    "COALESCE(s.roster_K, 0) AS k_slots",
    "COALESCE(s.roster_DEF, 0) AS def_slots",
    "s.waiver_budget",
)
_LS = cohort_league_settings_sql(extra_select=_TXN_EXTRAS)
_GS_P = grouping_sets_with_formats(
    [("teams", "roster", "ppr", "td"), ("teams", "roster", "ppr"),
     ("teams", "roster"), ()],
    tail=("NFL_player_id",),
)
# pos_grp is in EVERY denominator set: it scopes eligibility, it is not a dim that coarsens.
_GS_D = grouping_sets_with_formats(
    [("teams", "roster", "ppr", "td"), ("teams", "roster", "ppr"),
     ("teams", "roster"), ()],
    tail=("year", "pos_grp"),
)


def pruned_psv_sql(year: int, source: str = "public.player_slug_value") -> str:
    """Canonical slug values limited to player/slug pairs used by this year's txns.

    ``player_slug_value`` is the NFL population crossed with all 12 scoring slugs.  Reading
    the whole view before the transaction join multiplies an already-large yearly lookup by
    12 and exhausted LocalReader's memory even in sample mode.  ``btx`` is deliberately the
    pruning relation: it contains only eligible flx transaction players and their exact slug.
    Week is not pruned here because ROS calculations need every later week in the season.
    """
    return f"""
      SELECT v.NFL_player_id, v.week, v.slug, v.lamar
      FROM {source} v
      JOIN (SELECT DISTINCT slug, NFL_player_id FROM btx) needed
        USING (slug, NFL_player_id)
      WHERE v.year = {year}
    """


def player_sql(year: int, *, per_league: bool = False) -> str:
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
        if per_league else _GS_P
    )
    return f"""
WITH ls AS ({cohort_league_settings_sql(year=year, extra_select=_TXN_EXTRAS)}),
pos AS (SELECT NFL_player_id, position, broad_position, broad_positions FROM public.player_position WHERE year = {year}),
-- QUARANTINE: lineage-corrupt leagues (see the draft builder's bad CTE for the receipts --
-- txn ros-lamar in these leagues: p99 1.5x, max 4,317 vs 868 normal). transaction_score (a
-- pipeline model output, not recreatable) stays NULL for them.
bad AS (
  SELECT DISTINCT d.db_name
  FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year
  WHERE d.total_fantasy_points > 700),
-- CANONICAL ROS values for EVERY flx league (Joe 2026-07-19, ledger D2: value never
-- league-observed -- 376 corrupt league-years escape every gate, so native ros-lamar is
-- never served). player_lamar_ros_total = slug lamar over weeks AFTER the txn (pure player
-- fact); manager_lamar_ros_managed = same but only over the league's own rostered weeks
-- after the add (roster FACTS stay observed). Non-flx leagues have no slug -> NULL.
btx AS (
  SELECT DISTINCT ls.teams || '_flx_' || ls.ppr || '_' || ls.td AS slug, t.NFL_player_id, t.week
  FROM public.transactions t
  JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year AND ls.roster = 'flx'
  WHERE t.year = {year} AND t.NFL_player_id IS NOT NULL AND t.week IS NOT NULL),
psv AS MATERIALIZED ({pruned_psv_sql(year)}),
ros AS (
  SELECT btx.slug, btx.NFL_player_id, btx.week, SUM(psv.lamar) AS ros_lamar
  FROM btx JOIN psv ON psv.slug = btx.slug AND psv.NFL_player_id = btx.NFL_player_id
        AND psv.week > btx.week
  GROUP BY 1, 2, 3),
-- FANOUT GUARD (2026-07-20). One (league, player, week) can carry MANY add rows -- real
-- churn: added, dropped, re-added. 24.3% of 2024 add rows are such repeats, worst case 41.
-- player_fantasy carries a few duplicate rostered rows too (0.13%). Summing across the raw
-- join multiplies the ROS value by BOTH counts: it inflated J.K. Dobbins 2024 to 619.9
-- against a true canonical season LAMAR of 26.0 (24x). The ROS window for a given
-- (league, player, week) is a SINGLE fact, so both sides are reduced to distinct grain
-- before the sum.
rosm AS (
  SELECT tx.db_name, tx.NFL_player_id, tx.week, SUM(psv.lamar) AS rosm_lamar
  FROM (SELECT DISTINCT t.db_name, t.NFL_player_id, t.week, ls.teams, ls.ppr, ls.td
        FROM public.transactions t
        JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year AND ls.roster = 'flx'
        WHERE t.year = {year} AND t.NFL_player_id IS NOT NULL AND t.week IS NOT NULL
          AND t.transaction_type IN {_ADD}) tx
  JOIN (SELECT DISTINCT p.db_name, p.NFL_player_id, p.week
        FROM public.player_fantasy p
        JOIN ls ON ls.db_name = p.db_name AND ls.year = p.year
        WHERE p.year = {year} AND CAST(p.is_rostered AS INT) = 1
          AND p.NFL_player_id IS NOT NULL) pf
        ON pf.db_name = tx.db_name AND pf.NFL_player_id = tx.NFL_player_id
       AND pf.week > tx.week
  JOIN psv ON psv.slug = tx.teams || '_flx_' || tx.ppr || '_' || tx.td
        AND psv.NFL_player_id = pf.NFL_player_id AND psv.week = pf.week
  GROUP BY 1, 2, 3),
-- budget proxies for leagues whose budget is unrecorded or contradicted:
--   ms: top franchise's season SPEND (Joe 2026-07-17: "% of the manager who spent the most") --
--       an aggressive manager's spend ~= the budget; calibrated post-build vs known budgets.
--   mb: max single bid -- identity floor (a bid can't exceed its bidder's spend, but rows with
--       NULL franchise_id would undercount ms, so GREATEST keeps pct <= 100 unconditionally).
ms AS (SELECT db_name, MAX(spend) AS max_spend FROM (
         SELECT t.db_name, t.franchise_id, SUM(t.faab_bid) AS spend
         FROM public.transactions t
         JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year
         WHERE t.year = {year} AND t.faab_bid > 0 AND t.franchise_id IS NOT NULL
         GROUP BY 1, 2) GROUP BY 1),
mb AS (SELECT t.db_name, MAX(t.faab_bid) AS max_bid
       FROM public.transactions t
       JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year
       WHERE t.year = {year} AND t.faab_bid > 0 GROUP BY 1),
tx AS (
  SELECT t.NFL_player_id, ls.teams, ls.roster, ls.ppr, ls.td,
    ls.league_type, ls.lineup_mode, ls.keeper_mode, t.db_name,
    (t.transaction_type IN {_ADD}) AS is_add,
    (t.transaction_type = 'drop') AS is_drop,
    t.faab_bid,
    -- recorded budget wins unless a bid contradicts it (7 of 639 with-bid league-years);
    -- otherwise: top-franchise spend, floored by max bid. pct <= 100 by identity.
    100.0 * t.faab_bid / NULLIF(
      CASE WHEN ls.waiver_budget > 0 AND COALESCE(mb.max_bid, 0) <= ls.waiver_budget
           THEN ls.waiver_budget
           ELSE GREATEST(COALESCE(ms.max_spend, 0), COALESCE(mb.max_bid, 0)) END, 0) AS faab_pct,
    CASE WHEN b.db_name IS NULL THEN t.transaction_score END AS transaction_score,
    rosm.rosm_lamar AS manager_lamar_ros_managed,
    ros.ros_lamar AS player_lamar_ros_total,
    pos.position AS position,
    -- which eligibility denominator this player belongs to (see DENOM_SQL)
    CASE WHEN pos.position IN ('K','DEF') THEN pos.position ELSE 'SKILL' END AS pos_grp
  FROM public.transactions t JOIN ls ON t.db_name = ls.db_name AND t.year = ls.year
  LEFT JOIN mb ON mb.db_name = t.db_name
  LEFT JOIN ms ON ms.db_name = t.db_name
  LEFT JOIN bad b ON b.db_name = t.db_name
  LEFT JOIN ros ON ls.roster = 'flx'
         AND ros.slug = ls.teams || '_flx_' || ls.ppr || '_' || ls.td
         AND ros.NFL_player_id = t.NFL_player_id AND ros.week = t.week
  LEFT JOIN rosm ON rosm.db_name = t.db_name
         AND rosm.NFL_player_id = t.NFL_player_id AND rosm.week = t.week
  LEFT JOIN pos ON pos.NFL_player_id = t.NFL_player_id
  WHERE t.year = {year} AND t.NFL_player_id IS NOT NULL
    AND (t.transaction_type IN {_ADD} OR t.transaction_type = 'drop')
    -- Class A: a K/DEF move only counts in leagues where that position can actually start.
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
SELECT {cohort_select}, {year} AS year, NFL_player_id, {position_select}
       -- functionally determined by the player, so MAX() adds no rows and keeps the
       -- lattice unchanged while still carrying the join key to the denominator.
       MAX(pos_grp) AS pos_grp,
       COUNT(DISTINCT CASE WHEN is_add THEN db_name END) AS n_add_leagues,
       COUNT(DISTINCT CASE WHEN is_drop THEN db_name END) AS n_drop_leagues,
       -- headline: % of budget (unit-safe across $100/$200/$1000 leagues); raw bid kept as
       -- the legacy diagnostic, not for display.
       ROUND(AVG(CASE WHEN is_add AND faab_pct > 0 THEN faab_pct END),1) AS avg_faab_pct,
       ROUND(AVG(CASE WHEN is_add AND faab_bid > 0 THEN faab_bid END),1) AS avg_faab_bid,
       ROUND(AVG(CASE WHEN is_add THEN transaction_score END),1) AS avg_transaction_score,
       ROUND(AVG(CASE WHEN is_add THEN manager_lamar_ros_managed END),2) AS avg_add_lamar,
       ROUND(AVG(CASE WHEN is_drop THEN player_lamar_ros_total END),2) AS avg_drop_regret,
       -- Sufficient statistics for lossless public/private shard assembly. The assembler
       -- sums these and recalculates means; it never averages rounded shard outputs.
       SUM(CASE WHEN is_add AND faab_pct > 0 THEN faab_pct END) AS sum_faab_pct,
       COUNT(CASE WHEN is_add AND faab_pct > 0 THEN 1 END) AS n_faab_pct,
       SUM(CASE WHEN is_add AND faab_bid > 0 THEN faab_bid END) AS sum_faab_bid,
       COUNT(CASE WHEN is_add AND faab_bid > 0 THEN 1 END) AS n_faab_bid,
       SUM(CASE WHEN is_add THEN transaction_score END) AS sum_transaction_score,
       COUNT(CASE WHEN is_add AND transaction_score IS NOT NULL THEN 1 END) AS n_transaction_score,
       SUM(CASE WHEN is_add THEN manager_lamar_ros_managed END) AS sum_add_lamar,
       COUNT(CASE WHEN is_add AND manager_lamar_ros_managed IS NOT NULL THEN 1 END) AS n_add_lamar,
       SUM(CASE WHEN is_drop THEN player_lamar_ros_total END) AS sum_drop_regret,
       COUNT(CASE WHEN is_drop AND player_lamar_ros_total IS NOT NULL THEN 1 END) AS n_drop_regret
FROM tx GROUP BY {group_by}
"""


# The denominator must be ELIGIBILITY-SCOPED, not just the numerator (see the matchup
# builder's DENOM_SQL for the measured cost of getting this wrong). One denominator per
# position-group: K -> leagues with a K slot, DEF -> leagues with a DEF slot, SKILL -> all.
DENOM_SQL = f"""
WITH ls AS ({_LS}),
elig AS (
  SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode, keeper_mode,
         'SKILL' AS pos_grp FROM ls
  UNION ALL SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode,
         keeper_mode, 'K' FROM ls WHERE k_slots > 0
  UNION ALL SELECT db_name, year, teams, roster, ppr, td, league_type, lineup_mode,
         keeper_mode, 'DEF' FROM ls WHERE def_slots > 0
)
SELECT COALESCE(teams,'ALL') teams, COALESCE(roster,'ALL') roster, COALESCE(ppr,'ALL') ppr,
       COALESCE(td,'ALL') td, {FORMAT_SELECT}, year, pos_grp,
       COUNT(DISTINCT db_name) AS n_leagues
FROM elig WHERE {YEAR_PREDICATE} GROUP BY {_GS_D}
"""


def main() -> None:
    import os
    env_text = (ROOT / ".env").read_text(encoding="utf-8") if (ROOT / ".env").exists() else ""
    for line in env_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))
    from local_reader import LocalReader
    fly = LocalReader()  # local (real snapshot UNION grind corpus); offline, no Fly dependency

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(); con.execute("SET memory_limit='1500MB'")
    players: list[dict] = []
    for y in YEARS:
        rows = fly.query(player_sql(y), "___leagues")
        players.extend(rows)
        print(f"  [txn] {y}: {len(rows):,} cohort-player rows")
    con.register("p", typed_table(players, PLAYER_SCHEMA))
    con.register("d", typed_table(fly.query(DENOM_SQL, "___leagues"), DENOM_SCHEMA))
    con.execute(f"""
      CREATE TABLE final AS
      SELECT p.teams, p.roster, p.ppr, p.td,
        p.league_type, p.lineup_mode, p.keeper_mode,
        (CASE WHEN p.teams<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN p.roster<>'ALL' THEN 1 ELSE 0 END)
        +(CASE WHEN p.ppr<>'ALL' THEN 1 ELSE 0 END)+(CASE WHEN p.td<>'ALL' THEN 1 ELSE 0 END) AS cohort_level,
        (CASE WHEN p.league_type<>'ALL' THEN 1 ELSE 0 END)
        +(CASE WHEN p.lineup_mode<>'ALL' THEN 1 ELSE 0 END)
        +(CASE WHEN p.keeper_mode<>'ALL' THEN 1 ELSE 0 END) AS format_level,
        p.year, p.NFL_player_id,
        ROUND(100.0*p.n_add_leagues/NULLIF(d.n_leagues,0),1) AS add_rate_pct,
        ROUND(100.0*p.n_drop_leagues/NULLIF(d.n_leagues,0),1) AS drop_rate_pct,
        ROUND(100.0*(p.n_add_leagues-p.n_drop_leagues)/NULLIF(d.n_leagues,0),1) AS net_add_rate_pct,
        p.avg_faab_pct, p.avg_faab_bid, p.avg_transaction_score, p.avg_add_lamar, p.avg_drop_regret,
        p.sum_faab_pct, p.n_faab_pct, p.sum_faab_bid, p.n_faab_bid,
        p.sum_transaction_score, p.n_transaction_score,
        p.sum_add_lamar, p.n_add_lamar, p.sum_drop_regret, p.n_drop_regret,
        p.n_add_leagues, p.n_drop_leagues, d.n_leagues,
        CASE WHEN d.n_leagues >= {MIN_STABLE} THEN 'confident'
             WHEN d.n_leagues >= {MIN_DISPLAY} THEN 'mushy' ELSE 'insufficient' END AS confidence
      -- pos_grp in the join: each player divides by the leagues that could START them, not by
      -- every league in the cohort.
      FROM p JOIN d USING (
        teams, roster, ppr, td, league_type, lineup_mode, keeper_mode, year, pos_grp
      )
    """)
    con.execute(f"COPY (SELECT * FROM final ORDER BY year, cohort_level DESC, avg_add_lamar DESC NULLS LAST) TO '{OUT.as_posix()}'")
    n, conf = con.execute("SELECT COUNT(*), COUNT(*) FILTER (WHERE confidence='confident') FROM final").fetchone()
    dup = con.execute("""SELECT COUNT(*)-COUNT(DISTINCT (
        teams||roster||ppr||td||league_type||lineup_mode||keeper_mode||year||NFL_player_id
    )) FROM final""").fetchone()[0]
    print(f"[txn-cohort] {n:,} rows ({conf:,} confident, dup={dup}) -> {OUT}")


if __name__ == "__main__":
    main()
